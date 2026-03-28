//! Insert executor
//!
//! Inserts rows into a table.

use async_trait::async_trait;

use crate::planner::logical::{ResolvedColumn, ResolvedExpr};
use crate::raft::RowChange;
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

use crate::server::session::UserVariables;

use super::context::TransactionContext;
use super::encoding::{encode_pk_key, encode_row, encode_row_key};
use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Insert executor
///
/// Uses batch allocation for row IDs to reduce atomic contention.
/// Instead of calling next_row_id() N times (N atomics), we allocate
/// all IDs at once in open() (1 atomic per INSERT statement).
pub struct Insert {
    /// Table name
    table: String,
    /// Target columns
    columns: Vec<ResolvedColumn>,
    /// Values to insert (each inner vec is one row)
    values: Vec<Vec<ResolvedExpr>>,
    /// Transaction context (for MVCC versioning)
    txn_context: Option<TransactionContext>,
    /// Number of rows inserted
    rows_inserted: u64,
    /// Whether execution is complete
    done: bool,
    /// Pre-allocated row ID batch (start_local, node_id)
    row_id_batch: Option<(u64, u64)>,
    /// Next local ID to use from batch
    next_local_id: u64,
    /// Column indices that are auto_increment
    auto_increment_indices: Vec<usize>,
    /// Last auto-generated ID (for LAST_INSERT_ID)
    last_insert_id: u64,
    /// Primary key column indices (for PK-based storage keys)
    pk_column_indices: Vec<usize>,
    /// User variables
    user_variables: UserVariables,
    /// IGNORE modifier — suppress errors, skip bad rows
    ignore: bool,
}

impl Insert {
    /// Create a new insert executor
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
        txn_context: Option<TransactionContext>,
        auto_increment_indices: Vec<usize>,
        pk_column_indices: Vec<usize>,
        user_variables: UserVariables,
        ignore: bool,
    ) -> Self {
        Insert {
            table,
            columns,
            values,
            txn_context,
            rows_inserted: 0,
            done: false,
            row_id_batch: None,
            next_local_id: 0,
            auto_increment_indices,
            last_insert_id: 0,
            pk_column_indices,
            user_variables,
            ignore,
        }
    }

    /// Get the last auto-generated ID
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
}

#[async_trait]
impl Executor for Insert {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows_inserted = 0;
        self.done = false;

        // Pre-allocate row IDs for all rows (1 atomic instead of N)
        let row_count = self.values.len() as u64;
        if row_count > 0 {
            let (start_local, node_id) = allocate_row_id_batch(row_count);
            self.row_id_batch = Some((start_local, node_id));
            self.next_local_id = start_local;
        }

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Get batch info (set in open())
        let (_, node_id) = self.row_id_batch.ok_or_else(|| {
            super::error::ExecutorError::Internal("Row ID batch not initialized".to_string())
        })?;

        // Insert all rows
        let empty_row = Row::empty();

        // RAII guard: strict DML context cleared on drop (even on early return/error)
        let _strict_guard = super::eval::StrictDmlGuard::new(&self.user_variables);

        'row_loop: for value_row in &self.values {
            // Evaluate expressions to get datum values
            let mut datums = Vec::with_capacity(value_row.len());
            let mut skip_row = false;
            for (col_idx, expr) in value_row.iter().enumerate() {
                let datum = match evaluate(expr, &empty_row, &self.user_variables) {
                    Ok(d) => d,
                    Err(_) if self.ignore => {
                        skip_row = true;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                // Coerce value to match column type (e.g. Bytes → Bit)
                // In non-strict mode (sql_mode lacks STRICT_TRANS_TABLES), clamp
                // overflows to MIN/MAX with a warning instead of erroring.
                let datum = if col_idx < self.columns.len() {
                    let col_type = &self.columns[col_idx].data_type;
                    match super::eval::coerce_to_column_type(datum.clone(), col_type) {
                        Ok(d) => d,
                        Err(super::error::ExecutorError::DataOutOfRange(_))
                            if !super::eval::is_strict_trans_tables(&self.user_variables) =>
                        {
                            // Non-strict mode: clamp overflow to type boundary
                            super::eval::clamp_to_type_boundary(&datum, col_type)
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    datum
                };
                datums.push(datum);
            }
            if skip_row {
                self.next_local_id += 1;
                continue 'row_loop;
            }

            // Get next row ID from pre-allocated batch
            let row_id = encode_row_id(self.next_local_id, node_id);
            self.next_local_id += 1;

            // Replace NULL values in auto_increment columns with generated ID
            // Use the low 48 bits (local counter) as the auto_increment value
            let auto_id = row_id & 0x0000_FFFF_FFFF_FFFF;
            for &idx in &self.auto_increment_indices {
                if idx < datums.len() {
                    // MySQL: both NULL and 0 trigger auto_increment
                    let should_generate =
                        datums[idx].is_null() || matches!(datums[idx], super::datum::Datum::Int(0));
                    if should_generate {
                        datums[idx] = super::datum::Datum::Int(auto_id as i64);
                        self.last_insert_id = auto_id;
                    }
                }
            }

            // Validate NOT NULL constraints (after auto_increment so generated values pass).
            // MySQL behavior: single-row INSERT with explicit NULL → error 1048.
            // Multi-row INSERT in non-strict mode → convert NULL to column default.
            // INSERT IGNORE: convert NULL to column default instead of erroring.
            let is_multi_row = self.values.len() > 1;
            for (col_idx, datum) in datums.iter_mut().enumerate() {
                if col_idx < self.columns.len()
                    && !self.columns[col_idx].nullable
                    && datum.is_null()
                {
                    if is_multi_row || self.ignore {
                        // Non-strict mode / IGNORE: replace NULL with type default
                        *datum =
                            super::datum::Datum::default_for_type(&self.columns[col_idx].data_type);
                    } else {
                        return Err(super::error::ExecutorError::NullValue(format!(
                            "Column '{}' cannot be null",
                            self.columns[col_idx].name
                        )));
                    }
                }
            }

            let row = Row::new(datums);

            // Use PK-based storage key if PK columns are known, else fall back to row_id
            let key = if !self.pk_column_indices.is_empty() {
                let pk_values: Vec<_> = self
                    .pk_column_indices
                    .iter()
                    .map(|&idx| row.get(idx).unwrap().clone())
                    .collect();
                encode_pk_key(&self.table, &pk_values)
            } else {
                encode_row_key(&self.table, row_id)
            };

            let value = encode_row(&row);

            // Collect the change for Raft replication.
            let ctx = self.txn_context.as_mut().ok_or_else(|| {
                super::error::ExecutorError::Internal(
                    "INSERT requires transaction context".to_string(),
                )
            })?;
            ctx.add_change(RowChange::insert(&self.table, key.clone(), value.clone()));
            // Buffer for read-your-writes within this transaction
            ctx.buffer_write(key, value);
            self.rows_inserted += 1;
        }

        self.done = true;

        // Return a row indicating number of rows inserted and last auto-generated ID
        Ok(Some(Row::new(vec![
            super::datum::Datum::Int(self.rows_inserted as i64),
            super::datum::Datum::Int(self.last_insert_id as i64),
        ])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        self.txn_context
            .as_mut()
            .map(|ctx| ctx.take_changes())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::planner::logical::Literal;
    use crate::server::session::UserVariables;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[tokio::test]
    async fn test_insert() {
        use crate::executor::context::TransactionContext;
        use crate::txn::ReadView;

        let columns = vec![
            ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            },
            ResolvedColumn {
                table: "users".to_string(),
                name: "name".to_string(),
                index: 1,
                data_type: DataType::Varchar(100),
                nullable: true,
            },
        ];

        let values = vec![vec![
            ResolvedExpr::Literal(Literal::Integer(1)),
            ResolvedExpr::Literal(Literal::String("alice".to_string())),
        ]];

        // Provide transaction context (required for Raft-as-WAL)
        let txn_context = TransactionContext::new(1, ReadView::default());
        let mut insert = Insert::new(
            "users".to_string(),
            columns,
            values,
            Some(txn_context),
            vec![],
            vec![0], // id is PK at index 0
            empty_vars(),
            false,
        );
        insert.open().await.unwrap();

        let result = insert.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(1)); // 1 row inserted

        insert.close().await.unwrap();

        // Verify changes were collected (data written via Raft apply, not directly)
        let changes = insert.take_changes();
        assert_eq!(changes.len(), 1);
    }
}
