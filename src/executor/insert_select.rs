//! INSERT ... SELECT executor
//!
//! Reads rows from a source query executor and inserts them into a table.

use async_trait::async_trait;

use crate::planner::logical::ResolvedColumn;
use crate::raft::RowChange;
use crate::sql::resolver::parse_default_value;
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

use super::context::TransactionContext;
use super::datum::Datum;
use super::encoding::{encode_pk_key, encode_row, encode_row_key};
use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

/// INSERT ... SELECT executor
pub struct InsertSelect {
    /// Table name
    table: String,
    /// Target columns (always ALL table columns)
    columns: Vec<ResolvedColumn>,
    /// Source query executor
    source: Box<dyn Executor>,
    /// Transaction context (for MVCC versioning)
    txn_context: Option<TransactionContext>,
    /// Number of rows inserted
    rows_inserted: u64,
    /// Whether execution is complete
    done: bool,
    /// Column indices that are auto_increment
    auto_increment_indices: Vec<usize>,
    /// Last auto-generated ID (for LAST_INSERT_ID)
    last_insert_id: u64,
    /// Primary key column indices (for PK-based storage keys)
    pk_column_indices: Vec<usize>,
    /// Maps source column index → target column index for partial column lists.
    /// None when all columns are targeted (1:1 positional mapping).
    column_map: Option<Vec<usize>>,
    /// Precomputed: which target column indices are covered by column_map.
    /// Used to apply defaults for unmapped columns without per-row allocation.
    mapped_targets: std::collections::HashSet<usize>,
    /// IGNORE modifier — suppress errors, skip bad rows
    ignore: bool,
}

impl InsertSelect {
    /// Create a new INSERT ... SELECT executor
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: String,
        columns: Vec<ResolvedColumn>,
        source: Box<dyn Executor>,
        txn_context: Option<TransactionContext>,
        auto_increment_indices: Vec<usize>,
        pk_column_indices: Vec<usize>,
        column_map: Option<Vec<usize>>,
        ignore: bool,
    ) -> Self {
        let mapped_targets: std::collections::HashSet<usize> = column_map
            .as_ref()
            .map(|m| m.iter().copied().collect())
            .unwrap_or_default();
        InsertSelect {
            table,
            columns,
            source,
            txn_context,
            rows_inserted: 0,
            done: false,
            auto_increment_indices,
            last_insert_id: 0,
            pk_column_indices,
            column_map,
            mapped_targets,
            ignore,
        }
    }
}

#[async_trait]
impl Executor for InsertSelect {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows_inserted = 0;
        self.done = false;
        self.source.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Stream rows from source — insert each row as it arrives to avoid
        // buffering the entire result set in memory.
        while let Some(source_row) = self.source.next().await? {
            // Build a full-width row with NULLs for all table columns
            let mut datums: Vec<Datum> = vec![Datum::Null; self.columns.len()];

            if let Some(ref col_map) = self.column_map {
                // Partial column list: map source values to target positions
                for (src_idx, &target_idx) in col_map.iter().enumerate() {
                    let d = if src_idx < source_row.len() {
                        source_row.get(src_idx).cloned().unwrap_or(Datum::Null)
                    } else {
                        Datum::Null
                    };
                    if target_idx < self.columns.len() {
                        datums[target_idx] = super::eval::coerce_to_column_type(
                            d,
                            &self.columns[target_idx].data_type,
                        )?;
                    }
                }
            } else {
                // All columns targeted: 1:1 positional mapping
                for (col_idx, col) in self.columns.iter().enumerate() {
                    let d = if col_idx < source_row.len() {
                        source_row.get(col_idx).cloned().unwrap_or(Datum::Null)
                    } else {
                        Datum::Null
                    };
                    datums[col_idx] = super::eval::coerce_to_column_type(d, &col.data_type)?;
                }
            }

            // Apply column DEFAULT values for unmapped columns that are still NULL.
            // When column_map is present, only mapped columns get source values;
            // the rest should get their DEFAULT value if one is defined.
            if self.column_map.is_some() {
                for (col_idx, datum) in datums.iter_mut().enumerate() {
                    if !self.mapped_targets.contains(&col_idx)
                        && datum.is_null()
                        && col_idx < self.columns.len()
                    {
                        if let Some(ref default_expr) = self.columns[col_idx].default_value {
                            let lit = parse_default_value(default_expr);
                            *datum = Datum::from_literal(&lit);
                        }
                    }
                }
            }

            // Allocate row ID per-row
            let (local_id, node_id) = allocate_row_id_batch(1);
            let row_id = encode_row_id(local_id, node_id);

            // Replace NULL values in auto_increment columns with generated ID
            let auto_id = row_id & 0x0000_FFFF_FFFF_FFFF;
            for &idx in &self.auto_increment_indices {
                if idx < datums.len() && datums[idx].is_null() {
                    datums[idx] = Datum::Int(auto_id as i64);
                    self.last_insert_id = auto_id;
                }
            }

            // Validate NOT NULL constraints
            for (col_idx, datum) in datums.iter_mut().enumerate() {
                if col_idx < self.columns.len()
                    && !self.columns[col_idx].nullable
                    && datum.is_null()
                {
                    if self.ignore {
                        // Prefer column DEFAULT, fall back to type default
                        *datum = if let Some(ref default_expr) = self.columns[col_idx].default_value
                        {
                            Datum::from_literal(&parse_default_value(default_expr))
                        } else {
                            Datum::default_for_type(&self.columns[col_idx].data_type)
                        };
                    } else {
                        return Err(super::error::ExecutorError::NullValue(format!(
                            "Column '{}' cannot be null",
                            self.columns[col_idx].name
                        )));
                    }
                }
            }

            let row = Row::new(datums);

            // Build storage key
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

            let ctx = self.txn_context.as_mut().ok_or_else(|| {
                super::error::ExecutorError::Internal(
                    "INSERT requires transaction context".to_string(),
                )
            })?;

            // Intra-batch duplicate check: if this PK key was already inserted
            // in this same INSERT ... SELECT statement, it's a duplicate.
            if !self.pk_column_indices.is_empty() && ctx.has_buffered_key(&key) {
                if self.ignore {
                    continue;
                }
                return Err(super::error::ExecutorError::DuplicateKey(format!(
                    "Duplicate entry for key 'PRIMARY' in table '{}'",
                    self.table
                )));
            }

            ctx.add_change(RowChange::insert(&self.table, key.clone(), value.clone()));
            ctx.buffer_write(key, value);
            self.rows_inserted += 1;
        }

        self.done = true;

        Ok(Some(Row::new(vec![
            Datum::Int(self.rows_inserted as i64),
            Datum::Int(self.last_insert_id as i64),
        ])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.source.close().await
    }

    fn take_changes(&mut self) -> Vec<crate::raft::RowChange> {
        self.txn_context
            .as_mut()
            .map(|ctx| ctx.take_changes())
            .unwrap_or_default()
    }

    fn is_ignore_duplicates(&self) -> bool {
        self.ignore
    }
}
