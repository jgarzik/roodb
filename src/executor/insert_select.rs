//! INSERT ... SELECT executor
//!
//! Reads rows from a source query executor and inserts them into a table.

use async_trait::async_trait;

use crate::planner::logical::ResolvedColumn;
use crate::raft::RowChange;
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
    /// Target columns
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
        ignore: bool,
    ) -> Self {
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

        // Read all rows from source and insert them
        // First, collect all rows to know the count for batch allocation
        let mut source_rows = Vec::new();
        while let Some(row) = self.source.next().await? {
            source_rows.push(row);
        }

        // Pre-allocate row IDs
        let row_count = source_rows.len() as u64;
        let (start_local, node_id) = if row_count > 0 {
            allocate_row_id_batch(row_count)
        } else {
            (0, 0)
        };
        let mut next_local_id = start_local;

        for source_row in &source_rows {
            // Coerce source row values to match target column types
            let mut datums: Vec<Datum> = Vec::with_capacity(self.columns.len());
            for (col_idx, col) in self.columns.iter().enumerate() {
                let datum = if col_idx < source_row.len() {
                    let d = source_row.get(col_idx).cloned().unwrap_or(Datum::Null);
                    super::eval::coerce_to_column_type(d, &col.data_type)
                } else {
                    Datum::Null
                };
                datums.push(datum);
            }

            // Get next row ID
            let row_id = encode_row_id(next_local_id, node_id);
            next_local_id += 1;

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
                    if self.ignore || source_rows.len() > 1 {
                        *datum = Datum::default_for_type(&self.columns[col_idx].data_type);
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
}
