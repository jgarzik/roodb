//! ANALYZE TABLE executor
//!
//! Scans a table, counts rows and data sizes, stores statistics in
//! system.table_statistics, and returns a MySQL-format result set.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::catalog::system_tables::SYSTEM_TABLE_STATISTICS;
use crate::catalog::Catalog;
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};
use crate::txn::MvccStorage;

use super::datum::Datum;
use super::encoding::{decode_row, encode_row, encode_row_key, table_key_end, table_key_prefix};
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;
use super::Executor;

/// ANALYZE TABLE executor
pub struct AnalyzeTable {
    table: String,
    mvcc: Arc<MvccStorage>,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Arc<RaftNode>,
    done: bool,
    result_row: Option<Row>,
}

impl AnalyzeTable {
    pub fn new(
        table: String,
        mvcc: Arc<MvccStorage>,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        AnalyzeTable {
            table,
            mvcc,
            catalog,
            raft_node,
            done: false,
            result_row: None,
        }
    }
}

#[async_trait]
impl Executor for AnalyzeTable {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;

        // Verify table exists
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.table).is_none() {
                return Err(ExecutorError::TableNotFound(self.table.clone()));
            }
        }

        // Scan table data to count rows and sum sizes
        let prefix = table_key_prefix(&self.table);
        let end = table_key_end(&self.table);

        let rows = self
            .mvcc
            .scan_raw(&prefix, &end)
            .await
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

        const MVCC_HEADER_SIZE: usize = 17;

        let mut row_count: i64 = 0;
        let mut data_size: i64 = 0;
        for (key, value) in &rows {
            // Skip MVCC deleted rows
            if value.len() > MVCC_HEADER_SIZE && value[16] == 1 {
                continue;
            }
            row_count += 1;
            // Subtract MVCC header to report logical data size
            let logical_value_len = if value.len() > MVCC_HEADER_SIZE {
                value.len() - MVCC_HEADER_SIZE
            } else {
                value.len()
            };
            data_size += key.len() as i64 + logical_value_len as i64;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Upsert into system.table_statistics:
        // Scan for existing row, delete if found, then insert new
        let stats_prefix = table_key_prefix(SYSTEM_TABLE_STATISTICS);
        let stats_end = table_key_end(SYSTEM_TABLE_STATISTICS);

        let existing = self
            .mvcc
            .scan_raw(&stats_prefix, &stats_end)
            .await
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

        let mut changeset = ChangeSet::new(0);

        // Delete existing row for this table if found
        for (key, value) in existing {
            let row_data = if value.len() > 17 {
                &value[17..]
            } else {
                &value
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(Datum::String(name)) = row.get_opt(0) {
                    if name == &self.table {
                        changeset.push(RowChange::delete_with_value(
                            SYSTEM_TABLE_STATISTICS,
                            key,
                            row_data.to_vec(),
                        ));
                    }
                }
            }
        }

        // Insert new statistics row
        let stats_row = Row::new(vec![
            Datum::String(self.table.clone()),
            Datum::Int(row_count),
            Datum::Int(data_size),
            Datum::Timestamp(now),
        ]);

        let (local_id, node_id) = allocate_row_id_batch(1);
        let row_id = encode_row_id(local_id, node_id);
        let key = encode_row_key(SYSTEM_TABLE_STATISTICS, row_id);
        let value = encode_row(&stats_row);
        changeset.push(RowChange::insert(SYSTEM_TABLE_STATISTICS, key, value));

        // Propose changes via Raft
        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;

        // Build MySQL-format result row: (Table, Op, Msg_type, Msg_text)
        self.result_row = Some(Row::new(vec![
            Datum::String(self.table.clone()),
            Datum::String("analyze".to_string()),
            Datum::String("status".to_string()),
            Datum::String("OK".to_string()),
        ]));

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(self.result_row.take())
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }
}
