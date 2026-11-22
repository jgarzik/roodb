//! ANALYZE TABLE executor
//!
//! Scans a table, counts rows and data sizes, stores statistics in
//! system.table_statistics, and returns a MySQL-format result set.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::catalog::system_tables::SYSTEM_TABLE_STATISTICS;
use crate::catalog::Catalog;
use crate::raft::{ChangeSet, RaftNode};
use crate::txn::MvccStorage;

use super::datum::Datum;
use super::encoding::{table_key_end, table_key_prefix};
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

        use crate::raft::{is_mvcc_tombstone, MVCC_HEADER_SIZE};

        let mut row_count: i64 = 0;
        let mut data_size: i64 = 0;
        for (key, value) in &rows {
            // Skip MVCC deleted rows
            if is_mvcc_tombstone(value) {
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

        // The table's statistics row, keyed by the table's name: this one
        // takes the place of the last.
        let stats_row = Row::new(vec![
            Datum::String(self.table.clone()),
            Datum::Int(row_count),
            Datum::Int(data_size),
            Datum::Timestamp(now),
        ]);
        let mut changeset = ChangeSet::new(0);
        changeset.push(crate::catalog::store::put_row(
            SYSTEM_TABLE_STATISTICS,
            &stats_row,
        ));

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
