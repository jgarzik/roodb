//! Delete executor
//!
//! Deletes rows from a table that match a filter with MVCC support.

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;
use crate::raft::RowChange;
use crate::txn::MvccStorage;

use super::context::TransactionContext;
use super::encoding::{decode_row, encode_pk_key, table_key_end, table_key_prefix};
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Delete executor
pub struct Delete {
    /// Table name
    table: String,
    /// Optional filter predicate
    filter: Option<ResolvedExpr>,
    /// PK value for PointGet fast path (O(1) instead of full scan)
    key_value: Option<ResolvedExpr>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context (for MVCC visibility and versioning)
    txn_context: Option<TransactionContext>,
    /// Number of rows deleted
    rows_deleted: u64,
    /// Whether execution is complete
    done: bool,
}

impl Delete {
    /// Create a new delete executor
    pub fn new(
        table: String,
        filter: Option<ResolvedExpr>,
        key_value: Option<ResolvedExpr>,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        Delete {
            table,
            filter,
            key_value,
            mvcc,
            txn_context,
            rows_deleted: 0,
            done: false,
        }
    }
}

impl Delete {
    /// PointGet fast path: O(1) single-key lookup + delete
    async fn next_point_get(&mut self) -> ExecutorResult<()> {
        let key_expr = self.key_value.as_ref().unwrap();
        let key_datum = eval(key_expr, &Row::empty())?;
        let storage_key = encode_pk_key(&self.table, &[key_datum]);

        let ctx = self.txn_context.as_ref().ok_or_else(|| {
            super::error::ExecutorError::Internal("DELETE requires transaction context".to_string())
        })?;

        // Always read from MVCC storage (not write buffer) to get the correct
        // OCC version. The full-scan path also reads from MVCC, not the buffer.
        // The write buffer is for SELECT read-your-writes, not DML OCC checks.
        let result = self
            .mvcc
            .get_with_version(&storage_key, &ctx.read_view)
            .await?;

        if let Some((data, row_version)) = result {
            let row = decode_row(&data)?;
            if let Some(filter) = &self.filter {
                let result = eval(filter, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    return Ok(());
                }
            }
            let ctx = self.txn_context.as_mut().unwrap();
            ctx.add_change(RowChange::delete_with_version(
                &self.table,
                storage_key.clone(),
                row_version,
            ));
            ctx.buffer_delete(storage_key);
            self.rows_deleted += 1;
        }

        Ok(())
    }

    /// Full scan path: scan all rows in the table
    async fn next_full_scan(&mut self) -> ExecutorResult<()> {
        let prefix = table_key_prefix(&self.table);
        let end = table_key_end(&self.table);

        let kv_pairs = if let Some(ref ctx) = self.txn_context {
            self.mvcc
                .scan_with_versions(Some(&prefix), Some(&end), &ctx.read_view)
                .await?
        } else {
            self.mvcc
                .inner()
                .scan(Some(&prefix), Some(&end))
                .await?
                .into_iter()
                .map(|(k, v)| (k, v, 0u64))
                .collect()
        };

        let mut keys_to_delete = Vec::new();

        for (key, value, row_version) in kv_pairs {
            let row = decode_row(&value)?;
            if let Some(filter) = &self.filter {
                let result = eval(filter, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }
            keys_to_delete.push((key, row_version));
        }

        let ctx = self.txn_context.as_mut().ok_or_else(|| {
            super::error::ExecutorError::Internal("DELETE requires transaction context".to_string())
        })?;
        for (key, row_version) in keys_to_delete {
            ctx.add_change(RowChange::delete_with_version(
                &self.table,
                key.clone(),
                row_version,
            ));
            ctx.buffer_delete(key);
            self.rows_deleted += 1;
        }

        Ok(())
    }
}

#[async_trait]
impl Executor for Delete {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows_deleted = 0;
        self.done = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        if self.key_value.is_some() {
            self.next_point_get().await?;
        } else {
            self.next_full_scan().await?;
        }

        self.done = true;

        Ok(Some(Row::new(vec![super::datum::Datum::Int(
            self.rows_deleted as i64,
        )])))
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
    use crate::executor::datum::Datum;
    use crate::executor::encoding::{encode_pk_key, encode_row};
    use crate::planner::logical::{BinaryOp, Literal, ResolvedColumn};
    use crate::storage::traits::KeyValue;
    use crate::storage::{StorageEngine, StorageResult};
    use crate::txn::TransactionManager;
    use std::sync::Mutex;

    struct MockStorage {
        data: Mutex<Vec<KeyValue>>,
    }

    impl MockStorage {
        fn new(initial: Vec<KeyValue>) -> Self {
            MockStorage {
                data: Mutex::new(initial),
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let data = self.data.lock().unwrap();
            Ok(data.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
            let mut data = self.data.lock().unwrap();
            data.push((key.to_vec(), value.to_vec()));
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> StorageResult<()> {
            let mut data = self.data.lock().unwrap();
            data.retain(|(k, _)| k != key);
            Ok(())
        }

        async fn scan(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> StorageResult<Vec<KeyValue>> {
            let data = self.data.lock().unwrap();
            let filtered: Vec<_> = data
                .iter()
                .filter(|(k, _)| {
                    let after_start = start.is_none_or(|s| k.as_slice() >= s);
                    let before_end = end.is_none_or(|e| k.as_slice() < e);
                    after_start && before_end
                })
                .cloned()
                .collect();
            Ok(filtered)
        }

        async fn flush(&self) -> StorageResult<()> {
            Ok(())
        }

        async fn close(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    /// Encode data with MVCC header for test fixtures
    fn encode_with_mvcc_header(txn_id: u64, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(17 + data.len());
        result.extend_from_slice(&txn_id.to_le_bytes()); // DB_TRX_ID
        result.extend_from_slice(&0u64.to_le_bytes()); // DB_ROLL_PTR
        result.push(0); // deleted flag
        result.extend_from_slice(data);
        result
    }

    #[tokio::test]
    async fn test_delete_with_filter() {
        use crate::executor::context::TransactionContext;
        use crate::txn::ReadView;

        // Setup initial data with MVCC headers (txn_id=0 = committed)
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);
        let row3 = Row::new(vec![Datum::Int(3), Datum::String("carol".to_string())]);

        let initial = vec![
            (
                encode_pk_key("users", &[Datum::Int(1)]),
                encode_with_mvcc_header(0, &encode_row(&row1)),
            ),
            (
                encode_pk_key("users", &[Datum::Int(2)]),
                encode_with_mvcc_header(0, &encode_row(&row2)),
            ),
            (
                encode_pk_key("users", &[Datum::Int(3)]),
                encode_with_mvcc_header(0, &encode_row(&row3)),
            ),
        ];

        let storage = Arc::new(MockStorage::new(initial));
        let txn_manager = Arc::new(TransactionManager::new());
        let mvcc = Arc::new(MvccStorage::new(
            storage.clone() as Arc<dyn StorageEngine>,
            txn_manager,
        ));

        // Delete where id > 1
        let filter = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            })),
            op: BinaryOp::Gt,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            result_type: DataType::Boolean,
        };

        // Provide transaction context (required for Raft-as-WAL)
        let txn_context = TransactionContext::new(1, ReadView::default());
        let mut delete = Delete::new(
            "users".to_string(),
            Some(filter),
            None,
            mvcc,
            Some(txn_context),
        );
        delete.open().await.unwrap();

        let result = delete.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(2)); // 2 rows deleted

        delete.close().await.unwrap();

        // Verify changes were collected (data written via Raft apply, not directly)
        let changes = delete.take_changes();
        assert_eq!(changes.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_all() {
        use crate::executor::context::TransactionContext;
        use crate::txn::ReadView;

        // Setup initial data with MVCC headers (txn_id=0 = committed)
        let row1 = Row::new(vec![Datum::Int(1)]);
        let row2 = Row::new(vec![Datum::Int(2)]);

        let initial = vec![
            (
                encode_pk_key("users", &[Datum::Int(1)]),
                encode_with_mvcc_header(0, &encode_row(&row1)),
            ),
            (
                encode_pk_key("users", &[Datum::Int(2)]),
                encode_with_mvcc_header(0, &encode_row(&row2)),
            ),
        ];

        let storage = Arc::new(MockStorage::new(initial));
        let txn_manager = Arc::new(TransactionManager::new());
        let mvcc = Arc::new(MvccStorage::new(
            storage.clone() as Arc<dyn StorageEngine>,
            txn_manager,
        ));

        // Provide transaction context (required for Raft-as-WAL)
        let txn_context = TransactionContext::new(1, ReadView::default());
        let mut delete = Delete::new("users".to_string(), None, None, mvcc, Some(txn_context));
        delete.open().await.unwrap();

        let result = delete.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(2));

        delete.close().await.unwrap();

        // Verify changes were collected (data written via Raft apply, not directly)
        let changes = delete.take_changes();
        assert_eq!(changes.len(), 2);
    }
}
