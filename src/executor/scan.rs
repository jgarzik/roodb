//! TableScan executor
//!
//! Scans all rows from a table using MVCC-aware storage.

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;
use crate::txn::MvccStorage;

use crate::server::session::UserVariables;

use super::context::TransactionContext;
use super::encoding::{decode_row, table_key_end, table_key_prefix};
use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Table scan executor (streaming: decodes rows lazily in next())
pub struct TableScan {
    /// Table name
    table: String,
    /// Optional filter predicate (pushed down)
    filter: Option<ResolvedExpr>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context (for MVCC visibility)
    txn_context: Option<TransactionContext>,
    /// Raw key-value pairs from storage (decoded lazily in next())
    raw_pairs: Vec<Vec<u8>>,
    /// Current position in raw_pairs
    position: usize,
    /// User variables
    user_variables: UserVariables,
}

impl TableScan {
    /// Create a new table scan
    pub fn new(
        table: String,
        filter: Option<ResolvedExpr>,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
        user_variables: UserVariables,
    ) -> Self {
        TableScan {
            table,
            filter,
            mvcc,
            txn_context,
            raw_pairs: Vec::new(),
            position: 0,
            user_variables,
        }
    }
}

#[async_trait]
impl Executor for TableScan {
    async fn open(&mut self) -> ExecutorResult<()> {
        // Scan the table — store raw values, decode lazily in next()
        let prefix = table_key_prefix(&self.table);
        let end = table_key_end(&self.table);

        // Use MVCC scan with visibility filtering if we have a transaction context,
        // otherwise fall back to raw storage scan (for DDL or legacy tests)
        let kv_pairs = if let Some(ref ctx) = self.txn_context {
            self.mvcc
                .scan(Some(&prefix), Some(&end), &ctx.read_view)
                .await?
        } else {
            self.mvcc.inner().scan(Some(&prefix), Some(&end)).await?
        };

        // Collect keys that are buffered (for read-your-writes merge)
        let buffered_entries = if let Some(ref ctx) = self.txn_context {
            ctx.get_buffered_for_prefix(&prefix)
        } else {
            Vec::new()
        };

        // Build a set of buffered keys for quick lookup
        use std::collections::HashSet;
        let buffered_keys: HashSet<&[u8]> =
            buffered_entries.iter().map(|(k, _)| k.as_slice()).collect();

        // Collect raw values (not decoded yet) — skip keys overridden by buffer
        self.raw_pairs.clear();
        for (key, value) in kv_pairs {
            if buffered_keys.contains(key.as_slice()) {
                continue;
            }
            self.raw_pairs.push(value);
        }

        // Append buffered writes (uncommitted inserts/updates, skip deletes)
        for (_key, value_opt) in buffered_entries {
            if let Some(value) = value_opt {
                self.raw_pairs.push(value.to_vec());
            }
        }

        self.position = 0;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        // Decode and filter one row at a time (streaming)
        while self.position < self.raw_pairs.len() {
            let row = decode_row(&self.raw_pairs[self.position])?;
            self.position += 1;

            if let Some(filter) = &self.filter {
                let result = evaluate(filter, &row, &self.user_variables)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            return Ok(Some(row));
        }
        Ok(None)
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.raw_pairs.clear();
        self.position = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::datum::Datum;
    use crate::executor::encoding::{encode_pk_key, encode_row};
    use crate::server::session::UserVariables;
    use crate::storage::traits::KeyValue;
    use crate::storage::{StorageEngine, StorageResult};
    use crate::txn::TransactionManager;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

    struct MockStorage {
        data: Vec<KeyValue>,
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn get(&self, _key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn put(&self, _key: &[u8], _value: &[u8]) -> StorageResult<()> {
            Ok(())
        }

        async fn delete(&self, _key: &[u8]) -> StorageResult<()> {
            Ok(())
        }

        async fn scan(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> StorageResult<Vec<KeyValue>> {
            let filtered: Vec<_> = self
                .data
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

    fn make_test_mvcc() -> Arc<MvccStorage> {
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);

        let data = vec![
            (encode_pk_key("users", &[Datum::Int(1)]), encode_row(&row1)),
            (encode_pk_key("users", &[Datum::Int(2)]), encode_row(&row2)),
        ];

        let storage = Arc::new(MockStorage { data }) as Arc<dyn StorageEngine>;
        let txn_manager = Arc::new(TransactionManager::new());
        Arc::new(MvccStorage::new(storage, txn_manager))
    }

    #[tokio::test]
    async fn test_table_scan_basic() {
        let mvcc = make_test_mvcc();
        // No txn_context = use inner storage directly (legacy mode)
        let mut scan = TableScan::new("users".to_string(), None, mvcc, None, empty_vars());

        scan.open().await.unwrap();

        let row1 = scan.next().await.unwrap();
        assert!(row1.is_some());
        assert_eq!(row1.unwrap().get(0).unwrap().as_int(), Some(1));

        let row2 = scan.next().await.unwrap();
        assert!(row2.is_some());
        assert_eq!(row2.unwrap().get(0).unwrap().as_int(), Some(2));

        let row3 = scan.next().await.unwrap();
        assert!(row3.is_none());

        scan.close().await.unwrap();
    }
}
