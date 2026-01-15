//! Delete executor
//!
//! Deletes rows from a table that match a filter.

use std::sync::Arc;

use async_trait::async_trait;

use crate::sql::ResolvedExpr;
use crate::storage::StorageEngine;

use super::encoding::{decode_row, table_key_end, table_key_prefix};
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
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
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
        storage: Arc<dyn StorageEngine>,
    ) -> Self {
        Delete {
            table,
            filter,
            storage,
            rows_deleted: 0,
            done: false,
        }
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

        // Scan the table
        let prefix = table_key_prefix(&self.table);
        let end = table_key_end(&self.table);
        let kv_pairs = self.storage.scan(Some(&prefix), Some(&end)).await?;

        // Collect keys to delete (can't delete while iterating)
        let mut keys_to_delete = Vec::new();

        for (key, value) in kv_pairs {
            let row = decode_row(&value)?;

            // Apply filter
            if let Some(filter) = &self.filter {
                let result = eval(filter, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            keys_to_delete.push(key);
        }

        // Delete the rows
        for key in keys_to_delete {
            self.storage.delete(&key).await?;
            self.rows_deleted += 1;
        }

        self.done = true;

        Ok(Some(Row::new(vec![super::datum::Datum::Int(
            self.rows_deleted as i64,
        )])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::executor::datum::Datum;
    use crate::executor::encoding::{encode_row, encode_row_key};
    use crate::sql::{BinaryOp, Literal, ResolvedColumn};
    use crate::storage::traits::KeyValue;
    use crate::storage::StorageResult;
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
                    let after_start = start.map_or(true, |s| k.as_slice() >= s);
                    let before_end = end.map_or(true, |e| k.as_slice() < e);
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

    #[tokio::test]
    async fn test_delete_with_filter() {
        // Setup initial data
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);
        let row3 = Row::new(vec![Datum::Int(3), Datum::String("carol".to_string())]);

        let initial = vec![
            (encode_row_key("users", 1), encode_row(&row1)),
            (encode_row_key("users", 2), encode_row(&row2)),
            (encode_row_key("users", 3), encode_row(&row3)),
        ];

        let storage = Arc::new(MockStorage::new(initial));

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

        let mut delete = Delete::new("users".to_string(), Some(filter), storage.clone());
        delete.open().await.unwrap();

        let result = delete.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(2)); // 2 rows deleted

        // Verify only row 1 remains
        let data = storage.data.lock().unwrap();
        assert_eq!(data.len(), 1);

        delete.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_all() {
        let row1 = Row::new(vec![Datum::Int(1)]);
        let row2 = Row::new(vec![Datum::Int(2)]);

        let initial = vec![
            (encode_row_key("users", 1), encode_row(&row1)),
            (encode_row_key("users", 2), encode_row(&row2)),
        ];

        let storage = Arc::new(MockStorage::new(initial));

        // Delete all (no filter)
        let mut delete = Delete::new("users".to_string(), None, storage.clone());
        delete.open().await.unwrap();

        let result = delete.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(2));

        // Verify all deleted
        let data = storage.data.lock().unwrap();
        assert!(data.is_empty());

        delete.close().await.unwrap();
    }
}
