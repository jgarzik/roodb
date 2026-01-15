//! Update executor
//!
//! Updates rows in a table that match a filter.

use std::sync::Arc;

use async_trait::async_trait;

use crate::sql::{ResolvedColumn, ResolvedExpr};
use crate::storage::StorageEngine;

use super::encoding::{decode_row, encode_row, table_key_end, table_key_prefix};
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Update executor
pub struct Update {
    /// Table name
    table: String,
    /// Assignments: (column, new_value)
    assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
    /// Optional filter predicate
    filter: Option<ResolvedExpr>,
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
    /// Number of rows updated
    rows_updated: u64,
    /// Whether execution is complete
    done: bool,
}

impl Update {
    /// Create a new update executor
    pub fn new(
        table: String,
        assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
        filter: Option<ResolvedExpr>,
        storage: Arc<dyn StorageEngine>,
    ) -> Self {
        Update {
            table,
            assignments,
            filter,
            storage,
            rows_updated: 0,
            done: false,
        }
    }
}

#[async_trait]
impl Executor for Update {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows_updated = 0;
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

        for (key, value) in kv_pairs {
            let mut row = decode_row(&value)?;

            // Apply filter
            if let Some(filter) = &self.filter {
                let result = eval(filter, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            // Apply updates
            for (col, expr) in &self.assignments {
                let new_value = eval(expr, &row)?;
                row.set(col.index, new_value)?;
            }

            // Write back
            let new_value = encode_row(&row);
            self.storage.put(&key, &new_value).await?;
            self.rows_updated += 1;
        }

        self.done = true;

        Ok(Some(Row::new(vec![super::datum::Datum::Int(
            self.rows_updated as i64,
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
    use crate::executor::encoding::encode_row_key;
    use crate::sql::{BinaryOp, Literal};
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
            // Update existing or append
            if let Some(entry) = data.iter_mut().find(|(k, _)| k == key) {
                entry.1 = value.to_vec();
            } else {
                data.push((key.to_vec(), value.to_vec()));
            }
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
    async fn test_update() {
        // Setup initial data
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);

        let initial = vec![
            (encode_row_key("users", 1), encode_row(&row1)),
            (encode_row_key("users", 2), encode_row(&row2)),
        ];

        let storage = Arc::new(MockStorage::new(initial));

        // Update where id = 1
        let filter = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            })),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            result_type: DataType::Boolean,
        };

        let assignments = vec![(
            ResolvedColumn {
                table: "users".to_string(),
                name: "name".to_string(),
                index: 1,
                data_type: DataType::Varchar(100),
                nullable: true,
            },
            ResolvedExpr::Literal(Literal::String("alice_updated".to_string())),
        )];

        let mut update = Update::new("users".to_string(), assignments, Some(filter), storage.clone());
        update.open().await.unwrap();

        let result = update.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(1)); // 1 row updated

        // Verify the update
        let key = encode_row_key("users", 1);
        let value = storage.get(&key).await.unwrap().unwrap();
        let updated_row = decode_row(&value).unwrap();
        assert_eq!(updated_row.get(1).unwrap().as_str(), Some("alice_updated"));

        update.close().await.unwrap();
    }
}
