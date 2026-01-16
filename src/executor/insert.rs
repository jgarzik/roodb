//! Insert executor
//!
//! Inserts rows into a table with MVCC support.

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::{ResolvedColumn, ResolvedExpr};
use crate::raft::RowChange;
use crate::storage::next_row_id;
use crate::txn::MvccStorage;

use super::context::TransactionContext;
use super::encoding::{encode_row, encode_row_key};
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Insert executor
pub struct Insert {
    /// Table name
    table: String,
    /// Target columns
    _columns: Vec<ResolvedColumn>,
    /// Values to insert (each inner vec is one row)
    values: Vec<Vec<ResolvedExpr>>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context (for MVCC versioning)
    txn_context: Option<TransactionContext>,
    /// Number of rows inserted
    rows_inserted: u64,
    /// Whether execution is complete
    done: bool,
}

impl Insert {
    /// Create a new insert executor
    pub fn new(
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        Insert {
            table,
            _columns: columns,
            values,
            mvcc,
            txn_context,
            rows_inserted: 0,
            done: false,
        }
    }
}

#[async_trait]
impl Executor for Insert {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows_inserted = 0;
        self.done = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Insert all rows
        let empty_row = Row::empty();

        for value_row in &self.values {
            // Evaluate expressions to get datum values
            let mut datums = Vec::with_capacity(value_row.len());
            for expr in value_row {
                let datum = eval(expr, &empty_row)?;
                datums.push(datum);
            }

            let row = Row::new(datums);
            let row_id = next_row_id();

            let key = encode_row_key(&self.table, row_id);
            let value = encode_row(&row);

            // Collect the change for Raft replication.
            // Data is written to storage in apply() after Raft commit.
            // This ensures Raft-as-WAL: no writes until consensus.
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

        // Return a row indicating number of rows inserted
        Ok(Some(Row::new(vec![super::datum::Datum::Int(
            self.rows_inserted as i64,
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
    use crate::planner::logical::Literal;
    use crate::storage::traits::KeyValue;
    use crate::storage::{StorageEngine, StorageResult};
    use crate::txn::TransactionManager;
    use std::sync::Mutex;

    struct MockStorage {
        data: Mutex<Vec<KeyValue>>,
    }

    impl MockStorage {
        fn new() -> Self {
            MockStorage {
                data: Mutex::new(Vec::new()),
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

        async fn delete(&self, _key: &[u8]) -> StorageResult<()> {
            Ok(())
        }

        async fn scan(
            &self,
            _start: Option<&[u8]>,
            _end: Option<&[u8]>,
        ) -> StorageResult<Vec<KeyValue>> {
            Ok(Vec::new())
        }

        async fn flush(&self) -> StorageResult<()> {
            Ok(())
        }

        async fn close(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_insert() {
        use crate::executor::context::TransactionContext;
        use crate::txn::ReadView;

        let storage = Arc::new(MockStorage::new());
        let txn_manager = Arc::new(TransactionManager::new());
        let mvcc = Arc::new(MvccStorage::new(
            storage.clone() as Arc<dyn StorageEngine>,
            txn_manager,
        ));

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
            mvcc,
            Some(txn_context),
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
