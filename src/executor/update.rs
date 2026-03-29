//! Update executor
//!
//! Updates rows in a table that match a filter with MVCC support.

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::{ResolvedColumn, ResolvedExpr};
use crate::raft::RowChange;
use crate::txn::MvccStorage;

use crate::server::session::UserVariables;

use super::context::TransactionContext;
use super::datum::Datum;
use super::encoding::{decode_row, encode_pk_key, encode_row, table_key_end, table_key_prefix};
use super::error::ExecutorResult;
use super::eval::evaluate;
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
    /// PK value for PointGet fast path (O(1) instead of full scan)
    key_value: Option<ResolvedExpr>,
    /// ORDER BY expressions for UPDATE ... ORDER BY ... LIMIT
    order_by: Vec<(ResolvedExpr, bool)>,
    /// LIMIT for UPDATE ... LIMIT n
    limit: Option<usize>,
    /// Primary key column indices (positions in the full table row)
    pk_column_indices: Vec<usize>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context (for MVCC visibility and versioning)
    txn_context: Option<TransactionContext>,
    /// Number of rows updated
    rows_updated: u64,
    /// Whether execution is complete
    done: bool,
    /// User variables
    user_variables: UserVariables,
}

/// Parameters for creating an Update executor.
pub struct UpdateParams {
    pub table: String,
    pub assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
    pub filter: Option<ResolvedExpr>,
    pub key_value: Option<ResolvedExpr>,
    pub order_by: Vec<(ResolvedExpr, bool)>,
    pub limit: Option<usize>,
    pub pk_column_indices: Vec<usize>,
    pub mvcc: Arc<MvccStorage>,
    pub txn_context: Option<TransactionContext>,
    pub user_variables: UserVariables,
}

impl Update {
    /// Create a new update executor
    pub fn new(p: UpdateParams) -> Self {
        Update {
            table: p.table,
            assignments: p.assignments,
            filter: p.filter,
            key_value: p.key_value,
            order_by: p.order_by,
            limit: p.limit,
            pk_column_indices: p.pk_column_indices,
            mvcc: p.mvcc,
            txn_context: p.txn_context,
            rows_updated: 0,
            done: false,
            user_variables: p.user_variables,
        }
    }
}

impl Update {
    /// Emit the correct Raft changes for an updated row.
    ///
    /// If a PK column was modified, the storage key changes:
    /// we must delete the old key and insert at the new key.
    /// Otherwise, emit a normal update at the same key.
    fn emit_row_change(
        &mut self,
        old_key: Vec<u8>,
        row: &Row,
        row_version: u64,
    ) -> ExecutorResult<()> {
        let new_value = encode_row(row);

        // Compute new PK-based key from updated row values
        let new_key = if !self.pk_column_indices.is_empty() {
            let pk_values: Vec<_> = self
                .pk_column_indices
                .iter()
                .map(|&idx| row.get(idx).unwrap().clone())
                .collect();
            encode_pk_key(&self.table, &pk_values)
        } else {
            old_key.clone()
        };

        let ctx = self.txn_context.as_mut().ok_or_else(|| {
            super::error::ExecutorError::Internal("UPDATE requires transaction context".to_string())
        })?;

        if new_key != old_key {
            // PK changed: delete old key, insert at new key
            ctx.add_change(RowChange::delete_with_version(
                &self.table,
                old_key.clone(),
                row_version,
            ));
            ctx.buffer_delete(old_key);
            ctx.add_change(RowChange::insert(
                &self.table,
                new_key.clone(),
                new_value.clone(),
            ));
            ctx.buffer_write(new_key, new_value);
        } else {
            // Same key: normal update
            ctx.add_change(RowChange::update_with_version(
                &self.table,
                old_key.clone(),
                new_value.clone(),
                row_version,
            ));
            ctx.buffer_write(old_key, new_value);
        }

        self.rows_updated += 1;
        Ok(())
    }

    /// PointGet fast path: O(1) single-key lookup + update
    async fn next_point_get(&mut self) -> ExecutorResult<()> {
        let key_expr = self.key_value.as_ref().unwrap();
        let key_datum = evaluate(key_expr, &Row::empty(), &self.user_variables)?;
        let storage_key = encode_pk_key(&self.table, &[key_datum]);

        let ctx = self.txn_context.as_ref().ok_or_else(|| {
            super::error::ExecutorError::Internal("UPDATE requires transaction context".to_string())
        })?;

        // Always read from MVCC storage (not write buffer) to get the correct
        // OCC version. The full-scan path also reads from MVCC, not the buffer.
        // The write buffer is for SELECT read-your-writes, not DML OCC checks.
        let result = self
            .mvcc
            .get_with_version(&storage_key, &ctx.read_view)
            .await?;

        if let Some((data, row_version)) = result {
            let mut row = decode_row(&data)?;
            if let Some(filter) = &self.filter {
                let result = evaluate(filter, &row, &self.user_variables)?;
                if !result.as_bool().unwrap_or(false) {
                    return Ok(());
                }
            }
            for (col, expr) in &self.assignments {
                let mut new_value = evaluate(expr, &row, &self.user_variables)?;
                if new_value.is_null() && !col.nullable {
                    new_value = super::datum::Datum::default_for_type(&col.data_type);
                }
                row.set(col.index, new_value)?;
            }
            self.emit_row_change(storage_key, &row, row_version)?;
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

        // Collect matching rows (with keys for later update)
        let mut matching: Vec<(Vec<u8>, Vec<u8>, u64, Row)> = Vec::new();
        for (key, value, row_version) in kv_pairs {
            let row = decode_row(&value)?;
            if let Some(filter) = &self.filter {
                let result = evaluate(filter, &row, &self.user_variables)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }
            matching.push((key, value, row_version, row));
        }

        // Apply ORDER BY if present
        if !self.order_by.is_empty() {
            let order_by = &self.order_by;
            let user_vars = &self.user_variables;
            matching.sort_by(|a, b| {
                for (expr, asc) in order_by {
                    let va = evaluate(expr, &a.3, user_vars).unwrap_or(Datum::Null);
                    let vb = evaluate(expr, &b.3, user_vars).unwrap_or(Datum::Null);
                    let ord = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                    let ord = if *asc { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT if present
        if let Some(limit) = self.limit {
            matching.truncate(limit);
        }

        // Perform the updates
        for (key, _value, row_version, mut row) in matching {
            for (col, expr) in &self.assignments {
                let mut new_value = evaluate(expr, &row, &self.user_variables)?;
                if new_value.is_null() && !col.nullable {
                    new_value = super::datum::Datum::default_for_type(&col.data_type);
                }
                row.set(col.index, new_value)?;
            }
            self.emit_row_change(key, &row, row_version)?;
        }

        Ok(())
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

        // RAII guard: strict DML context cleared on drop (even on early return/error)
        let _strict_guard = super::eval::StrictDmlGuard::new(&self.user_variables);

        if self.key_value.is_some() {
            self.next_point_get().await?;
        } else {
            self.next_full_scan().await?;
        }

        self.done = true;

        Ok(Some(Row::new(vec![super::datum::Datum::Int(
            self.rows_updated as i64,
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
    use crate::executor::encoding::encode_pk_key;
    use crate::planner::logical::{BinaryOp, Literal};
    use crate::server::session::UserVariables;
    use crate::storage::traits::KeyValue;
    use crate::storage::{StorageEngine, StorageResult};
    use crate::txn::TransactionManager;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

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
    async fn test_update() {
        use crate::executor::context::TransactionContext;
        use crate::txn::ReadView;

        // Setup initial data with MVCC headers (txn_id=0 = committed)
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);

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

        // Update where id = 1
        let filter = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
                default_value: None,
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
                default_value: None,
            },
            ResolvedExpr::Literal(Literal::String("alice_updated".to_string())),
        )];

        // Provide transaction context (required for Raft-as-WAL)
        let txn_context = TransactionContext::new(1, ReadView::default());
        let mut update = Update::new(UpdateParams {
            table: "users".to_string(),
            assignments,
            filter: Some(filter),
            key_value: None,
            order_by: vec![],
            limit: None,
            pk_column_indices: vec![0], // id is PK at index 0
            mvcc,
            txn_context: Some(txn_context),
            user_variables: empty_vars(),
        });
        update.open().await.unwrap();

        let result = update.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(1)); // 1 row updated

        update.close().await.unwrap();

        // Verify changes were collected (data written via Raft apply, not directly)
        let changes = update.take_changes();
        assert_eq!(changes.len(), 1);
        // Non-PK update: single Update change at same key
        assert_eq!(changes[0].op, crate::raft::ChangeOp::Update);
    }

    #[tokio::test]
    async fn test_update_pk_column_recomputes_key() {
        use crate::executor::context::TransactionContext;
        use crate::raft::ChangeOp;
        use crate::txn::ReadView;

        // Setup: row with id=1 (PK), name="alice"
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);

        let initial = vec![(
            encode_pk_key("users", &[Datum::Int(1)]),
            encode_with_mvcc_header(0, &encode_row(&row1)),
        )];

        let storage = Arc::new(MockStorage::new(initial));
        let txn_manager = Arc::new(TransactionManager::new());
        let mvcc = Arc::new(MvccStorage::new(
            storage.clone() as Arc<dyn StorageEngine>,
            txn_manager,
        ));

        // UPDATE users SET id = 10 WHERE id = 1  (PK change!)
        let filter = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
                default_value: None,
            })),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            result_type: DataType::Boolean,
        };

        let assignments = vec![(
            ResolvedColumn {
                table: "users".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
                default_value: None,
            },
            ResolvedExpr::Literal(Literal::Integer(10)),
        )];

        let txn_context = TransactionContext::new(1, ReadView::default());
        let mut update = Update::new(UpdateParams {
            table: "users".to_string(),
            assignments,
            filter: Some(filter),
            key_value: None,
            order_by: vec![],
            limit: None,
            pk_column_indices: vec![0], // id is PK at index 0
            mvcc,
            txn_context: Some(txn_context),
            user_variables: empty_vars(),
        });
        update.open().await.unwrap();

        let result = update.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(1)); // 1 row updated

        update.close().await.unwrap();

        let changes = update.take_changes();
        // PK changed: should produce DELETE(old_key) + INSERT(new_key) = 2 changes
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].op, ChangeOp::Delete);
        assert_eq!(changes[0].key, encode_pk_key("users", &[Datum::Int(1)]));
        assert_eq!(changes[1].op, ChangeOp::Insert);
        assert_eq!(changes[1].key, encode_pk_key("users", &[Datum::Int(10)]));
    }
}
