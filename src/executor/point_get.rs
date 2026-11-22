//! PointGet executor
//!
//! O(log n) single-row lookup by primary key using storage get() instead of scan().

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;
use crate::txn::MvccStorage;

use crate::server::session::UserVariables;

use super::context::TransactionContext;
use super::encoding::{decode_row, encode_pk_key};
use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Point get executor - single-row lookup by primary key
pub struct PointGet {
    /// Table name
    table: crate::catalog::TableName,
    /// The table's ID: what its keys begin with
    table_id: crate::executor::encoding::TableId,
    /// Primary key value expression
    key_expr: ResolvedExpr,
    /// The key column's type, if known.
    key_type: Option<crate::catalog::DataType>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context
    txn_context: Option<TransactionContext>,
    /// The fetched row (if any)
    result: Option<Row>,
    /// Whether we've already returned the result
    returned: bool,
    /// User variables
    user_variables: UserVariables,
}

impl PointGet {
    pub fn new(
        table: crate::catalog::TableName,
        table_id: crate::executor::encoding::TableId,
        key_expr: ResolvedExpr,
        key_type: Option<crate::catalog::DataType>,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
        user_variables: UserVariables,
    ) -> Self {
        PointGet {
            table,
            table_id,
            key_expr,
            key_type,
            mvcc,
            txn_context,
            result: None,
            returned: false,
            user_variables,
        }
    }
}

#[async_trait]
impl Executor for PointGet {
    async fn open(&mut self) -> ExecutorResult<()> {
        // Evaluate the key expression to get the PK value
        // Use an empty row since the key expression is a literal
        let empty_row = Row::empty();
        let key_datum = evaluate(&self.key_expr, &empty_row, &self.user_variables)?;
        // In the key column's type. A value the column cannot hold (2.5 in
        // an integer column) is the key of no row.
        let Some(key_datum) = super::key::for_point(key_datum, self.key_type.as_ref()) else {
            self.result = None;
            return Ok(());
        };

        // Encode the storage key using PK-based encoding
        let storage_key = encode_pk_key(self.table_id, &[key_datum]);

        // Do the O(log n) lookup
        let value = if let Some(ref ctx) = self.txn_context {
            if ctx.read_locks(&self.table) {
                // SELECT ... FOR UPDATE / FOR SHARE: lock the row, then
                // read it as it stands. If there is no row, lock the gap it
                // would be in (REPEATABLE READ), so that it stays missing.
                use super::context::LockedRow;
                match ctx.locked_read(&self.mvcc, &storage_key).await? {
                    LockedRow::Row(data) => {
                        self.result = Some(decode_row(&data)?);
                    }
                    LockedRow::Skipped => {}
                    LockedRow::Missing { new_lock } => {
                        // The gap is locked before the key is given back,
                        // so that nothing can be inserted in between.
                        if ctx.locks_gaps() && !ctx.is_buffered_deleted(&storage_key) {
                            let (low, high) = self
                                .mvcc
                                .gap_around(
                                    &storage_key,
                                    &super::encoding::table_key_prefix(self.table_id),
                                    &super::encoding::table_key_end(self.table_id),
                                )
                                .await?;
                            ctx.lock_gap(&low, &high);
                        }
                        if new_lock {
                            ctx.unlock_record(&storage_key);
                        }
                    }
                }
                return Ok(());
            }
            self.mvcc.get(&storage_key, &ctx.read_view).await?
        } else {
            self.mvcc.inner().get(&storage_key).await?
        };

        if let Some(data) = value {
            let row = decode_row(&data)?;

            // Check buffered writes for this key
            if let Some(ref ctx) = self.txn_context {
                if let Some(buffered) = ctx.get_buffered(&storage_key) {
                    if let Some(buf_data) = buffered {
                        self.result = Some(decode_row(buf_data)?);
                    }
                    // else: buffered as delete, no result
                    return Ok(());
                }
            }

            self.result = Some(row);
        } else {
            // Not in storage, check buffer for uncommitted insert
            if let Some(ref ctx) = self.txn_context {
                if let Some(Some(buf_data)) = ctx.get_buffered(&storage_key) {
                    self.result = Some(decode_row(buf_data)?);
                }
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.returned {
            return Ok(None);
        }
        self.returned = true;
        Ok(self.result.take())
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.result = None;
        self.returned = false;
        Ok(())
    }
}
