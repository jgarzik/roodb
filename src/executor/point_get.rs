//! PointGet executor
//!
//! O(log n) single-row lookup by primary key using storage get() instead of scan().

use std::sync::Arc;

use async_trait::async_trait;

use crate::executor::datum::Datum;
use crate::planner::logical::ResolvedExpr;
use crate::txn::MvccStorage;

use super::context::TransactionContext;
use super::encoding::{decode_row, encode_row_key};
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Point get executor - single-row lookup by primary key
pub struct PointGet {
    /// Table name
    table: String,
    /// Primary key value expression
    key_expr: ResolvedExpr,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context
    txn_context: Option<TransactionContext>,
    /// The fetched row (if any)
    result: Option<Row>,
    /// Whether we've already returned the result
    returned: bool,
}

impl PointGet {
    pub fn new(
        table: String,
        key_expr: ResolvedExpr,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        PointGet {
            table,
            key_expr,
            mvcc,
            txn_context,
            result: None,
            returned: false,
        }
    }
}

#[async_trait]
impl Executor for PointGet {
    async fn open(&mut self) -> ExecutorResult<()> {
        // Evaluate the key expression to get the PK value
        // Use an empty row since the key expression is a literal
        let empty_row = Row::empty();
        let key_datum = eval(&self.key_expr, &empty_row)?;

        // Convert Datum to u64 row key
        let row_id = match &key_datum {
            Datum::Int(i) => *i as u64,
            _ => return Ok(()), // Non-integer PK, no result
        };

        // Encode the storage key
        let storage_key = encode_row_key(&self.table, row_id);

        // Do the O(log n) lookup
        let value = if let Some(ref ctx) = self.txn_context {
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
