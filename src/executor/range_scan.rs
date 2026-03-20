//! RangeScan executor
//!
//! Scans a range of rows by primary key bounds using storage scan() with
//! tight byte-range bounds instead of full table scan.

use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;
use crate::txn::MvccStorage;

use crate::server::session::UserVariables;

use super::context::TransactionContext;
use super::encoding::{decode_row, encode_pk_key, table_key_end, table_key_prefix};
use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Range scan executor — scans only the PK range instead of the full table.
pub struct RangeScan {
    /// Table name
    table: String,
    /// Start PK bound expression (None = table start)
    start_expr: Option<ResolvedExpr>,
    /// End PK bound expression (None = table end)
    end_expr: Option<ResolvedExpr>,
    /// Whether start bound is inclusive
    inclusive_start: bool,
    /// Whether end bound is inclusive
    inclusive_end: bool,
    /// Remaining filter predicate not pushed to storage bounds
    remaining_filter: Option<ResolvedExpr>,
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Transaction context
    txn_context: Option<TransactionContext>,
    /// Raw values from storage (decoded lazily in next())
    raw_pairs: Vec<Vec<u8>>,
    /// Current position in raw_pairs
    position: usize,
    /// User variables
    user_variables: UserVariables,
}

/// Configuration for a range scan's bounds
pub struct RangeScanBounds {
    pub start_expr: Option<ResolvedExpr>,
    pub end_expr: Option<ResolvedExpr>,
    pub inclusive_start: bool,
    pub inclusive_end: bool,
    pub remaining_filter: Option<ResolvedExpr>,
}

impl RangeScan {
    pub fn new(
        table: String,
        bounds: RangeScanBounds,
        mvcc: Arc<MvccStorage>,
        txn_context: Option<TransactionContext>,
        user_variables: UserVariables,
    ) -> Self {
        RangeScan {
            table,
            start_expr: bounds.start_expr,
            end_expr: bounds.end_expr,
            inclusive_start: bounds.inclusive_start,
            inclusive_end: bounds.inclusive_end,
            remaining_filter: bounds.remaining_filter,
            mvcc,
            txn_context,
            raw_pairs: Vec::new(),
            position: 0,
            user_variables,
        }
    }

    /// Increment a byte key to make an exclusive bound from an inclusive one.
    /// Appends 0x00 to the key, which is the next key in sorted order after
    /// any key with the given prefix.
    fn increment_key(mut key: Vec<u8>) -> Vec<u8> {
        key.push(0x00);
        key
    }
}

#[async_trait]
impl Executor for RangeScan {
    async fn open(&mut self) -> ExecutorResult<()> {
        let empty_row = Row::empty();

        // Compute start bound
        let start_key = if let Some(ref expr) = self.start_expr {
            let datum = evaluate(expr, &empty_row, &self.user_variables)?;
            let key = encode_pk_key(&self.table, &[datum]);
            if self.inclusive_start {
                key
            } else {
                // Exclusive start: increment to skip the exact match
                Self::increment_key(key)
            }
        } else {
            table_key_prefix(&self.table)
        };

        // Compute end bound (storage scan end is always exclusive)
        let end_key = if let Some(ref expr) = self.end_expr {
            let datum = evaluate(expr, &empty_row, &self.user_variables)?;
            let key = encode_pk_key(&self.table, &[datum]);
            if self.inclusive_end {
                // Inclusive end: increment so storage scan includes this key
                Self::increment_key(key)
            } else {
                key
            }
        } else {
            table_key_end(&self.table)
        };

        // MVCC scan with tight bounds
        let kv_pairs = if let Some(ref ctx) = self.txn_context {
            self.mvcc
                .scan(Some(&start_key), Some(&end_key), &ctx.read_view)
                .await?
        } else {
            self.mvcc
                .inner()
                .scan(Some(&start_key), Some(&end_key))
                .await?
        };

        // Collect keys that are buffered (for read-your-writes merge)
        let buffered_entries = if let Some(ref ctx) = self.txn_context {
            // Only get buffered entries in our range
            ctx.get_buffered_for_range(&start_key, &end_key)
        } else {
            Vec::new()
        };

        // Build a set of buffered keys for quick lookup
        use std::collections::HashSet;
        let buffered_keys: HashSet<&[u8]> =
            buffered_entries.iter().map(|(k, _)| k.as_slice()).collect();

        // Collect raw values — skip keys overridden by buffer
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
        while self.position < self.raw_pairs.len() {
            let row = decode_row(&self.raw_pairs[self.position])?;
            self.position += 1;

            if let Some(filter) = &self.remaining_filter {
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
