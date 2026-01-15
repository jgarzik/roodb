//! Transaction context for executors
//!
//! Provides transaction state needed by executors to implement MVCC.

use crate::txn::ReadView;

/// Transaction context passed to executors
///
/// Contains the transaction ID and read view needed for:
/// - Visibility filtering during scans (read_view)
/// - Versioned writes during insert/update/delete (txn_id)
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transaction ID for versioned writes
    pub txn_id: u64,
    /// Read view for visibility filtering
    pub read_view: ReadView,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(txn_id: u64, read_view: ReadView) -> Self {
        Self { txn_id, read_view }
    }
}
