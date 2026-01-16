//! Transaction context for executors
//!
//! Provides transaction state needed by executors to implement MVCC.

use crate::raft::RowChange;
use crate::txn::ReadView;

/// Transaction context passed to executors
///
/// Contains the transaction ID and read view needed for:
/// - Visibility filtering during scans (read_view)
/// - Versioned writes during insert/update/delete (txn_id)
/// - Collecting row changes for Raft replication (changes)
#[derive(Debug, Clone, Default)]
pub struct TransactionContext {
    /// Transaction ID for versioned writes
    pub txn_id: u64,
    /// Read view for visibility filtering
    pub read_view: ReadView,
    /// Row changes collected during DML execution
    ///
    /// These changes are proposed to Raft for replication after
    /// the executor completes successfully.
    pub changes: Vec<RowChange>,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(txn_id: u64, read_view: ReadView) -> Self {
        Self {
            txn_id,
            read_view,
            changes: Vec::new(),
        }
    }

    /// Add a row change to be replicated
    pub fn add_change(&mut self, change: RowChange) {
        self.changes.push(change);
    }

    /// Check if there are any pending changes
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Take the collected changes, leaving the context empty
    pub fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}
