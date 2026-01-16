//! Transaction context for executors
//!
//! Provides transaction state needed by executors to implement MVCC.

use std::collections::HashMap;

use crate::raft::RowChange;
use crate::txn::ReadView;

/// Transaction context passed to executors
///
/// Contains the transaction ID and read view needed for:
/// - Visibility filtering during scans (read_view)
/// - Versioned writes during insert/update/delete (txn_id)
/// - Collecting row changes for Raft replication (changes)
/// - Buffering uncommitted writes for read-your-writes (write_buffer)
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
    /// Write buffer for read-your-writes within this transaction
    ///
    /// Maps row keys to their uncommitted values (None = deleted).
    /// Reads check this buffer first before querying storage.
    write_buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(txn_id: u64, read_view: ReadView) -> Self {
        Self {
            txn_id,
            read_view,
            changes: Vec::new(),
            write_buffer: HashMap::new(),
        }
    }

    /// Create a transaction context with pre-existing pending changes (for read-your-writes)
    pub fn with_pending_changes(
        txn_id: u64,
        read_view: ReadView,
        pending_changes: &[RowChange],
    ) -> Self {
        let mut ctx = Self::new(txn_id, read_view);
        // Populate write buffer from pending changes for read-your-writes
        for change in pending_changes {
            match change.op {
                crate::raft::ChangeOp::Insert | crate::raft::ChangeOp::Update => {
                    if let Some(ref value) = change.value {
                        ctx.write_buffer
                            .insert(change.key.clone(), Some(value.clone()));
                    }
                }
                crate::raft::ChangeOp::Delete => {
                    ctx.write_buffer.insert(change.key.clone(), None);
                }
            }
        }
        ctx
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

    /// Buffer a write for read-your-writes semantics
    pub fn buffer_write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_buffer.insert(key, Some(value));
    }

    /// Buffer a delete for read-your-writes semantics
    pub fn buffer_delete(&mut self, key: Vec<u8>) {
        self.write_buffer.insert(key, None);
    }

    /// Check if a key exists in the write buffer
    ///
    /// Returns:
    /// - Some(Some(value)) if the key was written (insert/update)
    /// - Some(None) if the key was deleted
    /// - None if the key is not in the buffer
    pub fn get_buffered(&self, key: &[u8]) -> Option<Option<&Vec<u8>>> {
        self.write_buffer.get(key).map(|v| v.as_ref())
    }

    /// Check if a key is marked as deleted in the buffer
    pub fn is_buffered_deleted(&self, key: &[u8]) -> bool {
        matches!(self.write_buffer.get(key), Some(None))
    }

    /// Get all buffered writes for a table prefix (for scans)
    ///
    /// Returns a Vec of (key, Option<value>) pairs where the key starts with the prefix.
    pub fn get_buffered_for_prefix(&self, prefix: &[u8]) -> Vec<(&Vec<u8>, Option<&Vec<u8>>)> {
        self.write_buffer
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k, v.as_ref()))
            .collect()
    }
}
