//! Transaction context for executors
//!
//! Provides transaction state needed by executors to implement MVCC.

use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use crate::raft::RowChange;
use crate::txn::{LockError, LockManager, LockMode, ReadView};

use super::error::{ExecutorError, ExecutorResult};

/// How a statement takes row locks: where, and how long it will wait.
#[derive(Debug, Clone)]
pub struct LockHandle {
    pub manager: Arc<LockManager>,
    /// How long to wait for a lock another transaction holds.
    pub timeout: Duration,
    /// Whether ranges that were read are locked against inserts (REPEATABLE
    /// READ) or only the rows themselves (READ COMMITTED).
    pub gap_locks: bool,
}

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
    /// Row locking for this statement. `None` where there is nothing to
    /// lock against: plain reads, and executors run without a server.
    locks: Option<LockHandle>,
}

fn lock_error(e: LockError) -> ExecutorError {
    match e {
        LockError::Deadlock => ExecutorError::LockDeadlock,
        LockError::Timeout => ExecutorError::LockWaitTimeout,
    }
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(txn_id: u64, read_view: ReadView) -> Self {
        Self {
            txn_id,
            read_view,
            changes: Vec::new(),
            write_buffer: HashMap::new(),
            locks: None,
        }
    }

    /// Take row locks through `locks` for the rest of this statement.
    pub fn with_locks(mut self, locks: LockHandle) -> Self {
        self.locks = Some(locks);
        self
    }

    /// Whether this statement takes row locks.
    pub fn locks_rows(&self) -> bool {
        self.locks.is_some()
    }

    /// Lock a row for this transaction, waiting for others that hold it.
    pub async fn lock_record(&self, key: &[u8], mode: LockMode) -> ExecutorResult<()> {
        match &self.locks {
            Some(locks) => locks
                .manager
                .lock_record(self.txn_id, key, mode, locks.timeout)
                .await
                .map_err(lock_error),
            None => Ok(()),
        }
    }

    /// Wait until this transaction may insert a row at `key`.
    pub async fn lock_insert(&self, key: &[u8]) -> ExecutorResult<()> {
        match &self.locks {
            Some(locks) => locks
                .manager
                .lock_insert(self.txn_id, key, locks.timeout)
                .await
                .map_err(lock_error),
            None => Ok(()),
        }
    }

    /// Keep other transactions from inserting between `low` and `high`, if
    /// this transaction locks the ranges it reads.
    pub fn lock_gap(&self, low: &[u8], high: &[u8]) {
        if let Some(locks) = &self.locks {
            if locks.gap_locks {
                locks.manager.lock_gap(self.txn_id, low, high);
            }
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

    /// Check if a key exists in the write buffer (for duplicate detection)
    pub fn has_buffered_key(&self, key: &[u8]) -> bool {
        matches!(self.write_buffer.get(key), Some(Some(_)))
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

    /// A stored row as this transaction should see it when changing it
    /// again: with the transaction's own earlier change to it, if it made
    /// one. `None` if the row does not exist or the transaction has deleted
    /// it. The version stays that of the stored row, which is what the
    /// change will be checked against when it is applied.
    ///
    /// (A row the transaction itself inserted is not in storage yet and is
    /// not found this way.)
    pub fn with_own_change(
        &self,
        key: &[u8],
        stored: Option<(Vec<u8>, u64)>,
    ) -> Option<(Vec<u8>, u64)> {
        let (data, version) = stored?;
        match self.write_buffer.get(key) {
            Some(Some(own)) => Some((own.clone(), version)),
            Some(None) => None,
            None => Some((data, version)),
        }
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

    /// Get all buffered writes within a key range (for range scans)
    ///
    /// Returns entries where start <= key < end.
    pub fn get_buffered_for_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Vec<(&Vec<u8>, Option<&Vec<u8>>)> {
        self.write_buffer
            .iter()
            .filter(|(k, _)| k.as_slice() >= start && k.as_slice() < end)
            .map(|(k, v)| (k, v.as_ref()))
            .collect()
    }
}
