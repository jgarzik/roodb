//! Transaction context for executors
//!
//! Provides transaction state needed by executors to implement MVCC.

use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use crate::raft::RowChange;
use crate::storage::traits::KeyValue;
use crate::txn::{LockError, LockManager, LockMode, MvccStorage, ReadView};

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
    /// Set for a locking read (`SELECT ... FOR UPDATE` / `FOR SHARE`): the
    /// rows the statement reads are locked too.
    pub read: Option<ReadLock>,
}

/// The locking a `SELECT ... FOR UPDATE` / `FOR SHARE` asks for.
#[derive(Debug, Clone)]
pub struct ReadLock {
    pub mode: LockMode,
    pub wait: LockWait,
    /// Tables named after `OF`, lower-cased; empty means every table read.
    pub of: Vec<String>,
}

/// What a locking read does about a row another transaction has locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    /// Wait for it (up to the lock wait timeout).
    Wait,
    /// Fail the statement (`NOWAIT`).
    NoWait,
    /// Leave the row out of the result (`SKIP LOCKED`).
    SkipLocked,
}

/// A row as a locking read finds it.
pub enum LockedRow {
    /// Locked; this is the row as it stands now.
    Row(Vec<u8>),
    /// There is no such row (any more). The key is still locked if this
    /// read locked it (`new_lock`): the caller decides what to hold in its
    /// place before giving it back with `unlock_record`.
    Missing { new_lock: bool },
    /// Locked by another transaction, and the statement said SKIP LOCKED.
    Skipped,
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
    /// Returns whether the lock is new to the transaction (and so may be
    /// given back with `unlock_record` if the row turns out not to matter).
    pub async fn lock_record(&self, key: &[u8], mode: LockMode) -> ExecutorResult<bool> {
        match &self.locks {
            Some(locks) => locks
                .manager
                .lock_record(self.txn_id, key, mode, locks.timeout)
                .await
                .map_err(lock_error),
            None => Ok(false),
        }
    }

    /// Whether this statement is a locking read of `table`.
    pub fn read_locks(&self, table: &str) -> bool {
        self.locks
            .as_ref()
            .and_then(|locks| locks.read.as_ref())
            .is_some_and(|read| {
                read.of.is_empty() || read.of.iter().any(|t| t.eq_ignore_ascii_case(table))
            })
    }

    /// For a locking read: lock the row at `key` and return it as it stands
    /// now. A row this transaction wrote itself is returned as it left it;
    /// it holds that row already.
    pub async fn locked_read(&self, mvcc: &MvccStorage, key: &[u8]) -> ExecutorResult<LockedRow> {
        match self.write_buffer.get(key) {
            Some(Some(own)) => return Ok(LockedRow::Row(own.clone())),
            Some(None) => return Ok(LockedRow::Missing { new_lock: false }),
            None => {}
        }
        let Some((locks, read)) = self
            .locks
            .as_ref()
            .and_then(|locks| locks.read.as_ref().map(|read| (locks, read)))
        else {
            return Ok(match mvcc.get_current(key).await? {
                Some((data, _)) => LockedRow::Row(data),
                None => LockedRow::Missing { new_lock: false },
            });
        };
        let new_lock = match read.wait {
            LockWait::Wait => locks
                .manager
                .lock_record(self.txn_id, key, read.mode, locks.timeout)
                .await
                .map_err(lock_error)?,
            LockWait::NoWait | LockWait::SkipLocked => {
                match locks.manager.try_lock_record(self.txn_id, key, read.mode) {
                    Some(new_lock) => new_lock,
                    None if read.wait == LockWait::NoWait => return Err(ExecutorError::LockNoWait),
                    None => return Ok(LockedRow::Skipped),
                }
            }
        };
        match mvcc.get_current(key).await? {
            Some((data, _)) => Ok(LockedRow::Row(data)),
            None => Ok(LockedRow::Missing { new_lock }),
        }
    }

    /// For a locking read of the keys in `[start, end)` of a table whose
    /// keys span `(low, high)`: lock the range against inserts (REPEATABLE
    /// READ), then lock each row in it and return the rows as they stand
    /// now. Rows of this transaction's own that are not stored yet are not
    /// included; the caller adds them from the write buffer as for any scan.
    pub async fn locking_scan(
        &self,
        mvcc: &MvccStorage,
        start: &[u8],
        end: &[u8],
        low: &[u8],
        high: &[u8],
    ) -> ExecutorResult<Vec<KeyValue>> {
        if self.locks_gaps() {
            // The gap reaches from the last row before the range to the
            // first row at or after its end: nothing may be inserted
            // anywhere a row of the range could be.
            let before = if start <= low {
                low.to_vec()
            } else {
                mvcc.gap_around(start, low, high).await?.0
            };
            let after = if end >= high {
                high.to_vec()
            } else if mvcc.inner().get(end).await?.is_some() {
                // A key is stored right at the end of the range (a row, or
                // the marker of a deleted one): the gap stops there.
                end.to_vec()
            } else {
                mvcc.gap_around(end, low, high).await?.1
            };
            self.lock_gap(&before, &after);
        }
        // Which keys there are is read after the gap is locked, and as
        // things stand now rather than in the statement's snapshot: a
        // locking read sees the latest committed rows.
        let mut keys: Vec<Vec<u8>> = mvcc
            .inner()
            .scan(Some(start), Some(end))
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        self.add_keys_being_inserted(&mut keys, start, end);
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            if self.write_buffer.contains_key(&key) {
                continue;
            }
            match self.locked_read(mvcc, &key).await? {
                LockedRow::Row(data) => rows.push((key, data)),
                LockedRow::Missing { new_lock: true } => self.unlock_record(&key),
                LockedRow::Missing { new_lock: false } | LockedRow::Skipped => {}
            }
        }
        Ok(rows)
    }

    /// Add to `keys` (the stored keys of `[start, end)`, in order) the keys
    /// in that range that other transactions have locked. Among them are
    /// rows being inserted: not stored yet, so a scan would pass them by,
    /// and appearing in the range once their transaction commits. A
    /// statement that locks the range locks these keys too, which makes it
    /// wait for those transactions and see what they left.
    pub fn add_keys_being_inserted(&self, keys: &mut Vec<Vec<u8>>, start: &[u8], end: &[u8]) {
        let Some(locks) = &self.locks else { return };
        // `start` itself belongs to the range; the lock table's range is
        // open at both ends.
        let mut locked = locks.manager.others_locked_in(self.txn_id, start, end);
        if locks
            .manager
            .others_locked_in(self.txn_id, &[], end)
            .iter()
            .any(|key| key.as_slice() == start)
        {
            locked.push(start.to_vec());
        }
        if locked.is_empty() {
            return;
        }
        keys.extend(locked);
        keys.sort_unstable();
        keys.dedup();
    }

    /// Wait until this transaction may insert a row at `key`, and (for a
    /// key another transaction could also choose, `lock_key`) lock the key.
    ///
    /// The key is locked before the gap is looked at: otherwise another
    /// transaction could look for the key while this one waits for it,
    /// find nothing, lock the gap, and then see this row appear in it. But
    /// the key is not held while waiting for a gap lock to go away: the
    /// holder of that gap may want to insert this very key, and it comes
    /// first.
    pub async fn lock_for_insert(&self, key: &[u8], lock_key: bool) -> ExecutorResult<()> {
        let Some(locks) = &self.locks else {
            return Ok(());
        };
        if !lock_key {
            return self.lock_insert(key).await;
        }
        loop {
            let new_lock = self.lock_record(key, LockMode::Exclusive).await?;
            if !locks.manager.insert_blocked(self.txn_id, key) {
                return Ok(());
            }
            if new_lock {
                self.unlock_record(key);
            }
            self.lock_insert(key).await?;
        }
    }

    /// Give back a lock taken on a row only to look at it.
    pub fn unlock_record(&self, key: &[u8]) {
        if let Some(locks) = &self.locks {
            locks.manager.unlock_record(self.txn_id, key);
        }
    }

    /// Whether this transaction locks the ranges it reads, so that what it
    /// read stays as it was (REPEATABLE READ), or only the rows it changes.
    pub fn locks_gaps(&self) -> bool {
        self.locks.as_ref().is_some_and(|locks| locks.gap_locks)
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
