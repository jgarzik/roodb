//! Transaction manager - central coordinator for transactions
//!
//! Manages the lifecycle of transactions including:
//! - BEGIN: Start a new transaction, allocate transaction ID
//! - COMMIT: Mark transaction as committed, persist to WAL
//! - ROLLBACK: Apply undo log in reverse, mark as aborted

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{
    IsolationLevel, ReadView, TimeoutConfig, Transaction, TransactionError, TransactionResult,
    TransactionState, UndoLog,
};
use crate::storage::StorageEngine;

/// Convert a RwLock PoisonError to TransactionError
fn lock_poisoned<T>(_: PoisonError<T>) -> TransactionError {
    TransactionError::Internal("Lock poisoned: a thread panicked while holding the lock".into())
}

/// Helper trait to convert RwLock results to TransactionResult
trait LockResultExt<T> {
    fn or_poisoned(self) -> TransactionResult<T>;
}

impl<'a, T> LockResultExt<RwLockReadGuard<'a, T>>
    for Result<RwLockReadGuard<'a, T>, PoisonError<RwLockReadGuard<'a, T>>>
{
    fn or_poisoned(self) -> TransactionResult<RwLockReadGuard<'a, T>> {
        self.map_err(lock_poisoned)
    }
}

impl<'a, T> LockResultExt<RwLockWriteGuard<'a, T>>
    for Result<RwLockWriteGuard<'a, T>, PoisonError<RwLockWriteGuard<'a, T>>>
{
    fn or_poisoned(self) -> TransactionResult<RwLockWriteGuard<'a, T>> {
        self.map_err(lock_poisoned)
    }
}

/// Transaction manager - coordinates all transaction operations
pub struct TransactionManager {
    /// Next transaction ID to assign
    next_txn_id: AtomicU64,

    /// Currently active transactions
    active_transactions: RwLock<HashMap<u64, Transaction>>,

    /// Set of committed transaction IDs (for visibility checks)
    /// We keep track of recently committed transactions until they can be purged
    committed_txns: RwLock<HashSet<u64>>,

    /// Undo log for rollback and MVCC
    undo_log: UndoLog,

    /// Whether this node is the Raft leader (can accept writes)
    is_leader: AtomicBool,

    /// Timeout configuration
    timeout_config: RwLock<TimeoutConfig>,

    /// Storage engine reference (for rollback operations)
    storage: Option<Arc<dyn StorageEngine>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            // Start at 1 so 0 can be used as "no transaction"
            next_txn_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashSet::new()),
            undo_log: UndoLog::new(),
            is_leader: AtomicBool::new(true), // Default to leader for single-node
            timeout_config: RwLock::new(TimeoutConfig::default()),
            storage: None,
        }
    }

    /// Create a transaction manager with storage reference
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashSet::new()),
            undo_log: UndoLog::new(),
            is_leader: AtomicBool::new(true),
            timeout_config: RwLock::new(TimeoutConfig::default()),
            storage: Some(storage),
        }
    }

    /// Set the storage engine (for rollback operations)
    pub fn set_storage(&mut self, storage: Arc<dyn StorageEngine>) {
        self.storage = Some(storage);
    }

    /// Set whether this node is the Raft leader
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    /// Check if this node is the Raft leader
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Update timeout configuration
    pub fn set_timeout_config(&self, config: TimeoutConfig) {
        match self.timeout_config.write() {
            Ok(mut guard) => *guard = config,
            Err(poisoned) => {
                tracing::warn!("Timeout config lock poisoned, recovering");
                *poisoned.into_inner() = config;
            }
        }
    }

    /// Get timeout configuration
    pub fn timeout_config(&self) -> TimeoutConfig {
        match self.timeout_config.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                tracing::warn!("Timeout config lock poisoned, recovering");
                poisoned.into_inner().clone()
            }
        }
    }

    /// Get a reference to the undo log
    pub fn undo_log(&self) -> &UndoLog {
        &self.undo_log
    }

    /// Begin a new transaction
    ///
    /// # Arguments
    /// * `isolation_level` - The isolation level for this transaction
    /// * `is_read_only` - Whether this is a read-only transaction (replica connection)
    pub fn begin(
        &self,
        isolation_level: IsolationLevel,
        is_read_only: bool,
    ) -> TransactionResult<Transaction> {
        // Check if writes are allowed
        if !is_read_only && !self.is_leader() {
            return Err(TransactionError::ReadOnly);
        }

        // Allocate transaction ID
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        // Create transaction
        let txn = Transaction::new(txn_id, isolation_level, is_read_only);

        // Register as active
        {
            let mut active = self.active_transactions.write().or_poisoned()?;
            active.insert(txn_id, txn.clone());
        }

        Ok(txn)
    }

    /// Commit a transaction
    ///
    /// Marks the transaction as committed and adds to committed set.
    /// The undo log for INSERT operations can be discarded.
    /// UPDATE/DELETE undo logs are kept for MVCC until purge.
    pub async fn commit(&self, txn_id: u64) -> TransactionResult<()> {
        // Get and validate transaction
        {
            let mut active = self.active_transactions.write().or_poisoned()?;
            let txn = active
                .remove(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;

            if txn.state != TransactionState::Active {
                return Err(TransactionError::NotActive(txn_id, txn.state));
            }
        }

        // Add to committed set
        {
            let mut committed = self.committed_txns.write().or_poisoned()?;
            committed.insert(txn_id);
        }

        // For now, we keep UPDATE/DELETE undo logs for MVCC visibility
        // INSERT undo logs can be removed since the row now exists
        //
        // Note: WAL-based commit logging (Record::commit) is available but not
        // yet integrated. Currently, commit durability is achieved via storage.flush()
        // which syncs the storage engine to disk. For a full WAL-based recovery
        // system, we would write a COMMIT record to WAL before marking committed.

        // Flush storage to ensure durability
        if let Some(storage) = &self.storage {
            storage.flush().await?;
        }

        Ok(())
    }

    /// Rollback a transaction
    ///
    /// Applies undo records in reverse order to restore previous state.
    pub async fn rollback(&self, txn_id: u64) -> TransactionResult<()> {
        // Get and validate transaction
        {
            let mut active = self.active_transactions.write().or_poisoned()?;
            let txn = active
                .remove(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;

            if txn.state != TransactionState::Active {
                return Err(TransactionError::NotActive(txn_id, txn.state));
            }
        }

        // Get undo records in reverse order
        let undo_records = self.undo_log.get_records_reversed(txn_id);

        // Apply undo operations
        if let Some(storage) = &self.storage {
            for record in undo_records {
                match record.undo_type {
                    super::UndoType::Insert => {
                        // Row was inserted - delete it
                        storage.delete(&record.row_key).await?;
                    }
                    super::UndoType::Update => {
                        // Row was updated - restore old value
                        if let Some(old_data) = &record.old_data {
                            storage.put(&record.row_key, old_data).await?;
                        }
                    }
                    super::UndoType::Delete => {
                        // Row was deleted - restore it
                        if let Some(old_data) = &record.old_data {
                            storage.put(&record.row_key, old_data).await?;
                        }
                    }
                }
            }
        }

        // Remove undo records
        self.undo_log.remove_transaction(txn_id);

        // Don't add to committed set - transaction was rolled back

        Ok(())
    }

    /// Create a read view for MVCC visibility
    ///
    /// The read view captures the current state of active transactions
    /// to determine which row versions are visible.
    pub fn create_read_view(&self, txn_id: u64) -> TransactionResult<ReadView> {
        let active = self.active_transactions.read().or_poisoned()?;
        let active_ids: HashSet<u64> = active.keys().copied().collect();
        let max_txn_id = self.next_txn_id.load(Ordering::SeqCst) - 1;

        Ok(ReadView::new(txn_id, active_ids, max_txn_id))
    }

    /// Check if a transaction is committed
    pub fn is_committed(&self, txn_id: u64) -> bool {
        let committed = self.committed_txns.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Committed txns lock poisoned, recovering");
            poisoned.into_inner()
        });
        committed.contains(&txn_id)
    }

    /// Check if a transaction is active
    pub fn is_active(&self, txn_id: u64) -> bool {
        let active = self.active_transactions.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Active transactions lock poisoned, recovering");
            poisoned.into_inner()
        });
        active.contains_key(&txn_id)
    }

    /// Get a transaction by ID (if active)
    pub fn get_transaction(&self, txn_id: u64) -> Option<Transaction> {
        let active = self.active_transactions.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Active transactions lock poisoned, recovering");
            poisoned.into_inner()
        });
        active.get(&txn_id).cloned()
    }

    /// Update last activity time for a transaction
    pub fn touch(&self, txn_id: u64) {
        let mut active = self.active_transactions.write().unwrap_or_else(|poisoned| {
            tracing::warn!("Active transactions lock poisoned, recovering");
            poisoned.into_inner()
        });
        if let Some(txn) = active.get_mut(&txn_id) {
            txn.touch();
        }
    }

    /// Check for timed out transactions and roll them back
    ///
    /// Returns the IDs of transactions that were rolled back.
    pub async fn check_timeouts(&self) -> Vec<u64> {
        let timeout_config = self.timeout_config();

        if !timeout_config.has_idle_timeout() {
            return vec![];
        }

        let idle_timeout = timeout_config.idle_in_transaction_timeout;

        // Find timed out transactions
        let timed_out: Vec<u64> = {
            let active = self.active_transactions.read().unwrap_or_else(|poisoned| {
                tracing::warn!("Active transactions lock poisoned, recovering");
                poisoned.into_inner()
            });
            active
                .iter()
                .filter(|(_, txn)| txn.idle_duration() > idle_timeout)
                .map(|(id, _)| *id)
                .collect()
        };

        // Roll them back
        let mut rolled_back = Vec::new();
        for txn_id in timed_out {
            if let Err(e) = self.rollback(txn_id).await {
                tracing::warn!("Failed to rollback timed out transaction {}: {}", txn_id, e);
            } else {
                tracing::info!(
                    "Rolled back transaction {} due to idle timeout ({:?})",
                    txn_id,
                    idle_timeout
                );
                rolled_back.push(txn_id);
            }
        }

        rolled_back
    }

    /// Get the minimum active transaction ID
    ///
    /// Used for purging old undo records that are no longer needed.
    pub fn min_active_txn_id(&self) -> u64 {
        let active = self.active_transactions.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Active transactions lock poisoned, recovering");
            poisoned.into_inner()
        });
        active.keys().min().copied().unwrap_or(u64::MAX)
    }

    /// Get the current transaction ID counter (for debugging)
    pub fn current_txn_id(&self) -> u64 {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    /// Get count of active transactions
    pub fn active_count(&self) -> usize {
        let active = self.active_transactions.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Active transactions lock poisoned, recovering");
            poisoned.into_inner()
        });
        active.len()
    }

    /// Get count of committed transactions (in memory)
    pub fn committed_count(&self) -> usize {
        let committed = self.committed_txns.read().unwrap_or_else(|poisoned| {
            tracing::warn!("Committed txns lock poisoned, recovering");
            poisoned.into_inner()
        });
        committed.len()
    }

    /// Purge old committed transaction records
    ///
    /// Call this periodically to free memory from old committed transaction IDs.
    pub fn purge_committed(&self, min_txn_id: u64) {
        let mut committed = self.committed_txns.write().unwrap_or_else(|poisoned| {
            tracing::warn!("Committed txns lock poisoned, recovering");
            poisoned.into_inner()
        });
        committed.retain(|&id| id >= min_txn_id);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Self {
            txn_id: self.txn_id,
            state: self.state,
            isolation_level: self.isolation_level,
            read_view: self.read_view.clone(),
            start_time: self.start_time,
            last_activity: self.last_activity,
            is_read_only: self.is_read_only,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();

        assert_eq!(txn.txn_id, 1);
        assert!(txn.is_active());
        assert!(!txn.is_read_only);
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_begin_read_only() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(IsolationLevel::RepeatableRead, true).unwrap();

        assert!(txn.is_read_only);
    }

    #[test]
    fn test_begin_fails_on_replica() {
        let mgr = TransactionManager::new();
        mgr.set_leader(false);

        // Read-only should succeed
        let txn = mgr.begin(IsolationLevel::RepeatableRead, true).unwrap();
        assert!(txn.is_read_only);

        // Read-write should fail
        let result = mgr.begin(IsolationLevel::RepeatableRead, false);
        assert!(matches!(result, Err(TransactionError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_commit() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn_id = txn.txn_id;

        assert!(mgr.is_active(txn_id));
        assert!(!mgr.is_committed(txn_id));

        mgr.commit(txn_id).await.unwrap();

        assert!(!mgr.is_active(txn_id));
        assert!(mgr.is_committed(txn_id));
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn test_rollback() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn_id = txn.txn_id;

        mgr.rollback(txn_id).await.unwrap();

        assert!(!mgr.is_active(txn_id));
        assert!(!mgr.is_committed(txn_id));
    }

    #[tokio::test]
    async fn test_commit_not_found() {
        let mgr = TransactionManager::new();
        let result = mgr.commit(999).await;
        assert!(matches!(result, Err(TransactionError::NotFound(999))));
    }

    #[test]
    fn test_create_read_view() {
        let mgr = TransactionManager::new();

        // Start some transactions
        let txn1 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn2 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn3 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();

        // Create read view for txn2
        let view = mgr.create_read_view(txn2.txn_id).unwrap();

        assert_eq!(view.creator_txn_id, txn2.txn_id);
        assert!(view.active_txn_ids.contains(&txn1.txn_id));
        assert!(view.active_txn_ids.contains(&txn2.txn_id));
        assert!(view.active_txn_ids.contains(&txn3.txn_id));
    }

    #[test]
    fn test_transaction_id_increment() {
        let mgr = TransactionManager::new();

        let txn1 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn2 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let txn3 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();

        assert_eq!(txn1.txn_id, 1);
        assert_eq!(txn2.txn_id, 2);
        assert_eq!(txn3.txn_id, 3);
    }

    #[test]
    fn test_min_active_txn_id() {
        let mgr = TransactionManager::new();

        assert_eq!(mgr.min_active_txn_id(), u64::MAX); // No active txns

        let _txn1 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();
        let _txn2 = mgr.begin(IsolationLevel::RepeatableRead, false).unwrap();

        assert_eq!(mgr.min_active_txn_id(), 1);
    }
}
