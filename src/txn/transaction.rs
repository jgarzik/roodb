//! Transaction struct and related types
//!
//! # Isolation Levels
//!
//! RooDB implements **InnoDB-style snapshot isolation** using MVCC (Multi-Version
//! Concurrency Control). The default isolation level is `REPEATABLE READ`.
//!
//! ## Current Implementation
//!
//! | Level | Implemented | Behavior |
//! |-------|-------------|----------|
//! | `READ UNCOMMITTED` | Partial | Falls back to READ COMMITTED behavior |
//! | `READ COMMITTED` | Yes | Each statement sees committed data as of statement start |
//! | `REPEATABLE READ` | Yes | Transaction sees consistent snapshot from first read |
//! | `SERIALIZABLE` | No | Falls back to REPEATABLE READ (no range locks yet) |
//!
//! ## Anomaly Prevention
//!
//! | Anomaly | Prevented at REPEATABLE READ? |
//! |---------|-------------------------------|
//! | Dirty reads | Yes |
//! | Non-repeatable reads | Yes |
//! | Phantom reads | Mostly (via OCC, not gap locks) |
//! | Lost updates | Yes (via OCC version checking) |
//!
//! ## How It Works
//!
//! 1. **MVCC**: Each row stores a transaction ID (`txn_id`) indicating which transaction
//!    last modified it. Old versions are preserved via undo log for visibility.
//!
//! 2. **Read View**: At transaction start (for REPEATABLE READ), a read view captures
//!    the set of active transactions. Rows modified by active or future transactions
//!    are invisible; only committed data from before the snapshot is visible.
//!
//! 3. **OCC (Optimistic Concurrency Control)**: UPDATE/DELETE operations record the
//!    row version they read. At commit time (Raft apply), if the row version changed,
//!    the operation fails with a write conflict error.
//!
//! ## Limitations
//!
//! - **No true SERIALIZABLE**: Gap locks and predicate locks are not implemented.
//!   SERIALIZABLE falls back to REPEATABLE READ.
//! - **No READ UNCOMMITTED**: Dirty reads aren't supported; falls back to READ COMMITTED.
//! - **Conflict errors are late**: Write conflicts are detected at Raft apply time,
//!   not at execution time. The client receives an error after commit attempt.

use std::time::Instant;

use super::{ReadView, TransactionError, TransactionResult};

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Dirty reads allowed - can see uncommitted changes from other transactions
    ReadUncommitted,

    /// Only see committed data, but each query gets a fresh snapshot
    ReadCommitted,

    /// Default. Snapshot taken at first read, consistent for entire transaction
    #[default]
    RepeatableRead,

    /// Strictest level - full serialization (requires locks, not yet implemented)
    Serializable,
}

impl IsolationLevel {
    /// Parse from SQL string (case-insensitive)
    pub fn from_sql(s: &str) -> Option<Self> {
        match s.to_uppercase().replace('-', " ").as_str() {
            "READ UNCOMMITTED" => Some(Self::ReadUncommitted),
            "READ COMMITTED" => Some(Self::ReadCommitted),
            "REPEATABLE READ" => Some(Self::RepeatableRead),
            "SERIALIZABLE" => Some(Self::Serializable),
            _ => None,
        }
    }

    /// Convert to SQL string
    pub fn to_sql(&self) -> &'static str {
        match self {
            Self::ReadUncommitted => "READ-UNCOMMITTED",
            Self::ReadCommitted => "READ-COMMITTED",
            Self::RepeatableRead => "REPEATABLE-READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }

    /// Whether this isolation level uses a single read view for the entire transaction
    pub fn uses_transaction_snapshot(&self) -> bool {
        matches!(self, Self::RepeatableRead | Self::Serializable)
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and accepting operations
    Active,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    Aborted,
}

/// A database transaction
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction ID (monotonically increasing)
    pub txn_id: u64,

    /// Current state of the transaction
    pub state: TransactionState,

    /// Isolation level for this transaction
    pub isolation_level: IsolationLevel,

    /// Read view for MVCC visibility (created lazily for REPEATABLE READ)
    pub read_view: Option<ReadView>,

    /// When the transaction started
    pub start_time: Instant,

    /// When the last statement was executed (for idle timeout)
    pub last_activity: Instant,

    /// Whether this is a read-only transaction (replica connections)
    pub is_read_only: bool,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(txn_id: u64, isolation_level: IsolationLevel, is_read_only: bool) -> Self {
        let now = Instant::now();
        Self {
            txn_id,
            state: TransactionState::Active,
            isolation_level,
            read_view: None,
            start_time: now,
            last_activity: now,
            is_read_only,
        }
    }

    /// Check if this transaction is still active
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Update the last activity timestamp
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Get the duration since last activity
    pub fn idle_duration(&self) -> std::time::Duration {
        self.last_activity.elapsed()
    }

    /// Get the total duration of this transaction
    pub fn duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Mark as committed
    ///
    /// Returns an error if the transaction is not active.
    pub fn commit(&mut self) -> TransactionResult<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive(self.txn_id, self.state));
        }
        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Mark as aborted
    ///
    /// Returns an error if the transaction is not active.
    pub fn abort(&mut self) -> TransactionResult<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive(self.txn_id, self.state));
        }
        self.state = TransactionState::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_parsing() {
        assert_eq!(
            IsolationLevel::from_sql("READ UNCOMMITTED"),
            Some(IsolationLevel::ReadUncommitted)
        );
        assert_eq!(
            IsolationLevel::from_sql("read-committed"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::from_sql("REPEATABLE READ"),
            Some(IsolationLevel::RepeatableRead)
        );
        assert_eq!(
            IsolationLevel::from_sql("serializable"),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(IsolationLevel::from_sql("invalid"), None);
    }

    #[test]
    fn test_transaction_lifecycle() {
        let mut txn = Transaction::new(1, IsolationLevel::RepeatableRead, false);
        assert!(txn.is_active());
        assert_eq!(txn.state, TransactionState::Active);

        txn.commit().unwrap();
        assert!(!txn.is_active());
        assert_eq!(txn.state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_abort() {
        let mut txn = Transaction::new(1, IsolationLevel::RepeatableRead, false);
        txn.abort().unwrap();
        assert!(!txn.is_active());
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_double_commit_fails() {
        let mut txn = Transaction::new(1, IsolationLevel::RepeatableRead, false);
        txn.commit().unwrap();
        // Second commit should fail
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_commit_after_abort_fails() {
        let mut txn = Transaction::new(1, IsolationLevel::RepeatableRead, false);
        txn.abort().unwrap();
        // Commit after abort should fail
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_read_only_transaction() {
        let txn = Transaction::new(1, IsolationLevel::RepeatableRead, true);
        assert!(txn.is_read_only);
    }
}
