//! Transaction struct and related types

use std::time::Instant;

use super::ReadView;

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
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark as aborted
    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
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

        txn.commit();
        assert!(!txn.is_active());
        assert_eq!(txn.state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_abort() {
        let mut txn = Transaction::new(1, IsolationLevel::RepeatableRead, false);
        txn.abort();
        assert!(!txn.is_active());
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_read_only_transaction() {
        let txn = Transaction::new(1, IsolationLevel::RepeatableRead, true);
        assert!(txn.is_read_only);
    }
}
