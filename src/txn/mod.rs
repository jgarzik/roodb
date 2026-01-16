//! Transaction management module
//!
//! Provides ACID-compliant transaction support with:
//! - MVCC (Multi-Version Concurrency Control) using InnoDB-style undo logs
//! - Isolation levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ
//! - PostgreSQL-style timeout handling for resource safety
//! - Leader/replica awareness for distributed operation

mod manager;
mod mvcc_storage;
mod purge;
mod read_view;
mod transaction;
mod undo_log;

pub use manager::TransactionManager;
pub use mvcc_storage::MvccStorage;
pub use purge::PurgeTask;
pub use read_view::ReadView;
pub use transaction::{IsolationLevel, Transaction, TransactionState};
pub use undo_log::{UndoLog, UndoRecord, UndoType};

use std::time::Duration;
use thiserror::Error;

/// Transaction operation errors
#[derive(Error, Debug)]
pub enum TransactionError {
    /// Transaction not found
    #[error("Transaction {0} not found")]
    NotFound(u64),

    /// Transaction already committed or aborted
    #[error("Transaction {0} is not active (state: {1:?})")]
    NotActive(u64, TransactionState),

    /// Attempt to write on read-only connection (replica)
    #[error("Cannot write: connection is read-only (connected to replica)")]
    ReadOnly,

    /// Transaction already in progress
    #[error("Transaction already in progress")]
    AlreadyInTransaction,

    /// No transaction in progress
    #[error("No transaction in progress")]
    NoTransaction,

    /// Transaction timed out
    #[error("Transaction {0} timed out after {1:?} idle")]
    IdleTimeout(u64, Duration),

    /// Unsupported isolation level
    #[error("Isolation level {0} is not supported")]
    UnsupportedIsolationLevel(String),

    /// Storage error during transaction operation
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for transaction operations
pub type TransactionResult<T> = Result<T, TransactionError>;

/// Timeout configuration for transactions (PostgreSQL-style)
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Terminate session if idle inside transaction > this duration
    /// PostgreSQL: idle_in_transaction_session_timeout
    /// Default: 10 minutes (Duration::ZERO = disabled)
    pub idle_in_transaction_timeout: Duration,

    /// Abort statement if execution > this duration
    /// PostgreSQL: statement_timeout
    /// Default: disabled (Duration::ZERO)
    pub statement_timeout: Duration,

    /// Abort statement if waiting for lock > this duration
    /// PostgreSQL: lock_timeout
    /// Default: disabled (Duration::ZERO)
    pub lock_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            idle_in_transaction_timeout: Duration::from_secs(600), // 10 minutes
            statement_timeout: Duration::ZERO,                     // disabled
            lock_timeout: Duration::ZERO,                          // disabled
        }
    }
}

impl TimeoutConfig {
    /// Create a config with all timeouts disabled
    pub fn disabled() -> Self {
        Self {
            idle_in_transaction_timeout: Duration::ZERO,
            statement_timeout: Duration::ZERO,
            lock_timeout: Duration::ZERO,
        }
    }

    /// Check if idle timeout is enabled
    pub fn has_idle_timeout(&self) -> bool {
        !self.idle_in_transaction_timeout.is_zero()
    }
}
