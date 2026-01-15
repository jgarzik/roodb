//! Session state for MySQL connections

use crate::txn::{IsolationLevel, TimeoutConfig};

/// Per-connection session state
#[derive(Debug, Clone)]
pub struct Session {
    /// Unique connection identifier
    pub connection_id: u32,
    /// Currently selected database (USE db)
    pub database: Option<String>,
    /// Authenticated username
    pub user: String,

    // Transaction state
    /// Active transaction ID (None = autocommit mode, no explicit transaction)
    pub current_txn: Option<u64>,
    /// Whether autocommit is enabled (default: true)
    pub autocommit: bool,
    /// Transaction isolation level for this session
    pub isolation_level: IsolationLevel,
    /// Whether this is a read-only connection (connected to replica)
    pub is_read_only: bool,
    /// Per-session timeout configuration
    pub timeout_config: TimeoutConfig,
}

impl Session {
    /// Create a new session with default state
    pub fn new(connection_id: u32) -> Self {
        Self {
            connection_id,
            database: None,
            user: String::new(),
            // Transaction defaults
            current_txn: None,
            autocommit: true,
            isolation_level: IsolationLevel::default(),
            is_read_only: false,
            timeout_config: TimeoutConfig::default(),
        }
    }

    /// Create a new session for a replica (read-only)
    pub fn new_read_only(connection_id: u32) -> Self {
        let mut session = Self::new(connection_id);
        session.is_read_only = true;
        session
    }

    /// Set the authenticated user
    pub fn set_user(&mut self, user: String) {
        self.user = user;
    }

    /// Set the current database
    pub fn set_database(&mut self, database: Option<String>) {
        self.database = database;
    }

    /// Check if we're in an explicit transaction
    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some()
    }

    /// Start a transaction
    pub fn begin_transaction(&mut self, txn_id: u64) {
        self.current_txn = Some(txn_id);
    }

    /// End the current transaction
    pub fn end_transaction(&mut self) {
        self.current_txn = None;
    }

    /// Set autocommit mode
    pub fn set_autocommit(&mut self, enabled: bool) {
        self.autocommit = enabled;
    }

    /// Set isolation level
    pub fn set_isolation_level(&mut self, level: IsolationLevel) {
        self.isolation_level = level;
    }

    /// Get the MySQL status flags for this session
    pub fn status_flags(&self) -> u16 {
        use crate::protocol::mysql::status_flags;

        let mut flags = 0u16;

        if self.autocommit {
            flags |= status_flags::SERVER_STATUS_AUTOCOMMIT;
        }

        if self.in_transaction() {
            flags |= status_flags::SERVER_STATUS_IN_TRANS;
        }

        flags
    }
}
