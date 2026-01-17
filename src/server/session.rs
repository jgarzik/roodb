//! Session state for RooDB connections

use crate::raft::RowChange;
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};
use crate::txn::{IsolationLevel, TimeoutConfig};

/// Default batch size for row ID allocation
const ROW_ID_BATCH_SIZE: u64 = 1000;

/// Pre-allocated batch of IDs for session-local allocation
///
/// This reduces atomic contention by allocating IDs in batches.
/// When the batch is exhausted, a new batch is allocated from the global counter.
#[derive(Debug, Clone)]
pub struct IdBatch {
    /// Next local ID to allocate
    next_local: u64,
    /// End of the allocated range (exclusive)
    end_local: u64,
    /// Node ID for encoding full row IDs
    node_id: u64,
}

impl IdBatch {
    /// Create an empty batch (will allocate on first use)
    pub fn empty() -> Self {
        Self {
            next_local: 0,
            end_local: 0,
            node_id: 0,
        }
    }

    /// Check if the batch is exhausted
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.next_local >= self.end_local
    }

    /// Get the next ID from this batch, or None if exhausted
    #[inline]
    pub fn next_id(&mut self) -> Option<u64> {
        if self.next_local < self.end_local {
            let local = self.next_local;
            self.next_local += 1;
            Some(encode_row_id(local, self.node_id))
        } else {
            None
        }
    }

    /// Refill this batch from the global allocator
    fn refill(&mut self, count: u64) {
        let (start, node_id) = allocate_row_id_batch(count);
        self.next_local = start;
        self.end_local = start + count;
        self.node_id = node_id;
    }
}

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
    /// Accumulated changes for explicit transaction (proposed on COMMIT)
    pending_changes: Vec<RowChange>,

    // ID batching for reduced atomic contention
    /// Pre-allocated batch of row IDs for this session
    row_id_batch: IdBatch,
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
            pending_changes: Vec::new(),
            // ID batching
            row_id_batch: IdBatch::empty(),
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

    /// Get the protocol status flags for this session
    pub fn status_flags(&self) -> u16 {
        use crate::protocol::roodb::status_flags;

        let mut flags = 0u16;

        if self.autocommit {
            flags |= status_flags::SERVER_STATUS_AUTOCOMMIT;
        }

        if self.in_transaction() {
            flags |= status_flags::SERVER_STATUS_IN_TRANS;
        }

        flags
    }

    /// Add changes to the pending transaction (for explicit transactions)
    pub fn add_pending_changes(&mut self, changes: Vec<RowChange>) {
        self.pending_changes.extend(changes);
    }

    /// Take all pending changes (called on COMMIT)
    pub fn take_pending_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.pending_changes)
    }

    /// Check if there are pending changes
    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.is_empty()
    }

    /// Get a reference to pending changes (for read-your-writes)
    pub fn get_pending_changes(&self) -> &[RowChange] {
        &self.pending_changes
    }

    /// Clear pending changes without returning them (called on ROLLBACK)
    pub fn clear_pending_changes(&mut self) {
        self.pending_changes.clear();
    }

    /// Get the next row ID from this session's batch
    ///
    /// This reduces atomic contention by allocating IDs in batches of 1000.
    /// When the batch is exhausted, a new batch is allocated from the global counter.
    #[inline]
    pub fn next_row_id(&mut self) -> u64 {
        // Try to get from existing batch
        if let Some(id) = self.row_id_batch.next_id() {
            return id;
        }

        // Batch exhausted, refill and return first ID
        self.row_id_batch.refill(ROW_ID_BATCH_SIZE);
        self.row_id_batch
            .next_id()
            .expect("freshly refilled batch should have IDs")
    }
}
