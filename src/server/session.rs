//! Session state for RooDB connections

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// MySQL 8.0 default sql_mode values
const DEFAULT_SQL_MODES: &[&str] = &[
    "ONLY_FULL_GROUP_BY",
    "STRICT_TRANS_TABLES",
    "NO_ZERO_IN_DATE",
    "NO_ZERO_DATE",
    "ERROR_FOR_DIVISION_BY_ZERO",
    "NO_ENGINE_SUBSTITUTION",
];

fn default_sql_modes() -> HashSet<String> {
    DEFAULT_SQL_MODES.iter().map(|s| s.to_string()).collect()
}

use parking_lot::RwLock;

use crate::executor::Datum;
use crate::raft::RowChange;
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};
use crate::txn::{IsolationLevel, TimeoutConfig};

/// Thread-safe per-session user variable storage (@var)
pub type UserVariables = Arc<RwLock<HashMap<String, Datum>>>;

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

/// A warning generated during query execution
#[derive(Debug, Clone)]
pub struct Warning {
    /// Warning level (Note, Warning, Error)
    pub level: String,
    /// MySQL-compatible warning code
    pub code: u16,
    /// Human-readable message
    pub message: String,
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

    // Warning tracking
    /// Warnings accumulated during the current statement
    warnings: Vec<Warning>,

    // User variables (@var)
    /// Per-session user variable storage
    user_variables: UserVariables,

    /// SQL-level prepared statements (PREPARE stmt FROM 'sql')
    pub sql_prepared_stmts: std::collections::HashMap<String, String>,

    /// Active SQL modes (e.g., NO_UNSIGNED_SUBTRACTION, STRICT_TRANS_TABLES)
    sql_modes: HashSet<String>,

    /// Temporary table names owned by this session (cleaned up on disconnect)
    temp_table_names: Vec<String>,
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
            // Warnings
            warnings: Vec::new(),
            // User variables
            user_variables: Arc::new(RwLock::new(HashMap::new())),
            // SQL prepared statements
            sql_prepared_stmts: std::collections::HashMap::new(),
            // SQL modes — MySQL 8.0 defaults
            sql_modes: default_sql_modes(),
            // Temporary tables
            temp_table_names: Vec::new(),
        }
    }

    /// Initialize evaluator flags from the current sql_mode.
    /// Call after creating a session to sync UserVariables with sql_modes.
    pub fn init_eval_flags(&self) {
        crate::executor::eval::set_error_for_division_by_zero(
            &self.user_variables,
            self.has_sql_mode("ERROR_FOR_DIVISION_BY_ZERO"),
        );
        crate::executor::eval::set_strict_trans_tables(
            &self.user_variables,
            self.has_sql_mode("STRICT_TRANS_TABLES"),
        );
        // Store connection_id for advisory lock ownership
        {
            use crate::executor::datum::Datum;
            let mut w = self.user_variables.write();
            w.insert(
                "__sys_connection_id".to_string(),
                Datum::Int(self.connection_id as i64),
            );
            // Store user and database for USER()/DATABASE() functions
            w.insert(
                "__sys_user".to_string(),
                Datum::String(format!("{}@localhost", self.user)),
            );
            w.insert(
                "__sys_database".to_string(),
                match &self.database {
                    Some(db) => Datum::String(db.clone()),
                    None => Datum::Null,
                },
            );
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
        // Sync to user variables for DATABASE()/SCHEMA() function
        let mut w = self.user_variables.write();
        w.insert(
            "__sys_database".to_string(),
            match &self.database {
                Some(db) => crate::executor::datum::Datum::String(db.clone()),
                None => crate::executor::datum::Datum::Null,
            },
        );
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

    // ============ Warning tracking ============

    /// Add a warning to the current statement's warning list
    pub fn add_warning(&mut self, level: &str, code: u16, message: String) {
        self.warnings.push(Warning {
            level: level.to_string(),
            code,
            message,
        });
    }

    /// Clear warnings (called at start of each statement)
    pub fn clear_warnings(&mut self) {
        self.warnings.clear();
    }

    /// Get the current warning count
    pub fn warning_count(&self) -> u16 {
        self.warnings.len() as u16
    }

    /// Get a reference to accumulated warnings
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    // ============ User variables (@var) ============

    /// Get a clone of the user variables handle (Arc-cloned, cheap)
    pub fn user_variables(&self) -> UserVariables {
        self.user_variables.clone()
    }

    /// Get a user variable value, returns Datum::Null if not set
    pub fn get_user_variable(&self, name: &str) -> Datum {
        let vars = self.user_variables.read();
        vars.get(&name.to_lowercase())
            .cloned()
            .unwrap_or(Datum::Null)
    }

    /// Set a user variable
    pub fn set_user_variable(&self, name: &str, value: Datum) {
        let mut vars = self.user_variables.write();
        vars.insert(name.to_lowercase(), value);
    }

    // ============ SQL Mode ============

    /// Set sql_mode from a comma-separated string. Empty string clears all modes.
    pub fn set_sql_mode(&mut self, mode_str: &str) {
        self.sql_modes.clear();
        let trimmed = mode_str.trim().trim_matches('\'').trim_matches('"');
        if trimmed.is_empty() {
            return;
        }
        if trimmed.eq_ignore_ascii_case("DEFAULT") {
            self.sql_modes = default_sql_modes();
            return;
        }
        for mode in trimmed.split(',') {
            let m = mode.trim().to_uppercase();
            if !m.is_empty() {
                self.sql_modes.insert(m);
            }
        }
    }

    /// Check if a specific sql_mode is active
    pub fn has_sql_mode(&self, mode: &str) -> bool {
        self.sql_modes.contains(&mode.to_uppercase())
    }

    /// Get the current sql_mode as a comma-separated string
    pub fn sql_mode_string(&self) -> String {
        let mut modes: Vec<&str> = self.sql_modes.iter().map(|s| s.as_str()).collect();
        modes.sort();
        modes.join(",")
    }

    // ============ Temporary Tables ============

    /// Register a temporary table name for cleanup on disconnect
    pub fn register_temp_table(&mut self, name: String) {
        self.temp_table_names.push(name);
    }

    /// Take all temporary table names (called on disconnect for cleanup)
    pub fn take_temp_tables(&mut self) -> Vec<String> {
        std::mem::take(&mut self.temp_table_names)
    }
}
