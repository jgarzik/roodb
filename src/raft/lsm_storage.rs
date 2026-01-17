//! LSM-backed Raft storage implementation
//!
//! Persists all Raft state (log entries, vote, membership) to LSM storage for durability.
//! This enables cluster-ready multi-node operation with proper crash recovery.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftLogId, RaftLogReader, RaftSnapshotBuilder};
use openraft::{ErrorSubject, ErrorVerb};
use parking_lot::RwLock;

use crate::catalog::system_tables::{
    is_system_table, row_to_index_def, rows_to_table_def, SYSTEM_COLUMNS, SYSTEM_INDEXES,
    SYSTEM_TABLES,
};
use crate::catalog::Catalog;
use crate::executor::encoding::{decode_row, table_key_end, table_key_prefix};
use crate::raft::changes::ChangeOp;
use crate::raft::types::{
    Command, CommandResponse, Entry, LogId, LogState, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, TypeConfig, Vote,
};
use crate::storage::StorageEngine;

/// MVCC row header size: DB_TRX_ID (8 bytes) + DB_ROLL_PTR (8 bytes) + deleted flag (1 byte)
const MVCC_HEADER_SIZE: usize = 17;

/// Encode row data with MVCC header for storage
///
/// Format: [DB_TRX_ID:8][DB_ROLL_PTR:8][deleted:1][row_data]
fn encode_mvcc_row(txn_id: u64, deleted: bool, data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(MVCC_HEADER_SIZE + data.len());
    result.extend_from_slice(&txn_id.to_le_bytes()); // DB_TRX_ID
    result.extend_from_slice(&0u64.to_le_bytes()); // DB_ROLL_PTR = 0 (no version chain)
    result.push(if deleted { 1 } else { 0 }); // deleted flag
    result.extend_from_slice(data);
    result
}

/// Extract txn_id (version) from a stored row with MVCC header
///
/// Returns None if the row doesn't exist or is too short to have a header.
fn extract_row_version(encoded: &[u8]) -> Option<u64> {
    if encoded.len() < MVCC_HEADER_SIZE {
        return None;
    }
    Some(u64::from_le_bytes(
        encoded[0..8].try_into().expect("slice is exactly 8 bytes"),
    ))
}

// Key prefixes for Raft state in LSM
const VOTE_KEY: &[u8] = b"_raft:vote";
const PURGED_KEY: &[u8] = b"_raft:log_state:purged";
const LAST_APPLIED_KEY: &[u8] = b"_raft:sm:last_applied";
const MEMBERSHIP_KEY: &[u8] = b"_raft:sm:membership";
const LOG_PREFIX: &[u8] = b"_raft:log:";

/// Create a read error
fn read_err(msg: impl ToString) -> StorageError {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Read,
        std::io::Error::other(msg.to_string()),
    )
}

/// Create a write error
fn write_err(msg: impl ToString) -> StorageError {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Write,
        std::io::Error::other(msg.to_string()),
    )
}

/// Encode a log entry key with big-endian index for correct lexicographic ordering
fn log_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(LOG_PREFIX.len() + 8);
    key.extend_from_slice(LOG_PREFIX);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

/// Extract log index from a log key
fn log_index_from_key(key: &[u8]) -> Option<u64> {
    if key.len() == LOG_PREFIX.len() + 8 && key.starts_with(LOG_PREFIX) {
        let bytes: [u8; 8] = key[LOG_PREFIX.len()..].try_into().ok()?;
        Some(u64::from_be_bytes(bytes))
    } else {
        None
    }
}

/// Compute end key for log range scan (prefix + max)
fn log_key_end() -> Vec<u8> {
    let mut key = Vec::with_capacity(LOG_PREFIX.len() + 8);
    key.extend_from_slice(LOG_PREFIX);
    key.extend_from_slice(&u64::MAX.to_be_bytes());
    key
}

/// LSM-backed Raft storage
///
/// Stores all Raft state durably in LSM:
/// - Vote: `_raft:vote`
/// - Log entries: `_raft:log:{BE_u64_index}`
/// - Last purged: `_raft:log_state:purged`
/// - Last applied: `_raft:sm:last_applied`
/// - Membership: `_raft:sm:membership`
#[derive(Clone)]
pub struct LsmRaftStorage {
    storage: Arc<dyn StorageEngine>,
    /// Catalog reference for updating schema on DDL changes
    catalog: Arc<RwLock<Catalog>>,
    // In-memory caches for hot paths
    cached_vote: Arc<RwLock<Option<Vote>>>,
    cached_last_applied: Arc<RwLock<Option<LogId>>>,
    cached_membership: Arc<RwLock<StoredMembership>>,
    cached_last_log_id: Arc<RwLock<Option<LogId>>>,
    cached_last_purged: Arc<RwLock<Option<LogId>>>,
}

impl std::fmt::Debug for LsmRaftStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmRaftStorage")
            .field("cached_vote", &*self.cached_vote.read())
            .field("cached_last_applied", &*self.cached_last_applied.read())
            .field("cached_last_log_id", &*self.cached_last_log_id.read())
            .finish()
    }
}

impl LsmRaftStorage {
    /// Create new LSM-backed Raft storage
    ///
    /// On creation, reads existing state from LSM to populate caches.
    pub async fn new(
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Result<Self, StorageError> {
        let this = Self {
            storage,
            catalog,
            cached_vote: Arc::new(RwLock::new(None)),
            cached_last_applied: Arc::new(RwLock::new(None)),
            cached_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_last_log_id: Arc::new(RwLock::new(None)),
            cached_last_purged: Arc::new(RwLock::new(None)),
        };

        // Load existing state from LSM into caches
        this.load_cached_state().await?;

        Ok(this)
    }

    /// Load all cached state from LSM
    async fn load_cached_state(&self) -> Result<(), StorageError> {
        // Load vote
        if let Some(data) = self.storage.get(VOTE_KEY).await.map_err(read_err)? {
            let vote: Vote = bincode::deserialize(&data).map_err(read_err)?;
            *self.cached_vote.write() = Some(vote);
        }

        // Load last purged
        if let Some(data) = self.storage.get(PURGED_KEY).await.map_err(read_err)? {
            let log_id: LogId = bincode::deserialize(&data).map_err(read_err)?;
            *self.cached_last_purged.write() = Some(log_id);
        }

        // Load last applied
        if let Some(data) = self.storage.get(LAST_APPLIED_KEY).await.map_err(read_err)? {
            let log_id: LogId = bincode::deserialize(&data).map_err(read_err)?;
            *self.cached_last_applied.write() = Some(log_id);
        }

        // Load membership
        if let Some(data) = self.storage.get(MEMBERSHIP_KEY).await.map_err(read_err)? {
            let membership: StoredMembership = bincode::deserialize(&data).map_err(read_err)?;
            *self.cached_membership.write() = membership;
        }

        // Scan for last log entry
        let entries = self
            .storage
            .scan(Some(LOG_PREFIX), Some(&log_key_end()))
            .await
            .map_err(read_err)?;

        if let Some((ref key, ref data)) = entries.last() {
            if let Some(_index) = log_index_from_key(key) {
                let entry: Entry = bincode::deserialize(data).map_err(read_err)?;
                *self.cached_last_log_id.write() = Some(*entry.get_log_id());
            }
        }

        tracing::info!(
            vote = ?*self.cached_vote.read(),
            last_purged = ?*self.cached_last_purged.read(),
            last_log = ?*self.cached_last_log_id.read(),
            last_applied = ?*self.cached_last_applied.read(),
            "Loaded Raft state from LSM"
        );

        Ok(())
    }

    /// Get storage engine reference (for applying data changes)
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    /// Rebuild catalog entries for tables affected by system table changes
    ///
    /// Called after apply() when system.tables/columns/indexes changes are detected.
    /// Scans storage to reconstruct TableDef/IndexDef and updates catalog.
    async fn rebuild_catalog_for_tables(
        &self,
        affected_tables: &std::collections::HashSet<String>,
        dropped_tables: &std::collections::HashSet<String>,
    ) -> Result<(), StorageError> {
        use crate::catalog::TableDef;

        // First, scan storage to collect table definitions (no lock held)
        let mut table_defs: Vec<(String, TableDef)> = Vec::new();

        for table_name in affected_tables {
            if is_system_table(table_name) || dropped_tables.contains(table_name) {
                continue;
            }

            // Scan system.columns for this table to rebuild TableDef
            let prefix = table_key_prefix(SYSTEM_COLUMNS);
            let end = table_key_end(SYSTEM_COLUMNS);
            let rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(read_err)?;

            // Collect column rows for this table (skip MVCC header)
            let mut column_rows = Vec::new();
            for (_key, value) in rows {
                // Skip MVCC header (17 bytes: 8 + 8 + 1)
                if value.len() <= MVCC_HEADER_SIZE {
                    continue;
                }
                // Check if deleted
                if value[16] == 1 {
                    continue;
                }
                let row_data = &value[MVCC_HEADER_SIZE..];
                if let Ok(row) = decode_row(row_data) {
                    // Check if this row belongs to the table we're rebuilding
                    if let Some(crate::executor::Datum::String(tbl)) = row.values().first() {
                        if tbl == table_name {
                            column_rows.push(row);
                        }
                    }
                }
            }

            if !column_rows.is_empty() {
                // Rebuild TableDef from column rows
                if let Some(table_def) = rows_to_table_def(table_name, &column_rows) {
                    tracing::debug!(table = %table_name, columns = column_rows.len(), "Rebuilding table in catalog (Raft apply)");
                    table_defs.push((table_name.clone(), table_def));
                }
            }
        }

        // Now acquire lock and update catalog (no await after this)
        let mut catalog = self.catalog.write();

        // Handle dropped tables
        for table_name in dropped_tables {
            if !is_system_table(table_name) {
                tracing::debug!(table = %table_name, "Removing table from catalog (Raft apply)");
                // Ignore error if table doesn't exist (may have been dropped already)
                let _ = catalog.drop_table(table_name);
            }
        }

        // Apply table definitions
        for (table_name, table_def) in table_defs {
            // Remove existing and recreate (handles both create and modify)
            let _ = catalog.drop_table(&table_name);
            if let Err(e) = catalog.create_table(table_def) {
                tracing::warn!(table = %table_name, error = %e, "Failed to create table in catalog");
            }
        }

        Ok(())
    }

    /// Rebuild index catalog entries for affected indexes
    async fn rebuild_catalog_for_indexes(
        &self,
        affected_indexes: &std::collections::HashSet<String>,
        dropped_indexes: &std::collections::HashSet<String>,
    ) -> Result<(), StorageError> {
        use crate::catalog::IndexDef;

        // First, scan storage to collect index definitions (no lock held)
        let mut index_defs: Vec<IndexDef> = Vec::new();

        for index_name in affected_indexes {
            if dropped_indexes.contains(index_name) {
                continue;
            }

            // Scan system.indexes to find this index
            let prefix = table_key_prefix(SYSTEM_INDEXES);
            let end = table_key_end(SYSTEM_INDEXES);
            let rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(read_err)?;

            for (_key, value) in rows {
                if value.len() <= MVCC_HEADER_SIZE {
                    continue;
                }
                if value[16] == 1 {
                    continue;
                }
                let row_data = &value[MVCC_HEADER_SIZE..];
                if let Ok(row) = decode_row(row_data) {
                    if let Some(crate::executor::Datum::String(idx_name)) = row.values().first() {
                        if idx_name == index_name {
                            if let Some(index_def) = row_to_index_def(&row) {
                                tracing::debug!(index = %index_name, "Creating index in catalog (Raft apply)");
                                index_defs.push(index_def);
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Now acquire lock and update catalog (no await after this)
        let mut catalog = self.catalog.write();

        // Handle dropped indexes
        for index_name in dropped_indexes {
            tracing::debug!(index = %index_name, "Removing index from catalog (Raft apply)");
            let _ = catalog.drop_index(index_name);
        }

        // Apply index definitions
        for index_def in index_defs {
            let _ = catalog.create_index(index_def);
        }

        Ok(())
    }

    /// Rebuild entire catalog from system tables after snapshot install
    async fn rebuild_catalog_from_system_tables(&self) -> Result<(), StorageError> {
        use crate::catalog::{IndexDef, TableDef};

        let mut table_defs: Vec<TableDef> = Vec::new();
        let mut index_defs: Vec<IndexDef> = Vec::new();

        // Scan system.columns to rebuild all tables
        let prefix = table_key_prefix(SYSTEM_COLUMNS);
        let end = table_key_end(SYSTEM_COLUMNS);
        let column_rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(read_err)?;

        // Group columns by table name
        let mut columns_by_table: std::collections::HashMap<
            String,
            Vec<crate::executor::row::Row>,
        > = std::collections::HashMap::new();

        for (_key, value) in column_rows {
            if value.len() <= MVCC_HEADER_SIZE {
                continue;
            }
            if value[16] == 1 {
                continue; // Skip deleted
            }
            let row_data = &value[MVCC_HEADER_SIZE..];
            if let Ok(row) = decode_row(row_data) {
                if let Some(crate::executor::Datum::String(table_name)) = row.values().first() {
                    if !is_system_table(table_name) {
                        columns_by_table
                            .entry(table_name.clone())
                            .or_default()
                            .push(row);
                    }
                }
            }
        }

        // Build TableDef for each table
        for (table_name, column_rows) in columns_by_table {
            if let Some(table_def) = rows_to_table_def(&table_name, &column_rows) {
                table_defs.push(table_def);
            }
        }

        // Scan system.indexes to rebuild all indexes
        let prefix = table_key_prefix(SYSTEM_INDEXES);
        let end = table_key_end(SYSTEM_INDEXES);
        let index_rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(read_err)?;

        for (_key, value) in index_rows {
            if value.len() <= MVCC_HEADER_SIZE {
                continue;
            }
            if value[16] == 1 {
                continue; // Skip deleted
            }
            let row_data = &value[MVCC_HEADER_SIZE..];
            if let Ok(row) = decode_row(row_data) {
                if let Some(index_def) = row_to_index_def(&row) {
                    index_defs.push(index_def);
                }
            }
        }

        // Update catalog (clear and rebuild)
        let mut catalog = self.catalog.write();

        // Clear existing user tables/indexes (keep system tables)
        let existing_tables: Vec<String> = catalog
            .list_tables()
            .iter()
            .filter(|t| !is_system_table(t))
            .map(|t| t.to_string())
            .collect();
        for table_name in existing_tables {
            let _ = catalog.drop_table(&table_name);
        }

        // Add tables from snapshot
        for table_def in table_defs {
            tracing::debug!(table = %table_def.name, "Rebuilding table from snapshot");
            let _ = catalog.create_table(table_def);
        }

        // Add indexes from snapshot
        for index_def in index_defs {
            tracing::debug!(index = %index_def.name, "Rebuilding index from snapshot");
            let _ = catalog.create_index(index_def);
        }

        tracing::info!("Rebuilt catalog from system tables after snapshot install");
        Ok(())
    }

    /// Helper to track system table changes for catalog rebuild
    #[allow(clippy::too_many_arguments)]
    fn track_system_table_change(
        &self,
        table: &str,
        value: &[u8],
        is_delete: bool,
        affected_tables: &mut std::collections::HashSet<String>,
        dropped_tables: &mut std::collections::HashSet<String>,
        affected_indexes: &mut std::collections::HashSet<String>,
        dropped_indexes: &mut std::collections::HashSet<String>,
    ) {
        // Only process system tables
        if table == SYSTEM_TABLES {
            // Decode row to get table_name (first column)
            if let Ok(row) = decode_row(value) {
                if let Some(crate::executor::Datum::String(table_name)) = row.values().first() {
                    if is_delete {
                        dropped_tables.insert(table_name.clone());
                    } else {
                        affected_tables.insert(table_name.clone());
                    }
                }
            }
        } else if table == SYSTEM_COLUMNS {
            // Decode row to get table_name (first column)
            if let Ok(row) = decode_row(value) {
                if let Some(crate::executor::Datum::String(table_name)) = row.values().first() {
                    // Column changes affect the table definition
                    if !is_delete {
                        affected_tables.insert(table_name.clone());
                    }
                    // Note: For column deletes during DROP TABLE, the table is already marked dropped
                }
            }
        } else if table == SYSTEM_INDEXES {
            // Decode row to get index_name (first column)
            if let Ok(row) = decode_row(value) {
                if let Some(crate::executor::Datum::String(index_name)) = row.values().first() {
                    if is_delete {
                        dropped_indexes.insert(index_name.clone());
                    } else {
                        affected_indexes.insert(index_name.clone());
                    }
                }
            }
        }
    }
}

impl RaftLogReader<TypeConfig> for LsmRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        use std::ops::Bound;

        let start_index = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&i) => i + 1,
            Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            Bound::Included(&i) => Some(i + 1),
            Bound::Excluded(&i) => Some(i),
            Bound::Unbounded => None,
        };

        let start_key = log_key(start_index);
        let end_key = end_index.map(log_key).unwrap_or_else(log_key_end);

        let entries = self
            .storage
            .scan(Some(&start_key), Some(&end_key))
            .await
            .map_err(read_err)?;

        let mut result = Vec::with_capacity(entries.len());
        for (_key, data) in entries {
            let entry: Entry = bincode::deserialize(&data).map_err(read_err)?;
            result.push(entry);
        }

        Ok(result)
    }
}

impl RaftLogStorage<TypeConfig> for LsmRaftStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState, StorageError> {
        let last_purged = *self.cached_last_purged.read();
        let last_log_id = *self.cached_last_log_id.read();

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let data = bincode::serialize(vote).map_err(write_err)?;

        self.storage.put(VOTE_KEY, &data).await.map_err(write_err)?;

        // CRITICAL: flush to disk before returning - vote durability prevents double-voting
        // Uses Critical priority to bypass backpressure and ensure durability
        self.storage.flush_critical().await.map_err(write_err)?;

        *self.cached_vote.write() = Some(*vote);

        tracing::debug!(?vote, "Saved vote to LSM");
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(*self.cached_vote.read())
    }

    async fn save_committed(&mut self, _committed: Option<LogId>) -> Result<(), StorageError> {
        // Committed index is tracked implicitly by last_applied
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut last_log_id = None;

        for entry in entries {
            let log_id = *entry.get_log_id();
            let key = log_key(log_id.index);
            let data = bincode::serialize(&entry).map_err(write_err)?;

            self.storage.put(&key, &data).await.map_err(write_err)?;

            last_log_id = Some(log_id);
        }

        if let Some(log_id) = last_log_id {
            *self.cached_last_log_id.write() = Some(log_id);
        }

        // Notify that log is persisted (in memtable, will be flushed with next flush cycle)
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Delete all entries with index >= log_id.index
        let start_key = log_key(log_id.index);
        let end_key = log_key_end();

        let entries = self
            .storage
            .scan(Some(&start_key), Some(&end_key))
            .await
            .map_err(read_err)?;

        for (key, _) in entries {
            self.storage.delete(&key).await.map_err(write_err)?;
        }

        // Update cached last log id by scanning for new max
        let entries = self
            .storage
            .scan(Some(LOG_PREFIX), Some(&start_key))
            .await
            .map_err(read_err)?;

        if let Some((ref _key, ref data)) = entries.last() {
            let entry: Entry = bincode::deserialize(data).map_err(read_err)?;
            *self.cached_last_log_id.write() = Some(*entry.get_log_id());
        } else {
            *self.cached_last_log_id.write() = *self.cached_last_purged.read();
        }

        tracing::debug!(?log_id, "Truncated log");
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Delete all entries with index <= log_id.index
        let end_key = log_key(log_id.index + 1);

        let entries = self
            .storage
            .scan(Some(LOG_PREFIX), Some(&end_key))
            .await
            .map_err(read_err)?;

        for (key, _) in entries {
            self.storage.delete(&key).await.map_err(write_err)?;
        }

        // Persist last purged
        let data = bincode::serialize(&log_id).map_err(write_err)?;
        self.storage
            .put(PURGED_KEY, &data)
            .await
            .map_err(write_err)?;

        *self.cached_last_purged.write() = Some(log_id);

        tracing::debug!(?log_id, "Purged log entries");
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for LsmRaftStorage {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let last_applied = *self.cached_last_applied.read();
        let membership = self.cached_membership.read().clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        use std::collections::HashSet;

        let mut results = Vec::new();

        // Track system table changes for catalog updates
        let mut affected_tables: HashSet<String> = HashSet::new();
        let mut dropped_tables: HashSet<String> = HashSet::new();
        let mut affected_indexes: HashSet<String> = HashSet::new();
        let mut dropped_indexes: HashSet<String> = HashSet::new();

        for entry in entries {
            let log_id = *entry.get_log_id();

            match &entry.payload {
                EntryPayload::Blank => {
                    results.push(CommandResponse::Ok(None));
                }
                EntryPayload::Normal(cmd) => {
                    let resp = match cmd {
                        Command::DataChange(changeset) => {
                            // Apply each row change to storage with MVCC headers.
                            // This is the unified write path for both leader and follower:
                            // - Leader: collected changes during execution, now persisting after Raft commit
                            // - Follower: received changes via AppendEntries, now persisting locally
                            let mut conflict_error: Option<String> = None;

                            for change in &changeset.changes {
                                // OCC version check for UPDATE/DELETE operations
                                // If expected_version is set, verify the row hasn't been modified
                                if let Some(expected_version) = change.expected_version {
                                    if let Ok(Some(current_data)) =
                                        self.storage.get(&change.key).await
                                    {
                                        let current_version =
                                            extract_row_version(&current_data).unwrap_or(0);
                                        if current_version != expected_version {
                                            // Conflict detected - row was modified by another transaction
                                            tracing::warn!(
                                                table = %change.table,
                                                expected = expected_version,
                                                actual = current_version,
                                                "OCC conflict: row was modified by another transaction"
                                            );
                                            conflict_error = Some(format!(
                                                "Write conflict: row in '{}' was modified by another transaction (expected version {}, found {})",
                                                change.table, expected_version, current_version
                                            ));
                                            break;
                                        }
                                    }
                                    // If row doesn't exist anymore, that's also a conflict for UPDATE
                                    else if change.op == ChangeOp::Update {
                                        tracing::warn!(
                                            table = %change.table,
                                            "OCC conflict: row no longer exists"
                                        );
                                        conflict_error = Some(format!(
                                            "Write conflict: row in '{}' no longer exists",
                                            change.table
                                        ));
                                        break;
                                    }
                                    // For DELETE, if row doesn't exist, that's ok (idempotent)
                                }

                                match change.op {
                                    ChangeOp::Insert | ChangeOp::Update => {
                                        // Encode with MVCC header and write
                                        if let Some(ref value) = change.value {
                                            // Check for duplicate table creation (race condition fix)
                                            // This makes apply() idempotent for CREATE TABLE
                                            if change.table == SYSTEM_TABLES
                                                && change.op == ChangeOp::Insert
                                            {
                                                if let Ok(row) = decode_row(value) {
                                                    if let Some(crate::executor::Datum::String(
                                                        table_name,
                                                    )) = row.values().first()
                                                    {
                                                        let catalog = self.catalog.read();
                                                        if catalog.get_table(table_name).is_some() {
                                                            // Table already exists - skip this insert
                                                            // This handles the race condition where two
                                                            // CREATE TABLE proposals both passed local
                                                            // checks but Raft serializes them here
                                                            tracing::debug!(
                                                                table = %table_name,
                                                                "Skipping duplicate CREATE TABLE in apply()"
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }

                                            // Check for duplicate index creation
                                            if change.table == SYSTEM_INDEXES
                                                && change.op == ChangeOp::Insert
                                            {
                                                if let Ok(row) = decode_row(value) {
                                                    if let Some(crate::executor::Datum::String(
                                                        index_name,
                                                    )) = row.values().first()
                                                    {
                                                        let catalog = self.catalog.read();
                                                        if catalog.get_index(index_name).is_some() {
                                                            tracing::debug!(
                                                                index = %index_name,
                                                                "Skipping duplicate CREATE INDEX in apply()"
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }

                                            let encoded =
                                                encode_mvcc_row(changeset.txn_id, false, value);
                                            self.storage
                                                .put(&change.key, &encoded)
                                                .await
                                                .map_err(write_err)?;

                                            // Track system table changes for catalog rebuild
                                            self.track_system_table_change(
                                                &change.table,
                                                value,
                                                false, // not a delete
                                                &mut affected_tables,
                                                &mut dropped_tables,
                                                &mut affected_indexes,
                                                &mut dropped_indexes,
                                            );
                                        }
                                    }
                                    ChangeOp::Delete => {
                                        // Write tombstone (deleted=true, empty data)
                                        let encoded = encode_mvcc_row(changeset.txn_id, true, &[]);
                                        self.storage
                                            .put(&change.key, &encoded)
                                            .await
                                            .map_err(write_err)?;

                                        // Track system table deletions
                                        // For deletes, we need to read the existing value to find table name
                                        if let Some(ref value) = change.value {
                                            self.track_system_table_change(
                                                &change.table,
                                                value,
                                                true, // is a delete
                                                &mut affected_tables,
                                                &mut dropped_tables,
                                                &mut affected_indexes,
                                                &mut dropped_indexes,
                                            );
                                        }
                                    }
                                }
                            }

                            // Return conflict error or success
                            if let Some(err_msg) = conflict_error {
                                tracing::debug!(
                                    txn_id = changeset.txn_id,
                                    "RAFT APPLY: OCC conflict detected"
                                );
                                CommandResponse::Error(err_msg)
                            } else {
                                tracing::debug!(
                                    txn_id = changeset.txn_id,
                                    changes = changeset.changes.len(),
                                    "RAFT APPLY: wrote {} changes to storage",
                                    changeset.changes.len()
                                );
                                CommandResponse::Ok(None)
                            }
                        }
                        Command::Noop => CommandResponse::Ok(None),
                    };
                    results.push(resp);
                }
                EntryPayload::Membership(ref m) => {
                    let membership = StoredMembership::new(Some(log_id), m.clone());

                    // Persist membership
                    let data = bincode::serialize(&membership).map_err(write_err)?;
                    self.storage
                        .put(MEMBERSHIP_KEY, &data)
                        .await
                        .map_err(write_err)?;

                    *self.cached_membership.write() = membership;
                    results.push(CommandResponse::Ok(None));
                }
            }

            // Persist last applied
            let data = bincode::serialize(&log_id).map_err(write_err)?;
            self.storage
                .put(LAST_APPLIED_KEY, &data)
                .await
                .map_err(write_err)?;
            *self.cached_last_applied.write() = Some(log_id);
        }

        // Rebuild catalog for affected system table entries
        if !affected_tables.is_empty() || !dropped_tables.is_empty() {
            self.rebuild_catalog_for_tables(&affected_tables, &dropped_tables)
                .await?;
        }
        if !affected_indexes.is_empty() || !dropped_indexes.is_empty() {
            self.rebuild_catalog_for_indexes(&affected_indexes, &dropped_indexes)
                .await?;
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let snapshot_data = snapshot.into_inner();

        // Parse snapshot data if present
        if !snapshot_data.is_empty() {
            // Delete existing user data (preserve Raft metadata)
            let existing_data = self.storage.scan(None, None).await.map_err(read_err)?;
            let mut deleted_count = 0;
            for (key, _) in existing_data {
                if !key.starts_with(RAFT_PREFIX) {
                    self.storage.delete(&key).await.map_err(write_err)?;
                    deleted_count += 1;
                }
            }

            // Parse snapshot: [count: u64][entries...]
            if snapshot_data.len() < 8 {
                return Err(read_err("Invalid snapshot: too short"));
            }
            let count = u64::from_le_bytes(
                snapshot_data[..8]
                    .try_into()
                    .map_err(|_| read_err("Invalid snapshot: count parse error"))?,
            ) as usize;
            let mut offset = 8;

            // Install each entry
            let mut installed_count = 0;
            for _ in 0..count {
                // [key_len: u32][key][value_len: u32][value]
                if offset + 4 > snapshot_data.len() {
                    return Err(read_err("Invalid snapshot: truncated key length"));
                }
                let key_len = u32::from_le_bytes(
                    snapshot_data[offset..offset + 4]
                        .try_into()
                        .map_err(|_| read_err("Invalid snapshot: key length parse error"))?,
                ) as usize;
                offset += 4;

                if offset + key_len > snapshot_data.len() {
                    return Err(read_err("Invalid snapshot: truncated key"));
                }
                let key = &snapshot_data[offset..offset + key_len];
                offset += key_len;

                if offset + 4 > snapshot_data.len() {
                    return Err(read_err("Invalid snapshot: truncated value length"));
                }
                let value_len = u32::from_le_bytes(
                    snapshot_data[offset..offset + 4]
                        .try_into()
                        .map_err(|_| read_err("Invalid snapshot: value length parse error"))?,
                ) as usize;
                offset += 4;

                if offset + value_len > snapshot_data.len() {
                    return Err(read_err("Invalid snapshot: truncated value"));
                }
                let value = &snapshot_data[offset..offset + value_len];
                offset += value_len;

                self.storage.put(key, value).await.map_err(write_err)?;
                installed_count += 1;
            }

            tracing::info!(
                ?meta,
                deleted = deleted_count,
                installed = installed_count,
                bytes = snapshot_data.len(),
                "Installed snapshot data"
            );
        }

        // Update last applied from snapshot metadata
        if let Some(log_id) = meta.last_log_id {
            let data = bincode::serialize(&log_id).map_err(write_err)?;
            self.storage
                .put(LAST_APPLIED_KEY, &data)
                .await
                .map_err(write_err)?;
            *self.cached_last_applied.write() = Some(log_id);
        }

        // Update membership from snapshot
        let data = bincode::serialize(&meta.last_membership).map_err(write_err)?;
        self.storage
            .put(MEMBERSHIP_KEY, &data)
            .await
            .map_err(write_err)?;
        *self.cached_membership.write() = meta.last_membership.clone();

        // Rebuild catalog from system tables after snapshot install
        self.rebuild_catalog_from_system_tables().await?;

        // Flush to ensure all snapshot data is persisted
        self.storage.flush().await.map_err(write_err)?;

        tracing::info!(?meta, "Installed snapshot");
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let last_applied = *self.cached_last_applied.read();
        let membership = self.cached_membership.read().clone();

        // Scan all user data (exclude Raft metadata keys)
        let all_data = self.storage.scan(None, None).await.map_err(read_err)?;
        let user_data: Vec<_> = all_data
            .into_iter()
            .filter(|(key, _)| !key.starts_with(RAFT_PREFIX))
            .collect();

        // Serialize snapshot data: [count: u64][entries...]
        let mut snapshot_data = Vec::new();
        snapshot_data.extend_from_slice(&(user_data.len() as u64).to_le_bytes());

        for (key, value) in &user_data {
            snapshot_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            snapshot_data.extend_from_slice(key);
            snapshot_data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            snapshot_data.extend_from_slice(value);
        }

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: format!(
                "{}-lsm-{}",
                last_applied.map(|l| l.to_string()).unwrap_or_default(),
                user_data.len()
            ),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        }))
    }
}

/// Raft metadata key prefix - excluded from snapshots
const RAFT_PREFIX: &[u8] = b"_raft:";

impl RaftSnapshotBuilder<TypeConfig> for LsmRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let last_applied = *self.cached_last_applied.read();
        let membership = self.cached_membership.read().clone();

        // Flush storage to ensure all data is persisted
        self.storage.flush().await.map_err(write_err)?;

        // Scan all user data (exclude Raft metadata keys)
        let all_data = self.storage.scan(None, None).await.map_err(read_err)?;
        let user_data: Vec<_> = all_data
            .into_iter()
            .filter(|(key, _)| !key.starts_with(RAFT_PREFIX))
            .collect();

        // Serialize snapshot data: [count: u64][entries...]
        let mut snapshot_data = Vec::new();
        snapshot_data.extend_from_slice(&(user_data.len() as u64).to_le_bytes());

        for (key, value) in &user_data {
            // [key_len: u32][key][value_len: u32][value]
            snapshot_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            snapshot_data.extend_from_slice(key);
            snapshot_data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            snapshot_data.extend_from_slice(value);
        }

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: format!(
                "{}-lsm-{}",
                last_applied.map(|l| l.to_string()).unwrap_or_default(),
                user_data.len()
            ),
        };

        tracing::info!(
            ?meta,
            entries = user_data.len(),
            bytes = snapshot_data.len(),
            "Built snapshot"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_key_encoding() {
        let key = log_key(12345);
        assert!(key.starts_with(LOG_PREFIX));
        assert_eq!(log_index_from_key(&key), Some(12345));
    }

    #[test]
    fn test_log_key_ordering() {
        // Verify big-endian encoding gives correct lexicographic order
        let key1 = log_key(1);
        let key2 = log_key(2);
        let key100 = log_key(100);
        let key1000 = log_key(1000);

        assert!(key1 < key2);
        assert!(key2 < key100);
        assert!(key100 < key1000);
    }
}
