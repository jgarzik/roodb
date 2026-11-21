//! LSM-backed Raft storage implementation
//!
//! Persists all Raft state (log entries, vote, membership) to LSM storage for durability.
//! This enables cluster-ready multi-node operation with proper crash recovery.

// openraft dictates these error types; they are large by its design, and the
// functions here feed directly into openraft trait methods that return them.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::Arc;

use futures_util::{Stream, TryStreamExt};
use openraft::entry::RaftEntry;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use parking_lot::RwLock;

use crate::catalog::system_tables::{
    is_system_table, row_to_index_def, row_to_procedure_def, row_to_view_def, rows_to_constraints,
    rows_to_table_def, SYSTEM_COLUMNS, SYSTEM_CONSTRAINTS, SYSTEM_DATABASES, SYSTEM_INDEXES,
    SYSTEM_PROCEDURES, SYSTEM_TABLES, SYSTEM_VIEWS,
};
use crate::catalog::Catalog;
use crate::executor::encoding::{decode_row, table_key_end, table_key_prefix};
use crate::raft::changes::{ChangeOp, ChangeSet};
use crate::raft::types::{
    Command, CommandResponse, LogId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership,
    TypeConfig,
};
use crate::storage::traits::WriteOp;
use crate::storage::StorageEngine;

use super::types::{is_mvcc_tombstone, MVCC_HEADER_SIZE};

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

/// Extract row data from a value that may be MVCC-wrapped or raw.
/// Returns None if the row is deleted. Handles both MVCC-wrapped rows
/// (from normal operations) and raw rows (from roodb_init).
fn extract_row_data(value: &[u8]) -> Option<&[u8]> {
    if value.len() > MVCC_HEADER_SIZE {
        // MVCC-wrapped: check deleted flag
        if is_mvcc_tombstone(value) {
            return None; // deleted
        }
        Some(&value[MVCC_HEADER_SIZE..])
    } else {
        // Raw row (from roodb_init, no MVCC header)
        Some(value)
    }
}

// Key prefixes for Raft state in LSM
const LAST_APPLIED_KEY: &[u8] = b"_raft:sm:last_applied";
const MEMBERSHIP_KEY: &[u8] = b"_raft:sm:membership";

/// Storage methods report plain I/O errors; openraft adds the context.
type StorageError = std::io::Error;

/// Create a read error
fn read_err(msg: impl ToString) -> StorageError {
    std::io::Error::other(format!("raft storage read: {}", msg.to_string()))
}

/// Create a write error
fn write_err(msg: impl ToString) -> StorageError {
    std::io::Error::other(format!("raft storage write: {}", msg.to_string()))
}

/// The Raft state machine: applies committed log entries to the LSM.
///
/// The log itself lives in the [`LogStore`](crate::raft::LogStore). What is
/// kept here, in the LSM alongside the data, is the state machine's own
/// position:
/// - Last applied: `_raft:sm:last_applied`
/// - Membership: `_raft:sm:membership`
///
/// # Locking
///
/// - `catalog` - Schema metadata. Acquired briefly during apply() for DDL
///   changes, never held during storage I/O.
/// - `cached_*` - In-memory copies of the state machine's position, updated
///   after the storage write. Never held across await points.
#[derive(Clone)]
pub struct LsmRaftStorage {
    storage: Arc<dyn StorageEngine>,
    /// Catalog reference for updating schema on DDL changes.
    catalog: Arc<RwLock<Catalog>>,
    cached_last_applied: Arc<RwLock<Option<LogId>>>,
    cached_membership: Arc<RwLock<StoredMembership>>,
    /// No other node will ever ask this one for a snapshot's contents.
    single_node: bool,
}

/// A change to a system table, noted so the catalog can follow once the
/// change has been written.
struct TrackedChange {
    table: String,
    value: Vec<u8>,
    is_delete: bool,
}

/// What an accepted changeset does.
#[derive(Default)]
struct ChangesetPlan {
    writes: Vec<WriteOp>,
    tracked: Vec<TrackedChange>,
    /// Tables and indexes this changeset creates.
    new_tables: Vec<String>,
    new_indexes: Vec<String>,
}

impl std::fmt::Debug for LsmRaftStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmRaftStorage")
            .field("cached_last_applied", &*self.cached_last_applied.read())
            .finish()
    }
}

impl LsmRaftStorage {
    /// Create the state machine over a storage engine.
    ///
    /// Reads its position from the LSM to populate the caches.
    pub async fn new(
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        single_node: bool,
    ) -> Result<Self, StorageError> {
        let this = Self {
            storage,
            catalog,
            cached_last_applied: Arc::new(RwLock::new(None)),
            cached_membership: Arc::new(RwLock::new(StoredMembership::default())),
            single_node,
        };

        // Load existing state from LSM into caches
        this.load_cached_state().await?;

        Ok(this)
    }

    /// The state machine's position as flushed to disk: what a restart would
    /// resume from. `None` if nothing has been flushed yet.
    async fn durable_position(&self) -> Result<Option<SnapshotMeta>, StorageError> {
        let Some(data) = self
            .storage
            .get_durable(LAST_APPLIED_KEY)
            .await
            .map_err(read_err)?
        else {
            return Ok(None);
        };
        let last_applied: LogId = bincode::deserialize(&data).map_err(read_err)?;
        let membership = match self
            .storage
            .get_durable(MEMBERSHIP_KEY)
            .await
            .map_err(read_err)?
        {
            Some(data) => bincode::deserialize(&data).map_err(read_err)?,
            None => StoredMembership::default(),
        };
        Ok(Some(SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: membership,
        }))
    }

    /// Load all cached state from LSM
    async fn load_cached_state(&self) -> Result<(), StorageError> {
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

        tracing::info!(
            last_applied = ?*self.cached_last_applied.read(),
            "Loaded state machine position from LSM"
        );

        // Load databases from system.databases into catalog
        // These may have been written by roodb_init (without MVCC headers)
        // or via Raft apply (with MVCC headers). Handle both.
        self.load_databases_into_catalog().await?;

        Ok(())
    }

    /// Load database names from system.databases into the catalog
    ///
    /// Handles both raw rows (from roodb_init) and MVCC-wrapped rows (from Raft apply).
    async fn load_databases_into_catalog(&self) -> Result<(), StorageError> {
        let prefix = table_key_prefix(SYSTEM_DATABASES);
        let end = table_key_end(SYSTEM_DATABASES);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(read_err)?;

        let mut catalog = self.catalog.write();
        for (_key, value) in rows {
            let Some(row_data) = extract_row_data(&value) else {
                continue;
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(crate::executor::Datum::String(db_name)) = row.values().first() {
                    catalog.register_database(db_name.clone());
                }
            }
        }

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

            // Collect column rows for this table
            let mut column_rows = Vec::new();
            for (_key, value) in rows {
                let Some(row_data) = extract_row_data(&value) else {
                    continue;
                };
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
                if let Some(mut table_def) = rows_to_table_def(table_name, &column_rows) {
                    // Scan for constraints for this table
                    let prefix = table_key_prefix(SYSTEM_CONSTRAINTS);
                    let end = table_key_end(SYSTEM_CONSTRAINTS);
                    let constraint_data = self
                        .storage
                        .scan(Some(&prefix), Some(&end))
                        .await
                        .map_err(read_err)?;

                    let mut constraint_rows = Vec::new();
                    for (_key, value) in constraint_data {
                        let Some(row_data) = extract_row_data(&value) else {
                            continue;
                        };
                        if let Ok(row) = decode_row(row_data) {
                            if let Some(crate::executor::Datum::String(tbl)) = row.values().first()
                            {
                                if tbl == table_name {
                                    constraint_rows.push(row);
                                }
                            }
                        }
                    }

                    // Attach constraints to table def
                    let constraints = rows_to_constraints(&constraint_rows);
                    for constraint in constraints {
                        table_def = table_def.constraint(constraint);
                    }

                    tracing::debug!(table = %table_name, columns = column_rows.len(), constraints = constraint_rows.len(), "Rebuilding table in catalog (Raft apply)");
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
                let Some(row_data) = extract_row_data(&value) else {
                    continue;
                };
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
            let Some(row_data) = extract_row_data(&value) else {
                continue;
            };
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

        // Scan system.constraints to rebuild constraints
        let prefix = table_key_prefix(SYSTEM_CONSTRAINTS);
        let end = table_key_end(SYSTEM_CONSTRAINTS);
        let constraint_rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(read_err)?;

        // Group constraints by table name
        let mut constraints_by_table: std::collections::HashMap<
            String,
            Vec<crate::executor::row::Row>,
        > = std::collections::HashMap::new();

        for (_key, value) in constraint_rows {
            let Some(row_data) = extract_row_data(&value) else {
                continue;
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(crate::executor::Datum::String(table_name)) = row.values().first() {
                    if !is_system_table(table_name) {
                        constraints_by_table
                            .entry(table_name.clone())
                            .or_default()
                            .push(row);
                    }
                }
            }
        }

        // Build TableDef for each table (with constraints)
        for (table_name, column_rows) in columns_by_table {
            if let Some(mut table_def) = rows_to_table_def(&table_name, &column_rows) {
                // Attach constraints if any
                if let Some(constraint_rows) = constraints_by_table.get(&table_name) {
                    let constraints = rows_to_constraints(constraint_rows);
                    for constraint in constraints {
                        table_def = table_def.constraint(constraint);
                    }
                }
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
            let Some(row_data) = extract_row_data(&value) else {
                continue;
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(index_def) = row_to_index_def(&row) {
                    index_defs.push(index_def);
                }
            }
        }

        // Scan system.databases to rebuild database list
        let mut database_names: Vec<String> = Vec::new();
        {
            let prefix = table_key_prefix(SYSTEM_DATABASES);
            let end = table_key_end(SYSTEM_DATABASES);
            let db_rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(read_err)?;

            for (_key, value) in db_rows {
                let Some(row_data) = extract_row_data(&value) else {
                    continue;
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(crate::executor::Datum::String(db_name)) = row.values().first() {
                        database_names.push(db_name.clone());
                    }
                }
            }
        }

        // Scan system.views to rebuild views
        let mut view_defs: Vec<crate::catalog::ViewDef> = Vec::new();
        {
            let prefix = table_key_prefix(SYSTEM_VIEWS);
            let end = table_key_end(SYSTEM_VIEWS);
            let view_rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(read_err)?;

            for (_key, value) in view_rows {
                let Some(row_data) = extract_row_data(&value) else {
                    continue;
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(view_def) = row_to_view_def(&row) {
                        view_defs.push(view_def);
                    }
                }
            }
        }

        // Scan system.procedures to rebuild stored procedures
        let mut procedure_defs: Vec<crate::catalog::ProcedureDef> = Vec::new();
        {
            let prefix = table_key_prefix(SYSTEM_PROCEDURES);
            let end = table_key_end(SYSTEM_PROCEDURES);
            let proc_rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(read_err)?;

            for (_key, value) in proc_rows {
                let Some(row_data) = extract_row_data(&value) else {
                    continue;
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(proc_def) = row_to_procedure_def(&row) {
                        procedure_defs.push(proc_def);
                    }
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

        // Rebuild databases
        catalog.clear_user_databases();
        for db_name in database_names {
            catalog.register_database(db_name);
        }

        // Rebuild procedures
        catalog.clear_procedures();
        for proc_def in procedure_defs {
            tracing::debug!(procedure = %proc_def.name, "Rebuilding procedure from snapshot");
            catalog.register_procedure(proc_def);
        }

        // Rebuild views
        catalog.clear_views();
        for view_def in view_defs {
            tracing::debug!(view = %view_def.name, "Rebuilding view from snapshot");
            catalog.register_view(view_def);
        }

        tracing::info!("Rebuilt catalog from system tables after snapshot install");
        Ok(())
    }

    /// Work out what a changeset writes, without writing anything.
    ///
    /// Every check runs first: the version checks against what is stored now,
    /// then the duplicate-key checks, which also see the changeset's own
    /// earlier changes (a DELETE followed by an INSERT of the same key is not
    /// a duplicate). If any check fails the changeset writes nothing at all;
    /// the inner `Err` carries the message for the client.
    async fn plan_changeset(
        &self,
        changeset: &ChangeSet,
        existing_tables: &std::collections::HashSet<String>,
        existing_indexes: &std::collections::HashSet<String>,
    ) -> Result<Result<ChangesetPlan, String>, StorageError> {
        // Version checks see storage as it was before this changeset, which
        // is what the executors saw when they read the rows.
        for change in &changeset.changes {
            let Some(expected_version) = change.expected_version else {
                continue;
            };
            match self.storage.get(&change.key).await.map_err(read_err)? {
                Some(current) => {
                    let current_version = extract_row_version(&current).unwrap_or(0);
                    if current_version != expected_version {
                        tracing::warn!(
                            table = %change.table,
                            expected = expected_version,
                            actual = current_version,
                            "OCC conflict: row was modified by another transaction"
                        );
                        return Ok(Err(format!(
                            "Write conflict: row in '{}' was modified by another transaction (expected version {}, found {})",
                            change.table, expected_version, current_version
                        )));
                    }
                }
                // A row that is already gone is fine to delete, not to update.
                None if change.op == ChangeOp::Update => {
                    tracing::warn!(table = %change.table, "OCC conflict: row no longer exists");
                    return Ok(Err(format!(
                        "Write conflict: row in '{}' no longer exists",
                        change.table
                    )));
                }
                None => {}
            }
        }

        let mut plan = ChangesetPlan::default();
        // Whether each key touched so far holds a live row after those changes.
        let mut live: std::collections::HashMap<&[u8], bool> = std::collections::HashMap::new();
        for change in &changeset.changes {
            match (&change.op, &change.value) {
                (ChangeOp::Insert | ChangeOp::Update, Some(value)) => {
                    if change.op == ChangeOp::Insert && !is_system_table(&change.table) {
                        let exists = match live.get(change.key.as_slice()) {
                            Some(live) => *live,
                            None => self
                                .storage
                                .get(&change.key)
                                .await
                                .map_err(read_err)?
                                .is_some_and(|row| !is_mvcc_tombstone(&row)),
                        };
                        if exists {
                            if changeset.ignore_duplicates {
                                // INSERT IGNORE: skip this row.
                                continue;
                            }
                            tracing::warn!(
                                table = %change.table,
                                "INSERT conflict: row already exists with this key"
                            );
                            return Ok(Err(format!(
                                "Duplicate key: row in '{}' already exists",
                                change.table
                            )));
                        }
                    }

                    // Two sessions can race to create the same table or
                    // index; the second creation is dropped.
                    if change.op == ChangeOp::Insert
                        && (change.table == SYSTEM_TABLES || change.table == SYSTEM_INDEXES)
                    {
                        let name =
                            decode_row(value)
                                .ok()
                                .and_then(|row| match row.values().first() {
                                    Some(crate::executor::Datum::String(name)) => {
                                        Some(name.clone())
                                    }
                                    _ => None,
                                });
                        if let Some(name) = name {
                            let (existing, new) = if change.table == SYSTEM_TABLES {
                                (existing_tables, &mut plan.new_tables)
                            } else {
                                (existing_indexes, &mut plan.new_indexes)
                            };
                            if existing.contains(&name) || new.contains(&name) {
                                tracing::debug!(
                                    table = %change.table,
                                    name = %name,
                                    "Skipping duplicate CREATE in apply()"
                                );
                                continue;
                            }
                            new.push(name);
                        }
                    }

                    plan.writes.push((
                        change.key.clone(),
                        Some(encode_mvcc_row(changeset.txn_id, false, value)),
                    ));
                    live.insert(change.key.as_slice(), true);
                    plan.tracked.push(TrackedChange {
                        table: change.table.clone(),
                        value: value.clone(),
                        is_delete: false,
                    });
                }
                // An insert or update with no value changes nothing.
                (ChangeOp::Insert | ChangeOp::Update, None) => {}
                (ChangeOp::Delete, value) => {
                    plan.writes.push((
                        change.key.clone(),
                        Some(encode_mvcc_row(changeset.txn_id, true, &[])),
                    ));
                    live.insert(change.key.as_slice(), false);
                    if let Some(value) = value {
                        plan.tracked.push(TrackedChange {
                            table: change.table.clone(),
                            value: value.clone(),
                            is_delete: true,
                        });
                    }
                }
            }
        }
        Ok(Ok(plan))
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
        } else if table == SYSTEM_DATABASES {
            // Database name changes — update catalog directly
            if let Ok(row) = decode_row(value) {
                if let Some(crate::executor::Datum::String(db_name)) = row.values().first() {
                    let mut catalog = self.catalog.write();
                    if is_delete {
                        let _ = catalog.drop_database(db_name);
                    } else {
                        catalog.register_database(db_name.clone());
                    }
                }
            }
        } else if table == SYSTEM_PROCEDURES {
            // Procedure changes — update catalog directly
            if let Ok(row) = decode_row(value) {
                if let Some(proc_def) = row_to_procedure_def(&row) {
                    let mut catalog = self.catalog.write();
                    if is_delete {
                        let _ = catalog.drop_procedure(&proc_def.name);
                    } else {
                        catalog.register_procedure(proc_def);
                    }
                }
            }
        } else if table == SYSTEM_VIEWS {
            // View changes — update catalog directly
            if let Ok(row) = decode_row(value) {
                if let Some(view_def) = row_to_view_def(&row) {
                    let mut catalog = self.catalog.write();
                    if is_delete {
                        let _ = catalog.drop_view(&view_def.name, true);
                    } else {
                        catalog.replace_view(view_def);
                    }
                }
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for LsmRaftStorage {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let last_applied = *self.cached_last_applied.read();
        let membership = self.cached_membership.read().clone();
        Ok((last_applied, membership))
    }

    async fn apply<Strm>(&mut self, mut stream: Strm) -> Result<(), StorageError>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, std::io::Error>>
            + Unpin
            + OptionalSend,
    {
        use std::collections::HashSet;

        // Take everything openraft has ready; each entry comes with the
        // channel its client is waiting on (none on followers).
        let mut entries = Vec::new();
        let mut responders = Vec::new();
        while let Some((entry, responder)) = stream.try_next().await? {
            entries.push(entry);
            responders.push(responder);
        }

        let start = std::time::Instant::now();
        let mut results = Vec::new();

        // Track system table changes for catalog updates
        let mut affected_tables: HashSet<String> = HashSet::new();
        let mut dropped_tables: HashSet<String> = HashSet::new();
        let mut affected_indexes: HashSet<String> = HashSet::new();
        let mut dropped_indexes: HashSet<String> = HashSet::new();

        // PERFORMANCE: Snapshot existing table/index names once before the loop.
        // This avoids repeated catalog lock acquisitions for duplicate DDL detection.
        // We'll add newly created names to these sets as we process them.
        let (mut existing_tables, mut existing_indexes) = {
            let catalog = self.catalog.read();
            (catalog.table_names(), catalog.index_names())
        };

        for entry in entries {
            let log_id = entry.log_id();

            // Everything an entry changes goes to storage as one batch,
            // together with the entry's position. Storage keeps a batch whole,
            // so what is on disk is always the result of applying the log up
            // to exactly the position recorded with it - which is what lets a
            // restart resume from that position by replaying the log.
            let mut batch: Vec<WriteOp> = Vec::new();
            let mut tracked: Vec<TrackedChange> = Vec::new();
            let mut membership_change = None;

            let response = match &entry.payload {
                EntryPayload::Blank => CommandResponse::Ok(None),
                EntryPayload::Normal(Command::Noop) => CommandResponse::Ok(None),
                EntryPayload::Normal(Command::DataChange(changeset)) => {
                    match self
                        .plan_changeset(changeset, &existing_tables, &existing_indexes)
                        .await?
                    {
                        Ok(plan) => {
                            tracing::debug!(
                                txn_id = changeset.txn_id,
                                changes = changeset.changes.len(),
                                writes = plan.writes.len(),
                                "RAFT APPLY: changeset accepted"
                            );
                            batch = plan.writes;
                            tracked = plan.tracked;
                            existing_tables.extend(plan.new_tables);
                            existing_indexes.extend(plan.new_indexes);
                            CommandResponse::Ok(None)
                        }
                        Err(message) => {
                            tracing::debug!(
                                txn_id = changeset.txn_id,
                                error = %message,
                                "RAFT APPLY: changeset rejected, nothing written"
                            );
                            CommandResponse::Error(message)
                        }
                    }
                }
                EntryPayload::Membership(m) => {
                    let membership = StoredMembership::new(Some(log_id), m.clone());
                    let data = bincode::serialize(&membership).map_err(write_err)?;
                    batch.push((MEMBERSHIP_KEY.to_vec(), Some(data)));
                    membership_change = Some(membership);
                    CommandResponse::Ok(None)
                }
            };

            let position = bincode::serialize(&log_id).map_err(write_err)?;
            batch.push((LAST_APPLIED_KEY.to_vec(), Some(position)));
            self.storage.write_batch(batch).await.map_err(write_err)?;

            // Memory follows storage.
            *self.cached_last_applied.write() = Some(log_id);
            if let Some(membership) = membership_change {
                *self.cached_membership.write() = membership;
            }
            for change in tracked {
                self.track_system_table_change(
                    &change.table,
                    &change.value,
                    change.is_delete,
                    &mut affected_tables,
                    &mut dropped_tables,
                    &mut affected_indexes,
                    &mut dropped_indexes,
                );
            }
            results.push(response);
        }

        tracing::debug!(target: "roodb::perf", elapsed_us = start.elapsed().as_micros() as u64, entries = results.len(), "raft_apply");

        // Rebuild catalog for affected system table entries
        if !affected_tables.is_empty() || !dropped_tables.is_empty() {
            self.rebuild_catalog_for_tables(&affected_tables, &dropped_tables)
                .await?;
        }
        if !affected_indexes.is_empty() || !dropped_indexes.is_empty() {
            self.rebuild_catalog_for_indexes(&affected_indexes, &dropped_indexes)
                .await?;
        }

        for (response, responder) in results.into_iter().zip(responders) {
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: SnapshotData,
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
        if self.single_node {
            // What has been flushed is a snapshot: it is the state at the
            // position flushed with it. Nobody will ask for its contents, so
            // none are produced.
            let Some(meta) = self.durable_position().await? else {
                return Ok(None);
            };
            return Ok(Some(Snapshot {
                meta,
                snapshot: Cursor::new(Vec::new()),
            }));
        }

        // A follower may need the contents. This runs between applies, so
        // the scan and the position agree.
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
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_data),
        }))
    }
}

/// Raft metadata key prefix - excluded from snapshots
const RAFT_PREFIX: &[u8] = b"_raft:";

impl RaftSnapshotBuilder<TypeConfig> for LsmRaftStorage {
    type SnapshotData = SnapshotData;

    /// A snapshot is a checkpoint: flush what has been applied, and report
    /// the position that is now on disk. The log may then be purged up to
    /// that position - and no further, since everything after it exists only
    /// in memory and in the log.
    ///
    /// openraft runs this beside apply, not between applies, so the position
    /// is read back from what was flushed rather than taken from memory. Only
    /// the position is used from the result; the contents, when a follower
    /// needs them, come from `get_current_snapshot`.
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        self.storage.flush().await.map_err(write_err)?;
        let meta = self.durable_position().await?.unwrap_or(SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
        });
        tracing::info!(?meta, "Checkpointed state machine");
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(Vec::new()),
        })
    }
}
