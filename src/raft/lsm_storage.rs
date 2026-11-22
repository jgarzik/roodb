//! LSM-backed Raft storage implementation
//!
//! Persists all Raft state (log entries, vote, membership) to LSM storage for durability.
//! This enables cluster-ready multi-node operation with proper crash recovery.

// openraft dictates these error types; they are large by its design, and the
// functions here feed directly into openraft trait methods that return them.
#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::{Stream, TryStreamExt};
use openraft::entry::RaftEntry;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use parking_lot::RwLock;

use crate::catalog::store::{decode_table, table_of_key, TableRecord};
use crate::catalog::system_tables::{
    is_system_table, row_to_procedure_def, row_to_trigger_def, row_to_view_def, SYSTEM_DATABASES,
    SYSTEM_PROCEDURES, SYSTEM_TABLES, SYSTEM_TRIGGERS, SYSTEM_VIEWS,
};
use crate::catalog::Catalog;
use crate::executor::encoding::{decode_row, table_of_row_key, TableId};
use crate::raft::changes::{ChangeOp, ChangeSet};
use crate::raft::types::{
    Command, CommandResponse, LogId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership,
    TypeConfig,
};
use crate::storage::traits::WriteOp;
use crate::storage::StorageEngine;

use super::types::{encode_mvcc_row, is_mvcc_tombstone, MVCC_HEADER_SIZE};

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
const LAST_APPLIED_KEY: &[u8] = b"_raft:sm:last_applied";
const MEMBERSHIP_KEY: &[u8] = b"_raft:sm:membership";
/// A bound on the highest transaction ID applied (u64, little endian).
const MAX_TXN_KEY: &[u8] = b"_raft:sm:max_txn_id";

/// Present once the seed entry has been applied.
pub const SEEDED_KEY: &[u8] = b"_raft:sm:seeded";

/// The next node ID to hand out. The first node is 1.
const NEXT_NODE_ID_KEY: &[u8] = b"_raft:sm:next_node_id";

/// Keys with this prefix are the state machine's own bookkeeping; every
/// other key is data.
pub const RAFT_KEY_PREFIX: &[u8] = b"_raft:";

/// The next object ID to hand out (u64, little endian).
const NEXT_OBJECT_ID_KEY: &[u8] = b"_raft:sm:next_object_id";

/// The reserved row ID bound of the cluster (u64, little endian): no row ID
/// below it is handed out again. Written first by initialisation, for the
/// IDs its rows use, then raised by `Command::ReserveRowIds`.
pub const ROW_ID_KEY: &[u8] = b"_raft:sm:row_id";

/// The stored bound is moved this far past the ID that crosses it, so that it
/// is rewritten once per this many transactions rather than once per entry.
const TXN_ID_STEP: u64 = 1024;

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
    /// The highest transaction ID applied, or a bound above it; shared with
    /// the transaction manager, which keeps its own IDs ahead of it.
    applied_txn_floor: Arc<AtomicU64>,
    /// The next AUTO_INCREMENT value of each table, as held in memory.
    auto_increment: Arc<crate::txn::AutoIncrement>,
    /// The bound as stored: never below `applied_txn_floor`.
    stored_txn_bound: Arc<AtomicU64>,
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
    /// What the changeset does to tables, in order: the catalog follows
    /// once the changeset is written.
    catalog: Vec<CatalogOp>,
    /// Tables this changeset drops, by ID.
    dropped_tables: Vec<TableId>,
}

/// A change to a table as the catalog sees it.
enum CatalogOp {
    /// The table is now this.
    Set(Box<TableRecord>),
    /// The table is gone.
    Unset(String),
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
    ) -> Result<Self, StorageError> {
        let this = Self {
            storage,
            catalog,
            cached_last_applied: Arc::new(RwLock::new(None)),
            cached_membership: Arc::new(RwLock::new(StoredMembership::default())),
            applied_txn_floor: Arc::new(AtomicU64::new(0)),
            stored_txn_bound: Arc::new(AtomicU64::new(0)),
            auto_increment: Arc::new(crate::txn::AutoIncrement::new()),
        };

        // Load existing state from LSM into caches
        this.load_cached_state().await?;

        Ok(this)
    }

    /// Load the bounds on transaction IDs and row IDs from storage. They only
    /// ever move forward, here as everywhere.
    async fn load_counters(&self) -> Result<(), StorageError> {
        // Transaction IDs at or below this may be in rows on disk.
        if let Some(data) = self.storage.get(MAX_TXN_KEY).await.map_err(read_err)? {
            let bytes: [u8; 8] = data
                .as_slice()
                .try_into()
                .map_err(|_| read_err("stored transaction ID bound is malformed"))?;
            let bound = u64::from_le_bytes(bytes);
            self.applied_txn_floor.fetch_max(bound, Ordering::SeqCst);
            self.stored_txn_bound.fetch_max(bound, Ordering::SeqCst);
        }

        // Row IDs: resume past what was reserved, and hand none out until a
        // new range has been reserved through Raft.
        let reserved = match self.storage.get(ROW_ID_KEY).await.map_err(read_err)? {
            Some(data) => {
                let bytes: [u8; 8] = data
                    .as_slice()
                    .try_into()
                    .map_err(|_| read_err("stored row ID bound is malformed"))?;
                u64::from_le_bytes(bytes)
            }
            None => 0,
        };
        crate::storage::row_id::generator().restore(reserved);

        Ok(())
    }

    /// How many nodes the cluster has, by the membership this node has
    /// stored. Known as soon as the state machine is loaded, before Raft
    /// has started.
    pub fn member_count(&self) -> usize {
        self.cached_membership.read().nodes().count()
    }

    /// The AUTO_INCREMENT counters, shared with the transaction manager:
    /// inserts take values from them, and applying keeps them in step with
    /// what is stored.
    pub fn auto_increment(&self) -> Arc<crate::txn::AutoIncrement> {
        self.auto_increment.clone()
    }

    /// The highest transaction ID applied, or a bound above it. The
    /// transaction manager keeps the IDs it hands out ahead of this.
    pub fn applied_txn_floor(&self) -> Arc<AtomicU64> {
        self.applied_txn_floor.clone()
    }

    /// The state machine's position as flushed to disk: what a restart would
    /// resume from. `None` if nothing has been flushed yet.
    async fn durable_position(&self) -> Result<Option<SnapshotMeta>, StorageError> {
        // The position and the membership are two keys, and a flush can land
        // between reading one and the other. Read the position again
        // afterwards and go round if it moved.
        loop {
            let Some(position) = self
                .storage
                .get_durable(LAST_APPLIED_KEY)
                .await
                .map_err(read_err)?
            else {
                return Ok(None);
            };
            let membership = self
                .storage
                .get_durable(MEMBERSHIP_KEY)
                .await
                .map_err(read_err)?;
            let again = self
                .storage
                .get_durable(LAST_APPLIED_KEY)
                .await
                .map_err(read_err)?;
            if again.as_deref() != Some(position.as_slice()) {
                continue;
            }
            let last_applied: LogId = bincode::deserialize(&position).map_err(read_err)?;
            let membership = match membership {
                Some(data) => bincode::deserialize(&data).map_err(read_err)?,
                None => StoredMembership::default(),
            };
            return Ok(Some(SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: membership,
            }));
        }
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

        self.load_counters().await?;

        tracing::info!(
            last_applied = ?*self.cached_last_applied.read(),
            txn_id_floor = self.applied_txn_floor.load(Ordering::SeqCst),
            row_ids = ?crate::storage::row_id::generator().demand(),
            "Loaded state machine position from LSM"
        );

        // The catalog is not stored anywhere but in the system tables.
        // Rebuild all of it - databases, tables, indexes, views, procedures -
        // from what is on disk. Entries replayed from the log afterwards
        // update it as they are applied, as they do while running.
        self.rebuild_catalog_from_system_tables().await?;

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
    async fn rebuild_catalog_from_system_tables(&self) -> Result<(), StorageError> {
        use crate::catalog::store::scan_rows;
        use crate::executor::Datum;

        // Every table's record: its definition and its indexes.
        let mut records: Vec<TableRecord> = Vec::new();
        for stored in scan_rows(self.storage.as_ref(), SYSTEM_TABLES)
            .await
            .map_err(read_err)?
        {
            match decode_table(&stored.data) {
                Some(record) => records.push(record),
                None => {
                    return Err(read_err(format!(
                        "the stored definition of table {:?} cannot be read",
                        table_of_key(&stored.key).unwrap_or_default()
                    )))
                }
            }
        }

        let database_names: Vec<String> = scan_rows(self.storage.as_ref(), SYSTEM_DATABASES)
            .await
            .map_err(read_err)?
            .into_iter()
            .filter_map(|stored| match stored.row.values().first() {
                Some(Datum::String(name)) => Some(name.clone()),
                _ => None,
            })
            .collect();
        let view_defs: Vec<crate::catalog::ViewDef> =
            scan_rows(self.storage.as_ref(), SYSTEM_VIEWS)
                .await
                .map_err(read_err)?
                .iter()
                .filter_map(|stored| row_to_view_def(&stored.row))
                .collect();
        let procedure_defs: Vec<crate::catalog::ProcedureDef> =
            scan_rows(self.storage.as_ref(), SYSTEM_PROCEDURES)
                .await
                .map_err(read_err)?
                .iter()
                .filter_map(|stored| row_to_procedure_def(&stored.row))
                .collect();
        let trigger_defs: Vec<crate::catalog::TriggerDef> =
            scan_rows(self.storage.as_ref(), SYSTEM_TRIGGERS)
                .await
                .map_err(read_err)?
                .iter()
                .filter_map(|stored| row_to_trigger_def(&stored.row))
                .collect();

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
            catalog.unset_table(&table_name);
        }

        // Tables and their indexes, as stored
        for record in records {
            tracing::debug!(table = %record.def.name, "Rebuilding table from storage");
            catalog.set_table(record.def, record.indexes);
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

        // Rebuild triggers
        catalog.clear_triggers();
        for trigger in trigger_defs {
            catalog.create_trigger(trigger);
        }

        // Rebuild views
        catalog.clear_views();
        for view_def in view_defs {
            tracing::debug!(view = %view_def.name, "Rebuilding view from snapshot");
            catalog.register_view(view_def);
        }

        tracing::info!("Rebuilt catalog from system tables");
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
                    // A key that holds a live row cannot be inserted again:
                    // a row of a user's table, or a row of a system table,
                    // which is keyed by what it describes (two sessions
                    // creating the same account, view or database at once).
                    let is_table_record = change.table == SYSTEM_TABLES;
                    if change.op == ChangeOp::Insert {
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
                            if is_table_record {
                                // Two sessions raced to create the table:
                                // the second is told, as if it had found
                                // the table there.
                                return Ok(Err(format!(
                                    "Table '{}' already exists",
                                    table_of_key(&change.key).unwrap_or_default()
                                )));
                            }
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

                    if is_table_record {
                        match decode_table(value) {
                            // A temporary table belongs to one session on
                            // one node. Its definition is never stored.
                            Some(record) if record.def.is_temporary => {
                                return Ok(Err(format!(
                                    "'{}' is a temporary table: its definition is not stored",
                                    record.def.name
                                )))
                            }
                            Some(record) => plan.catalog.push(CatalogOp::Set(Box::new(record))),
                            None => {
                                return Ok(Err(
                                    "a table definition in the changeset cannot be read"
                                        .to_string(),
                                ))
                            }
                        }
                    }

                    plan.writes.push((
                        change.key.clone(),
                        Some(encode_mvcc_row(changeset.txn_id, false, value)),
                    ));
                    live.insert(change.key.as_slice(), true);
                    if !is_table_record {
                        plan.tracked.push(TrackedChange {
                            table: change.table.clone(),
                            value: value.clone(),
                            is_delete: false,
                        });
                    }
                }
                // An insert or update with no value changes nothing.
                (ChangeOp::Insert | ChangeOp::Update, None) => {}
                (ChangeOp::Delete, value) => {
                    plan.writes.push((
                        change.key.clone(),
                        Some(encode_mvcc_row(changeset.txn_id, true, &[])),
                    ));
                    live.insert(change.key.as_slice(), false);
                    if change.table == SYSTEM_TABLES {
                        if let Some(name) = table_of_key(&change.key) {
                            plan.catalog.push(CatalogOp::Unset(name));
                        }
                    } else if let Some(value) = value {
                        plan.tracked.push(TrackedChange {
                            table: change.table.clone(),
                            value: value.clone(),
                            is_delete: true,
                        });
                    }
                }
            }
        }
        // Tables the changeset leaves dropped: those whose record it removes
        // without storing a record of the same ID again (a renamed table's
        // record moves; the table stays).
        let mut kept: Vec<TableId> = Vec::new();
        for op in &plan.catalog {
            if let CatalogOp::Set(record) = op {
                kept.push(record.def.id);
            }
        }
        for op in &plan.catalog {
            let CatalogOp::Unset(name) = op else { continue };
            let id = self.catalog.read().get_table(name).map(|def| def.id);
            if let Some(id) = id {
                if !kept.contains(&id) && !plan.dropped_tables.contains(&id) {
                    plan.dropped_tables.push(id);
                }
            }
        }
        // AUTO_INCREMENT counters. A dropped table's counter goes with it;
        // an inserted row raises its table's stored counter to the value
        // the row carries. (A row's table is the ID its key begins with.)
        for table in &plan.dropped_tables {
            plan.writes
                .push((crate::txn::auto_inc::storage_key(*table), None));
            self.auto_increment.forget(*table);
        }
        let mut counters: std::collections::BTreeMap<TableId, u64> =
            std::collections::BTreeMap::new();
        for change in &changeset.changes {
            if let (Some(next), Some(table)) = (change.auto_inc, table_of_row_key(&change.key)) {
                let counter = counters.entry(table).or_insert(0);
                *counter = (*counter).max(next);
            }
        }
        for (table, next) in counters {
            let key = crate::txn::auto_inc::storage_key(table);
            let stored = if plan.dropped_tables.contains(&table) {
                None
            } else {
                self.storage
                    .get(&key)
                    .await
                    .map_err(read_err)?
                    .and_then(|stored| crate::txn::auto_inc::decode(&stored))
            };
            if stored.is_none_or(|stored| next > stored) {
                plan.writes.push((key, Some(next.to_le_bytes().to_vec())));
                self.auto_increment.raise(table, next);
            }
        }
        // An entry the storage engine cannot hold is a reason to reject the
        // changeset, never to fail the apply: a failed apply stops the state
        // machine, and the entry would fail again on every replay.
        for (key, value) in &plan.writes {
            let value_len = value.as_ref().map_or(0, Vec::len);
            if let Err(e) = crate::storage::lsm::block::check_entry_size(key.len(), value_len) {
                return Ok(Err(e.to_string()));
            }
        }
        Ok(Ok(plan))
    }

    /// Bring the catalog up to date with a change to one of the system
    /// tables that hold databases, procedures, triggers and views. (Tables
    /// follow from their records: see `CatalogOp`.)
    fn track_system_table_change(&self, table: &str, value: &[u8], is_delete: bool) {
        if table == SYSTEM_DATABASES {
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
        } else if table == SYSTEM_TRIGGERS {
            // Trigger changes - update catalog directly
            if let Ok(row) = decode_row(value) {
                if is_delete {
                    if let Some(crate::executor::Datum::String(name)) = row.values().first() {
                        let _ = self.catalog.write().drop_trigger(name);
                    }
                } else if let Some(trigger) = row_to_trigger_def(&row) {
                    self.catalog.write().create_trigger(trigger);
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

        for entry in entries {
            let log_id = entry.log_id();

            // Everything an entry changes goes to storage as one batch,
            // together with the entry's position. Storage keeps a batch whole,
            // so what is on disk is always the result of applying the log up
            // to exactly the position recorded with it - which is what lets a
            // restart resume from that position by replaying the log.
            let mut batch: Vec<WriteOp> = Vec::new();
            let mut tracked: Vec<TrackedChange> = Vec::new();
            let mut catalog_ops: Vec<CatalogOp> = Vec::new();
            let mut membership_change = None;
            let mut new_txn_id = None;
            let mut reserved_row_ids = None;
            let mut seeded = false;

            let response = match &entry.payload {
                EntryPayload::Blank => CommandResponse::Ok(None),
                EntryPayload::Normal(Command::Noop) => CommandResponse::Ok(None),
                EntryPayload::Normal(Command::ReserveRowIds { upto }) => {
                    // Reservations can be proposed out of order; the stored
                    // bound only ever grows, as the one in memory does.
                    let stored = match self.storage.get(ROW_ID_KEY).await.map_err(read_err)? {
                        Some(data) => data
                            .as_slice()
                            .try_into()
                            .map(u64::from_le_bytes)
                            .map_err(|_| read_err("stored row ID bound is malformed"))?,
                        None => 0,
                    };
                    let bound = stored.max(*upto);
                    batch.push((ROW_ID_KEY.to_vec(), Some(bound.to_le_bytes().to_vec())));
                    // Every node follows the bound: whichever becomes
                    // primary goes on past it.
                    reserved_row_ids = Some(bound);
                    CommandResponse::Ok(None)
                }
                EntryPayload::Normal(Command::Seed(entries)) => {
                    for (key, value) in entries {
                        batch.push((key.clone(), Some(value.clone())));
                    }
                    batch.push((SEEDED_KEY.to_vec(), Some(vec![1])));
                    seeded = true;
                    CommandResponse::Ok(None)
                }
                EntryPayload::Normal(Command::AllocIds { count }) => {
                    let next = match self
                        .storage
                        .get(NEXT_OBJECT_ID_KEY)
                        .await
                        .map_err(read_err)?
                    {
                        Some(data) => data
                            .as_slice()
                            .try_into()
                            .map(u64::from_le_bytes)
                            .map_err(|_| read_err("stored object ID counter is malformed"))?,
                        None => crate::catalog::system_tables::FIRST_USER_TABLE_ID,
                    };
                    batch.push((
                        NEXT_OBJECT_ID_KEY.to_vec(),
                        Some(next.saturating_add(*count).to_le_bytes().to_vec()),
                    ));
                    CommandResponse::Ok(Some(next.to_le_bytes().to_vec()))
                }
                EntryPayload::Normal(Command::AllocNodeId) => {
                    let next = match self.storage.get(NEXT_NODE_ID_KEY).await.map_err(read_err)? {
                        Some(data) => data
                            .as_slice()
                            .try_into()
                            .map(u64::from_le_bytes)
                            .map_err(|_| read_err("stored node ID counter is malformed"))?,
                        None => 2,
                    };
                    batch.push((
                        NEXT_NODE_ID_KEY.to_vec(),
                        Some((next + 1).to_le_bytes().to_vec()),
                    ));
                    CommandResponse::Ok(Some(next.to_le_bytes().to_vec()))
                }
                EntryPayload::Normal(Command::DataChange(changeset)) => {
                    match self.plan_changeset(changeset).await? {
                        Ok(plan) => {
                            new_txn_id = Some(changeset.txn_id);
                            tracing::debug!(
                                txn_id = changeset.txn_id,
                                changes = changeset.changes.len(),
                                writes = plan.writes.len(),
                                "RAFT APPLY: changeset accepted"
                            );
                            batch = plan.writes;
                            tracked = plan.tracked;
                            catalog_ops = plan.catalog;
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

            // Rows now carry this transaction's ID. Keep the stored bound
            // above every ID in a row, in the same batch as the rows.
            if let Some(txn_id) = new_txn_id {
                if txn_id > self.stored_txn_bound.load(Ordering::SeqCst) {
                    let bound = txn_id + TXN_ID_STEP;
                    batch.push((MAX_TXN_KEY.to_vec(), Some(bound.to_le_bytes().to_vec())));
                    self.stored_txn_bound.store(bound, Ordering::SeqCst);
                }
            }

            let position = bincode::serialize(&log_id).map_err(write_err)?;
            batch.push((LAST_APPLIED_KEY.to_vec(), Some(position)));
            self.storage.write_batch(batch).await.map_err(write_err)?;
            if let Some(txn_id) = new_txn_id {
                self.applied_txn_floor.fetch_max(txn_id, Ordering::SeqCst);
            }
            // The seed brings rows of the system tables with it: let the
            // catalog see them.
            if seeded {
                self.rebuild_catalog_from_system_tables().await?;
            }
            // Only now that the reservation is recorded may the IDs be used.
            if let Some(upto) = reserved_row_ids {
                crate::storage::row_id::generator().extend_limit(upto);
            }

            // Memory follows storage.
            *self.cached_last_applied.write() = Some(log_id);
            if let Some(membership) = membership_change {
                *self.cached_membership.write() = membership;
            }
            // The catalog follows each entry as it is written: the next
            // entry is checked against the catalog this one leaves.
            if !catalog_ops.is_empty() {
                let mut catalog = self.catalog.write();
                for op in catalog_ops {
                    match op {
                        CatalogOp::Set(record) => catalog.set_table(record.def, record.indexes),
                        CatalogOp::Unset(name) => catalog.unset_table(&name),
                    }
                }
            }
            for change in tracked {
                self.track_system_table_change(&change.table, &change.value, change.is_delete);
            }
            results.push(response);
        }

        tracing::debug!(target: "roodb::perf", elapsed_us = start.elapsed().as_micros() as u64, entries = results.len(), "raft_apply");

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
        // The contents arrived as a complete replacement for what this node
        // holds, already written out; switching to it is one step. It is
        // the sender's storage as it stood at the snapshot's position, the
        // state machine's own records (position, membership, counters)
        // included: they are the same on every node.
        match snapshot {
            SnapshotData::Received(ingest) => {
                ingest.commit().await.map_err(write_err)?;
                tracing::info!(?meta, "Installing snapshot data");
            }
            // Only a position: there is nothing to replace.
            SnapshotData::None => {}
            SnapshotData::Stored(_) => {
                return Err(read_err("a snapshot to send cannot be installed"));
            }
        }

        let mut batch: Vec<WriteOp> = Vec::new();
        if let Some(log_id) = meta.last_log_id {
            let data = bincode::serialize(&log_id).map_err(write_err)?;
            batch.push((LAST_APPLIED_KEY.to_vec(), Some(data)));
        }
        let data = bincode::serialize(&meta.last_membership).map_err(write_err)?;
        batch.push((MEMBERSHIP_KEY.to_vec(), Some(data)));
        self.storage.write_batch(batch).await.map_err(write_err)?;

        if let Some(log_id) = meta.last_log_id {
            *self.cached_last_applied.write() = Some(log_id);
        }
        *self.cached_membership.write() = meta.last_membership.clone();

        // The snapshot's rows carry transaction IDs and row IDs this node
        // may never have seen; pick up the bounds that came with them.
        self.load_counters().await?;

        // Rebuild catalog from system tables after snapshot install
        self.rebuild_catalog_from_system_tables().await?;

        // Flush to ensure all snapshot data is persisted
        self.storage.flush().await.map_err(write_err)?;

        tracing::info!(?meta, "Installed snapshot");
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        // Alone in the cluster: no other node will ever ask for a
        // snapshot's contents.
        let alone = self.cached_membership.read().nodes().count() <= 1;
        if alone {
            // What has been flushed is a snapshot: it is the state at the
            // position flushed with it. Nobody will ask for its contents, so
            // none are produced.
            let Some(meta) = self.durable_position().await? else {
                return Ok(None);
            };
            return Ok(Some(Snapshot {
                meta,
                snapshot: SnapshotData::None,
            }));
        }

        // Another node may need the contents. They are what has been
        // flushed, held still as a view; the position and membership that
        // go with them are read from the view itself, since they were
        // flushed in the same batches as the data they describe.
        let view = self.storage.snapshot_view().await.map_err(read_err)?;
        let Some(position) = view.get(LAST_APPLIED_KEY).await.map_err(read_err)? else {
            return Ok(None);
        };
        let last_applied: LogId = bincode::deserialize(&position).map_err(read_err)?;
        let membership = match view.get(MEMBERSHIP_KEY).await.map_err(read_err)? {
            Some(data) => bincode::deserialize(&data).map_err(read_err)?,
            None => StoredMembership::default(),
        };
        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: membership,
            },
            snapshot: SnapshotData::Stored(view),
        }))
    }
}

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
            snapshot: SnapshotData::None,
        })
    }
}
