//! Raft storage implementation for OpenRaft 0.9 (storage-v2)
//!
//! Implements the OpenRaft storage traits:
//! - `RaftLogStorage` for log persistence (in-memory for now)
//! - `RaftStateMachine` for applying committed entries to LSM storage

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftLogId, RaftLogReader, RaftSnapshotBuilder};
use parking_lot::RwLock;

use crate::raft::changes::ChangeOp;
use crate::raft::types::{
    Command, CommandResponse, Entry, LogId, LogState, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, TypeConfig, Vote,
};
use crate::storage::StorageEngine;

/// MVCC row header size: txn_id(8) + roll_ptr(8) + deleted(1) = 17 bytes
const MVCC_HEADER_SIZE: usize = 17;

/// Encode a row with MVCC header format (matches MvccStorage::encode_row)
///
/// This ensures Raft-applied data has the same format as direct MVCC writes,
/// making the two write paths idempotent and compatible with MVCC reads.
fn encode_mvcc_row(txn_id: u64, deleted: bool, data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(MVCC_HEADER_SIZE + data.len());
    result.extend_from_slice(&txn_id.to_le_bytes()); // DB_TRX_ID
    result.extend_from_slice(&0u64.to_le_bytes()); // DB_ROLL_PTR (0 = no undo chain needed)
    result.push(if deleted { 1 } else { 0 }); // deleted flag
    result.extend_from_slice(data);
    result
}

/// In-memory log storage
#[derive(Debug, Default)]
struct LogData {
    /// Last purged log ID
    last_purged_log_id: Option<LogId>,
    /// Log entries
    log: BTreeMap<u64, Entry>,
    /// Current vote
    vote: Option<Vote>,
}

/// State machine data
///
/// Tracks applied state and provides access to the underlying storage engine.
#[derive(Default)]
struct StateMachineData {
    /// Last applied log ID
    last_applied_log: Option<LogId>,
    /// Current membership
    last_membership: StoredMembership,
    /// Underlying storage engine for persisting data
    ///
    /// When changes are applied, they are written to this storage.
    /// If None, changes are only stored in-memory (for testing).
    storage: Option<Arc<dyn StorageEngine>>,
}

impl std::fmt::Debug for StateMachineData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachineData")
            .field("last_applied_log", &self.last_applied_log)
            .field("last_membership", &self.last_membership)
            .field("storage", &self.storage.as_ref().map(|_| "<storage>"))
            .finish()
    }
}

/// In-memory Raft storage
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    log: Arc<RwLock<LogData>>,
    sm: Arc<RwLock<StateMachineData>>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new MemStorage with a storage engine for applying changes
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            log: Arc::new(RwLock::new(LogData::default())),
            sm: Arc::new(RwLock::new(StateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                storage: Some(storage),
            })),
        }
    }

    /// Set the storage engine for applying changes
    ///
    /// This should be called before the Raft node starts processing entries.
    pub fn set_storage(&self, storage: Arc<dyn StorageEngine>) {
        self.sm.write().storage = Some(storage);
    }
}

impl RaftLogReader<TypeConfig> for MemStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let log = self.log.read();
        let entries: Vec<_> = log.log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for MemStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState, StorageError> {
        let log = self.log.read();
        let last = log.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        let last_purged = log.last_purged_log_id;

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        self.log.write().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(self.log.read().vote)
    }

    async fn save_committed(&mut self, _committed: Option<LogId>) -> Result<(), StorageError> {
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
        let mut log = self.log.write();
        for entry in entries {
            log.log.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut log = self.log.write();
        let keys: Vec<_> = log.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            log.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut log = self.log.write();
        let keys: Vec<_> = log.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            log.log.remove(&key);
        }
        log.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for MemStorage {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let sm = self.sm.read();
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Get storage reference (if any) while holding lock briefly
        let storage = self.sm.read().storage.clone();

        let mut results = Vec::new();
        let entries_vec: Vec<Entry> = entries.into_iter().collect();

        for entry in &entries_vec {
            let log_id = *entry.get_log_id();

            match &entry.payload {
                EntryPayload::Blank => {
                    results.push(CommandResponse::Ok(None));
                }
                EntryPayload::Normal(cmd) => {
                    let resp = match cmd {
                        Command::DataChange(changeset) => {
                            // Apply changes to storage with MVCC format
                            // This matches the format used by MvccStorage::put/delete,
                            // making Raft apply idempotent with direct MVCC writes.
                            if let Some(ref storage) = storage {
                                for change in &changeset.changes {
                                    let result = match change.op {
                                        ChangeOp::Insert | ChangeOp::Update => {
                                            let data = change.value.as_deref().unwrap_or(&[]);
                                            let encoded =
                                                encode_mvcc_row(changeset.txn_id, false, data);
                                            storage.put(&change.key, &encoded).await
                                        }
                                        ChangeOp::Delete => {
                                            // MVCC deletes write a tombstone, not actual delete
                                            let encoded =
                                                encode_mvcc_row(changeset.txn_id, true, &[]);
                                            storage.put(&change.key, &encoded).await
                                        }
                                    };

                                    if let Err(e) = result {
                                        tracing::error!("Failed to apply change to storage: {}", e);
                                        // Continue applying other changes
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    "No storage engine configured, changes not persisted"
                                );
                            }
                            CommandResponse::Ok(None)
                        }
                        Command::Noop => CommandResponse::Ok(None),
                    };
                    results.push(resp);
                }
                EntryPayload::Membership(ref m) => {
                    self.sm.write().last_membership =
                        StoredMembership::new(Some(log_id), m.clone());
                    results.push(CommandResponse::Ok(None));
                }
            }

            // Update last applied log
            self.sm.write().last_applied_log = Some(log_id);
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
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let mut sm = self.sm.write();
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();

        // Note: Snapshot data would need to be applied to LSM storage.
        // This is a placeholder - full snapshot support requires LSM integration.
        // For now, we rely on log replay for state recovery.

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let sm = self.sm.read();

        // Note: Full snapshot support would serialize LSM state.
        // For now, return empty snapshot - rely on log replay.
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!(
                "{}-log",
                sm.last_applied_log
                    .map(|l| l.to_string())
                    .unwrap_or_default()
            ),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let sm = self.sm.read();

        // Note: Full snapshot support would serialize LSM state.
        // For now, return empty snapshot - rely on log replay.
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!(
                "{}-log",
                sm.last_applied_log
                    .map(|l| l.to_string())
                    .unwrap_or_default()
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}
