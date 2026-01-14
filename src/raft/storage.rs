//! In-memory Raft storage implementation for OpenRaft 0.9 (storage-v2)

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftLogId, RaftLogReader, RaftSnapshotBuilder};
use parking_lot::RwLock;

use crate::raft::types::{
    Command, CommandResponse, Entry, LogId, LogState,
    Snapshot, SnapshotMeta, StorageError, StoredMembership, TypeConfig, Vote,
};

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

/// In-memory state machine
#[derive(Debug, Default)]
struct StateMachineData {
    /// Last applied log ID
    last_applied_log: Option<LogId>,
    /// Current membership
    last_membership: StoredMembership,
    /// Key-value store (the actual data)
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
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

    /// Get a value from the state machine
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.sm.read().kv.get(key).cloned()
    }

    /// Scan a range of keys
    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.sm
            .read()
            .kv
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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
        let mut sm = self.sm.write();
        let mut results = Vec::new();

        for entry in entries {
            sm.last_applied_log = Some(*entry.get_log_id());

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(CommandResponse::Ok(None));
                }
                EntryPayload::Normal(cmd) => {
                    let resp = match cmd {
                        Command::Put { key, value } => {
                            sm.kv.insert(key, value);
                            CommandResponse::Ok(None)
                        }
                        Command::Delete { key } => {
                            let old = sm.kv.remove(&key);
                            CommandResponse::Ok(old)
                        }
                        Command::Noop => CommandResponse::Ok(None),
                    };
                    results.push(resp);
                }
                EntryPayload::Membership(ref m) => {
                    sm.last_membership = StoredMembership::new(Some(*entry.get_log_id()), m.clone());
                    results.push(CommandResponse::Ok(None));
                }
            }
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
        let mut sm = self.sm.write();
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();

        // Deserialize snapshot data
        let data = snapshot.into_inner();
        if !data.is_empty() {
            if let Ok(kv) = bincode::deserialize::<BTreeMap<Vec<u8>, Vec<u8>>>(&data) {
                sm.kv = kv;
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let sm = self.sm.read();

        let data = bincode::serialize(&sm.kv).unwrap_or_default();

        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!(
                "{}-mem",
                sm.last_applied_log
                    .map(|l| l.to_string())
                    .unwrap_or_default()
            ),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let sm = self.sm.read();

        let data = bincode::serialize(&sm.kv).unwrap_or_default();

        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!(
                "{}-mem",
                sm.last_applied_log
                    .map(|l| l.to_string())
                    .unwrap_or_default()
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
