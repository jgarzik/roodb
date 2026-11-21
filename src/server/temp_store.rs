//! Storage for a session's temporary tables.
//!
//! A temporary table belongs to one connection and disappears with it. Its
//! rows are kept here, in memory, for that connection only. They never go
//! into the Raft log or the shared storage engine: other nodes have no use
//! for them, and after a restart there is no session left to own them.
//!
//! [`SessionStorage`] is what a session reads and writes through. Keys that
//! fall in one of the session's temporary tables are served from memory;
//! everything else goes to the shared engine.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::executor::encoding::{table_key_end, table_key_prefix};
use crate::raft::{encode_mvcc_row, is_mvcc_tombstone, ChangeOp, RowChange};
use crate::storage::error::StorageResult;
use crate::storage::traits::{KeyValue, WriteOp};
use crate::storage::StorageEngine;

#[derive(Default)]
struct TempState {
    /// Key range of each temporary table: `[prefix, end)`.
    tables: Vec<(String, Vec<u8>, Vec<u8>)>,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl TempState {
    fn owns(&self, key: &[u8]) -> bool {
        self.tables
            .iter()
            .any(|(_, prefix, end)| key >= prefix.as_slice() && key < end.as_slice())
    }
}

/// A session's view of storage: its temporary tables in memory, the rest in
/// the shared engine.
pub struct SessionStorage {
    shared: Arc<dyn StorageEngine>,
    temp: RwLock<TempState>,
    /// Whether this session has any temporary table. Most never do, and then
    /// every operation goes straight to the shared engine.
    has_temp: AtomicBool,
}

impl SessionStorage {
    pub fn new(shared: Arc<dyn StorageEngine>) -> Self {
        Self {
            shared,
            temp: RwLock::new(TempState::default()),
            has_temp: AtomicBool::new(false),
        }
    }

    fn owns(&self, key: &[u8]) -> bool {
        self.has_temp.load(Ordering::Relaxed) && self.temp.read().owns(key)
    }

    /// Start keeping `table`'s rows in this session.
    pub fn add_table(&self, table: &str) {
        let mut temp = self.temp.write();
        temp.tables.push((
            table.to_string(),
            table_key_prefix(table),
            table_key_end(table),
        ));
        self.has_temp.store(true, Ordering::Relaxed);
    }

    /// Whether `table` is one of this session's temporary tables.
    pub fn is_temp_table(&self, table: &str) -> bool {
        self.has_temp.load(Ordering::Relaxed)
            && self
                .temp
                .read()
                .tables
                .iter()
                .any(|(name, _, _)| name == table)
    }

    /// Drop a temporary table and its rows. Returns false if it was not one.
    pub fn remove_table(&self, table: &str) -> bool {
        let mut temp = self.temp.write();
        let Some(position) = temp.tables.iter().position(|(name, _, _)| name == table) else {
            return false;
        };
        let (_, prefix, end) = temp.tables.remove(position);
        let keys: Vec<Vec<u8>> = temp
            .rows
            .range(prefix..end)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            temp.rows.remove(&key);
        }
        self.has_temp
            .store(!temp.tables.is_empty(), Ordering::Relaxed);
        true
    }

    /// Drop every temporary table. Returns their names.
    pub fn clear(&self) -> Vec<String> {
        let mut temp = self.temp.write();
        self.has_temp.store(false, Ordering::Relaxed);
        temp.rows.clear();
        temp.tables.drain(..).map(|(name, _, _)| name).collect()
    }

    /// Separate changes to this session's temporary tables from the rest:
    /// `(temporary, shared)`.
    pub fn split(&self, changes: Vec<RowChange>) -> (Vec<RowChange>, Vec<RowChange>) {
        if !self.has_temp.load(Ordering::Relaxed) {
            return (Vec::new(), changes);
        }
        let temp = self.temp.read();
        changes
            .into_iter()
            .partition(|change| temp.owns(&change.key))
    }

    /// Check that committing these temporary-table changes would not insert a
    /// key that is already there. Changes earlier in the list count: deleting
    /// a row and inserting it again is fine.
    pub fn check(&self, changes: &[RowChange], ignore_duplicates: bool) -> Result<(), String> {
        if changes.is_empty() || ignore_duplicates {
            return Ok(());
        }
        let temp = self.temp.read();
        let mut live: std::collections::HashMap<&[u8], bool> = std::collections::HashMap::new();
        for change in changes {
            match change.op {
                ChangeOp::Insert => {
                    let exists = match live.get(change.key.as_slice()) {
                        Some(live) => *live,
                        None => temp
                            .rows
                            .get(&change.key)
                            .is_some_and(|row| !is_mvcc_tombstone(row)),
                    };
                    if exists {
                        return Err(format!(
                            "Duplicate key: row in '{}' already exists",
                            change.table
                        ));
                    }
                    live.insert(change.key.as_slice(), true);
                }
                ChangeOp::Update => {
                    live.insert(change.key.as_slice(), true);
                }
                ChangeOp::Delete => {
                    live.insert(change.key.as_slice(), false);
                }
            }
        }
        Ok(())
    }

    /// Apply committed changes to this session's temporary tables. Rows are
    /// stored as the shared engine stores them, so reads treat them alike.
    pub fn apply(&self, txn_id: u64, changes: Vec<RowChange>, ignore_duplicates: bool) {
        if changes.is_empty() {
            return;
        }
        let mut temp = self.temp.write();
        for change in changes {
            match (change.op, change.value) {
                (ChangeOp::Insert, Some(value)) => {
                    let exists = temp
                        .rows
                        .get(&change.key)
                        .is_some_and(|row| !is_mvcc_tombstone(row));
                    if exists && ignore_duplicates {
                        continue;
                    }
                    temp.rows
                        .insert(change.key, encode_mvcc_row(txn_id, false, &value));
                }
                (ChangeOp::Update, Some(value)) => {
                    temp.rows
                        .insert(change.key, encode_mvcc_row(txn_id, false, &value));
                }
                (ChangeOp::Insert | ChangeOp::Update, None) => {}
                (ChangeOp::Delete, _) => {
                    temp.rows
                        .insert(change.key, encode_mvcc_row(txn_id, true, &[]));
                }
            }
        }
    }
}

#[async_trait]
impl StorageEngine for SessionStorage {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        if self.owns(key) {
            return Ok(self.temp.read().rows.get(key).cloned());
        }
        self.shared.get(key).await
    }

    async fn get_durable(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        // Temporary rows are never durable.
        if self.owns(key) {
            return Ok(None);
        }
        self.shared.get_durable(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        if self.owns(key) {
            self.temp.write().rows.insert(key.to_vec(), value.to_vec());
            return Ok(());
        }
        self.shared.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        if self.owns(key) {
            self.temp.write().rows.remove(key);
            return Ok(());
        }
        self.shared.delete(key).await
    }

    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
        if !self.has_temp.load(Ordering::Relaxed) {
            return self.shared.write_batch(batch).await;
        }
        let mut shared = Vec::new();
        {
            let mut temp = self.temp.write();
            for (key, value) in batch {
                if temp.owns(&key) {
                    match value {
                        Some(value) => temp.rows.insert(key, value),
                        None => temp.rows.remove(&key),
                    };
                } else {
                    shared.push((key, value));
                }
            }
        }
        if shared.is_empty() {
            return Ok(());
        }
        self.shared.write_batch(shared).await
    }

    /// A scan is served from wherever its start key lives: scans are made
    /// over one table at a time.
    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        if let Some(start) = start {
            if self.owns(start) {
                let temp = self.temp.read();
                let rows = match end {
                    Some(end) => temp.rows.range(start.to_vec()..end.to_vec()),
                    None => temp.rows.range(start.to_vec()..),
                };
                return Ok(rows.map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }
        self.shared.scan(start, end).await
    }

    async fn flush(&self) -> StorageResult<()> {
        self.shared.flush().await
    }

    async fn close(&self) -> StorageResult<()> {
        // The shared engine is not this session's to close.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shared engine that must never be touched for temporary rows.
    struct Untouched;

    #[async_trait]
    impl StorageEngine for Untouched {
        async fn get(&self, _key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            Ok(Some(b"shared".to_vec()))
        }
        async fn get_durable(&self, _key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            Ok(Some(b"shared".to_vec()))
        }
        async fn put(&self, key: &[u8], _value: &[u8]) -> StorageResult<()> {
            panic!("temporary row written to shared storage: {key:?}");
        }
        async fn delete(&self, key: &[u8]) -> StorageResult<()> {
            panic!("temporary row deleted from shared storage: {key:?}");
        }
        async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
            panic!("temporary rows written to shared storage: {batch:?}");
        }
        async fn scan(
            &self,
            _start: Option<&[u8]>,
            _end: Option<&[u8]>,
        ) -> StorageResult<Vec<KeyValue>> {
            Ok(vec![(b"shared-key".to_vec(), b"shared".to_vec())])
        }
        async fn flush(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn close(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    fn key(table: &str, id: u8) -> Vec<u8> {
        let mut key = table_key_prefix(table);
        key.push(id);
        key
    }

    #[tokio::test]
    async fn temporary_rows_stay_in_the_session() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        // No temporary tables: everything is the shared engine's.
        assert_eq!(
            storage.get(&key("tmp", 1)).await.unwrap(),
            Some(b"shared".to_vec())
        );

        storage.add_table("tmp");
        assert!(storage.is_temp_table("tmp"));
        assert_eq!(storage.get(&key("tmp", 1)).await.unwrap(), None);

        let changes = vec![
            RowChange::insert("tmp", key("tmp", 1), b"one".to_vec()),
            RowChange::insert("other", key("other", 1), b"shared".to_vec()),
            RowChange::insert("tmp", key("tmp", 2), b"two".to_vec()),
        ];
        let (temp, shared) = storage.split(changes);
        assert_eq!(temp.len(), 2);
        assert_eq!(shared.len(), 1);
        storage.check(&temp, false).unwrap();
        storage.apply(7, temp, false);

        let rows = storage
            .scan(Some(&table_key_prefix("tmp")), Some(&table_key_end("tmp")))
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0].1[17..], b"one");
        // Other tables still come from the shared engine.
        assert_eq!(
            storage.get(&key("other", 1)).await.unwrap(),
            Some(b"shared".to_vec())
        );
        assert_eq!(storage.get_durable(&key("tmp", 1)).await.unwrap(), None);

        // A table whose name merely starts the same is not temporary.
        assert!(!storage.is_temp_table("tmp2"));
        assert_eq!(
            storage.get(&key("tmp2", 1)).await.unwrap(),
            Some(b"shared".to_vec())
        );
    }

    #[tokio::test]
    async fn duplicates_are_refused_and_deletes_free_the_key() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("tmp");
        storage.apply(
            1,
            vec![RowChange::insert("tmp", key("tmp", 1), b"a".to_vec())],
            false,
        );

        let dup = vec![RowChange::insert("tmp", key("tmp", 1), b"b".to_vec())];
        assert!(storage.check(&dup, false).is_err());
        assert!(storage.check(&dup, true).is_ok());
        storage.apply(2, dup, true);
        assert_eq!(
            &storage.get(&key("tmp", 1)).await.unwrap().unwrap()[17..],
            b"a"
        );

        let replace = vec![
            RowChange::delete("tmp", key("tmp", 1)),
            RowChange::insert("tmp", key("tmp", 1), b"c".to_vec()),
        ];
        storage.check(&replace, false).unwrap();
        storage.apply(3, replace, false);
        assert_eq!(
            &storage.get(&key("tmp", 1)).await.unwrap().unwrap()[17..],
            b"c"
        );

        let twice = vec![
            RowChange::insert("tmp", key("tmp", 9), b"x".to_vec()),
            RowChange::insert("tmp", key("tmp", 9), b"y".to_vec()),
        ];
        assert!(storage.check(&twice, false).is_err());
    }

    #[tokio::test]
    async fn dropping_a_table_drops_its_rows_only() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("a");
        storage.add_table("b");
        storage.apply(
            1,
            vec![
                RowChange::insert("a", key("a", 1), b"a".to_vec()),
                RowChange::insert("b", key("b", 1), b"b".to_vec()),
            ],
            false,
        );
        assert!(storage.remove_table("a"));
        assert!(!storage.remove_table("a"));
        assert!(!storage.is_temp_table("a"));
        assert!(storage.get(&key("b", 1)).await.unwrap().is_some());
        // `a` is an ordinary name again.
        assert_eq!(
            storage.get(&key("a", 1)).await.unwrap(),
            Some(b"shared".to_vec())
        );

        assert_eq!(storage.clear(), ["b"]);
        assert_eq!(
            storage.get(&key("b", 1)).await.unwrap(),
            Some(b"shared".to_vec())
        );
    }
}
