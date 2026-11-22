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

use crate::executor::encoding::{table_range, TableId};
use crate::raft::{encode_mvcc_row, is_mvcc_tombstone, ChangeOp, RowChange};
use crate::storage::error::StorageResult;
use crate::storage::traits::{KeyValue, WriteOp};
use crate::storage::StorageEngine;

/// Most memory one session's temporary tables may hold. They live in the
/// server's memory, so without a limit one session could exhaust it.
pub const TEMP_TABLE_LIMIT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Default)]
struct TempState {
    /// Key range of each temporary table: `[prefix, end)`.
    tables: Vec<(String, Vec<u8>, Vec<u8>)>,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Bytes of keys and values in `rows`.
    bytes: usize,
}

impl TempState {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.bytes += key.len() + value.len();
        if let Some(old) = self.rows.insert(key.clone(), value) {
            self.bytes -= key.len() + old.len();
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(old) = self.rows.remove(key) {
            self.bytes -= key.len() + old.len();
        }
    }
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
    pub fn add_table(&self, table: &str, id: TableId) {
        let mut temp = self.temp.write();
        // Everything stored under the table's ID: its rows and its indexes.
        let (start, end) = table_range(id);
        temp.tables.push((table.to_string(), start, end));
        self.has_temp.store(true, Ordering::Relaxed);
    }

    /// Names of this session's temporary tables.
    pub fn table_names(&self) -> Vec<String> {
        if !self.has_temp.load(Ordering::Relaxed) {
            return Vec::new();
        }
        self.temp
            .read()
            .tables
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect()
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
            temp.remove(&key);
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
        temp.bytes = 0;
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

    /// The key range `[start, end)` of one of this session's temporary
    /// tables, or `None` if `table` is not one.
    pub fn key_range(&self, table: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        self.temp
            .read()
            .tables
            .iter()
            .find(|(name, _, _)| name == table)
            .map(|(_, start, end)| (start.clone(), end.clone()))
    }

    /// Check that committing these temporary-table changes would not insert a
    /// key that is already there, nor take the session over its limit.
    /// Changes earlier in the list count: deleting a row and inserting it
    /// again is fine.
    pub fn check(&self, changes: &[RowChange], ignore_duplicates: bool) -> Result<(), String> {
        if changes.is_empty() {
            return Ok(());
        }
        let temp = self.temp.read();
        // Refuse the commit that would take the session over its limit.
        // What counts is the size afterwards: an update replaces a row and a
        // delete frees one, so a full table can still be changed or emptied.
        let mut sizes: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
        let mut after = temp.bytes;
        for change in changes {
            let key = change.key.as_slice();
            let old = match sizes.get(key) {
                Some(size) => *size,
                None => temp.rows.get(key).map_or(0, |row| key.len() + row.len()),
            };
            let new = match (&change.op, &change.value) {
                (ChangeOp::Delete, _) => 0,
                // INSERT IGNORE leaves a row that is already there as it is.
                (ChangeOp::Insert, _) if ignore_duplicates && old > 0 => old,
                (_, Some(value)) => key.len() + value.len() + crate::raft::MVCC_HEADER_SIZE,
                (_, None) => old,
            };
            after = after - old + new;
            sizes.insert(key, new);
        }
        if after > TEMP_TABLE_LIMIT_BYTES && after > temp.bytes {
            return Err(format!(
                "The table '{}' is full: temporary tables of one session may hold {} MiB",
                changes[0].table,
                TEMP_TABLE_LIMIT_BYTES >> 20
            ));
        }
        if ignore_duplicates {
            return Ok(());
        }
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
                    temp.insert(change.key, encode_mvcc_row(txn_id, false, &value));
                }
                (ChangeOp::Update, Some(value)) => {
                    temp.insert(change.key, encode_mvcc_row(txn_id, false, &value));
                }
                (ChangeOp::Insert | ChangeOp::Update, None) => {}
                // No other session reads these rows, so no one needs to be
                // told that a row was deleted: it is simply removed.
                (ChangeOp::Delete, _) => temp.remove(&change.key),
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
            self.temp.write().insert(key.to_vec(), value.to_vec());
            return Ok(());
        }
        self.shared.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        if self.owns(key) {
            self.temp.write().remove(key);
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
                        Some(value) => temp.insert(key, value),
                        None => temp.remove(&key),
                    }
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

    /// Like a scan, a range belongs to whichever side its start key lives
    /// on: ranges are those of one table at a time. This session's rows in
    /// the range go in either case. The shared engine is told only when the
    /// range is not one of this session's temporary tables: a temporary
    /// table hides a shared table of the same name, whose keys are the same,
    /// and dropping the one must not empty the other.
    async fn drop_range(&self, start: &[u8], end: &[u8]) -> StorageResult<()> {
        if start >= end {
            return Ok(());
        }
        if self.has_temp.load(Ordering::Relaxed) {
            use std::ops::Bound::{Excluded, Included};
            let mut temp = self.temp.write();
            let keys: Vec<Vec<u8>> = temp
                .rows
                .range::<[u8], _>((Included(start), Excluded(end)))
                .map(|(key, _)| key.clone())
                .collect();
            for key in keys {
                temp.remove(&key);
            }
            if temp.owns(start) {
                return Ok(());
            }
        }
        self.shared.drop_range(start, end).await
    }

    /// A scan is served from wherever its start key lives: scans are made
    /// over one table at a time.
    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        if let Some(start) = start {
            if self.owns(start) {
                let temp = self.temp.read();
                let rows = match end {
                    // An empty or inverted range holds nothing (and a
                    // BTreeMap panics if asked for one).
                    Some(end) if start >= end => return Ok(Vec::new()),
                    Some(end) => temp.rows.range(start.to_vec()..end.to_vec()),
                    None => temp.rows.range(start.to_vec()..),
                };
                return Ok(rows.map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }
        self.shared.scan(start, end).await
    }

    async fn next_key(&self, key: &[u8], high: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        if self.owns(key) || self.owns(high) {
            use std::ops::Bound::{Excluded, Unbounded};
            let temp = self.temp.read();
            return Ok(temp
                .rows
                .range::<[u8], _>((Excluded(key), Unbounded))
                .next()
                .map(|(k, _)| k.clone())
                .filter(|k| k.as_slice() < high));
        }
        self.shared.next_key(key, high).await
    }

    async fn prev_key(&self, key: &[u8], low: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        if self.owns(key) || self.owns(low) {
            use std::ops::Bound::{Excluded, Unbounded};
            let temp = self.temp.read();
            return Ok(temp
                .rows
                .range::<[u8], _>((Unbounded, Excluded(key)))
                .next_back()
                .map(|(k, _)| k.clone())
                .filter(|k| k.as_slice() >= low));
        }
        self.shared.prev_key(key, low).await
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
    use crate::executor::encoding::{table_key_end, table_key_prefix};

    /// A table ID for a test's table, from its name.
    fn tid(name: &str) -> crate::executor::encoding::TableId {
        crate::executor::encoding::TableId(
            name.bytes()
                .fold(5000, |id, byte| id * 31 + u64::from(byte)),
        )
    }

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
        async fn drop_range(&self, start: &[u8], end: &[u8]) -> StorageResult<()> {
            panic!("range dropped from shared storage: {start:?}..{end:?}");
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
        let mut key = table_key_prefix(tid(table));
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

        storage.add_table("tmp", tid("tmp"));
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
            .scan(
                Some(&table_key_prefix(tid("tmp"))),
                Some(&table_key_end(tid("tmp"))),
            )
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
        storage.add_table("tmp", tid("tmp"));
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
    async fn a_session_cannot_hold_unlimited_temporary_rows() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("tmp", tid("tmp"));
        // 64 MiB at a time: four fit, the fifth does not.
        for round in 0..5u8 {
            let big = vec![RowChange::insert(
                "tmp",
                key("tmp", round),
                vec![0u8; 64 * 1024 * 1024 - 1024],
            )];
            let checked = storage.check(&big, false);
            if round < 4 {
                checked.unwrap();
                storage.apply(1, big, false);
            } else {
                let err = checked.unwrap_err();
                assert!(err.contains("is full"), "{err}");
            }
        }
        // Dropping the table gives the room back.
        assert!(storage.remove_table("tmp"));
        storage.add_table("tmp", tid("tmp"));
        storage
            .check(
                &[RowChange::insert("tmp", key("tmp", 9), vec![0u8; 1 << 20])],
                false,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn a_full_table_can_still_be_changed_and_emptied() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("tmp", tid("tmp"));
        // Three rows of 64 MiB: an update of all three would not fit if it
        // were counted as growth.
        let rows = |value: u8| -> Vec<RowChange> {
            (0..3u8)
                .map(|i| RowChange::insert("tmp", key("tmp", i), vec![value; 64 << 20]))
                .collect()
        };
        let inserted = rows(1);
        storage.check(&inserted, false).unwrap();
        storage.apply(1, inserted, false);
        let updated: Vec<RowChange> = rows(2)
            .into_iter()
            .map(|mut change| {
                change.op = ChangeOp::Update;
                change
            })
            .collect();
        storage.check(&updated, false).unwrap();
        storage.apply(2, updated, false);

        // Deleting frees the room at once: nothing is kept for a deleted row.
        let deleted: Vec<RowChange> = (0..3u8)
            .map(|i| RowChange::delete("tmp", key("tmp", i)))
            .collect();
        storage.check(&deleted, false).unwrap();
        storage.apply(3, deleted, false);
        assert_eq!(storage.temp.read().bytes, 0);
        assert!(storage.temp.read().rows.is_empty());
        let again = rows(3);
        storage.check(&again, false).unwrap();
    }

    #[tokio::test]
    async fn an_empty_or_inverted_range_holds_nothing() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("tmp", tid("tmp"));
        storage.apply(
            1,
            vec![RowChange::insert("tmp", key("tmp", 5), vec![1])],
            false,
        );
        let high = key("tmp", 9);
        let low = key("tmp", 1);
        assert!(storage
            .scan(Some(&high), Some(&low))
            .await
            .unwrap()
            .is_empty());
        assert!(storage
            .scan(Some(&low), Some(&low))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            storage.scan(Some(&low), Some(&high)).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn dropping_a_table_drops_its_rows_only() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("a", tid("a"));
        storage.add_table("b", tid("b"));
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

    #[tokio::test]
    async fn a_dropped_range_of_a_temporary_table_stays_in_the_session() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("a");
        storage.add_table("b");
        storage.apply(
            1,
            vec![
                RowChange::insert("a", key("a", 1), b"a".to_vec()),
                RowChange::insert("a", key("a", 2), b"a".to_vec()),
                RowChange::insert("b", key("b", 1), b"b".to_vec()),
            ],
            false,
        );
        let before = storage.temp.read().bytes;
        // The shared engine is not told: it would panic.
        storage
            .drop_range(&table_key_prefix("a"), &table_key_end("a"))
            .await
            .unwrap();
        assert_eq!(storage.get(&key("a", 1)).await.unwrap(), None);
        assert_eq!(storage.get(&key("a", 2)).await.unwrap(), None);
        assert!(storage.get(&key("b", 1)).await.unwrap().is_some());
        assert!(storage.temp.read().bytes < before);
        // Empty and inverted ranges hold nothing.
        storage
            .drop_range(&table_key_end("b"), &table_key_prefix("b"))
            .await
            .unwrap();
        assert!(storage.get(&key("b", 1)).await.unwrap().is_some());
    }

    #[tokio::test]
    #[should_panic(expected = "range dropped from shared storage")]
    async fn a_dropped_range_elsewhere_goes_to_the_shared_engine() {
        let storage = SessionStorage::new(Arc::new(Untouched));
        storage.add_table("a");
        let _ = storage
            .drop_range(&table_key_prefix("other"), &table_key_end("other"))
            .await;
    }
}
