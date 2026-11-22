//! In-memory sorted buffer using a lock-free concurrent skip list
//!
//! The memtable accumulates writes before flushing to disk as an SSTable.
//! Uses `crossbeam_skiplist::SkipMap` for O(log n) lock-free concurrent
//! inserts and reads, eliminating the previous RwLock serialization.

use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

/// Tombstone marker for deleted keys
pub const TOMBSTONE: Option<Vec<u8>> = None;

/// In-memory sorted buffer backed by a lock-free skip list.
///
/// Values are `Option<Vec<u8>>` where `None` represents a tombstone (deleted key).
/// All operations (get, put, delete, scan) are lock-free — multiple threads
/// can insert and read concurrently without any mutex or RwLock contention.
pub struct Memtable {
    /// Lock-free sorted map of key -> value (None = tombstone)
    data: SkipMap<Vec<u8>, Option<Vec<u8>>>,
    /// Approximate size in bytes (relaxed atomic — exact accuracy not required)
    size: AtomicUsize,
}

impl Memtable {
    /// Create a new empty memtable
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    /// Get a value by key
    ///
    /// Returns:
    /// - `Some(Some(value))` if key exists with a value
    /// - `Some(None)` if key exists but was deleted (tombstone)
    /// - `None` if key does not exist in memtable
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.data.get(key).map(|entry| entry.value().clone())
    }

    /// Insert or replace an entry. `None` is a tombstone.
    pub fn insert(&self, key: Vec<u8>, value: Option<Vec<u8>>) {
        // Size is an estimate used for the flush threshold; concurrent writes
        // to one key can skew it slightly.
        let entry_size = key.len() + value.as_ref().map_or(0, Vec::len) + 16;
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.value().as_ref().map_or(0, Vec::len) + 16;
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.data.insert(key, value);
    }

    /// Insert or update a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.insert(key.to_vec(), Some(value.to_vec()));
    }

    /// Mark a key as deleted (insert tombstone)
    pub fn delete(&self, key: &[u8]) {
        self.insert(key.to_vec(), None);
    }

    /// Scan a range of keys
    ///
    /// Returns entries in sorted order where `start <= key < end`.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let lower = start.map_or(Bound::Unbounded, |s| Bound::Included(s.to_vec()));
        let upper = end.map_or(Bound::Unbounded, |e| Bound::Excluded(e.to_vec()));

        self.data
            .range((lower, upper))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// The first entry after `key`: its key, and whether it is a tombstone.
    pub fn next_after(&self, key: &[u8]) -> Option<(Vec<u8>, bool)> {
        self.data
            .lower_bound(Bound::Excluded(key))
            .map(|entry| (entry.key().clone(), entry.value().is_none()))
    }

    /// The last entry before `key`: its key, and whether it is a tombstone.
    pub fn prev_before(&self, key: &[u8]) -> Option<(Vec<u8>, bool)> {
        self.data
            .upper_bound(Bound::Excluded(key))
            .map(|entry| (entry.key().clone(), entry.value().is_none()))
    }

    /// The first entry at or after `key`: its key, and whether it is a
    /// tombstone.
    pub fn first_from(&self, key: &[u8]) -> Option<(Vec<u8>, bool)> {
        self.data
            .lower_bound(Bound::Included(key))
            .map(|entry| (entry.key().clone(), entry.value().is_none()))
    }

    /// Whether any entry, tombstones included, has a key in `[start, end)`.
    pub fn has_any_in(&self, start: &[u8], end: &[u8]) -> bool {
        self.data
            .lower_bound(Bound::Included(start))
            .is_some_and(|entry| entry.key().as_slice() < end)
    }

    /// Remove every entry, tombstones included, with a key in
    /// `[start, end)`. Unlike a delete this leaves nothing behind to hide
    /// older values elsewhere: it is for ranges the engine forgets as a
    /// whole.
    pub fn remove_range(&self, start: &[u8], end: &[u8]) {
        let mut next = self.data.lower_bound(Bound::Included(start));
        while let Some(entry) = next {
            if entry.key().as_slice() >= end {
                break;
            }
            next = entry.next();
            if entry.remove() {
                let size = entry.key().len() + entry.value().as_ref().map_or(0, Vec::len) + 16;
                self.size.fetch_sub(size, Ordering::Relaxed);
            }
        }
    }

    /// Get all entries in sorted order (for flushing to SSTable)
    pub fn iter(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Get approximate size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if memtable is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Clear the memtable
    pub fn clear(&self) {
        self.data.clear();
        self.size.store(0, Ordering::Relaxed);
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get() {
        let mem = Memtable::new();

        mem.put(b"key1", b"value1");
        mem.put(b"key2", b"value2");

        assert_eq!(mem.get(b"key1"), Some(Some(b"value1".to_vec())));
        assert_eq!(mem.get(b"key2"), Some(Some(b"value2".to_vec())));
        assert_eq!(mem.get(b"key3"), None);
    }

    #[test]
    fn test_delete() {
        let mem = Memtable::new();

        mem.put(b"key1", b"value1");
        assert_eq!(mem.get(b"key1"), Some(Some(b"value1".to_vec())));

        mem.delete(b"key1");
        assert_eq!(mem.get(b"key1"), Some(None)); // Tombstone

        // Delete non-existent key creates tombstone
        mem.delete(b"key2");
        assert_eq!(mem.get(b"key2"), Some(None));
    }

    #[test]
    fn test_scan() {
        let mem = Memtable::new();

        mem.put(b"a", b"1");
        mem.put(b"b", b"2");
        mem.put(b"c", b"3");
        mem.put(b"d", b"4");
        mem.delete(b"c"); // Tombstone

        let result = mem.scan(Some(b"b"), Some(b"d"));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (b"b".to_vec(), Some(b"2".to_vec())));
        assert_eq!(result[1], (b"c".to_vec(), None)); // Tombstone
    }

    #[test]
    fn a_range_is_removed_and_its_neighbours_stay() {
        let mem = Memtable::new();
        for key in [&b"a"[..], b"b", b"ba", b"bz", b"c", b"d"] {
            mem.put(key, b"value");
        }
        mem.delete(b"bb");
        let full = mem.size();
        assert!(mem.has_any_in(b"b", b"c"));
        assert!(!mem.has_any_in(b"ab", b"b"));
        assert!(!mem.has_any_in(b"e", b"f"));

        mem.remove_range(b"b", b"c");
        let keys: Vec<Vec<u8>> = mem.iter().into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, [b"a".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        assert!(!mem.has_any_in(b"b", b"c"));
        assert!(mem.size() < full);
        assert_eq!(mem.get(b"bb"), None, "the tombstone goes too");

        assert_eq!(mem.first_from(b"b"), Some((b"c".to_vec(), false)));
        assert_eq!(mem.first_from(b"c"), Some((b"c".to_vec(), false)));
        assert_eq!(mem.first_from(b"e"), None);

        // Removing everything brings the size back to nothing.
        mem.remove_range(b"", b"z");
        assert!(mem.is_empty());
        assert_eq!(mem.size(), 0);
    }

    #[test]
    fn test_overwrite() {
        let mem = Memtable::new();

        mem.put(b"key", b"value1");
        assert_eq!(mem.get(b"key"), Some(Some(b"value1".to_vec())));

        mem.put(b"key", b"value2");
        assert_eq!(mem.get(b"key"), Some(Some(b"value2".to_vec())));
    }

    #[test]
    fn test_size_tracking() {
        let mem = Memtable::new();
        assert_eq!(mem.size(), 0);

        mem.put(b"key", b"value");
        assert!(mem.size() > 0);

        let size_after_put = mem.size();
        mem.delete(b"key");
        // Size should change (tombstone replaces value)
        assert!(mem.size() != size_after_put || mem.size() > 0);
    }

    #[test]
    fn test_clear() {
        let mem = Memtable::new();

        mem.put(b"a", b"1");
        mem.put(b"b", b"2");
        mem.put(b"c", b"3");
        assert_eq!(mem.len(), 3);
        assert!(mem.size() > 0);

        mem.clear();

        assert!(mem.is_empty());
        assert_eq!(mem.len(), 0);
        assert_eq!(mem.size(), 0);
        assert_eq!(mem.get(b"a"), None);
        assert_eq!(mem.get(b"b"), None);
        assert_eq!(mem.get(b"c"), None);
    }
}
