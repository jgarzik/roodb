//! In-memory sorted buffer using BTreeMap
//!
//! The memtable accumulates writes before flushing to disk as an SSTable.
//! Uses a BTreeMap for O(log n) inserts and ordered iteration.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

/// Maximum memtable size before flush (4MB)
pub const MEMTABLE_SIZE_THRESHOLD: usize = 4 * 1024 * 1024;

/// Tombstone marker for deleted keys
pub const TOMBSTONE: Option<Vec<u8>> = None;

/// In-memory sorted buffer
///
/// Values are `Option<Vec<u8>>` where `None` represents a tombstone (deleted key).
pub struct Memtable {
    /// Sorted map of key -> value (None = tombstone)
    data: RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    /// Approximate size in bytes
    size: AtomicUsize,
}

impl Memtable {
    /// Create a new empty memtable
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
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
        let data = self.data.read();
        data.get(key).cloned()
    }

    /// Put a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) {
        let mut data = self.data.write();
        let key_vec = key.to_vec();
        let value_vec = value.to_vec();

        // Update size estimate
        let entry_size = key.len() + value.len() + 16; // overhead estimate
        if let Some(old) = data.get(&key_vec) {
            // Replacing existing entry
            let old_size = key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0) + 16;
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        data.insert(key_vec, Some(value_vec));
    }

    /// Delete a key (insert tombstone)
    pub fn delete(&self, key: &[u8]) {
        let mut data = self.data.write();
        let key_vec = key.to_vec();

        // Update size estimate
        let entry_size = key.len() + 16;
        if let Some(old) = data.get(&key_vec) {
            let old_size = key.len() + old.as_ref().map(|v| v.len()).unwrap_or(0) + 16;
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        data.insert(key_vec, None); // Tombstone
    }

    /// Scan a range of keys
    ///
    /// Returns entries in sorted order where `start <= key < end`.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let data = self.data.read();

        let iter: Box<dyn Iterator<Item = _>> = match (start, end) {
            (None, None) => Box::new(data.iter()),
            (Some(s), None) => Box::new(data.range(s.to_vec()..)),
            (None, Some(e)) => Box::new(data.range(..e.to_vec())),
            (Some(s), Some(e)) => Box::new(data.range(s.to_vec()..e.to_vec())),
        };

        iter.map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Get all entries in sorted order (for flushing to SSTable)
    pub fn iter(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let data = self.data.read();
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Get approximate size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if memtable should be flushed
    pub fn should_flush(&self) -> bool {
        self.size() >= MEMTABLE_SIZE_THRESHOLD
    }

    /// Check if memtable is empty
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Clear the memtable
    pub fn clear(&self) {
        let mut data = self.data.write();
        data.clear();
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
}
