//! In-memory sorted buffer using a lock-free concurrent skip list
//!
//! The memtable accumulates writes before flushing to disk as an SSTable.
//! Uses `crossbeam_skiplist::SkipMap` for O(log n) lock-free concurrent
//! inserts and reads, eliminating the previous RwLock serialization.
//!
//! A memtable also holds the key ranges forgotten by the batches written to
//! it (`WriteOp::ForgetRange`). Forgetting a range removes the memtable's
//! own entries there, and the range is kept so that reads leave out what
//! older memtables and the tables hold in it. An entry written to the range
//! afterwards is a new write: the memtable's entries are never hidden by
//! its own ranges. The ranges go to the manifest with the flush that writes
//! the memtable out.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

/// A key range `[start, end)`.
pub type KeyRange = (Vec<u8>, Vec<u8>);

/// An entry: its key, and its value or `None` for a tombstone.
pub type Entry = (Vec<u8>, Option<Vec<u8>>);

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
    /// The ranges forgotten by batches written to this memtable, start to
    /// end, kept disjoint: ranges that overlap or touch are joined (they
    /// all hide the same things, and are listed by the same flush).
    /// Forgetting a range holds this lock exclusively while it removes the
    /// range's entries, so a reader that holds it shared sees a range either
    /// not yet forgotten or forgotten and its entries gone.
    forgotten: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    /// Whether `forgotten` holds anything: saves point reads the lock.
    has_forgotten: AtomicBool,
}

impl Memtable {
    /// Create a new empty memtable
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
            forgotten: RwLock::new(BTreeMap::new()),
            has_forgotten: AtomicBool::new(false),
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

    /// Forget every key in `[start, end)`: remove the entries this memtable
    /// has there, tombstones included, and keep the range, which hides what
    /// older memtables and tables hold in it. Entries inserted afterwards
    /// are new writes and are not affected.
    pub fn forget_range(&self, start: &[u8], end: &[u8]) {
        if start >= end {
            return;
        }
        let mut forgotten = self.forgotten.write();
        if add_range(&mut forgotten, start, end) {
            self.size
                .fetch_add(start.len() + end.len() + 16, Ordering::Relaxed);
        }
        self.has_forgotten.store(true, Ordering::SeqCst);
        self.remove_range(start, end);
    }

    /// Whether a range forgotten in this memtable holds `key`: if so, and
    /// the memtable has no entry for the key, the key is gone, whatever
    /// older memtables and tables hold.
    ///
    /// To be asked after [`Self::get`] found nothing. An entry is removed
    /// only after its range is recorded, so a reader that misses an entry
    /// because it was just forgotten finds the range here.
    pub fn forgets(&self, key: &[u8]) -> bool {
        self.has_forgotten.load(Ordering::SeqCst)
            && self
                .forgotten
                .read()
                .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
                .next_back()
                .is_some_and(|(_, end)| key < end.as_slice())
    }

    /// The ranges forgotten in this memtable, in key order, no two
    /// overlapping or touching.
    pub fn forgotten(&self) -> Vec<KeyRange> {
        self.forgotten_within(None, None)
    }

    /// Those of [`Self::forgotten`] that hold a key of `[start, end)`
    /// (unbounded where `None`).
    pub fn forgotten_within(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<KeyRange> {
        if !self.has_forgotten.load(Ordering::SeqCst) {
            return Vec::new();
        }
        ranges_within(&self.forgotten.read(), start, end)
    }

    /// [`Self::scan`] together with [`Self::forgotten_within`], as of one
    /// moment: no range is in the one whose entries are still in the other.
    pub fn scan_with_forgotten(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> (Vec<Entry>, Vec<KeyRange>) {
        let forgotten = self.forgotten.read();
        (self.scan(start, end), ranges_within(&forgotten, start, end))
    }

    /// Remove every entry, tombstones included, with a key in
    /// `[start, end)`. Unlike a delete this leaves nothing behind.
    fn remove_range(&self, start: &[u8], end: &[u8]) {
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

    /// Whether the memtable holds nothing to flush: no entry and no
    /// forgotten range.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && !self.has_forgotten.load(Ordering::SeqCst)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Clear the memtable
    pub fn clear(&self) {
        let mut forgotten = self.forgotten.write();
        forgotten.clear();
        self.has_forgotten.store(false, Ordering::SeqCst);
        self.data.clear();
        self.size.store(0, Ordering::Relaxed);
    }
}

/// Add `[start, end)` to a set of disjoint ranges, joining it with those it
/// overlaps or touches. Says whether the set changed.
fn add_range(ranges: &mut BTreeMap<Vec<u8>, Vec<u8>>, start: &[u8], end: &[u8]) -> bool {
    let (mut start, mut end) = (start.to_vec(), end.to_vec());
    // The range that begins at or before this one may reach into it.
    if let Some((before, its_end)) = ranges
        .range::<[u8], _>((Bound::Unbounded, Bound::Included(&start[..])))
        .next_back()
    {
        if *its_end >= end {
            return false;
        }
        if *its_end >= start {
            start.clone_from(before);
        }
    }
    // Those that begin inside it, or right at its end, become part of it.
    let inside: Vec<Vec<u8>> = ranges
        .range::<[u8], _>((Bound::Included(&start[..]), Bound::Included(&end[..])))
        .map(|(start, _)| start.clone())
        .collect();
    for inside in inside {
        if let Some(its_end) = ranges.remove(&inside) {
            if its_end > end {
                end = its_end;
            }
        }
    }
    ranges.insert(start, end);
    true
}

/// The ranges of a disjoint set that hold a key of `[start, end)`
/// (unbounded where `None`), in key order.
fn ranges_within(
    ranges: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Vec<KeyRange> {
    if let (Some(start), Some(end)) = (start, end) {
        if start >= end {
            return Vec::new();
        }
    }
    // One range may begin before `start` and reach past it.
    let before = start.and_then(|start| {
        ranges
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(start)))
            .next_back()
            .filter(|(_, its_end)| its_end.as_slice() > start)
    });
    let lower = start.map_or(Bound::Unbounded, Bound::Included);
    let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
    before
        .into_iter()
        .chain(ranges.range::<[u8], _>((lower, upper)))
        .map(|(start, end)| (start.clone(), end.clone()))
        .collect()
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
    fn a_forgotten_range_loses_its_entries_and_is_kept() {
        let mem = Memtable::new();
        for key in [&b"a"[..], b"b", b"ba", b"bz", b"c", b"d"] {
            mem.put(key, b"value");
        }
        mem.delete(b"bb");
        let full = mem.size();
        assert!(!mem.forgets(b"b"));
        assert!(mem.forgotten().is_empty());

        mem.forget_range(b"b", b"c");
        let keys: Vec<Vec<u8>> = mem.iter().into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, [b"a".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        assert!(mem.size() < full);
        assert_eq!(mem.get(b"bb"), None, "the tombstone goes too");
        assert_eq!(mem.forgotten(), [(b"b".to_vec(), b"c".to_vec())]);
        for (key, forgotten) in [("ab", false), ("b", true), ("bzz", true), ("c", false)] {
            assert_eq!(mem.forgets(key.as_bytes()), forgotten, "{key}");
        }

        assert_eq!(mem.first_from(b"b"), Some((b"c".to_vec(), false)));
        assert_eq!(mem.first_from(b"c"), Some((b"c".to_vec(), false)));
        assert_eq!(mem.first_from(b"e"), None);

        // What is written to the range afterwards is a new write.
        mem.put(b"bb", b"new");
        assert_eq!(mem.get(b"bb"), Some(Some(b"new".to_vec())));
        let (entries, forgotten) = mem.scan_with_forgotten(Some(b"b"), Some(b"c"));
        assert_eq!(entries, [(b"bb".to_vec(), Some(b"new".to_vec()))]);
        assert_eq!(forgotten.len(), 1);
        // Forgotten again, it goes; the range is remembered once.
        mem.forget_range(b"b", b"c");
        assert_eq!(mem.get(b"bb"), None);
        assert_eq!(mem.forgotten().len(), 1);
        let size = mem.size();
        mem.forget_range(b"ba", b"bb");
        assert_eq!(
            mem.size(),
            size,
            "a range inside a remembered one adds nothing"
        );

        // Empty and inverted ranges hold nothing.
        mem.forget_range(b"d", b"d");
        mem.forget_range(b"z", b"a");
        assert_eq!(mem.forgotten().len(), 1);
        assert_eq!(mem.get(b"d"), Some(Some(b"value".to_vec())));
    }

    #[test]
    fn forgotten_ranges_that_overlap_or_touch_are_joined() {
        let ranges = |mem: &Memtable| -> Vec<(String, String)> {
            mem.forgotten()
                .into_iter()
                .map(|(start, end)| {
                    (
                        String::from_utf8(start).unwrap(),
                        String::from_utf8(end).unwrap(),
                    )
                })
                .collect()
        };
        let pair = |start: &str, end: &str| (start.to_string(), end.to_string());
        let mem = Memtable::new();
        mem.forget_range(b"m", b"p");
        mem.forget_range(b"c", b"e");
        mem.forget_range(b"t", b"v");
        assert_eq!(
            ranges(&mem),
            [pair("c", "e"), pair("m", "p"), pair("t", "v")]
        );
        // Inside one, and the same one again: nothing new.
        mem.forget_range(b"n", b"o");
        mem.forget_range(b"m", b"p");
        assert_eq!(ranges(&mem).len(), 3);
        // Touching at either end, and overlapping at either end.
        mem.forget_range(b"e", b"g");
        mem.forget_range(b"k", b"m");
        assert_eq!(
            ranges(&mem),
            [pair("c", "g"), pair("k", "p"), pair("t", "v")]
        );
        mem.forget_range(b"b", b"d");
        mem.forget_range(b"o", b"q");
        assert_eq!(
            ranges(&mem),
            [pair("b", "g"), pair("k", "q"), pair("t", "v")]
        );
        // Same start, longer.
        mem.forget_range(b"t", b"w");
        assert_eq!(ranges(&mem)[2], pair("t", "w"));

        for (key, forgotten) in [
            ("a", false),
            ("b", true),
            ("fz", true),
            ("g", false),
            ("j", false),
            ("k", true),
            ("pz", true),
            ("q", false),
            ("t", true),
            ("w", false),
        ] {
            assert_eq!(mem.forgets(key.as_bytes()), forgotten, "{key}");
        }

        // Only the ranges that hold a key of the stretch asked about.
        let within = |start: Option<&str>, end: Option<&str>| -> Vec<String> {
            mem.forgotten_within(start.map(str::as_bytes), end.map(str::as_bytes))
                .into_iter()
                .map(|(start, _)| String::from_utf8(start).unwrap())
                .collect()
        };
        assert_eq!(within(None, None), ["b", "k", "t"]);
        assert_eq!(within(Some("g"), Some("k")), Vec::<String>::new());
        assert_eq!(within(Some("f"), Some("k")), ["b"]);
        assert_eq!(within(Some("g"), Some("l")), ["k"]);
        assert_eq!(within(Some("c"), Some("u")), ["b", "k", "t"]);
        assert_eq!(within(Some("l"), None), ["k", "t"]);
        assert_eq!(within(None, Some("k")), ["b"]);
        assert_eq!(within(Some("m"), Some("m")), Vec::<String>::new());
        assert_eq!(within(Some("u"), Some("c")), Vec::<String>::new());

        // One over all of them leaves one.
        mem.forget_range(b"a", b"z");
        assert_eq!(ranges(&mem), [pair("a", "z")]);
    }

    #[test]
    fn a_memtable_with_only_a_forgotten_range_is_not_empty() {
        let mem = Memtable::new();
        assert!(mem.is_empty());
        mem.forget_range(b"b", b"c");
        assert!(!mem.is_empty(), "the range has to be flushed");
        assert!(mem.size() > 0);
        assert!(mem.iter().is_empty());

        mem.clear();
        assert!(mem.is_empty());
        assert!(mem.forgotten().is_empty());
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
