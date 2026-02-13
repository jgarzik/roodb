//! K-way merge iterator for sorted sources
//!
//! Used by LSM scan and compaction to merge entries from memtables
//! and SSTables efficiently using a min-heap instead of collecting
//! all entries and sorting.
//!
//! Time complexity: O(n log k) where k = number of sources,
//! vs O(n log n) for the previous collect-and-sort approach.
//! Memory: O(k) for the heap, not O(n) for a combined Vec.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// An entry in the merge heap.
///
/// Uses reversed ordering so that `BinaryHeap` (a max-heap) acts as a min-heap:
/// smallest key first, and among equal keys, lowest priority first (newest data).
struct HeapEntry {
    key: Vec<u8>,
    /// Lower priority = newer data = wins deduplication
    priority: usize,
    /// Index into the `sources` vector
    source: usize,
    /// Position of this entry within its source
    position: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed for min-heap behavior
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.priority.cmp(&self.priority))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge over pre-sorted sources using a min-heap.
///
/// Each source must yield entries in ascending key order. The merge
/// deduplicates by key, keeping the entry from the source with the
/// lowest priority value (newest data).
pub struct MergeIterator {
    sources: Vec<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    priorities: Vec<usize>,
    heap: BinaryHeap<HeapEntry>,
}

impl MergeIterator {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            priorities: Vec::new(),
            heap: BinaryHeap::new(),
        }
    }

    /// Add a pre-sorted source with a priority (lower = newer = wins dedup).
    pub fn add(&mut self, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>, priority: usize) {
        if entries.is_empty() {
            return;
        }
        let source = self.sources.len();
        self.sources.push(entries);
        self.priorities.push(priority);

        // Seed the heap with the first entry (take ownership of key)
        let key = std::mem::take(&mut self.sources[source][0].0);
        self.heap.push(HeapEntry {
            key,
            priority,
            source,
            position: 0,
        });
    }

    /// Merge all sources, deduplicating by key.
    ///
    /// Returns entries in ascending key order. For duplicate keys,
    /// keeps the entry with the lowest priority (newest). Tombstones
    /// (`None` values) are included — use [`merge_live`] to strip them.
    pub fn merge(mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut result = Vec::new();

        while let Some(entry) = self.heap.pop() {
            // Advance: seed next entry from this source
            let next_pos = entry.position + 1;
            if next_pos < self.sources[entry.source].len() {
                let key = std::mem::take(&mut self.sources[entry.source][next_pos].0);
                self.heap.push(HeapEntry {
                    key,
                    priority: self.priorities[entry.source],
                    source: entry.source,
                    position: next_pos,
                });
            }

            // Dedup: skip if same key as the last emitted entry
            let is_dup = match result.last() {
                Some((ref last_key, _)) => *last_key == entry.key,
                None => false,
            };
            if is_dup {
                continue;
            }

            // Take ownership of the value from the source
            let value = std::mem::take(&mut self.sources[entry.source][entry.position].1);
            result.push((entry.key, value));
        }

        result
    }

    /// Merge and strip tombstones.
    ///
    /// Returns only live (non-deleted) entries as `(key, value)` pairs.
    pub fn merge_live(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.merge()
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect()
    }
}

impl Default for MergeIterator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_source() {
        let mut m = MergeIterator::new();
        m.add(
            vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), Some(b"2".to_vec())),
                (b"c".to_vec(), Some(b"3".to_vec())),
            ],
            0,
        );
        let result = m.merge_live();
        assert_eq!(
            result,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn test_two_sources_no_overlap() {
        let mut m = MergeIterator::new();
        m.add(
            vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"c".to_vec(), Some(b"3".to_vec())),
            ],
            0,
        );
        m.add(
            vec![
                (b"b".to_vec(), Some(b"2".to_vec())),
                (b"d".to_vec(), Some(b"4".to_vec())),
            ],
            1,
        );
        let result = m.merge_live();
        assert_eq!(
            result,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
            ]
        );
    }

    #[test]
    fn test_dedup_newer_wins() {
        let mut m = MergeIterator::new();
        // Source 0 (newer): key "a" = "new"
        m.add(vec![(b"a".to_vec(), Some(b"new".to_vec()))], 0);
        // Source 1 (older): key "a" = "old"
        m.add(vec![(b"a".to_vec(), Some(b"old".to_vec()))], 1);
        let result = m.merge_live();
        assert_eq!(result, vec![(b"a".to_vec(), b"new".to_vec())]);
    }

    #[test]
    fn test_tombstone_hides_value() {
        let mut m = MergeIterator::new();
        // Source 0 (newer): tombstone for "a"
        m.add(vec![(b"a".to_vec(), None)], 0);
        // Source 1 (older): value for "a"
        m.add(vec![(b"a".to_vec(), Some(b"old".to_vec()))], 1);
        let result = m.merge_live();
        assert!(result.is_empty());
    }

    #[test]
    fn test_tombstone_in_merge() {
        let mut m = MergeIterator::new();
        m.add(vec![(b"a".to_vec(), None)], 0);
        m.add(vec![(b"a".to_vec(), Some(b"old".to_vec()))], 1);
        let result = m.merge();
        assert_eq!(result, vec![(b"a".to_vec(), None)]);
    }

    #[test]
    fn test_empty_sources() {
        let mut m = MergeIterator::new();
        m.add(vec![], 0);
        m.add(vec![], 1);
        let result = m.merge_live();
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_sources() {
        let m = MergeIterator::new();
        let result = m.merge_live();
        assert!(result.is_empty());
    }

    #[test]
    fn test_mixed_overlap_and_unique() {
        let mut m = MergeIterator::new();
        // Memtable (newest): overwrites "b", adds "d"
        m.add(
            vec![
                (b"b".to_vec(), Some(b"b_new".to_vec())),
                (b"d".to_vec(), Some(b"d_val".to_vec())),
            ],
            0,
        );
        // SSTable (older): original "a", "b", "c"
        m.add(
            vec![
                (b"a".to_vec(), Some(b"a_val".to_vec())),
                (b"b".to_vec(), Some(b"b_old".to_vec())),
                (b"c".to_vec(), Some(b"c_val".to_vec())),
            ],
            1,
        );
        let result = m.merge_live();
        assert_eq!(
            result,
            vec![
                (b"a".to_vec(), b"a_val".to_vec()),
                (b"b".to_vec(), b"b_new".to_vec()),
                (b"c".to_vec(), b"c_val".to_vec()),
                (b"d".to_vec(), b"d_val".to_vec()),
            ]
        );
    }

    #[test]
    fn test_many_sources() {
        let mut m = MergeIterator::new();
        for i in 0..10u8 {
            m.add(
                vec![(vec![i], Some(vec![i]))],
                i as usize,
            );
        }
        let result = m.merge_live();
        assert_eq!(result.len(), 10);
        for (i, (k, v)) in result.iter().enumerate() {
            assert_eq!(k, &vec![i as u8]);
            assert_eq!(v, &vec![i as u8]);
        }
    }

    #[test]
    fn test_three_way_dedup() {
        let mut m = MergeIterator::new();
        m.add(vec![(b"x".to_vec(), Some(b"mem".to_vec()))], 0);
        m.add(vec![(b"x".to_vec(), Some(b"imm".to_vec()))], 1);
        m.add(vec![(b"x".to_vec(), Some(b"sst".to_vec()))], 100);
        let result = m.merge_live();
        assert_eq!(result, vec![(b"x".to_vec(), b"mem".to_vec())]);
    }
}
