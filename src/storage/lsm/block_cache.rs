//! LRU block cache for SSTable data and index blocks.
//!
//! Caches raw block data keyed by (sstable_name, block_offset) to avoid
//! redundant disk I/O for hot data. Self-tuning via LRU eviction.

use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

/// Cache key: (sstable_name, block_offset)
type CacheKey = (Arc<str>, u64);

/// Default cache capacity: 64MB (enough for typical benchmark datasets)
pub const DEFAULT_CACHE_CAPACITY: usize = 64 * 1024 * 1024;

/// LRU block cache shared across SSTable readers.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    cache: LruCache<CacheKey, Arc<Vec<u8>>>,
    capacity_bytes: usize,
    current_bytes: usize,
}

impl BlockCache {
    /// Create a new block cache with the given byte capacity.
    pub fn new(capacity_bytes: usize) -> Self {
        // Use a large entry count limit; we enforce size via bytes
        let max_entries = NonZeroUsize::new(capacity_bytes / 64).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(CacheInner {
                cache: LruCache::new(max_entries),
                capacity_bytes,
                current_bytes: 0,
            }),
        }
    }

    /// Look up a cached block. Returns None on miss.
    pub fn get(&self, sstable: &Arc<str>, offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = (sstable.clone(), offset);
        let mut inner = self.inner.lock();
        inner.cache.get(&key).cloned()
    }

    /// Insert a block into the cache. Evicts LRU entries if over capacity.
    pub fn insert(&self, sstable: &Arc<str>, offset: u64, block: Vec<u8>) {
        let block_size = block.len();
        let key = (sstable.clone(), offset);
        let value = Arc::new(block);

        let mut inner = self.inner.lock();

        // Evict until we have room
        while inner.current_bytes + block_size > inner.capacity_bytes {
            if let Some((_, evicted)) = inner.cache.pop_lru() {
                inner.current_bytes -= evicted.len();
            } else {
                break; // Cache is empty
            }
        }

        // Don't cache blocks larger than the total capacity
        if block_size > inner.capacity_bytes {
            return;
        }

        if let Some((_old_key, old_value)) = inner.cache.push(key, value) {
            // Key already existed, subtract old size
            inner.current_bytes -= old_value.len();
        }
        inner.current_bytes += block_size;
    }

    /// Invalidate all entries for a given SSTable (e.g., after compaction removes it).
    pub fn invalidate_sstable(&self, sstable: &str) {
        let mut inner = self.inner.lock();
        // Collect keys to remove (can't mutate while iterating)
        let keys_to_remove: Vec<CacheKey> = inner
            .cache
            .iter()
            .filter(|(k, _)| k.0.as_ref() == sstable)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(evicted) = inner.cache.pop(&key) {
                inner.current_bytes -= evicted.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_get_insert() {
        let cache = BlockCache::new(1024);
        let name: Arc<str> = Arc::from("test.sst");

        assert!(cache.get(&name, 0).is_none());

        cache.insert(&name, 0, vec![1, 2, 3]);
        let result = cache.get(&name, 0);
        assert!(result.is_some());
        assert_eq!(&**result.unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn test_eviction() {
        // 100 byte capacity
        let cache = BlockCache::new(100);
        let name: Arc<str> = Arc::from("test.sst");

        // Insert 50 bytes
        cache.insert(&name, 0, vec![0u8; 50]);
        // Insert 60 bytes - should evict the first
        cache.insert(&name, 4096, vec![1u8; 60]);

        assert!(cache.get(&name, 0).is_none());
        assert!(cache.get(&name, 4096).is_some());
    }

    #[test]
    fn test_invalidate() {
        let cache = BlockCache::new(1024);
        let name1: Arc<str> = Arc::from("a.sst");
        let name2: Arc<str> = Arc::from("b.sst");

        cache.insert(&name1, 0, vec![1]);
        cache.insert(&name2, 0, vec![2]);

        cache.invalidate_sstable("a.sst");

        assert!(cache.get(&name1, 0).is_none());
        assert!(cache.get(&name2, 0).is_some());
    }
}
