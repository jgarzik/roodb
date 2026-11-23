//! LRU block cache for SSTable data and index blocks.
//!
//! Caches raw block data keyed by (sstable_name, block_offset) to avoid
//! redundant disk I/O for hot data. Self-tuning via LRU eviction.
//!
//! The cache is split into shards, each with its own lock and its own LRU
//! order, so readers on different cores rarely meet on a lock. A block lives
//! in the shard its key hashes to and each shard gets an equal share of the
//! byte budget.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

/// Cache key: (SSTable name, block offset)
type CacheKey = (Arc<str>, u64);

/// Default block cache capacity: 64MB
pub const DEFAULT_CACHE_CAPACITY: usize = 64 * 1024 * 1024;

/// Most shards a cache is split into.
const MAX_SHARDS: usize = 16;

/// A shard is only worth having if it can hold a useful number of blocks;
/// small caches get fewer shards.
const MIN_SHARD_BYTES: usize = 4 * 1024 * 1024;

/// Thread-safe LRU block cache with byte-based capacity.
pub struct BlockCache {
    shards: Box<[Mutex<Shard>]>,
    /// Total capacity as last set; each shard holds an equal share.
    capacity_bytes: AtomicUsize,
    /// Held while the capacity is changed, so two concurrent resizes cannot
    /// leave the shards at one value and `capacity_bytes` at the other.
    resize: Mutex<()>,
}

struct Shard {
    cache: LruCache<CacheKey, Arc<Vec<u8>>>,
    capacity_bytes: usize,
    current_bytes: usize,
}

impl Shard {
    fn evict_to(&mut self, limit: usize) {
        while self.current_bytes > limit {
            match self.cache.pop_lru() {
                Some((_, evicted)) => self.current_bytes -= evicted.len(),
                None => break,
            }
        }
    }
}

impl BlockCache {
    /// Create a new block cache with the given byte capacity.
    pub fn new(capacity_bytes: usize) -> Self {
        let shard_count = (capacity_bytes / MIN_SHARD_BYTES).clamp(1, MAX_SHARDS);
        let shard_capacity = capacity_bytes / shard_count;
        // Entry counts are not the limit; bytes are.
        let shards = (0..shard_count)
            .map(|_| {
                Mutex::new(Shard {
                    cache: LruCache::unbounded(),
                    capacity_bytes: shard_capacity,
                    current_bytes: 0,
                })
            })
            .collect();
        Self {
            shards,
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            resize: Mutex::new(()),
        }
    }

    fn shard(&self, sstable: &str, offset: u64) -> &Mutex<Shard> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sstable.hash(&mut hasher);
        offset.hash(&mut hasher);
        &self.shards[(hasher.finish() % self.shards.len() as u64) as usize]
    }

    /// Look up a cached block. Promotes to MRU on hit.
    pub fn get(&self, sstable: &Arc<str>, offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = (sstable.clone(), offset);
        self.shard(sstable, offset).lock().cache.get(&key).cloned()
    }

    /// Insert a block into the cache, evicting LRU entries if over capacity.
    pub fn insert(&self, sstable: &Arc<str>, offset: u64, block: Vec<u8>) {
        let block_size = block.len();
        let key = (sstable.clone(), offset);
        let value = Arc::new(block);

        let mut shard = self.shard(sstable, offset).lock();
        // A block larger than a shard's share is not cached at all.
        if block_size > shard.capacity_bytes {
            return;
        }
        let limit = shard.capacity_bytes - block_size;
        shard.evict_to(limit);

        if let Some((_old_key, old_value)) = shard.cache.push(key, value) {
            // Key already existed, subtract old size
            shard.current_bytes -= old_value.len();
        }
        shard.current_bytes += block_size;
    }

    /// Dynamically resize the cache. Evicts LRU entries if shrinking.
    pub fn set_capacity(&self, new_capacity_bytes: usize) {
        let _resizing = self.resize.lock();
        if self
            .capacity_bytes
            .swap(new_capacity_bytes, Ordering::Relaxed)
            == new_capacity_bytes
        {
            return;
        }
        let shard_capacity = new_capacity_bytes / self.shards.len();
        for shard in self.shards.iter() {
            let mut shard = shard.lock();
            shard.capacity_bytes = shard_capacity;
            shard.evict_to(shard_capacity);
        }
    }

    /// Current memory usage in bytes.
    pub fn current_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().current_bytes).sum()
    }

    /// Configured capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity_bytes.load(Ordering::Relaxed)
    }

    /// Remove all cached blocks for a given SSTable (called on compaction/deletion).
    pub fn invalidate_sstable(&self, sstable: &str) {
        for shard in self.shards.iter() {
            let mut shard = shard.lock();
            // Collect keys to remove (can't mutate while iterating)
            let keys_to_remove: Vec<CacheKey> = shard
                .cache
                .iter()
                .filter(|(k, _)| k.0.as_ref() == sstable)
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys_to_remove {
                if let Some(evicted) = shard.cache.pop(&key) {
                    shard.current_bytes -= evicted.len();
                }
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
    fn test_set_capacity_shrink() {
        let cache = BlockCache::new(200);
        let name: Arc<str> = Arc::from("test.sst");

        // Insert 150 bytes total (3 x 50)
        cache.insert(&name, 0, vec![0u8; 50]);
        cache.insert(&name, 4096, vec![1u8; 50]);
        cache.insert(&name, 8192, vec![2u8; 50]);
        assert_eq!(cache.current_bytes(), 150);

        // Shrink to 80 — should evict LRU entries until under budget
        cache.set_capacity(80);
        assert_eq!(cache.capacity(), 80);
        assert!(cache.current_bytes() <= 80);
        // The most recently used entry should survive
        assert!(cache.get(&name, 8192).is_some());
    }

    #[test]
    fn test_set_capacity_grow() {
        // Start small but big enough for max_entries (capacity/64) to hold several blocks
        let cache = BlockCache::new(1024);
        let name: Arc<str> = Arc::from("test.sst");

        // Fill with 3 blocks (300 bytes)
        cache.insert(&name, 0, vec![0u8; 100]);
        cache.insert(&name, 4096, vec![1u8; 100]);
        cache.insert(&name, 8192, vec![2u8; 100]);
        assert_eq!(cache.current_bytes(), 300);

        // Shrink to 200 — evicts 1 entry
        cache.set_capacity(200);
        assert!(cache.current_bytes() <= 200);
        let bytes_after_shrink = cache.current_bytes();

        // Grow to 2048
        cache.set_capacity(2048);
        assert_eq!(cache.capacity(), 2048);

        // New inserts should succeed without evicting existing entries
        cache.insert(&name, 12288, vec![3u8; 100]);
        cache.insert(&name, 16384, vec![4u8; 100]);
        assert_eq!(cache.current_bytes(), bytes_after_shrink + 200);
        assert!(cache.get(&name, 12288).is_some());
        assert!(cache.get(&name, 16384).is_some());
    }

    #[test]
    fn test_capacity_and_current_bytes() {
        let cache = BlockCache::new(1024);
        assert_eq!(cache.capacity(), 1024);
        assert_eq!(cache.current_bytes(), 0);

        let name: Arc<str> = Arc::from("test.sst");
        cache.insert(&name, 0, vec![0u8; 100]);
        assert_eq!(cache.current_bytes(), 100);

        cache.insert(&name, 4096, vec![0u8; 200]);
        assert_eq!(cache.current_bytes(), 300);

        // Update existing entry — old size subtracted, new size added
        cache.insert(&name, 0, vec![0u8; 50]);
        assert_eq!(cache.current_bytes(), 250);
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

    #[test]
    fn large_caches_are_sharded_and_keep_to_their_budget() {
        let capacity = 64 * 1024 * 1024;
        let cache = BlockCache::new(capacity);
        assert_eq!(cache.shards.len(), MAX_SHARDS);
        assert_eq!(cache.capacity(), capacity);

        // Twice the budget in 4 KiB blocks across several tables.
        let names: Vec<Arc<str>> = (0..8).map(|i| Arc::from(format!("{i:06}.sst"))).collect();
        for i in 0..(2 * capacity / 4096) as u64 {
            cache.insert(&names[(i % 8) as usize], i * 4096, vec![0u8; 4096]);
        }
        assert!(cache.current_bytes() <= capacity);
        // Every shard took its share: the cache is close to full.
        assert!(cache.current_bytes() > capacity / 2);

        cache.set_capacity(capacity / 4);
        assert!(cache.current_bytes() <= capacity / 4);

        cache.invalidate_sstable("000003.sst");
        for i in 0..1024u64 {
            assert!(cache.get(&names[3], i * 4096).is_none());
        }
    }

    #[test]
    fn concurrent_readers_and_writers() {
        let cache = Arc::new(BlockCache::new(32 * 1024 * 1024));
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    let name: Arc<str> = Arc::from(format!("{t:06}.sst"));
                    for i in 0..20_000u64 {
                        let offset = (i % 512) * 4096;
                        cache.insert(&name, offset, vec![t as u8; 4096]);
                        if let Some(block) = cache.get(&name, offset) {
                            assert!(block.iter().all(|b| *b == t as u8));
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(cache.current_bytes() <= cache.capacity());
    }
}
