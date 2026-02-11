//! LSM-Tree storage engine implementation

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Mutex as TokioMutex;

use crate::io::scheduler::IoPriority;
use crate::io::{AsyncIO, AsyncIOFactory};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::block_cache::{BlockCache, DEFAULT_CACHE_CAPACITY};
use crate::storage::lsm::compaction::{
    execute_l0_compaction, finalize_l0_compaction, needs_l0_compaction, prepare_l0_compaction,
};
use crate::storage::lsm::manifest::{Manifest, SstableInfo};
use crate::storage::lsm::memtable::Memtable;
use crate::storage::lsm::sstable::{SstableReader, SstableWriter};
use crate::storage::traits::{KeyValue, StorageEngine};

/// LSM-Tree storage engine configuration
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Data directory
    pub dir: PathBuf,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
        }
    }
}

/// LSM-Tree storage engine
pub struct LsmEngine<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    factory: Arc<F>,
    /// Active memtable for writes (wrapped in RwLock for atomic rotation)
    memtable: RwLock<Arc<Memtable>>,
    /// Immutable memtables being flushed
    imm_memtables: RwLock<Vec<Arc<Memtable>>>,
    /// Manifest for SSTable tracking (RwLock: readers concurrent, writers exclusive)
    manifest: RwLock<Manifest>,
    /// Compaction serialization - prevents concurrent compactions from racing
    /// on file deletion. Separate from manifest lock to allow reads during compaction.
    compaction_mutex: TokioMutex<()>,
    /// LRU block cache shared across all SSTable reads
    block_cache: Arc<BlockCache>,
    /// Cache of open SSTable readers — avoids reopening fd/parsing metadata per scan
    reader_cache: RwLock<HashMap<Arc<str>, Arc<SstableReader<IO>>>>,
    /// Closed flag
    closed: AtomicBool,
    /// Phantom for IO type
    _io: std::marker::PhantomData<IO>,
}

impl<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> LsmEngine<IO, F> {
    /// Create a new LSM engine
    pub async fn open(factory: Arc<F>, config: LsmConfig) -> StorageResult<Self> {
        // Create directory if needed
        std::fs::create_dir_all(&config.dir)?;

        // Open manifest
        let manifest = Manifest::open(&config.dir)?;
        let block_cache = Arc::new(BlockCache::new(DEFAULT_CACHE_CAPACITY));

        // Warmup: open readers for all existing SSTables and eager-load blocks
        let mut reader_map: HashMap<Arc<str>, Arc<SstableReader<IO>>> = HashMap::new();
        for level in manifest.levels() {
            for info in &level.files {
                let path = manifest.sstable_path(&info.name);
                let cache_name: Arc<str> = info.name.as_str().into();
                match SstableReader::open_with_cache(
                    &*factory,
                    &path,
                    IoPriority::QueryRead,
                    Some(block_cache.clone()),
                )
                .await
                {
                    Ok(reader) => {
                        let reader = Arc::new(reader);
                        // Eager-load blocks for small SSTables (< 16MB)
                        if info.size < (DEFAULT_CACHE_CAPACITY / 4) as u64 {
                            let _ = reader.eager_load_blocks().await;
                        }
                        reader_map.insert(cache_name, reader);
                    }
                    Err(e) => {
                        tracing::warn!(path = ?path, error = %e, "Failed to warmup SSTable reader");
                    }
                }
            }
        }

        Ok(Self {
            factory,
            memtable: RwLock::new(Arc::new(Memtable::new())),
            imm_memtables: RwLock::new(Vec::new()),
            manifest: RwLock::new(manifest),
            compaction_mutex: TokioMutex::new(()),
            block_cache,
            reader_cache: RwLock::new(reader_map),
            closed: AtomicBool::new(false),
            _io: std::marker::PhantomData,
        })
    }

    /// Flush memtable to L0 SSTable
    async fn flush_memtable(&self, mem: Arc<Memtable>) -> StorageResult<()> {
        self.flush_memtable_with_priority(mem, IoPriority::Flush)
            .await
    }

    /// Flush memtable to L0 SSTable with specified priority
    ///
    /// # Locking
    /// This function minimizes manifest lock contention by using a split-phase approach:
    /// 1. Brief lock to reserve SSTable name (increment sequence)
    /// 2. Release lock during expensive SSTable I/O operations
    /// 3. Re-acquire lock to add metadata and save
    ///
    /// This allows concurrent operations to proceed while SSTable writes happen.
    /// The sequence number reservation ensures unique file names across concurrent flushes.
    async fn flush_memtable_with_priority(
        &self,
        mem: Arc<Memtable>,
        priority: IoPriority,
    ) -> StorageResult<()> {
        if mem.is_empty() {
            return Ok(());
        }

        // Phase 1: Brief lock to reserve SSTable name
        let (name, path) = {
            let mut manifest = self.manifest.write();
            let name = manifest.next_sstable_name();
            let path = manifest.sstable_path(&name);
            (name, path)
        };

        // Phase 2: Write SSTable without holding manifest lock
        let mut writer = SstableWriter::create(&*self.factory, &path, priority).await?;

        let entries = mem.iter();
        for (key, value) in entries {
            writer.add(key, value).await?;
        }

        writer.finish().await?;

        // Get file metadata (still no lock needed)
        let size = std::fs::metadata(&path)?.len();
        let all_entries = mem.iter();
        let min_key = all_entries
            .first()
            .map(|(k, _)| k.clone())
            .unwrap_or_default();
        let max_key = all_entries
            .last()
            .map(|(k, _)| k.clone())
            .unwrap_or_default();

        // Phase 3: Re-acquire write lock to update manifest (no await under lock)
        let cache_name: Arc<str> = name.as_str().into();
        let needs_compact = {
            let mut manifest = self.manifest.write();
            manifest.add_sstable(
                0,
                SstableInfo {
                    name,
                    min_key,
                    max_key,
                    size,
                },
            );
            manifest.save()?;
            needs_l0_compaction(&manifest)
        };

        // Write-through: open reader and cache it (async, outside lock)
        let reader = SstableReader::open_with_cache(
            &*self.factory,
            &path,
            IoPriority::QueryRead,
            Some(self.block_cache.clone()),
        )
        .await?;
        let reader = Arc::new(reader);
        let _ = reader.eager_load_blocks().await;
        self.reader_cache.write().insert(cache_name, reader);

        // Check if compaction is needed
        if needs_compact {
            self.maybe_compact().await?;
        }

        Ok(())
    }

    /// Trigger compaction if needed
    ///
    /// # Locking
    /// Uses split-phase locking to minimize manifest lock contention:
    /// 1. Acquire compaction_mutex to serialize compactions (prevents file deletion races)
    /// 2. Brief manifest lock to check if compaction needed and gather job info
    /// 3. Release manifest lock during expensive I/O operations
    /// 4. Re-acquire manifest lock to finalize manifest updates
    ///
    /// The compaction_mutex ensures only one compaction runs at a time, preventing
    /// races where one compaction deletes files another is about to read.
    /// The manifest lock is released during I/O to allow concurrent reads.
    async fn maybe_compact(&self) -> StorageResult<()> {
        // Serialize compactions to prevent file deletion races
        let _compaction_guard = self.compaction_mutex.lock().await;

        // Phase 1: Brief manifest write lock to check and prepare compaction job
        let job = {
            let mut manifest = self.manifest.write();
            prepare_l0_compaction(&mut manifest)
        };

        // If no compaction needed, we're done
        let job = match job {
            Some(j) => j,
            None => return Ok(()),
        };

        // Phase 2: Execute I/O without holding manifest lock (but with compaction_mutex)
        let new_file = execute_l0_compaction(&*self.factory, &job).await?;

        // Invalidate block cache and reader cache for SSTables being removed
        {
            let mut rc = self.reader_cache.write();
            for f in &job.l0_files {
                self.block_cache.invalidate_sstable(&f.name);
                rc.remove(f.name.as_str());
            }
            for f in &job.l1_files {
                self.block_cache.invalidate_sstable(&f.name);
                rc.remove(f.name.as_str());
            }
        }

        // Write-through: open reader for new compaction output and eager-load (async, outside manifest lock)
        if let Some(ref _nf) = new_file {
            let output_path = &job.output_path;
            let cache_name: Arc<str> = job.output_name.as_str().into();
            match SstableReader::open_with_cache(
                &*self.factory,
                output_path,
                IoPriority::QueryRead,
                Some(self.block_cache.clone()),
            )
            .await
            {
                Ok(reader) => {
                    let reader = Arc::new(reader);
                    let _ = reader.eager_load_blocks().await;
                    self.reader_cache.write().insert(cache_name, reader);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to cache compaction output reader");
                }
            }
        }

        // Phase 3: Brief write lock to finalize manifest
        {
            let mut manifest = self.manifest.write();
            finalize_l0_compaction(&mut manifest, &job, new_file)?;
        }

        Ok(())
    }

    /// Read from all levels (memtable, imm memtables, SSTables)
    async fn read_all(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        // SNAPSHOT ISOLATION: Capture consistent point-in-time view of memtables.
        // CRITICAL: Both memtable and imm_memtables must be read while holding the
        // memtable lock to prevent a race where put() rotates the memtable between
        // our two reads, making an entry temporarily invisible.
        let (memtable_snapshot, imm_snapshot) = {
            let mem_guard = self.memtable.read();
            let memtable_snapshot = Arc::clone(&*mem_guard);
            let imm_snapshot: Vec<Arc<Memtable>> = self.imm_memtables.read().clone();
            (memtable_snapshot, imm_snapshot)
        };

        // Check active memtable snapshot first
        if let Some(result) = memtable_snapshot.get(key) {
            return Ok(Some(result));
        }

        // Check immutable memtable snapshots (newest first)
        for mem in imm_snapshot.iter().rev() {
            if let Some(result) = mem.get(key) {
                return Ok(Some(result));
            }
        }

        // Snapshot SSTable readers from cache under brief read lock, then release
        let readers: Vec<Arc<SstableReader<IO>>> = {
            let manifest = self.manifest.read();
            let mut readers = Vec::new();
            for (level_num, level) in manifest.levels().iter().enumerate() {
                let files: Vec<_> = if level_num == 0 {
                    level.files.iter().rev().collect()
                } else {
                    level.files.iter().collect()
                };
                for info in files {
                    if key < info.min_key.as_slice() || key > info.max_key.as_slice() {
                        continue;
                    }
                    let cache_name: Arc<str> = info.name.as_str().into();
                    if let Some(r) = self.reader_cache.read().get(&cache_name).cloned() {
                        readers.push(r);
                    }
                }
            }
            readers
        };

        // Point lookups on pre-resolved readers, lock-free
        for reader in readers {
            if let Some(result) = reader.get(key).await? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> StorageEngine
    for LsmEngine<IO, F>
{
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        match self.read_all(key).await? {
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Ok(None), // Tombstone
            None => Ok(None),       // Not found
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        // Get current memtable and insert
        let current_mem = Arc::clone(&*self.memtable.read());
        current_mem.put(key, value);

        // Check if memtable needs flushing
        if current_mem.should_flush() {
            // Rotate memtable: create new one and move old to immutables
            let new_mem = Arc::new(Memtable::new());
            {
                let mut mem_guard = self.memtable.write();
                // Only rotate if we're still looking at the same memtable.
                // Another thread may have already rotated between our should_flush check
                // and acquiring this write lock.
                if Arc::ptr_eq(&current_mem, &*mem_guard) {
                    let old_mem = std::mem::replace(&mut *mem_guard, new_mem);
                    // Push old memtable to immutables (data preserved, NOT cleared)
                    self.imm_memtables.write().push(old_mem);
                }
                // If not the same, another thread already rotated - nothing to do
            }
            // Note: Flush happens in background or on explicit flush() call
        }

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        self.memtable.read().delete(key);
        Ok(())
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        let t_start = std::time::Instant::now();

        // SNAPSHOT ISOLATION: Capture consistent point-in-time view of all data sources.
        // Brief lock: snapshot memtables + pre-resolve SSTable readers, then release.
        let (memtable_snapshot, imm_snapshot, readers) = {
            let manifest = self.manifest.read();

            // Clone Arc pointers atomically
            let mem_guard = self.memtable.read();
            let memtable_snapshot = Arc::clone(&*mem_guard);
            let imm_snapshot: Vec<Arc<Memtable>> = self.imm_memtables.read().clone();

            // Pre-resolve all SSTable readers from cache while holding manifest lock
            let mut readers: Vec<(Arc<SstableReader<IO>>, usize)> = Vec::new();
            let mut source_idx = 100usize;
            for (level_num, level) in manifest.levels().iter().enumerate() {
                let files: Vec<_> = if level_num == 0 {
                    level.files.iter().rev().collect()
                } else {
                    level.files.iter().collect()
                };
                for info in files {
                    let cache_name: Arc<str> = info.name.as_str().into();
                    let reader = {
                        let guard = self.reader_cache.read();
                        guard.get(&cache_name).cloned()
                    };
                    if let Some(r) = reader {
                        readers.push((r, source_idx));
                    }
                    // Cache miss: skip this SSTable (warmup should have loaded it)
                    source_idx += 1;
                }
            }

            (memtable_snapshot, imm_snapshot, readers)
            // manifest lock released here
        };

        let lock_ns = t_start.elapsed().as_nanos() as u64;

        // Collect entries from all sources — lock-free from here
        let mut all_entries: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = Vec::new();

        // Add from active memtable snapshot (source 0 = newest)
        for (key, value) in memtable_snapshot.scan(start, end) {
            all_entries.push((key, value, 0));
        }

        // Add from immutable memtables snapshot
        for (idx, mem) in imm_snapshot.iter().rev().enumerate() {
            for (key, value) in mem.scan(start, end) {
                all_entries.push((key, value, idx + 1));
            }
        }

        // Add from SSTables (using pre-resolved readers, no lock needed)
        for (reader, source_idx) in &readers {
            for (key, value) in reader.scan_range_batched(start, end).await? {
                all_entries.push((key, value, *source_idx));
            }
        }

        // Sort by key, then source (newer first)
        all_entries.sort_by(|a, b| match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            other => other,
        });

        // Deduplicate: keep newest version of each key, skip tombstones
        // Use swap-ownership to avoid key.clone()
        let mut result: Vec<KeyValue> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;

        for (key, value, _) in all_entries {
            if last_key.as_ref() != Some(&key) {
                if let Some(v) = value {
                    result.push((key.clone(), v));
                }
                last_key = Some(key);
            }
        }

        let io_ns = t_start.elapsed().as_nanos() as u64 - lock_ns;
        tracing::debug!(
            target: "roodb::perf",
            lock_us = lock_ns / 1000,
            io_us = io_ns / 1000,
            entries = result.len(),
            "lsm_scan"
        );

        Ok(result)
    }

    async fn flush(&self) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        // Rotate current memtable to immutable and create fresh one
        {
            let mut mem_guard = self.memtable.write();
            if !mem_guard.is_empty() {
                let old_mem = std::mem::replace(&mut *mem_guard, Arc::new(Memtable::new()));
                self.imm_memtables.write().push(old_mem);
            }
        }

        // Flush immutable memtables one at a time
        // CRITICAL: Only remove from imm_memtables AFTER successful write to disk
        // to prevent visibility gaps where entries are not in memtable, imm_memtables, or disk
        loop {
            // Peek at the oldest memtable (first in vec) but don't remove yet
            let mem = {
                let imm = self.imm_memtables.read();
                imm.first().cloned()
            };

            let mem = match mem {
                Some(m) => m,
                None => break, // No more memtables to flush
            };

            // Flush to disk (memtable still visible in imm_memtables during this)
            self.flush_memtable(Arc::clone(&mem)).await?;

            // Now safe to remove - data is on disk
            {
                let mut imm = self.imm_memtables.write();
                if imm.first().map(|m| Arc::ptr_eq(m, &mem)).unwrap_or(false) {
                    imm.remove(0);
                }
            }
        }

        Ok(())
    }

    async fn flush_critical(&self) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        // Rotate current memtable to immutable and create fresh one
        {
            let mut mem_guard = self.memtable.write();
            if !mem_guard.is_empty() {
                let old_mem = std::mem::replace(&mut *mem_guard, Arc::new(Memtable::new()));
                self.imm_memtables.write().push(old_mem);
            }
        }

        // Flush immutable memtables one at a time with Critical priority (WAL)
        // CRITICAL: Only remove from imm_memtables AFTER successful write to disk
        // to prevent visibility gaps where entries are not in memtable, imm_memtables, or disk
        loop {
            // Peek at the oldest memtable (first in vec) but don't remove yet
            let mem = {
                let imm = self.imm_memtables.read();
                imm.first().cloned()
            };

            let mem = match mem {
                Some(m) => m,
                None => break, // No more memtables to flush
            };

            // Flush to disk (memtable still visible in imm_memtables during this)
            self.flush_memtable_with_priority(Arc::clone(&mem), IoPriority::Wal)
                .await?;

            // Now safe to remove - data is on disk
            {
                let mut imm = self.imm_memtables.write();
                if imm.first().map(|m| Arc::ptr_eq(m, &mem)).unwrap_or(false) {
                    imm.remove(0);
                }
            }
        }

        Ok(())
    }

    async fn close(&self) -> StorageResult<()> {
        // Flush first, then mark as closed
        // Rotate current memtable to immutable
        {
            let mut mem_guard = self.memtable.write();
            if !mem_guard.is_empty() {
                let old_mem = std::mem::replace(&mut *mem_guard, Arc::new(Memtable::new()));
                self.imm_memtables.write().push(old_mem);
            }
        }

        // Flush immutable memtables one at a time
        // CRITICAL: Only remove from imm_memtables AFTER successful write to disk
        loop {
            let mem = {
                let imm = self.imm_memtables.read();
                imm.first().cloned()
            };

            let mem = match mem {
                Some(m) => m,
                None => break,
            };

            self.flush_memtable(Arc::clone(&mem)).await?;

            {
                let mut imm = self.imm_memtables.write();
                if imm.first().map(|m| Arc::ptr_eq(m, &mem)).unwrap_or(false) {
                    imm.remove(0);
                }
            }
        }

        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}
