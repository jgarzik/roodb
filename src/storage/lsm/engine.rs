//! LSM-Tree storage engine implementation

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::io::scheduler::IoPriority;
use crate::io::{AsyncIO, AsyncIOFactory};
use crate::storage::error::{StorageError, StorageResult};
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
    /// Manifest for SSTable tracking
    manifest: Mutex<Manifest>,
    /// Compaction serialization - prevents concurrent compactions from racing
    /// on file deletion. Separate from manifest lock to allow reads during compaction.
    compaction_mutex: Mutex<()>,
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

        Ok(Self {
            factory,
            memtable: RwLock::new(Arc::new(Memtable::new())),
            imm_memtables: RwLock::new(Vec::new()),
            manifest: Mutex::new(manifest),
            compaction_mutex: Mutex::new(()),
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
            let mut manifest = self.manifest.lock().await;
            let name = manifest.next_sstable_name();
            let path = manifest.sstable_path(&name);
            // Manifest lock released here - sequence number is reserved
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

        // Phase 3: Re-acquire lock to update manifest
        let mut manifest = self.manifest.lock().await;
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

        // Check if compaction is needed
        if needs_l0_compaction(&manifest) {
            drop(manifest); // Release lock before compaction
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

        // Phase 1: Brief manifest lock to check and prepare compaction job
        let job = {
            let mut manifest = self.manifest.lock().await;
            prepare_l0_compaction(&mut manifest)
            // Manifest lock released here
        };

        // If no compaction needed, we're done
        let job = match job {
            Some(j) => j,
            None => return Ok(()),
        };

        // Phase 2: Execute I/O without holding manifest lock (but with compaction_mutex)
        let new_file = execute_l0_compaction(&*self.factory, &job).await?;

        // Phase 3: Re-acquire manifest lock to finalize
        let mut manifest = self.manifest.lock().await;
        finalize_l0_compaction(&mut manifest, &job, new_file)?;

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

        // Check SSTables level by level
        let manifest = self.manifest.lock().await;

        for level in manifest.levels() {
            // For L0, check all files (they can overlap)
            // For L1+, files don't overlap so we can stop at first match
            for info in &level.files {
                // Quick key range check
                if key < info.min_key.as_slice() || key > info.max_key.as_slice() {
                    continue;
                }

                let path = manifest.sstable_path(&info.name);
                let reader =
                    SstableReader::open(&*self.factory, &path, IoPriority::QueryRead).await?;

                if let Some(result) = reader.get(key).await? {
                    return Ok(Some(result));
                }
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

        // SNAPSHOT ISOLATION: Capture consistent point-in-time view of all data sources.
        // We acquire manifest lock first (async), then atomically snapshot memtables.
        // CRITICAL: Both memtable and imm_memtables must be read while holding the
        // memtable lock to prevent a race where put() rotates the memtable between
        // our two reads, making an entry temporarily invisible.
        let manifest = self.manifest.lock().await;

        // Clone Arc pointers atomically - hold memtable read lock while reading both
        let (memtable_snapshot, imm_snapshot) = {
            let mem_guard = self.memtable.read();
            let memtable_snapshot = Arc::clone(&*mem_guard);
            let imm_snapshot: Vec<Arc<Memtable>> = self.imm_memtables.read().clone();
            (memtable_snapshot, imm_snapshot)
        };

        // Collect entries from all sources
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

        // Add from SSTables (manifest still locked)
        let mut source_idx = 100; // Start at higher index for SSTables

        for (level_num, level) in manifest.levels().iter().enumerate() {
            // For L0, files can overlap and newer files have higher names
            // Collect files in order: newest first for L0, normal order for other levels
            let files: Vec<_> = if level_num == 0 {
                level.files.iter().rev().collect()
            } else {
                level.files.iter().collect()
            };

            for info in files {
                let path = manifest.sstable_path(&info.name);
                let reader =
                    SstableReader::open(&*self.factory, &path, IoPriority::QueryRead).await?;

                for (key, value) in reader.scan_range(start, end).await? {
                    all_entries.push((key, value, source_idx));
                }
                source_idx += 1;
            }
        }

        // Sort by key, then source (newer first)
        all_entries.sort_by(|a, b| match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            other => other,
        });

        // Deduplicate: keep newest version of each key, skip tombstones
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
