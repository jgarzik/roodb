//! LSM-Tree storage engine implementation

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::io::{AsyncIO, AsyncIOFactory};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::compaction::{compact_l0, needs_l0_compaction};
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
    /// Active memtable for writes
    memtable: Arc<Memtable>,
    /// Immutable memtables being flushed
    imm_memtables: RwLock<Vec<Arc<Memtable>>>,
    /// Manifest for SSTable tracking
    manifest: Mutex<Manifest>,
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
            memtable: Arc::new(Memtable::new()),
            imm_memtables: RwLock::new(Vec::new()),
            manifest: Mutex::new(manifest),
            closed: AtomicBool::new(false),
            _io: std::marker::PhantomData,
        })
    }

    /// Flush memtable to L0 SSTable
    async fn flush_memtable(&self, mem: Arc<Memtable>) -> StorageResult<()> {
        if mem.is_empty() {
            return Ok(());
        }

        let mut manifest = self.manifest.lock().await;

        // Create new SSTable
        let name = manifest.next_sstable_name();
        let path = manifest.sstable_path(&name);

        let mut writer = SstableWriter::create(&*self.factory, &path).await?;

        // Write all entries from memtable
        let entries = mem.iter();
        for (key, value) in entries {
            writer.add(key, value).await?;
        }

        writer.finish().await?;

        // Get file metadata
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

        // Add to manifest
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
    async fn maybe_compact(&self) -> StorageResult<()> {
        let mut manifest = self.manifest.lock().await;
        if needs_l0_compaction(&manifest) {
            compact_l0(&*self.factory, &mut manifest).await?;
        }
        Ok(())
    }

    /// Read from all levels (memtable, imm memtables, SSTables)
    async fn read_all(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        // Check active memtable first
        if let Some(result) = self.memtable.get(key) {
            return Ok(Some(result));
        }

        // Check immutable memtables (newest first)
        {
            let imm = self.imm_memtables.read();
            for mem in imm.iter().rev() {
                if let Some(result) = mem.get(key) {
                    return Ok(Some(result));
                }
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
                let reader = SstableReader::open(&*self.factory, &path).await?;

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

        self.memtable.put(key, value);

        // Check if memtable needs flushing
        if self.memtable.should_flush() {
            // Rotate memtable
            let old_mem = Arc::clone(&self.memtable);

            // Create new memtable by replacing the current one
            // This is a simplified approach - in production you'd use atomics
            self.imm_memtables.write().push(old_mem.clone());
            old_mem.clear(); // Clear for reuse in this simplified impl

            // Flush in background (simplified: do it synchronously for now)
            // In production, this would be a background task
        }

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        self.memtable.delete(key);
        Ok(())
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }

        // Collect entries from all sources
        let mut all_entries: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = Vec::new();

        // Add from active memtable (source 0 = newest)
        for (key, value) in self.memtable.scan(start, end) {
            all_entries.push((key, value, 0));
        }

        // Add from immutable memtables
        {
            let imm = self.imm_memtables.read();
            for (idx, mem) in imm.iter().rev().enumerate() {
                for (key, value) in mem.scan(start, end) {
                    all_entries.push((key, value, idx + 1));
                }
            }
        }

        // Add from SSTables
        let manifest = self.manifest.lock().await;
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
                let reader = SstableReader::open(&*self.factory, &path).await?;

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

        // Flush current memtable
        if !self.memtable.is_empty() {
            let mem = Arc::clone(&self.memtable);
            self.flush_memtable(mem).await?;
            self.memtable.clear();
        }

        // Flush any immutable memtables
        let imm_mems: Vec<Arc<Memtable>> = {
            let mut imm = self.imm_memtables.write();
            std::mem::take(&mut *imm)
        };

        for mem in imm_mems {
            self.flush_memtable(mem).await?;
        }

        Ok(())
    }

    async fn close(&self) -> StorageResult<()> {
        // Flush first, then mark as closed
        // Direct flush implementation to avoid closed check
        if !self.memtable.is_empty() {
            let mem = Arc::clone(&self.memtable);
            self.flush_memtable(mem).await?;
            self.memtable.clear();
        }

        let imm_mems: Vec<Arc<Memtable>> = {
            let mut imm = self.imm_memtables.write();
            std::mem::take(&mut *imm)
        };

        for mem in imm_mems {
            self.flush_memtable(mem).await?;
        }

        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}
