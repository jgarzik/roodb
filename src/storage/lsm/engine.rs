//! LSM-Tree storage engine implementation
//!
//! Writes go to an in-memory memtable. A full memtable is frozen and written
//! out as a table in level 0; compaction then merges tables down the levels.
//! There is no log here: what makes a write durable before its memtable is
//! flushed is the Raft log, which is replayed from the last flushed position
//! after a restart.
//!
//! Order of operations, which is what makes a crash at any point safe:
//! - A new table is written and synced, then recorded in the manifest, and
//!   only then is the memtable it came from (or the tables it replaces)
//!   let go.
//! - The manifest is replaced atomically. Files it does not list are deleted
//!   at startup.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::task::JoinHandle;

use crate::io::{IoEngine, Urgency};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::block::check_entry_size;
use crate::storage::lsm::block_cache::BlockCache;
use crate::storage::lsm::compaction::{
    apply_compaction, execute_compaction, pick_compaction, CompactionLimits, L0_COMPACTION_TRIGGER,
    L1_BASE_SIZE, LAST_LEVEL,
};
use crate::storage::lsm::manifest::{persist, Manifest, SstableInfo};
use crate::storage::lsm::memtable::Memtable;
use crate::storage::lsm::merge_iter::MergeIterator;
use crate::storage::lsm::sstable::{SstableReader, SstableWriter};
use crate::storage::traits::{KeyValue, StorageEngine, WriteOp};

/// Default total memory budget: 128MB (block cache + memtables share this pool)
pub const DEFAULT_TOTAL_CACHE_BYTES: usize = 128 * 1024 * 1024;

/// Minimum block cache size to prevent starvation under heavy write pressure
const MIN_BLOCK_CACHE_BYTES: usize = 4 * 1024 * 1024;

/// Frozen memtables allowed to wait for the background flusher. At this many,
/// writers pause until one has been written out, which bounds memory when
/// writes outrun the disk.
const MAX_FROZEN_MEMTABLES: usize = 4;

/// How long the background worker waits before retrying after a failure.
const BACKGROUND_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// A point at which a flush or compaction can be made to stop, leaving the
/// directory as a crash at that moment would. Used by tests that reopen the
/// directory afterwards; never set in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// A flushed table is on disk but not yet in the manifest.
    FlushAfterTableWritten,
    /// A compaction's output is on disk but not yet in the manifest.
    CompactionAfterOutputWritten,
    /// The manifest lists the compaction's output; its inputs are not yet
    /// deleted.
    CompactionAfterManifestSaved,
}

/// LSM-Tree storage engine configuration
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Data directory
    pub dir: PathBuf,
    /// Unified memory budget shared between block cache and memtables.
    /// Block cache capacity is dynamically adjusted based on memtable pressure.
    /// Memtable flush threshold is derived as `total_cache_bytes / 2`.
    pub total_cache_bytes: usize,
    /// Number of level-0 tables that triggers compaction into level 1.
    pub l0_compaction_trigger: usize,
    /// Size of level 1 that triggers compaction into level 2; each deeper
    /// level is allowed ten times more.
    pub level_base_bytes: u64,
    /// Stop at this point as a crash would (tests only).
    pub crash_point: Option<CrashPoint>,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            total_cache_bytes: DEFAULT_TOTAL_CACHE_BYTES,
            l0_compaction_trigger: L0_COMPACTION_TRIGGER,
            level_base_bytes: L1_BASE_SIZE,
            crash_point: None,
        }
    }
}

/// Everything a read looks at, captured at one instant, in search order.
struct ReadSources {
    active: Arc<Memtable>,
    /// Frozen memtables, oldest first.
    frozen: Vec<Arc<Memtable>>,
    /// Tables, newest data first: level 0 newest to oldest, then each level.
    tables: Vec<Arc<SstableReader>>,
}

/// LSM-Tree storage engine.
///
/// A background task writes frozen memtables out and compacts; it stops when
/// the engine is closed or dropped.
pub struct LsmEngine {
    inner: Arc<Inner>,
    worker: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct Inner {
    io: IoEngine,
    /// Active memtable for writes (wrapped in RwLock for atomic rotation)
    memtable: RwLock<Arc<Memtable>>,
    /// Frozen memtables waiting to be written out, oldest first.
    imm_memtables: RwLock<Vec<Arc<Memtable>>>,
    /// Which tables exist at which level.
    manifest: RwLock<Manifest>,
    /// Held while the manifest is changed and written, so changes reach disk
    /// in the order they were made.
    manifest_io: TokioMutex<()>,
    /// Held while memtables are being written out, so that they reach disk
    /// one at a time and oldest first.
    flush_lock: TokioMutex<()>,
    /// One compaction at a time: two could otherwise pick the same input.
    compaction_lock: TokioMutex<()>,
    /// Total memory budget (block cache + memtables)
    total_cache_bytes: usize,
    /// Block cache for SSTable data blocks
    block_cache: Arc<BlockCache>,
    /// An open reader for every table in the manifest. A table is added here
    /// before it is added to the manifest and removed after it is removed
    /// there, so anything the manifest lists can be found here.
    reader_cache: RwLock<HashMap<Arc<str>, Arc<SstableReader>>>,
    limits: CompactionLimits,
    crash_point: Option<CrashPoint>,
    /// Wakes the background worker: a memtable was frozen, or it is time to
    /// stop. Shared with the worker, which otherwise holds the engine only
    /// weakly so that dropping the engine lets it go.
    work: Arc<Notify>,
    /// Signalled each time a frozen memtable has been written out.
    flushed: Notify,
    /// The last background flush or compaction failed with this error. Paused
    /// writers report it instead of waiting on a disk that is not coming back.
    background_error: parking_lot::Mutex<Option<String>>,
    /// The worker should exit.
    stopping: AtomicBool,
    /// Closed flag
    closed: AtomicBool,
}

impl LsmEngine {
    /// Open the engine on a directory, creating it if needed.
    pub async fn open(io: IoEngine, config: LsmConfig) -> StorageResult<Self> {
        let inner = Arc::new(Inner::open(io, config).await?);
        let worker = tokio::spawn(background_worker(
            Arc::downgrade(&inner),
            Arc::clone(&inner.work),
        ));
        Ok(Self {
            inner,
            worker: parking_lot::Mutex::new(Some(worker)),
        })
    }

    /// Current block cache capacity in bytes (for testing/observability).
    pub fn block_cache_capacity(&self) -> usize {
        self.inner.block_cache.capacity()
    }

    /// Stop the background worker and wait for it to finish what it is doing.
    async fn stop_worker(&self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        self.inner.work.notify_one();
        let worker = self.worker.lock().take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        // Without close(): tell the worker to go. It holds the engine only
        // while working, so it will not keep the files open.
        self.inner.stopping.store(true, Ordering::SeqCst);
        self.inner.work.notify_one();
    }
}

/// Write frozen memtables out and compact, whenever woken.
async fn background_worker(engine: Weak<Inner>, work: Arc<Notify>) {
    loop {
        work.notified().await;
        loop {
            let Some(inner) = engine.upgrade() else {
                return;
            };
            if inner.stopping.load(Ordering::SeqCst) {
                return;
            }
            match inner.background_pass().await {
                Ok(()) => {
                    *inner.background_error.lock() = None;
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, "background flush failed; will retry");
                    *inner.background_error.lock() = Some(e.to_string());
                    // Let paused writers see the error.
                    inner.flushed.notify_waiters();
                    drop(inner);
                    tokio::time::sleep(BACKGROUND_RETRY).await;
                }
            }
        }
    }
}

impl Inner {
    async fn open(io: IoEngine, config: LsmConfig) -> StorageResult<Self> {
        std::fs::create_dir_all(&config.dir)?;

        let manifest = Manifest::open(&config.dir)?;
        let total_cache_bytes = config.total_cache_bytes;
        // Memtable starts empty, so full budget goes to block cache
        let block_cache = Arc::new(BlockCache::new(total_cache_bytes));

        let mut reader_map: HashMap<Arc<str>, Arc<SstableReader>> = HashMap::new();
        for level in manifest.levels() {
            for info in &level.files {
                let path = manifest.sstable_path(&info.name);
                // A table the manifest lists holds data nothing else has. If
                // it is missing or damaged, refuse to start rather than serve
                // what is left as if it were everything.
                let reader = SstableReader::open_with_cache(
                    &io,
                    &path,
                    Urgency::Foreground,
                    Some(block_cache.clone()),
                )
                .await
                .map_err(|e| {
                    StorageError::Manifest(format!(
                        "table {} listed in the manifest cannot be opened: {e}",
                        path.display()
                    ))
                })?;
                // Warm the cache with small tables (< 25% of budget).
                if info.size < (total_cache_bytes / 4) as u64 {
                    reader.eager_load_blocks().await?;
                }
                reader_map.insert(info.name.as_str().into(), Arc::new(reader));
            }
        }

        Ok(Self {
            io,
            memtable: RwLock::new(Arc::new(Memtable::new())),
            imm_memtables: RwLock::new(Vec::new()),
            manifest: RwLock::new(manifest),
            manifest_io: TokioMutex::new(()),
            flush_lock: TokioMutex::new(()),
            compaction_lock: TokioMutex::new(()),
            total_cache_bytes,
            block_cache,
            reader_cache: RwLock::new(reader_map),
            limits: CompactionLimits {
                l0_trigger: config.l0_compaction_trigger,
                level_base_bytes: config.level_base_bytes,
            },
            crash_point: config.crash_point,
            work: Arc::new(Notify::new()),
            flushed: Notify::new(),
            background_error: parking_lot::Mutex::new(None),
            stopping: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    fn check_open(&self) -> StorageResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }
        Ok(())
    }

    /// Stop here if the configured crash point says so.
    fn crash_at(&self, point: CrashPoint) -> StorageResult<()> {
        if self.crash_point == Some(point) {
            return Err(StorageError::StdIo(std::io::Error::other(format!(
                "stopped at crash point {point:?}"
            ))));
        }
        Ok(())
    }

    /// Change the manifest and make the change durable.
    ///
    /// Readers see the change as soon as it is made; callers must not discard
    /// anything the old manifest still needs (a memtable, compaction inputs)
    /// until this returns.
    async fn commit_manifest(&self, change: impl FnOnce(&mut Manifest)) -> StorageResult<()> {
        let _writing = self.manifest_io.lock().await;
        let (dir, data) = {
            let mut manifest = self.manifest.write();
            change(&mut manifest);
            (manifest.dir().to_path_buf(), manifest.snapshot())
        };
        persist(&self.io, &dir, &data).await
    }

    /// Open a reader on a table this engine has just written and register it,
    /// which must happen before the table is added to the manifest.
    async fn register_table(
        &self,
        info: &SstableInfo,
        path: &std::path::Path,
    ) -> StorageResult<()> {
        let reader = SstableReader::open_with_cache(
            &self.io,
            path,
            Urgency::Foreground,
            Some(self.block_cache.clone()),
        )
        .await?;
        // Fresh data is the data most likely to be read next.
        reader.eager_load_blocks().await?;
        self.reader_cache
            .write()
            .insert(info.name.as_str().into(), Arc::new(reader));
        Ok(())
    }

    /// Write one frozen memtable out as a level-0 table.
    async fn flush_memtable(&self, mem: &Memtable, urgency: Urgency) -> StorageResult<()> {
        let entries = mem.iter();
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(());
        };
        let min_key = first.0.clone();
        let max_key = last.0.clone();

        let (name, path) = {
            let mut manifest = self.manifest.write();
            let name = manifest.next_sstable_name();
            let path = manifest.sstable_path(&name);
            (name, path)
        };

        let mut writer = SstableWriter::create(&self.io, &path, urgency).await?;
        for (key, value) in entries {
            writer.add(key, value).await?;
        }
        writer.finish().await?;
        self.crash_at(CrashPoint::FlushAfterTableWritten)?;

        let info = SstableInfo {
            name,
            min_key,
            max_key,
            size: std::fs::metadata(&path)?.len(),
        };
        self.register_table(&info, &path).await?;
        self.commit_manifest(|manifest| manifest.add_sstable(0, info))
            .await
    }

    /// Whether any level is over its limit.
    fn compaction_due(&self) -> bool {
        let manifest = self.manifest.read();
        (0..LAST_LEVEL).any(|level| self.limits.needs_compaction(&manifest, level))
    }

    /// Run compactions until no level is over its limit.
    async fn compact(&self) -> StorageResult<()> {
        let _compacting = self.compaction_lock.lock().await;
        loop {
            let job = {
                let mut manifest = self.manifest.write();
                pick_compaction(&mut manifest, &self.limits)
            };
            let Some(job) = job else {
                return Ok(());
            };
            tracing::debug!(
                source = job.source_level,
                target = job.target_level,
                source_files = job.source_files.len(),
                target_files = job.target_files.len(),
                drop_tombstones = job.drop_tombstones,
                "compaction"
            );

            let output = execute_compaction(&self.io, &job).await?;
            self.crash_at(CrashPoint::CompactionAfterOutputWritten)?;

            // The output becomes visible, and durable, before any input is
            // touched. From here a crash leaves the inputs as unlisted files,
            // which startup deletes.
            if let Some(info) = &output {
                self.register_table(info, &job.output_path).await?;
            }
            self.commit_manifest(|manifest| apply_compaction(manifest, &job, output))
                .await?;
            self.crash_at(CrashPoint::CompactionAfterManifestSaved)?;

            {
                let mut readers = self.reader_cache.write();
                for name in job.input_names() {
                    readers.remove(name);
                    self.block_cache.invalidate_sstable(name);
                }
            }
            let dir = self.manifest.read().dir().to_path_buf();
            for name in job.input_names() {
                // A reader that captured this table before the manifest
                // changed keeps its open handle and can finish. If the delete
                // fails the file is unlisted and startup will remove it.
                if let Err(e) = std::fs::remove_file(dir.join(name)) {
                    tracing::warn!(file = name, error = %e, "failed to delete compacted table");
                }
            }
        }
    }

    /// Apply writes to the active memtable as one unit.
    ///
    /// The memtable lock is held (shared) across every insert. Rotation takes
    /// it exclusively, so a memtable is never frozen and written out with part
    /// of a batch in it or while an insert is still on its way, and what
    /// reaches disk is always whole batches.
    async fn write(&self, batch: impl IntoIterator<Item = WriteOp>) -> StorageResult<()> {
        self.check_open()?;
        self.wait_for_flusher().await?;
        // Refuse what the table format cannot hold before any of the batch is
        // applied, so a batch is never half written.
        let batch: Vec<WriteOp> = batch.into_iter().collect();
        for (key, value) in &batch {
            check_entry_size(key.len(), value.as_ref().map_or(0, Vec::len))?;
        }
        let full = {
            let memtable = self.memtable.read();
            for (key, value) in batch {
                memtable.insert(key, value);
            }
            memtable.size() >= self.memtable_limit()
        };
        if full {
            self.rotate(self.memtable_limit());
            self.rebalance_cache();
            self.work.notify_one();
        }
        Ok(())
    }

    /// Pause while too many frozen memtables are waiting to be written out.
    async fn wait_for_flusher(&self) -> StorageResult<()> {
        loop {
            // Register for the wake-up before checking, so a flush that
            // finishes in between is not missed.
            let flushed = self.flushed.notified();
            if self.imm_memtables.read().len() < MAX_FROZEN_MEMTABLES {
                return Ok(());
            }
            if let Some(error) = self.background_error.lock().clone() {
                return Err(StorageError::StdIo(std::io::Error::other(format!(
                    "writes are paused: memtables cannot be flushed: {error}"
                ))));
            }
            self.work.notify_one();
            flushed.await;
        }
    }

    /// One round of background work: write frozen memtables out, then compact.
    async fn background_pass(&self) -> StorageResult<()> {
        self.flush_frozen(Urgency::Background).await?;
        if self.compaction_due() {
            self.compact().await?;
        }
        Ok(())
    }

    /// Size at which the active memtable is frozen: half the memory budget.
    fn memtable_limit(&self) -> usize {
        self.total_cache_bytes / 2
    }

    /// Freeze the active memtable if it holds at least `min_size` bytes and
    /// start a new one.
    fn rotate(&self, min_size: usize) {
        let mut active = self.memtable.write();
        if active.is_empty() || active.size() < min_size {
            return;
        }
        let frozen = std::mem::replace(&mut *active, Arc::new(Memtable::new()));
        self.imm_memtables.write().push(frozen);
    }

    /// Write every frozen memtable out, oldest first.
    async fn flush_frozen(&self, urgency: Urgency) -> StorageResult<()> {
        let _flushing = self.flush_lock.lock().await;
        // A memtable stays readable in `imm_memtables` until its table is on
        // disk and in the manifest; only then is it removed.
        loop {
            let Some(oldest) = self.imm_memtables.read().first().cloned() else {
                return Ok(());
            };
            self.flush_memtable(&oldest, urgency).await?;
            {
                let mut frozen = self.imm_memtables.write();
                if frozen.first().is_some_and(|m| Arc::ptr_eq(m, &oldest)) {
                    frozen.remove(0);
                }
            }
            self.rebalance_cache();
            self.flushed.notify_waiters();
        }
    }

    /// Write everything in memory out - the active memtable included - then
    /// compact if due.
    async fn flush_all(&self, urgency: Urgency) -> StorageResult<()> {
        self.rotate(0);
        self.flush_frozen(urgency).await?;
        if self.compaction_due() {
            self.compact().await?;
        }
        Ok(())
    }

    /// Rebalance block cache capacity based on current memtable pressure.
    ///
    /// Gives block cache all remaining budget after subtracting active + immutable
    /// memtable sizes, with a minimum floor to prevent starvation.
    fn rebalance_cache(&self) {
        let mem_size = self.memtable.read().size();
        let imm_size: usize = self.imm_memtables.read().iter().map(|m| m.size()).sum();
        let mem_total = mem_size + imm_size;
        let new_cap = self
            .total_cache_bytes
            .saturating_sub(mem_total)
            .max(MIN_BLOCK_CACHE_BYTES);
        self.block_cache.set_capacity(new_cap);
    }

    /// Capture what a read has to look at.
    ///
    /// Taken under the manifest and memtable locks together, so the view is of
    /// one instant: data moves only from the active memtable to a frozen one
    /// to a table, and each step is visible at its destination before it
    /// disappears from its source.
    fn read_sources(&self) -> StorageResult<ReadSources> {
        let manifest = self.manifest.read();
        let active = self.memtable.read();
        let frozen = self.imm_memtables.read().clone();
        let readers = self.reader_cache.read();

        let mut tables = Vec::new();
        for (level_num, level) in manifest.levels().iter().enumerate() {
            let mut push = |info: &SstableInfo| -> StorageResult<()> {
                let reader = readers.get(info.name.as_str()).ok_or_else(|| {
                    StorageError::Manifest(format!("no open reader for table {}", info.name))
                })?;
                tables.push(Arc::clone(reader));
                Ok(())
            };
            if level_num == 0 {
                // Level 0 files overlap; the newest was added last.
                for info in level.files.iter().rev() {
                    push(info)?;
                }
            } else {
                for info in &level.files {
                    push(info)?;
                }
            }
        }

        Ok(ReadSources {
            active: Arc::clone(&active),
            frozen,
            tables,
        })
    }

    /// Look a key up everywhere, newest data first. `Some(None)` means the
    /// newest entry for the key is a tombstone.
    async fn read_all(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        let sources = self.read_sources()?;

        if let Some(result) = sources.active.get(key) {
            return Ok(Some(result));
        }
        for mem in sources.frozen.iter().rev() {
            if let Some(result) = mem.get(key) {
                return Ok(Some(result));
            }
        }
        for table in &sources.tables {
            if !table.may_contain(key) {
                continue;
            }
            if let Some(result) = table.get(key).await? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}

impl Inner {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.check_open()?;
        // A tombstone and a key that was never written read the same.
        Ok(self.read_all(key).await?.flatten())
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        self.check_open()?;

        let t_start = std::time::Instant::now();
        let sources = self.read_sources()?;
        let lock_ns = t_start.elapsed().as_nanos() as u64;

        // K-way merge of pre-sorted sources; the lowest priority number wins
        // where two sources hold the same key.
        let mut merger = MergeIterator::new();
        let mut priority = 0;
        merger.add(sources.active.scan(start, end), priority);
        for mem in sources.frozen.iter().rev() {
            priority += 1;
            merger.add(mem.scan(start, end), priority);
        }
        for table in &sources.tables {
            priority += 1;
            merger.add(table.scan_range_batched(start, end).await?, priority);
        }

        // Merge, dedup, and strip tombstones in O(n log k)
        let result: Vec<KeyValue> = merger.merge_live();

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
}

#[async_trait]
impl StorageEngine for LsmEngine {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.inner
            .write(std::iter::once((key.to_vec(), Some(value.to_vec()))))
            .await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.inner
            .write(std::iter::once((key.to_vec(), None)))
            .await
    }

    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
        self.inner.write(batch).await
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        self.inner.scan(start, end).await
    }

    async fn flush(&self) -> StorageResult<()> {
        self.inner.check_open()?;
        self.inner.flush_all(Urgency::Background).await
    }

    async fn flush_critical(&self) -> StorageResult<()> {
        self.inner.check_open()?;
        self.inner.flush_all(Urgency::Critical).await
    }

    async fn close(&self) -> StorageResult<()> {
        // Stop background work, write out what is in memory, then refuse
        // further use.
        self.stop_worker().await;
        self.inner.flush_all(Urgency::Background).await?;
        self.inner.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}
