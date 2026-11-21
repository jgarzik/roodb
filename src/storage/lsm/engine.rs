//! LSM-Tree storage engine implementation
//!
//! Writes go to an in-memory memtable. A full memtable is frozen and written
//! out as a table in level 0; compaction then merges tables down the levels.
//! There is no log here. A write that has not been flushed yet exists only in
//! memory as far as this module is concerned; making it survive a crash is
//! the job of the Raft log, which is replayed from the last flushed position.
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
    apply_compaction, execute_compaction, pick_compaction, CompactionJob, CompactionLimits,
    L0_COMPACTION_TRIGGER, L1_BASE_SIZE, LAST_LEVEL,
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

/// How long a background task waits before retrying after a failure.
const BACKGROUND_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// File whose lock marks a data directory as in use.
const LOCK_FILE: &str = "LOCK";

/// How long `open` waits for a directory that is still locked. An engine
/// dropped without `close()` releases the lock as soon as its background
/// tasks finish what they were doing.
const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

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
/// Two background tasks do the slow work: one writes frozen memtables out,
/// the other compacts. They are separate so that a long compaction never
/// holds up a flush that writers are waiting for. Both stop when the engine
/// is closed or dropped.
pub struct LsmEngine {
    inner: Arc<Inner>,
    workers: parking_lot::Mutex<Vec<JoinHandle<()>>>,
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
    /// Wakes the flush task: a memtable was frozen, or it is time to stop.
    /// Shared with the task, which otherwise holds the engine only weakly so
    /// that dropping the engine lets it go.
    work: Arc<Notify>,
    /// Wakes the compaction task.
    compact_work: Arc<Notify>,
    /// Exclusive lock on the data directory, held until the engine is closed
    /// or dropped.
    dir_lock: parking_lot::Mutex<Option<std::fs::File>>,
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
        let workers = vec![
            tokio::spawn(background_task(
                Arc::downgrade(&inner),
                Arc::clone(&inner.work),
                Work::Flush,
            )),
            tokio::spawn(background_task(
                Arc::downgrade(&inner),
                Arc::clone(&inner.compact_work),
                Work::Compact,
            )),
        ];
        Ok(Self {
            inner,
            workers: parking_lot::Mutex::new(workers),
        })
    }

    /// Current block cache capacity in bytes (for testing/observability).
    pub fn block_cache_capacity(&self) -> usize {
        self.inner.block_cache.capacity()
    }

    /// Stop the background tasks and wait for them to finish what they are
    /// doing.
    async fn stop_workers(&self) {
        self.inner.stop();
        let workers = std::mem::take(&mut *self.workers.lock());
        for worker in workers {
            let _ = worker.await;
        }
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        // Without close(): tell the tasks to go. They hold the engine only
        // while working and stop before starting another table, so the files
        // and the directory lock are released promptly.
        self.inner.stop();
    }
}

/// What a background task does when woken.
#[derive(Clone, Copy, Debug)]
enum Work {
    Flush,
    Compact,
}

/// Run one kind of background work whenever woken, retrying after failures.
async fn background_task(engine: Weak<Inner>, wake: Arc<Notify>, work: Work) {
    loop {
        wake.notified().await;
        loop {
            let Some(inner) = engine.upgrade() else {
                return;
            };
            if inner.stopping.load(Ordering::SeqCst) {
                return;
            }
            let result = match work {
                Work::Flush => inner.background_flush().await,
                Work::Compact => inner.compact(true).await,
            };
            match result {
                Ok(()) => {
                    if matches!(work, Work::Flush) {
                        *inner.background_error.lock() = None;
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!(?work, error = %e, "background work failed; will retry");
                    if matches!(work, Work::Flush) {
                        *inner.background_error.lock() = Some(e.to_string());
                        // Let paused writers see the error.
                        inner.flushed.notify_waiters();
                    }
                    drop(inner);
                    tokio::time::sleep(BACKGROUND_RETRY).await;
                }
            }
        }
    }
}

/// Take the exclusive lock on a data directory, waiting a little for a
/// previous engine in this process to let go of it.
async fn lock_dir(dir: &std::path::Path) -> StorageResult<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK_FILE))?;
    let deadline = std::time::Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(StorageError::Manifest(format!(
                    "data directory {} is in use by another RooDB process",
                    dir.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }
    }
}

impl Inner {
    async fn open(io: IoEngine, config: LsmConfig) -> StorageResult<Self> {
        std::fs::create_dir_all(&config.dir)?;
        // Before anything is read or cleaned up: two engines on one directory
        // would overwrite each other's manifest and delete each other's files.
        let dir_lock = lock_dir(&config.dir).await?;

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
            compact_work: Arc::new(Notify::new()),
            dir_lock: parking_lot::Mutex::new(Some(dir_lock)),
            flushed: Notify::new(),
            background_error: parking_lot::Mutex::new(None),
            stopping: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    /// Tell the background tasks to exit.
    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.work.notify_one();
        self.compact_work.notify_one();
        // Writers paused on the flusher must not wait for a task that is gone.
        self.flushed.notify_waiters();
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
    /// until this returns successfully.
    ///
    /// If the new manifest never reached disk, the change is undone so that
    /// memory matches what a restart would find. If it reached disk but could
    /// not be confirmed durable ([`StorageError::ManifestNotSynced`]) the
    /// change stands: memory and disk agree, and the caller gets the error so
    /// that it neither releases what the old manifest needed nor deletes what
    /// the new one lists.
    async fn commit_manifest(&self, change: impl FnOnce(&mut Manifest)) -> StorageResult<()> {
        let _writing = self.manifest_io.lock().await;
        let (dir, data, before) = {
            let mut manifest = self.manifest.write();
            let before = manifest.levels_snapshot();
            change(&mut manifest);
            (manifest.dir().to_path_buf(), manifest.snapshot(), before)
        };
        let result = persist(&self.io, &dir, &data).await;
        if let Err(e) = &result {
            if !matches!(e, StorageError::ManifestNotSynced(_)) {
                self.manifest.write().restore_levels(before);
            }
        }
        result
    }

    /// Forget a table that was written but could not be installed, and delete
    /// its file. Left alone, a retry would write the same data again under a
    /// new name and the abandoned files would pile up until restart.
    ///
    /// `error` is why installing it failed. Nothing is deleted if the manifest
    /// on disk may already list the table.
    fn discard_table(&self, name: &str, path: &std::path::Path, error: &StorageError) {
        if matches!(error, StorageError::ManifestNotSynced(_)) {
            return;
        }
        // An armed crash point is simulating a crash, which cleans nothing up.
        if self.crash_point.is_some() {
            return;
        }
        self.reader_cache.write().remove(name);
        self.block_cache.invalidate_sstable(name);
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(file = name, error = %e, "failed to delete abandoned table");
            }
        }
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
        // Fresh data is the data most likely to be read next, but warming
        // must not push everything else out of the cache, and failing to
        // warm is no reason to fail the flush or compaction.
        if info.size < (self.total_cache_bytes / 4) as u64 {
            if let Err(e) = reader.eager_load_blocks().await {
                tracing::warn!(table = %info.name, error = %e, "cache warm-up failed");
            }
        }
        self.reader_cache
            .write()
            .insert(info.name.as_str().into(), Arc::new(reader));
        Ok(())
    }

    /// Write one frozen memtable out as a level-0 table.
    async fn flush_memtable(&self, mem: &Memtable, urgency: Urgency) -> StorageResult<()> {
        let entries = mem.iter();
        if entries.is_empty() {
            return Ok(());
        }
        let (name, path) = {
            let mut manifest = self.manifest.write();
            let name = manifest.next_sstable_name();
            let path = manifest.sstable_path(&name);
            (name, path)
        };
        let result = self
            .write_level0_table(entries, &name, &path, urgency)
            .await;
        if let Err(e) = &result {
            self.discard_table(&name, &path, e);
        }
        result
    }

    async fn write_level0_table(
        &self,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        name: &str,
        path: &std::path::Path,
        urgency: Urgency,
    ) -> StorageResult<()> {
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(());
        };
        let min_key = first.0.clone();
        let max_key = last.0.clone();

        let mut writer = SstableWriter::create(&self.io, path, urgency).await?;
        for (key, value) in entries {
            writer.add(key, value).await?;
        }
        writer.finish().await?;
        self.crash_at(CrashPoint::FlushAfterTableWritten)?;

        let info = SstableInfo {
            name: name.to_string(),
            min_key,
            max_key,
            size: std::fs::metadata(path)?.len(),
        };
        self.register_table(&info, path).await?;
        self.commit_manifest(|manifest| manifest.add_sstable(0, info))
            .await
    }

    /// Whether any level is over its limit.
    fn compaction_due(&self) -> bool {
        let manifest = self.manifest.read();
        (0..LAST_LEVEL).any(|level| self.limits.needs_compaction(&manifest, level))
    }

    /// Run compactions until no level is over its limit. A background run
    /// stops early when the engine is shutting down.
    async fn compact(&self, background: bool) -> StorageResult<()> {
        let _compacting = self.compaction_lock.lock().await;
        loop {
            if background && self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
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

            let installed = self.run_compaction(&job).await;
            if let Err(e) = &installed {
                self.discard_table(&job.output_name, &job.output_path, e);
            }
            installed?;
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

    /// Write a compaction's output and install it in the manifest.
    async fn run_compaction(&self, job: &CompactionJob) -> StorageResult<()> {
        let output = execute_compaction(&self.io, job).await?;
        self.crash_at(CrashPoint::CompactionAfterOutputWritten)?;

        // The output becomes visible, and durable, before any input is
        // touched. From here a crash leaves the inputs as unlisted files,
        // which startup deletes.
        if let Some(info) = &output {
            self.register_table(info, &job.output_path).await?;
        }
        self.commit_manifest(|manifest| apply_compaction(manifest, job, output))
            .await
    }

    /// Apply writes to the active memtable as one unit.
    ///
    /// The memtable lock is held (shared) across every insert. Rotation takes
    /// it exclusively, so a memtable is never frozen and written out with part
    /// of a batch in it or while an insert is still on its way, and what
    /// reaches disk is always whole batches.
    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
        self.check_open()?;
        // Refuse what the table format cannot hold before any of the batch is
        // applied, so a batch is never half written.
        for (key, value) in &batch {
            check_entry_size(key.len(), value.as_ref().map_or(0, Vec::len))?;
        }
        self.wait_for_flusher().await?;
        let full = {
            let memtable = self.memtable.read();
            for (key, value) in batch {
                memtable.insert(key, value);
            }
            memtable.size() >= self.memtable_limit()
        };
        self.after_write(full);
        Ok(())
    }

    /// Apply a single write.
    async fn write_one(&self, key: &[u8], value: Option<&[u8]>) -> StorageResult<()> {
        self.check_open()?;
        check_entry_size(key.len(), value.map_or(0, <[u8]>::len))?;
        self.wait_for_flusher().await?;
        let full = {
            let memtable = self.memtable.read();
            memtable.insert(key.to_vec(), value.map(<[u8]>::to_vec));
            memtable.size() >= self.memtable_limit()
        };
        self.after_write(full);
        Ok(())
    }

    /// Freeze the active memtable if the write filled it, and wake the
    /// background task.
    fn after_write(&self, full: bool) {
        if full {
            self.rotate(self.memtable_limit());
            self.rebalance_cache();
            self.work.notify_one();
        }
    }

    /// Pause while too many frozen memtables are waiting to be written out.
    async fn wait_for_flusher(&self) -> StorageResult<()> {
        // The usual case, kept cheap: nothing to wait for.
        if self.imm_memtables.read().len() < MAX_FROZEN_MEMTABLES {
            return Ok(());
        }
        loop {
            // Nobody is left to flush once the background tasks have gone.
            if self.stopping.load(Ordering::SeqCst) {
                return Err(StorageError::Closed);
            }
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

    /// The flush task's work: write frozen memtables out, then hand over to
    /// the compaction task if a level is now over its limit. Writers may be
    /// waiting on this, so it does not run at background urgency.
    async fn background_flush(&self) -> StorageResult<()> {
        self.flush_frozen(Urgency::Foreground, true).await?;
        if self.compaction_due() {
            self.compact_work.notify_one();
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
    async fn flush_frozen(&self, urgency: Urgency, background: bool) -> StorageResult<()> {
        let _flushing = self.flush_lock.lock().await;
        // A memtable stays readable in `imm_memtables` until its table is on
        // disk and in the manifest; only then is it removed.
        loop {
            if background && self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
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
        self.flush_frozen(urgency, false).await?;
        if self.compaction_due() {
            self.compact(false).await?;
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

    /// The nearest live key after `key` (`forward`) or before it, without
    /// reading the range in between: each memtable and table is asked for
    /// its own nearest entry, and the nearest of those is the answer unless
    /// the newest entry for that key is a tombstone, in which case the
    /// search goes on from there.
    async fn seek(&self, key: &[u8], forward: bool) -> StorageResult<Option<Vec<u8>>> {
        let sources = self.read_sources()?;
        let mut from = key.to_vec();
        loop {
            // Newest source first, so that of two entries for one key the
            // first one seen is the one that counts.
            let mut nearest: Option<(Vec<u8>, bool)> = None;
            let mut consider = |candidate: Option<(Vec<u8>, bool)>| {
                let Some(candidate) = candidate else { return };
                let closer = match &nearest {
                    None => true,
                    Some((best, _)) if forward => candidate.0 < *best,
                    Some((best, _)) => candidate.0 > *best,
                };
                if closer {
                    nearest = Some(candidate);
                }
            };
            let memtables = std::iter::once(&sources.active).chain(sources.frozen.iter().rev());
            for memtable in memtables {
                consider(if forward {
                    memtable.next_after(&from)
                } else {
                    memtable.prev_before(&from)
                });
            }
            for table in &sources.tables {
                consider(if forward {
                    table.next_after(&from).await?
                } else {
                    table.prev_before(&from).await?
                });
            }
            match nearest {
                None => return Ok(None),
                Some((found, false)) => return Ok(Some(found)),
                // Deleted: look past it.
                Some((found, true)) => from = found,
            }
        }
    }

    /// Look a key up everywhere, newest data first. `Some(None)` means the
    /// newest entry for the key is a tombstone.
    async fn read_all(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        // Memtables first, and for most reads of recent data that is all.
        // Capturing them before the tables is safe: data only ever moves from
        // a memtable to a table, and a table is listed before its memtable is
        // released, so the table list read afterwards holds whatever these
        // memtables no longer do.
        let (active, frozen) = {
            let active = self.memtable.read();
            (Arc::clone(&active), self.imm_memtables.read().clone())
        };
        if let Some(result) = active.get(key) {
            return Ok(Some(result));
        }
        for mem in frozen.iter().rev() {
            if let Some(result) = mem.get(key) {
                return Ok(Some(result));
            }
        }

        for table in self.tables_for(key)? {
            if let Some(result) = table.get(key).await? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    /// The tables whose key range covers `key`, newest data first.
    fn tables_for(&self, key: &[u8]) -> StorageResult<Vec<Arc<SstableReader>>> {
        let manifest = self.manifest.read();
        let readers = self.reader_cache.read();
        let mut tables = Vec::new();
        for (level_num, level) in manifest.levels().iter().enumerate() {
            let mut consider = |info: &SstableInfo| -> StorageResult<()> {
                if key < info.min_key.as_slice() || key > info.max_key.as_slice() {
                    return Ok(());
                }
                let reader = readers.get(info.name.as_str()).ok_or_else(|| {
                    StorageError::Manifest(format!("no open reader for table {}", info.name))
                })?;
                tables.push(Arc::clone(reader));
                Ok(())
            };
            if level_num == 0 {
                for info in level.files.iter().rev() {
                    consider(info)?;
                }
            } else {
                for info in &level.files {
                    consider(info)?;
                }
            }
        }
        Ok(tables)
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

    async fn get_durable(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.check_open()?;
        for table in self.inner.tables_for(key)? {
            if let Some(result) = table.get(key).await? {
                return Ok(result);
            }
        }
        Ok(None)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.inner.write_one(key, Some(value)).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.inner.write_one(key, None).await
    }

    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
        self.inner.write_batch(batch).await
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        self.inner.scan(start, end).await
    }

    async fn next_key(&self, key: &[u8], high: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.check_open()?;
        Ok(self
            .inner
            .seek(key, true)
            .await?
            .filter(|found| found.as_slice() < high))
    }

    async fn prev_key(&self, key: &[u8], low: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.check_open()?;
        Ok(self
            .inner
            .seek(key, false)
            .await?
            .filter(|found| found.as_slice() >= low))
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
        // Write out what is in memory first. If that fails the engine is
        // still whole - background tasks included - and close can be retried.
        self.inner.flush_all(Urgency::Background).await?;
        self.stop_workers().await;
        self.inner.closed.store(true, Ordering::SeqCst);
        // A closed engine does nothing more with the directory, whoever still
        // holds a handle to it; let the next one in.
        self.inner.dir_lock.lock().take();
        Ok(())
    }
}
