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
//!
//! Forgotten ranges (`WriteOp::ForgetRange`): a key range can be given up as
//! a whole, which is how a dropped table's rows go. Forgetting a range is a
//! write of its batch like the puts and deletes around it:
//! - It lands in the active memtable, which drops the entries it has in the
//!   range and keeps the range. From then on reads leave out whatever older
//!   memtables and the tables hold there. What is written to the range
//!   afterwards, in the same memtable or a later one, is a new write and is
//!   read as usual.
//! - It becomes durable with the rest of its batch: the flush that writes
//!   the memtable out lists its ranges in the manifest in the same change
//!   that lists the table. Before that the manifest knows nothing of the
//!   range, so a crash leaves the directory as it was before the batch.
//! - A listed range has a number, and every table records the number of the
//!   newest range listed when it was written (`filtered_seq`). The range
//!   forgets what tables with a lower number hold in it, and nothing of the
//!   others. The compaction task removes tables that hold nothing else,
//!   rewrites those that hold other keys too, and takes the range off the
//!   list once no table can hold a key it forgets.

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
use crate::storage::lsm::compaction::{merged_chunk, TableSource};
use crate::storage::lsm::manifest::{
    forgetting_range, forgotten_range, persist, ForgottenRange, Manifest, SstableInfo,
};
use crate::storage::lsm::memtable::{KeyRange, Memtable};
use crate::storage::lsm::merge_iter::MergeIterator;
use crate::storage::lsm::sstable::{SstableReader, SstableWriter};
use crate::storage::traits::{Ingest, KeyValue, SnapshotView, StorageEngine, WriteOp};

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
    /// A flush has listed the forgotten ranges of its memtable in the
    /// manifest, with its table; nothing else has been done about them.
    /// (Before that point a crash leaves no trace of the ranges: see
    /// `FlushAfterTableWritten`.)
    FlushAfterRangesSaved,
    /// The tables that held nothing but forgotten keys are off the manifest;
    /// their files are not yet deleted.
    ForgottenTablesUnlisted,
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
    tables: Vec<TableSource>,
    /// The forgotten ranges the manifest listed at that instant. What they
    /// forget of each table is not to be returned.
    forgotten: Vec<ForgottenRange>,
}

impl ReadSources {
    /// For each source below the active memtable, the ranges forgotten in
    /// the memtables newer than it: first for each frozen memtable, newest
    /// to oldest, then (last) for the tables. Only ranges that hold a key
    /// of `[start, end)` are of interest (unbounded where `None`).
    /// `active_forgotten` is what the active memtable had forgotten there
    /// when it was read.
    fn forgotten_above(
        &self,
        active_forgotten: Vec<KeyRange>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<Vec<KeyRange>> {
        let mut above = Vec::with_capacity(self.frozen.len() + 1);
        let mut ranges = active_forgotten;
        above.push(ranges.clone());
        for memtable in self.frozen.iter().rev() {
            let own = memtable.forgotten_within(start, end);
            if !own.is_empty() {
                ranges.extend(own);
                ranges = disjoint(ranges);
            }
            above.push(ranges.clone());
        }
        above
    }
}

/// `ranges` as a sorted list of ranges that neither overlap nor touch,
/// holding the same keys.
fn disjoint(mut ranges: Vec<KeyRange>) -> Vec<KeyRange> {
    if ranges.len() < 2 {
        return ranges;
    }
    ranges.sort();
    let mut merged: Vec<KeyRange> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some((_, last_end)) if start <= *last_end => {
                if end > *last_end {
                    *last_end = end;
                }
            }
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// What is forgotten of the entries one source holds: a key in one of these
/// ranges is not to be read from that source.
#[derive(Clone, Copy)]
struct Forgotten<'a> {
    /// Ranges forgotten in memtables newer than the source (sorted, not
    /// overlapping or touching). They forget whatever the source holds.
    above: &'a [KeyRange],
    /// Ranges the manifest lists, for a table; none for a memtable, which
    /// is newer than all of them.
    listed: &'a [ForgottenRange],
    /// The table's `filtered_seq`: listed ranges numbered above it apply.
    filtered_seq: u64,
}

impl<'a> Forgotten<'a> {
    /// For a memtable below those that forgot `above`.
    fn of_memtable(above: &'a [KeyRange]) -> Self {
        Self {
            above,
            listed: &[],
            filtered_seq: 0,
        }
    }

    /// For a table below memtables that forgot `above`.
    fn of_table(above: &'a [KeyRange], listed: &'a [ForgottenRange], table: &TableSource) -> Self {
        Self {
            above,
            listed,
            filtered_seq: table.filtered_seq,
        }
    }

    /// The range that forgets `key` as this source holds it, if any.
    fn range_of(&self, key: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
        let after = self
            .above
            .partition_point(|(start, _)| start.as_slice() <= key);
        if let Some((start, end)) = self.above[..after].last() {
            if key < end.as_slice() {
                return Some((start, end));
            }
        }
        forgetting_range(self.listed, key, self.filtered_seq)
            .map(|range| (range.start.as_slice(), range.end.as_slice()))
    }

    /// Take the forgotten entries out of what the source gave.
    fn strip(&self, entries: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        if self.above.is_empty() && self.listed.is_empty() {
            return;
        }
        entries.retain(|(key, _)| self.range_of(key).is_none());
    }
}

/// The table files as they were at one moment. Table files never change,
/// and a file that compaction deletes meanwhile stays readable through the
/// handle held here, so the view stays whole for as long as it is kept.
///
/// The same goes for forgotten ranges. The view leaves out what the ranges
/// listed when it was taken forget of its tables. A range listed afterwards
/// is not the view's concern: it goes on showing all of that range's keys,
/// from the files it holds, though the engine has unlisted and deleted them.
/// Like every other write, a range forgotten by a batch that had not been
/// flushed when the view was taken is not in the view.
struct TableView {
    /// Newest data first.
    tables: Vec<TableSource>,
    /// The forgotten ranges listed when the view was taken.
    forgotten: Vec<ForgottenRange>,
    /// The files behind `tables`, and the engine they belong to.
    paths: Vec<PathBuf>,
    engine: Weak<Inner>,
}

impl Drop for TableView {
    fn drop(&mut self) {
        // Where an open file cannot be deleted (Windows), a table that
        // was retired - compacted, replaced, or forgotten - while this view
        // held it open is still on disk. Now that the view lets go, delete what is no longer listed.
        // (Elsewhere the file is long gone and there is nothing to do.)
        self.tables.clear();
        let Some(engine) = self.engine.upgrade() else {
            return;
        };
        let readers = engine.reader_cache.read();
        for path in &self.paths {
            let listed = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| readers.contains_key(name));
            if !listed {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[async_trait]
impl SnapshotView for TableView {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        read_tables(&self.tables, &self.forgotten, key).await
    }

    async fn next_chunk(
        &self,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> StorageResult<(Vec<KeyValue>, bool)> {
        let mut after = after.map(<[u8]>::to_vec);
        loop {
            let (entries, done) =
                merged_chunk(&self.tables, after.as_deref(), max_bytes, &self.forgotten).await?;
            let last = entries.last().map(|(key, _)| key.clone());
            let live: Vec<KeyValue> = entries
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect();
            if !live.is_empty() || done {
                return Ok((live, done));
            }
            // Everything in this stretch was deleted: go on from its end.
            after = last;
        }
    }
}

/// Look `key` up in `tables` (newest data first), leaving out what the
/// `forgotten` ranges forget of them.
async fn read_tables(
    tables: &[TableSource],
    forgotten: &[ForgottenRange],
    key: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    // At most one listed range holds the key. A table it applies to holds
    // nothing of the key that counts, and need not be read.
    let range = forgotten_range(forgotten, key);
    for source in tables {
        if range.is_some_and(|range| range.applies_to(source.filtered_seq)) {
            continue;
        }
        if let Some(found) = source.table.get(key).await? {
            // A tombstone and a key that was never written read the same.
            return Ok(found);
        }
    }
    Ok(None)
}

/// A replacement for everything the engine holds, written as table files
/// of its own. See `Inner::replace_all`.
struct TableIngest {
    engine: Arc<Inner>,
    /// The file being written: its name, path, writer, first key and size.
    current: Option<(String, PathBuf, SstableWriter, Vec<u8>, u64)>,
    last_key: Vec<u8>,
    finished: Vec<(SstableInfo, PathBuf)>,
    committed: bool,
}

/// Size at which an ingested table file is closed and another begun.
const INGEST_FILE_BYTES: u64 = 64 * 1024 * 1024;

impl TableIngest {
    async fn finish_file(&mut self) -> StorageResult<()> {
        let Some((name, path, writer, min_key, _)) = self.current.take() else {
            return Ok(());
        };
        writer.finish().await?;
        self.finished.push((
            SstableInfo {
                name,
                min_key,
                max_key: self.last_key.clone(),
                size: std::fs::metadata(&path)?.len(),
                // The replacement starts with no range forgotten, and any
                // forgotten later is numbered above this.
                filtered_seq: 0,
            },
            path,
        ));
        Ok(())
    }
}

#[async_trait]
impl Ingest for TableIngest {
    async fn add(&mut self, entries: Vec<KeyValue>) -> StorageResult<()> {
        for (key, value) in entries {
            check_entry_size(key.len(), value.len())?;
            if self.current.is_none() {
                let (name, path) = {
                    let mut manifest = self.engine.manifest.write();
                    let name = manifest.next_sstable_name();
                    let path = manifest.sstable_path(&name);
                    (name, path)
                };
                let writer =
                    SstableWriter::create(&self.engine.io, &path, Urgency::Background).await?;
                self.current = Some((name, path, writer, key.clone(), 0));
            }
            let (_, _, writer, _, bytes) = self.current.as_mut().expect("just set");
            *bytes += (key.len() + value.len()) as u64;
            let full = *bytes >= INGEST_FILE_BYTES;
            self.last_key.clone_from(&key);
            writer.add(key, Some(value)).await?;
            if full {
                self.finish_file().await?;
            }
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> StorageResult<()> {
        self.finish_file().await?;
        let tables = std::mem::take(&mut self.finished);
        let result = self.engine.replace_all(&tables).await;
        if let Err(e) = &result {
            for (info, path) in &tables {
                self.engine.discard_table(&info.name, path, e);
            }
        }
        self.committed = true;
        result
    }
}

impl Drop for TableIngest {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Abandoned: the files were never listed, so removing them is all
        // there is to do. (If that fails, the next start removes them.)
        let unfinished = self.current.take().map(|(_, path, ..)| path);
        for path in self
            .finished
            .iter()
            .map(|(_, path)| path)
            .chain(&unfinished)
        {
            let _ = std::fs::remove_file(path);
        }
    }
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
        // Ranges forgotten before the last shutdown or crash may still have
        // tables to be removed or rewritten: nothing else would wake the
        // compaction task for them.
        if inner.compaction_due() {
            inner.compact_work.notify_one();
        }
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
            let before = manifest.state_snapshot();
            change(&mut manifest);
            (manifest.dir().to_path_buf(), manifest.snapshot(), before)
        };
        let result = persist(&self.io, &dir, &data).await;
        if let Err(e) = &result {
            if !matches!(e, StorageError::ManifestNotSynced(_)) {
                self.manifest.write().restore_state(before);
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

    /// Write one frozen memtable out: its entries as a level-0 table, and
    /// the ranges forgotten in it as entries of the manifest's list, in one
    /// change of the manifest. Either all of the memtable is on disk after
    /// this or none of it, forgotten ranges included.
    ///
    /// Nothing is left out of the table. Every range the manifest lists was
    /// forgotten before anything in this memtable was written (memtables
    /// are flushed oldest first, and a range is listed by the flush of its
    /// memtable), and a range forgotten in this memtable has already taken
    /// the entries that preceded it.
    async fn flush_memtable(&self, mem: &Memtable, urgency: Urgency) -> StorageResult<()> {
        let entries = mem.iter();
        let forgotten = mem.forgotten();
        let table = if entries.is_empty() {
            None
        } else {
            let mut manifest = self.manifest.write();
            let name = manifest.next_sstable_name();
            let path = manifest.sstable_path(&name);
            Some((name, path))
        };
        if table.is_none() && forgotten.is_empty() {
            return Ok(());
        }
        let result = self
            .install_flush(entries, &forgotten, table.as_ref(), urgency)
            .await;
        if let (Err(e), Some((name, path))) = (&result, &table) {
            self.discard_table(name, path, e);
        }
        result
    }

    async fn install_flush(
        &self,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        forgotten: &[KeyRange],
        table: Option<&(String, PathBuf)>,
        urgency: Urgency,
    ) -> StorageResult<()> {
        let mut info = None;
        if let (Some((name, path)), Some(first), Some(last)) =
            (table, entries.first(), entries.last())
        {
            let min_key = first.0.clone();
            let max_key = last.0.clone();

            let mut writer = SstableWriter::create(&self.io, path, urgency).await?;
            for (key, value) in entries {
                writer.add(key, value).await?;
            }
            writer.finish().await?;
            self.crash_at(CrashPoint::FlushAfterTableWritten)?;

            let table = SstableInfo {
                name: name.clone(),
                min_key,
                max_key,
                size: std::fs::metadata(path)?.len(),
                // Set when the table is listed, below.
                filtered_seq: 0,
            };
            self.register_table(&table, path).await?;
            info = Some(table);
        }
        // The memtable's ranges and its table become durable together, as
        // the batches that wrote them require. The table counts as written
        // after every range listed up to now, its own memtable's included:
        // what it holds in them was written after they were forgotten.
        self.commit_manifest(|manifest| {
            for (start, end) in forgotten {
                manifest.forget(start, end);
            }
            if let Some(mut table) = info {
                table.filtered_seq = manifest.forgotten_seq();
                manifest.add_sstable(0, table);
            }
        })
        .await?;
        if !forgotten.is_empty() {
            self.crash_at(CrashPoint::FlushAfterRangesSaved)?;
        }
        Ok(())
    }

    /// Whether the compaction task has work: a level is over its limit, or
    /// forgotten ranges are listed.
    fn compaction_due(&self) -> bool {
        self.limits.work_due(&self.manifest.read())
    }

    /// Run compactions until no level is over its limit and no table may
    /// hold forgotten keys, then retire the forgotten ranges that nothing
    /// holds any more. A background run stops early when the engine is
    /// shutting down.
    async fn compact(&self, background: bool) -> StorageResult<()> {
        let _compacting = self.compaction_lock.lock().await;
        loop {
            if background && self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.remove_forgotten_tables().await?;
            let job = {
                let mut manifest = self.manifest.write();
                pick_compaction(&mut manifest, &self.limits)
            };
            let Some(job) = job else {
                return self.retire_forgotten().await;
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

            self.delete_unlisted(job.input_names());
        }
    }

    /// Let go of tables the manifest no longer lists, and delete their
    /// files.
    fn delete_unlisted<'a>(&self, names: impl Iterator<Item = &'a str> + Clone) {
        {
            let mut readers = self.reader_cache.write();
            for name in names.clone() {
                readers.remove(name);
                self.block_cache.invalidate_sstable(name);
            }
        }
        let dir = self.manifest.read().dir().to_path_buf();
        for name in names {
            // A reader or a view that captured this table before the
            // manifest changed keeps its open handle and can finish. If the
            // delete fails the file is unlisted and startup will remove it
            // (or the view that holds it open, when it lets go).
            if let Err(e) = std::fs::remove_file(dir.join(name)) {
                tracing::warn!(file = name, error = %e, "failed to delete retired table");
            }
        }
    }

    /// Take the tables that hold nothing but forgotten keys - they lie
    /// wholly inside a range that was forgotten after they were written -
    /// off the manifest and delete their files. Called with the compaction
    /// lock held, so none of them is being compacted into something else
    /// meanwhile.
    async fn remove_forgotten_tables(&self) -> StorageResult<()> {
        let doomed: Vec<(u32, String)> = {
            let manifest = self.manifest.read();
            let forgotten = manifest.forgotten();
            manifest
                .levels()
                .iter()
                .flat_map(|level| level.files.iter().map(|file| (level.level, file)))
                .filter(|(_, file)| {
                    forgotten.iter().any(|range| {
                        range.applies_to(file.filtered_seq)
                            && range.covers(&file.min_key, &file.max_key)
                    })
                })
                .map(|(level, file)| (level, file.name.clone()))
                .collect()
        };
        if doomed.is_empty() {
            return Ok(());
        }
        tracing::debug!(tables = doomed.len(), "removing forgotten tables");
        // Unlisted, durably, before any file goes: a crash from here on
        // leaves unlisted files, which startup deletes.
        self.commit_manifest(|manifest| {
            for (level, name) in &doomed {
                manifest.remove_sstable(*level, name);
            }
        })
        .await?;
        self.crash_at(CrashPoint::ForgottenTablesUnlisted)?;
        self.delete_unlisted(doomed.iter().map(|(_, name)| name.as_str()));
        Ok(())
    }

    /// Take forgotten ranges off the manifest once no table can hold a key
    /// they forget: then there is nothing left for the range to hide.
    ///
    /// Called with the compaction lock held, so no compaction is about to
    /// list an output written from tables that predate a range. Flushes
    /// need not be kept out, and memtables need not be looked at: a range
    /// is listed by the flush of the memtable it was forgotten in, after
    /// every older memtable has been flushed, so all that was written before
    /// the range is in listed tables by the time the range is, and whatever
    /// a memtable holds, or a flush lists later, was written after it (the
    /// table of a flush records the number of the newest range listed in
    /// that same change of the manifest, or before).
    async fn retire_forgotten(&self) -> StorageResult<()> {
        let any = {
            let manifest = self.manifest.read();
            manifest
                .forgotten()
                .iter()
                .any(|range| manifest.can_retire(range))
        };
        if !any {
            return Ok(());
        }
        self.commit_manifest(Manifest::retire_forgotten).await
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

    /// Apply writes to the active memtable as one unit, in order.
    ///
    /// The memtable lock is held (shared) across every write. Rotation takes
    /// it exclusively, so a memtable is never frozen and written out with part
    /// of a batch in it or while a write is still on its way, and what
    /// reaches disk is always whole batches.
    ///
    /// A forgotten range goes to the memtable like the rest: it takes the
    /// entries the memtable has in the range (so the earlier writes of this
    /// batch too) and stays with the memtable, hiding what is older, until
    /// the memtable's flush lists it in the manifest. Nothing reaches disk
    /// here.
    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()> {
        self.check_open()?;
        // Refuse what the table format cannot hold before any of the batch is
        // applied, so a batch is never half written.
        let mut forgets = false;
        for write in &batch {
            match write {
                WriteOp::Put(key, value) => check_entry_size(key.len(), value.len())?,
                WriteOp::Delete(key) => check_entry_size(key.len(), 0)?,
                WriteOp::ForgetRange { .. } => forgets = true,
            }
        }
        self.wait_for_flusher().await?;
        let full = {
            let memtable = self.memtable.read();
            for write in batch {
                match write {
                    WriteOp::Put(key, value) => memtable.insert(key, Some(value)),
                    WriteOp::Delete(key) => memtable.insert(key, None),
                    WriteOp::ForgetRange { start, end } => memtable.forget_range(&start, &end),
                }
            }
            memtable.size() >= self.memtable_limit()
        };
        if forgets && !full {
            // The memtable may have shrunk.
            self.rebalance_cache();
        }
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
        let forgotten = manifest.forgotten().to_vec();

        let mut tables = Vec::new();
        for (level_num, level) in manifest.levels().iter().enumerate() {
            let mut push = |info: &SstableInfo| -> StorageResult<()> {
                let reader = readers.get(info.name.as_str()).ok_or_else(|| {
                    StorageError::Manifest(format!("no open reader for table {}", info.name))
                })?;
                tables.push(TableSource {
                    table: Arc::clone(reader),
                    filtered_seq: info.filtered_seq,
                });
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
            forgotten,
        })
    }

    /// The nearest live key after `key` (`forward`) or before it, without
    /// reading the range in between: each memtable and table is asked for
    /// its own nearest entry, and the nearest of those is the answer unless
    /// the newest entry for that key is a tombstone, in which case the
    /// search goes on from there.
    ///
    /// The search stops at `bound`: a key at or past `bound` going forward,
    /// or before it going backward, is not an answer, and nothing beyond it
    /// is looked at.
    ///
    /// A forgotten range is stepped over whole: a source whose nearest
    /// entry lies in a range that forgets it moves straight to its first
    /// entry on the far side.
    async fn seek(
        &self,
        key: &[u8],
        forward: bool,
        bound: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        let sources = self.read_sources()?;
        let within = |found: &[u8]| {
            if forward {
                found < bound
            } else {
                found >= bound
            }
        };

        // One cursor per source, newest source first: its nearest entry
        // beyond `key`. This is a merge of the sources: at each step the
        // nearest of the cursors is the next key overall, and only the
        // cursors standing on that key move on.
        #[derive(Clone, Copy)]
        enum Source<'a> {
            Memory(&'a Memtable),
            Table(&'a SstableReader),
        }
        /// A key and whether it is deleted.
        type Found = (Vec<u8>, bool);
        async fn step(
            source: Source<'_>,
            from: &[u8],
            forward: bool,
        ) -> StorageResult<Option<Found>> {
            match (source, forward) {
                (Source::Memory(memtable), true) => Ok(memtable.next_after(from)),
                (Source::Memory(memtable), false) => Ok(memtable.prev_before(from)),
                (Source::Table(table), true) => table.next_after(from).await,
                (Source::Table(table), false) => table.prev_before(from).await,
            }
        }
        /// The source's nearest entry beyond `from` that is not forgotten.
        async fn advance(
            source: Source<'_>,
            forgotten: Forgotten<'_>,
            from: &[u8],
            forward: bool,
        ) -> StorageResult<Option<Found>> {
            let mut found = step(source, from, forward).await?;
            while let Some((key, _)) = &found {
                let Some((start, end)) = forgotten.range_of(key) else {
                    break;
                };
                // Out of the range on its far side.
                found = match (source, forward) {
                    (Source::Memory(memtable), true) => memtable.first_from(end),
                    (Source::Table(table), true) => table.first_from(end).await?,
                    (_, false) => step(source, start, false).await?,
                };
            }
            Ok(found)
        }
        // Only what lies between `key` and `bound` is looked at.
        let (low, high) = if forward { (key, bound) } else { (bound, key) };
        let active_forgotten = sources.active.forgotten_within(Some(low), Some(high));
        let above = sources.forgotten_above(active_forgotten, Some(low), Some(high));
        let below_all = above.last().map_or(&[][..], Vec::as_slice);
        let mut cursors: Vec<(Source<'_>, Forgotten<'_>, Option<Found>)> = Vec::new();
        cursors.push((
            Source::Memory(&sources.active),
            Forgotten::of_memtable(&[]),
            None,
        ));
        for (memtable, above) in sources.frozen.iter().rev().zip(&above) {
            cursors.push((
                Source::Memory(memtable),
                Forgotten::of_memtable(above),
                None,
            ));
        }
        for table in &sources.tables {
            cursors.push((
                Source::Table(&table.table),
                Forgotten::of_table(below_all, &sources.forgotten, table),
                None,
            ));
        }
        for (source, forgotten, at) in &mut cursors {
            *at = advance(*source, *forgotten, key, forward).await?;
        }

        loop {
            // The first cursor (newest source) among those on the nearest
            // key decides whether that key is live.
            let mut nearest: Option<&Found> = None;
            for (_, _, at) in &cursors {
                let Some(candidate) = at else { continue };
                let closer = match nearest {
                    None => true,
                    Some((best, _)) if forward => candidate.0 < *best,
                    Some((best, _)) => candidate.0 > *best,
                };
                if closer {
                    nearest = Some(candidate);
                }
            }
            let Some((found, deleted)) = nearest.cloned() else {
                return Ok(None);
            };
            if !within(&found) {
                return Ok(None);
            }
            if !deleted {
                return Ok(Some(found));
            }
            // Deleted: every source that has this key moves past it.
            for (source, forgotten, at) in &mut cursors {
                if at.as_ref().is_some_and(|(k, _)| *k == found) {
                    *at = advance(*source, *forgotten, &found, forward).await?;
                }
            }
        }
    }

    /// Replace everything the engine holds with `tables` (non-overlapping,
    /// in key order): one change of the manifest, before which readers and
    /// a restart see the old contents, and after which they see the new.
    /// What was in memory is dropped with the old tables.
    ///
    /// The forgotten ranges go too, those the manifest lists and, with the
    /// memtables, those not yet flushed. The new contents come from a view,
    /// which left out what every range flushed before it was taken had
    /// forgotten, so there is nothing here for those ranges to hide; and a
    /// range forgotten after the view's position is forgotten again when the
    /// log is replayed from that position.
    async fn replace_all(&self, tables: &[(SstableInfo, PathBuf)]) -> StorageResult<()> {
        // No flush or compaction may be under way: it would add tables of
        // the old contents to the new manifest.
        let _flushing = self.flush_lock.lock().await;
        let _compacting = self.compaction_lock.lock().await;
        for (info, path) in tables {
            self.register_table(info, path).await?;
        }
        let mut retired: Vec<String> = Vec::new();
        self.commit_manifest(|manifest| {
            for (level, files) in manifest.levels().to_vec().iter().enumerate() {
                for file in &files.files {
                    retired.push(file.name.clone());
                    manifest.remove_sstable(level as u32, &file.name);
                }
            }
            for (info, _) in tables {
                manifest.add_sstable(LAST_LEVEL, info.clone());
            }
            manifest.clear_forgotten();
            // Under the manifest lock, as readers take their view: nobody
            // sees the new tables together with the old memory.
            *self.memtable.write() = Arc::new(Memtable::new());
            self.imm_memtables.write().clear();
        })
        .await?;

        self.delete_unlisted(retired.iter().map(String::as_str));
        self.rebalance_cache();
        Ok(())
    }

    /// Look a key up everywhere, newest data first.
    async fn read_all(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        // Memtables first, and for most reads of recent data that is all.
        // Capturing them before the tables is safe: data only ever moves from
        // a memtable to a table, and a table is listed before its memtable is
        // released, so the table list read afterwards holds whatever these
        // memtables no longer do. The same goes for forgotten ranges, which
        // the manifest lists before their memtable is released.
        let (active, frozen) = {
            let active = self.memtable.read();
            (Arc::clone(&active), self.imm_memtables.read().clone())
        };
        for memtable in std::iter::once(&active).chain(frozen.iter().rev()) {
            // A tombstone and a key that was never written read the same.
            if let Some(result) = memtable.get(key) {
                return Ok(result);
            }
            // Forgotten here, and not written since: whatever is older is
            // gone.
            if memtable.forgets(key) {
                return Ok(None);
            }
        }
        self.read_durable(key).await
    }

    /// Look a key up in the tables: what has been flushed.
    async fn read_durable(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let (tables, forgotten) = self.tables_for(key)?;
        read_tables(&tables, forgotten.as_slice(), key).await
    }

    /// The tables whose key range covers `key`, newest data first, and the
    /// listed forgotten range that holds the key, if any.
    fn tables_for(&self, key: &[u8]) -> StorageResult<(Vec<TableSource>, Option<ForgottenRange>)> {
        let manifest = self.manifest.read();
        let forgotten = forgotten_range(manifest.forgotten(), key).cloned();
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
                tables.push(TableSource {
                    table: Arc::clone(reader),
                    filtered_seq: info.filtered_seq,
                });
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
        Ok((tables, forgotten))
    }
}

impl Inner {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.check_open()?;
        self.read_all(key).await
    }

    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>> {
        self.check_open()?;

        let t_start = std::time::Instant::now();
        let sources = self.read_sources()?;
        let lock_ns = t_start.elapsed().as_nanos() as u64;

        // K-way merge of pre-sorted sources; the lowest priority number wins
        // where two sources hold the same key.
        // Each source is first rid of what has been forgotten of it: by
        // the memtables above it, and for a table by the listed ranges too.
        let mut merger = MergeIterator::new();
        let mut priority = 0;
        let (entries, active_forgotten) = sources.active.scan_with_forgotten(start, end);
        merger.add(entries, priority);
        let above = sources.forgotten_above(active_forgotten, start, end);
        for (mem, above) in sources.frozen.iter().rev().zip(&above) {
            priority += 1;
            let mut entries = mem.scan(start, end);
            Forgotten::of_memtable(above).strip(&mut entries);
            merger.add(entries, priority);
        }
        let below_all = above.last().map_or(&[][..], Vec::as_slice);
        for table in &sources.tables {
            priority += 1;
            let mut entries = table.table.scan_range_batched(start, end).await?;
            Forgotten::of_table(below_all, &sources.forgotten, table).strip(&mut entries);
            merger.add(entries, priority);
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
        // The tables and the ranges the manifest lists: a range forgotten
        // by a batch still in memory is no more durable than the rest of
        // that batch, and a restart would find the key.
        self.inner.read_durable(key).await
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

    async fn snapshot_view(&self) -> StorageResult<Arc<dyn SnapshotView>> {
        self.inner.check_open()?;
        let ReadSources {
            tables, forgotten, ..
        } = self.inner.read_sources()?;
        let dir = self.inner.manifest.read().dir().to_path_buf();
        let paths = tables
            .iter()
            .map(|source| dir.join(source.table.file_name()))
            .collect();
        Ok(Arc::new(TableView {
            tables,
            forgotten,
            paths,
            engine: Arc::downgrade(&self.inner),
        }))
    }

    async fn ingest(self: Arc<Self>) -> StorageResult<Box<dyn Ingest>> {
        self.inner.check_open()?;
        Ok(Box::new(TableIngest {
            engine: self.inner.clone(),
            current: None,
            last_key: Vec::new(),
            finished: Vec::new(),
            committed: false,
        }))
    }

    async fn next_key(&self, key: &[u8], high: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.check_open()?;
        self.inner.seek(key, true, high).await
    }

    async fn prev_key(&self, key: &[u8], low: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.check_open()?;
        self.inner.seek(key, false, low).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoBackend;

    /// Keys of three tables side by side, `a:`, `b:` and `c:`, spread over
    /// the active memtable, a frozen memtable and tables in several levels.
    /// Forgetting `b:` removes its keys from every kind of read at once,
    /// wherever they are, and nothing else, while the range itself is still
    /// only in memory. (A frozen memtable can only be held still from in
    /// here, which is why this test is.)
    #[tokio::test]
    async fn a_forgotten_range_vanishes_from_memory_and_every_level() {
        const PER_ROUND: u32 = 150;
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            dir: dir.path().to_path_buf(),
            // Tiny levels: a few flushes spread the data over several.
            l0_compaction_trigger: 2,
            level_base_bytes: 16 * 1024,
            ..Default::default()
        };
        let io = IoEngine::new(IoBackend::from_env().unwrap()).unwrap();
        let engine = LsmEngine::open(io.clone(), config.clone()).await.unwrap();
        let key =
            |table: char, round: u32, n: u32| format!("{table}:{round:02}-{n:04}").into_bytes();
        let write_round = |round: u32| {
            let engine = &engine;
            async move {
                for n in 0..PER_ROUND {
                    for table in ['a', 'b', 'c'] {
                        let value = [round as u8; 100];
                        engine.put(&key(table, round, n), &value).await.unwrap();
                    }
                }
            }
        };

        // The keys on either side of the range, in a table: the last one
        // before its start, and its end, which is not part of it.
        engine.put(b"b9", b"before").await.unwrap();
        engine.put(b"b;", b"end").await.unwrap();
        for round in 0..7 {
            write_round(round).await;
            engine.flush().await.unwrap();
        }
        let levels_in_use = {
            let manifest = engine.inner.manifest.read();
            manifest
                .levels()
                .iter()
                .filter(|level| !level.files.is_empty())
                .count()
        };
        assert!(levels_in_use >= 2, "the tables are all in one level");
        // A frozen memtable, which stays frozen: nothing wakes the flusher.
        write_round(7).await;
        engine.inner.rotate(0);
        assert_eq!(engine.inner.imm_memtables.read().len(), 1);
        // And the active one, with neighbours of its own and the first key
        // of the range.
        write_round(8).await;
        engine.put(b"b9z", b"before").await.unwrap();
        engine.put(b"b:", b"first").await.unwrap();
        engine.put(b"b;0", b"after").await.unwrap();

        engine
            .write_batch(vec![WriteOp::forget(b"b:", b"b;")])
            .await
            .unwrap();

        // The range is with the active memtable, which has given up its own
        // entries there. The frozen memtable is as it was - it may be
        // flushed on its own, and what it holds was written before the
        // range was forgotten - and the manifest knows nothing yet.
        let frozen = engine.inner.imm_memtables.read().clone();
        assert_eq!(frozen.len(), 1, "the frozen memtable was flushed");
        assert_eq!(
            frozen[0].scan(Some(b"b:"), Some(b"b;")).len(),
            PER_ROUND as usize
        );
        assert!(frozen[0].forgotten().is_empty());
        let active = engine.inner.memtable.read().clone();
        assert!(active.scan(Some(b"b:"), Some(b"b;")).is_empty());
        assert_eq!(active.forgotten(), [(b"b:".to_vec(), b"b;".to_vec())]);
        assert!(engine.inner.manifest.read().forgotten().is_empty());

        assert_eq!(engine.get(b"b:").await.unwrap(), None);
        for round in 0..9 {
            for n in (0..PER_ROUND).step_by(7) {
                assert_eq!(
                    engine.get(&key('b', round, n)).await.unwrap(),
                    None,
                    "round {round}"
                );
                for table in ['a', 'c'] {
                    assert_eq!(
                        engine.get(&key(table, round, n)).await.unwrap(),
                        Some(vec![round as u8; 100]),
                        "{table} round {round}"
                    );
                }
            }
        }
        assert!(engine
            .scan(Some(b"b:"), Some(b"b;"))
            .await
            .unwrap()
            .is_empty());
        let all = engine.scan(None, None).await.unwrap();
        assert_eq!(all.len(), 2 * 9 * PER_ROUND as usize + 4);
        assert!(all
            .iter()
            .all(|(k, _)| !(b"b:"[..] <= k[..] && k[..] < b"b;"[..])));
        let around: Vec<&[u8]> = all
            .iter()
            .map(|(k, _)| k.as_slice())
            .filter(|k| k.starts_with(b"b"))
            .collect();
        assert_eq!(around, [&b"b9"[..], b"b9z", b"b;", b"b;0"]);

        // The nearest key on either side is found across the range, from
        // outside it and from inside it, in both directions.
        let (low, high) = (&b"a"[..], &b"z"[..]);
        let next = |from: Vec<u8>, high: &'static [u8]| {
            let engine = &engine;
            async move { engine.next_key(&from, high).await.unwrap() }
        };
        let prev = |from: Vec<u8>, low: &'static [u8]| {
            let engine = &engine;
            async move { engine.prev_key(&from, low).await.unwrap() }
        };
        assert_eq!(next(b"b9".to_vec(), high).await, Some(b"b9z".to_vec()));
        assert_eq!(next(b"b9z".to_vec(), high).await, Some(b"b;".to_vec()));
        assert_eq!(next(b"b:".to_vec(), high).await, Some(b"b;".to_vec()));
        assert_eq!(next(key('b', 4, 10), high).await, Some(b"b;".to_vec()));
        assert_eq!(next(b"b;".to_vec(), high).await, Some(b"b;0".to_vec()));
        assert_eq!(prev(b"b;0".to_vec(), low).await, Some(b"b;".to_vec()));
        assert_eq!(prev(b"b;".to_vec(), low).await, Some(b"b9z".to_vec()));
        assert_eq!(prev(key('b', 4, 10), low).await, Some(b"b9z".to_vec()));
        assert_eq!(prev(b"b:".to_vec(), low).await, Some(b"b9z".to_vec()));
        assert_eq!(prev(b"b9z".to_vec(), low).await, Some(b"b9".to_vec()));
        // A search bounded inside the range finds nothing there.
        assert_eq!(next(b"b9z".to_vec(), b"b:5").await, None);
        assert_eq!(prev(b"b;".to_vec(), b"b:5").await, None);
        assert_eq!(next(b"b9z".to_vec(), b"b;").await, None);
        assert_eq!(prev(b"b;".to_vec(), b"b:").await, None);

        // The same once everything is in tables, and after a restart.
        engine.flush().await.unwrap();
        assert!(engine
            .scan(Some(b"b:"), Some(b"b;"))
            .await
            .unwrap()
            .is_empty());
        assert!(engine.scan(None, None).await.unwrap() == all);
        assert_eq!(next(b"b9z".to_vec(), high).await, Some(b"b;".to_vec()));
        assert_eq!(prev(b"b;".to_vec(), low).await, Some(b"b9z".to_vec()));
        engine.close().await.unwrap();
        drop(engine);

        let engine = LsmEngine::open(io, config).await.unwrap();
        assert!(engine.scan(None, None).await.unwrap() == all);
        assert_eq!(engine.get(&key('b', 0, 0)).await.unwrap(), None);
        assert_eq!(engine.get(&key('b', 8, 0)).await.unwrap(), None);
        engine.close().await.unwrap();
    }

    /// The compaction task retires ranges with a change of the manifest,
    /// and a flush can get its own change in first: one that lists a range
    /// whose keys the tables still hold. The retirement must judge the
    /// ranges as they are when its change is made, not as they were when it
    /// decided there was something to retire, or it takes the new range off
    /// the list with the rest and the keys come back.
    ///
    /// (The two changes are lined up here by holding the lock that orders
    /// changes of the manifest, which only a test inside the engine can.)
    #[tokio::test]
    async fn a_range_listed_while_others_are_retired_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            dir: dir.path().to_path_buf(),
            // Nothing is compacted for its size.
            l0_compaction_trigger: 100,
            ..Default::default()
        };
        let io = IoEngine::new(IoBackend::from_env().unwrap()).unwrap();
        let engine = LsmEngine::open(io, config).await.unwrap();
        for n in 0..50 {
            let key = format!("b:{n:03}");
            engine.put(key.as_bytes(), b"old").await.unwrap();
        }
        engine.put(b"c:", b"other").await.unwrap();
        engine.flush().await.unwrap();

        // A listed range that no table holds a key of: ready to be retired.
        engine.inner.manifest.write().forget(b"x:", b"x;");
        // And a range forgotten in a frozen memtable, about to be flushed:
        // the table holds its keys.
        engine
            .write_batch(vec![WriteOp::forget(b"b:", b"b;")])
            .await
            .unwrap();
        engine.inner.rotate(0);

        // The flush queues to change the manifest; the compaction task,
        // having found a range to retire, queues behind it.
        let writing = engine.inner.manifest_io.lock().await;
        let flush = {
            let inner = Arc::clone(&engine.inner);
            tokio::spawn(async move { inner.flush_frozen(Urgency::Foreground, false).await })
        };
        tokio::task::yield_now().await;
        assert!(
            engine.inner.flush_lock.try_lock().is_err(),
            "the flush is not waiting to change the manifest"
        );
        let compaction = {
            let inner = Arc::clone(&engine.inner);
            tokio::spawn(async move { inner.compact(false).await })
        };
        tokio::task::yield_now().await;
        assert!(
            engine.inner.compaction_lock.try_lock().is_err(),
            "the compaction task is not waiting to change the manifest"
        );
        let listed = |engine: &LsmEngine| -> Vec<Vec<u8>> {
            let manifest = engine.inner.manifest.read();
            manifest
                .forgotten()
                .iter()
                .map(|range| range.start.clone())
                .collect()
        };
        assert_eq!(listed(&engine), [b"x:".to_vec()]);
        drop(writing);
        flush.await.unwrap().unwrap();
        compaction.await.unwrap().unwrap();

        // The idle range is retired; the one the flush listed is not, and
        // goes on hiding the keys the table holds.
        assert_eq!(listed(&engine), [b"b:".to_vec()]);
        assert!(engine.inner.imm_memtables.read().is_empty());
        assert_eq!(engine.get(b"b:010").await.unwrap(), None);
        assert!(engine
            .scan(Some(b"b:"), Some(b"b;"))
            .await
            .unwrap()
            .is_empty());

        // The next round of the compaction task rewrites the table and
        // only then retires the range.
        engine.inner.compact(false).await.unwrap();
        assert!(listed(&engine).is_empty());
        assert_eq!(engine.get(b"b:010").await.unwrap(), None);
        assert_eq!(engine.scan(None, None).await.unwrap().len(), 1);
        engine.close().await.unwrap();
    }
}
