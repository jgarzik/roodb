//! The Raft log, on disk.
//!
//! This is the server's write-ahead log: a commit is acknowledged once its log
//! entry has been written here, and after a restart the state machine is
//! brought up to date by replaying entries from the position it last flushed.
//!
//! # Files
//!
//! The log lives in `raft/` under the data directory as a sequence of
//! fixed-size segment files, `log-<seq>.seg`, written through the block I/O
//! engine with direct I/O. A segment starts with a one-page header and is then
//! filled front to back with records:
//!
//! ```text
//! record:  | payload len (4B) | CRC32 (4B) | kind (1B) | payload |
//! kinds:   Entry, Vote, TruncateAfter, Purge, Committed, End
//! ```
//!
//! The CRC covers the segment's sequence number as well as the record, so
//! bytes left over from an earlier life of a recycled file never pass for
//! records of this one. Reading stops at the first record that does not
//! check out: in the last segment that is the end of the log (a torn write at
//! worst); in any earlier segment the writer left an `End` record, and
//! anything else there is corruption.
//!
//! The vote, the purge point and the commit point are records in the same
//! stream, and each segment header carries their values as of when the
//! segment was started, so old segments can be deleted without losing them.
//!
//! # Writing
//!
//! Callers frame their records and assign file positions under one lock, then
//! hand the bytes to a single writer task. The writer takes everything that
//! has queued up, writes it with one request, and only then completes the
//! waiting commits - so commits arriving together share a write, and with
//! `Durability::Fsync` they share the sync as well.
//!
//! Segments are preallocated. A spare is kept ready and purged segments are
//! recycled, so appending never has to grow a file.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::OptionalSend;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::io::{AlignedBuffer, File, IoEngine, OpenOptions, Urgency, PAGE_SIZE};
use crate::raft::types::{Entry, LogId, LogState, TypeConfig, Vote};

/// Directory, under the data directory, that holds the log.
pub const LOG_DIR: &str = "raft";

/// Default segment size.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Default budget for log entries kept decoded in memory.
pub const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

const MAGIC: &[u8; 8] = b"ROOLOG01";
const FORMAT_VERSION: u32 = 1;
const HEADER_SIZE: usize = PAGE_SIZE;
const HEADER_CRC_AT: usize = HEADER_SIZE - 4;
/// Record header: payload length, CRC, kind.
const RECORD_HEADER: usize = 9;
const SPARE_FILE: &str = "spare.seg";
const SPARE_TMP: &str = "spare.tmp";

const KIND_ENTRY: u8 = 1;
const KIND_VOTE: u8 = 2;
const KIND_TRUNCATE_AFTER: u8 = 3;
const KIND_PURGE: u8 = 4;
const KIND_COMMITTED: u8 = 5;
const KIND_END: u8 = 6;

/// When a commit is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Once the log write has completed. Survives the server crashing or being
    /// killed; a power failure or kernel crash can lose the last commits still
    /// in the drive's volatile cache.
    Os,
    /// Once the log write has been flushed to stable storage. Survives power
    /// failure.
    Fsync,
}

impl Durability {
    /// The name used on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            Durability::Os => "os",
            Durability::Fsync => "fsync",
        }
    }
}

impl std::str::FromStr for Durability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "os" => Ok(Durability::Os),
            "fsync" => Ok(Durability::Fsync),
            other => Err(format!(
                "unknown durability '{other}' (expected os or fsync)"
            )),
        }
    }
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Log store settings.
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// The log directory itself (normally `<data dir>/raft`).
    pub dir: PathBuf,
    pub durability: Durability,
    /// Size of each segment file. A multiple of the page size.
    pub segment_bytes: u64,
    /// Budget for entries kept decoded in memory. Older entries are read back
    /// from their segment when needed.
    pub cache_bytes: usize,
}

impl LogStoreConfig {
    /// Default settings for a data directory.
    pub fn for_data_dir(data_dir: &Path, durability: Durability) -> Self {
        Self {
            dir: data_dir.join(LOG_DIR),
            durability,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            cache_bytes: DEFAULT_CACHE_BYTES,
        }
    }
}

/// Counters, for tests and observability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogStats {
    /// Entries appended.
    pub entries: u64,
    /// Write requests issued for the log. Commits that arrive together share
    /// one, so under load this is well below `entries`.
    pub writes: u64,
    /// Of those, how many also flushed to stable storage.
    pub syncs: u64,
    /// Bytes written, padding included.
    pub bytes: u64,
    /// Segment files started.
    pub segments: u64,
}

#[derive(Default)]
struct Counters {
    entries: AtomicU64,
    writes: AtomicU64,
    syncs: AtomicU64,
    bytes: AtomicU64,
    segments: AtomicU64,
}

/// The values a segment starts from: enough to rebuild the log's state when
/// every earlier segment is gone.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Base {
    vote: Option<Vote>,
    committed: Option<LogId>,
    purged: Option<LogId>,
    /// The entry just before this segment's first entry.
    prev_log_id: Option<LogId>,
}

fn corrupt(what: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.into())
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| corrupt(format!("encoding a log record: {e}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> io::Result<T> {
    bincode::deserialize(bytes).map_err(|e| corrupt(format!("decoding a log record: {e}")))
}

fn record_crc(seq: u64, kind: u8, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&seq.to_le_bytes());
    hasher.update(&[kind]);
    hasher.update(payload);
    hasher.finalize()
}

/// Append one framed record to `out`.
fn frame(seq: u64, kind: u8, payload: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&record_crc(seq, kind, payload).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
}

fn segment_name(seq: u64) -> String {
    format!("log-{seq:016}.seg")
}

fn segment_seq(name: &str) -> Option<u64> {
    name.strip_prefix("log-")?
        .strip_suffix(".seg")?
        .parse()
        .ok()
}

fn encode_header(seq: u64, size: u64, base: &Base) -> io::Result<Vec<u8>> {
    let base = encode(base)?;
    let mut page = vec![0u8; HEADER_SIZE];
    if 32 + base.len() > HEADER_CRC_AT {
        return Err(corrupt("segment header does not fit in a page"));
    }
    page[0..8].copy_from_slice(MAGIC);
    page[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    page[12..20].copy_from_slice(&seq.to_le_bytes());
    page[20..28].copy_from_slice(&size.to_le_bytes());
    page[28..32].copy_from_slice(&(base.len() as u32).to_le_bytes());
    page[32..32 + base.len()].copy_from_slice(&base);
    let crc = crc32fast::hash(&page[..HEADER_CRC_AT]);
    page[HEADER_CRC_AT..].copy_from_slice(&crc.to_le_bytes());
    Ok(page)
}

/// Parse a segment header. Returns the segment's sequence number and base.
fn decode_header(page: &[u8]) -> io::Result<(u64, Base)> {
    if page.len() < HEADER_SIZE || &page[0..8] != MAGIC {
        return Err(corrupt("not a log segment"));
    }
    let stored = u32::from_le_bytes(page[HEADER_CRC_AT..HEADER_SIZE].try_into().expect("4"));
    if stored != crc32fast::hash(&page[..HEADER_CRC_AT]) {
        return Err(corrupt("log segment header checksum mismatch"));
    }
    let version = u32::from_le_bytes(page[8..12].try_into().expect("4"));
    if version != FORMAT_VERSION {
        return Err(corrupt(format!(
            "log segment format {version} is not supported (this build uses {FORMAT_VERSION})"
        )));
    }
    let seq = u64::from_le_bytes(page[12..20].try_into().expect("8"));
    let base_len = u32::from_le_bytes(page[28..32].try_into().expect("4")) as usize;
    if 32 + base_len > HEADER_CRC_AT {
        return Err(corrupt("log segment header is malformed"));
    }
    Ok((seq, decode(&page[32..32 + base_len])?))
}

/// What to do when a write has completed.
enum Done {
    /// Nothing is waiting.
    None,
    /// An append: tell openraft the entries are on disk.
    Flushed(IOFlushed<TypeConfig>),
    /// A caller is waiting for the result.
    Notify(oneshot::Sender<io::Result<()>>),
}

impl Done {
    fn complete(self, result: &io::Result<()>) {
        let result = || match result {
            Ok(()) => Ok(()),
            Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
        };
        match self {
            Done::None => {}
            Done::Flushed(callback) => callback.io_completed(result()),
            Done::Notify(tx) => {
                let _ = tx.send(result());
            }
        }
    }
}

/// Work for the writer task, in the order it must happen.
enum Cmd {
    /// Bytes for the current segment, contiguous with what came before.
    Data {
        bytes: Vec<u8>,
        done: Done,
        /// Flush to stable storage whatever the durability setting says.
        force_sync: bool,
        /// Where these bytes end: segment and offset.
        end: (u64, u64),
    },
    /// Start a new segment.
    Roll {
        seq: u64,
        size: u64,
        header: Vec<u8>,
    },
    /// These segments hold nothing that is still needed.
    Delete { seqs: Vec<u64> },
    /// Finish everything queued, then exit.
    Close(oneshot::Sender<()>),
}

/// Where an entry is and, while cached, the entry itself.
struct Slot {
    log_id: LogId,
    seq: u64,
    /// Offset of the record's payload in its segment.
    offset: u64,
    len: u32,
    entry: Option<Entry>,
}

/// The log as callers see it, and the position the next record goes to.
struct Mem {
    /// Entries present, oldest first; contiguous in index.
    slots: VecDeque<Slot>,
    vote: Option<Vote>,
    committed: Option<LogId>,
    /// `committed` has changed since it was last written.
    committed_dirty: bool,
    purged: Option<LogId>,
    last_log_id: Option<LogId>,
    /// Bytes of entries held in `slots`.
    cached_bytes: usize,
    /// Index up to which entries have been dropped from the cache.
    evicted_upto: u64,
    /// Segment being written and the offset of its next byte.
    seq: u64,
    offset: u64,
    /// Size of the segment being written.
    size: u64,
    /// Highest entry index each segment holds.
    segment_last_index: BTreeMap<u64, Option<u64>>,
}

impl Mem {
    fn base(&self) -> Base {
        Base {
            vote: self.vote,
            committed: self.committed,
            purged: self.purged,
            prev_log_id: self.last_log_id,
        }
    }

    fn first_index(&self) -> Option<u64> {
        self.slots.front().map(|s| s.log_id.index())
    }
}

/// Shared with the writer task.
struct WriterShared {
    io: IoEngine,
    config: LogStoreConfig,
    /// Open segment files, for reading entries back.
    files: Mutex<HashMap<u64, File>>,
    /// How far the log has been written: segment and offset. An entry whose
    /// record ends at or before this is on disk and may be dropped from
    /// memory. Positions only move forward and are never reused, unlike entry
    /// indexes, which a truncation hands out again.
    written: Mutex<(u64, u64)>,
    /// The first write error. Once set, the log refuses further work.
    failed: Mutex<Option<String>>,
    counters: Counters,
}

impl WriterShared {
    fn segment_path(&self, seq: u64) -> PathBuf {
        self.config.dir.join(segment_name(seq))
    }

    fn check(&self) -> io::Result<()> {
        match &*self.failed.lock() {
            Some(error) => Err(io::Error::other(format!(
                "the Raft log is unusable after a write failure: {error}"
            ))),
            None => Ok(()),
        }
    }
}

struct Shared {
    writer: Arc<WriterShared>,
    mem: Mutex<Mem>,
    tx: mpsc::UnboundedSender<Cmd>,
}

/// The on-disk Raft log. Cheap to clone; clones share the log.
#[derive(Clone)]
pub struct LogStore {
    shared: Arc<Shared>,
}

/// State handed to the writer task when it starts.
struct WriterTail {
    file: File,
    /// Start of the page the next byte falls in.
    page_offset: u64,
    /// Bytes already written from `page_offset` up to the next byte.
    partial: Vec<u8>,
}

async fn sync_dir(io: &IoEngine, dir: &Path) -> io::Result<()> {
    let options = OpenOptions {
        direct: false,
        ..OpenOptions::read_only()
    };
    // Not every platform can open a directory; where it cannot, a rename is
    // as durable as it is going to get.
    if let Ok(file) = io.open(dir, options).await {
        file.sync_all(Urgency::Critical)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}

async fn read_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let start = offset - offset % PAGE_SIZE as u64;
    let span = (offset - start) as usize + len;
    let buf = file
        .read_at(AlignedBuffer::new(span)?, start, Urgency::Foreground)
        .await
        .map_err(io::Error::other)?;
    let skip = (offset - start) as usize;
    if buf.len() < skip + len {
        return Err(corrupt("log segment is shorter than its records"));
    }
    Ok(buf[skip..skip + len].to_vec())
}

/// Create a segment file of `size` bytes with its space reserved.
async fn create_segment(io: &IoEngine, path: &Path, size: u64) -> io::Result<File> {
    let file = io
        .open(path, OpenOptions::create_new())
        .await
        .map_err(io::Error::other)?;
    file.allocate(size).await.map_err(io::Error::other)?;
    Ok(file)
}

/// Prepare a spare segment: a full-size file whose blocks have all been
/// written, so that using it later involves no allocation in the filesystem.
async fn prepare_spare(io: IoEngine, dir: PathBuf, size: u64) {
    let tmp = dir.join(SPARE_TMP);
    let spare = dir.join(SPARE_FILE);
    if spare.exists() {
        return;
    }
    let _ = std::fs::remove_file(&tmp);
    let result: io::Result<()> = async {
        let file = create_segment(&io, &tmp, size).await?;
        let chunk = 1 << 20;
        let mut offset = 0;
        while offset < size {
            let len = chunk.min((size - offset) as usize);
            let mut buf = AlignedBuffer::new(len)?;
            buf.zero();
            buf.set_len(len);
            file.write_at(buf, offset, Urgency::Background)
                .await
                .map_err(io::Error::other)?;
            offset += len as u64;
        }
        file.sync_data(Urgency::Background)
            .await
            .map_err(io::Error::other)?;
        drop(file);
        std::fs::rename(&tmp, &spare)?;
        Ok(())
    }
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "could not prepare a spare log segment");
        let _ = std::fs::remove_file(&tmp);
    }
}

/// The writer task: the only code that writes to segment files.
async fn writer_task(
    shared: Arc<WriterShared>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    mut tail: WriterTail,
) {
    let mut queue: VecDeque<Cmd> = VecDeque::new();
    loop {
        if queue.is_empty() {
            match rx.recv().await {
                Some(cmd) => queue.push_back(cmd),
                None => return,
            }
        }
        // Everything that has arrived by now goes out together.
        while let Ok(cmd) = rx.try_recv() {
            queue.push_back(cmd);
        }

        // Gather the run of data at the front of the queue into one write.
        let mut bytes: Vec<u8> = Vec::new();
        let mut waiting: Vec<Done> = Vec::new();
        let mut sync = false;
        let mut end: Option<(u64, u64)> = None;
        while let Some(Cmd::Data { .. }) = queue.front() {
            let Some(Cmd::Data {
                bytes: more,
                done,
                force_sync,
                end: this_end,
            }) = queue.pop_front()
            else {
                unreachable!("front was checked to be data");
            };
            if matches!(done, Done::Flushed(_)) && shared.config.durability == Durability::Fsync {
                sync = true;
            }
            sync |= force_sync;
            bytes.extend_from_slice(&more);
            waiting.push(done);
            end = Some(this_end);
        }

        if !waiting.is_empty() {
            let result = match shared.check() {
                Ok(()) => write_run(&shared, &mut tail, &bytes, sync).await,
                Err(e) => Err(e),
            };
            if let Err(e) = &result {
                tracing::error!(error = %e, "Raft log write failed");
                shared.failed.lock().get_or_insert_with(|| e.to_string());
            } else if let Some(end) = end {
                *shared.written.lock() = end;
            }
            for done in waiting {
                done.complete(&result);
            }
            continue;
        }

        match queue.pop_front() {
            Some(Cmd::Roll { seq, size, header }) => {
                let result = match shared.check() {
                    // The finished segment, End record included, is made
                    // durable before its successor exists. Recovery relies on
                    // it: a segment with a successor must end properly.
                    Ok(()) => match tail.file.sync_data(Urgency::Critical).await {
                        Ok(()) => roll(&shared, seq, size, header).await,
                        Err(e) => Err(e.into()),
                    },
                    Err(e) => Err(e),
                };
                match result {
                    Ok(new_tail) => tail = new_tail,
                    Err(e) => {
                        tracing::error!(error = %e, seq, "could not start a new Raft log segment");
                        shared.failed.lock().get_or_insert_with(|| e.to_string());
                    }
                }
            }
            Some(Cmd::Delete { seqs }) => delete_segments(&shared, seqs).await,
            Some(Cmd::Close(done)) => {
                let _ = done.send(());
                return;
            }
            Some(Cmd::Data { .. }) | None => {}
        }
    }
}

/// Write `bytes` at the tail of the current segment with one request.
async fn write_run(
    shared: &WriterShared,
    tail: &mut WriterTail,
    bytes: &[u8],
    sync: bool,
) -> io::Result<()> {
    // Direct I/O writes whole pages, so the write starts at the beginning of
    // the page the tail is in and repeats the bytes already there.
    let total = tail.partial.len() + bytes.len();
    let mut buf = AlignedBuffer::new(total.max(1))?;
    {
        let dst = buf.as_mut_slice();
        dst[..tail.partial.len()].copy_from_slice(&tail.partial);
        dst[tail.partial.len()..total].copy_from_slice(bytes);
    }
    buf.set_len(total);

    let padded = AlignedBuffer::round_up(total) as u64;
    if sync {
        tail.file
            .write_at_sync(buf, tail.page_offset, Urgency::Critical)
            .await
            .map_err(io::Error::other)?;
        shared.counters.syncs.fetch_add(1, Ordering::Relaxed);
    } else {
        tail.file
            .write_at(buf, tail.page_offset, Urgency::Critical)
            .await
            .map_err(io::Error::other)?;
    }
    shared.counters.writes.fetch_add(1, Ordering::Relaxed);
    shared.counters.bytes.fetch_add(padded, Ordering::Relaxed);

    // Keep what lies in the last, incomplete page for the next write.
    let whole = total - total % PAGE_SIZE;
    let mut partial = Vec::with_capacity(PAGE_SIZE);
    if whole < tail.partial.len() {
        partial.extend_from_slice(&tail.partial[whole..]);
        partial.extend_from_slice(bytes);
    } else {
        partial.extend_from_slice(&bytes[whole - tail.partial.len()..]);
    }
    tail.partial = partial;
    tail.page_offset += whole as u64;
    Ok(())
}

/// Start segment `seq`: take the spare if there is one, write the header and
/// make the file and its name durable before any record goes into it.
async fn roll(
    shared: &Arc<WriterShared>,
    seq: u64,
    size: u64,
    header: Vec<u8>,
) -> io::Result<WriterTail> {
    let dir = &shared.config.dir;
    let path = shared.segment_path(seq);
    let spare = dir.join(SPARE_FILE);
    let spare_fits = size == shared.config.segment_bytes
        && std::fs::metadata(&spare).is_ok_and(|m| m.len() == size);
    let file = if spare_fits && std::fs::rename(&spare, &path).is_ok() {
        shared
            .io
            .open(&path, OpenOptions::existing())
            .await
            .map_err(io::Error::other)?
    } else {
        create_segment(&shared.io, &path, size).await?
    };

    let mut buf = AlignedBuffer::new(HEADER_SIZE)?;
    buf.copy_from_slice(&header)?;
    file.write_at_sync(buf, 0, Urgency::Critical)
        .await
        .map_err(io::Error::other)?;
    sync_dir(&shared.io, dir).await?;

    shared.files.lock().insert(seq, file.clone());
    shared.counters.segments.fetch_add(1, Ordering::Relaxed);

    // Have the next one ready before it is needed.
    tokio::spawn(prepare_spare(
        shared.io.clone(),
        dir.clone(),
        shared.config.segment_bytes,
    ));
    Ok(WriterTail {
        file,
        page_offset: HEADER_SIZE as u64,
        partial: Vec::new(),
    })
}

async fn delete_segments(shared: &WriterShared, seqs: Vec<u64>) {
    let spare = shared.config.dir.join(SPARE_FILE);
    for seq in seqs {
        shared.files.lock().remove(&seq);
        let path = shared.segment_path(seq);
        // A purged segment of the standard size becomes the spare if there is
        // none: its blocks are already allocated and written.
        let recyclable = !spare.exists()
            && std::fs::metadata(&path).is_ok_and(|m| m.len() == shared.config.segment_bytes);
        let result = if recyclable {
            std::fs::rename(&path, &spare)
        } else {
            std::fs::remove_file(&path)
        };
        if let Err(e) = result {
            tracing::warn!(seq, error = %e, "could not remove a purged Raft log segment");
        }
    }
}

/// What reading a segment's records found.
struct Scan {
    /// Offset just past the last good record.
    end: u64,
    /// The segment finished with an `End` record.
    ended: bool,
}

impl LogStore {
    /// Open the log in `config.dir`, creating it if it does not exist, and
    /// rebuild its state from what is on disk.
    pub async fn open(io: IoEngine, config: LogStoreConfig) -> io::Result<Self> {
        if config.segment_bytes < 4 * PAGE_SIZE as u64
            || !config.segment_bytes.is_multiple_of(PAGE_SIZE as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log segment size must be a multiple of the page size and at least four pages",
            ));
        }
        std::fs::create_dir_all(&config.dir)?;
        let _ = std::fs::remove_file(config.dir.join(SPARE_TMP));

        let mut seqs: Vec<u64> = std::fs::read_dir(&config.dir)?
            .filter_map(Result::ok)
            .filter_map(|e| segment_seq(&e.file_name().to_string_lossy()))
            .collect();
        seqs.sort_unstable();

        // A crash while a segment was being started leaves a file that was
        // never written to: its header is made durable before the first
        // record goes in. Such a file is the newest one and is recognisable -
        // too short, all zeros, or (a recycled file) still carrying the header
        // of the segment it used to be. Drop it; the log ends in the segment
        // before. A header that is none of those is damage, and is reported.
        if let Some(&newest) = seqs.last() {
            let path = config.dir.join(segment_name(newest));
            let page = {
                let file = io.open(&path, OpenOptions::existing()).await?;
                if file.size()? >= HEADER_SIZE as u64 {
                    read_exact(&file, 0, HEADER_SIZE).await?
                } else {
                    Vec::new()
                }
            };
            let never_started = page.is_empty()
                || page.iter().all(|b| *b == 0)
                || decode_header(&page).is_ok_and(|(seq, _)| seq != newest);
            if never_started {
                tracing::warn!(
                    seq = newest,
                    "discarding a Raft log segment that was never started"
                );
                std::fs::remove_file(&path)?;
                seqs.pop();
            }
        }

        let writer = Arc::new(WriterShared {
            io: io.clone(),
            config: config.clone(),
            files: Mutex::new(HashMap::new()),
            written: Mutex::new((0, 0)),
            failed: Mutex::new(None),
            counters: Counters::default(),
        });

        let mut mem = Mem {
            slots: VecDeque::new(),
            vote: None,
            committed: None,
            committed_dirty: false,
            purged: None,
            last_log_id: None,
            cached_bytes: 0,
            evicted_upto: 0,
            seq: 0,
            offset: HEADER_SIZE as u64,
            size: config.segment_bytes,
            segment_last_index: BTreeMap::new(),
        };

        let mut tail: Option<WriterTail> = None;
        let count = seqs.len();
        for (position, seq) in seqs.iter().copied().enumerate() {
            let last = position + 1 == count;
            let path = writer.segment_path(seq);
            let file = io
                .open(&path, OpenOptions::existing())
                .await
                .map_err(io::Error::other)?;
            let size = file.size().map_err(io::Error::other)?;
            let contents = if size >= HEADER_SIZE as u64 {
                read_exact(&file, 0, size as usize).await?
            } else {
                Vec::new()
            };
            let header = decode_header(&contents).and_then(|(found, base)| {
                if found == seq {
                    Ok(base)
                } else {
                    Err(corrupt("log segment header names a different segment"))
                }
            });
            let base = header?;

            if position == 0 {
                mem.vote = base.vote;
                mem.committed = base.committed;
                mem.purged = base.purged;
                // Entries before this segment are gone, whatever the purge
                // record in a deleted segment said.
                if base.prev_log_id.map(|id| id.index()) > mem.purged.map(|id| id.index()) {
                    mem.purged = base.prev_log_id;
                }
                mem.last_log_id = mem.purged;
            }

            mem.segment_last_index.insert(seq, None);
            let scan = Self::replay_segment(&mut mem, seq, &contents)?;
            // Every segment but the newest was closed with an End record
            // before its successor was started. (If the successor was never
            // started it has been dropped above, and this is the newest.)
            if !last && !scan.ended {
                return Err(corrupt(format!(
                    "Raft log segment {seq} is damaged at offset {}",
                    scan.end
                )));
            }
            writer.files.lock().insert(seq, file.clone());

            mem.seq = seq;
            mem.offset = scan.end;
            mem.size = size;
            if last && !scan.ended {
                let page_offset = scan.end - scan.end % PAGE_SIZE as u64;
                let partial = contents[page_offset as usize..scan.end as usize].to_vec();
                // Whatever lies beyond the end of the log - the rest of a
                // torn write, or whole records that followed it - must not be
                // there when new records are written over this spot: an old
                // record that happened to start where a new one ends would be
                // replayed as if it came after it.
                if let Some(extent) = contents[scan.end as usize..].iter().rposition(|b| *b != 0) {
                    let dirty_end = (scan.end as usize + extent + 1).next_multiple_of(PAGE_SIZE);
                    let span = dirty_end - page_offset as usize;
                    let mut buf = AlignedBuffer::new(span)?;
                    buf.zero();
                    buf.as_mut_slice()[..partial.len()].copy_from_slice(&partial);
                    buf.set_len(span);
                    file.write_at_sync(buf, page_offset, Urgency::Critical)
                        .await?;
                }
                tail = Some(WriterTail {
                    file,
                    page_offset,
                    partial,
                });
            }
        }

        // Everything recovered is on disk.
        *writer.written.lock() = (mem.seq, mem.offset);
        Self::evict(&mut mem, &writer);

        let (tx, rx) = mpsc::unbounded_channel();
        let tail = match tail {
            Some(tail) => tail,
            None => {
                // No usable segment to continue in: start one. The writer has
                // not started yet, so do its work here.
                let seq = if seqs.is_empty() { 1 } else { mem.seq + 1 };
                let header = encode_header(seq, config.segment_bytes, &mem.base())?;
                let tail = roll(&writer, seq, config.segment_bytes, header).await?;
                mem.seq = seq;
                mem.offset = HEADER_SIZE as u64;
                mem.size = config.segment_bytes;
                mem.segment_last_index.insert(seq, None);
                tail
            }
        };

        tokio::spawn(writer_task(writer.clone(), rx, tail));
        tokio::spawn(prepare_spare(io, config.dir.clone(), config.segment_bytes));

        Ok(Self {
            shared: Arc::new(Shared {
                writer,
                mem: Mutex::new(mem),
                tx,
            }),
        })
    }

    /// Apply the records of one segment to `mem`.
    fn replay_segment(mem: &mut Mem, seq: u64, contents: &[u8]) -> io::Result<Scan> {
        let mut pos = HEADER_SIZE;
        loop {
            let Some(header) = contents.get(pos..pos + RECORD_HEADER) else {
                return Ok(Scan {
                    end: pos as u64,
                    ended: false,
                });
            };
            let len = u32::from_le_bytes(header[0..4].try_into().expect("4")) as usize;
            let crc = u32::from_le_bytes(header[4..8].try_into().expect("4"));
            let kind = header[8];
            let payload_at = pos + RECORD_HEADER;
            let good = (KIND_ENTRY..=KIND_END).contains(&kind)
                && contents
                    .get(payload_at..payload_at + len)
                    .is_some_and(|payload| record_crc(seq, kind, payload) == crc);
            if !good {
                return Ok(Scan {
                    end: pos as u64,
                    ended: false,
                });
            }
            let payload = &contents[payload_at..payload_at + len];
            pos = payload_at + len;

            match kind {
                KIND_ENTRY => {
                    let entry: Entry = decode(payload)?;
                    let log_id = entry.log_id();
                    let expected = mem.last_log_id.map_or(0, |id| id.index() + 1);
                    if log_id.index() != expected {
                        return Err(corrupt(format!(
                            "Raft log segment {seq} holds entry {} where entry {expected} belongs",
                            log_id.index()
                        )));
                    }
                    mem.cached_bytes += len;
                    mem.slots.push_back(Slot {
                        log_id,
                        seq,
                        offset: payload_at as u64,
                        len: len as u32,
                        entry: Some(entry),
                    });
                    mem.last_log_id = Some(log_id);
                    mem.segment_last_index.insert(seq, Some(log_id.index()));
                }
                KIND_VOTE => mem.vote = Some(decode(payload)?),
                KIND_TRUNCATE_AFTER => Self::truncate_mem(mem, decode(payload)?),
                KIND_PURGE => Self::purge_mem(mem, decode(payload)?),
                KIND_COMMITTED => mem.committed = decode(payload)?,
                KIND_END => {
                    return Ok(Scan {
                        end: pos as u64,
                        ended: true,
                    })
                }
                _ => unreachable!("kind was range-checked"),
            }
        }
    }

    fn truncate_mem(mem: &mut Mem, last_log_id: Option<LogId>) {
        let keep_upto = last_log_id.map(|id| id.index());
        while let Some(slot) = mem.slots.back() {
            if keep_upto.is_some_and(|keep| slot.log_id.index() <= keep) {
                break;
            }
            if let Some(slot) = mem.slots.pop_back() {
                if slot.entry.is_some() {
                    mem.cached_bytes -= slot.len as usize;
                }
            }
        }
        mem.last_log_id = mem.slots.back().map(|s| s.log_id).or(mem.purged);
        // Indexes past here will be handed out again; eviction must look at
        // the new entries, not skip them as already done.
        let next = mem.last_log_id.map_or(0, |id| id.index() + 1);
        mem.evicted_upto = mem.evicted_upto.min(next);
    }

    fn purge_mem(mem: &mut Mem, log_id: LogId) {
        if mem.purged.is_some_and(|p| p.index() >= log_id.index()) {
            return;
        }
        while let Some(slot) = mem.slots.front() {
            if slot.log_id.index() > log_id.index() {
                break;
            }
            if let Some(slot) = mem.slots.pop_front() {
                if slot.entry.is_some() {
                    mem.cached_bytes -= slot.len as usize;
                }
            }
        }
        mem.purged = Some(log_id);
        if mem
            .last_log_id
            .is_none_or(|last| last.index() < log_id.index())
        {
            mem.last_log_id = Some(log_id);
        }
    }

    /// Drop the oldest cached entries while over budget. Only entries already
    /// written can go: an entry not yet on disk exists nowhere else.
    fn evict(mem: &mut Mem, writer: &WriterShared) {
        if mem.cached_bytes <= writer.config.cache_bytes {
            return;
        }
        let written = *writer.written.lock();
        let Some(first) = mem.first_index() else {
            return;
        };
        let mut at = mem.evicted_upto.max(first);
        while mem.cached_bytes > writer.config.cache_bytes {
            let Some(slot) = mem.slots.get_mut((at - first) as usize) else {
                break;
            };
            if (slot.seq, slot.offset + slot.len as u64) > written {
                break;
            }
            if slot.entry.take().is_some() {
                mem.cached_bytes -= slot.len as usize;
            }
            at += 1;
        }
        mem.evicted_upto = at;
    }

    /// Frame a record, give it its place in the log and queue it for writing.
    /// Returns the offset of its payload. Called with the state lock held, so
    /// records reach the writer in the order their positions were assigned.
    fn enqueue(
        &self,
        mem: &mut Mem,
        records: &[(u8, Vec<u8>)],
        done: Done,
        force_sync: bool,
        last_index: Option<u64>,
    ) -> io::Result<Vec<u64>> {
        let writer = &self.shared.writer;
        let needed: u64 = records
            .iter()
            .map(|(_, payload)| (RECORD_HEADER + payload.len()) as u64)
            .sum();
        // Room is always kept for the End record that closes the segment.
        let end_record = RECORD_HEADER as u64;
        if mem.offset + needed + end_record > mem.size {
            let mut bytes = Vec::with_capacity(RECORD_HEADER);
            frame(mem.seq, KIND_END, &[], &mut bytes);
            mem.offset += end_record;
            self.send(Cmd::Data {
                bytes,
                done: Done::None,
                force_sync: false,
                end: (mem.seq, mem.offset),
            })?;

            // A batch larger than a standard segment gets one made to fit.
            let fits = HEADER_SIZE as u64 + needed + end_record;
            let size = writer
                .config
                .segment_bytes
                .max(fits.next_multiple_of(PAGE_SIZE as u64));
            let seq = mem.seq + 1;
            let header = encode_header(seq, size, &mem.base())?;
            self.send(Cmd::Roll { seq, size, header })?;
            mem.seq = seq;
            mem.offset = HEADER_SIZE as u64;
            mem.size = size;
            mem.segment_last_index.insert(seq, None);
        }

        let mut bytes = Vec::with_capacity(needed as usize);
        let mut offsets = Vec::with_capacity(records.len());
        for (kind, payload) in records {
            offsets.push(mem.offset + bytes.len() as u64 + RECORD_HEADER as u64);
            frame(mem.seq, *kind, payload, &mut bytes);
        }
        mem.offset += needed;
        if let Some(index) = last_index {
            mem.segment_last_index.insert(mem.seq, Some(index));
        }
        self.send(Cmd::Data {
            bytes,
            done,
            force_sync,
            end: (mem.seq, mem.offset),
        })?;
        Ok(offsets)
    }

    fn send(&self, cmd: Cmd) -> io::Result<()> {
        self.shared
            .tx
            .send(cmd)
            .map_err(|_| io::Error::other("the Raft log writer has stopped"))
    }

    /// Counters since the store was opened.
    pub fn stats(&self) -> LogStats {
        let c = &self.shared.writer.counters;
        LogStats {
            entries: c.entries.load(Ordering::Relaxed),
            writes: c.writes.load(Ordering::Relaxed),
            syncs: c.syncs.load(Ordering::Relaxed),
            bytes: c.bytes.load(Ordering::Relaxed),
            segments: c.segments.load(Ordering::Relaxed),
        }
    }

    /// Bytes the log's live segments occupy on disk.
    pub fn disk_bytes(&self) -> u64 {
        let mem = self.shared.mem.lock();
        let earlier = mem.segment_last_index.len().saturating_sub(1) as u64;
        earlier * self.shared.writer.config.segment_bytes + mem.offset
    }

    /// Finish every write that has been queued and stop the writer.
    pub async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Close(tx)).is_ok() {
            let _ = rx.await;
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => i + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => i.saturating_add(1),
            Bound::Excluded(i) => *i,
            Bound::Unbounded => u64::MAX,
        };

        // Take what is cached; note where the rest is.
        let mut found: Vec<Result<Entry, (u64, u64, u32)>> = Vec::new();
        {
            let mem = self.shared.mem.lock();
            let Some(first) = mem.first_index() else {
                return Ok(Vec::new());
            };
            let from = start.max(first);
            let to = end.min(first + mem.slots.len() as u64);
            for index in from..to {
                let slot = &mem.slots[(index - first) as usize];
                found.push(match &slot.entry {
                    Some(entry) => Ok(entry.clone()),
                    None => Err((slot.seq, slot.offset, slot.len)),
                });
            }
        }

        let mut entries = Vec::with_capacity(found.len());
        for item in found {
            match item {
                Ok(entry) => entries.push(entry),
                Err((seq, offset, len)) => {
                    let file = self.shared.writer.files.lock().get(&seq).cloned();
                    let file =
                        file.ok_or_else(|| corrupt(format!("Raft log segment {seq} is gone")))?;
                    // Read the record with its header and check it before
                    // trusting it: this is data coming back from disk.
                    let record = read_exact(
                        &file,
                        offset - RECORD_HEADER as u64,
                        RECORD_HEADER + len as usize,
                    )
                    .await?;
                    let stored_len = u32::from_le_bytes(record[0..4].try_into().expect("4"));
                    let stored_crc = u32::from_le_bytes(record[4..8].try_into().expect("4"));
                    let payload = &record[RECORD_HEADER..];
                    if stored_len != len
                        || record[8] != KIND_ENTRY
                        || stored_crc != record_crc(seq, KIND_ENTRY, payload)
                    {
                        return Err(corrupt(format!(
                            "Raft log segment {seq} is damaged at offset {offset}"
                        )));
                    }
                    entries.push(decode(payload)?);
                }
            }
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.shared.mem.lock().vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState, io::Error> {
        let mem = self.shared.mem.lock();
        Ok(LogState {
            last_purged_log_id: mem.purged,
            last_log_id: mem.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.shared.writer.check()?;
        let (tx, rx) = oneshot::channel();
        {
            let mut mem = self.shared.mem.lock();
            mem.vote = Some(*vote);
            // A vote must never be forgotten, whatever the durability mode:
            // voting twice in one term can elect two leaders.
            self.enqueue(
                &mut mem,
                &[(KIND_VOTE, encode(vote)?)],
                Done::Notify(tx),
                true,
                None,
            )?;
        }
        rx.await
            .map_err(|_| io::Error::other("the Raft log writer has stopped"))?
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        // Written along with the next append rather than on its own: it only
        // ever needs to be a lower bound.
        let mut mem = self.shared.mem.lock();
        if mem.committed != committed {
            mem.committed = committed;
            mem.committed_dirty = true;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.shared.mem.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.shared.writer.check()?;
        let entries: Vec<Entry> = entries.into_iter().collect();
        let mut records = Vec::with_capacity(entries.len() + 1);
        for entry in &entries {
            records.push((KIND_ENTRY, encode(entry)?));
        }
        let count = entries.len();

        let mut mem = self.shared.mem.lock();
        let committed_first = mem.committed_dirty;
        if committed_first {
            records.insert(0, (KIND_COMMITTED, encode(&mem.committed)?));
            mem.committed_dirty = false;
        }
        let last_index = entries.last().map(|e| e.log_id().index());
        let offsets = self.enqueue(
            &mut mem,
            &records,
            Done::Flushed(callback),
            false,
            last_index,
        )?;

        // The entries are readable from here on, before they are on disk.
        let seq = mem.seq;
        let skip = usize::from(committed_first);
        for (i, entry) in entries.into_iter().enumerate() {
            let len = records[skip + i].1.len() as u32;
            let log_id = entry.log_id();
            mem.cached_bytes += len as usize;
            mem.slots.push_back(Slot {
                log_id,
                seq,
                offset: offsets[skip + i],
                len,
                entry: Some(entry),
            });
            mem.last_log_id = Some(log_id);
        }
        Self::evict(&mut mem, &self.shared.writer);
        self.shared
            .writer
            .counters
            .entries
            .fetch_add(count as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        self.shared.writer.check()?;
        let mut mem = self.shared.mem.lock();
        Self::truncate_mem(&mut mem, last_log_id);
        // Entries appended later reuse these indexes; the record tells
        // recovery to forget the old ones first.
        self.enqueue(
            &mut mem,
            &[(KIND_TRUNCATE_AFTER, encode(&last_log_id)?)],
            Done::None,
            false,
            None,
        )?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        self.shared.writer.check()?;
        let mut mem = self.shared.mem.lock();
        Self::purge_mem(&mut mem, log_id);
        self.enqueue(
            &mut mem,
            &[(KIND_PURGE, encode(&log_id)?)],
            Done::None,
            false,
            None,
        )?;

        // Segments wholly at or below the purge point are no longer needed.
        // The one being written stays, as does anything after an entry that
        // is still live.
        let current = mem.seq;
        let mut dead = Vec::new();
        for (&seq, &last_index) in &mem.segment_last_index {
            if seq >= current {
                break;
            }
            if last_index.is_some_and(|last| last > log_id.index()) {
                break;
            }
            dead.push(seq);
        }
        if !dead.is_empty() {
            for seq in &dead {
                mem.segment_last_index.remove(seq);
            }
            self.send(Cmd::Delete { seqs: dead })?;
        }
        Ok(())
    }
}
