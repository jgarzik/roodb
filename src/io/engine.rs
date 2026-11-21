//! The block I/O engine.
//!
//! One [`IoEngine`] is created at startup and every file the server touches is
//! opened through it. The backend is picked at run time:
//!
//! - `uring` (Linux): a few reactor threads, each owning one io_uring. Requests
//!   from every task are queued to a reactor, which submits whatever has
//!   accumulated with a single system call and completes the waiting tasks
//!   directly. Files are registered with the ring.
//! - `posix`: a pool of worker threads doing positional reads and writes. Used
//!   on other platforms and wherever io_uring is unavailable (old kernels,
//!   seccomp profiles that block it).
//!
//! Files are opened for direct I/O where the platform supports it, so the
//! server's own caches are the only caches. All offsets and transfer sizes are
//! multiples of [`PAGE_SIZE`].
//!
//! Buffers are passed in by value and handed back on completion. A request that
//! has been submitted keeps its buffer and its file alive until the kernel is
//! done with them, whether or not the caller is still waiting.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use super::aligned_buffer::AlignedBuffer;
use super::error::{IoError, IoResult};
use super::pool::Pool;
use super::scheduler::Urgency;
use super::traits::PAGE_SIZE;

/// Which backend to run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// io_uring where it works, otherwise the worker pool.
    Auto,
    /// io_uring; an error if the kernel refuses it.
    Uring,
    /// The portable worker pool.
    Posix,
}

impl IoBackend {
    /// The name used on the command line and in logs.
    pub fn as_str(self) -> &'static str {
        match self {
            IoBackend::Auto => "auto",
            IoBackend::Uring => "uring",
            IoBackend::Posix => "posix",
        }
    }
}

impl std::str::FromStr for IoBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(IoBackend::Auto),
            "uring" | "io_uring" | "io-uring" => Ok(IoBackend::Uring),
            "posix" => Ok(IoBackend::Posix),
            other => Err(format!(
                "unknown I/O backend '{other}' (expected auto, uring or posix)"
            )),
        }
    }
}

impl std::fmt::Display for IoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Engine sizing. Zero means "choose for this machine".
#[derive(Debug, Clone, Copy, Default)]
pub struct IoConfig {
    /// io_uring reactor threads.
    pub reactors: usize,
    /// Worker threads of the posix backend.
    pub workers: usize,
}

/// How a file is opened.
#[derive(Debug, Clone, Copy)]
pub struct OpenOptions {
    /// Create the file if it does not exist.
    pub create: bool,
    /// Fail if the file already exists (implies `create`).
    pub exclusive: bool,
    /// Discard existing contents.
    pub truncate: bool,
    /// Open without write access.
    pub read_only: bool,
    /// Bypass the kernel page cache where the platform allows it.
    pub direct: bool,
}

impl OpenOptions {
    /// An existing file, read-write, direct.
    pub fn existing() -> Self {
        Self {
            create: false,
            exclusive: false,
            truncate: false,
            read_only: false,
            direct: true,
        }
    }

    /// An existing file, read-only, direct.
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            ..Self::existing()
        }
    }

    /// Create if missing, keep contents if present.
    pub fn create() -> Self {
        Self {
            create: true,
            ..Self::existing()
        }
    }

    /// A file that must not exist yet.
    pub fn create_new() -> Self {
        Self {
            create: true,
            exclusive: true,
            ..Self::existing()
        }
    }

    /// Create if missing and discard any existing contents.
    pub fn create_truncate() -> Self {
        Self {
            create: true,
            truncate: true,
            ..Self::existing()
        }
    }
}

/// Counters for the whole engine.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub ops: AtomicU64,
    pub submits: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub syncs: AtomicU64,
}

/// A snapshot of the engine counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStats {
    /// Requests accepted.
    pub ops: u64,
    /// System calls made to hand requests to the kernel. With io_uring one
    /// call carries every request queued at that moment, so under load this is
    /// far below `ops`.
    pub submits: u64,
    /// Bytes read from files.
    pub bytes_read: u64,
    /// Bytes written to files.
    pub bytes_written: u64,
    /// Sync requests, including those attached to a write.
    pub syncs: u64,
}

/// What a request does.
pub(crate) enum OpKind {
    /// Fill `buf` up to its capacity, stopping early only at end of file.
    Read { buf: AlignedBuffer },
    /// Write the first `len` bytes of `buf`, then optionally sync
    /// (`Some(true)` = data only).
    Write {
        buf: AlignedBuffer,
        len: usize,
        sync: Option<bool>,
    },
    /// Flush the file (`data_only` skips metadata that is not needed to read
    /// the data back).
    Sync { data_only: bool },
    /// Reserve space for `len` bytes starting at the request offset.
    Allocate { len: u64 },
}

/// A request travelling to a backend.
pub(crate) struct Op {
    pub file: Arc<Handle>,
    pub kind: OpKind,
    pub offset: u64,
    pub urgency: Urgency,
    pub done: oneshot::Sender<Completion>,
}

/// The outcome of a request: bytes transferred, and the buffer if it had one.
pub(crate) struct Completion {
    pub result: std::io::Result<usize>,
    pub buf: Option<AlignedBuffer>,
}

impl Op {
    /// Finish the request. The caller may have stopped waiting; that is fine.
    pub(crate) fn complete(self, result: std::io::Result<usize>) {
        let buf = match self.kind {
            OpKind::Read { buf } | OpKind::Write { buf, .. } => Some(buf),
            OpKind::Sync { .. } | OpKind::Allocate { .. } => None,
        };
        let _ = self.done.send(Completion { result, buf });
    }
}

/// Where a file's requests go.
pub(crate) enum Route {
    Pool(Arc<super::pool::Shared>),
    #[cfg(target_os = "linux")]
    Uring(super::reactor::FileRoute),
}

/// The open file shared by a [`File`] and its in-flight requests.
pub(crate) struct Handle {
    pub file: std::fs::File,
    pub route: Route,
    pub counters: Arc<Counters>,
}

enum Backend {
    Pool(Pool),
    #[cfg(target_os = "linux")]
    Uring(super::reactor::Uring),
}

struct EngineInner {
    backend: Backend,
    kind: IoBackend,
    counters: Arc<Counters>,
}

/// The process-wide I/O engine. Cheap to clone.
#[derive(Clone)]
pub struct IoEngine {
    inner: Arc<EngineInner>,
}

impl IoEngine {
    /// Start an engine with default sizing.
    pub fn new(backend: IoBackend) -> IoResult<Self> {
        Self::with_config(backend, IoConfig::default())
    }

    /// Start an engine.
    pub fn with_config(backend: IoBackend, config: IoConfig) -> IoResult<Self> {
        let counters = Arc::new(Counters::default());
        let (backend, kind) = Self::start(backend, config, &counters)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                backend,
                kind,
                counters,
            }),
        })
    }

    #[cfg(target_os = "linux")]
    fn start(
        backend: IoBackend,
        config: IoConfig,
        counters: &Arc<Counters>,
    ) -> IoResult<(Backend, IoBackend)> {
        match backend {
            IoBackend::Posix => Ok((
                Backend::Pool(Pool::start(config.workers)?),
                IoBackend::Posix,
            )),
            IoBackend::Uring => Ok((
                Backend::Uring(super::reactor::Uring::start(
                    config.reactors,
                    counters.clone(),
                )?),
                IoBackend::Uring,
            )),
            IoBackend::Auto => {
                match super::reactor::Uring::start(config.reactors, counters.clone()) {
                    Ok(uring) => Ok((Backend::Uring(uring), IoBackend::Uring)),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "io_uring is not available; using the posix I/O backend"
                        );
                        Ok((
                            Backend::Pool(Pool::start(config.workers)?),
                            IoBackend::Posix,
                        ))
                    }
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn start(
        backend: IoBackend,
        config: IoConfig,
        _counters: &Arc<Counters>,
    ) -> IoResult<(Backend, IoBackend)> {
        match backend {
            IoBackend::Uring => Err(IoError::NotSupported(
                "io_uring is only available on Linux".to_string(),
            )),
            IoBackend::Posix | IoBackend::Auto => Ok((
                Backend::Pool(Pool::start(config.workers)?),
                IoBackend::Posix,
            )),
        }
    }

    /// The backend actually running (never [`IoBackend::Auto`]).
    pub fn backend(&self) -> IoBackend {
        self.inner.kind
    }

    /// Current counters.
    pub fn stats(&self) -> IoStats {
        let c = &self.inner.counters;
        IoStats {
            ops: c.ops.load(Ordering::Relaxed),
            submits: c.submits.load(Ordering::Relaxed),
            bytes_read: c.bytes_read.load(Ordering::Relaxed),
            bytes_written: c.bytes_written.load(Ordering::Relaxed),
            syncs: c.syncs.load(Ordering::Relaxed),
        }
    }

    /// Open a file.
    pub async fn open(&self, path: &Path, options: OpenOptions) -> IoResult<File> {
        let file = open_file(path, options)?;
        let counters = self.inner.counters.clone();
        let route = match &self.inner.backend {
            Backend::Pool(pool) => Route::Pool(pool.shared()),
            #[cfg(target_os = "linux")]
            Backend::Uring(uring) => Route::Uring(uring.route_for(&file).await),
        };
        Ok(File {
            handle: Arc::new(Handle {
                file,
                route,
                counters,
            }),
        })
    }
}

impl std::fmt::Debug for IoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoEngine")
            .field("backend", &self.inner.kind)
            .finish()
    }
}

fn open_file(path: &Path, options: OpenOptions) -> IoResult<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    if !options.read_only {
        opts.write(true);
        if options.exclusive {
            opts.create_new(true);
        } else if options.create {
            opts.create(true);
        }
        if options.truncate {
            opts.truncate(true);
        }
    }
    #[cfg(target_os = "linux")]
    if options.direct {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_DIRECT);
    }
    let file = match opts.open(path) {
        Ok(file) => file,
        // Some filesystems (tmpfs, a few overlay setups) refuse direct I/O.
        // Buffered I/O is slower and double-caches, but it is correct.
        #[cfg(target_os = "linux")]
        Err(e) if options.direct && e.raw_os_error() == Some(libc::EINVAL) => {
            use std::os::unix::fs::OpenOptionsExt;
            tracing::warn!(
                path = %path.display(),
                "filesystem does not support direct I/O; using buffered I/O"
            );
            opts.custom_flags(0).open(path)?
        }
        Err(e) => return Err(e.into()),
    };
    #[cfg(target_os = "macos")]
    if options.direct {
        use std::os::unix::io::AsRawFd;
        // macOS has no O_DIRECT; F_NOCACHE turns off caching for this descriptor.
        // SAFETY: fcntl on a descriptor we own, with an integer argument.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(file)
}

/// An open file. Cheap to clone; clones share the descriptor.
#[derive(Clone)]
pub struct File {
    handle: Arc<Handle>,
}

impl File {
    fn check_offset(offset: u64) -> IoResult<()> {
        if !offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(IoError::OffsetAlignment {
                offset,
                alignment: PAGE_SIZE,
            });
        }
        Ok(())
    }

    async fn run(&self, kind: OpKind, offset: u64, urgency: Urgency) -> IoResult<Completion> {
        let (done, wait) = oneshot::channel();
        let op = Op {
            file: self.handle.clone(),
            kind,
            offset,
            urgency,
            done,
        };
        self.handle.counters.ops.fetch_add(1, Ordering::Relaxed);
        let accepted = match &self.handle.route {
            Route::Pool(shared) => shared.submit(op),
            #[cfg(target_os = "linux")]
            Route::Uring(route) => route.submit(op),
        };
        if !accepted {
            return Err(IoError::Io(std::io::Error::other(
                "the I/O engine has shut down",
            )));
        }
        wait.await.map_err(|_| {
            IoError::Io(std::io::Error::other(
                "the I/O engine dropped a request while shutting down",
            ))
        })
    }

    /// Read into `buf` starting at `offset`, filling it to capacity unless the
    /// file ends first. The buffer comes back with its length set to the
    /// number of bytes read.
    pub async fn read_at(
        &self,
        buf: AlignedBuffer,
        offset: u64,
        urgency: Urgency,
    ) -> IoResult<AlignedBuffer> {
        Self::check_offset(offset)?;
        let done = self.run(OpKind::Read { buf }, offset, urgency).await?;
        let n = done.result?;
        let mut buf = done.buf.expect("a read returns its buffer");
        buf.set_len(n);
        self.handle
            .counters
            .bytes_read
            .fetch_add(n as u64, Ordering::Relaxed);
        Ok(buf)
    }

    /// Write the contents of `buf` at `offset`. The length is rounded up to a
    /// whole page and the padding is zero. The buffer is handed back for reuse.
    pub async fn write_at(
        &self,
        buf: AlignedBuffer,
        offset: u64,
        urgency: Urgency,
    ) -> IoResult<AlignedBuffer> {
        self.write(buf, offset, urgency, None).await
    }

    /// Write, then flush the data to stable storage, as one request. Returns
    /// once both have completed.
    pub async fn write_at_sync(
        &self,
        buf: AlignedBuffer,
        offset: u64,
        urgency: Urgency,
    ) -> IoResult<AlignedBuffer> {
        self.handle.counters.syncs.fetch_add(1, Ordering::Relaxed);
        self.write(buf, offset, urgency, Some(true)).await
    }

    async fn write(
        &self,
        mut buf: AlignedBuffer,
        offset: u64,
        urgency: Urgency,
        sync: Option<bool>,
    ) -> IoResult<AlignedBuffer> {
        Self::check_offset(offset)?;
        let data_len = buf.len();
        let len = AlignedBuffer::round_up(data_len);
        if len == 0 {
            return Ok(buf);
        }
        buf.as_mut_slice()[data_len..len].fill(0);
        let done = self
            .run(OpKind::Write { buf, len, sync }, offset, urgency)
            .await?;
        done.result?;
        self.handle
            .counters
            .bytes_written
            .fetch_add(len as u64, Ordering::Relaxed);
        Ok(done.buf.expect("a write returns its buffer"))
    }

    /// Flush written data to stable storage. Metadata that is not needed to
    /// read the data back (timestamps) may be left for later.
    pub async fn sync_data(&self, urgency: Urgency) -> IoResult<()> {
        self.sync(true, urgency).await
    }

    /// Flush data and all metadata to stable storage.
    pub async fn sync_all(&self, urgency: Urgency) -> IoResult<()> {
        self.sync(false, urgency).await
    }

    async fn sync(&self, data_only: bool, urgency: Urgency) -> IoResult<()> {
        self.handle.counters.syncs.fetch_add(1, Ordering::Relaxed);
        let done = self.run(OpKind::Sync { data_only }, 0, urgency).await?;
        done.result?;
        Ok(())
    }

    /// Reserve disk space for the first `len` bytes of the file and extend it
    /// to that size if it is shorter. Existing data is kept.
    pub async fn allocate(&self, len: u64) -> IoResult<()> {
        let done = self
            .run(OpKind::Allocate { len }, 0, Urgency::Background)
            .await?;
        done.result?;
        Ok(())
    }

    /// Current size in bytes.
    pub fn size(&self) -> IoResult<u64> {
        Ok(self.handle.file.metadata()?.len())
    }

    /// Set the size, discarding data past it or extending with zeros.
    pub fn set_len(&self, size: u64) -> IoResult<()> {
        self.handle.file.set_len(size)?;
        Ok(())
    }
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File").finish_non_exhaustive()
    }
}
