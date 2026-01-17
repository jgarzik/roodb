//! Main I/O scheduler implementation
//!
//! Coordinates I/O requests across priority queues, handles backpressure,
//! and batches operations for efficient submission.
//!
//! # Architecture
//!
//! The scheduler wraps raw AsyncIO handles with scheduling coordination:
//!
//! ```text
//! IoScheduler<IO, F> implements AsyncIOFactory<IO = ScheduledHandle<IO>>
//!       │
//!       └─> open() returns ScheduledHandle<IO>
//!                  │
//!                  └─> implements AsyncIO
//!                      └─> delegates to inner IO with metrics/backpressure
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Notify;

use super::backpressure::BackpressureController;
use super::config::SchedulerConfig;
use super::metrics::IoMetrics;
use super::{IoContext, IoPriority, Urgency};
use crate::io::aligned_buffer::AlignedBuffer;
use crate::io::error::{IoError, IoResult};
use crate::io::traits::{AsyncIO, AsyncIOFactory};

// ============================================================================
// ScheduledHandle - AsyncIO wrapper with scheduling
// ============================================================================

/// A file handle wrapped with scheduler coordination.
///
/// This implements `AsyncIO` and adds:
/// - Backpressure checking before I/O operations
/// - Metrics collection for all operations
/// - Priority-based decisions using `IoContext`
pub struct ScheduledHandle<IO: AsyncIO> {
    /// The underlying I/O handle
    inner: IO,
    /// Shared metrics
    metrics: Arc<IoMetrics>,
    /// Shared backpressure controller
    backpressure: Arc<BackpressureController>,
    /// Scheduler configuration
    config: SchedulerConfig,
}

impl<IO: AsyncIO> ScheduledHandle<IO> {
    /// Create a new scheduled handle
    fn new(
        inner: IO,
        metrics: Arc<IoMetrics>,
        backpressure: Arc<BackpressureController>,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            inner,
            metrics,
            backpressure,
            config,
        }
    }

    /// Check backpressure and wait if needed based on urgency
    async fn check_backpressure(&self, ctx: &IoContext) {
        match ctx.urgency {
            Urgency::Critical => {
                // Critical operations never wait
            }
            Urgency::Foreground => {
                // Foreground waits only if saturated
                if self.backpressure.should_reject_writes() {
                    self.backpressure.wait_if_saturated().await;
                }
            }
            Urgency::Background => {
                // Background waits if under any pressure
                if self.backpressure.should_slow_flush() {
                    self.backpressure.wait_if_heavy().await;
                }
            }
        }
    }

    /// Record operation completion and update backpressure
    fn record_completion(&self, ctx: &IoContext, latency: std::time::Duration, size: usize) {
        // Use legacy priority for metrics (until metrics are updated)
        let priority = match ctx.urgency {
            Urgency::Critical => IoPriority::Wal,
            Urgency::Foreground => IoPriority::QueryRead,
            Urgency::Background => IoPriority::Flush,
        };
        self.metrics.record_completion(priority, latency, size);

        // Update backpressure state
        let queue_depth = self.metrics.queue_depth.load(Ordering::Relaxed) as usize;
        let avg_latency = self.metrics.average_latency();
        self.backpressure
            .update(queue_depth, avg_latency, &self.config);
    }
}

#[async_trait]
impl<IO: AsyncIO + 'static> AsyncIO for ScheduledHandle<IO> {
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize> {
        // Default to foreground read context
        self.read_at_with_context(buf, offset, IoContext::foreground_read())
            .await
    }

    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize> {
        // Default to background write context
        self.write_at_with_context(buf, offset, IoContext::background_write())
            .await
    }

    async fn sync(&self) -> IoResult<()> {
        // Default to background sync context
        self.sync_with_context(IoContext::background_sync()).await
    }

    async fn sync_with_context(&self, ctx: IoContext) -> IoResult<()> {
        ScheduledHandle::sync_with_context(self, ctx).await
    }

    async fn file_size(&self) -> IoResult<u64> {
        self.inner.file_size().await
    }

    async fn truncate(&self, size: u64) -> IoResult<()> {
        self.inner.truncate(size).await
    }

    async fn read_at_priority(
        &self,
        buf: &mut AlignedBuffer,
        offset: u64,
        priority: IoPriority,
    ) -> IoResult<usize> {
        self.read_at_with_context(buf, offset, priority.to_context())
            .await
    }

    async fn write_at_priority(
        &self,
        buf: &AlignedBuffer,
        offset: u64,
        priority: IoPriority,
    ) -> IoResult<usize> {
        self.write_at_with_context(buf, offset, priority.to_context())
            .await
    }

    fn supports_batch(&self) -> bool {
        self.inner.supports_batch()
    }

    async fn read_batch(&self, requests: &[(u64, usize)]) -> Vec<IoResult<AlignedBuffer>> {
        self.inner.read_batch(requests).await
    }

    async fn write_batch(&self, requests: &[(u64, AlignedBuffer)]) -> Vec<IoResult<usize>> {
        self.inner.write_batch(requests).await
    }
}

/// Extended AsyncIO methods with IoContext support
impl<IO: AsyncIO + 'static> ScheduledHandle<IO> {
    /// Read with full IoContext
    pub async fn read_at_with_context(
        &self,
        buf: &mut AlignedBuffer,
        offset: u64,
        ctx: IoContext,
    ) -> IoResult<usize> {
        self.check_backpressure(&ctx).await;

        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        let result = self.inner.read_at(buf, offset).await;

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.record_completion(&ctx, latency, buf.len());

        if result.is_ok() {
            self.metrics.record_read(buf.len());
        }

        result
    }

    /// Write with full IoContext
    pub async fn write_at_with_context(
        &self,
        buf: &AlignedBuffer,
        offset: u64,
        ctx: IoContext,
    ) -> IoResult<usize> {
        self.check_backpressure(&ctx).await;

        let start = Instant::now();
        let size = buf.len();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        let result = self.inner.write_at(buf, offset).await;

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.record_completion(&ctx, latency, size);

        if let Ok(bytes) = result {
            self.metrics.record_write(bytes);
        }

        result
    }

    /// Sync with full IoContext
    pub async fn sync_with_context(&self, ctx: IoContext) -> IoResult<()> {
        self.check_backpressure(&ctx).await;

        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        let result = self.inner.sync().await;

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.record_completion(&ctx, latency, 0);

        result
    }
}

// ============================================================================
// Legacy FileHandle (kept for compatibility with existing code)
// ============================================================================

/// Handle to an open file managed by the scheduler
///
/// **Deprecated**: Use `ScheduledHandle` directly via `IoScheduler` as an
/// `AsyncIOFactory`. This type is kept for backward compatibility.
#[derive(Clone)]
pub struct FileHandle {
    /// Unique file ID
    id: u64,
    /// Path to the file
    path: PathBuf,
    /// Reference to scheduler inner state
    scheduler: Arc<IoSchedulerInner>,
}

impl FileHandle {
    /// Get the file ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read data at the specified offset
    pub async fn read(
        &self,
        offset: u64,
        size: usize,
        priority: IoPriority,
    ) -> IoResult<AlignedBuffer> {
        self.scheduler.read(self.id, offset, size, priority).await
    }

    /// Write data at the specified offset
    pub async fn write(
        &self,
        offset: u64,
        data: AlignedBuffer,
        priority: IoPriority,
    ) -> IoResult<usize> {
        self.scheduler.write(self.id, offset, data, priority).await
    }

    /// Sync the file to disk
    pub async fn sync(&self, priority: IoPriority) -> IoResult<()> {
        self.scheduler.sync(self.id, priority).await
    }

    /// Get the file size
    pub async fn file_size(&self) -> IoResult<u64> {
        self.scheduler.file_size(self.id).await
    }

    /// Truncate the file
    pub async fn truncate(&self, size: u64) -> IoResult<()> {
        self.scheduler.truncate(self.id, size).await
    }
}

/// Open file entry
struct OpenFile<IO: AsyncIO> {
    io: Arc<IO>,
}

/// Legacy stub - FileHandle operations return NotSupported errors
struct IoSchedulerInner;

impl IoSchedulerInner {
    async fn read(
        &self,
        _file_id: u64,
        _offset: u64,
        _size: usize,
        _priority: IoPriority,
    ) -> IoResult<AlignedBuffer> {
        Err(IoError::NotSupported(
            "Legacy FileHandle: use IoScheduler as AsyncIOFactory".into(),
        ))
    }

    async fn write(
        &self,
        _file_id: u64,
        _offset: u64,
        _data: AlignedBuffer,
        _priority: IoPriority,
    ) -> IoResult<usize> {
        Err(IoError::NotSupported(
            "Legacy FileHandle: use IoScheduler as AsyncIOFactory".into(),
        ))
    }

    async fn sync(&self, _file_id: u64, _priority: IoPriority) -> IoResult<()> {
        Err(IoError::NotSupported(
            "Legacy FileHandle: use IoScheduler as AsyncIOFactory".into(),
        ))
    }

    async fn file_size(&self, _file_id: u64) -> IoResult<u64> {
        Err(IoError::NotSupported(
            "Legacy FileHandle: use IoScheduler as AsyncIOFactory".into(),
        ))
    }

    async fn truncate(&self, _file_id: u64, _size: u64) -> IoResult<()> {
        Err(IoError::NotSupported(
            "Legacy FileHandle: use IoScheduler as AsyncIOFactory".into(),
        ))
    }
}

/// I/O scheduler with typed factory
///
/// Manages I/O requests with priority scheduling, backpressure control,
/// and metrics collection.
pub struct IoScheduler<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    /// I/O factory for opening files
    factory: Arc<F>,

    /// Open files registry
    files: RwLock<HashMap<u64, OpenFile<IO>>>,

    /// Backpressure controller
    backpressure: Arc<BackpressureController>,

    /// Metrics
    metrics: Arc<IoMetrics>,

    /// Configuration
    config: SchedulerConfig,

    /// Next file ID
    next_file_id: AtomicU64,

    /// Shutdown flag
    shutdown: AtomicBool,

    /// Notify when work is available
    work_notify: Notify,
}

impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> IoScheduler<IO, F> {
    /// Create a new I/O scheduler
    pub fn new(factory: Arc<F>, config: SchedulerConfig) -> Arc<Self> {
        let metrics = Arc::new(IoMetrics::new());
        let backpressure = Arc::new(BackpressureController::with_metrics(Arc::clone(&metrics)));

        Arc::new(Self {
            factory,
            files: RwLock::new(HashMap::new()),
            backpressure,
            metrics,
            config,
            next_file_id: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            work_notify: Notify::new(),
        })
    }

    /// Create with default configuration
    pub fn with_factory(factory: Arc<F>) -> Arc<Self> {
        Self::new(factory, SchedulerConfig::default())
    }

    /// Open a file
    pub async fn open(self: &Arc<Self>, path: &Path, create: bool) -> IoResult<FileHandle> {
        let io = self.factory.open(path, create).await?;
        let id = self.next_file_id.fetch_add(1, Ordering::Relaxed);

        self.files.write().insert(id, OpenFile { io: Arc::new(io) });

        // Create a simple inner reference for FileHandle (legacy stub)
        let inner = Arc::new(IoSchedulerInner);

        Ok(FileHandle {
            id,
            path: path.to_path_buf(),
            scheduler: inner,
        })
    }

    /// Close a file
    pub fn close(&self, file_id: u64) {
        self.files.write().remove(&file_id);
    }

    /// Read data with priority
    pub async fn read(
        &self,
        file_id: u64,
        offset: u64,
        size: usize,
        priority: IoPriority,
    ) -> IoResult<AlignedBuffer> {
        // Check backpressure for low-priority operations
        if priority >= IoPriority::Compaction {
            self.backpressure.wait_if_saturated().await;
        }

        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Clone the Arc before releasing the lock to avoid holding it across await
        let io = {
            let files = self.files.read();
            let file = files
                .get(&file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
            Arc::clone(&file.io)
        };

        let result = {
            let mut buf = AlignedBuffer::new(size)?;
            io.read_at(&mut buf, offset).await?;
            Ok(buf)
        };

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.metrics.record_completion(priority, latency, size);

        if let Ok(ref buf) = result {
            self.metrics.record_read(buf.len());
        }

        // Update backpressure state
        self.update_backpressure();

        result
    }

    /// Write data with priority
    pub async fn write(
        &self,
        file_id: u64,
        offset: u64,
        data: AlignedBuffer,
        priority: IoPriority,
    ) -> IoResult<usize> {
        // Check backpressure for low-priority operations
        if priority >= IoPriority::Flush && self.backpressure.should_slow_flush() {
            self.backpressure.wait_if_heavy().await;
        }
        if priority >= IoPriority::Compaction {
            self.backpressure.wait_if_saturated().await;
        }

        let start = Instant::now();
        let size = data.len();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Clone the Arc before releasing the lock to avoid holding it across await
        let io = {
            let files = self.files.read();
            let file = files
                .get(&file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
            Arc::clone(&file.io)
        };

        let result = io.write_at(&data, offset).await;

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.metrics.record_completion(priority, latency, size);

        if let Ok(bytes) = result {
            self.metrics.record_write(bytes);
        }

        // Update backpressure state
        self.update_backpressure();

        result
    }

    /// Sync a file with priority
    pub async fn sync(&self, file_id: u64, priority: IoPriority) -> IoResult<()> {
        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Clone the Arc before releasing the lock to avoid holding it across await
        let io = {
            let files = self.files.read();
            let file = files
                .get(&file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
            Arc::clone(&file.io)
        };

        let result = io.sync().await;

        let latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.metrics.record_completion(priority, latency, 0);

        // Update backpressure state
        self.update_backpressure();

        result
    }

    /// Get file size
    pub async fn file_size(&self, file_id: u64) -> IoResult<u64> {
        let io = {
            let files = self.files.read();
            let file = files
                .get(&file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
            Arc::clone(&file.io)
        };
        io.file_size().await
    }

    /// Truncate file
    pub async fn truncate(&self, file_id: u64, size: u64) -> IoResult<()> {
        let io = {
            let files = self.files.read();
            let file = files
                .get(&file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
            Arc::clone(&file.io)
        };
        io.truncate(size).await
    }

    /// Get the backpressure controller
    pub fn backpressure(&self) -> &BackpressureController {
        &self.backpressure
    }

    /// Get metrics
    pub fn metrics(&self) -> &IoMetrics {
        &self.metrics
    }

    /// Get a metrics snapshot
    pub fn metrics_snapshot(&self) -> super::metrics::IoMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Check if under backpressure
    pub fn is_under_pressure(&self) -> bool {
        self.backpressure.is_under_pressure()
    }

    /// Shutdown the scheduler
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.work_notify.notify_waiters();
    }

    /// Update backpressure state based on current metrics
    fn update_backpressure(&self) {
        let queue_depth = self.metrics.queue_depth.load(Ordering::Relaxed) as usize;
        let avg_latency = self.metrics.average_latency();
        self.backpressure
            .update(queue_depth, avg_latency, &self.config);
    }

    /// Open a file and return a ScheduledHandle
    ///
    /// This is the preferred way to open files - returns a handle that
    /// implements AsyncIO with full scheduling support.
    pub async fn open_scheduled(&self, path: &Path, create: bool) -> IoResult<ScheduledHandle<IO>> {
        let io = self.factory.open(path, create).await?;
        Ok(ScheduledHandle::new(
            io,
            Arc::clone(&self.metrics),
            Arc::clone(&self.backpressure),
            self.config.clone(),
        ))
    }
}

// ============================================================================
// ScheduledIOFactory - AsyncIOFactory wrapper with scheduling
// ============================================================================

/// A factory that wraps another factory with scheduling coordination.
///
/// This implements `AsyncIOFactory` and can be used as a drop-in replacement
/// for `UringIOFactory` or `PosixIOFactory`. All file handles returned will
/// have scheduling (backpressure, metrics) built in.
///
/// # Example
///
/// ```ignore
/// let base_factory = Arc::new(PosixIOFactory);
/// let scheduled_factory = ScheduledIOFactory::new(base_factory, SchedulerConfig::default());
///
/// // Use scheduled_factory with LsmEngine
/// let engine = LsmEngine::open(Arc::new(scheduled_factory), config).await?;
/// ```
pub struct ScheduledIOFactory<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    /// The underlying factory
    factory: Arc<F>,
    /// Shared metrics across all handles
    metrics: Arc<IoMetrics>,
    /// Shared backpressure controller
    backpressure: Arc<BackpressureController>,
    /// Configuration
    config: SchedulerConfig,
    /// Phantom for IO type
    _io: std::marker::PhantomData<IO>,
}

impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> ScheduledIOFactory<IO, F> {
    /// Create a new scheduled factory
    pub fn new(factory: Arc<F>, config: SchedulerConfig) -> Self {
        let metrics = Arc::new(IoMetrics::new());
        let backpressure = Arc::new(BackpressureController::with_metrics(Arc::clone(&metrics)));

        Self {
            factory,
            metrics,
            backpressure,
            config,
            _io: std::marker::PhantomData,
        }
    }

    /// Create with default configuration
    pub fn with_factory(factory: Arc<F>) -> Self {
        Self::new(factory, SchedulerConfig::default())
    }

    /// Get the backpressure controller
    pub fn backpressure(&self) -> &BackpressureController {
        &self.backpressure
    }

    /// Get metrics
    pub fn metrics(&self) -> &IoMetrics {
        &self.metrics
    }

    /// Get a metrics snapshot
    pub fn metrics_snapshot(&self) -> super::metrics::IoMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Check if under backpressure
    pub fn is_under_pressure(&self) -> bool {
        self.backpressure.is_under_pressure()
    }
}

#[async_trait]
impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> AsyncIOFactory
    for ScheduledIOFactory<IO, F>
{
    type IO = ScheduledHandle<IO>;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        let inner = self.factory.open(path, create).await?;
        Ok(ScheduledHandle::new(
            inner,
            Arc::clone(&self.metrics),
            Arc::clone(&self.backpressure),
            self.config.clone(),
        ))
    }
}

/// Batch I/O submitter for io_uring
///
/// Collects multiple I/O operations and submits them in a single syscall.
/// This is used internally by the scheduler on Linux for better throughput.
#[cfg(target_os = "linux")]
pub struct BatchSubmitter {
    /// Pending operations
    pending: parking_lot::Mutex<Vec<PendingOp>>,
    /// Maximum batch size
    max_batch: usize,
    /// Notify when batch should be submitted
    #[allow(dead_code)]
    submit_notify: Notify,
}

/// A pending I/O operation in the batch
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub struct PendingOp {
    /// The I/O operation
    pub op: super::IoOp,
    /// Priority class
    pub priority: IoPriority,
    /// When the operation was submitted
    pub submitted_at: Instant,
    /// Channel to send completion result
    pub completion_tx: tokio::sync::oneshot::Sender<IoResult<super::IoCompletion>>,
}

#[cfg(target_os = "linux")]
impl BatchSubmitter {
    /// Create a new batch submitter
    pub fn new(max_batch: usize) -> Self {
        Self {
            pending: parking_lot::Mutex::new(Vec::with_capacity(max_batch)),
            max_batch,
            submit_notify: Notify::new(),
        }
    }

    /// Add an operation to the batch
    ///
    /// Returns a receiver for the completion result.
    pub fn add(
        &self,
        op: super::IoOp,
        priority: IoPriority,
    ) -> tokio::sync::oneshot::Receiver<IoResult<super::IoCompletion>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let pending_op = PendingOp {
            op,
            priority,
            submitted_at: Instant::now(),
            completion_tx: tx,
        };

        let mut pending = self.pending.lock();
        pending.push(pending_op);

        if pending.len() >= self.max_batch {
            self.submit_notify.notify_one();
        }

        rx
    }

    /// Check if batch is ready to submit
    pub fn is_ready(&self) -> bool {
        self.pending.lock().len() >= self.max_batch
    }

    /// Get current batch size
    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.pending.lock().is_empty()
    }

    /// Take all pending operations
    pub fn take(&self) -> Vec<PendingOp> {
        std::mem::take(&mut *self.pending.lock())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::default_io_factory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_scheduler_basic_ops() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let scheduler = IoScheduler::new(factory, SchedulerConfig::default());

        let path = dir.path().join("test.dat");

        // Open file
        let handle = scheduler.open(&path, true).await.unwrap();

        // Write data
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"hello world").unwrap();
        let written = scheduler
            .write(handle.id(), 0, buf, IoPriority::Flush)
            .await
            .unwrap();
        assert!(written > 0);

        // Sync
        scheduler
            .sync(handle.id(), IoPriority::Flush)
            .await
            .unwrap();

        // Read back
        let read_buf = scheduler
            .read(handle.id(), 0, 4096, IoPriority::QueryRead)
            .await
            .unwrap();
        assert_eq!(&read_buf[..11], b"hello world");

        // Check metrics
        let snapshot = scheduler.metrics_snapshot();
        assert!(snapshot.requests_submitted >= 3);
        assert!(snapshot.requests_completed >= 3);
    }

    #[tokio::test]
    async fn test_backpressure_levels() {
        let factory = Arc::new(default_io_factory());
        let config = SchedulerConfig::default()
            .with_target_queue_depth(10)
            .with_high_watermark(20);
        let scheduler = IoScheduler::new(factory, config);

        // Initially no pressure
        assert!(!scheduler.is_under_pressure());

        // Manually trigger backpressure update with high queue depth
        scheduler.metrics.queue_depth.store(15, Ordering::Relaxed);
        scheduler.update_backpressure();
        assert!(scheduler.backpressure().should_slow_flush());

        // Reset
        scheduler.metrics.queue_depth.store(0, Ordering::Relaxed);
        scheduler.update_backpressure();
        assert!(!scheduler.is_under_pressure());
    }
}
