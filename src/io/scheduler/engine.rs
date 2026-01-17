//! I/O Engine with hybrid concurrency model
//!
//! Provides pipeline execution for foreground (low latency) and batch execution
//! for background (throughput) with hot/cold ring management.
//!
//! # Architecture
//!
//! ```text
//!                     IoEngine
//!                        │
//!          ┌─────────────┴─────────────┐
//!          │                           │
//!    Foreground Path            Background Path
//!    (immediate submit)         (batched submit)
//!          │                           │
//!    ┌─────┴─────┐                    │
//!    │           │              BackgroundBatcher
//!  hot rings   cold ring              │
//!    │           │              AdaptiveLimiter
//!    └─────┬─────┘                    │
//!          │                          │
//!          └──────────┬───────────────┘
//!                     │
//!              Completion Poller
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::{oneshot, Notify};

use super::batcher::{BackgroundBatcher, BatchResult};
use super::config::SchedulerConfig;
use super::limiter::AdaptiveLimiter;
use super::metrics::IoMetrics;
use super::throughput::ThroughputTracker;
use super::{IoContext, OpKind, Urgency};
use crate::io::aligned_buffer::AlignedBuffer;
use crate::io::error::{IoError, IoResult};
use crate::io::traits::{AsyncIO, AsyncIOFactory};

/// Maximum in-flight operations per ring
const MAX_IN_FLIGHT_PER_RING: usize = 64;

/// Default hot ring promotion threshold (access count in window)
const HOT_PROMOTION_THRESHOLD: usize = 3;

/// LRU clock hand interval
const LRU_CLOCK_INTERVAL_MS: u64 = 100;

// ============================================================================
// InFlightOp - Tracking pending operations
// ============================================================================

/// A pending I/O operation waiting for completion
struct InFlightOp {
    /// Completion channel
    tx: oneshot::Sender<IoResult<OpResult>>,
    /// When operation started (for latency tracking)
    #[allow(dead_code)]
    started_at: Instant,
    /// Operation size in bytes (for throughput tracking)
    #[allow(dead_code)]
    size: usize,
    /// Urgency level (for priority decisions)
    #[allow(dead_code)]
    urgency: Urgency,
    /// Operation kind (for metrics)
    #[allow(dead_code)]
    kind: OpKind,
}

/// Result of an I/O operation
#[derive(Debug)]
pub enum OpResult {
    /// Read completed with data
    Read(AlignedBuffer),
    /// Write completed with bytes written
    Write(usize),
    /// Sync completed
    Sync,
}

// ============================================================================
// FileRing - Per-file I/O state
// ============================================================================

/// Per-file I/O ring state
///
/// Wraps an AsyncIO handle with in-flight operation tracking.
/// For io_uring, this provides non-blocking submit with async completion.
pub struct FileRing<IO: AsyncIO> {
    /// The underlying I/O handle
    io: Arc<IO>,
    /// File path (for re-opening if needed)
    path: PathBuf,
    /// In-flight operations by op ID
    in_flight: HashMap<u64, InFlightOp>,
    /// Next operation ID
    next_op_id: AtomicU64,
    /// Current in-flight count
    in_flight_count: usize,
    /// Last access time (for LRU)
    last_access: Instant,
    /// Access count in current window (for hot promotion)
    access_count: usize,
}

impl<IO: AsyncIO> FileRing<IO> {
    /// Create a new file ring
    pub fn new(io: Arc<IO>, path: PathBuf) -> Self {
        Self {
            io,
            path,
            in_flight: HashMap::with_capacity(MAX_IN_FLIGHT_PER_RING),
            next_op_id: AtomicU64::new(1),
            in_flight_count: 0,
            last_access: Instant::now(),
            access_count: 0,
        }
    }

    /// Generate next operation ID
    fn next_op_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Check if ring has capacity for more operations
    pub fn has_capacity(&self) -> bool {
        self.in_flight_count < MAX_IN_FLIGHT_PER_RING
    }

    /// Get current in-flight count
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count
    }

    /// Touch for LRU tracking
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }

    /// Reset access count (called at window boundaries)
    pub fn reset_access_count(&mut self) {
        self.access_count = 0;
    }

    /// Check if file should be promoted to hot
    pub fn should_promote(&self) -> bool {
        self.access_count >= HOT_PROMOTION_THRESHOLD
    }

    /// Get last access time
    pub fn last_access(&self) -> Instant {
        self.last_access
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get underlying I/O handle
    pub fn io(&self) -> &Arc<IO> {
        &self.io
    }

    /// Submit a read operation
    ///
    /// Returns immediately with a receiver for the result.
    pub async fn submit_read(
        &mut self,
        offset: u64,
        size: usize,
        ctx: IoContext,
    ) -> IoResult<oneshot::Receiver<IoResult<OpResult>>> {
        self.touch();

        let (tx, rx) = oneshot::channel();
        let op_id = self.next_op_id();

        let in_flight = InFlightOp {
            tx,
            started_at: Instant::now(),
            size,
            urgency: ctx.urgency,
            kind: ctx.kind,
        };

        self.in_flight.insert(op_id, in_flight);
        self.in_flight_count += 1;

        // Execute the I/O operation
        let io = Arc::clone(&self.io);
        let mut buf = AlignedBuffer::new(size)?;

        // For now, execute synchronously and complete immediately
        // In Phase 4, we'll integrate with io_uring's non-blocking submit
        let result = io.read_at(&mut buf, offset).await;

        // Remove from in-flight
        if let Some(in_flight) = self.in_flight.remove(&op_id) {
            self.in_flight_count -= 1;
            let _ = in_flight.tx.send(result.map(|_| OpResult::Read(buf)));
        }

        Ok(rx)
    }

    /// Submit a write operation
    pub async fn submit_write(
        &mut self,
        offset: u64,
        data: AlignedBuffer,
        ctx: IoContext,
    ) -> IoResult<oneshot::Receiver<IoResult<OpResult>>> {
        self.touch();

        let (tx, rx) = oneshot::channel();
        let op_id = self.next_op_id();
        let size = data.len();

        let in_flight = InFlightOp {
            tx,
            started_at: Instant::now(),
            size,
            urgency: ctx.urgency,
            kind: ctx.kind,
        };

        self.in_flight.insert(op_id, in_flight);
        self.in_flight_count += 1;

        // Execute the I/O operation
        let io = Arc::clone(&self.io);
        let result = io.write_at(&data, offset).await;

        // Remove from in-flight
        if let Some(in_flight) = self.in_flight.remove(&op_id) {
            self.in_flight_count -= 1;
            let _ = in_flight.tx.send(result.map(OpResult::Write));
        }

        Ok(rx)
    }

    /// Submit a sync operation
    pub async fn submit_sync(
        &mut self,
        ctx: IoContext,
    ) -> IoResult<oneshot::Receiver<IoResult<OpResult>>> {
        self.touch();

        let (tx, rx) = oneshot::channel();
        let op_id = self.next_op_id();

        let in_flight = InFlightOp {
            tx,
            started_at: Instant::now(),
            size: 0,
            urgency: ctx.urgency,
            kind: ctx.kind,
        };

        self.in_flight.insert(op_id, in_flight);
        self.in_flight_count += 1;

        // Execute the I/O operation
        let io = Arc::clone(&self.io);
        let result = io.sync().await;

        // Remove from in-flight
        if let Some(in_flight) = self.in_flight.remove(&op_id) {
            self.in_flight_count -= 1;
            let _ = in_flight.tx.send(result.map(|_| OpResult::Sync));
        }

        Ok(rx)
    }
}

// ============================================================================
// RingManager - Hot/Cold ring management
// ============================================================================

/// Manages hot and cold rings with LRU eviction
pub struct RingManager<IO: AsyncIO> {
    /// Hot rings (dedicated per file) - keyed by file_id
    hot_rings: HashMap<u64, FileRing<IO>>,
    /// Cold ring (shared) - keyed by file_id
    cold_rings: HashMap<u64, FileRing<IO>>,
    /// File ID to path mapping
    file_paths: HashMap<u64, PathBuf>,
    /// Maximum hot rings
    max_hot_rings: usize,
    /// LRU clock for eviction
    lru_clock: Instant,
}

impl<IO: AsyncIO> RingManager<IO> {
    /// Create a new ring manager
    pub fn new(max_hot_rings: usize) -> Self {
        Self {
            hot_rings: HashMap::with_capacity(max_hot_rings),
            cold_rings: HashMap::new(),
            file_paths: HashMap::new(),
            max_hot_rings,
            lru_clock: Instant::now(),
        }
    }

    /// Get or create a ring for a file
    ///
    /// For foreground operations, tries to use/create a hot ring.
    /// For background operations, uses cold ring.
    pub fn get_ring(&mut self, file_id: u64, urgency: Urgency) -> Option<&mut FileRing<IO>> {
        match urgency {
            Urgency::Critical | Urgency::Foreground => {
                // Try hot ring first
                if self.hot_rings.contains_key(&file_id) {
                    return self.hot_rings.get_mut(&file_id);
                }
                // Fall back to cold ring if it exists
                self.cold_rings.get_mut(&file_id)
            }
            Urgency::Background => {
                // Background always uses cold ring
                self.cold_rings.get_mut(&file_id)
            }
        }
    }

    /// Register a file with its ring
    pub fn register(&mut self, file_id: u64, ring: FileRing<IO>, urgency: Urgency) {
        let path = ring.path().to_path_buf();
        self.file_paths.insert(file_id, path);

        match urgency {
            Urgency::Critical | Urgency::Foreground => {
                // Try to add as hot ring
                if self.hot_rings.len() < self.max_hot_rings {
                    self.hot_rings.insert(file_id, ring);
                } else {
                    // Evict LRU hot ring to cold, then add new hot
                    self.evict_lru_hot();
                    self.hot_rings.insert(file_id, ring);
                }
            }
            Urgency::Background => {
                self.cold_rings.insert(file_id, ring);
            }
        }
    }

    /// Promote a file from cold to hot
    pub fn promote_to_hot(&mut self, file_id: u64) -> bool {
        if self.hot_rings.contains_key(&file_id) {
            return true; // Already hot
        }

        if let Some(ring) = self.cold_rings.remove(&file_id) {
            if self.hot_rings.len() >= self.max_hot_rings {
                self.evict_lru_hot();
            }
            self.hot_rings.insert(file_id, ring);
            true
        } else {
            false
        }
    }

    /// Demote a file from hot to cold
    pub fn demote_to_cold(&mut self, file_id: u64) -> bool {
        if let Some(ring) = self.hot_rings.remove(&file_id) {
            self.cold_rings.insert(file_id, ring);
            true
        } else {
            false
        }
    }

    /// Evict the least recently used hot ring to cold
    fn evict_lru_hot(&mut self) {
        if self.hot_rings.is_empty() {
            return;
        }

        // Find LRU hot ring
        let lru_id = self
            .hot_rings
            .iter()
            .min_by_key(|(_, ring)| ring.last_access())
            .map(|(id, _)| *id);

        if let Some(file_id) = lru_id {
            self.demote_to_cold(file_id);
        }
    }

    /// Run LRU clock tick - resets access counts and potentially evicts
    pub fn clock_tick(&mut self) {
        if self.lru_clock.elapsed() < Duration::from_millis(LRU_CLOCK_INTERVAL_MS) {
            return;
        }
        self.lru_clock = Instant::now();

        // Check for cold rings that should be promoted
        let promote_ids: Vec<u64> = self
            .cold_rings
            .iter()
            .filter(|(_, ring)| ring.should_promote())
            .map(|(id, _)| *id)
            .collect();

        for file_id in promote_ids {
            self.promote_to_hot(file_id);
        }

        // Reset access counts
        for ring in self.hot_rings.values_mut() {
            ring.reset_access_count();
        }
        for ring in self.cold_rings.values_mut() {
            ring.reset_access_count();
        }
    }

    /// Remove a file
    pub fn remove(&mut self, file_id: u64) {
        self.hot_rings.remove(&file_id);
        self.cold_rings.remove(&file_id);
        self.file_paths.remove(&file_id);
    }

    /// Get statistics
    pub fn stats(&self) -> RingManagerStats {
        RingManagerStats {
            hot_ring_count: self.hot_rings.len(),
            cold_ring_count: self.cold_rings.len(),
            max_hot_rings: self.max_hot_rings,
        }
    }

    /// Check if file is registered
    pub fn contains(&self, file_id: u64) -> bool {
        self.hot_rings.contains_key(&file_id) || self.cold_rings.contains_key(&file_id)
    }

    /// Check if file is hot
    pub fn is_hot(&self, file_id: u64) -> bool {
        self.hot_rings.contains_key(&file_id)
    }

    /// Get I/O handle for a file (searching both hot and cold rings)
    pub fn get_io(&self, file_id: u64) -> Option<Arc<IO>> {
        if let Some(ring) = self.hot_rings.get(&file_id) {
            Some(Arc::clone(ring.io()))
        } else if let Some(ring) = self.cold_rings.get(&file_id) {
            Some(Arc::clone(ring.io()))
        } else {
            None
        }
    }
}

/// Ring manager statistics
#[derive(Debug, Clone)]
pub struct RingManagerStats {
    pub hot_ring_count: usize,
    pub cold_ring_count: usize,
    pub max_hot_rings: usize,
}

// ============================================================================
// IoEngine - Central coordinator
// ============================================================================

/// Central I/O engine with hybrid concurrency model
///
/// Coordinates foreground (pipeline) and background (batch) I/O paths,
/// manages hot/cold rings, and integrates adaptive rate limiting.
pub struct IoEngine<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    /// Factory for creating I/O handles
    factory: Arc<F>,
    /// Ring manager for hot/cold ring allocation
    rings: RwLock<RingManager<IO>>,
    /// Background operation batcher
    batcher: BackgroundBatcher,
    /// Throughput tracker for adaptive rate limiting
    throughput: ThroughputTracker,
    /// Adaptive rate limiter for background I/O
    limiter: AdaptiveLimiter,
    /// I/O metrics
    metrics: Arc<IoMetrics>,
    /// Configuration (stored for future use in deadline calculation)
    #[allow(dead_code)]
    config: SchedulerConfig,
    /// Next file ID
    next_file_id: AtomicU64,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Work available notification
    work_notify: Notify,
}

impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> IoEngine<IO, F> {
    /// Create a new I/O engine
    pub fn new(factory: Arc<F>, config: SchedulerConfig) -> Arc<Self> {
        let metrics = Arc::new(IoMetrics::new());
        let throughput = ThroughputTracker::with_config(
            config.throughput_window,
            0.2, // EMA alpha
        );
        let limiter = AdaptiveLimiter::new(&config);
        let batcher = BackgroundBatcher::new(&config);

        Arc::new(Self {
            factory,
            rings: RwLock::new(RingManager::new(config.max_hot_rings)),
            batcher,
            throughput,
            limiter,
            metrics,
            config,
            next_file_id: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            work_notify: Notify::new(),
        })
    }

    /// Open a file and register with the engine
    pub async fn open(&self, path: &Path, create: bool, ctx: IoContext) -> IoResult<u64> {
        let io = self.factory.open(path, create).await?;
        let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);

        let ring = FileRing::new(Arc::new(io), path.to_path_buf());

        {
            let mut rings = self.rings.write();
            rings.register(file_id, ring, ctx.urgency);
        }

        Ok(file_id)
    }

    /// Close a file
    pub fn close(&self, file_id: u64) {
        self.rings.write().remove(file_id);
    }

    /// Submit a foreground read (immediate execution)
    pub async fn read_foreground(
        &self,
        file_id: u64,
        offset: u64,
        size: usize,
        ctx: IoContext,
    ) -> IoResult<AlignedBuffer> {
        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Get the I/O handle (release lock before await)
        let io = {
            let mut rings = self.rings.write();
            rings.clock_tick();
            rings
                .get_io(file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?
        };

        // Execute I/O outside the lock
        let mut buf = AlignedBuffer::new(size)?;
        let result = io.read_at(&mut buf, offset).await;

        let _latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);

        // Record metrics
        self.throughput.record(ctx.urgency, size);

        match result {
            Ok(_) => {
                self.metrics.record_read(buf.len());
                Ok(buf)
            }
            Err(e) => Err(e),
        }
    }

    /// Submit a foreground write (immediate execution)
    pub async fn write_foreground(
        &self,
        file_id: u64,
        offset: u64,
        data: AlignedBuffer,
        ctx: IoContext,
    ) -> IoResult<usize> {
        let start = Instant::now();
        let size = data.len();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Get the I/O handle (release lock before await)
        let io = {
            let mut rings = self.rings.write();
            rings.clock_tick();
            rings
                .get_io(file_id)
                .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?
        };

        // Execute I/O outside the lock
        let result = io.write_at(&data, offset).await;

        let _latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);

        // Record metrics
        self.throughput.record(ctx.urgency, size);

        match result {
            Ok(bytes) => {
                self.metrics.record_write(bytes);
                Ok(bytes)
            }
            Err(e) => Err(e),
        }
    }

    /// Submit a foreground sync (immediate execution)
    pub async fn sync_foreground(&self, file_id: u64, _ctx: IoContext) -> IoResult<()> {
        let start = Instant::now();
        self.metrics
            .requests_submitted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);

        // Get the I/O handle (release lock before await)
        let io = self
            .rings
            .read()
            .get_io(file_id)
            .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;

        // Execute I/O outside the lock
        let result = io.sync().await;

        let _latency = start.elapsed();
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);

        result
    }

    /// Submit a background read (batched execution)
    ///
    /// The operation is queued and executed when the batch is ready.
    pub fn submit_background_read(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        size: usize,
        ctx: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        self.batcher
            .submit_read(file_id, file_path, offset, size, ctx)
    }

    /// Submit a background write (batched execution)
    pub fn submit_background_write(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        data: AlignedBuffer,
        ctx: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        self.batcher
            .submit_write(file_id, file_path, offset, data, ctx)
    }

    /// Submit a background sync (batched execution)
    pub fn submit_background_sync(
        &self,
        file_id: u64,
        file_path: PathBuf,
        ctx: IoContext,
    ) -> Option<oneshot::Receiver<BatchResult>> {
        self.batcher.submit_sync(file_id, file_path, ctx)
    }

    /// Smart I/O dispatch based on context urgency
    ///
    /// Critical/Foreground: immediate execution
    /// Background: batched execution with rate limiting
    pub async fn read(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        size: usize,
        ctx: IoContext,
    ) -> IoResult<AlignedBuffer> {
        match ctx.urgency {
            Urgency::Critical | Urgency::Foreground => {
                self.read_foreground(file_id, offset, size, ctx).await
            }
            Urgency::Background => {
                // Check rate limiter
                self.limiter.acquire(size as u64).await;

                // Submit to batcher
                let rx = self
                    .submit_background_read(file_id, file_path, offset, size, ctx)
                    .ok_or_else(|| IoError::Io(std::io::Error::other("Batcher shutdown")))?;

                // Wait for result
                match rx.await {
                    Ok(BatchResult::Read(Ok(buf))) => Ok(buf),
                    Ok(BatchResult::Read(Err(e))) => Err(IoError::Io(std::io::Error::other(e))),
                    Ok(_) => Err(IoError::Io(std::io::Error::other("Unexpected result"))),
                    Err(_) => Err(IoError::Io(std::io::Error::other("Operation cancelled"))),
                }
            }
        }
    }

    /// Smart write dispatch based on context urgency
    pub async fn write(
        &self,
        file_id: u64,
        file_path: PathBuf,
        offset: u64,
        data: AlignedBuffer,
        ctx: IoContext,
    ) -> IoResult<usize> {
        match ctx.urgency {
            Urgency::Critical | Urgency::Foreground => {
                self.write_foreground(file_id, offset, data, ctx).await
            }
            Urgency::Background => {
                let size = data.len();

                // Check rate limiter
                self.limiter.acquire(size as u64).await;

                // Submit to batcher
                let rx = self
                    .submit_background_write(file_id, file_path, offset, data, ctx)
                    .ok_or_else(|| IoError::Io(std::io::Error::other("Batcher shutdown")))?;

                // Wait for result
                match rx.await {
                    Ok(BatchResult::Write(Ok(bytes))) => Ok(bytes),
                    Ok(BatchResult::Write(Err(e))) => Err(IoError::Io(std::io::Error::other(e))),
                    Ok(_) => Err(IoError::Io(std::io::Error::other("Unexpected result"))),
                    Err(_) => Err(IoError::Io(std::io::Error::other("Operation cancelled"))),
                }
            }
        }
    }

    /// Smart sync dispatch based on context urgency
    pub async fn sync(&self, file_id: u64, file_path: PathBuf, ctx: IoContext) -> IoResult<()> {
        match ctx.urgency {
            Urgency::Critical | Urgency::Foreground => self.sync_foreground(file_id, ctx).await,
            Urgency::Background => {
                // Submit to batcher
                let rx = self
                    .submit_background_sync(file_id, file_path, ctx)
                    .ok_or_else(|| IoError::Io(std::io::Error::other("Batcher shutdown")))?;

                // Wait for result
                match rx.await {
                    Ok(BatchResult::Sync(Ok(()))) => Ok(()),
                    Ok(BatchResult::Sync(Err(e))) => Err(IoError::Io(std::io::Error::other(e))),
                    Ok(_) => Err(IoError::Io(std::io::Error::other("Unexpected result"))),
                    Err(_) => Err(IoError::Io(std::io::Error::other("Operation cancelled"))),
                }
            }
        }
    }

    /// Update rate limiter allocation (call periodically)
    pub fn update_allocation(&self) {
        if self.throughput.tick() {
            self.limiter.update_allocation(&self.throughput);
        }
    }

    /// Learn disk capacity from observed latency
    pub fn learn_capacity(&self, p99_latency: Duration) {
        self.limiter.learn_capacity(p99_latency);
    }

    /// Get metrics
    pub fn metrics(&self) -> &IoMetrics {
        &self.metrics
    }

    /// Get throughput tracker
    pub fn throughput(&self) -> &ThroughputTracker {
        &self.throughput
    }

    /// Get rate limiter
    pub fn limiter(&self) -> &AdaptiveLimiter {
        &self.limiter
    }

    /// Get batcher
    pub fn batcher(&self) -> &BackgroundBatcher {
        &self.batcher
    }

    /// Get ring manager stats
    pub fn ring_stats(&self) -> RingManagerStats {
        self.rings.read().stats()
    }

    /// Check if file is hot
    pub fn is_hot(&self, file_id: u64) -> bool {
        self.rings.read().is_hot(file_id)
    }

    /// Process a single batch of background operations
    ///
    /// Takes pending operations from the batcher, groups them by file,
    /// and executes them using the underlying I/O handles.
    pub async fn process_batch(&self) {
        let batch = self.batcher.take_batch_by_file();
        if batch.is_empty() {
            return;
        }

        // Process each file's operations
        for (file_id, ops) in batch {
            self.process_file_batch(file_id, ops).await;
        }

        // Update throughput tracking and rate limiting
        self.update_allocation();
    }

    /// Process a batch of operations for a single file
    async fn process_file_batch(&self, file_id: u64, ops: Vec<super::batcher::PendingOp>) {
        use super::batcher::BatchOp;

        // Get the I/O handle for this file
        let io_opt = self.rings.read().get_io(file_id);

        // If file not found, try to open it (for background ops that arrive
        // before explicit open, e.g., compaction on previously closed files)
        let io = match io_opt {
            Some(io) => io,
            None => {
                // File not registered - fail all ops
                for op in ops {
                    let result = match op.op {
                        BatchOp::Read { .. } => {
                            BatchResult::Read(Err("File not found".to_string()))
                        }
                        BatchOp::Write { .. } => {
                            BatchResult::Write(Err("File not found".to_string()))
                        }
                        BatchOp::Sync => BatchResult::Sync(Err("File not found".to_string())),
                    };
                    let _ = op.tx.send(result);
                }
                return;
            }
        };

        // Separate operations by type for batch submission
        let mut reads: Vec<(usize, u64, usize, oneshot::Sender<BatchResult>)> = Vec::new();
        let mut writes: Vec<(usize, u64, AlignedBuffer, oneshot::Sender<BatchResult>)> = Vec::new();
        let mut syncs: Vec<oneshot::Sender<BatchResult>> = Vec::new();

        for (idx, op) in ops.into_iter().enumerate() {
            match op.op {
                BatchOp::Read { offset, size } => {
                    reads.push((idx, offset, size, op.tx));
                }
                BatchOp::Write { offset, data } => {
                    writes.push((idx, offset, data, op.tx));
                }
                BatchOp::Sync => {
                    syncs.push(op.tx);
                }
            }
        }

        // Execute reads in batch if supported
        if !reads.is_empty() {
            self.execute_read_batch(&io, reads).await;
        }

        // Execute writes in batch if supported
        if !writes.is_empty() {
            self.execute_write_batch(&io, writes).await;
        }

        // Execute syncs (single sync covers all prior writes)
        if !syncs.is_empty() {
            let result = io.sync().await;
            for tx in syncs {
                let batch_result = match &result {
                    Ok(()) => BatchResult::Sync(Ok(())),
                    Err(e) => BatchResult::Sync(Err(format!("{}", e))),
                };
                let _ = tx.send(batch_result);
            }
        }
    }

    /// Execute a batch of read operations
    async fn execute_read_batch(
        &self,
        io: &Arc<IO>,
        reads: Vec<(usize, u64, usize, oneshot::Sender<BatchResult>)>,
    ) {
        if io.supports_batch() {
            // True batch submission
            let requests: Vec<(u64, usize)> =
                reads.iter().map(|(_, off, sz, _)| (*off, *sz)).collect();
            let results = io.read_batch(&requests).await;

            for ((_, _, size, tx), result) in reads.into_iter().zip(results.into_iter()) {
                self.throughput.record(Urgency::Background, size);
                let batch_result = match result {
                    Ok(buf) => {
                        self.metrics.record_read(buf.len());
                        BatchResult::Read(Ok(buf))
                    }
                    Err(e) => BatchResult::Read(Err(format!("{}", e))),
                };
                let _ = tx.send(batch_result);
            }
        } else {
            // Fall back to sequential execution
            for (_, offset, size, tx) in reads {
                let mut buf = match AlignedBuffer::new(size) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(BatchResult::Read(Err(format!("{}", e))));
                        continue;
                    }
                };

                let result = io.read_at(&mut buf, offset).await;
                self.throughput.record(Urgency::Background, size);

                let batch_result = match result {
                    Ok(_) => {
                        self.metrics.record_read(buf.len());
                        BatchResult::Read(Ok(buf))
                    }
                    Err(e) => BatchResult::Read(Err(format!("{}", e))),
                };
                let _ = tx.send(batch_result);
            }
        }
    }

    /// Execute a batch of write operations
    async fn execute_write_batch(
        &self,
        io: &Arc<IO>,
        writes: Vec<(usize, u64, AlignedBuffer, oneshot::Sender<BatchResult>)>,
    ) {
        if io.supports_batch() {
            // True batch submission - need to separate data from senders first
            let mut senders: Vec<(usize, oneshot::Sender<BatchResult>)> =
                Vec::with_capacity(writes.len());
            let mut requests: Vec<(u64, AlignedBuffer)> = Vec::with_capacity(writes.len());
            let mut sizes: Vec<usize> = Vec::with_capacity(writes.len());

            for (idx, offset, data, tx) in writes {
                sizes.push(data.len());
                requests.push((offset, data));
                senders.push((idx, tx));
            }

            let results = io.write_batch(&requests).await;

            for ((_idx, tx), (result, size)) in senders
                .into_iter()
                .zip(results.into_iter().zip(sizes.into_iter()))
            {
                self.throughput.record(Urgency::Background, size);
                let batch_result = match result {
                    Ok(bytes) => {
                        self.metrics.record_write(bytes);
                        BatchResult::Write(Ok(bytes))
                    }
                    Err(e) => BatchResult::Write(Err(format!("{}", e))),
                };
                let _ = tx.send(batch_result);
            }
        } else {
            // Fall back to sequential execution
            for (_, offset, data, tx) in writes {
                let size = data.len();
                let result = io.write_at(&data, offset).await;
                self.throughput.record(Urgency::Background, size);

                let batch_result = match result {
                    Ok(bytes) => {
                        self.metrics.record_write(bytes);
                        BatchResult::Write(Ok(bytes))
                    }
                    Err(e) => BatchResult::Write(Err(format!("{}", e))),
                };
                let _ = tx.send(batch_result);
            }
        }
    }

    /// Run the background batch processor loop
    ///
    /// This should be spawned as a tokio task when the engine is started.
    /// It continuously waits for batches and processes them.
    pub async fn run_batch_processor(self: Arc<Self>) {
        while !self.is_shutdown() {
            // Wait for batch to be ready
            self.batcher.wait_for_batch().await;

            if self.is_shutdown() {
                break;
            }

            // Process the batch
            self.process_batch().await;
        }

        // Process any remaining operations on shutdown
        self.process_batch().await;
    }

    /// Spawn the batch processor as a background task
    ///
    /// Returns a JoinHandle that can be used to wait for completion.
    pub fn spawn_batch_processor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.run_batch_processor().await;
        })
    }

    /// Shutdown the engine
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.batcher.shutdown();
        self.work_notify.notify_waiters();
    }

    /// Check if shutdown
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

// ============================================================================
// IoEngineHandle - AsyncIO wrapper for per-file operations
// ============================================================================

/// A file handle backed by the IoEngine
///
/// This implements `AsyncIO` and provides automatic context-based dispatch:
/// - Critical/Foreground operations use immediate pipeline execution
/// - Background operations use batched execution with rate limiting
pub struct IoEngineHandle<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    /// Reference to the engine
    engine: Arc<IoEngine<IO, F>>,
    /// File ID within the engine
    file_id: u64,
    /// File path (for background operations)
    path: PathBuf,
}

impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> IoEngineHandle<IO, F> {
    /// Create a new engine handle
    fn new(engine: Arc<IoEngine<IO, F>>, file_id: u64, path: PathBuf) -> Self {
        Self {
            engine,
            file_id,
            path,
        }
    }

    /// Get the file ID
    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read with explicit context
    pub async fn read_with_context(
        &self,
        buf: &mut AlignedBuffer,
        offset: u64,
        ctx: IoContext,
    ) -> IoResult<usize> {
        let result = self
            .engine
            .read(self.file_id, self.path.clone(), offset, buf.capacity(), ctx)
            .await?;
        let len = result.len();
        buf.as_mut_slice()[..len].copy_from_slice(&result[..len]);
        buf.set_len(len);
        Ok(len)
    }

    /// Write with explicit context
    pub async fn write_with_context(
        &self,
        buf: &AlignedBuffer,
        offset: u64,
        ctx: IoContext,
    ) -> IoResult<usize> {
        // Need to create a copy since we take ownership in the write path
        let mut data = AlignedBuffer::new(buf.len())?;
        data.as_mut_slice()[..buf.len()].copy_from_slice(buf.as_slice());
        data.set_len(buf.len());
        self.engine
            .write(self.file_id, self.path.clone(), offset, data, ctx)
            .await
    }

    /// Sync with explicit context
    pub async fn sync_with_context(&self, ctx: IoContext) -> IoResult<()> {
        self.engine.sync(self.file_id, self.path.clone(), ctx).await
    }
}

#[async_trait]
impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> AsyncIO
    for IoEngineHandle<IO, F>
{
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize> {
        // Default to foreground read
        self.read_with_context(buf, offset, IoContext::foreground_read())
            .await
    }

    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize> {
        // Default to background write (most common case for storage)
        self.write_with_context(buf, offset, IoContext::background_write())
            .await
    }

    async fn sync(&self) -> IoResult<()> {
        // Default to background sync
        self.sync_with_context(IoContext::background_sync()).await
    }

    async fn sync_with_context(&self, ctx: IoContext) -> IoResult<()> {
        IoEngineHandle::sync_with_context(self, ctx).await
    }

    async fn file_size(&self) -> IoResult<u64> {
        // Get from the underlying IO
        let io = self
            .engine
            .rings
            .read()
            .get_io(self.file_id)
            .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
        io.file_size().await
    }

    async fn truncate(&self, size: u64) -> IoResult<()> {
        let io = self
            .engine
            .rings
            .read()
            .get_io(self.file_id)
            .ok_or_else(|| IoError::Io(std::io::Error::other("File not found")))?;
        io.truncate(size).await
    }

    async fn read_at_priority(
        &self,
        buf: &mut AlignedBuffer,
        offset: u64,
        priority: super::IoPriority,
    ) -> IoResult<usize> {
        self.read_with_context(buf, offset, priority.to_context())
            .await
    }

    async fn write_at_priority(
        &self,
        buf: &AlignedBuffer,
        offset: u64,
        priority: super::IoPriority,
    ) -> IoResult<usize> {
        self.write_with_context(buf, offset, priority.to_context())
            .await
    }

    fn supports_batch(&self) -> bool {
        true
    }

    async fn read_batch(&self, requests: &[(u64, usize)]) -> Vec<IoResult<AlignedBuffer>> {
        // Use foreground context for batch reads (typically query-related)
        let mut results = Vec::with_capacity(requests.len());
        for &(offset, size) in requests {
            let result = self
                .engine
                .read(
                    self.file_id,
                    self.path.clone(),
                    offset,
                    size,
                    IoContext::foreground_read(),
                )
                .await;
            results.push(result);
        }
        results
    }

    async fn write_batch(&self, requests: &[(u64, AlignedBuffer)]) -> Vec<IoResult<usize>> {
        // Use background context for batch writes (typically flush/compaction)
        let mut results = Vec::with_capacity(requests.len());
        for (offset, buf) in requests {
            let mut data = match AlignedBuffer::new(buf.len()) {
                Ok(d) => d,
                Err(e) => {
                    results.push(Err(e));
                    continue;
                }
            };
            data.as_mut_slice()[..buf.len()].copy_from_slice(buf.as_slice());
            data.set_len(buf.len());

            let result = self
                .engine
                .write(
                    self.file_id,
                    self.path.clone(),
                    *offset,
                    data,
                    IoContext::background_write(),
                )
                .await;
            results.push(result);
        }
        results
    }
}

// ============================================================================
// IoEngineFactory - AsyncIOFactory implementation
// ============================================================================

/// Factory that creates IoEngineHandle instances
///
/// This can be used as a drop-in replacement for `ScheduledIOFactory`.
/// It provides full scheduling support with hybrid concurrency.
pub struct IoEngineFactory<IO: AsyncIO, F: AsyncIOFactory<IO = IO>> {
    /// The underlying engine
    engine: Arc<IoEngine<IO, F>>,
}

impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> IoEngineFactory<IO, F> {
    /// Create a new engine factory
    pub fn new(factory: Arc<F>, config: SchedulerConfig) -> Self {
        Self {
            engine: IoEngine::new(factory, config),
        }
    }

    /// Create with default configuration
    pub fn with_factory(factory: Arc<F>) -> Self {
        Self::new(factory, SchedulerConfig::default())
    }

    /// Get the underlying engine
    pub fn engine(&self) -> &Arc<IoEngine<IO, F>> {
        &self.engine
    }

    /// Start the background batch processor
    ///
    /// Call this after creating the factory to enable background batching.
    pub fn start_batch_processor(&self) -> tokio::task::JoinHandle<()> {
        self.engine.spawn_batch_processor()
    }

    /// Get metrics
    pub fn metrics(&self) -> &IoMetrics {
        self.engine.metrics()
    }

    /// Get throughput tracker
    pub fn throughput(&self) -> &ThroughputTracker {
        self.engine.throughput()
    }

    /// Get rate limiter
    pub fn limiter(&self) -> &AdaptiveLimiter {
        self.engine.limiter()
    }

    /// Shutdown the factory and engine
    pub fn shutdown(&self) {
        self.engine.shutdown();
    }
}

#[async_trait]
impl<IO: AsyncIO + 'static, F: AsyncIOFactory<IO = IO> + 'static> AsyncIOFactory
    for IoEngineFactory<IO, F>
{
    type IO = IoEngineHandle<IO, F>;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        let file_id = self
            .engine
            .open(path, create, IoContext::foreground_write())
            .await?;
        Ok(IoEngineHandle::new(
            Arc::clone(&self.engine),
            file_id,
            path.to_path_buf(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::default_io_factory;
    use tempfile::tempdir;

    fn make_config() -> SchedulerConfig {
        SchedulerConfig::default()
            .with_max_hot_rings(4)
            .with_background_batch_size(4)
    }

    #[tokio::test]
    async fn test_engine_open_close() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let engine = IoEngine::new(factory, make_config());

        let path = dir.path().join("test.dat");
        let ctx = IoContext::foreground_write();

        let file_id = engine.open(&path, true, ctx).await.unwrap();
        assert!(engine.rings.read().contains(file_id));

        engine.close(file_id);
        assert!(!engine.rings.read().contains(file_id));
    }

    #[tokio::test]
    async fn test_engine_foreground_write_read() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let engine = IoEngine::new(factory, make_config());

        let path = dir.path().join("test.dat");
        let ctx = IoContext::foreground_write();

        let file_id = engine.open(&path, true, ctx).await.unwrap();

        // Write
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"hello engine").unwrap();
        let written = engine
            .write_foreground(file_id, 0, buf, IoContext::foreground_write())
            .await
            .unwrap();
        assert!(written > 0);

        // Sync
        engine
            .sync_foreground(file_id, IoContext::foreground_write())
            .await
            .unwrap();

        // Read back
        let read_buf = engine
            .read_foreground(file_id, 0, 4096, IoContext::foreground_read())
            .await
            .unwrap();
        assert_eq!(&read_buf[..12], b"hello engine");
    }

    #[tokio::test]
    async fn test_ring_manager_hot_cold() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let config = SchedulerConfig::default().with_max_hot_rings(2);
        let engine = IoEngine::new(factory, config);

        // Open 3 files as foreground (should exceed max_hot_rings)
        let mut file_ids = Vec::new();
        for i in 0..3 {
            let path = dir.path().join(format!("test{}.dat", i));
            let file_id = engine
                .open(&path, true, IoContext::foreground_write())
                .await
                .unwrap();
            file_ids.push(file_id);
        }

        let stats = engine.ring_stats();
        // Should have 2 hot and 1 cold (due to LRU eviction)
        assert_eq!(stats.hot_ring_count, 2);
        assert_eq!(stats.cold_ring_count, 1);
    }

    #[tokio::test]
    async fn test_smart_dispatch() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let engine = IoEngine::new(factory, make_config());

        let path = dir.path().join("test.dat");
        let file_id = engine
            .open(&path, true, IoContext::foreground_write())
            .await
            .unwrap();

        // Foreground write (immediate)
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"smart dispatch test").unwrap();
        let written = engine
            .write(file_id, path.clone(), 0, buf, IoContext::foreground_write())
            .await
            .unwrap();
        assert!(written > 0);

        // Foreground read (immediate)
        let read_buf = engine
            .read(file_id, path.clone(), 0, 4096, IoContext::foreground_read())
            .await
            .unwrap();
        assert_eq!(&read_buf[..19], b"smart dispatch test");
    }

    #[test]
    fn test_ring_manager_promotion() {
        let manager: RingManager<crate::io::PosixIO> = RingManager::new(2);

        // Simulate cold ring with high access count
        // (Would need actual file for full test)
        assert_eq!(manager.stats().hot_ring_count, 0);
        assert_eq!(manager.stats().cold_ring_count, 0);
    }

    #[tokio::test]
    async fn test_batch_processor() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let config = SchedulerConfig::default()
            .with_max_hot_rings(4)
            .with_background_batch_size(2)
            .with_background_batch_deadline(std::time::Duration::from_millis(10));
        let engine = IoEngine::new(factory, config);

        let path = dir.path().join("batch_test.dat");
        let file_id = engine
            .open(&path, true, IoContext::background_write())
            .await
            .unwrap();

        // First, write some data directly so we can read it
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"batch processor test data").unwrap();
        engine
            .write_foreground(file_id, 0, buf, IoContext::foreground_write())
            .await
            .unwrap();
        engine
            .sync_foreground(file_id, IoContext::foreground_write())
            .await
            .unwrap();

        // Now submit background operations
        let rx1 = engine
            .submit_background_read(file_id, path.clone(), 0, 4096, IoContext::background_read())
            .unwrap();
        let rx2 = engine
            .submit_background_read(file_id, path.clone(), 0, 4096, IoContext::background_read())
            .unwrap();

        // Process the batch manually
        engine.process_batch().await;

        // Verify results
        match rx1.await.unwrap() {
            BatchResult::Read(Ok(data)) => {
                assert_eq!(&data[..25], b"batch processor test data");
            }
            other => panic!("Expected Read result, got {:?}", other),
        }
        match rx2.await.unwrap() {
            BatchResult::Read(Ok(data)) => {
                assert_eq!(&data[..25], b"batch processor test data");
            }
            other => panic!("Expected Read result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_background_write_batch() {
        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let config = SchedulerConfig::default()
            .with_max_hot_rings(4)
            .with_background_batch_size(2);
        let engine = IoEngine::new(factory, config);

        let path = dir.path().join("write_batch_test.dat");
        let file_id = engine
            .open(&path, true, IoContext::background_write())
            .await
            .unwrap();

        // Submit background write operations
        let mut buf1 = AlignedBuffer::page().unwrap();
        buf1.copy_from_slice(b"write batch test 1").unwrap();
        let rx1 = engine
            .submit_background_write(
                file_id,
                path.clone(),
                0,
                buf1,
                IoContext::background_write(),
            )
            .unwrap();

        let mut buf2 = AlignedBuffer::page().unwrap();
        buf2.copy_from_slice(b"write batch test 2").unwrap();
        let rx2 = engine
            .submit_background_write(
                file_id,
                path.clone(),
                4096,
                buf2,
                IoContext::background_write(),
            )
            .unwrap();

        // Process the batch
        engine.process_batch().await;

        // Verify write results
        match rx1.await.unwrap() {
            BatchResult::Write(Ok(bytes)) => {
                assert!(bytes > 0);
            }
            other => panic!("Expected Write result, got {:?}", other),
        }
        match rx2.await.unwrap() {
            BatchResult::Write(Ok(bytes)) => {
                assert!(bytes > 0);
            }
            other => panic!("Expected Write result, got {:?}", other),
        }

        // Sync and read back to verify
        engine
            .sync_foreground(file_id, IoContext::foreground_write())
            .await
            .unwrap();

        let read_buf = engine
            .read_foreground(file_id, 0, 4096, IoContext::foreground_read())
            .await
            .unwrap();
        assert_eq!(&read_buf[..18], b"write batch test 1");
    }

    #[tokio::test]
    async fn test_io_engine_factory() {
        use crate::io::traits::AsyncIOFactory;

        let dir = tempdir().unwrap();
        let factory = Arc::new(default_io_factory());
        let engine_factory = IoEngineFactory::new(factory, make_config());

        // Open a file via the factory
        let path = dir.path().join("factory_test.dat");
        let handle = engine_factory.open(&path, true).await.unwrap();

        // Write via the handle (uses background by default)
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"engine factory test").unwrap();

        // Use write_with_context for foreground write
        let written = handle
            .write_with_context(&buf, 0, IoContext::foreground_write())
            .await
            .unwrap();
        assert!(written > 0);

        // Sync
        handle
            .sync_with_context(IoContext::foreground_write())
            .await
            .unwrap();

        // Read back via the handle
        let mut read_buf = AlignedBuffer::page().unwrap();
        handle
            .read_with_context(&mut read_buf, 0, IoContext::foreground_read())
            .await
            .unwrap();
        assert_eq!(&read_buf[..19], b"engine factory test");

        // Verify metrics are tracked
        assert!(engine_factory.metrics().bytes_read.load(Ordering::Relaxed) > 0);
        assert!(
            engine_factory
                .metrics()
                .bytes_written
                .load(Ordering::Relaxed)
                > 0
        );

        // Shutdown
        engine_factory.shutdown();
    }
}
