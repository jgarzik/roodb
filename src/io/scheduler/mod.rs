//! I/O Scheduler for RooDB
//!
//! Provides priority-based I/O scheduling, batching, and backpressure control.
//!
//! # Features
//! - Two-dimensional scheduling: Urgency (Critical/Foreground/Background) × OpKind (Read/Write/Sync)
//! - Backpressure detection and signaling
//! - Batch submission for io_uring (up to 32 ops per syscall)
//! - Support for 4KB pages and 2MB large chunks
//! - Latency tracking and metrics
//!
//! # Urgency Levels
//! - **Critical**: Blocks COMMIT acknowledgment (Raft vote, transaction durability)
//! - **Foreground**: User-visible query latency (SLO-bound)
//! - **Background**: Flush, compaction - can defer under load

pub mod backpressure;
pub mod batch;
pub mod batcher;
pub mod config;
pub mod engine;
pub mod limiter;
pub mod metrics;
#[allow(clippy::module_inception)]
pub mod scheduler;
pub mod throughput;

pub use backpressure::{BackpressureController, BackpressureLevel};
pub use batch::PriorityQueues;
pub use batcher::{BackgroundBatcher, BatchOp, BatchResult, PendingOp};
pub use config::{SchedulerConfig, LARGE_CHUNK_SIZE};
pub use engine::{
    FileRing, IoEngine, IoEngineFactory, IoEngineHandle, OpResult, RingManager, RingManagerStats,
};
pub use limiter::AdaptiveLimiter;
pub use metrics::{IoMetrics, IoMetricsSnapshot, LatencyHistogram};
pub use scheduler::{FileHandle, IoScheduler, ScheduledHandle, ScheduledIOFactory};
pub use throughput::ThroughputTracker;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::aligned_buffer::AlignedBuffer;
use super::error::IoResult;

// ============================================================================
// New two-dimensional I/O context system
// ============================================================================

/// Urgency level - how soon must this I/O complete?
///
/// This is orthogonal to the operation type (read/write/sync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Urgency {
    /// Blocks COMMIT acknowledgment - user waiting for durability.
    /// Used for Raft vote persistence, transaction commit sync.
    /// Always processed immediately regardless of backpressure.
    Critical = 0,

    /// User query latency - SLO-bound.
    /// Used for foreground query reads and writes.
    /// Paused only under severe backpressure.
    Foreground = 1,

    /// Background work - can defer under load.
    /// Used for memtable flush, compaction.
    /// Paused when system is under any backpressure.
    Background = 2,
}

impl Urgency {
    /// Number of urgency levels
    pub const COUNT: usize = 3;

    /// Get all urgency levels in priority order
    pub const fn all() -> [Urgency; 3] {
        [Urgency::Critical, Urgency::Foreground, Urgency::Background]
    }
}

/// I/O operation kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// Read data from storage
    Read,
    /// Write data to storage
    Write,
    /// Sync/fsync - flush to durable storage.
    /// Also serves as a barrier for prior writes to the same file.
    Sync,
}

/// Full I/O context combining urgency and operation kind.
///
/// This replaces the single-dimensional IoPriority with a two-dimensional
/// model that separates "how urgent" from "what operation".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoContext {
    /// How urgent is this operation?
    pub urgency: Urgency,
    /// What kind of operation?
    pub kind: OpKind,
}

impl IoContext {
    /// Create a new IoContext
    pub const fn new(urgency: Urgency, kind: OpKind) -> Self {
        Self { urgency, kind }
    }

    /// Critical sync - blocks COMMIT acknowledgment (Raft vote, txn durability)
    pub const fn critical_sync() -> Self {
        Self {
            urgency: Urgency::Critical,
            kind: OpKind::Sync,
        }
    }

    /// Critical write - urgent data write
    pub const fn critical_write() -> Self {
        Self {
            urgency: Urgency::Critical,
            kind: OpKind::Write,
        }
    }

    /// Foreground read - user query (SLO-bound)
    pub const fn foreground_read() -> Self {
        Self {
            urgency: Urgency::Foreground,
            kind: OpKind::Read,
        }
    }

    /// Foreground write - user-facing write
    pub const fn foreground_write() -> Self {
        Self {
            urgency: Urgency::Foreground,
            kind: OpKind::Write,
        }
    }

    /// Background write - memtable flush, compaction writes
    pub const fn background_write() -> Self {
        Self {
            urgency: Urgency::Background,
            kind: OpKind::Write,
        }
    }

    /// Background read - compaction reads
    pub const fn background_read() -> Self {
        Self {
            urgency: Urgency::Background,
            kind: OpKind::Read,
        }
    }

    /// Background sync - SSTable finish, compaction finish
    pub const fn background_sync() -> Self {
        Self {
            urgency: Urgency::Background,
            kind: OpKind::Sync,
        }
    }

    /// Get deadline duration based on urgency and operation kind
    pub fn deadline_duration(&self, config: &SchedulerConfig) -> Option<Duration> {
        match (self.urgency, self.kind) {
            // Critical operations have tight deadlines
            (Urgency::Critical, OpKind::Sync) => Some(Duration::from_millis(10)),
            (Urgency::Critical, OpKind::Write) => Some(Duration::from_millis(5)),
            (Urgency::Critical, OpKind::Read) => Some(Duration::from_millis(5)),

            // Foreground operations use configured SLO
            (Urgency::Foreground, OpKind::Read) => Some(config.query_latency_slo),
            (Urgency::Foreground, OpKind::Write) => Some(Duration::from_millis(100)),
            (Urgency::Foreground, OpKind::Sync) => Some(Duration::from_millis(200)),

            // Background operations have no deadline (best effort)
            (Urgency::Background, _) => None,
        }
    }
}

// ============================================================================
// Legacy IoPriority (kept for backward compatibility during transition)
// ============================================================================

/// I/O priority classes (lower value = higher priority)
///
/// **Deprecated**: Use `IoContext` instead for new code. This enum conflates
/// urgency with operation type. Kept for backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum IoPriority {
    /// WAL/Raft writes - highest priority for durability
    Wal = 0,
    /// Foreground query reads - user-facing latency sensitive
    QueryRead = 1,
    /// Memtable flush writes - background but needed for writes
    Flush = 2,
    /// Compaction I/O - lowest priority, yields to foreground
    Compaction = 3,
}

impl IoPriority {
    /// Number of priority levels
    pub const COUNT: usize = 4;

    /// Get all priority levels in order
    pub const fn all() -> [IoPriority; 4] {
        [
            IoPriority::Wal,
            IoPriority::QueryRead,
            IoPriority::Flush,
            IoPriority::Compaction,
        ]
    }

    /// Get the deadline duration for this priority
    pub fn deadline_duration(&self, config: &SchedulerConfig) -> Option<Duration> {
        // Delegate to IoContext conversion
        self.to_context().deadline_duration(config)
    }

    /// Convert legacy IoPriority to new IoContext
    ///
    /// This provides a reasonable default mapping, but callers should
    /// migrate to using IoContext directly for more precise control.
    pub const fn to_context(&self) -> IoContext {
        match self {
            IoPriority::Wal => IoContext::critical_write(),
            IoPriority::QueryRead => IoContext::foreground_read(),
            IoPriority::Flush => IoContext::background_write(),
            IoPriority::Compaction => IoContext::background_write(),
        }
    }
}

impl From<IoPriority> for IoContext {
    fn from(priority: IoPriority) -> Self {
        priority.to_context()
    }
}

/// Chunk size for I/O operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSize {
    /// 4KB page - SSTable blocks, WAL records, index
    Page,
    /// 2MB large chunk - memtable flush, compaction
    Large,
    /// Custom size (will be rounded to PAGE_SIZE)
    Custom(usize),
}

impl ChunkSize {
    /// Get the size in bytes
    pub fn bytes(&self) -> usize {
        match self {
            ChunkSize::Page => super::traits::PAGE_SIZE,
            ChunkSize::Large => LARGE_CHUNK_SIZE,
            ChunkSize::Custom(n) => AlignedBuffer::round_up(*n),
        }
    }

    /// Check if this is a large chunk
    pub fn is_large(&self) -> bool {
        matches!(self, ChunkSize::Large)
            || matches!(self, ChunkSize::Custom(n) if *n >= LARGE_CHUNK_SIZE)
    }
}

/// I/O operation type
#[derive(Debug)]
pub enum IoOp {
    /// Read data at offset
    Read {
        file_id: u64,
        offset: u64,
        size: usize,
        /// Buffer to read into (provided by scheduler)
        buffer: Option<AlignedBuffer>,
    },
    /// Write data at offset
    Write {
        file_id: u64,
        offset: u64,
        data: AlignedBuffer,
    },
    /// Sync file to disk
    Sync { file_id: u64 },
}

impl IoOp {
    /// Get the file ID for this operation
    pub fn file_id(&self) -> u64 {
        match self {
            IoOp::Read { file_id, .. } => *file_id,
            IoOp::Write { file_id, .. } => *file_id,
            IoOp::Sync { file_id } => *file_id,
        }
    }

    /// Get the estimated size of this operation
    pub fn size(&self) -> usize {
        match self {
            IoOp::Read { size, .. } => *size,
            IoOp::Write { data, .. } => data.len(),
            IoOp::Sync { .. } => 0,
        }
    }
}

/// A scheduled I/O request
pub struct IoRequest {
    /// Unique request ID
    pub id: u64,
    /// Priority class
    pub priority: IoPriority,
    /// The I/O operation
    pub op: IoOp,
    /// Deadline for this request (None = no deadline)
    pub deadline: Option<Instant>,
    /// When the request was submitted
    pub submitted_at: Instant,
    /// Channel to send completion result
    pub completion_tx: oneshot::Sender<IoResult<IoCompletion>>,
}

impl IoRequest {
    /// Check if this request has expired its deadline
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }

    /// Time until deadline (None if no deadline or already expired)
    pub fn time_to_deadline(&self) -> Option<Duration> {
        self.deadline
            .and_then(|d| d.checked_duration_since(Instant::now()))
    }
}

/// Completion result for an I/O operation
#[derive(Debug)]
pub struct IoCompletion {
    /// Bytes read or written
    pub bytes: usize,
    /// Latency of the operation
    pub latency: Duration,
    /// Read data (for read operations)
    pub data: Option<AlignedBuffer>,
}

/// Atomic counter for generating unique request IDs
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID
pub fn next_request_id() -> u64 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}
