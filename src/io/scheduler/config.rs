//! I/O scheduler configuration

use std::time::Duration;

/// Large chunk size (2MB) for bulk I/O operations
pub const LARGE_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Default maximum batch size for io_uring submission
pub const DEFAULT_MAX_BATCH_SIZE: usize = 32;

/// Default target queue depth before light backpressure
pub const DEFAULT_TARGET_QUEUE_DEPTH: usize = 48;

/// Default high watermark for severe backpressure
pub const DEFAULT_HIGH_WATERMARK: usize = 56;

/// Default query latency SLO (P99)
pub const DEFAULT_QUERY_LATENCY_SLO_MS: u64 = 10;

/// Default background batch deadline (ms)
pub const DEFAULT_BACKGROUND_BATCH_DEADLINE_MS: u64 = 1;

/// Default minimum background bandwidth fraction when foreground is active
pub const DEFAULT_MIN_BACKGROUND_FRACTION: f64 = 0.2;

/// Default foreground activity threshold (1 MB/s)
pub const DEFAULT_FOREGROUND_THRESHOLD_BYTES: u64 = 1_000_000;

/// Default initial disk capacity estimate (500 MB/s)
pub const DEFAULT_INITIAL_DISK_CAPACITY_BYTES: u64 = 500_000_000;

/// Default maximum hot rings for foreground I/O
pub const DEFAULT_MAX_HOT_RINGS: usize = 256;

/// Default throughput measurement window (ms)
pub const DEFAULT_THROUGHPUT_WINDOW_MS: u64 = 100;

/// Configuration for the I/O scheduler
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum operations to batch in single io_uring submit
    pub max_batch_size: usize,

    /// Target queue depth before backpressure kicks in
    pub target_queue_depth: usize,

    /// High watermark for severe backpressure
    pub high_watermark: usize,

    /// Latency SLO for queries (P99)
    pub query_latency_slo: Duration,

    /// Enable 2MB large chunks for bulk I/O
    pub enable_large_chunks: bool,

    /// Compaction I/O bandwidth limit (bytes/sec, 0 = unlimited)
    /// Note: Now superseded by adaptive limiter, kept for compatibility
    pub compaction_bandwidth_limit: u64,

    // --- New: Batching ---
    /// Maximum background ops before forced batch submit
    pub background_batch_size: usize,

    /// Maximum wait time before submitting background batch
    pub background_batch_deadline: Duration,

    // --- New: Adaptive Rate Limiting ---
    /// Minimum bandwidth fraction for background when foreground is active
    pub min_background_fraction: f64,

    /// Foreground throughput threshold to consider "active" (bytes/sec)
    pub foreground_threshold: u64,

    /// Initial disk capacity estimate (bytes/sec) - learned over time
    pub initial_disk_capacity: u64,

    /// Throughput measurement window
    pub throughput_window: Duration,

    // --- New: Ring Management ---
    /// Maximum dedicated rings for hot files (foreground I/O)
    pub max_hot_rings: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            target_queue_depth: DEFAULT_TARGET_QUEUE_DEPTH,
            high_watermark: DEFAULT_HIGH_WATERMARK,
            query_latency_slo: Duration::from_millis(DEFAULT_QUERY_LATENCY_SLO_MS),
            enable_large_chunks: true,
            compaction_bandwidth_limit: 0, // unlimited (legacy)

            // Batching
            background_batch_size: DEFAULT_MAX_BATCH_SIZE,
            background_batch_deadline: Duration::from_millis(DEFAULT_BACKGROUND_BATCH_DEADLINE_MS),

            // Adaptive rate limiting
            min_background_fraction: DEFAULT_MIN_BACKGROUND_FRACTION,
            foreground_threshold: DEFAULT_FOREGROUND_THRESHOLD_BYTES,
            initial_disk_capacity: DEFAULT_INITIAL_DISK_CAPACITY_BYTES,
            throughput_window: Duration::from_millis(DEFAULT_THROUGHPUT_WINDOW_MS),

            // Ring management
            max_hot_rings: DEFAULT_MAX_HOT_RINGS,
        }
    }
}

impl SchedulerConfig {
    /// Create a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size.min(64); // io_uring queue depth limit
        self
    }

    /// Set the target queue depth
    pub fn with_target_queue_depth(mut self, depth: usize) -> Self {
        self.target_queue_depth = depth;
        self
    }

    /// Set the high watermark
    pub fn with_high_watermark(mut self, watermark: usize) -> Self {
        self.high_watermark = watermark;
        self
    }

    /// Set the query latency SLO
    pub fn with_query_latency_slo(mut self, slo: Duration) -> Self {
        self.query_latency_slo = slo;
        self
    }

    /// Enable or disable large chunks
    pub fn with_large_chunks(mut self, enabled: bool) -> Self {
        self.enable_large_chunks = enabled;
        self
    }

    /// Set the compaction bandwidth limit (legacy)
    pub fn with_compaction_bandwidth_limit(mut self, limit: u64) -> Self {
        self.compaction_bandwidth_limit = limit;
        self
    }

    /// Set background batch size
    pub fn with_background_batch_size(mut self, size: usize) -> Self {
        self.background_batch_size = size.min(64);
        self
    }

    /// Set background batch deadline
    pub fn with_background_batch_deadline(mut self, deadline: Duration) -> Self {
        self.background_batch_deadline = deadline;
        self
    }

    /// Set minimum background fraction
    pub fn with_min_background_fraction(mut self, fraction: f64) -> Self {
        self.min_background_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Set foreground activity threshold
    pub fn with_foreground_threshold(mut self, threshold: u64) -> Self {
        self.foreground_threshold = threshold;
        self
    }

    /// Set initial disk capacity estimate
    pub fn with_initial_disk_capacity(mut self, capacity: u64) -> Self {
        self.initial_disk_capacity = capacity;
        self
    }

    /// Set throughput measurement window
    pub fn with_throughput_window(mut self, window: Duration) -> Self {
        self.throughput_window = window;
        self
    }

    /// Set maximum hot rings
    pub fn with_max_hot_rings(mut self, max: usize) -> Self {
        self.max_hot_rings = max;
        self
    }
}
