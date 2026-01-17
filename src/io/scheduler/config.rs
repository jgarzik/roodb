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
    pub compaction_bandwidth_limit: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            target_queue_depth: DEFAULT_TARGET_QUEUE_DEPTH,
            high_watermark: DEFAULT_HIGH_WATERMARK,
            query_latency_slo: Duration::from_millis(DEFAULT_QUERY_LATENCY_SLO_MS),
            enable_large_chunks: true,
            compaction_bandwidth_limit: 0, // unlimited
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

    /// Set the compaction bandwidth limit
    pub fn with_compaction_bandwidth_limit(mut self, limit: u64) -> Self {
        self.compaction_bandwidth_limit = limit;
        self
    }
}
