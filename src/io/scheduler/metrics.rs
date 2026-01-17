//! I/O metrics collection for scheduler
//!
//! Provides latency histograms, counters, and gauges for monitoring
//! I/O performance and backpressure state.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

use super::IoPriority;

/// Number of histogram buckets
const HISTOGRAM_BUCKETS: usize = 8;

/// Latency histogram with fixed buckets
///
/// Buckets: <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, <1s, >1s
#[derive(Debug)]
pub struct LatencyHistogram {
    /// Bucket counts
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
    /// Sum of all latencies in microseconds
    sum_us: AtomicU64,
    /// Total count
    count: AtomicU64,
}

impl LatencyHistogram {
    /// Create a new empty histogram
    pub const fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record a latency value
    pub fn record(&self, latency: Duration) {
        let us = latency.as_micros() as u64;
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let bucket = match us {
            0..=999 => 0,         // <1ms
            1000..=4999 => 1,     // <5ms
            5000..=9999 => 2,     // <10ms
            10000..=49999 => 3,   // <50ms
            50000..=99999 => 4,   // <100ms
            100000..=499999 => 5, // <500ms
            500000..=999999 => 6, // <1s
            _ => 7,               // >1s
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Get the average latency
    pub fn average(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let sum_us = self.sum_us.load(Ordering::Relaxed);
        Duration::from_micros(sum_us / count)
    }

    /// Get the total count
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Estimate a percentile (approximate based on buckets)
    ///
    /// Returns the upper bound of the bucket containing the percentile.
    pub fn percentile(&self, p: f64) -> Duration {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }

        let target = (total as f64 * p / 100.0) as u64;
        let mut cumulative = 0u64;

        let bucket_bounds_us = [1000, 5000, 10000, 50000, 100000, 500000, 1000000, u64::MAX];

        for (i, bound) in bucket_bounds_us.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(*bound);
            }
        }

        Duration::from_secs(1)
    }

    /// Get P50 (median)
    pub fn p50(&self) -> Duration {
        self.percentile(50.0)
    }

    /// Get P99
    pub fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    /// Get bucket counts as array
    pub fn buckets(&self) -> [u64; HISTOGRAM_BUCKETS] {
        [
            self.buckets[0].load(Ordering::Relaxed),
            self.buckets[1].load(Ordering::Relaxed),
            self.buckets[2].load(Ordering::Relaxed),
            self.buckets[3].load(Ordering::Relaxed),
            self.buckets[4].load(Ordering::Relaxed),
            self.buckets[5].load(Ordering::Relaxed),
            self.buckets[6].load(Ordering::Relaxed),
            self.buckets[7].load(Ordering::Relaxed),
        ]
    }

    /// Reset the histogram
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.sum_us.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// I/O metrics for the scheduler
#[derive(Debug)]
pub struct IoMetrics {
    // Counters
    /// Total I/O requests submitted
    pub requests_submitted: AtomicU64,
    /// Total I/O requests completed
    pub requests_completed: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,

    // Gauges
    /// Current queue depth
    pub queue_depth: AtomicU32,
    /// Current backpressure level (0-3)
    pub backpressure_level: AtomicU8,
    /// Currently in-flight operations
    pub in_flight_ops: AtomicU32,

    // Per-priority latency histograms
    /// WAL latency histogram
    pub wal_latency: LatencyHistogram,
    /// Query read latency histogram
    pub query_latency: LatencyHistogram,
    /// Flush latency histogram
    pub flush_latency: LatencyHistogram,
    /// Compaction latency histogram
    pub compaction_latency: LatencyHistogram,

    // Batch statistics
    /// Batch size histogram (reuse LatencyHistogram for distribution)
    pub batch_sizes: LatencyHistogram,
    /// Total batches submitted
    pub batches_submitted: AtomicU64,
}

impl IoMetrics {
    /// Create new metrics
    pub const fn new() -> Self {
        Self {
            requests_submitted: AtomicU64::new(0),
            requests_completed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            queue_depth: AtomicU32::new(0),
            backpressure_level: AtomicU8::new(0),
            in_flight_ops: AtomicU32::new(0),
            wal_latency: LatencyHistogram::new(),
            query_latency: LatencyHistogram::new(),
            flush_latency: LatencyHistogram::new(),
            compaction_latency: LatencyHistogram::new(),
            batch_sizes: LatencyHistogram::new(),
            batches_submitted: AtomicU64::new(0),
        }
    }

    /// Record a completed I/O operation
    pub fn record_completion(&self, priority: IoPriority, latency: Duration, bytes: usize) {
        self.requests_completed.fetch_add(1, Ordering::Relaxed);

        // Record to appropriate histogram
        match priority {
            IoPriority::Wal => self.wal_latency.record(latency),
            IoPriority::QueryRead => self.query_latency.record(latency),
            IoPriority::Flush => self.flush_latency.record(latency),
            IoPriority::Compaction => self.compaction_latency.record(latency),
        }

        // Update bytes counters based on operation type
        // Note: caller should use record_read or record_write for accurate tracking
        let _ = bytes;
    }

    /// Record bytes read
    pub fn record_read(&self, bytes: usize) {
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record bytes written
    pub fn record_write(&self, bytes: usize) {
        self.bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record a batch submission
    pub fn record_batch(&self, size: usize) {
        self.batches_submitted.fetch_add(1, Ordering::Relaxed);
        // Record batch size as "latency" in microseconds for histogram reuse
        self.batch_sizes.record(Duration::from_micros(size as u64));
    }

    /// Get the latency histogram for a priority
    pub fn latency_for(&self, priority: IoPriority) -> &LatencyHistogram {
        match priority {
            IoPriority::Wal => &self.wal_latency,
            IoPriority::QueryRead => &self.query_latency,
            IoPriority::Flush => &self.flush_latency,
            IoPriority::Compaction => &self.compaction_latency,
        }
    }

    /// Get average latency across all priorities
    pub fn average_latency(&self) -> Duration {
        let mut total_sum = 0u64;
        let mut total_count = 0u64;

        for priority in IoPriority::all() {
            let hist = self.latency_for(priority);
            total_sum += hist.sum_us.load(Ordering::Relaxed);
            total_count += hist.count.load(Ordering::Relaxed);
        }

        if total_count == 0 {
            Duration::ZERO
        } else {
            Duration::from_micros(total_sum / total_count)
        }
    }

    /// Take a snapshot of current metrics
    pub fn snapshot(&self) -> IoMetricsSnapshot {
        IoMetricsSnapshot {
            requests_submitted: self.requests_submitted.load(Ordering::Relaxed),
            requests_completed: self.requests_completed.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            backpressure_level: self.backpressure_level.load(Ordering::Relaxed),
            in_flight_ops: self.in_flight_ops.load(Ordering::Relaxed),
            avg_latency: self.average_latency(),
            wal_p99: self.wal_latency.p99(),
            query_p99: self.query_latency.p99(),
            flush_p99: self.flush_latency.p99(),
            compaction_p99: self.compaction_latency.p99(),
            batches_submitted: self.batches_submitted.load(Ordering::Relaxed),
        }
    }
}

impl Default for IoMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of I/O metrics
#[derive(Debug, Clone)]
pub struct IoMetricsSnapshot {
    pub requests_submitted: u64,
    pub requests_completed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub queue_depth: u32,
    pub backpressure_level: u8,
    pub in_flight_ops: u32,
    pub avg_latency: Duration,
    pub wal_p99: Duration,
    pub query_p99: Duration,
    pub flush_p99: Duration,
    pub compaction_p99: Duration,
    pub batches_submitted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_recording() {
        let hist = LatencyHistogram::new();

        // Record some latencies
        hist.record(Duration::from_micros(500)); // <1ms bucket
        hist.record(Duration::from_millis(3)); // <5ms bucket
        hist.record(Duration::from_millis(8)); // <10ms bucket
        hist.record(Duration::from_millis(25)); // <50ms bucket

        assert_eq!(hist.count(), 4);

        let buckets = hist.buckets();
        assert_eq!(buckets[0], 1); // <1ms
        assert_eq!(buckets[1], 1); // <5ms
        assert_eq!(buckets[2], 1); // <10ms
        assert_eq!(buckets[3], 1); // <50ms
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = IoMetrics::new();

        metrics.requests_submitted.store(100, Ordering::Relaxed);
        metrics.requests_completed.store(95, Ordering::Relaxed);
        metrics.bytes_read.store(1024 * 1024, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_submitted, 100);
        assert_eq!(snapshot.requests_completed, 95);
        assert_eq!(snapshot.bytes_read, 1024 * 1024);
    }
}
