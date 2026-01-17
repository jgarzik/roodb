//! Adaptive rate limiter for background I/O
//!
//! Implements token bucket rate limiting with dynamic bandwidth allocation
//! based on foreground activity and learned disk capacity.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::config::SchedulerConfig;
use super::ThroughputTracker;

/// Minimum disk capacity estimate (10 MB/s)
const MIN_DISK_CAPACITY: u64 = 10_000_000;

/// Maximum disk capacity estimate (5 GB/s)
const MAX_DISK_CAPACITY: u64 = 5_000_000_000;

/// Latency target for capacity learning (50ms p99)
const LATENCY_TARGET_MS: u64 = 50;

/// Reduce capacity by this factor when latency too high
const CAPACITY_DECREASE_FACTOR: f64 = 0.90;

/// Increase capacity by this factor when latency is low
const CAPACITY_INCREASE_FACTOR: f64 = 1.05;

/// Latency threshold below which we increase capacity (target / 2)
const LATENCY_INCREASE_THRESHOLD_MS: u64 = LATENCY_TARGET_MS / 2;

/// Adaptive rate limiter for background I/O
///
/// Uses a token bucket algorithm with dynamically adjusted rates based on:
/// 1. Foreground activity (reduce background when foreground is active)
/// 2. Observed latency (learn actual disk capacity)
pub struct AdaptiveLimiter {
    /// Estimated disk capacity (bytes/sec) - learned over time
    disk_capacity: AtomicU64,

    /// Minimum background fraction when foreground is active
    min_bg_fraction: f64,

    /// Foreground activity threshold (bytes/sec)
    fg_threshold: u64,

    /// Token bucket state
    bucket: Mutex<TokenBucket>,

    /// Last capacity adjustment time
    last_adjustment: Mutex<Instant>,

    /// Adjustment interval
    adjustment_interval: Duration,
}

/// Token bucket state
#[derive(Debug)]
struct TokenBucket {
    /// Available tokens (bytes)
    tokens: u64,
    /// Maximum tokens (bucket size)
    max_tokens: u64,
    /// Token refill rate (bytes/sec)
    refill_rate: u64,
    /// Last refill time
    last_refill: Instant,
}

impl TokenBucket {
    fn new(initial_capacity: u64) -> Self {
        // Bucket holds 100ms worth of tokens
        let max_tokens = initial_capacity / 10;
        Self {
            tokens: max_tokens,
            max_tokens,
            refill_rate: initial_capacity,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let refill_amount = (self.refill_rate as f64 * elapsed.as_secs_f64()) as u64;

        self.tokens = (self.tokens + refill_amount).min(self.max_tokens);
        self.last_refill = now;
    }

    /// Try to acquire tokens, returns true if successful
    fn try_acquire(&mut self, bytes: u64) -> bool {
        self.refill();

        if self.tokens >= bytes {
            self.tokens -= bytes;
            true
        } else {
            false
        }
    }

    /// Update rate and bucket size
    fn set_rate(&mut self, rate: u64) {
        self.refill_rate = rate;
        self.max_tokens = rate / 10; // 100ms buffer
                                     // Don't reduce tokens below current level
        self.tokens = self.tokens.min(self.max_tokens);
    }

    /// Get current fill level (0.0 to 1.0)
    fn fill_level(&self) -> f64 {
        if self.max_tokens == 0 {
            return 1.0;
        }
        self.tokens as f64 / self.max_tokens as f64
    }
}

impl AdaptiveLimiter {
    /// Create a new adaptive limiter
    pub fn new(config: &SchedulerConfig) -> Self {
        let disk_capacity = config.initial_disk_capacity;

        Self {
            disk_capacity: AtomicU64::new(disk_capacity),
            min_bg_fraction: config.min_background_fraction,
            fg_threshold: config.foreground_threshold,
            bucket: Mutex::new(TokenBucket::new(disk_capacity)),
            last_adjustment: Mutex::new(Instant::now()),
            adjustment_interval: Duration::from_millis(100),
        }
    }

    /// Create with specific parameters
    pub fn with_config(initial_capacity: u64, min_bg_fraction: f64, fg_threshold: u64) -> Self {
        Self {
            disk_capacity: AtomicU64::new(initial_capacity),
            min_bg_fraction,
            fg_threshold,
            bucket: Mutex::new(TokenBucket::new(initial_capacity)),
            last_adjustment: Mutex::new(Instant::now()),
            adjustment_interval: Duration::from_millis(100),
        }
    }

    /// Try to acquire bandwidth for background I/O
    ///
    /// Returns true if the operation should proceed, false if it should wait.
    pub fn try_acquire(&self, bytes: u64) -> bool {
        self.bucket.lock().try_acquire(bytes)
    }

    /// Wait until bandwidth is available
    ///
    /// Polls with exponential backoff until tokens are available.
    pub async fn acquire(&self, bytes: u64) {
        let mut wait_us = 100;
        const MAX_WAIT_US: u64 = 10_000; // 10ms max

        while !self.try_acquire(bytes) {
            tokio::time::sleep(Duration::from_micros(wait_us)).await;
            wait_us = (wait_us * 2).min(MAX_WAIT_US);
        }
    }

    /// Update allocation based on throughput tracker
    ///
    /// Call this periodically (e.g., every throughput window tick).
    pub fn update_allocation(&self, tracker: &ThroughputTracker) {
        let fg_rate = tracker.foreground_throughput();
        let disk_capacity = self.disk_capacity.load(Ordering::Relaxed);

        // Calculate background fraction
        let bg_fraction = if fg_rate < self.fg_threshold {
            // Foreground idle: background gets full bandwidth
            1.0
        } else {
            // Foreground active: limit background
            let fg_fraction = (fg_rate as f64) / (disk_capacity as f64);
            (1.0 - fg_fraction).max(self.min_bg_fraction)
        };

        // Calculate new background rate
        let bg_rate = (disk_capacity as f64 * bg_fraction) as u64;

        // Update token bucket
        self.bucket.lock().set_rate(bg_rate);
    }

    /// Learn disk capacity from observed latency
    ///
    /// Call this periodically with observed p99 latency.
    /// Adjusts capacity estimate up or down based on latency.
    pub fn learn_capacity(&self, p99_latency: Duration) {
        // Rate limit adjustments
        {
            let mut last = self.last_adjustment.lock();
            if last.elapsed() < self.adjustment_interval {
                return;
            }
            *last = Instant::now();
        }

        let current = self.disk_capacity.load(Ordering::Relaxed);
        let latency_ms = p99_latency.as_millis() as u64;

        let new_capacity = if latency_ms > LATENCY_TARGET_MS {
            // Latency too high - reduce capacity estimate
            let reduced = (current as f64 * CAPACITY_DECREASE_FACTOR) as u64;
            reduced.max(MIN_DISK_CAPACITY)
        } else if latency_ms < LATENCY_INCREASE_THRESHOLD_MS {
            // Latency very low - increase capacity estimate
            let increased = (current as f64 * CAPACITY_INCREASE_FACTOR) as u64;
            increased.min(MAX_DISK_CAPACITY)
        } else {
            // Latency in acceptable range - no change
            current
        };

        if new_capacity != current {
            self.disk_capacity.store(new_capacity, Ordering::Relaxed);
        }
    }

    /// Get current disk capacity estimate
    pub fn disk_capacity(&self) -> u64 {
        self.disk_capacity.load(Ordering::Relaxed)
    }

    /// Get current background rate
    pub fn current_rate(&self) -> u64 {
        self.bucket.lock().refill_rate
    }

    /// Get current fill level (0.0 to 1.0)
    pub fn fill_level(&self) -> f64 {
        self.bucket.lock().fill_level()
    }

    /// Get minimum background fraction
    pub fn min_background_fraction(&self) -> f64 {
        self.min_bg_fraction
    }

    /// Get foreground activity threshold
    pub fn foreground_threshold(&self) -> u64 {
        self.fg_threshold
    }

    /// Reset to initial state
    pub fn reset(&self, capacity: u64) {
        self.disk_capacity.store(capacity, Ordering::Relaxed);
        *self.bucket.lock() = TokenBucket::new(capacity);
    }
}

impl Default for AdaptiveLimiter {
    fn default() -> Self {
        Self::with_config(
            500_000_000, // 500 MB/s
            0.2,         // 20% min background
            1_000_000,   // 1 MB/s threshold
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::scheduler::Urgency;

    #[test]
    fn test_limiter_creation() {
        let limiter = AdaptiveLimiter::default();
        assert_eq!(limiter.disk_capacity(), 500_000_000);
        assert_eq!(limiter.min_background_fraction(), 0.2);
    }

    #[test]
    fn test_token_bucket_acquire() {
        let limiter = AdaptiveLimiter::with_config(
            1_000_000, // 1 MB/s
            0.2, 100_000,
        );

        // Should be able to acquire up to bucket size
        // Bucket holds 100ms worth = 100KB
        assert!(limiter.try_acquire(50_000));
        assert!(limiter.try_acquire(50_000));

        // Should fail for more than remaining
        assert!(!limiter.try_acquire(50_000));
    }

    #[test]
    fn test_capacity_learning_decrease() {
        let limiter = AdaptiveLimiter::with_config(
            1_000_000_000, // 1 GB/s
            0.2,
            1_000_000,
        );

        let initial = limiter.disk_capacity();

        // Wait for adjustment interval
        std::thread::sleep(Duration::from_millis(110));

        // High latency should decrease capacity
        limiter.learn_capacity(Duration::from_millis(100));

        let after = limiter.disk_capacity();
        assert!(
            after < initial,
            "Capacity should decrease: {} -> {}",
            initial,
            after
        );
    }

    #[test]
    fn test_capacity_learning_increase() {
        let limiter = AdaptiveLimiter::with_config(
            100_000_000, // 100 MB/s
            0.2,
            1_000_000,
        );

        let initial = limiter.disk_capacity();

        // Wait for adjustment interval
        std::thread::sleep(Duration::from_millis(110));

        // Low latency should increase capacity
        limiter.learn_capacity(Duration::from_millis(10));

        let after = limiter.disk_capacity();
        assert!(
            after > initial,
            "Capacity should increase: {} -> {}",
            initial,
            after
        );
    }

    #[test]
    fn test_allocation_update_foreground_idle() {
        let limiter = AdaptiveLimiter::with_config(
            1_000_000_000, // 1 GB/s
            0.2,
            1_000_000, // 1 MB/s threshold
        );

        let tracker = ThroughputTracker::with_config(Duration::from_millis(10), 1.0);

        // No foreground activity - record nothing and tick
        std::thread::sleep(Duration::from_millis(15));
        tracker.tick();

        // Update allocation - should get full bandwidth
        limiter.update_allocation(&tracker);

        // Rate should be close to full disk capacity
        let rate = limiter.current_rate();
        assert!(rate > 900_000_000, "Rate should be near full: {}", rate);
    }

    #[test]
    fn test_allocation_update_foreground_active() {
        let limiter = AdaptiveLimiter::with_config(
            1_000_000_000, // 1 GB/s
            0.2,
            1_000_000, // 1 MB/s threshold
        );

        let tracker = ThroughputTracker::with_config(Duration::from_millis(10), 1.0);

        // Simulate heavy foreground activity - record enough data that even with
        // CI timing variance (sleep taking 100ms+ instead of 15ms), we still
        // exceed the 1 MB/s threshold significantly
        tracker.record(Urgency::Foreground, 100_000_000); // 100 MB

        std::thread::sleep(Duration::from_millis(15));
        tracker.tick();

        // Update allocation - background should be limited
        limiter.update_allocation(&tracker);

        let rate = limiter.current_rate();
        // Should be at least min_bg_fraction * capacity = 200 MB/s
        assert!(rate >= 200_000_000, "Rate should be at least 20%: {}", rate);
        // Should be less than full capacity (wide margin for CI variance)
        assert!(rate < 900_000_000, "Rate should be limited: {}", rate);
    }

    #[test]
    fn test_capacity_clamping() {
        // Test minimum clamping
        let limiter = AdaptiveLimiter::with_config(MIN_DISK_CAPACITY + 1_000_000, 0.2, 1_000_000);

        // Force decrease several times
        for _ in 0..10 {
            std::thread::sleep(Duration::from_millis(110));
            limiter.learn_capacity(Duration::from_millis(1000)); // Very high latency
        }

        assert!(
            limiter.disk_capacity() >= MIN_DISK_CAPACITY,
            "Should not go below minimum"
        );

        // Test maximum clamping
        let limiter = AdaptiveLimiter::with_config(MAX_DISK_CAPACITY - 100_000_000, 0.2, 1_000_000);

        // Force increase several times
        for _ in 0..10 {
            std::thread::sleep(Duration::from_millis(110));
            limiter.learn_capacity(Duration::from_millis(1)); // Very low latency
        }

        assert!(
            limiter.disk_capacity() <= MAX_DISK_CAPACITY,
            "Should not go above maximum"
        );
    }
}
