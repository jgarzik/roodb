//! Throughput tracking for adaptive rate limiting
//!
//! Provides sliding window EMA calculation of I/O throughput per urgency class.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::Urgency;

/// Default window size for throughput calculation
const DEFAULT_WINDOW_MS: u64 = 100;

/// Default EMA smoothing factor (0.2 = 20% new, 80% old)
const DEFAULT_ALPHA: f64 = 0.2;

/// Throughput tracker with sliding window EMA
///
/// Tracks bytes per second for foreground (Critical + Foreground urgency)
/// and background (Background urgency) I/O separately.
#[derive(Debug)]
pub struct ThroughputTracker {
    /// Foreground bytes in current window
    foreground_bytes: AtomicU64,
    /// Background bytes in current window
    background_bytes: AtomicU64,

    /// EMA of foreground throughput (bytes/sec)
    foreground_ema: AtomicU64,
    /// EMA of background throughput (bytes/sec)
    background_ema: AtomicU64,

    /// Window management (protected by mutex for consistency)
    window_state: Mutex<WindowState>,

    /// Window duration
    window: Duration,
    /// EMA smoothing factor
    alpha: f64,
}

#[derive(Debug)]
struct WindowState {
    /// Window start time
    start: Instant,
    /// Last tick time
    last_tick: Instant,
}

impl ThroughputTracker {
    /// Create a new throughput tracker with default settings
    pub fn new() -> Self {
        Self::with_config(Duration::from_millis(DEFAULT_WINDOW_MS), DEFAULT_ALPHA)
    }

    /// Create with custom window and alpha
    pub fn with_config(window: Duration, alpha: f64) -> Self {
        let now = Instant::now();
        Self {
            foreground_bytes: AtomicU64::new(0),
            background_bytes: AtomicU64::new(0),
            foreground_ema: AtomicU64::new(0),
            background_ema: AtomicU64::new(0),
            window_state: Mutex::new(WindowState {
                start: now,
                last_tick: now,
            }),
            window,
            alpha,
        }
    }

    /// Record bytes for an I/O operation
    ///
    /// Call this when an operation completes.
    pub fn record(&self, urgency: Urgency, bytes: usize) {
        match urgency {
            Urgency::Critical | Urgency::Foreground => {
                self.foreground_bytes
                    .fetch_add(bytes as u64, Ordering::Relaxed);
            }
            Urgency::Background => {
                self.background_bytes
                    .fetch_add(bytes as u64, Ordering::Relaxed);
            }
        }
    }

    /// Tick the window - call periodically (e.g., every 10-50ms)
    ///
    /// Updates EMA if window has elapsed. Returns true if EMA was updated.
    pub fn tick(&self) -> bool {
        let now = Instant::now();
        let mut state = self.window_state.lock();

        let elapsed = now.duration_since(state.start);
        if elapsed < self.window {
            return false;
        }

        // Calculate throughput for this window
        let fg_bytes = self.foreground_bytes.swap(0, Ordering::Relaxed);
        let bg_bytes = self.background_bytes.swap(0, Ordering::Relaxed);

        let window_secs = elapsed.as_secs_f64();
        let fg_rate = (fg_bytes as f64 / window_secs) as u64;
        let bg_rate = (bg_bytes as f64 / window_secs) as u64;

        // Update EMA: new_ema = alpha * current + (1-alpha) * old_ema
        let old_fg = self.foreground_ema.load(Ordering::Relaxed);
        let old_bg = self.background_ema.load(Ordering::Relaxed);

        let new_fg = (self.alpha * fg_rate as f64 + (1.0 - self.alpha) * old_fg as f64) as u64;
        let new_bg = (self.alpha * bg_rate as f64 + (1.0 - self.alpha) * old_bg as f64) as u64;

        self.foreground_ema.store(new_fg, Ordering::Relaxed);
        self.background_ema.store(new_bg, Ordering::Relaxed);

        // Reset window
        state.start = now;
        state.last_tick = now;

        true
    }

    /// Get current foreground throughput EMA (bytes/sec)
    pub fn foreground_throughput(&self) -> u64 {
        self.foreground_ema.load(Ordering::Relaxed)
    }

    /// Get current background throughput EMA (bytes/sec)
    pub fn background_throughput(&self) -> u64 {
        self.background_ema.load(Ordering::Relaxed)
    }

    /// Get total throughput EMA (bytes/sec)
    pub fn total_throughput(&self) -> u64 {
        self.foreground_throughput() + self.background_throughput()
    }

    /// Check if foreground is currently active (above threshold)
    pub fn is_foreground_active(&self, threshold_bytes_sec: u64) -> bool {
        self.foreground_throughput() >= threshold_bytes_sec
    }

    /// Get the window duration
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Get the alpha value
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Get current window bytes (not yet added to EMA)
    pub fn current_window_bytes(&self) -> (u64, u64) {
        (
            self.foreground_bytes.load(Ordering::Relaxed),
            self.background_bytes.load(Ordering::Relaxed),
        )
    }

    /// Reset all counters and EMAs
    pub fn reset(&self) {
        self.foreground_bytes.store(0, Ordering::Relaxed);
        self.background_bytes.store(0, Ordering::Relaxed);
        self.foreground_ema.store(0, Ordering::Relaxed);
        self.background_ema.store(0, Ordering::Relaxed);

        let now = Instant::now();
        let mut state = self.window_state.lock();
        state.start = now;
        state.last_tick = now;
    }
}

impl Default for ThroughputTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_throughput_recording() {
        let tracker = ThroughputTracker::with_config(
            Duration::from_millis(10),
            0.5, // Higher alpha for faster response in tests
        );

        // Record some bytes
        tracker.record(Urgency::Foreground, 1000);
        tracker.record(Urgency::Background, 500);

        let (fg, bg) = tracker.current_window_bytes();
        assert_eq!(fg, 1000);
        assert_eq!(bg, 500);
    }

    #[test]
    fn test_ema_calculation() {
        let tracker = ThroughputTracker::with_config(
            Duration::from_millis(10),
            1.0, // Alpha = 1.0 means use current value only (no smoothing)
        );

        // Record 10KB in 10ms window = ~1MB/s
        tracker.record(Urgency::Foreground, 10_000);

        // Wait for window to elapse
        thread::sleep(Duration::from_millis(15));
        assert!(tracker.tick());

        // Should have ~1MB/s throughput (with some timing variance)
        let throughput = tracker.foreground_throughput();
        assert!(throughput > 500_000, "throughput was {}", throughput);
        assert!(throughput < 2_000_000, "throughput was {}", throughput);
    }

    #[test]
    fn test_urgency_classification() {
        let tracker = ThroughputTracker::new();

        // Critical and Foreground both go to foreground counter
        tracker.record(Urgency::Critical, 100);
        tracker.record(Urgency::Foreground, 200);
        tracker.record(Urgency::Background, 50);

        let (fg, bg) = tracker.current_window_bytes();
        assert_eq!(fg, 300); // Critical + Foreground
        assert_eq!(bg, 50);
    }

    #[test]
    fn test_foreground_active_detection() {
        let tracker = ThroughputTracker::with_config(Duration::from_millis(10), 1.0);

        // Record 1MB worth of data
        tracker.record(Urgency::Foreground, 1_000_000);

        thread::sleep(Duration::from_millis(15));
        tracker.tick();

        // Should be active at 1MB/s threshold (we recorded 1MB in ~10ms = ~100MB/s)
        assert!(tracker.is_foreground_active(1_000_000));
    }
}
