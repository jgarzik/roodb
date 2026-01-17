//! Backpressure detection and signaling for I/O scheduler
//!
//! Monitors queue depth and latency to detect storage saturation,
//! signaling higher layers to slow down or pause non-critical work.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use super::config::SchedulerConfig;
use super::metrics::IoMetrics;

/// Backpressure level indicating storage load
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackpressureLevel {
    /// Normal operation - no backpressure
    Normal = 0,
    /// Light pressure - latency increasing, consider slowing non-critical work
    Light = 1,
    /// Heavy pressure - queue building up, pause compaction, slow flushes
    Heavy = 2,
    /// Saturated - storage overloaded, block all but WAL/Raft
    Saturated = 3,
}

impl BackpressureLevel {
    /// Convert from u8
    fn from_u8(v: u8) -> Self {
        match v {
            0 => BackpressureLevel::Normal,
            1 => BackpressureLevel::Light,
            2 => BackpressureLevel::Heavy,
            _ => BackpressureLevel::Saturated,
        }
    }
}

/// Controller for backpressure detection and signaling
///
/// Monitors I/O queue depth and latency to detect when storage is saturated.
/// Provides methods for higher layers to check if they should slow down.
pub struct BackpressureController {
    /// Current backpressure state
    state: AtomicU8,

    /// Notify waiters when state changes
    notify: Notify,

    /// Reference to metrics for updating backpressure gauge
    metrics: Option<std::sync::Arc<IoMetrics>>,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(BackpressureLevel::Normal as u8),
            notify: Notify::new(),
            metrics: None,
        }
    }

    /// Create with metrics reference for gauge updates
    pub fn with_metrics(metrics: std::sync::Arc<IoMetrics>) -> Self {
        Self {
            state: AtomicU8::new(BackpressureLevel::Normal as u8),
            notify: Notify::new(),
            metrics: Some(metrics),
        }
    }

    /// Get the current backpressure level
    pub fn level(&self) -> BackpressureLevel {
        BackpressureLevel::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Update backpressure state based on current metrics
    ///
    /// Called periodically by the scheduler to update state.
    pub fn update(&self, queue_depth: usize, avg_latency: Duration, config: &SchedulerConfig) {
        let level = if queue_depth >= config.high_watermark {
            BackpressureLevel::Saturated
        } else if queue_depth >= config.target_queue_depth {
            BackpressureLevel::Heavy
        } else if avg_latency > config.query_latency_slo {
            BackpressureLevel::Light
        } else {
            BackpressureLevel::Normal
        };

        let old = self.state.swap(level as u8, Ordering::SeqCst);
        if old != level as u8 {
            // Update metrics gauge if available
            if let Some(ref metrics) = self.metrics {
                metrics
                    .backpressure_level
                    .store(level as u8, Ordering::Relaxed);
            }
            // Wake any waiters
            self.notify.notify_waiters();
        }
    }

    /// Wait if storage is saturated
    ///
    /// For compaction and other low-priority operations that should
    /// pause when storage is overloaded.
    pub async fn wait_if_saturated(&self) {
        loop {
            let level = self.level();
            if level < BackpressureLevel::Saturated {
                return;
            }
            self.notify.notified().await;
        }
    }

    /// Wait if under heavy or saturated pressure
    ///
    /// For flush operations that should slow down but not completely stop.
    pub async fn wait_if_heavy(&self) {
        loop {
            let level = self.level();
            if level < BackpressureLevel::Heavy {
                return;
            }
            // Brief yield before checking again
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Check if memtable flush should slow down
    pub fn should_slow_flush(&self) -> bool {
        self.level() >= BackpressureLevel::Heavy
    }

    /// Check if compaction should pause
    pub fn should_pause_compaction(&self) -> bool {
        self.level() >= BackpressureLevel::Heavy
    }

    /// Check if new writes should be rejected
    ///
    /// Only returns true when completely saturated.
    pub fn should_reject_writes(&self) -> bool {
        self.level() >= BackpressureLevel::Saturated
    }

    /// Check if we're under any backpressure
    pub fn is_under_pressure(&self) -> bool {
        self.level() > BackpressureLevel::Normal
    }

    /// Force set the backpressure level (for testing)
    #[cfg(test)]
    pub fn set_level(&self, level: BackpressureLevel) {
        let old = self.state.swap(level as u8, Ordering::SeqCst);
        if old != level as u8 {
            self.notify.notify_waiters();
        }
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_levels() {
        let config = SchedulerConfig::default();
        let controller = BackpressureController::new();

        // Normal state initially
        assert_eq!(controller.level(), BackpressureLevel::Normal);
        assert!(!controller.should_slow_flush());
        assert!(!controller.should_pause_compaction());

        // Light pressure from latency
        controller.update(10, Duration::from_millis(20), &config);
        assert_eq!(controller.level(), BackpressureLevel::Light);
        assert!(!controller.should_slow_flush()); // Light doesn't pause

        // Heavy pressure from queue depth
        controller.update(50, Duration::from_millis(5), &config);
        assert_eq!(controller.level(), BackpressureLevel::Heavy);
        assert!(controller.should_slow_flush());
        assert!(controller.should_pause_compaction());

        // Saturated at high watermark
        controller.update(60, Duration::from_millis(5), &config);
        assert_eq!(controller.level(), BackpressureLevel::Saturated);
        assert!(controller.should_reject_writes());

        // Back to normal
        controller.update(10, Duration::from_millis(5), &config);
        assert_eq!(controller.level(), BackpressureLevel::Normal);
    }
}
