//! Per-query timing metrics for performance diagnostics
//!
//! Activated via `RUST_LOG=roodb::perf=debug`

use std::time::{Duration, Instant};

/// Accumulated timing for a single query execution
pub struct QueryMetrics {
    pub total: Duration,
    pub plan_ns: u64,
    pub exec_open_ns: u64,
    pub exec_iter_ns: u64,
    pub send_ns: u64,
}

impl QueryMetrics {
    pub fn log(&self, query_type: &str) {
        tracing::debug!(
            target: "roodb::perf",
            query_type,
            total_us = self.total.as_micros() as u64,
            plan_us = self.plan_ns / 1000,
            exec_open_us = self.exec_open_ns / 1000,
            exec_iter_us = self.exec_iter_ns / 1000,
            send_us = self.send_ns / 1000,
            "query_metrics"
        );
    }
}

/// Helper to time a section and return elapsed nanos
#[inline]
pub fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}
