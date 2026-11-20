//! Per-query timing metrics for performance diagnostics
//!
//! Activated via `RUST_LOG=roodb::perf=debug`

use std::time::Instant;

/// Log how long a statement took to execute, including sending its result.
pub fn log_query(start: Instant) {
    tracing::debug!(
        target: "roodb::perf",
        total_us = start.elapsed().as_micros() as u64,
        "query_metrics"
    );
}

/// Helper to time a section and return elapsed nanos
#[inline]
pub fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}
