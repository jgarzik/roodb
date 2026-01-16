//! Unified row ID generator
//!
//! Provides globally unique row IDs across all operations (INSERT, DDL, auth).
//! This replaces the scattered AtomicU64 counters that existed in different
//! executor modules.

use std::sync::atomic::{AtomicU64, Ordering};

/// Global row ID generator
///
/// Generates globally unique row IDs for all database operations.
/// Row IDs are monotonically increasing and unique within a server instance.
///
/// Note: In a distributed setting, row IDs should be combined with node ID
/// or use a distributed ID scheme (e.g., Snowflake) to ensure global uniqueness
/// across the cluster.
pub struct RowIdGenerator {
    /// Next row ID to allocate
    next_id: AtomicU64,
}

impl RowIdGenerator {
    /// Create a new row ID generator starting at 1
    pub const fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    /// Create a row ID generator starting at a specific value
    ///
    /// Used for recovery after restart to continue from the last allocated ID.
    pub const fn starting_at(start: u64) -> Self {
        Self {
            next_id: AtomicU64::new(start),
        }
    }

    /// Allocate the next row ID
    ///
    /// This is thread-safe and guaranteed to return unique IDs.
    #[inline]
    pub fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current counter value (for debugging/metrics)
    pub fn current(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }
}

impl Default for RowIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Global row ID generator instance
///
/// This is the single source of truth for row IDs in the database.
/// All modules should use this instead of maintaining their own counters.
static GLOBAL_ROW_ID_GEN: RowIdGenerator = RowIdGenerator::new();

/// Allocate the next globally unique row ID
#[inline]
pub fn next_row_id() -> u64 {
    GLOBAL_ROW_ID_GEN.next()
}

/// Get the current row ID counter value (for debugging/metrics)
pub fn current_row_id() -> u64 {
    GLOBAL_ROW_ID_GEN.current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_id_generator() {
        let gen = RowIdGenerator::new();
        assert_eq!(gen.next(), 1);
        assert_eq!(gen.next(), 2);
        assert_eq!(gen.next(), 3);
        assert_eq!(gen.current(), 4);
    }

    #[test]
    fn test_starting_at() {
        let gen = RowIdGenerator::starting_at(1000);
        assert_eq!(gen.next(), 1000);
        assert_eq!(gen.next(), 1001);
    }

    #[test]
    fn test_global_row_id() {
        // This test just verifies the global function works
        // Actual values depend on test order, so we just check it increases
        let id1 = next_row_id();
        let id2 = next_row_id();
        assert!(id2 > id1);
    }
}
