//! Unified row ID generator
//!
//! Provides globally unique row IDs across all operations (INSERT, DDL, auth).
//! Row IDs are unique across the cluster by encoding node_id in the high bits.
//!
//! Format: [16-bit node_id] [48-bit local counter]
//! This allows 65536 nodes and 281 trillion rows per node.

use std::sync::atomic::{AtomicU64, Ordering};

/// Node ID for this server instance (set at startup)
///
/// Used to ensure row IDs are globally unique across the cluster.
/// The node_id is encoded in the high 16 bits of each row ID.
static NODE_ID: AtomicU64 = AtomicU64::new(0);

/// Set the node ID for row ID generation
///
/// Must be called once at startup before any row IDs are generated.
/// The node_id should match the Raft node ID for consistency.
pub fn set_node_id(node_id: u64) {
    NODE_ID.store(node_id, Ordering::SeqCst);
}

/// Get the current node ID
pub fn get_node_id() -> u64 {
    NODE_ID.load(Ordering::SeqCst)
}

/// Global row ID generator
///
/// Generates globally unique row IDs for all database operations.
/// Row IDs encode the node_id in the high 16 bits to ensure uniqueness
/// across the cluster.
pub struct RowIdGenerator {
    /// Next local counter value
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

    /// Allocate a batch of row IDs
    ///
    /// Returns the start of the allocated range. The caller owns IDs from
    /// `start` to `start + count - 1` (inclusive).
    ///
    /// This reduces atomic contention by allocating many IDs at once.
    #[inline]
    pub fn allocate_batch(&self, count: u64) -> u64 {
        self.next_id.fetch_add(count, Ordering::SeqCst)
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
///
/// The row ID encodes the node_id in the high 16 bits and a local counter
/// in the low 48 bits, ensuring uniqueness across the cluster.
#[inline]
pub fn next_row_id() -> u64 {
    let local = GLOBAL_ROW_ID_GEN.next();
    let node_id = NODE_ID.load(Ordering::SeqCst);
    // Encode: [16-bit node_id][48-bit local counter]
    (node_id << 48) | (local & 0x0000_FFFF_FFFF_FFFF)
}

/// Allocate a batch of globally unique row IDs
///
/// Returns (start_local, node_id) where the caller owns local IDs from
/// `start_local` to `start_local + count - 1`. Use `encode_row_id()` to
/// convert each local ID to a full row ID.
///
/// This reduces atomic contention by allocating many IDs at once.
#[inline]
pub fn allocate_row_id_batch(count: u64) -> (u64, u64) {
    let start_local = GLOBAL_ROW_ID_GEN.allocate_batch(count);
    let node_id = NODE_ID.load(Ordering::SeqCst);
    (start_local, node_id)
}

/// Encode a local counter value into a full row ID
///
/// Combines the node_id (high 16 bits) with the local counter (low 48 bits).
#[inline]
pub fn encode_row_id(local: u64, node_id: u64) -> u64 {
    (node_id << 48) | (local & 0x0000_FFFF_FFFF_FFFF)
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

    #[test]
    fn test_node_id_encoding() {
        // Test that node_id is properly encoded in high bits
        set_node_id(5);
        let id = next_row_id();
        // Extract node_id from high 16 bits
        let extracted_node_id = id >> 48;
        assert_eq!(extracted_node_id, 5);
        // Local counter should be in low 48 bits
        let local_counter = id & 0x0000_FFFF_FFFF_FFFF;
        assert!(local_counter > 0);
        // Reset for other tests
        set_node_id(0);
    }
}
