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

/// Hands out row IDs, in order, from a range it has been allowed.
///
/// Row IDs become keys, so one must never be handed out twice - including
/// after a restart, when this counter starts again from what was saved. The
/// server therefore reserves IDs ahead of use: a range is recorded through
/// Raft before any ID in it is handed out, and after a restart allocation
/// resumes past everything ever reserved. The reserved range is `limit`;
/// allocation waits when it runs out, and whoever does the reserving watches
/// [`RowIdGenerator::demand`] to stay ahead.
///
/// Until [`RowIdGenerator::restore`] is called the range is unlimited: that
/// is how `roodb_init` and tests without a server use it.
pub struct RowIdGenerator {
    next_id: AtomicU64,
    /// IDs below this may be handed out. `u64::MAX` means unlimited.
    limit: AtomicU64,
    /// The end of the largest allocation that had to wait.
    wanted: AtomicU64,
}

/// How long an allocation waits for more IDs to be reserved before giving up.
const RESERVATION_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

impl RowIdGenerator {
    /// Create a new generator starting from 1
    pub const fn new() -> Self {
        Self::starting_at(1)
    }

    /// Create a generator starting from a specific value
    pub const fn starting_at(start: u64) -> Self {
        Self {
            next_id: AtomicU64::new(start),
            limit: AtomicU64::new(u64::MAX),
            wanted: AtomicU64::new(0),
        }
    }

    /// Generate the next row ID
    #[inline]
    pub fn next(&self) -> u64 {
        self.allocate_batch(1)
    }

    /// Allocate `count` consecutive IDs and return the first.
    ///
    /// Waits if the reserved range does not cover them yet.
    ///
    /// # Panics
    ///
    /// If nothing extends the range for 30 seconds. That means Raft cannot
    /// commit a reservation, and no write could succeed anyway.
    pub fn allocate_batch(&self, count: u64) -> u64 {
        let mut waited_since: Option<std::time::Instant> = None;
        loop {
            let start = self.next_id.load(Ordering::SeqCst);
            let end = start.saturating_add(count);
            if end <= self.limit.load(Ordering::SeqCst) {
                if self
                    .next_id
                    .compare_exchange(start, end, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return start;
                }
                continue;
            }
            // Out of reserved IDs: say how far this allocation needs the
            // range to reach, and give the reserver a moment.
            self.wanted.fetch_max(end, Ordering::SeqCst);
            let since = *waited_since.get_or_insert_with(std::time::Instant::now);
            assert!(
                since.elapsed() < RESERVATION_WAIT,
                "no row IDs could be reserved for {RESERVATION_WAIT:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Get current value without incrementing
    pub fn current(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    /// Start from saved state: no ID below `bound` is handed out, and none
    /// at all until a range is reserved with [`Self::extend_limit`].
    pub fn restore(&self, bound: u64) {
        let next = self.next_id.fetch_max(bound, Ordering::SeqCst).max(bound);
        // From unlimited to "nothing reserved yet"; never lower a limit a
        // reservation has already raised.
        let _ = self
            .limit
            .compare_exchange(u64::MAX, next, Ordering::SeqCst, Ordering::SeqCst);
        self.limit.fetch_max(bound, Ordering::SeqCst);
    }

    /// IDs below `upto` have been reserved and may be handed out.
    pub fn extend_limit(&self, upto: u64) {
        if self.limit.load(Ordering::SeqCst) != u64::MAX {
            self.limit.fetch_max(upto, Ordering::SeqCst);
        }
    }

    /// Skip every ID reserved so far. Called once after a restart has
    /// replayed the log: any of them may have been used before the restart.
    pub fn skip_reserved(&self) {
        let limit = self.limit.load(Ordering::SeqCst);
        if limit != u64::MAX {
            self.next_id.fetch_max(limit, Ordering::SeqCst);
        }
    }

    /// `(next, limit, wanted)`: the next ID, the end of the reserved range
    /// (`None` if unlimited), and the furthest any allocation has had to
    /// wait for.
    pub fn demand(&self) -> (u64, Option<u64>, u64) {
        let limit = self.limit.load(Ordering::SeqCst);
        (
            self.next_id.load(Ordering::SeqCst),
            (limit != u64::MAX).then_some(limit),
            self.wanted.load(Ordering::SeqCst),
        )
    }
}

impl Default for RowIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Global row ID generator
static GLOBAL_ROW_ID_GEN: RowIdGenerator = RowIdGenerator::new();

/// The process-wide generator, for the code that saves and reserves ranges.
pub fn generator() -> &'static RowIdGenerator {
    &GLOBAL_ROW_ID_GEN
}

/// Generate a globally unique row ID
///
/// Format: upper 16 bits = node_id, lower 48 bits = local counter
/// This supports up to 65535 nodes and ~281 trillion rows per node.
#[inline]
pub fn next_row_id() -> u64 {
    let local = GLOBAL_ROW_ID_GEN.next();
    let node_id = NODE_ID.load(Ordering::SeqCst);
    (node_id << 48) | (local & 0x0000_FFFF_FFFF_FFFF)
}

/// Allocate a batch of row IDs, returning the starting local counter and node_id
///
/// Returns (start_local, node_id) where caller can construct IDs as:
/// `(node_id << 48) | ((start_local + i) & 0x0000_FFFF_FFFF_FFFF)`
#[inline]
pub fn allocate_row_id_batch(count: u64) -> (u64, u64) {
    let start_local = GLOBAL_ROW_ID_GEN.allocate_batch(count);
    let node_id = NODE_ID.load(Ordering::SeqCst);
    (start_local, node_id)
}

/// Encode a local counter and node_id into a row ID
#[inline]
pub fn encode_row_id(local: u64, node_id: u64) -> u64 {
    (node_id << 48) | (local & 0x0000_FFFF_FFFF_FFFF)
}

/// Get current row ID without incrementing (for debugging/testing)
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

    #[test]
    fn unlimited_until_restored() {
        let gen = RowIdGenerator::new();
        assert_eq!(gen.allocate_batch(1_000_000), 1);
        assert_eq!(gen.demand(), (1_000_001, None, 0));
    }

    #[test]
    fn restore_resumes_past_saved_bound_and_waits_for_a_reservation() {
        let gen = std::sync::Arc::new(RowIdGenerator::new());
        gen.restore(5000);
        assert_eq!(gen.demand(), (5000, Some(5000), 0));

        // An allocation now has to wait until a range is reserved.
        let waiter = {
            let gen = gen.clone();
            std::thread::spawn(move || gen.allocate_batch(10))
        };
        while gen.demand().2 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(gen.demand().2, 5010, "the waiter says how far it needs");
        gen.extend_limit(6000);
        assert_eq!(waiter.join().unwrap(), 5000);
        assert_eq!(gen.next(), 5010);
    }

    #[test]
    fn ids_reserved_before_a_restart_are_skipped() {
        let gen = RowIdGenerator::new();
        // Saved state says IDs below 100 were reserved; the log then replays
        // two more reservations made before the crash.
        gen.restore(100);
        gen.extend_limit(200);
        gen.extend_limit(300);
        gen.skip_reserved();
        assert_eq!(gen.demand(), (300, Some(300), 0));
        gen.extend_limit(400);
        assert_eq!(gen.next(), 300);
    }

    #[test]
    fn restore_never_moves_backwards() {
        let gen = RowIdGenerator::new();
        gen.restore(1000);
        gen.extend_limit(2000);
        assert_eq!(gen.allocate_batch(500), 1000);
        // A second restore with an older bound changes nothing.
        gen.restore(10);
        assert_eq!(gen.demand(), (1500, Some(2000), 0));
        // extend_limit never shrinks either.
        gen.extend_limit(1200);
        assert_eq!(gen.demand().1, Some(2000));
    }
}
