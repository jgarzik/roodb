//! Row ID generator
//!
//! Row IDs for rows without a key of their own (tables without a primary
//! key, system table rows). One counter for the cluster: only the primary
//! hands IDs out, the ranges it may have used are recorded in the log
//! (`Command::ReserveRowIds`), and a node that becomes primary continues
//! past them. IDs therefore ascend in the order rows were inserted,
//! whichever node inserted them.

use std::sync::atomic::{AtomicU64, Ordering};

/// Hands out row IDs, in order.
///
/// Row IDs become keys, so one must never be handed out twice - including
/// after a restart, when this counter starts again from what was saved. The
/// server therefore reserves IDs through Raft: a bound is recorded in the
/// log, and after a restart allocation resumes past every bound recorded.
///
/// Handing out an ID never waits. What waits is the commit: before a
/// transaction's changes are proposed, the reserved bound must cover every ID
/// handed out so far ([`RowIdGenerator::uncovered`]). An ID above the bound
/// can therefore exist only in work that has not been committed, and reusing
/// it after a crash is harmless.
///
/// Until [`RowIdGenerator::restore`] is called no bound is tracked: that is
/// how `roodb_init` and tests without a server use it.
pub struct RowIdGenerator {
    next_id: AtomicU64,
    /// IDs below this are covered by a recorded reservation. `u64::MAX`
    /// means reservations are not in use.
    limit: AtomicU64,
}

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
        }
    }

    /// Generate the next row ID
    #[inline]
    pub fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Allocate `count` consecutive IDs and return the first.
    #[inline]
    pub fn allocate_batch(&self, count: u64) -> u64 {
        self.next_id.fetch_add(count, Ordering::SeqCst)
    }

    /// Get current value without incrementing
    pub fn current(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    /// Start from saved state: no ID below `bound` is handed out, and
    /// reservations are tracked from here on.
    pub fn restore(&self, bound: u64) {
        self.next_id.fetch_max(bound, Ordering::SeqCst);
        // From "not in use" to the saved bound; never lower a bound that a
        // reservation has already raised.
        let _ = self
            .limit
            .compare_exchange(u64::MAX, bound, Ordering::SeqCst, Ordering::SeqCst);
        self.limit.fetch_max(bound, Ordering::SeqCst);
    }

    /// A reservation of the IDs below `upto` has been recorded.
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

    /// If IDs have been handed out beyond the reserved bound, the value the
    /// bound has to reach to cover them. Nothing that might contain such an
    /// ID may be committed until a reservation up to there is recorded.
    pub fn uncovered(&self) -> Option<u64> {
        let limit = self.limit.load(Ordering::SeqCst);
        let next = self.next_id.load(Ordering::SeqCst);
        (limit != u64::MAX && next > limit).then_some(next)
    }

    /// `(next, limit)`: the next ID, and the reserved bound (`None` if
    /// reservations are not in use).
    pub fn demand(&self) -> (u64, Option<u64>) {
        let limit = self.limit.load(Ordering::SeqCst);
        (
            self.next_id.load(Ordering::SeqCst),
            (limit != u64::MAX).then_some(limit),
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

/// Generate the next row ID
#[inline]
pub fn next_row_id() -> u64 {
    GLOBAL_ROW_ID_GEN.next()
}

/// Allocate `count` consecutive row IDs and return the first.
#[inline]
pub fn allocate_row_id_batch(count: u64) -> u64 {
    GLOBAL_ROW_ID_GEN.allocate_batch(count)
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
    fn no_bound_is_tracked_until_restored() {
        let gen = RowIdGenerator::new();
        assert_eq!(gen.allocate_batch(1_000_000), 1);
        assert_eq!(gen.demand(), (1_000_001, None));
        assert_eq!(gen.uncovered(), None);
    }

    #[test]
    fn allocation_never_waits_but_reports_what_is_uncovered() {
        let gen = RowIdGenerator::new();
        gen.restore(5000);
        assert_eq!(gen.demand(), (5000, Some(5000)));
        assert_eq!(gen.uncovered(), None);

        // IDs are handed out at once, past the bound.
        assert_eq!(gen.allocate_batch(10), 5000);
        assert_eq!(gen.uncovered(), Some(5010));

        // A reservation that does not reach far enough leaves some uncovered.
        gen.extend_limit(5005);
        assert_eq!(gen.uncovered(), Some(5010));
        gen.extend_limit(6000);
        assert_eq!(gen.uncovered(), None);
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
        assert_eq!(gen.demand(), (300, Some(300)));
        assert_eq!(gen.next(), 300);
        assert_eq!(gen.uncovered(), Some(301));
    }

    #[test]
    fn restore_never_moves_backwards() {
        let gen = RowIdGenerator::new();
        gen.restore(1000);
        gen.extend_limit(2000);
        assert_eq!(gen.allocate_batch(500), 1000);
        // A second restore with an older bound changes nothing.
        gen.restore(10);
        assert_eq!(gen.demand(), (1500, Some(2000)));
        // extend_limit never shrinks either.
        gen.extend_limit(1200);
        assert_eq!(gen.demand().1, Some(2000));
    }
}
