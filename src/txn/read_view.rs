//! Read view for MVCC snapshot isolation
//!
//! A ReadView represents a consistent snapshot of the database at a point in time.
//! It determines which row versions are visible to a transaction based on their
//! transaction IDs.

use std::collections::HashSet;

/// A read view for MVCC visibility decisions
///
/// This implements the InnoDB-style read view algorithm:
/// - Rows created by the owning transaction are always visible
/// - Rows created by transactions that were committed before the read view was created are visible
/// - Rows created by transactions that were active when the read view was created are NOT visible
/// - Rows created by transactions that started after the read view was created are NOT visible
#[derive(Debug, Clone, Default)]
pub struct ReadView {
    /// The transaction ID that created this read view
    pub creator_txn_id: u64,

    /// Transaction IDs that were active when this read view was created
    /// These transactions' changes are NOT visible to us
    pub active_txn_ids: HashSet<u64>,

    /// The lowest active transaction ID when this read view was created
    /// Transactions with ID < this are definitely committed and visible
    pub min_active_txn_id: u64,

    /// The highest transaction ID that had been assigned when this read view was created
    /// Transactions with ID > this started after us and are NOT visible
    pub max_txn_id: u64,
}

impl ReadView {
    /// Create a new read view
    ///
    /// # Arguments
    /// * `creator_txn_id` - The transaction creating this read view
    /// * `active_txn_ids` - Set of currently active transaction IDs
    /// * `max_txn_id` - The highest transaction ID assigned so far
    pub fn new(creator_txn_id: u64, active_txn_ids: HashSet<u64>, max_txn_id: u64) -> Self {
        let min_active_txn_id = active_txn_ids
            .iter()
            .copied()
            .min()
            .unwrap_or(max_txn_id + 1);

        Self {
            creator_txn_id,
            active_txn_ids,
            min_active_txn_id,
            max_txn_id,
        }
    }

    /// Check if a row version created by `row_txn_id` is visible to this read view
    ///
    /// Visibility rules (InnoDB-style):
    /// 1. Own writes are always visible
    /// 2. Rows from transactions committed before min_active_txn_id are visible
    /// 3. Rows from transactions started after max_txn_id are NOT visible
    /// 4. Rows from transactions in active_txn_ids are NOT visible (they were uncommitted)
    /// 5. All other rows are visible (committed between min and max)
    pub fn is_visible(&self, row_txn_id: u64) -> bool {
        // Rule 1: My own writes are always visible
        if row_txn_id == self.creator_txn_id {
            return true;
        }

        // Rule 2: Created before oldest active transaction = definitely committed, visible
        if row_txn_id < self.min_active_txn_id {
            return true;
        }

        // Rule 3: Created after read view = started after us, not visible
        if row_txn_id > self.max_txn_id {
            return false;
        }

        // Rule 4: Was active when we started = was uncommitted, not visible
        if self.active_txn_ids.contains(&row_txn_id) {
            return false;
        }

        // Rule 5: Committed between min and max = visible
        true
    }

    /// Check if a deleted row should be visible
    /// A deletion is visible if the deleting transaction is visible
    pub fn is_deletion_visible(&self, delete_txn_id: u64) -> bool {
        self.is_visible(delete_txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_own_writes_visible() {
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);
        // Own transaction's writes are always visible
        assert!(view.is_visible(100));
    }

    #[test]
    fn test_old_committed_visible() {
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);
        // Transactions committed before min_active (50) are visible
        assert!(view.is_visible(10));
        assert!(view.is_visible(49));
    }

    #[test]
    fn test_future_transactions_not_visible() {
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);
        // Transactions started after max_txn_id are not visible
        assert!(!view.is_visible(101));
        assert!(!view.is_visible(200));
    }

    #[test]
    fn test_active_transactions_not_visible() {
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);
        // Transactions that were active are not visible
        assert!(!view.is_visible(50));
        assert!(!view.is_visible(75));
    }

    #[test]
    fn test_committed_between_min_max_visible() {
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);
        // Transaction 60 was committed (not in active set) - visible
        assert!(view.is_visible(60));
        // Transaction 90 was committed (not in active set) - visible
        assert!(view.is_visible(90));
    }

    #[test]
    fn test_empty_active_set() {
        // No active transactions except ourselves
        let view = ReadView::new(100, HashSet::new(), 100);
        // Everything up to max_txn_id should be visible
        assert!(view.is_visible(1));
        assert!(view.is_visible(50));
        assert!(view.is_visible(99));
        assert!(view.is_visible(100)); // own writes
        assert!(!view.is_visible(101)); // future
    }

    #[test]
    fn test_min_active_calculation() {
        let view = ReadView::new(100, HashSet::from([30, 50, 75]), 100);
        assert_eq!(view.min_active_txn_id, 30);

        // Transaction 29 is before min_active, should be visible
        assert!(view.is_visible(29));
        // Transaction 30 is in active set, not visible
        assert!(!view.is_visible(30));
    }
}
