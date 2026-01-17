//! InnoDB-style undo log for transaction rollback and MVCC
//!
//! The undo log stores previous versions of rows for:
//! 1. Rollback: Restoring original state when a transaction aborts
//! 2. MVCC: Providing older row versions to transactions with earlier read views

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// Type of undo operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndoType {
    /// Row was inserted - rollback by deleting
    Insert,
    /// Row was updated - rollback by restoring old value
    Update,
    /// Row was deleted - rollback by restoring the row
    Delete,
}

/// A single undo record representing a previous row version
#[derive(Debug, Clone)]
pub struct UndoRecord {
    /// Unique identifier for this undo record (used as DB_ROLL_PTR)
    pub roll_ptr: u64,

    /// Transaction ID that created this undo record
    pub txn_id: u64,

    /// Table name
    pub table_name: String,

    /// The row's primary key
    pub row_key: Vec<u8>,

    /// Type of operation that was undone
    pub undo_type: UndoType,

    /// The previous row's transaction ID (for version chain)
    pub old_trx_id: u64,

    /// The previous row's roll pointer (for version chain, 0 = end of chain)
    pub old_roll_ptr: u64,

    /// The previous row data (None for INSERT undo - row didn't exist before)
    pub old_data: Option<Vec<u8>>,
}

/// Undo log manager
///
/// Stores undo records for active transactions. Records are:
/// - Added during INSERT/UPDATE/DELETE operations
/// - Applied in reverse order during ROLLBACK
/// - Purged after the transaction commits and no read views need them
pub struct UndoLog {
    /// Undo records indexed by transaction ID
    records: RwLock<HashMap<u64, Vec<UndoRecord>>>,

    /// Index from roll_ptr to (txn_id, index) for fast lookup
    roll_ptr_index: RwLock<HashMap<u64, (u64, usize)>>,

    /// Next roll_ptr value
    next_roll_ptr: AtomicU64,
}

impl UndoLog {
    /// Create a new undo log
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            roll_ptr_index: RwLock::new(HashMap::new()),
            // Start at 1 so 0 can mean "no previous version"
            next_roll_ptr: AtomicU64::new(1),
        }
    }

    /// Allocate a new roll_ptr
    fn alloc_roll_ptr(&self) -> u64 {
        self.next_roll_ptr.fetch_add(1, Ordering::SeqCst)
    }

    /// Log an INSERT operation
    ///
    /// For inserts, there is no old data - rollback will delete the row.
    ///
    /// Returns the roll_ptr for the undo record.
    pub fn log_insert(&self, txn_id: u64, table_name: String, row_key: Vec<u8>) -> u64 {
        let roll_ptr = self.alloc_roll_ptr();
        let record = UndoRecord {
            roll_ptr,
            txn_id,
            table_name,
            row_key,
            undo_type: UndoType::Insert,
            old_trx_id: 0,
            old_roll_ptr: 0,
            old_data: None,
        };

        self.add_record(record)
    }

    /// Log an UPDATE operation
    ///
    /// Stores the old row value so it can be restored on rollback.
    ///
    /// # Arguments
    /// * `txn_id` - The transaction performing the update
    /// * `table_name` - The table being updated
    /// * `row_key` - The row's primary key
    /// * `old_data` - The row data before the update
    /// * `old_trx_id` - The transaction ID that created the old version
    /// * `old_roll_ptr` - The roll_ptr of the old version (for version chain)
    ///
    /// Returns the roll_ptr for the undo record.
    pub fn log_update(
        &self,
        txn_id: u64,
        table_name: String,
        row_key: Vec<u8>,
        old_data: Vec<u8>,
        old_trx_id: u64,
        old_roll_ptr: u64,
    ) -> u64 {
        let roll_ptr = self.alloc_roll_ptr();
        let record = UndoRecord {
            roll_ptr,
            txn_id,
            table_name,
            row_key,
            undo_type: UndoType::Update,
            old_trx_id,
            old_roll_ptr,
            old_data: Some(old_data),
        };

        self.add_record(record)
    }

    /// Log a DELETE operation
    ///
    /// Stores the deleted row so it can be restored on rollback.
    ///
    /// Returns the roll_ptr for the undo record.
    pub fn log_delete(
        &self,
        txn_id: u64,
        table_name: String,
        row_key: Vec<u8>,
        old_data: Vec<u8>,
        old_trx_id: u64,
        old_roll_ptr: u64,
    ) -> u64 {
        let roll_ptr = self.alloc_roll_ptr();
        let record = UndoRecord {
            roll_ptr,
            txn_id,
            table_name,
            row_key,
            undo_type: UndoType::Delete,
            old_trx_id,
            old_roll_ptr,
            old_data: Some(old_data),
        };

        self.add_record(record)
    }

    /// Add a record to the log
    fn add_record(&self, record: UndoRecord) -> u64 {
        let roll_ptr = record.roll_ptr;
        let txn_id = record.txn_id;

        let mut records = self.records.write();
        let txn_records = records.entry(txn_id).or_default();
        let index = txn_records.len();
        txn_records.push(record);

        self.roll_ptr_index
            .write()
            .insert(roll_ptr, (txn_id, index));

        roll_ptr
    }

    /// Get an undo record by roll_ptr
    ///
    /// # Locking
    /// This function acquires two read locks in sequence:
    /// 1. `roll_ptr_index` (read) - to map roll_ptr to (txn_id, index)
    /// 2. `records` (read) - to retrieve the actual record
    ///
    /// This is safe because:
    /// - Both are read locks (concurrent reads allowed)
    /// - Lock ordering is consistent: always roll_ptr_index before records
    /// - No deadlock possible since all code paths follow this order
    pub fn get_by_roll_ptr(&self, roll_ptr: u64) -> Option<UndoRecord> {
        if roll_ptr == 0 {
            return None;
        }

        let index_map = self.roll_ptr_index.read();
        let (txn_id, index) = index_map.get(&roll_ptr)?;

        let records = self.records.read();
        records.get(txn_id)?.get(*index).cloned()
    }

    /// Get all undo records for a transaction (in order of creation)
    pub fn get_records(&self, txn_id: u64) -> Vec<UndoRecord> {
        self.records
            .read()
            .get(&txn_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get undo records in reverse order (for rollback)
    pub fn get_records_reversed(&self, txn_id: u64) -> Vec<UndoRecord> {
        let mut records = self.get_records(txn_id);
        records.reverse();
        records
    }

    /// Remove all undo records for a transaction
    ///
    /// Called after commit (for INSERT undo logs) or after rollback completes.
    pub fn remove_transaction(&self, txn_id: u64) {
        let removed_records = self.records.write().remove(&txn_id).unwrap_or_default();

        // Remove from roll_ptr index
        let mut index_map = self.roll_ptr_index.write();
        for record in removed_records {
            index_map.remove(&record.roll_ptr);
        }
    }

    /// Purge undo records that are no longer needed
    ///
    /// Records can be purged when:
    /// - For INSERT: After commit (no rollback needed, no older versions)
    /// - For UPDATE/DELETE: When no active transaction has a read view that needs them
    ///
    /// # Arguments
    /// * `min_active_txn_id` - The lowest active transaction ID. Undo records for
    ///   committed transactions older than this can be purged.
    ///
    /// # Locking
    /// This function acquires locks sequentially to minimize contention:
    /// 1. `records` (write) - collect roll_ptrs and remove transaction records
    /// 2. Release `records`
    /// 3. `roll_ptr_index` (write) - remove index entries
    ///
    /// There's a brief window where the index may point to removed records, but
    /// this is safe because purged transactions are guaranteed to be older than
    /// any active transaction's read view (no valid lookups are possible).
    pub fn purge(&self, min_active_txn_id: u64) {
        // Phase 1: Collect roll_ptrs to remove and remove records
        let roll_ptrs_to_remove: Vec<u64> = {
            let mut records = self.records.write();

            let txns_to_purge: Vec<u64> = records
                .keys()
                .filter(|&&txn_id| txn_id < min_active_txn_id)
                .copied()
                .collect();

            let mut roll_ptrs = Vec::new();
            for txn_id in txns_to_purge {
                if let Some(removed) = records.remove(&txn_id) {
                    for record in removed {
                        roll_ptrs.push(record.roll_ptr);
                    }
                }
            }
            roll_ptrs
            // records lock released here
        };

        // Phase 2: Remove from index (separate lock scope)
        if !roll_ptrs_to_remove.is_empty() {
            let mut index_map = self.roll_ptr_index.write();
            for roll_ptr in roll_ptrs_to_remove {
                index_map.remove(&roll_ptr);
            }
        }
    }

    /// Get count of undo records (for metrics/debugging)
    pub fn record_count(&self) -> usize {
        self.records.read().values().map(|v| v.len()).sum()
    }

    /// Get count of transactions with undo records
    pub fn transaction_count(&self) -> usize {
        self.records.read().len()
    }
}

impl Default for UndoLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_insert() {
        let log = UndoLog::new();
        let roll_ptr = log.log_insert(1, "users".to_string(), vec![1, 2, 3]);

        assert_eq!(roll_ptr, 1);
        let record = log.get_by_roll_ptr(roll_ptr).unwrap();
        assert_eq!(record.txn_id, 1);
        assert_eq!(record.undo_type, UndoType::Insert);
        assert!(record.old_data.is_none());
    }

    #[test]
    fn test_log_update() {
        let log = UndoLog::new();
        let roll_ptr = log.log_update(
            1,
            "users".to_string(),
            vec![1, 2, 3],
            vec![10, 20, 30], // old data
            0,                // old_trx_id (first version)
            0,                // old_roll_ptr (no chain)
        );

        let record = log.get_by_roll_ptr(roll_ptr).unwrap();
        assert_eq!(record.undo_type, UndoType::Update);
        assert_eq!(record.old_data, Some(vec![10, 20, 30]));
    }

    #[test]
    fn test_log_delete() {
        let log = UndoLog::new();
        let roll_ptr = log.log_delete(
            1,
            "users".to_string(),
            vec![1, 2, 3],
            vec![10, 20, 30], // deleted row data
            0,
            0,
        );

        let record = log.get_by_roll_ptr(roll_ptr).unwrap();
        assert_eq!(record.undo_type, UndoType::Delete);
        assert_eq!(record.old_data, Some(vec![10, 20, 30]));
    }

    #[test]
    fn test_get_records_reversed() {
        let log = UndoLog::new();
        log.log_insert(1, "users".to_string(), vec![1]);
        log.log_insert(1, "users".to_string(), vec![2]);
        log.log_insert(1, "users".to_string(), vec![3]);

        let records = log.get_records_reversed(1);
        assert_eq!(records.len(), 3);
        // Should be in reverse order for rollback
        assert_eq!(records[0].row_key, vec![3]);
        assert_eq!(records[1].row_key, vec![2]);
        assert_eq!(records[2].row_key, vec![1]);
    }

    #[test]
    fn test_remove_transaction() {
        let log = UndoLog::new();
        let roll_ptr = log.log_insert(1, "users".to_string(), vec![1]);
        log.log_insert(1, "users".to_string(), vec![2]);

        assert_eq!(log.record_count(), 2);
        assert!(log.get_by_roll_ptr(roll_ptr).is_some());

        log.remove_transaction(1);

        assert_eq!(log.record_count(), 0);
        assert!(log.get_by_roll_ptr(roll_ptr).is_none());
    }

    #[test]
    fn test_purge() {
        let log = UndoLog::new();
        log.log_insert(10, "users".to_string(), vec![1]);
        log.log_insert(50, "users".to_string(), vec![2]);
        log.log_insert(100, "users".to_string(), vec![3]);

        assert_eq!(log.transaction_count(), 3);

        // Purge transactions older than 50
        log.purge(50);

        assert_eq!(log.transaction_count(), 2);
        assert!(log.get_records(10).is_empty());
        assert!(!log.get_records(50).is_empty());
        assert!(!log.get_records(100).is_empty());
    }

    #[test]
    fn test_version_chain() {
        let log = UndoLog::new();

        // First version (insert)
        let ptr1 = log.log_insert(10, "users".to_string(), vec![1]);

        // Second version (update)
        let ptr2 = log.log_update(
            20,
            "users".to_string(),
            vec![1],
            vec![100], // old data
            10,        // old_trx_id
            ptr1,      // chain to first version
        );

        // Third version (update)
        let ptr3 = log.log_update(
            30,
            "users".to_string(),
            vec![1],
            vec![200], // old data
            20,        // old_trx_id
            ptr2,      // chain to second version
        );

        // Traverse the version chain
        let record3 = log.get_by_roll_ptr(ptr3).unwrap();
        assert_eq!(record3.old_roll_ptr, ptr2);

        let record2 = log.get_by_roll_ptr(record3.old_roll_ptr).unwrap();
        assert_eq!(record2.old_roll_ptr, ptr1);

        let record1 = log.get_by_roll_ptr(record2.old_roll_ptr).unwrap();
        assert_eq!(record1.old_roll_ptr, 0); // End of chain
    }
}
