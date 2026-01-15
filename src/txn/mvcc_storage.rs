//! MVCC storage wrapper
//!
//! Wraps the underlying storage engine to provide MVCC semantics:
//! - Row versioning with transaction IDs
//! - Visibility filtering based on read views
//! - Undo log integration for rollback

use std::sync::Arc;

use super::{ReadView, TransactionManager, TransactionResult, UndoType};
use crate::storage::{StorageEngine, StorageResult};

/// A key-value pair for scan results
type KeyValue = (Vec<u8>, Vec<u8>);

/// Row header size: DB_TRX_ID (8 bytes) + DB_ROLL_PTR (8 bytes) + deleted flag (1 byte)
const ROW_HEADER_SIZE: usize = 17;

/// Offset of DB_TRX_ID in row header
const TXN_ID_OFFSET: usize = 0;

/// Offset of DB_ROLL_PTR in row header
const ROLL_PTR_OFFSET: usize = 8;

/// Offset of deleted flag in row header
const DELETED_OFFSET: usize = 16;

/// MVCC-aware storage wrapper
///
/// This wraps the underlying storage engine to provide:
/// - Transaction ID tracking on each row
/// - Version chain pointers via roll_ptr
/// - Visibility filtering based on read views
/// - Integration with undo log for rollback
pub struct MvccStorage {
    /// Underlying storage engine
    inner: Arc<dyn StorageEngine>,

    /// Transaction manager for undo log and visibility
    txn_manager: Arc<TransactionManager>,
}

impl MvccStorage {
    /// Create a new MVCC storage wrapper
    pub fn new(inner: Arc<dyn StorageEngine>, txn_manager: Arc<TransactionManager>) -> Self {
        Self { inner, txn_manager }
    }

    /// Get the underlying storage engine
    pub fn inner(&self) -> &Arc<dyn StorageEngine> {
        &self.inner
    }

    /// Encode a row with MVCC header
    ///
    /// Format: [DB_TRX_ID:8][DB_ROLL_PTR:8][deleted:1][row_data]
    fn encode_row(txn_id: u64, roll_ptr: u64, deleted: bool, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(ROW_HEADER_SIZE + data.len());
        result.extend_from_slice(&txn_id.to_le_bytes());
        result.extend_from_slice(&roll_ptr.to_le_bytes());
        result.push(if deleted { 1 } else { 0 });
        result.extend_from_slice(data);
        result
    }

    /// Decode MVCC header from a row
    ///
    /// Returns (txn_id, roll_ptr, deleted, data)
    fn decode_row(encoded: &[u8]) -> Option<(u64, u64, bool, &[u8])> {
        if encoded.len() < ROW_HEADER_SIZE {
            return None;
        }

        let txn_id = u64::from_le_bytes(encoded[TXN_ID_OFFSET..TXN_ID_OFFSET + 8].try_into().ok()?);
        let roll_ptr = u64::from_le_bytes(
            encoded[ROLL_PTR_OFFSET..ROLL_PTR_OFFSET + 8]
                .try_into()
                .ok()?,
        );
        let deleted = encoded[DELETED_OFFSET] != 0;
        let data = &encoded[ROW_HEADER_SIZE..];

        Some((txn_id, roll_ptr, deleted, data))
    }

    /// Get a row, returning the visible version for the given read view
    ///
    /// Traverses the version chain if needed to find a visible version.
    pub async fn get(&self, key: &[u8], read_view: &ReadView) -> StorageResult<Option<Vec<u8>>> {
        let encoded = match self.inner.get(key).await? {
            Some(data) => data,
            None => return Ok(None),
        };

        self.find_visible_version(&encoded, read_view).await
    }

    /// Find the visible version of a row, traversing version chain if needed
    async fn find_visible_version(
        &self,
        encoded: &[u8],
        read_view: &ReadView,
    ) -> StorageResult<Option<Vec<u8>>> {
        let (txn_id, roll_ptr, deleted, data) = match Self::decode_row(encoded) {
            Some(decoded) => decoded,
            None => {
                // Legacy row without MVCC header - treat as always visible
                return Ok(Some(encoded.to_vec()));
            }
        };

        // Check if this version is visible
        if read_view.is_visible(txn_id) {
            // If visible and deleted, the row doesn't exist for this view
            if deleted {
                // But we need to check the version chain for an older visible version
                return self.find_older_visible_version(roll_ptr, read_view).await;
            }
            return Ok(Some(data.to_vec()));
        }

        // This version is not visible, traverse the version chain
        self.find_older_visible_version(roll_ptr, read_view).await
    }

    /// Traverse the undo log version chain to find a visible version
    async fn find_older_visible_version(
        &self,
        roll_ptr: u64,
        read_view: &ReadView,
    ) -> StorageResult<Option<Vec<u8>>> {
        if roll_ptr == 0 {
            // End of version chain - no visible version
            return Ok(None);
        }

        // Get the undo record
        let undo_record = match self.txn_manager.undo_log().get_by_roll_ptr(roll_ptr) {
            Some(record) => record,
            None => {
                // Undo record was purged - no older versions available
                return Ok(None);
            }
        };

        // Check if this older version is visible
        if read_view.is_visible(undo_record.old_trx_id) {
            match undo_record.undo_type {
                UndoType::Insert => {
                    // Row didn't exist before this insert
                    return Ok(None);
                }
                UndoType::Update | UndoType::Delete => {
                    // Return the old data
                    return Ok(undo_record.old_data.clone());
                }
            }
        }

        // This version also not visible, continue traversing
        Box::pin(self.find_older_visible_version(undo_record.old_roll_ptr, read_view)).await
    }

    /// Put a row with MVCC tracking
    ///
    /// This creates a new version of the row and logs to the undo log.
    pub async fn put(&self, key: &[u8], value: &[u8], txn_id: u64) -> TransactionResult<()> {
        // Check if row exists (for undo log)
        let existing = self.inner.get(key).await?;

        let roll_ptr = if let Some(ref encoded) = existing {
            if let Some((old_txn_id, old_roll_ptr, _deleted, old_data)) = Self::decode_row(encoded)
            {
                // Log update to undo log
                self.txn_manager.undo_log().log_update(
                    txn_id,
                    String::new(), // Table name not needed for basic rollback
                    key.to_vec(),
                    old_data.to_vec(),
                    old_txn_id,
                    old_roll_ptr,
                )
            } else {
                // Legacy row - log as update with current data
                self.txn_manager.undo_log().log_update(
                    txn_id,
                    String::new(),
                    key.to_vec(),
                    encoded.clone(),
                    0,
                    0,
                )
            }
        } else {
            // New row - log insert
            self.txn_manager
                .undo_log()
                .log_insert(txn_id, String::new(), key.to_vec())
        };

        // Write the new version
        let encoded = Self::encode_row(txn_id, roll_ptr, false, value);
        self.inner.put(key, &encoded).await?;

        Ok(())
    }

    /// Delete a row with MVCC tracking
    ///
    /// This marks the row as deleted rather than removing it,
    /// allowing older transactions to still see it.
    pub async fn delete(&self, key: &[u8], txn_id: u64) -> TransactionResult<()> {
        // Get existing row
        let existing = match self.inner.get(key).await? {
            Some(data) => data,
            None => return Ok(()), // Row doesn't exist, nothing to delete
        };

        let (old_txn_id, old_roll_ptr, old_data) =
            if let Some((tid, rp, _deleted, data)) = Self::decode_row(&existing) {
                (tid, rp, data.to_vec())
            } else {
                // Legacy row
                (0, 0, existing.clone())
            };

        // Log delete to undo log
        let roll_ptr = self.txn_manager.undo_log().log_delete(
            txn_id,
            String::new(),
            key.to_vec(),
            old_data.clone(),
            old_txn_id,
            old_roll_ptr,
        );

        // Write tombstone (deleted flag = true, empty data)
        let encoded = Self::encode_row(txn_id, roll_ptr, true, &[]);
        self.inner.put(key, &encoded).await?;

        Ok(())
    }

    /// Scan a range, filtering by visibility
    pub async fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_view: &ReadView,
    ) -> StorageResult<Vec<KeyValue>> {
        let all_rows = self.inner.scan(start, end).await?;

        let mut visible_rows = Vec::new();
        for (key, value) in all_rows {
            if let Some(data) = self.find_visible_version(&value, read_view).await? {
                visible_rows.push((key, data));
            }
        }

        Ok(visible_rows)
    }

    /// Put a row without MVCC tracking (for bootstrap/system tables)
    pub async fn put_raw(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.inner.put(key, value).await
    }

    /// Get a row without MVCC decoding (for bootstrap/system tables)
    pub async fn get_raw(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    /// Flush to storage
    pub async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_encode_decode_row() {
        let data = b"hello world";
        let encoded = MvccStorage::encode_row(42, 100, false, data);

        let (txn_id, roll_ptr, deleted, decoded_data) = MvccStorage::decode_row(&encoded).unwrap();

        assert_eq!(txn_id, 42);
        assert_eq!(roll_ptr, 100);
        assert!(!deleted);
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn test_encode_decode_deleted() {
        let encoded = MvccStorage::encode_row(42, 100, true, &[]);

        let (txn_id, roll_ptr, deleted, data) = MvccStorage::decode_row(&encoded).unwrap();

        assert_eq!(txn_id, 42);
        assert_eq!(roll_ptr, 100);
        assert!(deleted);
        assert!(data.is_empty());
    }

    #[test]
    fn test_decode_short_data() {
        let short = vec![1, 2, 3]; // Less than header size
        assert!(MvccStorage::decode_row(&short).is_none());
    }

    #[test]
    fn test_read_view_visibility() {
        // Test that visibility logic is correct
        let view = ReadView::new(100, HashSet::from([50, 75]), 100);

        // Own transaction
        assert!(view.is_visible(100));

        // Old committed
        assert!(view.is_visible(10));
        assert!(view.is_visible(49));

        // Active at snapshot time
        assert!(!view.is_visible(50));
        assert!(!view.is_visible(75));

        // Committed between active txns
        assert!(view.is_visible(60));
        assert!(view.is_visible(90));

        // Future
        assert!(!view.is_visible(101));
    }
}
