//! Storage engine trait definition

use async_trait::async_trait;

use crate::storage::error::StorageResult;

/// A key-value pair
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// One write in a batch: the key, and the value to store or `None` to delete.
pub type WriteOp = (Vec<u8>, Option<Vec<u8>>);

/// Abstract storage engine interface
///
/// Provides ordered key-value storage with point lookups, range scans,
/// and atomic writes.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Get a value by key
    ///
    /// Returns `None` if the key does not exist or was deleted.
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Like `get`, but answers from what has been flushed to disk only,
    /// ignoring writes still in memory. This is the value a restart would
    /// find.
    async fn get_durable(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Put a key-value pair
    ///
    /// Overwrites any existing value for the key.
    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()>;

    /// Delete a key
    ///
    /// No-op if the key does not exist.
    async fn delete(&self, key: &[u8]) -> StorageResult<()>;

    /// Apply several writes as one unit: `Some` stores a value, `None`
    /// deletes the key. Readers may see the writes arrive one by one, but
    /// what reaches disk holds either all of a batch or none of it.
    async fn write_batch(&self, batch: Vec<WriteOp>) -> StorageResult<()>;

    /// Scan a range of keys
    ///
    /// Returns key-value pairs in sorted order where `start <= key < end`.
    /// If `start` is `None`, scan from the beginning.
    /// If `end` is `None`, scan to the end.
    async fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> StorageResult<Vec<KeyValue>>;

    /// The smallest key greater than `key` and less than `high`, if any.
    ///
    /// The default reads the whole range; an engine that can seek should
    /// override it.
    async fn next_key(&self, key: &[u8], high: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let mut after = key.to_vec();
        after.push(0);
        Ok(self
            .scan(Some(&after), Some(high))
            .await?
            .into_iter()
            .next()
            .map(|(k, _)| k))
    }

    /// The greatest key less than `key` and not less than `low`, if any.
    ///
    /// The default reads the whole range; an engine that can seek should
    /// override it.
    async fn prev_key(&self, key: &[u8], low: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.scan(Some(low), Some(key)).await?.pop().map(|(k, _)| k))
    }

    /// Flush all pending writes to disk
    async fn flush(&self) -> StorageResult<()>;

    /// Flush with Critical urgency for durability-critical operations
    ///
    /// Used for Raft vote persistence and other operations where the
    /// sync must complete before returning to ensure durability.
    /// Default implementation calls `flush()`.
    async fn flush_critical(&self) -> StorageResult<()> {
        self.flush().await
    }

    /// Close the storage engine
    async fn close(&self) -> StorageResult<()>;
}
