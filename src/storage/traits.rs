//! Storage engine trait definition

use async_trait::async_trait;

use crate::storage::error::StorageResult;

/// A key-value pair
pub type KeyValue = (Vec<u8>, Vec<u8>);

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

    /// Put a key-value pair
    ///
    /// Overwrites any existing value for the key.
    async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()>;

    /// Delete a key
    ///
    /// No-op if the key does not exist.
    async fn delete(&self, key: &[u8]) -> StorageResult<()>;

    /// Scan a range of keys
    ///
    /// Returns key-value pairs in sorted order where `start <= key < end`.
    /// If `start` is `None`, scan from the beginning.
    /// If `end` is `None`, scan to the end.
    async fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> StorageResult<Vec<KeyValue>>;

    /// Flush all pending writes to disk
    async fn flush(&self) -> StorageResult<()>;

    /// Close the storage engine
    async fn close(&self) -> StorageResult<()>;
}
