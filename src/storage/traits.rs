//! Storage engine trait definition

use async_trait::async_trait;

use crate::storage::error::StorageResult;

/// A key-value pair
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// One write in a batch: the key, and the value to store or `None` to delete.
pub type WriteOp = (Vec<u8>, Option<Vec<u8>>);

/// A view of the engine's contents as they were at one moment, unaffected
/// by what is written afterwards. Used to copy a whole database to another
/// node while this one goes on working.
#[async_trait]
pub trait SnapshotView: Send + Sync {
    /// The value of `key` in the view.
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Entries with keys greater than `after` (from the start if `None`),
    /// in key order, about `max_bytes` of them. The flag says whether these
    /// were the last.
    async fn next_chunk(
        &self,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> StorageResult<(Vec<KeyValue>, bool)>;
}

/// A complete replacement for the engine's contents, being written.
/// Nothing changes until `commit`; dropped before that, it leaves no trace.
#[async_trait]
pub trait Ingest: Send {
    /// Add entries. Across all calls they must come in ascending key order.
    async fn add(&mut self, entries: Vec<KeyValue>) -> StorageResult<()>;

    /// Replace everything the engine holds with what was added, atomically:
    /// readers see all of the old contents or all of the new, and so does a
    /// restart after a crash.
    async fn commit(self: Box<Self>) -> StorageResult<()>;
}

/// A view that is simply a copy of everything, for engines without a
/// cheaper way to hold a moment still.
struct CopiedView(Vec<KeyValue>);

#[async_trait]
impl SnapshotView for CopiedView {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .0
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|i| self.0[i].1.clone()))
    }

    async fn next_chunk(
        &self,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> StorageResult<(Vec<KeyValue>, bool)> {
        let start = after.map_or(0, |after| {
            self.0.partition_point(|(k, _)| k.as_slice() <= after)
        });
        let mut bytes = 0;
        let mut end = start;
        while end < self.0.len() && bytes < max_bytes {
            bytes += self.0[end].0.len() + self.0[end].1.len();
            end += 1;
        }
        Ok((self.0[start..end].to_vec(), end == self.0.len()))
    }
}

/// A replacement collected in memory and applied as one batch, for engines
/// without a cheaper way.
struct BatchIngest<E: StorageEngine + ?Sized> {
    engine: std::sync::Arc<E>,
    entries: Vec<KeyValue>,
}

#[async_trait]
impl<E: StorageEngine + ?Sized> Ingest for BatchIngest<E> {
    async fn add(&mut self, mut entries: Vec<KeyValue>) -> StorageResult<()> {
        self.entries.append(&mut entries);
        Ok(())
    }

    async fn commit(self: Box<Self>) -> StorageResult<()> {
        let mut batch: Vec<WriteOp> = self
            .engine
            .scan(None, None)
            .await?
            .into_iter()
            .map(|(key, _)| (key, None))
            .collect();
        batch.extend(
            self.entries
                .into_iter()
                .map(|(key, value)| (key, Some(value))),
        );
        self.engine.write_batch(batch).await
    }
}

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

    /// Forget every key in `[start, end)`, now and for good: the caller
    /// guarantees nothing will be written there again.
    ///
    /// When this returns, no read finds a key of the range any more, nor
    /// does a view taken from now on, and that survives a restart. The space
    /// the keys took is given back in the background. Calling it again with
    /// the same or an overlapping range is harmless. A view taken earlier
    /// is unaffected: it goes on showing the keys it had.
    async fn drop_range(&self, start: &[u8], end: &[u8]) -> StorageResult<()>;

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

    /// A view of what the engine has flushed to disk, as of now. What is
    /// still in memory is not in it: flush first to have everything.
    ///
    /// The default copies everything; an engine with immutable files should
    /// hold on to those instead.
    async fn snapshot_view(&self) -> StorageResult<std::sync::Arc<dyn SnapshotView>> {
        Ok(std::sync::Arc::new(CopiedView(
            self.scan(None, None).await?,
        )))
    }

    /// Start writing a complete replacement for the engine's contents.
    ///
    /// The default collects it in memory.
    async fn ingest(self: std::sync::Arc<Self>) -> StorageResult<Box<dyn Ingest>>
    where
        Self: 'static,
    {
        Ok(Box::new(BatchIngest {
            engine: self,
            entries: Vec::new(),
        }))
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
