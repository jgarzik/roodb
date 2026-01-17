//! Async IO traits for database storage

use async_trait::async_trait;

use super::aligned_buffer::AlignedBuffer;
use super::error::IoResult;
use super::scheduler::{IoContext, IoPriority};

/// Page size for direct IO alignment (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Large chunk size for bulk I/O (2MB)
pub const LARGE_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Async IO trait for file operations
///
/// All implementations must support direct IO with proper alignment:
/// - Buffers must be aligned to PAGE_SIZE
/// - File offsets must be aligned to PAGE_SIZE
/// - Read/write sizes must be multiples of PAGE_SIZE
#[async_trait]
pub trait AsyncIO: Send + Sync {
    /// Read data at the specified offset into the buffer
    ///
    /// The buffer must be page-aligned and the offset must be page-aligned.
    /// Returns the number of bytes read.
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize>;

    /// Write data from the buffer at the specified offset
    ///
    /// The buffer must be page-aligned and the offset must be page-aligned.
    /// Returns the number of bytes written.
    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize>;

    /// Sync all pending writes to disk
    async fn sync(&self) -> IoResult<()>;

    /// Get the current file size
    async fn file_size(&self) -> IoResult<u64>;

    /// Truncate or extend the file to the specified size
    async fn truncate(&self, size: u64) -> IoResult<()>;

    // --- Extended methods with default implementations ---

    /// Read data with priority hint
    ///
    /// Default implementation ignores priority and calls read_at.
    /// Implementations may use priority for scheduling decisions.
    async fn read_at_priority(
        &self,
        buf: &mut AlignedBuffer,
        offset: u64,
        _priority: IoPriority,
    ) -> IoResult<usize> {
        self.read_at(buf, offset).await
    }

    /// Write data with priority hint
    ///
    /// Default implementation ignores priority and calls write_at.
    /// Implementations may use priority for scheduling decisions.
    async fn write_at_priority(
        &self,
        buf: &AlignedBuffer,
        offset: u64,
        _priority: IoPriority,
    ) -> IoResult<usize> {
        self.write_at(buf, offset).await
    }

    /// Sync with full IoContext
    ///
    /// Default implementation ignores context and calls sync().
    /// ScheduledHandle overrides this for priority-based sync.
    async fn sync_with_context(&self, _ctx: IoContext) -> IoResult<()> {
        self.sync().await
    }

    /// Check if this I/O implementation supports batch operations
    ///
    /// Returns true if read_batch/write_batch are optimized.
    fn supports_batch(&self) -> bool {
        false
    }

    /// Read multiple regions in a batch
    ///
    /// Default implementation performs sequential reads.
    /// Implementations with batch support (io_uring, POSIX AIO) can optimize.
    ///
    /// Returns a Vec of results in the same order as requests.
    async fn read_batch(
        &self,
        requests: &[(u64, usize)], // (offset, size)
    ) -> Vec<IoResult<AlignedBuffer>> {
        let mut results = Vec::with_capacity(requests.len());
        for &(offset, size) in requests {
            let result = async {
                let mut buf = AlignedBuffer::new(size)?;
                self.read_at(&mut buf, offset).await?;
                Ok(buf)
            }
            .await;
            results.push(result);
        }
        results
    }

    /// Write multiple regions in a batch
    ///
    /// Default implementation performs sequential writes.
    /// Implementations with batch support can optimize.
    ///
    /// Returns a Vec of results in the same order as buffers.
    async fn write_batch(
        &self,
        requests: &[(u64, AlignedBuffer)], // (offset, data) - Note: takes references
    ) -> Vec<IoResult<usize>> {
        let mut results = Vec::with_capacity(requests.len());
        for (offset, buf) in requests {
            let result = self.write_at(buf, *offset).await;
            results.push(result);
        }
        results
    }
}

/// Factory for creating AsyncIO instances
#[async_trait]
pub trait AsyncIOFactory: Send + Sync {
    /// The type of AsyncIO this factory produces
    type IO: AsyncIO;

    /// Open or create a file for async IO
    async fn open(&self, path: &std::path::Path, create: bool) -> IoResult<Self::IO>;
}
