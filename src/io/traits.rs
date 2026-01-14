//! Async IO traits for database storage

use async_trait::async_trait;

use super::aligned_buffer::AlignedBuffer;
use super::error::IoResult;

/// Page size for direct IO alignment (4KB)
pub const PAGE_SIZE: usize = 4096;

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
}

/// Factory for creating AsyncIO instances
#[async_trait]
pub trait AsyncIOFactory: Send + Sync {
    /// The type of AsyncIO this factory produces
    type IO: AsyncIO;

    /// Open or create a file for async IO
    async fn open(&self, path: &std::path::Path, create: bool) -> IoResult<Self::IO>;
}
