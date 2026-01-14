//! WAL segment file management
//!
//! Each segment is a fixed-size file containing WAL records.
//! Segments are named by their starting LSN: `wal_<lsn>.log`

use std::path::{Path, PathBuf};

use crate::io::{AlignedBuffer, AsyncIO, AsyncIOFactory, PAGE_SIZE};
use crate::wal::error::{WalError, WalResult};
use crate::wal::record::{Record, HEADER_SIZE};

/// Default segment size (16MB)
pub const DEFAULT_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// Segment file header size (one page)
pub const SEGMENT_HEADER_SIZE: u64 = PAGE_SIZE as u64;

/// Magic number for segment files
const SEGMENT_MAGIC: u32 = 0x57414C53; // "WALS"

/// Segment header version
const SEGMENT_VERSION: u32 = 1;

/// A WAL segment file
pub struct Segment<IO: AsyncIO> {
    /// Segment ID (starting LSN)
    pub id: u64,
    /// File handle
    io: IO,
    /// Current write offset
    write_offset: u64,
    /// Maximum segment size
    max_size: u64,
    /// Path to the segment file
    path: PathBuf,
}

impl<IO: AsyncIO> Segment<IO> {
    /// Create a new segment
    pub async fn create<F: AsyncIOFactory<IO = IO>>(
        factory: &F,
        dir: &Path,
        id: u64,
        max_size: u64,
    ) -> WalResult<Self> {
        let path = segment_path(dir, id);
        let io = factory.open(&path, true).await?;

        // Write segment header
        let mut header_buf = AlignedBuffer::page()?;
        write_segment_header(&mut header_buf, id);
        io.write_at(&header_buf, 0).await?;
        io.sync().await?;

        Ok(Self {
            id,
            io,
            write_offset: SEGMENT_HEADER_SIZE,
            max_size,
            path,
        })
    }

    /// Open an existing segment
    pub async fn open<F: AsyncIOFactory<IO = IO>>(
        factory: &F,
        dir: &Path,
        id: u64,
        max_size: u64,
    ) -> WalResult<Self> {
        let path = segment_path(dir, id);
        let io = factory.open(&path, false).await?;

        // Read and validate header
        let mut header_buf = AlignedBuffer::page()?;
        io.read_at(&mut header_buf, 0).await?;
        validate_segment_header(&header_buf, id)?;

        // Find the write offset by scanning for the end
        let write_offset = find_write_offset(&io, max_size).await?;

        Ok(Self {
            id,
            io,
            write_offset,
            max_size,
            path,
        })
    }

    /// Append a record to the segment
    ///
    /// Returns the offset where the record was written, or None if segment is full.
    pub async fn append(&mut self, record: &Record) -> WalResult<Option<u64>> {
        let encoded = record.encode();
        let record_size = encoded.len() as u64;

        // Check if we have space
        if self.write_offset + record_size > self.max_size {
            return Ok(None);
        }

        // Align to page boundary for direct IO
        let aligned_offset = self.write_offset & !(PAGE_SIZE as u64 - 1);
        let offset_in_page = (self.write_offset - aligned_offset) as usize;

        // Read the current page if we're not at a page boundary
        let mut buf = AlignedBuffer::page()?;
        if offset_in_page > 0 {
            self.io.read_at(&mut buf, aligned_offset).await?;
        }

        // Copy record data into buffer
        let space_in_page = PAGE_SIZE - offset_in_page;
        if encoded.len() <= space_in_page {
            // Record fits in current page
            buf.as_mut_slice()[offset_in_page..offset_in_page + encoded.len()]
                .copy_from_slice(&encoded);
            buf.set_len(PAGE_SIZE);
            self.io.write_at(&buf, aligned_offset).await?;
        } else {
            // Record spans multiple pages - write first part
            buf.as_mut_slice()[offset_in_page..].copy_from_slice(&encoded[..space_in_page]);
            buf.set_len(PAGE_SIZE);
            self.io.write_at(&buf, aligned_offset).await?;

            // Write remaining data page by page
            let mut remaining = &encoded[space_in_page..];
            let mut page_offset = aligned_offset + PAGE_SIZE as u64;

            while !remaining.is_empty() {
                let mut page_buf = AlignedBuffer::page()?;
                let write_len = remaining.len().min(PAGE_SIZE);
                page_buf.as_mut_slice()[..write_len].copy_from_slice(&remaining[..write_len]);
                page_buf.set_len(PAGE_SIZE);
                self.io.write_at(&page_buf, page_offset).await?;
                remaining = &remaining[write_len..];
                page_offset += PAGE_SIZE as u64;
            }
        }

        let written_at = self.write_offset;
        self.write_offset += record_size;
        Ok(Some(written_at))
    }

    /// Read a record at the given offset
    pub async fn read_at(&self, offset: u64) -> WalResult<Record> {
        // First, read just the header to get the data length
        let aligned_offset = offset & !(PAGE_SIZE as u64 - 1);
        let offset_in_page = (offset - aligned_offset) as usize;

        let mut buf = AlignedBuffer::page()?;
        self.io.read_at(&mut buf, aligned_offset).await?;

        // Check if header is complete in this page
        if offset_in_page + HEADER_SIZE <= PAGE_SIZE {
            let header_bytes = &buf.as_slice()[offset_in_page..offset_in_page + HEADER_SIZE];
            let data_len = u16::from_be_bytes([header_bytes[5], header_bytes[6]]) as usize;
            let total_len = HEADER_SIZE + data_len;

            // Check if entire record fits in this page
            if offset_in_page + total_len <= PAGE_SIZE {
                return Record::decode(&buf.as_slice()[offset_in_page..offset_in_page + total_len]);
            }

            // Record spans pages - read more
            let mut record_buf = vec![0u8; total_len];
            let first_part = PAGE_SIZE - offset_in_page;
            record_buf[..first_part].copy_from_slice(&buf.as_slice()[offset_in_page..]);

            let mut remaining = &mut record_buf[first_part..];
            let mut page_offset = aligned_offset + PAGE_SIZE as u64;

            while !remaining.is_empty() {
                self.io.read_at(&mut buf, page_offset).await?;
                let read_len = remaining.len().min(PAGE_SIZE);
                remaining[..read_len].copy_from_slice(&buf.as_slice()[..read_len]);
                remaining = &mut remaining[read_len..];
                page_offset += PAGE_SIZE as u64;
            }

            return Record::decode(&record_buf);
        }

        // Header spans pages - this is rare but possible
        Err(WalError::CorruptedSegment { offset })
    }

    /// Sync the segment to disk
    pub async fn sync(&self) -> WalResult<()> {
        self.io.sync().await?;
        Ok(())
    }

    /// Get remaining space in the segment
    pub fn remaining_space(&self) -> u64 {
        self.max_size.saturating_sub(self.write_offset)
    }

    /// Check if segment is full
    pub fn is_full(&self) -> bool {
        self.write_offset >= self.max_size
    }

    /// Get the segment file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get current write offset
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }
}

/// Generate segment file path
pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("wal_{:016x}.log", id))
}

/// Write segment header to buffer
fn write_segment_header(buf: &mut AlignedBuffer, id: u64) {
    let slice = buf.as_mut_slice();
    slice[0..4].copy_from_slice(&SEGMENT_MAGIC.to_be_bytes());
    slice[4..8].copy_from_slice(&SEGMENT_VERSION.to_be_bytes());
    slice[8..16].copy_from_slice(&id.to_be_bytes());
    buf.set_len(PAGE_SIZE);
}

/// Validate segment header
fn validate_segment_header(buf: &AlignedBuffer, expected_id: u64) -> WalResult<()> {
    if buf.len() < 16 {
        return Err(WalError::InvalidHeader);
    }

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != SEGMENT_MAGIC {
        return Err(WalError::InvalidHeader);
    }

    let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if version != SEGMENT_VERSION {
        return Err(WalError::InvalidHeader);
    }

    let id = u64::from_be_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);
    if id != expected_id {
        return Err(WalError::InvalidHeader);
    }

    Ok(())
}

/// Find the write offset by scanning for valid records
async fn find_write_offset<IO: AsyncIO>(io: &IO, max_size: u64) -> WalResult<u64> {
    let mut offset = SEGMENT_HEADER_SIZE;
    let mut buf = AlignedBuffer::page()?;

    while offset < max_size {
        let aligned_offset = offset & !(PAGE_SIZE as u64 - 1);
        let offset_in_page = (offset - aligned_offset) as usize;

        if io.read_at(&mut buf, aligned_offset).await? == 0 {
            break;
        }

        // Check for valid record header
        if offset_in_page + HEADER_SIZE > PAGE_SIZE {
            break;
        }

        let header_bytes = &buf.as_slice()[offset_in_page..];
        if header_bytes.len() < HEADER_SIZE {
            break;
        }

        // Check if this looks like a valid record (non-zero type)
        let record_type = header_bytes[4];
        if record_type == 0 || record_type > 3 {
            break;
        }

        let data_len = u16::from_be_bytes([header_bytes[5], header_bytes[6]]) as u64;
        offset += HEADER_SIZE as u64 + data_len;
    }

    Ok(offset)
}
