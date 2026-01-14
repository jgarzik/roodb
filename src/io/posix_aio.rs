//! POSIX async IO fallback
//!
//! This module provides an async IO implementation using standard POSIX
//! file operations with tokio's blocking task pool. Used on non-Linux
//! platforms or when io_uring is not available.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::io::aligned_buffer::AlignedBuffer;
use crate::io::error::{IoError, IoResult};
use crate::io::traits::{AsyncIO, AsyncIOFactory, PAGE_SIZE};

/// POSIX-based async IO implementation
///
/// Uses tokio's spawn_blocking for file operations to avoid blocking
/// the async runtime.
pub struct PosixIO {
    file: Arc<Mutex<File>>,
}

impl PosixIO {
    /// Open a file for POSIX IO operations
    pub fn open(path: &Path, create: bool) -> IoResult<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        if create {
            opts.create(true);
        }

        // Note: We don't use O_DIRECT on POSIX fallback since it may not
        // be supported on all platforms (e.g., macOS)

        let file = opts.open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    fn check_alignment(buf: &AlignedBuffer, offset: u64) -> IoResult<()> {
        // Check buffer pointer alignment
        if !(buf.as_ptr() as usize).is_multiple_of(PAGE_SIZE) {
            return Err(IoError::Alignment {
                expected: PAGE_SIZE,
                actual: buf.as_ptr() as usize % PAGE_SIZE,
            });
        }

        // Check offset alignment
        if !offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(IoError::OffsetAlignment {
                offset,
                alignment: PAGE_SIZE,
            });
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncIO for PosixIO {
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize> {
        Self::check_alignment(buf, offset)?;

        let file = self.file.clone();
        let capacity = buf.capacity();

        // We need to read into a temporary buffer in the blocking task
        let result = tokio::task::spawn_blocking(move || {
            let mut file = file.lock();
            file.seek(SeekFrom::Start(offset))?;

            let mut temp = vec![0u8; capacity];
            let bytes_read = file.read(&mut temp)?;

            Ok::<(Vec<u8>, usize), std::io::Error>((temp, bytes_read))
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

        let (temp, bytes_read) = result;
        buf.as_mut_slice()[..bytes_read].copy_from_slice(&temp[..bytes_read]);
        buf.set_len(bytes_read);

        Ok(bytes_read)
    }

    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize> {
        Self::check_alignment(buf, offset)?;

        let file = self.file.clone();
        // Copy the data to avoid lifetime issues with spawn_blocking
        let data = buf.as_slice().to_vec();
        let write_len = data.len();

        tokio::task::spawn_blocking(move || {
            let mut file = file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&data)?;
            Ok::<usize, std::io::Error>(write_len)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
        .map_err(IoError::from)
    }

    async fn sync(&self) -> IoResult<()> {
        let file = self.file.clone();

        tokio::task::spawn_blocking(move || {
            let file = file.lock();
            file.sync_all()
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
        .map_err(IoError::from)
    }

    async fn file_size(&self) -> IoResult<u64> {
        let file = self.file.clone();

        tokio::task::spawn_blocking(move || {
            let file = file.lock();
            file.metadata().map(|m| m.len())
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
        .map_err(IoError::from)
    }

    async fn truncate(&self, size: u64) -> IoResult<()> {
        let file = self.file.clone();

        tokio::task::spawn_blocking(move || {
            let file = file.lock();
            file.set_len(size)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
        .map_err(IoError::from)
    }
}

/// Factory for creating PosixIO instances
pub struct PosixIOFactory;

#[async_trait]
impl AsyncIOFactory for PosixIOFactory {
    type IO = PosixIO;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        PosixIO::open(path, create)
    }
}
