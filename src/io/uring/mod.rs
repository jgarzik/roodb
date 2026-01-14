//! io_uring backend for Linux
//!
//! This module provides an async IO implementation using Linux's io_uring
//! interface for high-performance direct IO operations.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;

use crate::io::aligned_buffer::AlignedBuffer;
use crate::io::error::{IoError, IoResult};
use crate::io::traits::{AsyncIO, AsyncIOFactory, PAGE_SIZE};

/// io_uring-based async IO implementation
pub struct UringIO {
    file: File,
    ring: Arc<Mutex<IoUring>>,
}

impl UringIO {
    /// Open a file for io_uring operations
    pub fn open(path: &Path, create: bool) -> IoResult<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        if create {
            opts.create(true);
        }

        // Enable direct IO (O_DIRECT)
        opts.custom_flags(libc::O_DIRECT);

        let file = opts.open(path)?;

        // Create io_uring instance with reasonable queue depth
        let ring = IoUring::new(64)?;

        Ok(Self {
            file,
            ring: Arc::new(Mutex::new(ring)),
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
impl AsyncIO for UringIO {
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize> {
        Self::check_alignment(buf, offset)?;

        let fd = types::Fd(self.file.as_raw_fd());
        let read_e = opcode::Read::new(fd, buf.as_mut_ptr(), buf.capacity() as u32)
            .offset(offset)
            .build()
            .user_data(0x42);

        let mut ring = self.ring.lock();

        // Safety: the buffer lives until completion
        unsafe {
            ring.submission().push(&read_e).map_err(|_| {
                IoError::Io(std::io::Error::other("io_uring submission queue full"))
            })?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring.completion().next().ok_or_else(|| {
            IoError::Io(std::io::Error::other("io_uring completion missing"))
        })?;

        let result = cqe.result();
        if result < 0 {
            return Err(IoError::Io(std::io::Error::from_raw_os_error(-result)));
        }

        let bytes_read = result as usize;
        buf.set_len(bytes_read);
        Ok(bytes_read)
    }

    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize> {
        Self::check_alignment(buf, offset)?;

        // For direct IO, we need to write aligned sizes
        let write_len = AlignedBuffer::round_up(buf.len());
        if write_len > buf.capacity() {
            return Err(IoError::BufferSize {
                size: buf.len(),
                alignment: PAGE_SIZE,
            });
        }

        let fd = types::Fd(self.file.as_raw_fd());
        let write_e = opcode::Write::new(fd, buf.as_ptr(), write_len as u32)
            .offset(offset)
            .build()
            .user_data(0x43);

        let mut ring = self.ring.lock();

        // Safety: the buffer lives until completion
        unsafe {
            ring.submission().push(&write_e).map_err(|_| {
                IoError::Io(std::io::Error::other("io_uring submission queue full"))
            })?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring.completion().next().ok_or_else(|| {
            IoError::Io(std::io::Error::other("io_uring completion missing"))
        })?;

        let result = cqe.result();
        if result < 0 {
            return Err(IoError::Io(std::io::Error::from_raw_os_error(-result)));
        }

        Ok(result as usize)
    }

    async fn sync(&self) -> IoResult<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let fsync_e = opcode::Fsync::new(fd).build().user_data(0x44);

        let mut ring = self.ring.lock();

        unsafe {
            ring.submission().push(&fsync_e).map_err(|_| {
                IoError::Io(std::io::Error::other("io_uring submission queue full"))
            })?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring.completion().next().ok_or_else(|| {
            IoError::Io(std::io::Error::other("io_uring completion missing"))
        })?;

        let result = cqe.result();
        if result < 0 {
            return Err(IoError::Io(std::io::Error::from_raw_os_error(-result)));
        }

        Ok(())
    }

    async fn file_size(&self) -> IoResult<u64> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    async fn truncate(&self, size: u64) -> IoResult<()> {
        self.file.set_len(size)?;
        Ok(())
    }
}

/// Factory for creating UringIO instances
pub struct UringIOFactory;

#[async_trait]
impl AsyncIOFactory for UringIOFactory {
    type IO = UringIO;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        UringIO::open(path, create)
    }
}
