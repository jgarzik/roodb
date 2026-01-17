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

        // SAFETY: This unsafe block is sound because:
        // 1. The buffer `buf` is owned by this function and lives until submit_and_wait() returns
        // 2. submit_and_wait(1) blocks until the I/O operation completes
        // 3. The kernel only accesses the buffer during the I/O operation
        // 4. No aliasing occurs because we hold exclusive access to `buf`
        unsafe {
            ring.submission().push(&read_e).map_err(|_| {
                IoError::Io(std::io::Error::other("io_uring submission queue full"))
            })?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| IoError::Io(std::io::Error::other("io_uring completion missing")))?;

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

        // SAFETY: This unsafe block is sound because:
        // 1. The buffer `buf` is borrowed for the duration of this function call
        // 2. submit_and_wait(1) blocks until the I/O operation completes
        // 3. The kernel only reads from the buffer during the write operation
        // 4. No aliasing occurs because `buf` is immutably borrowed
        unsafe {
            ring.submission().push(&write_e).map_err(|_| {
                IoError::Io(std::io::Error::other("io_uring submission queue full"))
            })?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| IoError::Io(std::io::Error::other("io_uring completion missing")))?;

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

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| IoError::Io(std::io::Error::other("io_uring completion missing")))?;

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

    /// Check if batch operations are optimized
    ///
    /// io_uring supports true batch submission.
    fn supports_batch(&self) -> bool {
        true
    }

    /// Read multiple regions in a single io_uring submission
    ///
    /// Submits all reads to the submission queue and waits for all completions.
    async fn read_batch(&self, requests: &[(u64, usize)]) -> Vec<IoResult<AlignedBuffer>> {
        if requests.is_empty() {
            return Vec::new();
        }

        // Allocate buffers upfront
        let mut buffers: Vec<Option<AlignedBuffer>> = Vec::with_capacity(requests.len());
        for &(_, size) in requests {
            match AlignedBuffer::new(size) {
                Ok(buf) => buffers.push(Some(buf)),
                Err(e) => {
                    // If allocation fails, return early with error for that slot
                    let mut results: Vec<IoResult<AlignedBuffer>> =
                        Vec::with_capacity(requests.len());
                    for _ in 0..buffers.len() {
                        results.push(Err(IoError::NotSupported("allocation failed".into())));
                    }
                    results.push(Err(e));
                    for _ in (buffers.len() + 1)..requests.len() {
                        results.push(Err(IoError::NotSupported("allocation failed".into())));
                    }
                    return results;
                }
            }
        }

        let fd = types::Fd(self.file.as_raw_fd());
        let mut ring = self.ring.lock();

        // Build and submit all read operations
        for (i, (&(offset, _), buf_opt)) in requests.iter().zip(buffers.iter_mut()).enumerate() {
            if let Some(buf) = buf_opt {
                // Validate alignment
                if !offset.is_multiple_of(PAGE_SIZE as u64) {
                    // Mark this one as failed
                    *buf_opt = None;
                    continue;
                }

                let read_e = opcode::Read::new(fd, buf.as_mut_ptr(), buf.capacity() as u32)
                    .offset(offset)
                    .build()
                    .user_data(i as u64);

                // SAFETY: This unsafe block is sound because:
                // 1. Each buffer in the batch is stored in `bufs` which outlives this function
                // 2. submit_and_wait() is called below, blocking until all I/O completes
                // 3. The kernel only accesses buffers during the I/O operations
                // 4. Each buffer has exclusive access (stored in Option for ownership tracking)
                unsafe {
                    if ring.submission().push(&read_e).is_err() {
                        // Queue full - shouldn't happen with proper batching
                        *buf_opt = None;
                    }
                }
            }
        }

        // Submit all and wait for completions
        let submitted = match ring.submit_and_wait(requests.len()) {
            Ok(n) => n,
            Err(e) => {
                let mut results: Vec<IoResult<AlignedBuffer>> = Vec::with_capacity(requests.len());
                for _ in 0..requests.len() {
                    results.push(Err(IoError::Io(std::io::Error::from(e.kind()))));
                }
                return results;
            }
        };

        // Collect results - initialize with placeholder errors
        let mut results: Vec<IoResult<AlignedBuffer>> = Vec::with_capacity(requests.len());
        for _ in 0..requests.len() {
            results.push(Err(IoError::NotSupported("incomplete".into())));
        }

        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx >= requests.len() {
                continue;
            }

            let result = cqe.result();
            if result < 0 {
                results[idx] = Err(IoError::Io(std::io::Error::from_raw_os_error(-result)));
            } else if let Some(mut buf) = buffers[idx].take() {
                buf.set_len(result as usize);
                results[idx] = Ok(buf);
            }
        }

        // Handle any that weren't submitted
        for (i, buf_opt) in buffers.iter().enumerate() {
            if buf_opt.is_some() && matches!(results[i], Err(IoError::NotSupported(_))) {
                results[i] = Err(IoError::OffsetAlignment {
                    offset: requests[i].0,
                    alignment: PAGE_SIZE,
                });
            }
        }

        let _ = submitted; // silence unused warning
        results
    }

    /// Write multiple regions in a single io_uring submission
    ///
    /// Submits all writes to the submission queue and waits for all completions.
    async fn write_batch(&self, requests: &[(u64, AlignedBuffer)]) -> Vec<IoResult<usize>> {
        if requests.is_empty() {
            return Vec::new();
        }

        let fd = types::Fd(self.file.as_raw_fd());
        let mut ring = self.ring.lock();

        let mut valid_indices = Vec::with_capacity(requests.len());
        let mut results: Vec<IoResult<usize>> = Vec::with_capacity(requests.len());
        for _ in 0..requests.len() {
            results.push(Err(IoError::NotSupported("not submitted".into())));
        }

        // Build and submit all write operations
        for (i, (offset, buf)) in requests.iter().enumerate() {
            // Validate alignment
            if !offset.is_multiple_of(PAGE_SIZE as u64) {
                results[i] = Err(IoError::OffsetAlignment {
                    offset: *offset,
                    alignment: PAGE_SIZE,
                });
                continue;
            }

            // For direct IO, round up to page size
            let write_len = AlignedBuffer::round_up(buf.len());
            if write_len > buf.capacity() {
                results[i] = Err(IoError::BufferSize {
                    size: buf.len(),
                    alignment: PAGE_SIZE,
                });
                continue;
            }

            let write_e = opcode::Write::new(fd, buf.as_ptr(), write_len as u32)
                .offset(*offset)
                .build()
                .user_data(i as u64);

            // SAFETY: This unsafe block is sound because:
            // 1. The buffer references in the batch are borrowed from the caller
            // 2. submit_and_wait() is called below, blocking until all I/O completes
            // 3. The kernel only reads from buffers during write operations
            // 4. Buffers are immutably borrowed, preventing concurrent modification
            unsafe {
                if ring.submission().push(&write_e).is_err() {
                    results[i] = Err(IoError::Io(std::io::Error::other("queue full")));
                    continue;
                }
            }
            valid_indices.push(i);
        }

        if valid_indices.is_empty() {
            return results;
        }

        // Submit all and wait for completions
        if let Err(e) = ring.submit_and_wait(valid_indices.len()) {
            for &idx in &valid_indices {
                results[idx] = Err(IoError::Io(e.kind().into()));
            }
            return results;
        }

        // Collect results
        for cqe in ring.completion() {
            let idx = cqe.user_data() as usize;
            if idx >= requests.len() {
                continue;
            }

            let result = cqe.result();
            if result < 0 {
                results[idx] = Err(IoError::Io(std::io::Error::from_raw_os_error(-result)));
            } else {
                results[idx] = Ok(result as usize);
            }
        }

        results
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
