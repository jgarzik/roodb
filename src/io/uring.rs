//! io_uring backend for Linux
//!
//! This module provides an async IO implementation using Linux's io_uring
//! interface for high-performance direct IO operations.
//!
//! All blocking io_uring operations (`submit_and_wait`) are offloaded to
//! Tokio's blocking thread pool via `spawn_blocking`, preventing Tokio
//! worker threads from being stalled by I/O.

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
///
/// Each instance owns its own io_uring ring and file descriptor.
/// The ring mutex serializes operations on the same file handle,
/// while `spawn_blocking` ensures Tokio worker threads are never blocked.
pub struct UringIO {
    file: File,
    ring: Arc<Mutex<IoUring>>,
}

// SAFETY: UringIO is Send+Sync because:
// - File is Send+Sync
// - Arc<Mutex<IoUring>> is Send+Sync
// - All io_uring operations are serialized by the Mutex
unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

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

        let ring = self.ring.clone();
        let fd = self.file.as_raw_fd();
        let capacity = buf.capacity();

        // Offload blocking io_uring submit_and_wait to the blocking thread pool
        let result_buf = tokio::task::spawn_blocking(move || {
            let mut tmp_buf = AlignedBuffer::new(capacity)?;
            let fd = types::Fd(fd);
            let read_e = opcode::Read::new(fd, tmp_buf.as_mut_ptr(), capacity as u32)
                .offset(offset)
                .build()
                .user_data(0x42);

            let mut ring = ring.lock();

            // SAFETY: tmp_buf lives on this thread's stack until submit_and_wait returns.
            // The kernel only accesses the buffer during the I/O operation.
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
            tmp_buf.set_len(bytes_read);
            Ok(tmp_buf)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

        let bytes_read = result_buf.len();
        buf.as_mut_slice()[..bytes_read].copy_from_slice(result_buf.as_slice());
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

        let ring = self.ring.clone();
        let fd = self.file.as_raw_fd();

        // Copy data for ownership in blocking task (zero-padded to alignment)
        let mut data = vec![0u8; write_len];
        data[..buf.len()].copy_from_slice(buf.as_slice());

        tokio::task::spawn_blocking(move || {
            let mut tmp_buf = AlignedBuffer::new(write_len)?;
            tmp_buf.copy_from_slice(&data)?;

            let fd = types::Fd(fd);
            let write_e = opcode::Write::new(fd, tmp_buf.as_ptr(), write_len as u32)
                .offset(offset)
                .build()
                .user_data(0x43);

            let mut ring = ring.lock();

            // SAFETY: tmp_buf lives on this thread's stack until submit_and_wait returns.
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
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn sync(&self) -> IoResult<()> {
        let ring = self.ring.clone();
        let fd = self.file.as_raw_fd();

        tokio::task::spawn_blocking(move || {
            let fd = types::Fd(fd);
            let fsync_e = opcode::Fsync::new(fd).build().user_data(0x44);

            let mut ring = ring.lock();

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
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
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
    /// All reads are submitted as a batch and awaited via a single
    /// `submit_and_wait` call inside `spawn_blocking`.
    async fn read_batch(&self, requests: &[(u64, usize)]) -> Vec<IoResult<AlignedBuffer>> {
        if requests.is_empty() {
            return Vec::new();
        }

        let ring = self.ring.clone();
        let fd = self.file.as_raw_fd();
        let requests_owned: Vec<(u64, usize)> = requests.to_vec();

        match tokio::task::spawn_blocking(move || {
            read_batch_blocking(ring, fd, &requests_owned)
        })
        .await
        {
            Ok(results) => results,
            Err(e) => {
                let mut results = Vec::with_capacity(requests.len());
                for _ in 0..requests.len() {
                    results.push(Err(IoError::Io(std::io::Error::other(e.to_string()))));
                }
                results
            }
        }
    }

    /// Write multiple regions in a single io_uring submission
    ///
    /// All writes are submitted as a batch and awaited via a single
    /// `submit_and_wait` call inside `spawn_blocking`.
    async fn write_batch(&self, requests: &[(u64, AlignedBuffer)]) -> Vec<IoResult<usize>> {
        if requests.is_empty() {
            return Vec::new();
        }

        let ring = self.ring.clone();
        let fd = self.file.as_raw_fd();

        // Copy buffer data for ownership in the blocking task
        let owned_requests: Vec<(u64, Vec<u8>, usize)> = requests
            .iter()
            .map(|(offset, buf)| {
                let write_len = AlignedBuffer::round_up(buf.len());
                let mut data = vec![0u8; write_len];
                data[..buf.len()].copy_from_slice(buf.as_slice());
                (*offset, data, write_len)
            })
            .collect();

        match tokio::task::spawn_blocking(move || {
            write_batch_blocking(ring, fd, &owned_requests)
        })
        .await
        {
            Ok(results) => results,
            Err(e) => {
                let mut results = Vec::with_capacity(requests.len());
                for _ in 0..requests.len() {
                    results.push(Err(IoError::Io(std::io::Error::other(e.to_string()))));
                }
                results
            }
        }
    }
}

/// Execute batch reads synchronously (called from spawn_blocking)
fn read_batch_blocking(
    ring: Arc<Mutex<IoUring>>,
    raw_fd: i32,
    requests: &[(u64, usize)],
) -> Vec<IoResult<AlignedBuffer>> {
    // Allocate buffers upfront
    let mut buffers: Vec<Option<AlignedBuffer>> = Vec::with_capacity(requests.len());
    for &(_, size) in requests {
        match AlignedBuffer::new(size) {
            Ok(buf) => buffers.push(Some(buf)),
            Err(e) => {
                let mut results: Vec<IoResult<AlignedBuffer>> = Vec::with_capacity(requests.len());
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

    let fd = types::Fd(raw_fd);
    let mut ring = ring.lock();

    // Build and submit all read operations
    for (i, (&(offset, _), buf_opt)) in requests.iter().zip(buffers.iter_mut()).enumerate() {
        if let Some(buf) = buf_opt {
            if !offset.is_multiple_of(PAGE_SIZE as u64) {
                *buf_opt = None;
                continue;
            }

            let read_e = opcode::Read::new(fd, buf.as_mut_ptr(), buf.capacity() as u32)
                .offset(offset)
                .build()
                .user_data(i as u64);

            // SAFETY: Each buffer in buffers outlives submit_and_wait below.
            unsafe {
                if ring.submission().push(&read_e).is_err() {
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

    // Collect results
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

    let _ = submitted;
    results
}

/// Execute batch writes synchronously (called from spawn_blocking)
fn write_batch_blocking(
    ring: Arc<Mutex<IoUring>>,
    raw_fd: i32,
    requests: &[(u64, Vec<u8>, usize)], // (offset, data, write_len)
) -> Vec<IoResult<usize>> {
    let fd = types::Fd(raw_fd);
    let num_requests = requests.len();

    // Create aligned buffers from owned data
    let mut aligned_bufs: Vec<Option<AlignedBuffer>> = Vec::with_capacity(num_requests);
    let mut results: Vec<IoResult<usize>> = Vec::with_capacity(num_requests);
    for _ in 0..num_requests {
        results.push(Err(IoError::NotSupported("not submitted".into())));
    }

    for (i, (offset, data, write_len)) in requests.iter().enumerate() {
        if !offset.is_multiple_of(PAGE_SIZE as u64) {
            results[i] = Err(IoError::OffsetAlignment {
                offset: *offset,
                alignment: PAGE_SIZE,
            });
            aligned_bufs.push(None);
            continue;
        }

        match AlignedBuffer::new(*write_len) {
            Ok(mut buf) => {
                if buf.copy_from_slice(data).is_err() {
                    results[i] = Err(IoError::BufferSize {
                        size: data.len(),
                        alignment: PAGE_SIZE,
                    });
                    aligned_bufs.push(None);
                    continue;
                }
                aligned_bufs.push(Some(buf));
            }
            Err(e) => {
                results[i] = Err(e);
                aligned_bufs.push(None);
            }
        }
    }

    let mut ring = ring.lock();
    let mut valid_indices = Vec::with_capacity(num_requests);

    for (i, (offset, _, write_len)) in requests.iter().enumerate() {
        if let Some(ref buf) = aligned_bufs[i] {
            let write_e = opcode::Write::new(fd, buf.as_ptr(), *write_len as u32)
                .offset(*offset)
                .build()
                .user_data(i as u64);

            // SAFETY: aligned_bufs outlive submit_and_wait below.
            unsafe {
                if ring.submission().push(&write_e).is_err() {
                    results[i] = Err(IoError::Io(std::io::Error::other("queue full")));
                    continue;
                }
            }
            valid_indices.push(i);
        }
    }

    if valid_indices.is_empty() {
        return results;
    }

    if let Err(e) = ring.submit_and_wait(valid_indices.len()) {
        for &idx in &valid_indices {
            results[idx] = Err(IoError::Io(e.kind().into()));
        }
        return results;
    }

    for cqe in ring.completion() {
        let idx = cqe.user_data() as usize;
        if idx >= num_requests {
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

/// Factory for creating UringIO instances
pub struct UringIOFactory;

#[async_trait]
impl AsyncIOFactory for UringIOFactory {
    type IO = UringIO;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        UringIO::open(path, create)
    }
}
