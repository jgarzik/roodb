//! IOCP backend for Windows
//!
//! This module provides an async IO implementation using Windows I/O Completion Ports
//! for high-performance direct IO operations.

use std::ffi::OsStr;
use std::mem::MaybeUninit;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use windows::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FlushFileBuffers, GetFileSizeEx, ReadFile, SetEndOfFile, SetFilePointerEx,
    WriteFile, FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_FLAG_WRITE_THROUGH,
    FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_ALWAYS,
    OPEN_EXISTING, SET_FILE_POINTER_MOVE_METHOD,
};
use windows::Win32::System::IO::{CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED};

use crate::io::aligned_buffer::AlignedBuffer;
use crate::io::error::{IoError, IoResult};
use crate::io::traits::{AsyncIO, AsyncIOFactory, PAGE_SIZE};

/// IOCP-based async IO implementation for Windows
pub struct IocpIO {
    handle: HANDLE,
    iocp: HANDLE,
    inner: Arc<Mutex<IocpInner>>,
}

/// Inner state protected by mutex for concurrent access
struct IocpInner {
    // State for coordinating I/O operations
    // IOCP handles thread-safety, but we need mutex for our overlapped tracking
}

impl IocpIO {
    /// Open a file for IOCP operations
    pub fn open(path: &Path, create: bool) -> IoResult<Self> {
        // Convert path to wide string for Windows API
        let wide_path: Vec<u16> = OsStr::new(path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        let disposition = if create { OPEN_ALWAYS } else { OPEN_EXISTING };

        // Open with direct I/O flags:
        // - FILE_FLAG_NO_BUFFERING: Bypass OS cache (direct I/O)
        // - FILE_FLAG_OVERLAPPED: Enable async I/O
        // - FILE_FLAG_WRITE_THROUGH: Write directly to disk
        let handle = unsafe {
            CreateFileW(
                windows::core::PCWSTR::from_raw(wide_path.as_ptr()),
                (FILE_GENERIC_READ | FILE_GENERIC_WRITE).0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None, // No security attributes
                disposition,
                FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED | FILE_FLAG_WRITE_THROUGH,
                None, // No template file
            )?
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(IoError::Io(std::io::Error::last_os_error()));
        }

        // Create I/O completion port and associate with file handle
        let iocp = unsafe {
            CreateIoCompletionPort(
                handle,
                HANDLE::default(), // Create new IOCP
                0,                 // Completion key
                0,                 // Number of concurrent threads (0 = system default)
            )?
        };

        if iocp.is_invalid() {
            unsafe { CloseHandle(handle) }.ok();
            return Err(IoError::Io(std::io::Error::last_os_error()));
        }

        Ok(Self {
            handle,
            iocp,
            inner: Arc::new(Mutex::new(IocpInner {})),
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

    /// Create an OVERLAPPED structure for the given offset
    fn create_overlapped(offset: u64) -> OVERLAPPED {
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        overlapped
    }

    /// Wait for I/O completion on the IOCP
    fn wait_for_completion(&self) -> IoResult<u32> {
        let mut bytes_transferred: u32 = 0;
        let mut completion_key: usize = 0;
        let mut overlapped_ptr: *mut OVERLAPPED = ptr::null_mut();

        let result = unsafe {
            GetQueuedCompletionStatus(
                self.iocp,
                &mut bytes_transferred,
                &mut completion_key,
                &mut overlapped_ptr,
                u32::MAX, // INFINITE timeout
            )
        };

        if result.is_err() {
            return Err(IoError::Io(std::io::Error::last_os_error()));
        }

        Ok(bytes_transferred)
    }
}

impl Drop for IocpIO {
    fn drop(&mut self) {
        // Close handles in reverse order of creation
        unsafe {
            let _ = CloseHandle(self.iocp);
            let _ = CloseHandle(self.handle);
        }
    }
}

// SAFETY: IocpIO can be sent between threads because:
// 1. HANDLE is just a pointer-sized value
// 2. All mutable state is protected by Mutex
// 3. Windows IOCP is designed for multi-threaded access
unsafe impl Send for IocpIO {}
unsafe impl Sync for IocpIO {}

#[async_trait]
impl AsyncIO for IocpIO {
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize> {
        Self::check_alignment(buf, offset)?;

        let handle = self.handle;
        let iocp = self.iocp;
        let buf_ptr = buf.as_mut_ptr();
        let buf_capacity = buf.capacity() as u32;

        // Use spawn_blocking to avoid blocking the async runtime
        let bytes_read = tokio::task::spawn_blocking(move || {
            let mut overlapped = Self::create_overlapped(offset);
            let mut bytes_read: u32 = 0;

            // Initiate read operation
            let result = unsafe {
                ReadFile(
                    handle,
                    Some(std::slice::from_raw_parts_mut(
                        buf_ptr,
                        buf_capacity as usize,
                    )),
                    Some(&mut bytes_read),
                    Some(&mut overlapped),
                )
            };

            // For overlapped I/O, ReadFile returns false with ERROR_IO_PENDING
            // if the operation is pending
            if result.is_err() {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(997) {
                    // 997 = ERROR_IO_PENDING
                    return Err(IoError::Io(err));
                }
            }

            // Wait for completion
            let mut bytes_transferred: u32 = 0;
            let mut completion_key: usize = 0;
            let mut overlapped_ptr: *mut OVERLAPPED = ptr::null_mut();

            let wait_result = unsafe {
                GetQueuedCompletionStatus(
                    iocp,
                    &mut bytes_transferred,
                    &mut completion_key,
                    &mut overlapped_ptr,
                    u32::MAX,
                )
            };

            if wait_result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            Ok(bytes_transferred)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

        buf.set_len(bytes_read as usize);
        Ok(bytes_read as usize)
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

        let handle = self.handle;
        let iocp = self.iocp;
        // Copy data to avoid lifetime issues with spawn_blocking
        let data = buf.as_slice()[..write_len].to_vec();

        let bytes_written = tokio::task::spawn_blocking(move || {
            let mut overlapped = Self::create_overlapped(offset);
            let mut bytes_written: u32 = 0;

            // Initiate write operation
            let result = unsafe {
                WriteFile(
                    handle,
                    Some(&data),
                    Some(&mut bytes_written),
                    Some(&mut overlapped),
                )
            };

            // For overlapped I/O, WriteFile returns false with ERROR_IO_PENDING
            if result.is_err() {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(997) {
                    // ERROR_IO_PENDING
                    return Err(IoError::Io(err));
                }
            }

            // Wait for completion
            let mut bytes_transferred: u32 = 0;
            let mut completion_key: usize = 0;
            let mut overlapped_ptr: *mut OVERLAPPED = ptr::null_mut();

            let wait_result = unsafe {
                GetQueuedCompletionStatus(
                    iocp,
                    &mut bytes_transferred,
                    &mut completion_key,
                    &mut overlapped_ptr,
                    u32::MAX,
                )
            };

            if wait_result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            Ok(bytes_transferred)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

        Ok(bytes_written as usize)
    }

    async fn sync(&self) -> IoResult<()> {
        let handle = self.handle;

        tokio::task::spawn_blocking(move || {
            let result = unsafe { FlushFileBuffers(handle) };
            if result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn file_size(&self) -> IoResult<u64> {
        let handle = self.handle;

        tokio::task::spawn_blocking(move || {
            let mut size: i64 = 0;
            let result = unsafe { GetFileSizeEx(handle, &mut size) };
            if result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }
            Ok(size as u64)
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn truncate(&self, size: u64) -> IoResult<()> {
        let handle = self.handle;

        tokio::task::spawn_blocking(move || {
            // Move file pointer to desired size
            let mut new_pos: i64 = 0;
            let result = unsafe {
                SetFilePointerEx(
                    handle,
                    size as i64,
                    Some(&mut new_pos),
                    SET_FILE_POINTER_MOVE_METHOD(0), // FILE_BEGIN
                )
            };
            if result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            // Set end of file at current position
            let result = unsafe { SetEndOfFile(handle) };
            if result.is_err() {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            Ok(())
        })
        .await
        .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
    }

    /// Check if batch operations are optimized
    ///
    /// IOCP supports concurrent I/O operations.
    fn supports_batch(&self) -> bool {
        true
    }

    /// Read multiple regions using parallel IOCP operations
    async fn read_batch(&self, requests: &[(u64, usize)]) -> Vec<IoResult<AlignedBuffer>> {
        // Spawn all tasks first for parallelism
        let handles: Vec<_> = requests
            .iter()
            .map(|&(offset, size)| {
                let handle = self.handle;
                let iocp = self.iocp;

                tokio::spawn(async move {
                    // Validate alignment
                    if !offset.is_multiple_of(PAGE_SIZE as u64) {
                        return Err(IoError::OffsetAlignment {
                            offset,
                            alignment: PAGE_SIZE,
                        });
                    }

                    let mut buf = AlignedBuffer::new(size)?;
                    let buf_ptr = buf.as_mut_ptr();
                    let buf_capacity = buf.capacity() as u32;

                    let bytes_read = tokio::task::spawn_blocking(move || {
                        let mut overlapped = Self::create_overlapped(offset);
                        let mut bytes_read: u32 = 0;

                        let result = unsafe {
                            ReadFile(
                                handle,
                                Some(std::slice::from_raw_parts_mut(
                                    buf_ptr,
                                    buf_capacity as usize,
                                )),
                                Some(&mut bytes_read),
                                Some(&mut overlapped),
                            )
                        };

                        if result.is_err() {
                            let err = std::io::Error::last_os_error();
                            if err.raw_os_error() != Some(997) {
                                return Err(IoError::Io(err));
                            }
                        }

                        let mut bytes_transferred: u32 = 0;
                        let mut completion_key: usize = 0;
                        let mut overlapped_ptr: *mut OVERLAPPED = ptr::null_mut();

                        let wait_result = unsafe {
                            GetQueuedCompletionStatus(
                                iocp,
                                &mut bytes_transferred,
                                &mut completion_key,
                                &mut overlapped_ptr,
                                u32::MAX,
                            )
                        };

                        if wait_result.is_err() {
                            return Err(IoError::Io(std::io::Error::last_os_error()));
                        }

                        Ok(bytes_transferred)
                    })
                    .await
                    .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

                    buf.set_len(bytes_read as usize);
                    Ok(buf)
                })
            })
            .collect();

        // Await all in order
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(IoError::Io(std::io::Error::other(e.to_string())))),
            }
        }
        results
    }

    /// Write multiple regions using parallel IOCP operations
    async fn write_batch(&self, requests: &[(u64, AlignedBuffer)]) -> Vec<IoResult<usize>> {
        // Spawn all tasks first for parallelism
        let handles: Vec<_> = requests
            .iter()
            .map(|(offset, buf)| {
                let handle = self.handle;
                let iocp = self.iocp;
                let offset = *offset;
                // Copy data to avoid lifetime issues
                let write_len = AlignedBuffer::round_up(buf.len());
                let data = buf.as_slice()[..write_len.min(buf.capacity())].to_vec();
                let buf_capacity = buf.capacity();

                tokio::spawn(async move {
                    // Validate alignment
                    if !offset.is_multiple_of(PAGE_SIZE as u64) {
                        return Err(IoError::OffsetAlignment {
                            offset,
                            alignment: PAGE_SIZE,
                        });
                    }

                    if write_len > buf_capacity {
                        return Err(IoError::BufferSize {
                            size: data.len(),
                            alignment: PAGE_SIZE,
                        });
                    }

                    let bytes_written = tokio::task::spawn_blocking(move || {
                        let mut overlapped = Self::create_overlapped(offset);
                        let mut bytes_written: u32 = 0;

                        let result = unsafe {
                            WriteFile(
                                handle,
                                Some(&data),
                                Some(&mut bytes_written),
                                Some(&mut overlapped),
                            )
                        };

                        if result.is_err() {
                            let err = std::io::Error::last_os_error();
                            if err.raw_os_error() != Some(997) {
                                return Err(IoError::Io(err));
                            }
                        }

                        let mut bytes_transferred: u32 = 0;
                        let mut completion_key: usize = 0;
                        let mut overlapped_ptr: *mut OVERLAPPED = ptr::null_mut();

                        let wait_result = unsafe {
                            GetQueuedCompletionStatus(
                                iocp,
                                &mut bytes_transferred,
                                &mut completion_key,
                                &mut overlapped_ptr,
                                u32::MAX,
                            )
                        };

                        if wait_result.is_err() {
                            return Err(IoError::Io(std::io::Error::last_os_error()));
                        }

                        Ok(bytes_transferred)
                    })
                    .await
                    .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))??;

                    Ok(bytes_written as usize)
                })
            })
            .collect();

        // Await all in order
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(IoError::Io(std::io::Error::other(e.to_string())))),
            }
        }
        results
    }
}

/// Factory for creating IocpIO instances
pub struct IocpIOFactory;

#[async_trait]
impl AsyncIOFactory for IocpIOFactory {
    type IO = IocpIO;

    async fn open(&self, path: &Path, create: bool) -> IoResult<Self::IO> {
        IocpIO::open(path, create)
    }
}
