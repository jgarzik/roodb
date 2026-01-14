//! Page-aligned buffers for direct IO

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use super::error::{IoError, IoResult};
use super::traits::PAGE_SIZE;

/// A buffer aligned to PAGE_SIZE for direct IO operations
///
/// Direct IO requires buffers to be aligned to the filesystem's block size,
/// typically 4KB. This struct ensures proper alignment for all operations.
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
}

// Safety: AlignedBuffer owns its data and can be sent between threads
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Create a new aligned buffer with the given capacity
    ///
    /// Capacity will be rounded up to the nearest PAGE_SIZE multiple.
    pub fn new(capacity: usize) -> IoResult<Self> {
        let capacity = Self::round_up(capacity);
        if capacity == 0 {
            return Err(IoError::BufferSize {
                size: 0,
                alignment: PAGE_SIZE,
            });
        }

        let layout = Layout::from_size_align(capacity, PAGE_SIZE)
            .map_err(|_| IoError::Alignment {
                expected: PAGE_SIZE,
                actual: 0,
            })?;

        // Safety: layout has non-zero size
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            IoError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Failed to allocate aligned buffer",
            ))
        })?;

        Ok(Self {
            ptr,
            len: 0,
            capacity,
        })
    }

    /// Create a buffer with exactly one page
    pub fn page() -> IoResult<Self> {
        Self::new(PAGE_SIZE)
    }

    /// Create a buffer with multiple pages
    pub fn pages(count: usize) -> IoResult<Self> {
        Self::new(count * PAGE_SIZE)
    }

    /// Round up a size to the nearest PAGE_SIZE multiple
    #[inline]
    pub fn round_up(size: usize) -> usize {
        (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
    }

    /// Check if a size is page-aligned
    #[inline]
    pub fn is_aligned(size: usize) -> bool {
        size.is_multiple_of(PAGE_SIZE)
    }

    /// Get the buffer's capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the buffer's current length (data written)
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set the buffer's length
    ///
    /// # Panics
    /// Panics if len > capacity
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity, "len exceeds capacity");
        self.len = len;
    }

    /// Get a pointer to the buffer data
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Get a mutable pointer to the buffer data
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the buffer as a slice (up to len)
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // Safety: ptr is valid for len bytes
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get the buffer as a mutable slice (up to capacity)
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: ptr is valid for capacity bytes
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }

    /// Clear the buffer (set len to 0)
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Fill the buffer with zeros
    pub fn zero(&mut self) {
        // Safety: ptr is valid for capacity bytes
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.capacity);
        }
        self.len = 0;
    }

    /// Copy data into the buffer
    pub fn copy_from_slice(&mut self, data: &[u8]) -> IoResult<()> {
        if data.len() > self.capacity {
            return Err(IoError::BufferSize {
                size: data.len(),
                alignment: self.capacity,
            });
        }

        // Safety: both pointers are valid and don't overlap
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr(), data.len());
        }
        self.len = data.len();
        Ok(())
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, PAGE_SIZE)
            .expect("layout was valid at allocation");
        // Safety: ptr was allocated with this layout
        unsafe {
            alloc::dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        // Return slice up to len, not capacity
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl std::fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("aligned", &true)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        let buf = AlignedBuffer::new(100).unwrap();
        assert!((buf.as_ptr() as usize).is_multiple_of(PAGE_SIZE));
        assert_eq!(buf.capacity(), PAGE_SIZE); // Rounded up
    }

    #[test]
    fn test_round_up() {
        assert_eq!(AlignedBuffer::round_up(0), 0);
        assert_eq!(AlignedBuffer::round_up(1), PAGE_SIZE);
        assert_eq!(AlignedBuffer::round_up(PAGE_SIZE), PAGE_SIZE);
        assert_eq!(AlignedBuffer::round_up(PAGE_SIZE + 1), PAGE_SIZE * 2);
    }

    #[test]
    fn test_copy_from_slice() {
        let mut buf = AlignedBuffer::page().unwrap();
        buf.copy_from_slice(b"hello").unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..5], b"hello");
    }
}
