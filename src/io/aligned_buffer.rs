//! Page-aligned buffers for direct IO
//!
//! Uses safe Rust by over-allocating a Vec and finding an aligned offset within it.

use std::ops::{Deref, DerefMut};

use super::error::{IoError, IoResult};
use super::traits::PAGE_SIZE;

/// A buffer aligned to PAGE_SIZE for direct IO operations
///
/// Direct IO requires buffers to be aligned to the filesystem's block size,
/// typically 4KB. This struct ensures proper alignment by allocating a Vec
/// with extra space and finding an aligned offset within it.
pub struct AlignedBuffer {
    /// Backing storage (over-allocated to guarantee alignment)
    backing: Vec<u8>,
    /// Offset to the first aligned byte within backing
    offset: usize,
    /// Current length of valid data
    len: usize,
    /// Usable capacity (aligned region size)
    capacity: usize,
}

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

        // Allocate extra space to guarantee we can find an aligned start
        let backing = vec![0u8; capacity + PAGE_SIZE - 1];

        // Find first aligned offset within the backing buffer
        let base_addr = backing.as_ptr() as usize;
        let aligned_addr = (base_addr + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let offset = aligned_addr - base_addr;

        Ok(Self {
            backing,
            offset,
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

    /// Get a pointer to the buffer data (aligned)
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.backing[self.offset..].as_ptr()
    }

    /// Get a mutable pointer to the buffer data (aligned)
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.backing[self.offset..].as_mut_ptr()
    }

    /// Get the buffer as a slice (up to len)
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.backing[self.offset..self.offset + self.len]
    }

    /// Get the buffer as a mutable slice (up to capacity)
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.backing[self.offset..self.offset + self.capacity]
    }

    /// Clear the buffer (set len to 0)
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Fill the buffer with zeros
    pub fn zero(&mut self) {
        self.backing[self.offset..self.offset + self.capacity].fill(0);
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

        self.backing[self.offset..self.offset + data.len()].copy_from_slice(data);
        self.len = data.len();
        Ok(())
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
        &mut self.backing[self.offset..self.offset + self.len]
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
