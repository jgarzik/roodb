//! Page-aligned buffers for direct I/O.
//!
//! Alignment is obtained in safe Rust by over-allocating a `Vec` and using the
//! first aligned address inside it.
//!
//! Buffers are recycled: dropping one returns its memory to a process-wide
//! pool, and the next request for that size class takes it back. I/O-heavy
//! paths therefore do not hit the allocator, or touch fresh pages, per request.
//! A buffer from the pool holds whatever its previous user left in it.

use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::error::{IoError, IoResult};

/// Unit of alignment for direct I/O: buffer addresses, transfer sizes and file
/// offsets are all multiples of this.
pub const PAGE_SIZE: usize = 4096;

/// Size classes are powers of two pages, from one page to 2 MiB. Larger
/// buffers are allocated and freed directly.
const CLASSES: usize = 10;
/// The pool is split into shards to keep threads off each other's locks.
const SHARDS: usize = 16;
/// Idle memory a shard may hold. Beyond it, dropped buffers are freed.
const SHARD_BUDGET: usize = 8 << 20;

struct Shard {
    classes: [Mutex<Vec<Vec<u8>>>; CLASSES],
    bytes: AtomicUsize,
}

static POOL: [Shard; SHARDS] = [const {
    Shard {
        classes: [const { Mutex::new(Vec::new()) }; CLASSES],
        bytes: AtomicUsize::new(0),
    }
}; SHARDS];
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
static POOL_HITS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static MY_SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn my_shard() -> &'static Shard {
    let index = MY_SHARD.with(|cell| {
        if cell.get() == usize::MAX {
            cell.set(NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS);
        }
        cell.get()
    });
    &POOL[index]
}

/// The size class holding `capacity` bytes, if it is small enough to pool.
fn class_of(capacity: usize) -> Option<usize> {
    let pages = capacity / PAGE_SIZE;
    let class = pages.next_power_of_two().trailing_zeros() as usize;
    (class < CLASSES).then_some(class)
}

/// A buffer aligned to PAGE_SIZE for direct I/O.
///
/// `capacity` is the usable size and `len` the amount of valid data. The
/// bytes between the two are initialised but unspecified.
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
    /// Get a buffer with at least the given capacity, rounded up to a whole
    /// number of pages. The contents are unspecified; use [`Self::zero`] if
    /// zeros are needed.
    pub fn new(capacity: usize) -> IoResult<Self> {
        let capacity = Self::round_up(capacity);
        if capacity == 0 {
            return Err(IoError::BufferSize {
                size: 0,
                alignment: PAGE_SIZE,
            });
        }

        let backing = match class_of(capacity) {
            Some(class) => {
                let shard = my_shard();
                let recycled = shard.classes[class].lock().pop();
                match recycled {
                    Some(backing) => {
                        shard.bytes.fetch_sub(backing.len(), Ordering::Relaxed);
                        POOL_HITS.fetch_add(1, Ordering::Relaxed);
                        backing
                    }
                    // Extra space guarantees an aligned start inside.
                    None => vec![0u8; (PAGE_SIZE << class) + PAGE_SIZE - 1],
                }
            }
            None => vec![0u8; capacity + PAGE_SIZE - 1],
        };

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

    /// Number of allocations served from the pool since the process started.
    pub fn pool_hits() -> u64 {
        POOL_HITS.load(Ordering::Relaxed)
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

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let backing = std::mem::take(&mut self.backing);
        let size = backing.len();
        // Only memory that came from a size class goes back to the pool.
        let Some(class) = size
            .checked_sub(PAGE_SIZE - 1)
            .filter(|usable| usable.is_power_of_two())
            .and_then(class_of)
        else {
            return;
        };
        let shard = my_shard();
        if shard.bytes.load(Ordering::Relaxed) + size > SHARD_BUDGET {
            return;
        }
        shard.bytes.fetch_add(size, Ordering::Relaxed);
        shard.classes[class].lock().push(backing);
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

    #[test]
    fn dropped_buffers_are_reused() {
        let before = AlignedBuffer::pool_hits();
        for _ in 0..64 {
            let mut buf = AlignedBuffer::pages(96).unwrap();
            assert_eq!(buf.capacity(), 96 * PAGE_SIZE);
            assert!((buf.as_ptr() as usize).is_multiple_of(PAGE_SIZE));
            buf.as_mut_slice().fill(0xEE);
        }
        assert!(
            AlignedBuffer::pool_hits() > before,
            "64 allocate/drop rounds never reused a buffer"
        );
    }

    #[test]
    fn capacity_is_what_was_asked_for_not_the_size_class() {
        // 3 pages come from the 4-page class but must present as 3 pages:
        // a read fills a buffer to its capacity.
        let buf = AlignedBuffer::pages(3).unwrap();
        assert_eq!(buf.capacity(), 3 * PAGE_SIZE);
        let big = AlignedBuffer::new(5 << 20).unwrap();
        assert_eq!(big.capacity(), 5 << 20);
    }

    #[test]
    fn zero_clears_recycled_contents() {
        for _ in 0..8 {
            let mut buf = AlignedBuffer::pages(48).unwrap();
            buf.as_mut_slice().fill(0x77);
        }
        let mut buf = AlignedBuffer::pages(48).unwrap();
        buf.zero();
        assert!(buf.as_mut_slice().iter().all(|b| *b == 0));
    }
}
