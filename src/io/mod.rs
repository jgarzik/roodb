//! The block I/O layer.
//!
//! RooDB does its own file I/O rather than leaning on the kernel page cache:
//! files are opened for direct I/O and every read and write goes through one
//! [`IoEngine`], created at startup. The engine runs on io_uring where the
//! kernel provides it and on a worker pool elsewhere; the choice is made at
//! run time (`--io-backend`).
//!
//! All transfers use page-aligned buffers ([`AlignedBuffer`]) and offsets that
//! are multiples of [`PAGE_SIZE`]. Each request carries an [`Urgency`] so that
//! work a client is waiting on is not queued behind background work.

pub mod aligned_buffer;
pub mod engine;
pub mod error;
mod posix;

#[cfg(target_os = "linux")]
mod uring;

pub use aligned_buffer::{AlignedBuffer, PAGE_SIZE};
pub use engine::{File, IoBackend, IoConfig, IoEngine, IoStats, OpenOptions, Urgency};
pub use error::{IoError, IoResult};
