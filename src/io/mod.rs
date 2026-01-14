//! Async IO layer for RooDB
//!
//! This module provides a cross-platform async IO abstraction:
//! - Linux: io_uring for high-performance direct IO
//! - Other platforms: POSIX fallback using tokio's blocking pool
//!
//! All IO operations require 4KB-aligned buffers and offsets for
//! compatibility with direct IO.

pub mod aligned_buffer;
pub mod error;
pub mod posix_aio;
pub mod traits;

#[cfg(target_os = "linux")]
pub mod uring;

pub use aligned_buffer::AlignedBuffer;
pub use error::{IoError, IoResult};
pub use posix_aio::{PosixIO, PosixIOFactory};
pub use traits::{AsyncIO, AsyncIOFactory, PAGE_SIZE};

#[cfg(target_os = "linux")]
pub use uring::{UringIO, UringIOFactory};

/// Default IO factory for the current platform
///
/// Returns io_uring on Linux, POSIX fallback elsewhere.
#[cfg(target_os = "linux")]
pub fn default_io_factory() -> UringIOFactory {
    UringIOFactory
}

/// Default IO factory for the current platform
///
/// Returns POSIX fallback on non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub fn default_io_factory() -> PosixIOFactory {
    PosixIOFactory
}

/// Type alias for the default IO type on this platform
#[cfg(target_os = "linux")]
pub type DefaultIO = UringIO;

/// Type alias for the default IO type on this platform
#[cfg(not(target_os = "linux"))]
pub type DefaultIO = PosixIO;
