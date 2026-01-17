//! Async IO layer for RooDB
//!
//! This module provides a cross-platform async IO abstraction:
//! - Linux: io_uring for high-performance direct IO
//! - Other platforms: POSIX fallback using tokio's blocking pool
//!
//! All IO operations require 4KB-aligned buffers and offsets for
//! compatibility with direct IO.
//!
//! # I/O Scheduler
//!
//! The scheduler module provides priority-based I/O scheduling with
//! two-dimensional context (Urgency × OpKind):
//!
//! - **Urgency**: Critical (COMMIT-blocking), Foreground (query), Background (flush/compaction)
//! - **OpKind**: Read, Write, Sync
//! - Backpressure detection and signaling
//! - Batch submission for io_uring
//! - Support for 4KB pages and 2MB large chunks
//!
//! # Recommended Usage
//!
//! Use `scheduled_io_factory()` to get a factory with full scheduling support:
//!
//! ```ignore
//! let factory = Arc::new(scheduled_io_factory());
//! let engine = LsmEngine::open(factory, config).await?;
//! ```

use std::sync::Arc;

pub mod aligned_buffer;
pub mod error;
pub mod posix_aio;
pub mod scheduler;
pub mod traits;

#[cfg(target_os = "linux")]
pub mod uring;

pub use aligned_buffer::AlignedBuffer;
pub use error::{IoError, IoResult};
pub use posix_aio::{PosixIO, PosixIOFactory};
pub use traits::{AsyncIO, AsyncIOFactory, PAGE_SIZE};

// Re-export key scheduler types
pub use scheduler::{
    BackpressureController, BackpressureLevel, IoContext, IoPriority, OpKind, ScheduledHandle,
    ScheduledIOFactory, SchedulerConfig, Urgency,
};

#[cfg(target_os = "linux")]
pub use uring::{UringIO, UringIOFactory};

/// Default IO factory for the current platform (without scheduling)
///
/// Returns io_uring on Linux, POSIX fallback elsewhere.
///
/// **Note**: For production use, prefer `scheduled_io_factory()` which adds
/// backpressure, metrics, and priority-based scheduling.
#[cfg(target_os = "linux")]
pub fn default_io_factory() -> UringIOFactory {
    UringIOFactory
}

/// Default IO factory for the current platform (without scheduling)
///
/// Returns POSIX fallback on non-Linux platforms.
///
/// **Note**: For production use, prefer `scheduled_io_factory()` which adds
/// backpressure, metrics, and priority-based scheduling.
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

/// Type alias for the default IO factory on this platform
#[cfg(target_os = "linux")]
pub type DefaultIOFactory = UringIOFactory;

/// Type alias for the default IO factory on this platform
#[cfg(not(target_os = "linux"))]
pub type DefaultIOFactory = PosixIOFactory;

/// Type alias for a scheduled IO handle
pub type ScheduledIO = ScheduledHandle<DefaultIO>;

/// Type alias for a scheduled IO factory
pub type DefaultScheduledFactory = ScheduledIOFactory<DefaultIO, DefaultIOFactory>;

/// Create a scheduled IO factory for the current platform
///
/// This wraps the platform-specific backend (io_uring or POSIX) with
/// scheduling coordination including:
/// - Backpressure detection and throttling
/// - Metrics collection
/// - Priority-based decisions using IoContext
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use roodb::io::scheduled_io_factory;
/// use roodb::storage::lsm::{LsmEngine, LsmConfig};
///
/// let factory = Arc::new(scheduled_io_factory());
/// let engine = LsmEngine::open(factory, LsmConfig::default()).await?;
/// ```
pub fn scheduled_io_factory() -> DefaultScheduledFactory {
    ScheduledIOFactory::with_factory(Arc::new(default_io_factory()))
}

/// Create a scheduled IO factory with custom configuration
pub fn scheduled_io_factory_with_config(config: SchedulerConfig) -> DefaultScheduledFactory {
    ScheduledIOFactory::new(Arc::new(default_io_factory()), config)
}
