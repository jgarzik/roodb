//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable logging for database operations with:
//! - CRC32 checksums for corruption detection
//! - Segment-based file management
//! - Recovery support
//!
//! The WAL uses the IO layer for direct IO operations with 4KB alignment.

pub mod error;
pub mod manager;
pub mod record;
pub mod segment;

pub use error::{WalError, WalResult};
pub use manager::{WalConfig, WalManager};
pub use record::{Record, RecordType, HEADER_SIZE, MAX_RECORD_SIZE};
pub use segment::{Segment, DEFAULT_SEGMENT_SIZE, SEGMENT_HEADER_SIZE};
