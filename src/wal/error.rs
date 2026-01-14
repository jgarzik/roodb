//! WAL error types

use thiserror::Error;

/// WAL operation errors
#[derive(Error, Debug)]
pub enum WalError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] crate::io::IoError),

    /// Standard IO error
    #[error("IO error: {0}")]
    StdIo(#[from] std::io::Error),

    /// CRC checksum mismatch
    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    /// Invalid record header
    #[error("Invalid record header")]
    InvalidHeader,

    /// Record too large
    #[error("Record too large: {size} bytes (max {max})")]
    RecordTooLarge { size: usize, max: usize },

    /// Corrupted segment
    #[error("Corrupted segment at offset {offset}")]
    CorruptedSegment { offset: u64 },

    /// Segment not found
    #[error("Segment {id} not found")]
    SegmentNotFound { id: u64 },

    /// WAL is closed
    #[error("WAL is closed")]
    Closed,

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for WAL operations
pub type WalResult<T> = Result<T, WalError>;
