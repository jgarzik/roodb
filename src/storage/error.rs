//! Storage error types

use thiserror::Error;

/// Storage operation errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// IO error from underlying IO layer
    #[error("IO error: {0}")]
    Io(#[from] crate::io::IoError),

    /// Standard IO error
    #[error("IO error: {0}")]
    StdIo(#[from] std::io::Error),

    /// Corrupted block (CRC mismatch)
    #[error("Corrupted block at offset {offset}: expected CRC {expected:#x}, got {actual:#x}")]
    CorruptedBlock {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    /// Invalid SSTable format
    #[error("Invalid SSTable: {0}")]
    InvalidSstable(String),

    /// Key too large
    #[error("Key too large: {size} bytes (max {max})")]
    KeyTooLarge { size: usize, max: usize },

    /// Value too large
    #[error("Value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Manifest error
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Storage is closed
    #[error("Storage is closed")]
    Closed,
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;
