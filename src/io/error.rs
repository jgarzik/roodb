//! IO error types

use thiserror::Error;

/// IO operation errors
#[derive(Error, Debug)]
pub enum IoError {
    /// Standard IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Buffer alignment error
    #[error("Buffer not aligned to {expected} bytes (got offset {actual})")]
    Alignment { expected: usize, actual: usize },

    /// Buffer size error
    #[error("Buffer size {size} not a multiple of {alignment}")]
    BufferSize { size: usize, alignment: usize },

    /// File offset error
    #[error("File offset {offset} not aligned to {alignment}")]
    OffsetAlignment { offset: u64, alignment: usize },

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),
}

/// Result type for IO operations
pub type IoResult<T> = Result<T, IoError>;
