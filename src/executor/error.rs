//! Executor error types

use std::fmt;

use crate::catalog::DataType;
use crate::storage::StorageError;
use crate::txn::TransactionError;

/// Result type for executor operations
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor errors
#[derive(Debug)]
pub enum ExecutorError {
    /// Storage layer error
    Storage(StorageError),

    /// Transaction error
    Transaction(TransactionError),

    /// Type mismatch during evaluation
    TypeMismatch {
        expected: DataType,
        got: DataType,
        context: String,
    },

    /// Invalid operation (e.g., division by zero)
    InvalidOperation(String),

    /// Column not found during evaluation
    ColumnNotFound { table: String, column: String },

    /// Column index out of bounds
    ColumnIndexOutOfBounds { index: usize, row_len: usize },

    /// Null value where not allowed
    NullValue(String),

    /// Table not found
    TableNotFound(String),

    /// Encoding/decoding error
    Encoding(String),

    /// Internal executor error
    Internal(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::Storage(e) => write!(f, "storage error: {}", e),
            ExecutorError::Transaction(e) => write!(f, "transaction error: {}", e),
            ExecutorError::TypeMismatch {
                expected,
                got,
                context,
            } => {
                write!(
                    f,
                    "type mismatch: expected {:?}, got {:?} in {}",
                    expected, got, context
                )
            }
            ExecutorError::InvalidOperation(msg) => write!(f, "invalid operation: {}", msg),
            ExecutorError::ColumnNotFound { table, column } => {
                write!(f, "column not found: {}.{}", table, column)
            }
            ExecutorError::ColumnIndexOutOfBounds { index, row_len } => {
                write!(
                    f,
                    "column index {} out of bounds (row has {} columns)",
                    index, row_len
                )
            }
            ExecutorError::NullValue(context) => write!(f, "null value in {}", context),
            ExecutorError::TableNotFound(name) => write!(f, "table not found: {}", name),
            ExecutorError::Encoding(msg) => write!(f, "encoding error: {}", msg),
            ExecutorError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ExecutorError::Storage(e) => Some(e),
            ExecutorError::Transaction(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StorageError> for ExecutorError {
    fn from(e: StorageError) -> Self {
        ExecutorError::Storage(e)
    }
}

impl From<TransactionError> for ExecutorError {
    fn from(e: TransactionError) -> Self {
        ExecutorError::Transaction(e)
    }
}
