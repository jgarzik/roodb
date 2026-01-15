//! SQL error types

use std::fmt;

use crate::catalog::DataType;

/// SQL error types
#[derive(Debug, Clone)]
pub enum SqlError {
    /// Parse error from sqlparser
    Parse(String),
    /// Table not found during resolution
    TableNotFound(String),
    /// Column not found during resolution
    ColumnNotFound(String),
    /// Column is ambiguous (exists in multiple tables)
    AmbiguousColumn(String),
    /// Type mismatch in expression
    TypeMismatch { expected: DataType, found: DataType },
    /// Invalid operation
    InvalidOperation(String),
    /// Unsupported SQL feature
    Unsupported(String),
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::Parse(msg) => write!(f, "Parse error: {}", msg),
            SqlError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            SqlError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            SqlError::AmbiguousColumn(name) => {
                write!(f, "Column '{}' is ambiguous", name)
            }
            SqlError::TypeMismatch { expected, found } => {
                write!(
                    f,
                    "Type mismatch: expected {:?}, found {:?}",
                    expected, found
                )
            }
            SqlError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            SqlError::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
        }
    }
}

impl std::error::Error for SqlError {}

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        SqlError::Parse(err.to_string())
    }
}

/// Result type for SQL operations
pub type SqlResult<T> = Result<T, SqlError>;
