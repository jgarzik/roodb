//! MySQL protocol error types

use std::fmt;
use std::io;

use crate::executor::ExecutorError;
use crate::planner::PlannerError;
use crate::sql::SqlError;

/// MySQL protocol errors
#[derive(Debug)]
pub enum ProtocolError {
    /// I/O error during read/write
    Io(io::Error),
    /// Invalid packet format
    InvalidPacket(String),
    /// Authentication failed
    AuthFailed(String),
    /// Unsupported feature
    Unsupported(String),
    /// SQL parsing error
    Sql(SqlError),
    /// Query planning error
    Planner(PlannerError),
    /// Query execution error
    Executor(ExecutorError),
    /// Connection closed by client
    ConnectionClosed,
    /// TLS handshake error
    Tls(String),
    /// Internal error
    Internal(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Io(e) => write!(f, "I/O error: {}", e),
            ProtocolError::InvalidPacket(msg) => write!(f, "Invalid packet: {}", msg),
            ProtocolError::AuthFailed(msg) => write!(f, "Authentication failed: {}", msg),
            ProtocolError::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
            ProtocolError::Sql(e) => write!(f, "SQL error: {}", e),
            ProtocolError::Planner(e) => write!(f, "Planner error: {}", e),
            ProtocolError::Executor(e) => write!(f, "Executor error: {}", e),
            ProtocolError::ConnectionClosed => write!(f, "Connection closed"),
            ProtocolError::Tls(msg) => write!(f, "TLS error: {}", msg),
            ProtocolError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Io(e) => Some(e),
            ProtocolError::Sql(e) => Some(e),
            ProtocolError::Planner(e) => Some(e),
            ProtocolError::Executor(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        ProtocolError::Io(e)
    }
}

impl From<SqlError> for ProtocolError {
    fn from(e: SqlError) -> Self {
        ProtocolError::Sql(e)
    }
}

impl From<PlannerError> for ProtocolError {
    fn from(e: PlannerError) -> Self {
        ProtocolError::Planner(e)
    }
}

impl From<ExecutorError> for ProtocolError {
    fn from(e: ExecutorError) -> Self {
        ProtocolError::Executor(e)
    }
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// MySQL error codes
pub mod codes {
    pub const ER_SYNTAX_ERROR: u16 = 1064;
    pub const ER_NO_SUCH_TABLE: u16 = 1146;
    pub const ER_TABLE_EXISTS_ERROR: u16 = 1050;
    pub const ER_UNKNOWN_ERROR: u16 = 1105;
    pub const ER_ACCESS_DENIED: u16 = 1045;
    pub const ER_BAD_DB_ERROR: u16 = 1049;
    pub const ER_UNKNOWN_COM_ERROR: u16 = 1047;
}

/// SQL state codes
pub mod states {
    pub const SYNTAX_ERROR: &str = "42000";
    pub const NO_SUCH_TABLE: &str = "42S02";
    pub const TABLE_EXISTS: &str = "42S01";
    pub const GENERAL_ERROR: &str = "HY000";
    pub const ACCESS_DENIED: &str = "28000";
}
