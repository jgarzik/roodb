//! Statement-level errors reported to clients.

use std::borrow::Cow;
use std::fmt;

/// A statement failure, as reported to the client.
///
/// RooDB speaks the MySQL SQL dialect, so its native error catalogue is the
/// MySQL one: a numeric error code plus a five-character SQLSTATE. Wire
/// protocols with a different error model translate from these two fields
/// and never inspect the message text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    /// MySQL-compatible error number (e.g. 1146 for "no such table")
    pub code: u16,
    /// Five-character SQLSTATE (e.g. "42S02")
    pub sqlstate: Cow<'static, str>,
    /// Human-readable message
    pub message: String,
}

impl QueryError {
    /// Create an error from a code, SQLSTATE and message.
    pub fn new(
        code: u16,
        sqlstate: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            sqlstate: sqlstate.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ERROR {} ({}): {}",
            self.code, self.sqlstate, self.message
        )
    }
}

impl std::error::Error for QueryError {}
