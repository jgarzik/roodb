//! Prepared statement support (stub)
//!
//! Full prepared statement support is planned for a future phase.
//! This module provides placeholder types and error responses.

use super::error::{codes, states, ProtocolResult};
use super::resultset::encode_err_packet;

/// Prepared statement handle (stub)
#[derive(Debug)]
pub struct PreparedStatement {
    /// Statement ID
    pub id: u32,
    /// Original SQL
    pub sql: String,
    /// Parameter count
    pub param_count: u16,
    /// Column count
    pub column_count: u16,
}

/// Error response for unsupported prepared statement operations
pub fn unsupported_prepared_stmt_error() -> Vec<u8> {
    encode_err_packet(
        codes::ER_UNKNOWN_ERROR,
        states::GENERAL_ERROR,
        "Prepared statements not yet supported",
    )
}

/// Placeholder for future prepared statement manager
pub struct PreparedStatementManager;

impl PreparedStatementManager {
    /// Create a new prepared statement manager
    pub fn new() -> Self {
        PreparedStatementManager
    }

    /// Prepare a statement (returns error for now)
    pub fn prepare(&mut self, _sql: &str) -> ProtocolResult<PreparedStatement> {
        Err(super::error::ProtocolError::Unsupported(
            "Prepared statements not yet supported".to_string(),
        ))
    }

    /// Get a prepared statement by ID
    pub fn get(&self, _id: u32) -> Option<&PreparedStatement> {
        None
    }

    /// Close a prepared statement
    pub fn close(&mut self, _id: u32) -> bool {
        false
    }
}

impl Default for PreparedStatementManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_new() {
        let manager = PreparedStatementManager::new();
        assert!(manager.get(1).is_none());
    }

    #[test]
    fn test_unsupported_error() {
        let packet = unsupported_prepared_stmt_error();
        assert_eq!(packet[0], 0xff); // ERR packet
    }
}
