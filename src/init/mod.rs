//! Database initialization for RooDB
//!
//! This module handles first-time database initialization, creating the root user
//! and setting up required system state. Initialization uses direct storage writes
//! (no Raft) and should be run before starting the server.
//!
//! Environment Variables:
//! - `ROODB_ROOT_PASSWORD` - Set root password directly
//! - `ROODB_ROOT_PASSWORD_FILE` - Read password from file (Docker secrets)

use std::env;
use std::fs;
use std::sync::Arc;

use thiserror::Error;

use crate::catalog::system_tables::{SYSTEM_GRANTS, SYSTEM_USERS};
use crate::executor::encoding::{encode_row, encode_row_key};
use crate::executor::{Datum, Row};
use crate::protocol::roodb::auth::compute_password_hash;
use crate::sql::Privilege;
use crate::storage::schema_version::{write_schema_version, CURRENT_SCHEMA_VERSION};
use crate::storage::StorageEngine;

/// Root username
pub const ROOT_USER: &str = "root";

/// Default host pattern (matches all)
pub const DEFAULT_HOST: &str = "%";

/// Auth plugin for native password authentication
pub const AUTH_PLUGIN_NATIVE: &str = "mysql_native_password";

/// Initialization errors
#[derive(Error, Debug)]
pub enum InitError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Password hashing error: {0}")]
    PasswordHash(String),
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Configuration from environment variables
#[derive(Debug)]
pub struct InitConfig {
    pub password: Option<String>,
    pub password_file: Option<String>,
}

impl InitConfig {
    /// Read configuration from environment variables
    pub fn from_env() -> Self {
        InitConfig {
            password: env::var("ROODB_ROOT_PASSWORD").ok(),
            password_file: env::var("ROODB_ROOT_PASSWORD_FILE").ok(),
        }
    }

    /// Determine the root password from configuration
    pub fn determine_password(&self) -> Result<String, InitError> {
        // Priority: direct password > file
        if let Some(ref pwd) = self.password {
            return Ok(pwd.clone());
        }

        if let Some(ref file_path) = self.password_file {
            let content = fs::read_to_string(file_path)?;
            return Ok(content.trim().to_string());
        }

        Err(InitError::Config(
            "No root password configured. Set ROODB_ROOT_PASSWORD or ROODB_ROOT_PASSWORD_FILE"
                .to_string(),
        ))
    }
}

/// Hash a password for mysql_native_password auth
///
/// Returns hex-encoded SHA1(SHA1(password)) which is the format stored in mysql.user
pub fn hash_password(password: &str) -> String {
    compute_password_hash(password)
}

/// Verify a password against its hash
///
/// For mysql_native_password, the stored hash is SHA1(SHA1(password)) in hex.
pub fn verify_password(password: &str, stored_hash: &str) -> bool {
    // Empty password check
    if password.is_empty() && stored_hash.is_empty() {
        return true;
    }
    if password.is_empty() || stored_hash.is_empty() {
        return false;
    }

    // Compute hash of provided password and compare
    let computed_hash = compute_password_hash(password);
    computed_hash == stored_hash
}

/// Initialize the database with root user and schema version marker
///
/// This writes directly to storage (no Raft) and should only be called
/// once before the server is started.
pub async fn initialize_database(
    storage: &Arc<dyn StorageEngine>,
    password: &str,
) -> Result<(), InitError> {
    let password_hash = hash_password(password);

    // Create user row for root@%
    let user_row = Row::new(vec![
        Datum::String(ROOT_USER.to_string()),          // username
        Datum::String(DEFAULT_HOST.to_string()),       // host
        Datum::String(password_hash),                  // password_hash
        Datum::String(AUTH_PLUGIN_NATIVE.to_string()), // auth_plugin
        Datum::Null,                                   // ssl_subject
        Datum::Null,                                   // ssl_issuer
        Datum::Bool(false),                            // account_locked
        Datum::Bool(false),                            // password_expired
        Datum::Null,                                   // created_at
        Datum::Null,                                   // updated_at
    ]);

    // Create ALL PRIVILEGES grant on *.* with GRANT OPTION
    let grant_row = Row::new(vec![
        Datum::String(ROOT_USER.to_string()),               // grantee
        Datum::String(DEFAULT_HOST.to_string()),            // grantee_host
        Datum::String("USER".to_string()),                  // grantee_type
        Datum::String(Privilege::All.to_str().to_string()), // privilege
        Datum::String("GLOBAL".to_string()),                // object_type
        Datum::Null,                                        // database_name (null for global)
        Datum::Null,                                        // table_name (null for global)
        Datum::Bool(true),                                  // with_grant_option
        Datum::Null,                                        // granted_by
        Datum::Null,                                        // granted_at
    ]);

    // Use fixed row IDs for init (1 for user, 2 for grant)
    // These are safe since this is first-time init on empty storage
    let user_row_id: u64 = 1;
    let grant_row_id: u64 = 2;

    // Encode rows for storage
    let user_key = encode_row_key(SYSTEM_USERS, user_row_id);
    let user_value = encode_row(&user_row);

    let grant_key = encode_row_key(SYSTEM_GRANTS, grant_row_id);
    let grant_value = encode_row(&grant_row);

    // Write user row
    storage
        .put(&user_key, &user_value)
        .await
        .map_err(|e| InitError::Storage(e.to_string()))?;

    // Write grant row
    storage
        .put(&grant_key, &grant_value)
        .await
        .map_err(|e| InitError::Storage(e.to_string()))?;

    // Write schema version marker
    write_schema_version(storage, CURRENT_SCHEMA_VERSION)
        .await
        .map_err(|e| InitError::Storage(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_hash_and_verify_password() {
        let password = "test_password_123!";
        let hash = hash_password(password);

        // Hash should be hex-encoded (40 chars for SHA1 hash)
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Should verify correctly
        assert!(verify_password(password, &hash));

        // Wrong password should fail
        assert!(!verify_password("wrong_password", &hash));
    }

    #[test]
    #[serial]
    fn test_init_config_from_env() {
        // Clear any existing vars
        env::remove_var("ROODB_ROOT_PASSWORD");
        env::remove_var("ROODB_ROOT_PASSWORD_FILE");

        let config = InitConfig::from_env();
        assert!(config.password.is_none());
        assert!(config.password_file.is_none());

        // Test with direct password
        env::set_var("ROODB_ROOT_PASSWORD", "secret123");
        let config = InitConfig::from_env();
        assert_eq!(config.password, Some("secret123".to_string()));

        // Cleanup
        env::remove_var("ROODB_ROOT_PASSWORD");
    }

    #[test]
    #[serial]
    fn test_determine_password_priority() {
        env::remove_var("ROODB_ROOT_PASSWORD");
        env::remove_var("ROODB_ROOT_PASSWORD_FILE");

        // No config should error
        let config = InitConfig::from_env();
        assert!(config.determine_password().is_err());

        // Direct password
        env::set_var("ROODB_ROOT_PASSWORD", "direct");
        let config = InitConfig::from_env();
        let pwd = config.determine_password().unwrap();
        assert_eq!(pwd, "direct");

        // Cleanup
        env::remove_var("ROODB_ROOT_PASSWORD");
    }
}
