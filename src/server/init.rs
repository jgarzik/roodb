//! First-time initialization for RooDB
//!
//! Handles environment variable-based initialization for container deployments.
//!
//! Environment Variables:
//! - `ROODB_ROOT_PASSWORD` - Set root password directly
//! - `ROODB_ROOT_PASSWORD_FILE` - Read password from file (Docker secrets)

use std::env;
use std::fs;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;
use tracing::{error, info};

use crate::catalog::system_tables::{SYSTEM_GRANTS, SYSTEM_USERS};
use crate::catalog::Catalog;
use crate::executor::encoding::{encode_row, encode_row_key, table_key_end, table_key_prefix};
use crate::executor::{Datum, Row};
use crate::protocol::roodb::auth::compute_password_hash;
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::sql::Privilege;
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};
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
    #[error("Raft error: {0}")]
    Raft(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Transaction error: {0}")]
    Transaction(String),
}

/// Result of initialization
#[derive(Debug)]
pub enum InitResult {
    /// Database already initialized, skipped
    AlreadyInitialized,
    /// Successfully initialized
    Initialized,
    /// Initialization failed
    Failed(String),
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

/// Check if the database has been initialized (has users)
pub async fn is_initialized(storage: &Arc<dyn StorageEngine>) -> Result<bool, InitError> {
    // Check if system.users has any rows using raw storage scan
    let prefix = table_key_prefix(SYSTEM_USERS);
    let end = table_key_end(SYSTEM_USERS);

    let rows = storage
        .scan(Some(&prefix), Some(&end))
        .await
        .map_err(|e| InitError::Storage(e.to_string()))?;

    Ok(!rows.is_empty())
}

/// Initialize the root user and default grants
pub async fn initialize_root_user(
    password: &str,
    _storage: Arc<dyn StorageEngine>,
    _catalog: Arc<RwLock<Catalog>>,
    raft_node: Arc<RaftNode>,
) -> Result<(), InitError> {
    let password_hash = hash_password(password);

    // Create user row
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

    // Pre-allocate row IDs: 1 for user + 1 for grant
    let (mut next_local, node_id) = allocate_row_id_batch(2);

    // Encode rows for storage
    let user_key = encode_row_key(SYSTEM_USERS, encode_row_id(next_local, node_id));
    next_local += 1;
    let user_value = encode_row(&user_row);

    let grant_key = encode_row_key(SYSTEM_GRANTS, encode_row_id(next_local, node_id));
    let grant_value = encode_row(&grant_row);

    // Create ChangeSet for Raft replication
    let mut changeset = ChangeSet::new(0); // txn_id 0 for init
    changeset.push(RowChange::insert(SYSTEM_USERS, user_key, user_value));
    changeset.push(RowChange::insert(SYSTEM_GRANTS, grant_key, grant_value));

    // Apply through Raft
    raft_node
        .propose_changes(changeset)
        .await
        .map_err(|e| InitError::Raft(e.to_string()))?;

    info!(
        "Root user '{}' created with ALL PRIVILEGES on *.*",
        ROOT_USER
    );

    Ok(())
}

/// Perform first-time initialization if needed
pub async fn maybe_initialize(
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Arc<RaftNode>,
) -> InitResult {
    // Check if already initialized
    match is_initialized(&storage).await {
        Ok(true) => {
            info!("Database already initialized, skipping initialization");
            return InitResult::AlreadyInitialized;
        }
        Ok(false) => {
            info!("First-time initialization starting...");
        }
        Err(e) => {
            error!("Failed to check initialization status: {}", e);
            return InitResult::Failed(e.to_string());
        }
    }

    // Read configuration from environment
    let config = InitConfig::from_env();

    // Determine password
    let password = match config.determine_password() {
        Ok(p) => p,
        Err(e) => {
            error!("{}", e);
            return InitResult::Failed(e.to_string());
        }
    };

    // Initialize root user
    match initialize_root_user(&password, storage, catalog, raft_node).await {
        Ok(()) => InitResult::Initialized,
        Err(e) => {
            error!("Failed to initialize root user: {}", e);
            InitResult::Failed(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
