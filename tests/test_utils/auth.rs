//! Auth test utilities
//!
//! Provides helper functions for test authentication setup.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use roodb::catalog::system_tables::{SYSTEM_GRANTS, SYSTEM_USERS};
use roodb::executor::encoding::{encode_row, encode_row_key};
use roodb::executor::{Datum, Row};
use roodb::server::hash_password;
use roodb::sql::Privilege;
use roodb::storage::StorageEngine;

/// Row ID counter for test initialization
#[allow(dead_code)]
static TEST_ROW_ID: AtomicU64 = AtomicU64::new(1);

/// Initialize root user in storage for testing.
///
/// This creates the root user with the given password and grants ALL PRIVILEGES.
/// Should be called after creating storage but before starting the server.
#[allow(dead_code)]
pub async fn initialize_root_user(storage: &Arc<dyn StorageEngine>, password: &str) {
    let password_hash = hash_password(password);

    // Create user row for root@%
    let user_row = Row::new(vec![
        Datum::String("root".to_string()),                  // username
        Datum::String("%".to_string()),                     // host
        Datum::String(password_hash),                       // password_hash
        Datum::String("mysql_native_password".to_string()), // auth_plugin
        Datum::Null,                                        // ssl_subject
        Datum::Null,                                        // ssl_issuer
        Datum::Bool(false),                                 // account_locked
        Datum::Bool(false),                                 // password_expired
        Datum::Null,                                        // created_at
        Datum::Null,                                        // updated_at
    ]);

    // Create grant row for ALL PRIVILEGES on *.*
    let grant_row = Row::new(vec![
        Datum::String("root".to_string()),                  // grantee
        Datum::String("%".to_string()),                     // grantee_host
        Datum::String("USER".to_string()),                  // grantee_type
        Datum::String(Privilege::All.to_str().to_string()), // privilege
        Datum::String("GLOBAL".to_string()),                // object_type
        Datum::Null,                                        // database_name
        Datum::Null,                                        // table_name
        Datum::Bool(true),                                  // with_grant_option
        Datum::Null,                                        // granted_by
        Datum::Null,                                        // granted_at
    ]);

    // Encode and write rows
    let user_key = encode_row_key(SYSTEM_USERS, TEST_ROW_ID.fetch_add(1, Ordering::SeqCst));
    let user_value = encode_row(&user_row);

    let grant_key = encode_row_key(SYSTEM_GRANTS, TEST_ROW_ID.fetch_add(1, Ordering::SeqCst));
    let grant_value = encode_row(&grant_row);

    storage
        .put(&user_key, &user_value)
        .await
        .expect("Failed to write user");
    storage
        .put(&grant_key, &grant_value)
        .await
        .expect("Failed to write grant");
}
