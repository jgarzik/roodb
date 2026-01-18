//! Auth test utilities
//!
//! Provides helper functions for test authentication setup.
//!
//! This module re-exports the init module's initialize_database function
//! for backwards compatibility with existing tests.

use std::sync::Arc;

use roodb::init::initialize_database;
use roodb::storage::StorageEngine;

/// Initialize root user in storage for testing.
///
/// This creates the root user with the given password and grants ALL PRIVILEGES.
/// Should be called after creating storage but before starting the server.
#[allow(dead_code)]
pub async fn initialize_root_user(storage: &Arc<dyn StorageEngine>, password: &str) {
    initialize_database(storage, password)
        .await
        .expect("Failed to initialize database");
}
