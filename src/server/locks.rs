//! Advisory lock manager for MySQL GET_LOCK/RELEASE_LOCK/IS_FREE_LOCK compatibility.
//!
//! Provides named advisory locks shared across all connections on a server.
//! Locks are owned by connection ID and automatically released on disconnect.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// Global advisory lock manager (one per server process).
static GLOBAL_LOCKS: std::sync::LazyLock<AdvisoryLockManager> =
    std::sync::LazyLock::new(AdvisoryLockManager::new);

/// Get the global advisory lock manager.
pub fn global_lock_manager() -> &'static AdvisoryLockManager {
    &GLOBAL_LOCKS
}

/// Manages named advisory locks across all connections.
#[derive(Clone)]
pub struct AdvisoryLockManager {
    /// Map of lock_name → owning connection_id
    locks: Arc<RwLock<HashMap<String, u32>>>,
}

impl AdvisoryLockManager {
    fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Acquire a named lock for the given connection.
    /// Returns 1 if acquired, 0 if already held by another connection.
    pub fn get_lock(&self, name: &str, connection_id: u32) -> i64 {
        let mut locks = self.locks.write();
        match locks.get(name) {
            Some(&owner) if owner == connection_id => 1, // already own it
            Some(_) => 0,                                // held by another
            None => {
                locks.insert(name.to_string(), connection_id);
                1
            }
        }
    }

    /// Release a named lock owned by the given connection.
    /// Returns 1 if released, 0 if not owned by this connection, NULL (None) if not held.
    pub fn release_lock(&self, name: &str, connection_id: u32) -> Option<i64> {
        let mut locks = self.locks.write();
        match locks.get(name) {
            Some(&owner) if owner == connection_id => {
                locks.remove(name);
                Some(1)
            }
            Some(_) => Some(0), // held by another connection
            None => None,       // lock doesn't exist
        }
    }

    /// Check if a named lock is free.
    /// Returns 1 if free, 0 if in use.
    pub fn is_free_lock(&self, name: &str) -> i64 {
        let locks = self.locks.read();
        if locks.contains_key(name) {
            0
        } else {
            1
        }
    }

    /// Release all locks owned by a given connection (called on disconnect).
    pub fn release_all_for_connection(&self, connection_id: u32) {
        let mut locks = self.locks.write();
        locks.retain(|_, &mut owner| owner != connection_id);
    }
}
