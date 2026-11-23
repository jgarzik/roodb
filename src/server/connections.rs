//! The open connections, by connection ID, so that one session can
//! interrupt another (`KILL`).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::executor::cancel::Interrupt;

/// What `KILL` needs to know about a connection.
#[derive(Clone)]
pub struct ConnectionEntry {
    /// The user the connection logged in as (empty before login)
    pub user: String,
    pub interrupt: Arc<Interrupt>,
}

/// Every open connection of either protocol; IDs are shared.
#[derive(Default)]
pub struct Connections {
    open: Mutex<HashMap<u32, ConnectionEntry>>,
}

impl Connections {
    /// Add a connection; it is removed when the returned registration is
    /// dropped.
    pub fn register(self: &Arc<Self>, id: u32, interrupt: Arc<Interrupt>) -> Registration {
        self.open.lock().insert(
            id,
            ConnectionEntry {
                user: String::new(),
                interrupt,
            },
        );
        Registration {
            connections: self.clone(),
            id,
        }
    }

    /// The connection with this ID, if it is open.
    pub fn get(&self, id: u64) -> Option<ConnectionEntry> {
        let id = u32::try_from(id).ok()?;
        self.open.lock().get(&id).cloned()
    }

    fn set_user(&self, id: u32, user: &str) {
        if let Some(entry) = self.open.lock().get_mut(&id) {
            entry.user = user.to_string();
        }
    }
}

/// A connection's place in [`Connections`], for as long as it is open.
pub struct Registration {
    connections: Arc<Connections>,
    id: u32,
}

impl Registration {
    /// Record the user the connection logged in as.
    pub fn set_user(&self, user: &str) {
        self.connections.set_user(self.id, user);
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.connections.open.lock().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_until_dropped() {
        let connections = Arc::new(Connections::default());
        let registration = connections.register(7, Interrupt::new());
        registration.set_user("root");
        assert_eq!(connections.get(7).unwrap().user, "root");
        assert!(connections.get(8).is_none());
        assert!(connections.get(u64::MAX).is_none());
        drop(registration);
        assert!(connections.get(7).is_none());
    }
}
