//! Session state for MySQL connections

/// Per-connection session state
#[derive(Debug, Clone)]
pub struct Session {
    /// Unique connection identifier
    pub connection_id: u32,
    /// Currently selected database (USE db)
    pub database: Option<String>,
    /// Authenticated username
    pub user: String,
}

impl Session {
    /// Create a new session with default state
    pub fn new(connection_id: u32) -> Self {
        Self {
            connection_id,
            database: None,
            user: String::new(),
        }
    }

    /// Set the authenticated user
    pub fn set_user(&mut self, user: String) {
        self.user = user;
    }

    /// Set the current database
    pub fn set_database(&mut self, database: Option<String>) {
        self.database = database;
    }
}
