//! Auth executors
//!
//! Implements CREATE USER, DROP USER, ALTER USER, GRANT, REVOKE, SHOW GRANTS.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::catalog::system_tables::{SYSTEM_GRANTS, SYSTEM_USERS};
use crate::catalog::Catalog;
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::server::init::{hash_password, AUTH_PLUGIN_NATIVE, DEFAULT_HOST};
use crate::sql::privileges::{HostPattern, Privilege, PrivilegeObject};
use crate::storage::next_row_id;

use super::datum::Datum;
use super::encoding::{encode_row, encode_row_key, table_key_end, table_key_prefix};
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;
use super::Executor;

/// Helper to get a string from a datum option
fn get_string(datum: Option<&Datum>) -> Option<String> {
    match datum {
        Some(Datum::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Helper to get a bool from a datum option
fn get_bool(datum: Option<&Datum>) -> Option<bool> {
    match datum {
        Some(Datum::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// CREATE USER executor
pub struct CreateUser {
    /// Username
    username: String,
    /// Host pattern
    host: HostPattern,
    /// Password (plaintext, will be hashed)
    password: Option<String>,
    /// IF NOT EXISTS flag (TODO: check for existing user)
    _if_not_exists: bool,
    /// Raft node for replication
    raft_node: Arc<RaftNode>,
    /// Catalog reference (for checking existence)
    _catalog: Arc<RwLock<Catalog>>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl CreateUser {
    /// Create a new CREATE USER executor
    pub fn new(
        username: String,
        host: HostPattern,
        password: Option<String>,
        if_not_exists: bool,
        raft_node: Arc<RaftNode>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        CreateUser {
            username,
            host,
            password,
            _if_not_exists: if_not_exists,
            raft_node,
            _catalog: catalog,
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for CreateUser {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Hash the password
        let password_hash = match &self.password {
            Some(pwd) => hash_password(pwd),
            None => String::new(), // Empty password
        };

        // Create user row
        let user_row = Row::new(vec![
            Datum::String(self.username.clone()),          // username
            Datum::String(self.host.as_str().to_string()), // host
            Datum::String(password_hash),                  // password_hash
            Datum::String(AUTH_PLUGIN_NATIVE.to_string()), // auth_plugin
            Datum::Null,                                   // ssl_subject
            Datum::Null,                                   // ssl_issuer
            Datum::Bool(false),                            // account_locked
            Datum::Bool(false),                            // password_expired
            Datum::Null,                                   // created_at
            Datum::Null,                                   // updated_at
        ]);

        // Encode for storage
        let key = encode_row_key(SYSTEM_USERS, next_row_id());
        let value = encode_row(&user_row);

        // Create changeset and apply via Raft
        let mut changeset = ChangeSet::new(0);
        changeset.push(RowChange::insert(SYSTEM_USERS, key.clone(), value.clone()));

        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;

        // Store change for take_changes
        self.changes
            .push(RowChange::insert(SYSTEM_USERS, key, value));

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// DROP USER executor
pub struct DropUser {
    /// Username
    username: String,
    /// Host pattern
    host: HostPattern,
    /// IF EXISTS flag
    if_exists: bool,
    /// Raft node for replication
    raft_node: Arc<RaftNode>,
    /// Storage for scanning
    storage: Arc<dyn crate::storage::StorageEngine>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl DropUser {
    /// Create a new DROP USER executor
    pub fn new(
        username: String,
        host: HostPattern,
        if_exists: bool,
        raft_node: Arc<RaftNode>,
        storage: Arc<dyn crate::storage::StorageEngine>,
    ) -> Self {
        DropUser {
            username,
            host,
            if_exists,
            raft_node,
            storage,
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for DropUser {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Find user rows to delete
        let prefix = table_key_prefix(SYSTEM_USERS);
        let end = table_key_end(SYSTEM_USERS);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(|e| ExecutorError::Internal(format!("Storage error: {}", e)))?;

        let mut found = false;
        let mut changeset = ChangeSet::new(0);

        for (key, value) in rows {
            // Decode row and check username/host
            let row = super::encoding::decode_row(&value)?;
            let uname = get_string(row.get_opt(0));
            let host = get_string(row.get_opt(1));

            if uname.as_ref() == Some(&self.username) && host.as_deref() == Some(self.host.as_str())
            {
                // Mark for deletion
                changeset.push(RowChange::delete(SYSTEM_USERS, key.clone()));
                self.changes.push(RowChange::delete(SYSTEM_USERS, key));
                found = true;
                break;
            }
        }

        if !found && !self.if_exists {
            return Err(ExecutorError::Internal(format!(
                "User '{}@{}' does not exist",
                self.username,
                self.host.as_str()
            )));
        }

        if found {
            // Also delete any grants for this user
            let grant_prefix = table_key_prefix(SYSTEM_GRANTS);
            let grant_end = table_key_end(SYSTEM_GRANTS);

            let grant_rows = self
                .storage
                .scan(Some(&grant_prefix), Some(&grant_end))
                .await
                .map_err(|e| ExecutorError::Internal(format!("Storage error: {}", e)))?;

            for (key, value) in grant_rows {
                let row = super::encoding::decode_row(&value)?;
                let grantee = get_string(row.get_opt(0));
                let grantee_host = get_string(row.get_opt(1));

                if grantee.as_ref() == Some(&self.username)
                    && grantee_host.as_deref() == Some(self.host.as_str())
                {
                    changeset.push(RowChange::delete(SYSTEM_GRANTS, key.clone()));
                    self.changes.push(RowChange::delete(SYSTEM_GRANTS, key));
                }
            }

            self.raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(if found { 1 } else { 0 })])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// GRANT executor
pub struct Grant {
    /// Privileges to grant
    privileges: Vec<Privilege>,
    /// Object to grant on
    object: PrivilegeObject,
    /// Grantee username
    grantee: String,
    /// Grantee host
    grantee_host: HostPattern,
    /// WITH GRANT OPTION
    with_grant_option: bool,
    /// Raft node for replication
    raft_node: Arc<RaftNode>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl Grant {
    /// Create a new GRANT executor
    pub fn new(
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
        with_grant_option: bool,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        Grant {
            privileges,
            object,
            grantee,
            grantee_host,
            with_grant_option,
            raft_node,
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for Grant {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        let mut changeset = ChangeSet::new(0);
        let mut count = 0;

        // Expand ALL PRIVILEGES if needed
        let privs: Vec<Privilege> = if self.privileges.contains(&Privilege::All) {
            Privilege::expand_all()
        } else {
            self.privileges.clone()
        };

        for priv_type in &privs {
            let (object_type, db_name, table_name) = match &self.object {
                PrivilegeObject::Global => ("GLOBAL".to_string(), Datum::Null, Datum::Null),
                PrivilegeObject::Database(db) => (
                    "DATABASE".to_string(),
                    Datum::String(db.clone()),
                    Datum::Null,
                ),
                PrivilegeObject::Table { database, table } => (
                    "TABLE".to_string(),
                    Datum::String(database.clone()),
                    Datum::String(table.clone()),
                ),
            };

            // Create grant row
            let grant_row = Row::new(vec![
                Datum::String(self.grantee.clone()),                   // grantee
                Datum::String(self.grantee_host.as_str().to_string()), // grantee_host
                Datum::String("USER".to_string()),                     // grantee_type
                Datum::String(priv_type.to_str().to_string()),         // privilege
                Datum::String(object_type),                            // object_type
                db_name,                                               // database_name
                table_name,                                            // table_name
                Datum::Bool(self.with_grant_option),                   // with_grant_option
                Datum::Null,                                           // granted_by
                Datum::Null,                                           // granted_at
            ]);

            let key = encode_row_key(SYSTEM_GRANTS, next_row_id());
            let value = encode_row(&grant_row);

            changeset.push(RowChange::insert(SYSTEM_GRANTS, key.clone(), value.clone()));
            self.changes
                .push(RowChange::insert(SYSTEM_GRANTS, key, value));
            count += 1;
        }

        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(count)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// REVOKE executor
pub struct Revoke {
    /// Privileges to revoke
    privileges: Vec<Privilege>,
    /// Object to revoke from
    object: PrivilegeObject,
    /// Grantee username
    grantee: String,
    /// Grantee host
    grantee_host: HostPattern,
    /// Raft node for replication
    raft_node: Arc<RaftNode>,
    /// Storage for scanning
    storage: Arc<dyn crate::storage::StorageEngine>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl Revoke {
    /// Create a new REVOKE executor
    pub fn new(
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
        raft_node: Arc<RaftNode>,
        storage: Arc<dyn crate::storage::StorageEngine>,
    ) -> Self {
        Revoke {
            privileges,
            object,
            grantee,
            grantee_host,
            raft_node,
            storage,
            done: false,
            changes: Vec::new(),
        }
    }

    fn matches_object(&self, row: &Row) -> bool {
        // Check if grant row matches our object
        let object_type = get_string(row.get_opt(4));
        let db_name = get_string(row.get_opt(5));
        let table_name = get_string(row.get_opt(6));

        match (&self.object, object_type.as_deref()) {
            (PrivilegeObject::Global, Some("GLOBAL")) => true,
            (PrivilegeObject::Database(db), Some("DATABASE")) => {
                db_name.as_ref().map(|d| d == db).unwrap_or(false)
            }
            (PrivilegeObject::Table { database, table }, Some("TABLE")) => {
                db_name.as_ref().map(|d| d == database).unwrap_or(false)
                    && table_name.as_ref().map(|t| t == table).unwrap_or(false)
            }
            _ => false,
        }
    }
}

#[async_trait]
impl Executor for Revoke {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Expand ALL PRIVILEGES if needed
        let privs_to_revoke: Vec<String> = if self.privileges.contains(&Privilege::All) {
            Privilege::expand_all()
                .iter()
                .map(|p| p.to_str().to_string())
                .collect()
        } else {
            self.privileges
                .iter()
                .map(|p| p.to_str().to_string())
                .collect()
        };

        // Find matching grant rows
        let prefix = table_key_prefix(SYSTEM_GRANTS);
        let end = table_key_end(SYSTEM_GRANTS);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(|e| ExecutorError::Internal(format!("Storage error: {}", e)))?;

        let mut changeset = ChangeSet::new(0);
        let mut count = 0;

        for (key, value) in rows {
            let row = super::encoding::decode_row(&value)?;

            // Check grantee
            let grantee = get_string(row.get_opt(0));
            let grantee_host = get_string(row.get_opt(1));
            let privilege = get_string(row.get_opt(3));

            if grantee
                .as_ref()
                .map(|g| g == &self.grantee)
                .unwrap_or(false)
                && grantee_host
                    .as_deref()
                    .map(|h| h == self.grantee_host.as_str())
                    .unwrap_or(false)
                && self.matches_object(&row)
                && privilege
                    .as_ref()
                    .map(|p| privs_to_revoke.contains(p))
                    .unwrap_or(false)
            {
                changeset.push(RowChange::delete(SYSTEM_GRANTS, key.clone()));
                self.changes.push(RowChange::delete(SYSTEM_GRANTS, key));
                count += 1;
            }
        }

        if count > 0 {
            self.raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(count)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// SHOW GRANTS executor
pub struct ShowGrants {
    /// User to show grants for (None = current user)
    for_user: Option<(String, HostPattern)>,
    /// Storage for scanning
    storage: Arc<dyn crate::storage::StorageEngine>,
    /// Buffered rows
    rows: Vec<Row>,
    /// Current position
    position: usize,
    /// Whether we've scanned
    scanned: bool,
}

impl ShowGrants {
    /// Create a new SHOW GRANTS executor
    pub fn new(
        for_user: Option<(String, HostPattern)>,
        storage: Arc<dyn crate::storage::StorageEngine>,
    ) -> Self {
        ShowGrants {
            for_user,
            storage,
            rows: Vec::new(),
            position: 0,
            scanned: false,
        }
    }
}

#[async_trait]
impl Executor for ShowGrants {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows.clear();
        self.position = 0;
        self.scanned = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if !self.scanned {
            // Scan grants table
            let prefix = table_key_prefix(SYSTEM_GRANTS);
            let end = table_key_end(SYSTEM_GRANTS);

            let rows = self
                .storage
                .scan(Some(&prefix), Some(&end))
                .await
                .map_err(|e| ExecutorError::Internal(format!("Storage error: {}", e)))?;

            for (_key, value) in rows {
                let row = super::encoding::decode_row(&value)?;

                let grantee = get_string(row.get_opt(0));
                let grantee_host = get_string(row.get_opt(1));

                // Check if this grant is for the requested user
                if let Some((ref username, ref host)) = self.for_user {
                    if grantee.as_ref().map(|g| g != username).unwrap_or(true)
                        || grantee_host
                            .as_deref()
                            .map(|h| h != host.as_str())
                            .unwrap_or(true)
                    {
                        continue;
                    }
                }

                // Format grant as "GRANT privilege ON object TO user@host"
                let privilege = get_string(row.get_opt(3));
                let object_type = get_string(row.get_opt(4));
                let db_name = get_string(row.get_opt(5));
                let table_name = get_string(row.get_opt(6));
                let with_grant = get_bool(row.get_opt(7)).unwrap_or(false);

                // Format object
                let object_str = match object_type.as_deref() {
                    Some("GLOBAL") => "*.*".to_string(),
                    Some("DATABASE") => format!("{}.*", db_name.unwrap_or_default()),
                    Some("TABLE") => format!(
                        "{}.{}",
                        db_name.unwrap_or_default(),
                        table_name.unwrap_or_default()
                    ),
                    _ => "*.*".to_string(),
                };

                let grant_str = format!(
                    "GRANT {} ON {} TO '{}'@'{}'{}",
                    privilege.unwrap_or_default(),
                    object_str,
                    grantee.unwrap_or_default(),
                    grantee_host.unwrap_or_else(|| DEFAULT_HOST.to_string()),
                    if with_grant { " WITH GRANT OPTION" } else { "" }
                );

                self.rows.push(Row::new(vec![Datum::String(grant_str)]));
            }

            self.scanned = true;
        }

        if self.position >= self.rows.len() {
            return Ok(None);
        }

        let row = self.rows[self.position].clone();
        self.position += 1;
        Ok(Some(row))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }
}
