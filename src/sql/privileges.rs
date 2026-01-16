//! MySQL-compatible privilege system
//!
//! Implements privilege types and checking at multiple levels:
//! - Global: `*.*` - all databases, all tables
//! - Database: `db.*` - all tables in database
//! - Table: `db.table` - specific table
//! - Column: `db.table(col1,col2)` - specific columns (future)
//! - Routine: `db.procedure` - stored procedures (future)

use std::net::IpAddr;

/// Privilege types supported by RooDB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Privilege {
    /// Full access (expands to all privileges)
    All,
    /// Read data
    Select,
    /// Add data
    Insert,
    /// Modify data
    Update,
    /// Remove data
    Delete,
    /// Create tables/databases
    Create,
    /// Remove tables/databases
    Drop,
    /// Modify table structure
    Alter,
    /// Create/drop indexes
    Index,
    /// Grant privileges to others
    GrantOption,
}

impl Privilege {
    /// Parse privilege name from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ALL" | "ALL PRIVILEGES" => Some(Privilege::All),
            "SELECT" => Some(Privilege::Select),
            "INSERT" => Some(Privilege::Insert),
            "UPDATE" => Some(Privilege::Update),
            "DELETE" => Some(Privilege::Delete),
            "CREATE" => Some(Privilege::Create),
            "DROP" => Some(Privilege::Drop),
            "ALTER" => Some(Privilege::Alter),
            "INDEX" => Some(Privilege::Index),
            "GRANT OPTION" => Some(Privilege::GrantOption),
            _ => None,
        }
    }

    /// Convert to string representation for storage
    pub fn to_str(&self) -> &'static str {
        match self {
            Privilege::All => "ALL",
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Create => "CREATE",
            Privilege::Drop => "DROP",
            Privilege::Alter => "ALTER",
            Privilege::Index => "INDEX",
            Privilege::GrantOption => "GRANT OPTION",
        }
    }

    /// Expand ALL to list of individual privileges
    pub fn expand_all() -> Vec<Privilege> {
        vec![
            Privilege::Select,
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
            Privilege::Create,
            Privilege::Drop,
            Privilege::Alter,
            Privilege::Index,
        ]
    }
}

/// Object type for privilege grants
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeObject {
    /// Global: *.*
    Global,
    /// Database: db.*
    Database(String),
    /// Table: db.table
    Table { database: String, table: String },
}

impl PrivilegeObject {
    /// Parse from database and table names
    pub fn from_names(database: Option<&str>, table: Option<&str>) -> Self {
        match (database, table) {
            (None, None) => PrivilegeObject::Global,
            (Some(db), None) => PrivilegeObject::Database(db.to_string()),
            (Some(db), Some(tbl)) => PrivilegeObject::Table {
                database: db.to_string(),
                table: tbl.to_string(),
            },
            (None, Some(_)) => PrivilegeObject::Global, // Invalid, treat as global
        }
    }

    /// Check if this object covers another object (for privilege checking)
    /// e.g., Global covers Database, Database covers Table
    pub fn covers(&self, other: &PrivilegeObject) -> bool {
        match (self, other) {
            (PrivilegeObject::Global, _) => true,
            (PrivilegeObject::Database(db1), PrivilegeObject::Database(db2)) => db1 == db2,
            (PrivilegeObject::Database(db1), PrivilegeObject::Table { database, .. }) => {
                db1 == database
            }
            (
                PrivilegeObject::Table {
                    database: db1,
                    table: tbl1,
                },
                PrivilegeObject::Table {
                    database: db2,
                    table: tbl2,
                },
            ) => db1 == db2 && tbl1 == tbl2,
            _ => false,
        }
    }
}

/// Host pattern matching for MySQL-compatible 'user'@'host' syntax
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPattern(String);

impl HostPattern {
    /// Create a new host pattern
    pub fn new(pattern: impl Into<String>) -> Self {
        HostPattern(pattern.into())
    }

    /// Wildcard pattern matching any host
    pub fn any() -> Self {
        HostPattern("%".to_string())
    }

    /// Localhost pattern
    pub fn localhost() -> Self {
        HostPattern("localhost".to_string())
    }

    /// Get the pattern string
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Check if this pattern matches a client address
    pub fn matches(&self, client_host: &str, client_ip: Option<&IpAddr>) -> bool {
        let pattern = &self.0;

        // Exact match
        if pattern == client_host {
            return true;
        }

        // Wildcard matches everything
        if pattern == "%" {
            return true;
        }

        // localhost special handling
        if pattern == "localhost" {
            return client_host == "localhost"
                || client_host == "127.0.0.1"
                || client_host == "::1";
        }

        // IP pattern with % wildcard (e.g., "192.168.%")
        if pattern.contains('%') {
            let prefix = pattern.trim_end_matches('%');
            if client_host.starts_with(prefix) {
                return true;
            }
            if let Some(ip) = client_ip {
                if ip.to_string().starts_with(prefix) {
                    return true;
                }
            }
        }

        // IP address match
        if let Some(ip) = client_ip {
            if pattern == &ip.to_string() {
                return true;
            }
        }

        false
    }

    /// Check ordering for MySQL host precedence
    /// More specific patterns should be checked first
    pub fn specificity(&self) -> u8 {
        let pattern = &self.0;
        if pattern == "%" {
            0 // Least specific
        } else if pattern.contains('%') {
            1 // Wildcard pattern
        } else if pattern == "localhost" {
            2 // localhost
        } else {
            3 // Exact match (most specific)
        }
    }
}

/// A stored grant entry
#[derive(Debug, Clone)]
pub struct GrantEntry {
    pub grantee: String,
    pub grantee_host: HostPattern,
    pub privilege: Privilege,
    pub object: PrivilegeObject,
    pub with_grant_option: bool,
}

impl GrantEntry {
    /// Check if this grant applies to a user/host combination
    pub fn applies_to(&self, username: &str, host: &str, ip: Option<&IpAddr>) -> bool {
        self.grantee == username && self.grantee_host.matches(host, ip)
    }

    /// Check if this grant covers a privilege request
    pub fn covers(&self, privilege: Privilege, object: &PrivilegeObject) -> bool {
        // Check if the grant's object covers the requested object
        if !self.object.covers(object) {
            return false;
        }

        // Check privilege
        if self.privilege == Privilege::All {
            return true;
        }

        self.privilege == privilege
    }
}

/// Required privilege for an operation
#[derive(Debug, Clone)]
pub struct RequiredPrivilege {
    pub privilege: Privilege,
    pub object: PrivilegeObject,
}

impl RequiredPrivilege {
    pub fn new(privilege: Privilege, object: PrivilegeObject) -> Self {
        RequiredPrivilege { privilege, object }
    }

    /// Create for SELECT on a table
    pub fn select(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Select,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for INSERT on a table
    pub fn insert(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Insert,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for UPDATE on a table
    pub fn update(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Update,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for DELETE on a table
    pub fn delete(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Delete,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for CREATE TABLE in a database
    pub fn create_table(database: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Create,
            object: PrivilegeObject::Database(database.to_string()),
        }
    }

    /// Create for DROP TABLE
    pub fn drop_table(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Drop,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for CREATE INDEX
    pub fn create_index(database: &str, table: &str) -> Self {
        RequiredPrivilege {
            privilege: Privilege::Index,
            object: PrivilegeObject::Table {
                database: database.to_string(),
                table: table.to_string(),
            },
        }
    }

    /// Create for global privilege
    pub fn global(privilege: Privilege) -> Self {
        RequiredPrivilege {
            privilege,
            object: PrivilegeObject::Global,
        }
    }
}

/// Check if a set of grants satisfies a required privilege
pub fn check_privilege(grants: &[GrantEntry], required: &RequiredPrivilege) -> bool {
    for grant in grants {
        if grant.covers(required.privilege, &required.object) {
            return true;
        }
    }
    false
}

/// Check if a set of grants satisfies all required privileges
pub fn check_privileges(grants: &[GrantEntry], required: &[RequiredPrivilege]) -> bool {
    required.iter().all(|req| check_privilege(grants, req))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_pattern_matches() {
        // Wildcard matches everything
        let any = HostPattern::any();
        assert!(any.matches("example.com", None));
        assert!(any.matches("192.168.1.1", None));

        // Localhost matches local addresses
        let localhost = HostPattern::localhost();
        assert!(localhost.matches("localhost", None));
        assert!(localhost.matches("127.0.0.1", None));
        assert!(!localhost.matches("192.168.1.1", None));

        // IP prefix pattern
        let prefix = HostPattern::new("192.168.%");
        assert!(prefix.matches("192.168.1.1", None));
        assert!(prefix.matches("192.168.100.200", None));
        assert!(!prefix.matches("10.0.0.1", None));

        // Exact match
        let exact = HostPattern::new("db.example.com");
        assert!(exact.matches("db.example.com", None));
        assert!(!exact.matches("other.example.com", None));
    }

    #[test]
    fn test_privilege_object_covers() {
        let global = PrivilegeObject::Global;
        let db = PrivilegeObject::Database("mydb".to_string());
        let table = PrivilegeObject::Table {
            database: "mydb".to_string(),
            table: "users".to_string(),
        };

        // Global covers everything
        assert!(global.covers(&global));
        assert!(global.covers(&db));
        assert!(global.covers(&table));

        // Database covers same database and tables within it
        assert!(!db.covers(&global));
        assert!(db.covers(&db));
        assert!(db.covers(&table));

        // Table only covers same table
        assert!(!table.covers(&global));
        assert!(!table.covers(&db));
        assert!(table.covers(&table));

        // Different database doesn't cover
        let other_db = PrivilegeObject::Database("other".to_string());
        assert!(!db.covers(&other_db));
    }

    #[test]
    fn test_grant_covers() {
        let global_all = GrantEntry {
            grantee: "root".to_string(),
            grantee_host: HostPattern::any(),
            privilege: Privilege::All,
            object: PrivilegeObject::Global,
            with_grant_option: true,
        };

        // Global ALL covers everything
        assert!(global_all.covers(
            Privilege::Select,
            &PrivilegeObject::Table {
                database: "mydb".to_string(),
                table: "users".to_string()
            }
        ));

        let db_select = GrantEntry {
            grantee: "reader".to_string(),
            grantee_host: HostPattern::any(),
            privilege: Privilege::Select,
            object: PrivilegeObject::Database("mydb".to_string()),
            with_grant_option: false,
        };

        // Database SELECT covers table SELECT within that database
        assert!(db_select.covers(
            Privilege::Select,
            &PrivilegeObject::Table {
                database: "mydb".to_string(),
                table: "users".to_string()
            }
        ));

        // But not INSERT
        assert!(!db_select.covers(
            Privilege::Insert,
            &PrivilegeObject::Table {
                database: "mydb".to_string(),
                table: "users".to_string()
            }
        ));
    }

    #[test]
    fn test_check_privileges() {
        let grants = vec![
            GrantEntry {
                grantee: "app".to_string(),
                grantee_host: HostPattern::any(),
                privilege: Privilege::Select,
                object: PrivilegeObject::Database("appdb".to_string()),
                with_grant_option: false,
            },
            GrantEntry {
                grantee: "app".to_string(),
                grantee_host: HostPattern::any(),
                privilege: Privilege::Insert,
                object: PrivilegeObject::Database("appdb".to_string()),
                with_grant_option: false,
            },
        ];

        // Can SELECT from table in appdb
        assert!(check_privilege(
            &grants,
            &RequiredPrivilege::select("appdb", "users")
        ));

        // Can INSERT into table in appdb
        assert!(check_privilege(
            &grants,
            &RequiredPrivilege::insert("appdb", "orders")
        ));

        // Cannot DELETE
        assert!(!check_privilege(
            &grants,
            &RequiredPrivilege::delete("appdb", "users")
        ));

        // Cannot access other database
        assert!(!check_privilege(
            &grants,
            &RequiredPrivilege::select("otherdb", "users")
        ));
    }
}
