//! The name of a table (or view, or anything else that lives in a
//! database): which database, and what it is called there.

use serde::{Deserialize, Serialize};

/// A table's name: the database it is in, and its name within it. Two
/// tables of the same name in different databases are different tables,
/// and a dot inside either part is just a character of that part.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableName {
    /// The database. Empty for a name that belongs to none: the tables of
    /// a catalog used without a server (tests, tools).
    pub database: String,
    /// The table's name within its database.
    pub table: String,
}

impl TableName {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    /// A name in no database.
    pub fn bare(table: impl Into<String>) -> Self {
        Self::new("", table)
    }

    /// Whether the name is in database `database`.
    pub fn is_in(&self, database: &str) -> bool {
        self.database == database
    }
}

/// As messages show it: `database.table`, or the bare name if it is in no
/// database.
impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.database.is_empty() {
            f.write_str(&self.table)
        } else {
            write!(f, "{}.{}", self.database, self.table)
        }
    }
}

/// A bare name from a literal, for catalogs used without databases (tests
/// and tools). Names from statements are never made this way: they come
/// from the resolver, with their database.
impl From<&str> for TableName {
    fn from(table: &str) -> Self {
        Self::bare(table)
    }
}

impl From<&TableName> for TableName {
    fn from(name: &TableName) -> Self {
        name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dot_inside_a_name_is_part_of_the_name() {
        let plain = TableName::new("a", "b.c");
        let other = TableName::new("a.b", "c");
        assert_ne!(plain, other);
        assert!(plain.is_in("a") && !other.is_in("a"));
        // They read the same in a message; they are not the same name.
        assert_eq!(plain.to_string(), other.to_string());
        assert_eq!(TableName::bare("t").to_string(), "t");
        assert_eq!(TableName::from("t"), TableName::bare("t"));
    }
}
