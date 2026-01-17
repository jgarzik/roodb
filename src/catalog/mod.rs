//! Catalog - Schema metadata (tables, columns, indexes)
//!
//! The catalog stores metadata about database schema including
//! table definitions, column types, constraints, and indexes.
//!
//! Schema is persisted in system tables (system.tables, system.columns, system.indexes)
//! and the Catalog is a cache rebuilt from those tables on startup.

pub mod system_tables;

use std::collections::HashMap;

/// SQL data types supported by the database
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    /// Boolean (true/false)
    Boolean,
    /// 8-bit signed integer
    TinyInt,
    /// 16-bit signed integer
    SmallInt,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    BigInt,
    /// 32-bit floating point
    Float,
    /// 64-bit floating point
    Double,
    /// Variable-length string with max length
    Varchar(u32),
    /// Unlimited text
    Text,
    /// Binary data
    Blob,
    /// Timestamp (date and time)
    Timestamp,
}

impl DataType {
    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Int
                | DataType::BigInt
                | DataType::Float
                | DataType::Double
        )
    }

    /// Check if this type is an integer
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
        )
    }

    /// Check if this type is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Varchar(_) | DataType::Text)
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether NULL values are allowed
    pub nullable: bool,
    /// Default value expression (as string)
    pub default: Option<String>,
    /// Auto-increment column
    pub auto_increment: bool,
}

impl ColumnDef {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            default: None,
            auto_increment: false,
        }
    }

    /// Set nullable
    #[must_use]
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set default value
    #[must_use]
    pub fn default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }

    /// Set auto-increment
    #[must_use]
    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }
}

/// Table constraint
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    /// Primary key constraint
    PrimaryKey(Vec<String>),
    /// Unique constraint
    Unique(Vec<String>),
    /// Foreign key constraint
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    /// Check constraint (expression as string)
    Check(String),
}

/// Table definition
#[derive(Debug, Clone)]
pub struct TableDef {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Table constraints
    pub constraints: Vec<Constraint>,
}

impl TableDef {
    /// Create a new table definition
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            constraints: Vec::new(),
        }
    }

    /// Add a column
    #[must_use]
    pub fn column(mut self, col: ColumnDef) -> Self {
        self.columns.push(col);
        self
    }

    /// Add a constraint
    #[must_use]
    pub fn constraint(mut self, constraint: Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get primary key columns
    pub fn primary_key(&self) -> Option<&[String]> {
        for c in &self.constraints {
            if let Constraint::PrimaryKey(cols) = c {
                return Some(cols);
            }
        }
        None
    }
}

/// Index definition
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Table this index belongs to
    pub table: String,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
}

impl IndexDef {
    /// Create a new index definition
    pub fn new(name: impl Into<String>, table: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns,
            unique: false,
        }
    }

    /// Set unique
    #[must_use]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Catalog error
#[derive(Debug, Clone)]
pub enum CatalogError {
    /// Table already exists
    TableExists(String),
    /// Table not found
    TableNotFound(String),
    /// Index already exists
    IndexExists(String),
    /// Index not found
    IndexNotFound(String),
    /// Column not found
    ColumnNotFound(String, String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableExists(name) => write!(f, "Table '{}' already exists", name),
            CatalogError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            CatalogError::IndexExists(name) => write!(f, "Index '{}' already exists", name),
            CatalogError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            CatalogError::ColumnNotFound(table, col) => {
                write!(f, "Column '{}' not found in table '{}'", col, table)
            }
        }
    }
}

impl std::error::Error for CatalogError {}

/// Result type for catalog operations
pub type CatalogResult<T> = Result<T, CatalogError>;

/// Database catalog - stores schema metadata
#[derive(Debug, Default)]
pub struct Catalog {
    /// Tables by name
    tables: HashMap<String, TableDef>,
    /// Indexes by name
    indexes: HashMap<String, IndexDef>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Create a catalog with system tables pre-populated
    ///
    /// This should be used on server startup to bootstrap the catalog
    /// with the system table definitions that store schema metadata.
    pub fn with_system_tables() -> Self {
        let mut catalog = Self::new();
        for table_def in system_tables::bootstrap_system_tables() {
            catalog.tables.insert(table_def.name.clone(), table_def);
        }
        catalog
    }

    /// Register a table directly without validation (for rebuilding from storage)
    ///
    /// This bypasses the normal create_table checks and is used when
    /// rebuilding the catalog cache from system tables on startup.
    pub fn register_table(&mut self, def: TableDef) {
        self.tables.insert(def.name.clone(), def);
    }

    /// Register an index directly without validation (for rebuilding from storage)
    ///
    /// This bypasses the normal create_index checks and is used when
    /// rebuilding the catalog cache from system tables on startup.
    pub fn register_index(&mut self, def: IndexDef) {
        self.indexes.insert(def.name.clone(), def);
    }

    /// Clear all non-system tables (for testing or re-initialization)
    pub fn clear_user_tables(&mut self) {
        self.tables
            .retain(|name, _| system_tables::is_system_table(name));
        self.indexes.clear();
    }

    /// Create a table
    pub fn create_table(&mut self, def: TableDef) -> CatalogResult<()> {
        if self.tables.contains_key(&def.name) {
            return Err(CatalogError::TableExists(def.name.clone()));
        }
        self.tables.insert(def.name.clone(), def);
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> CatalogResult<()> {
        if self.tables.remove(name).is_none() {
            return Err(CatalogError::TableNotFound(name.to_string()));
        }
        // Also drop all indexes for this table
        self.indexes.retain(|_, idx| idx.table != name);
        Ok(())
    }

    /// Get a table definition
    pub fn get_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }

    /// Get a mutable table definition
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableDef> {
        self.tables.get_mut(name)
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Get all table names as a HashSet (for efficient duplicate checking)
    pub fn table_names(&self) -> std::collections::HashSet<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get all index names as a HashSet (for efficient duplicate checking)
    pub fn index_names(&self) -> std::collections::HashSet<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Create an index
    pub fn create_index(&mut self, def: IndexDef) -> CatalogResult<()> {
        // Verify table exists
        if !self.tables.contains_key(&def.table) {
            return Err(CatalogError::TableNotFound(def.table.clone()));
        }

        // Verify columns exist
        let table = self.tables.get(&def.table).unwrap();
        for col in &def.columns {
            if table.get_column(col).is_none() {
                return Err(CatalogError::ColumnNotFound(def.table.clone(), col.clone()));
            }
        }

        if self.indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name.clone()));
        }
        self.indexes.insert(def.name.clone(), def);
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> CatalogResult<()> {
        if self.indexes.remove(name).is_none() {
            return Err(CatalogError::IndexNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Get an index definition
    pub fn get_index(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.get(name)
    }

    /// Get all indexes for a table
    pub fn get_indexes_for_table(&self, table: &str) -> Vec<&IndexDef> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_create_drop_table() {
        let mut catalog = Catalog::new();

        let table = TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("name", DataType::Varchar(255)))
            .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

        // Create table
        catalog.create_table(table).unwrap();
        assert!(catalog.table_exists("users"));

        // Duplicate should fail
        let table2 = TableDef::new("users");
        assert!(matches!(
            catalog.create_table(table2),
            Err(CatalogError::TableExists(_))
        ));

        // Get table
        let t = catalog.get_table("users").unwrap();
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.primary_key(), Some(&["id".to_string()][..]));

        // List tables
        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"users"));

        // Drop table
        catalog.drop_table("users").unwrap();
        assert!(!catalog.table_exists("users"));

        // Drop non-existent should fail
        assert!(matches!(
            catalog.drop_table("users"),
            Err(CatalogError::TableNotFound(_))
        ));
    }

    #[test]
    fn test_catalog_column_types() {
        let table = TableDef::new("test")
            .column(ColumnDef::new("bool_col", DataType::Boolean))
            .column(ColumnDef::new("int_col", DataType::Int).nullable(false))
            .column(
                ColumnDef::new("str_col", DataType::Varchar(100)).default("'default'".to_string()),
            )
            .column(ColumnDef::new("auto_col", DataType::BigInt).auto_increment());

        assert_eq!(table.columns.len(), 4);

        let int_col = table.get_column("int_col").unwrap();
        assert!(!int_col.nullable);
        assert!(int_col.data_type.is_integer());
        assert!(int_col.data_type.is_numeric());

        let str_col = table.get_column("str_col").unwrap();
        assert!(str_col.data_type.is_string());
        assert_eq!(str_col.default, Some("'default'".to_string()));

        let auto_col = table.get_column("auto_col").unwrap();
        assert!(auto_col.auto_increment);

        assert!(table.get_column("nonexistent").is_none());
        assert_eq!(table.get_column_index("int_col"), Some(1));
    }

    #[test]
    fn test_catalog_constraints() {
        let table = TableDef::new("orders")
            .column(ColumnDef::new("id", DataType::Int))
            .column(ColumnDef::new("user_id", DataType::Int))
            .column(ColumnDef::new("status", DataType::Varchar(50)))
            .constraint(Constraint::PrimaryKey(vec!["id".to_string()]))
            .constraint(Constraint::ForeignKey {
                columns: vec!["user_id".to_string()],
                ref_table: "users".to_string(),
                ref_columns: vec!["id".to_string()],
            })
            .constraint(Constraint::Unique(vec![
                "user_id".to_string(),
                "status".to_string(),
            ]))
            .constraint(Constraint::Check(
                "status IN ('pending', 'complete')".to_string(),
            ));

        assert_eq!(table.constraints.len(), 4);
        assert_eq!(table.primary_key(), Some(&["id".to_string()][..]));
    }

    #[test]
    fn test_catalog_indexes() {
        let mut catalog = Catalog::new();

        // Create table first
        let table = TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int))
            .column(ColumnDef::new("email", DataType::Varchar(255)))
            .column(ColumnDef::new("name", DataType::Varchar(100)));

        catalog.create_table(table).unwrap();

        // Create index
        let idx = IndexDef::new("idx_email", "users", vec!["email".to_string()]).unique();
        catalog.create_index(idx).unwrap();

        // Get index
        let i = catalog.get_index("idx_email").unwrap();
        assert!(i.unique);
        assert_eq!(i.columns, vec!["email".to_string()]);

        // Create another index
        let idx2 = IndexDef::new("idx_name", "users", vec!["name".to_string()]);
        catalog.create_index(idx2).unwrap();

        // Get indexes for table
        let indexes = catalog.get_indexes_for_table("users");
        assert_eq!(indexes.len(), 2);

        // Duplicate index should fail
        let idx3 = IndexDef::new("idx_email", "users", vec!["name".to_string()]);
        assert!(matches!(
            catalog.create_index(idx3),
            Err(CatalogError::IndexExists(_))
        ));

        // Index on non-existent table should fail
        let idx4 = IndexDef::new("idx_bad", "nonexistent", vec!["col".to_string()]);
        assert!(matches!(
            catalog.create_index(idx4),
            Err(CatalogError::TableNotFound(_))
        ));

        // Index on non-existent column should fail
        let idx5 = IndexDef::new("idx_bad2", "users", vec!["nonexistent".to_string()]);
        assert!(matches!(
            catalog.create_index(idx5),
            Err(CatalogError::ColumnNotFound(_, _))
        ));

        // Drop index
        catalog.drop_index("idx_email").unwrap();
        assert!(catalog.get_index("idx_email").is_none());

        // Drop non-existent index should fail
        assert!(matches!(
            catalog.drop_index("idx_email"),
            Err(CatalogError::IndexNotFound(_))
        ));

        // Drop table should also drop its indexes
        catalog.drop_table("users").unwrap();
        let indexes = catalog.get_indexes_for_table("users");
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_data_type_helpers() {
        assert!(DataType::Int.is_numeric());
        assert!(DataType::BigInt.is_integer());
        assert!(DataType::Float.is_numeric());
        assert!(!DataType::Float.is_integer());
        assert!(DataType::Varchar(100).is_string());
        assert!(DataType::Text.is_string());
        assert!(!DataType::Blob.is_string());
        assert!(!DataType::Boolean.is_numeric());
    }
}
