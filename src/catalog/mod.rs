//! Catalog - Schema metadata (tables, columns, indexes)
//!
//! The catalog stores metadata about database schema including
//! table definitions, column types, constraints, and indexes.
//!
//! Schema is persisted in system tables (system.tables, system.columns, system.indexes)
//! and the Catalog is a cache rebuilt from those tables on startup.

pub mod system_tables;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    /// 64-bit unsigned integer
    BigIntUnsigned,
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
    /// Bit string with width 1..64
    Bit(u8),
    /// Timestamp (date and time)
    Timestamp,
    /// Fixed-point decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Spatial geometry (stored as WKB binary)
    Geometry,
    /// JSON document
    Json,
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
                | DataType::BigIntUnsigned
                | DataType::Float
                | DataType::Double
                | DataType::Bit(_)
                | DataType::Decimal { .. }
        )
    }

    /// Check if this type is an integer
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Int
                | DataType::BigInt
                | DataType::BigIntUnsigned
        )
    }

    /// Check if this type is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Varchar(_) | DataType::Text | DataType::Json)
    }

    /// Return the MySQL-compatible SQL name for use in CAST column headers.
    pub fn sql_name(&self) -> String {
        match self {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::TinyInt => "TINYINT".to_string(),
            DataType::SmallInt => "SMALLINT".to_string(),
            DataType::Int => "INT".to_string(),
            DataType::BigInt => "BIGINT".to_string(),
            DataType::BigIntUnsigned => "unsigned".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Double => "DOUBLE".to_string(),
            DataType::Varchar(n) => format!("CHAR({})", n),
            DataType::Text => "CHAR".to_string(),
            DataType::Blob => "BINARY".to_string(),
            DataType::Bit(w) => format!("BIT({})", w),
            DataType::Timestamp => "DATETIME".to_string(),
            DataType::Decimal { precision, scale } => {
                format!("DECIMAL({},{})", precision, scale)
            }
            DataType::Geometry => "GEOMETRY".to_string(),
            DataType::Json => "JSON".to_string(),
        }
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
        name: Option<String>,
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
    /// Whether this is a temporary table (session-scoped, not persisted)
    pub is_temporary: bool,
}

impl TableDef {
    /// Create a new table definition
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            constraints: Vec::new(),
            is_temporary: false,
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

/// Parameter mode for stored procedure parameters
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParamMode {
    In,
    Out,
    InOut,
}

/// Stored procedure parameter
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureParam {
    pub name: String,
    pub data_type: String,
    pub mode: ParamMode,
}

/// Stored procedure / function definition
#[derive(Debug, Clone)]
pub struct ProcedureDef {
    pub name: String,
    pub params: Vec<ProcedureParam>,
    pub body_sql: String,
    /// For CREATE FUNCTION: return type (e.g. "BIGINT"). None for procedures.
    pub returns_type: Option<String>,
}

/// View definition
#[derive(Debug, Clone)]
pub struct ViewDef {
    pub name: String,
    pub query_sql: String,
}

/// Trigger timing (BEFORE or AFTER)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
}

/// Trigger event (INSERT, UPDATE, DELETE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// Trigger definition — stores the parsed body as an AST statement
#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub name: String,
    pub table_name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    /// The trigger body as a parsed sqlparser Statement
    pub body: sqlparser::ast::Statement,
    /// Original SQL for SHOW CREATE TRIGGER
    pub create_sql: String,
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
    /// Invalid name (empty or otherwise invalid)
    InvalidName(String),
    /// Invalid constraint definition
    InvalidConstraint(String),
    /// Procedure already exists
    ProcedureExists(String),
    /// Procedure not found
    ProcedureNotFound(String),
    /// View already exists
    ViewExists(String),
    /// View not found
    ViewNotFound(String),
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
            CatalogError::InvalidName(msg) => write!(f, "Invalid name: {}", msg),
            CatalogError::InvalidConstraint(msg) => write!(f, "Invalid constraint: {}", msg),
            CatalogError::ProcedureExists(name) => {
                write!(f, "Procedure '{}' already exists", name)
            }
            CatalogError::ProcedureNotFound(name) => {
                write!(f, "Procedure '{}' not found", name)
            }
            CatalogError::ViewExists(name) => {
                write!(f, "View '{}' already exists", name)
            }
            CatalogError::ViewNotFound(name) => {
                write!(f, "View '{}' not found", name)
            }
        }
    }
}

impl std::error::Error for CatalogError {}

/// Result type for catalog operations
pub type CatalogResult<T> = Result<T, CatalogError>;

/// Catalog error for database operations
#[derive(Debug, Clone)]
pub enum DatabaseError {
    /// Database already exists
    DatabaseExists(String),
    /// Database not found
    DatabaseNotFound(String),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::DatabaseExists(name) => {
                write!(f, "Can't create database '{}'; database exists", name)
            }
            DatabaseError::DatabaseNotFound(name) => {
                write!(f, "Unknown database '{}'", name)
            }
        }
    }
}

impl std::error::Error for DatabaseError {}

/// Database catalog - stores schema metadata
#[derive(Debug, Default)]
pub struct Catalog {
    /// Tables by name
    tables: HashMap<String, TableDef>,
    /// Indexes by name
    indexes: HashMap<String, IndexDef>,
    /// Schema version counter (incremented on DDL operations)
    schema_version: u64,
    /// Known databases
    databases: HashSet<String>,
    /// Stored procedures by name
    procedures: HashMap<String, ProcedureDef>,
    /// Views by name
    views: HashMap<String, ViewDef>,
    /// Triggers by name
    triggers: HashMap<String, TriggerDef>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        let mut databases = HashSet::new();
        databases.insert("default".to_string());
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            schema_version: 0,
            databases,
            procedures: HashMap::new(),
            views: HashMap::new(),
            triggers: HashMap::new(),
        }
    }

    /// Get the current schema version (incremented on DDL changes)
    pub fn schema_version(&self) -> u64 {
        self.schema_version
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

    // ============ Database operations ============

    /// Create a database
    pub fn create_database(&mut self, name: &str) -> Result<(), DatabaseError> {
        if self.databases.contains(name) {
            return Err(DatabaseError::DatabaseExists(name.to_string()));
        }
        self.databases.insert(name.to_string());
        self.schema_version += 1;
        Ok(())
    }

    /// Drop a database
    pub fn drop_database(&mut self, name: &str) -> Result<(), DatabaseError> {
        if !self.databases.remove(name) {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }
        self.schema_version += 1;
        Ok(())
    }

    /// Check if a database exists
    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.contains(name)
    }

    /// List all databases (sorted)
    pub fn list_databases(&self) -> Vec<String> {
        let mut dbs: Vec<String> = self.databases.iter().cloned().collect();
        dbs.sort();
        dbs
    }

    /// Get tables that belong to a specific database
    ///
    /// Currently returns all non-system user tables (since tables don't have
    /// database prefixes yet). When qualified names are added, this will filter
    /// by database prefix.
    pub fn get_tables_in_database(&self, _db: &str) -> Vec<String> {
        let mut tables: Vec<String> = self
            .tables
            .keys()
            .filter(|name| !system_tables::is_system_table(name))
            .cloned()
            .collect();
        tables.sort();
        tables
    }

    /// Register a database directly (for rebuilding from storage)
    pub fn register_database(&mut self, name: String) {
        self.databases.insert(name);
    }

    /// Clear all non-default databases (for re-initialization)
    pub fn clear_user_databases(&mut self) {
        self.databases.retain(|name| name == "default");
    }

    // ============ Table operations ============

    /// Create a table
    pub fn create_table(&mut self, def: TableDef) -> CatalogResult<()> {
        // Validate table name
        if def.name.is_empty() {
            return Err(CatalogError::InvalidName(
                "Table name cannot be empty".into(),
            ));
        }

        // Validate constraint columns reference existing columns
        for constraint in &def.constraints {
            let cols = match constraint {
                Constraint::PrimaryKey(cols) | Constraint::Unique(cols) => cols,
                Constraint::ForeignKey { columns, .. } => columns,
                Constraint::Check(_) => continue,
            };
            if cols.is_empty() {
                return Err(CatalogError::InvalidConstraint(
                    "Constraint columns cannot be empty".into(),
                ));
            }
            for col in cols {
                if def.get_column(col).is_none() {
                    return Err(CatalogError::ColumnNotFound(def.name.clone(), col.clone()));
                }
            }
        }

        // Validate FK ref_table and ref_columns exist
        for constraint in &def.constraints {
            if let Constraint::ForeignKey {
                ref_table,
                ref_columns,
                ..
            } = constraint
            {
                if let Some(ref_def) = self.tables.get(ref_table) {
                    for rc in ref_columns {
                        if ref_def.get_column(rc).is_none() {
                            return Err(CatalogError::InvalidConstraint(format!(
                                "Foreign key references non-existent column '{}' in table '{}'",
                                rc, ref_table
                            )));
                        }
                    }
                } else {
                    return Err(CatalogError::InvalidConstraint(format!(
                        "Foreign key references non-existent table '{}'",
                        ref_table
                    )));
                }
            }
        }

        if self.tables.contains_key(&def.name) {
            return Err(CatalogError::TableExists(def.name.clone()));
        }
        self.tables.insert(def.name.clone(), def);
        self.schema_version += 1;
        Ok(())
    }

    /// Replace a table definition atomically (used by ALTER TABLE)
    ///
    /// Swaps the entire table definition after applying ALTER operations.
    /// The old definition is replaced with the new one. If the table name
    /// changed (RENAME), the old entry is removed and the new one inserted.
    pub fn replace_table(&mut self, old_name: &str, def: TableDef) {
        if old_name != def.name {
            self.tables.remove(old_name);
        }
        self.tables.insert(def.name.clone(), def);
        self.schema_version += 1;
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> CatalogResult<()> {
        if self.tables.remove(name).is_none() {
            return Err(CatalogError::TableNotFound(name.to_string()));
        }
        // Also drop all indexes for this table
        self.indexes.retain(|_, idx| idx.table != name);
        self.schema_version += 1;
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
        // Verify table exists and get reference
        let table = self
            .tables
            .get(&def.table)
            .ok_or_else(|| CatalogError::TableNotFound(def.table.clone()))?;
        for col in &def.columns {
            if table.get_column(col).is_none() {
                return Err(CatalogError::ColumnNotFound(def.table.clone(), col.clone()));
            }
        }

        if self.indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name.clone()));
        }
        self.indexes.insert(def.name.clone(), def);
        self.schema_version += 1;
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> CatalogResult<()> {
        if self.indexes.remove(name).is_none() {
            return Err(CatalogError::IndexNotFound(name.to_string()));
        }
        self.schema_version += 1;
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

    // ============ Procedure operations ============

    /// Create a stored procedure
    pub fn create_procedure(&mut self, def: ProcedureDef) -> CatalogResult<()> {
        if def.name.is_empty() {
            return Err(CatalogError::InvalidName(
                "Procedure name cannot be empty".into(),
            ));
        }
        if self.procedures.contains_key(&def.name) {
            return Err(CatalogError::ProcedureExists(def.name.clone()));
        }
        self.procedures.insert(def.name.clone(), def);
        self.schema_version += 1;
        Ok(())
    }

    /// Drop a stored procedure
    pub fn drop_procedure(&mut self, name: &str) -> CatalogResult<()> {
        if self.procedures.remove(name).is_none() {
            return Err(CatalogError::ProcedureNotFound(name.to_string()));
        }
        self.schema_version += 1;
        Ok(())
    }

    /// Get a stored procedure definition
    pub fn get_procedure(&self, name: &str) -> Option<&ProcedureDef> {
        self.procedures.get(name)
    }

    /// Register a procedure directly (for rebuilding from storage)
    pub fn register_procedure(&mut self, def: ProcedureDef) {
        self.procedures.insert(def.name.clone(), def);
    }

    /// Check if a procedure exists
    pub fn procedure_exists(&self, name: &str) -> bool {
        self.procedures.contains_key(name)
    }

    /// Clear all procedures (for re-initialization)
    pub fn clear_procedures(&mut self) {
        self.procedures.clear();
    }

    // ============ View operations ============

    /// Create a view
    pub fn create_view(&mut self, def: ViewDef) -> CatalogResult<()> {
        if self.views.contains_key(&def.name) {
            return Err(CatalogError::ViewExists(def.name.clone()));
        }
        self.views.insert(def.name.clone(), def);
        self.schema_version += 1;
        Ok(())
    }

    /// Create or replace a view
    pub fn replace_view(&mut self, def: ViewDef) {
        self.views.insert(def.name.clone(), def);
        self.schema_version += 1;
    }

    /// Drop a view
    pub fn drop_view(&mut self, name: &str, if_exists: bool) -> CatalogResult<()> {
        match self.views.remove(name) {
            Some(_) => {
                self.schema_version += 1;
                Ok(())
            }
            None if if_exists => Ok(()),
            None => Err(CatalogError::ViewNotFound(name.to_string())),
        }
    }

    /// Get a view definition
    pub fn get_view(&self, name: &str) -> Option<&ViewDef> {
        self.views.get(name)
    }

    /// Check if a view exists
    pub fn view_exists(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// Register a view directly (used during catalog rebuild from system tables)
    pub fn register_view(&mut self, def: ViewDef) {
        self.views.insert(def.name.clone(), def);
    }

    /// Clear all views (used before catalog rebuild)
    pub fn clear_views(&mut self) {
        self.views.clear();
    }

    /// Get all view names in current database
    pub fn get_view_names(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    // ============ Trigger operations ============

    /// Create a trigger
    pub fn create_trigger(&mut self, def: TriggerDef) {
        self.triggers.insert(def.name.clone(), def);
        self.schema_version += 1;
    }

    /// Drop a trigger
    pub fn drop_trigger(&mut self, name: &str) -> CatalogResult<()> {
        if self.triggers.remove(name).is_none() {
            return Err(CatalogError::ViewNotFound(format!(
                "Trigger '{}' not found",
                name
            )));
        }
        self.schema_version += 1;
        Ok(())
    }

    /// Get triggers for a specific table, timing, and event
    pub fn get_triggers(
        &self,
        table_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
    ) -> Vec<&TriggerDef> {
        self.triggers
            .values()
            .filter(|t| t.table_name == table_name && t.timing == timing && t.event == event)
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
                name: Some("fk_user".to_string()),
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
