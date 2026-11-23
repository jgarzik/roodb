//! Catalog - Schema metadata (tables, columns, indexes)
//!
//! The catalog stores metadata about database schema including
//! table definitions, column types, constraints, and indexes.
//!
//! Schema is persisted in the system tables (the tables of the database
//! `mysql`: one record per table in `mysql.tables`, with its columns and
//! indexes), and the Catalog is a cache rebuilt from those tables on startup.

pub mod name;
pub mod store;

pub use name::TableName;

use crate::executor::rowcodec::TableLayout;
pub mod system_tables;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// SQL data types supported by the database
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Whether every value a column of this type can hold is a value of
    /// `other`, stored the same way: then a column can be given the type
    /// `other` without touching its rows. The same type, a wider integer,
    /// a longer string, or more digits before the point.
    pub fn values_fit(&self, other: &DataType) -> bool {
        fn integer_width(data_type: &DataType) -> Option<u8> {
            match data_type {
                DataType::TinyInt => Some(1),
                DataType::SmallInt => Some(2),
                DataType::Int => Some(4),
                DataType::BigInt => Some(8),
                _ => None,
            }
        }
        if self == other {
            return true;
        }
        match (self, other) {
            (DataType::Varchar(from), DataType::Varchar(to)) => from <= to,
            (DataType::Varchar(_), DataType::Text) => true,
            (DataType::Float, DataType::Double) => true,
            (
                DataType::Decimal {
                    precision: from,
                    scale: from_scale,
                },
                DataType::Decimal {
                    precision: to,
                    scale: to_scale,
                },
            ) => from_scale == to_scale && from <= to,
            _ => match (integer_width(self), integer_width(other)) {
                (Some(from), Some(to)) => from <= to,
                _ => false,
            },
        }
    }

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

/// What a name stood for when it was looked at: see
/// `Catalog::object_stamp`. Two stamps of a name are equal as long as
/// nothing has changed the table or view between them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStamp {
    /// A table: its ID (another table under the name has another) and the
    /// version of its definition.
    Table {
        id: crate::executor::encoding::TableId,
        version: u64,
    },
    /// A view: a digest of its query.
    View(u64),
    /// Neither.
    Missing,
}

/// Where a column goes among a table's columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnPlace {
    /// After them all.
    Last,
    /// Before them all.
    First,
    /// Right after the column of this name.
    After(String),
}

/// Why a change to a table's columns cannot be made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterError {
    /// The table has no column of this name.
    NoSuchColumn(String),
    /// The table already has a column of this name.
    DuplicateColumn(String),
    /// A table cannot be left without columns.
    LastColumn,
    /// The change cannot be made by changing the definition alone: the
    /// table's rows would have to be rewritten, or stored under other
    /// keys. What the change is, for the message.
    NeedsRebuild(String),
}

impl std::fmt::Display for AlterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterError::NoSuchColumn(name) => write!(f, "Unknown column '{name}'"),
            AlterError::DuplicateColumn(name) => write!(f, "Duplicate column name '{name}'"),
            AlterError::LastColumn => write!(
                f,
                "You can't delete all columns with ALTER TABLE; use DROP TABLE instead"
            ),
            AlterError::NeedsRebuild(change) => write!(f, "{change}"),
        }
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// The column's number within its table: what its values are stored
    /// under in a row. Given when the column joins a table (`0` until
    /// then), kept for as long as the column exists, and never given to
    /// another column of the table, so that renaming, moving or dropping a
    /// column changes no stored row.
    #[serde(default)]
    pub id: u32,
    /// What a row written before this column existed reads as for it: the
    /// default the column was added with, as an encoded value
    /// (`encoding::encode_value`). `None` reads as `NULL`, and is what
    /// every column a table was created with has.
    #[serde(default)]
    pub missing: Option<Vec<u8>>,
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
            id: 0,
            missing: None,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constraint {
    /// Primary key constraint
    PrimaryKey(Vec<String>),
    /// Unique constraint
    Unique(Vec<String>),
    /// Foreign key constraint
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: TableName,
        ref_columns: Vec<String>,
    },
    /// Check constraint (expression as string)
    Check(String),
}

/// Table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    /// The table's number: what its rows are stored under. Assigned when
    /// the table is created and never changed; `TableId(0)` until then.
    pub id: crate::executor::encoding::TableId,
    /// How many times the table's definition has been changed since it was
    /// created. A change is stored as the next version of the definition
    /// it was made from, or not at all.
    #[serde(default)]
    pub version: u64,
    /// The table's name: its database and its name there
    pub name: TableName,
    /// The number the next column to join the table is given. It only
    /// rises: a dropped column's number is not given out again.
    #[serde(default)]
    pub next_column_id: u32,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Table constraints
    pub constraints: Vec<Constraint>,
    /// Whether this is a temporary table (session-scoped, not persisted)
    pub is_temporary: bool,
}

impl TableDef {
    /// Create a new table definition
    pub fn new(name: impl Into<TableName>) -> Self {
        Self {
            id: crate::executor::encoding::TableId(0),
            version: 0,
            name: name.into(),
            next_column_id: 1,
            columns: Vec::new(),
            constraints: Vec::new(),
            is_temporary: false,
        }
    }

    /// Give the table its number.
    #[must_use]
    pub fn with_id(mut self, id: crate::executor::encoding::TableId) -> Self {
        self.id = id;
        self
    }

    /// Add a column
    #[must_use]
    pub fn column(mut self, col: ColumnDef) -> Self {
        self.add_column(col, None);
        self
    }

    /// Add a column to the table, at `position` among its columns or, with
    /// `None`, after them all. It is given the table's next column number.
    pub fn add_column(&mut self, mut col: ColumnDef, position: Option<usize>) {
        col.id = self.next_column_id.max(1);
        self.next_column_id = col.id + 1;
        match position {
            Some(position) if position < self.columns.len() => self.columns.insert(position, col),
            _ => self.columns.push(col),
        }
    }

    /// The position of the column of this name (column names do not
    /// distinguish case).
    fn column_position(&self, name: &str) -> Result<usize, AlterError> {
        self.columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| AlterError::NoSuchColumn(name.to_string()))
    }

    /// Where `place` is among the columns, for a column that is not among
    /// them (yet, or any more).
    fn place(&self, place: &ColumnPlace) -> Result<usize, AlterError> {
        match place {
            ColumnPlace::Last => Ok(self.columns.len()),
            ColumnPlace::First => Ok(0),
            ColumnPlace::After(name) => Ok(self.column_position(name)? + 1),
        }
    }

    /// Add a column where `place` says. Rows that are already there read
    /// as the column's `missing` value for it; no row is touched.
    pub fn alter_add_column(
        &mut self,
        col: ColumnDef,
        place: &ColumnPlace,
    ) -> Result<(), AlterError> {
        if self.column_position(&col.name).is_ok() {
            return Err(AlterError::DuplicateColumn(col.name));
        }
        if col.auto_increment {
            // The rows that are there would each need a number.
            return Err(AlterError::NeedsRebuild(format!(
                "adding the AUTO_INCREMENT column '{}'",
                col.name
            )));
        }
        let position = self.place(place)?;
        self.add_column(col, Some(position));
        Ok(())
    }

    /// Drop a column. Its values stay in the rows that have them and are
    /// passed over from now on: the column's number is not used again. A
    /// unique constraint loses the column, and goes when it has no other.
    pub fn alter_drop_column(&mut self, name: &str) -> Result<(), AlterError> {
        let position = self.column_position(name)?;
        let name = self.columns[position].name.clone();
        let in_key = self
            .primary_key()
            .is_some_and(|key| key.iter().any(|column| column.eq_ignore_ascii_case(&name)));
        if in_key {
            // The rows are stored under their key.
            return Err(AlterError::NeedsRebuild(format!(
                "dropping the primary key column '{name}'"
            )));
        }
        if self.columns.len() == 1 {
            return Err(AlterError::LastColumn);
        }
        self.columns.remove(position);
        let names = |columns: &[String]| columns.iter().any(|c| c.eq_ignore_ascii_case(&name));
        for constraint in &mut self.constraints {
            if let Constraint::Unique(columns) = constraint {
                columns.retain(|column| !column.eq_ignore_ascii_case(&name));
            }
        }
        self.constraints.retain(|constraint| match constraint {
            Constraint::Unique(columns) => !columns.is_empty(),
            Constraint::ForeignKey { columns, .. } => !names(columns),
            Constraint::PrimaryKey(_) | Constraint::Check(_) => true,
        });
        Ok(())
    }

    /// Give a column another name. Its values are stored under its number,
    /// so nothing else changes but the constraints that name it.
    pub fn alter_rename_column(&mut self, old: &str, new: &str) -> Result<(), AlterError> {
        let position = self.column_position(old)?;
        if !old.eq_ignore_ascii_case(new) && self.column_position(new).is_ok() {
            return Err(AlterError::DuplicateColumn(new.to_string()));
        }
        let old = std::mem::replace(&mut self.columns[position].name, new.to_string());
        for constraint in &mut self.constraints {
            let columns = match constraint {
                Constraint::PrimaryKey(columns) | Constraint::Unique(columns) => columns,
                Constraint::ForeignKey { columns, .. } => columns,
                Constraint::Check(_) => continue,
            };
            for column in columns.iter_mut().filter(|c| c.eq_ignore_ascii_case(&old)) {
                *column = new.to_string();
            }
        }
        Ok(())
    }

    /// Move a column to where `place` says, among the others.
    pub fn alter_move_column(&mut self, name: &str, place: &ColumnPlace) -> Result<(), AlterError> {
        let position = self.column_position(name)?;
        if matches!(place, ColumnPlace::After(after) if after.eq_ignore_ascii_case(name)) {
            return Err(AlterError::NoSuchColumn(name.to_string()));
        }
        let column = self.columns.remove(position);
        match self.place(place) {
            Ok(to) => {
                self.columns.insert(to, column);
                Ok(())
            }
            Err(e) => {
                self.columns.insert(position, column);
                Err(e)
            }
        }
    }

    /// Give a column another type. Allowed where every value the column's
    /// rows can hold is stored the same way under the new type; anything
    /// else means rewriting the rows.
    pub fn alter_column_type(&mut self, name: &str, data_type: DataType) -> Result<(), AlterError> {
        let position = self.column_position(name)?;
        let column = &mut self.columns[position];
        if !column.data_type.values_fit(&data_type) {
            return Err(AlterError::NeedsRebuild(format!(
                "changing column '{}' from {:?} to {:?}",
                column.name, column.data_type, data_type
            )));
        }
        column.data_type = data_type;
        Ok(())
    }

    /// How the table's rows are stored and read: its columns' numbers, in
    /// the columns' order, and what each reads as in a row older than it.
    pub fn row_shape(
        &self,
    ) -> crate::executor::ExecutorResult<crate::executor::rowcodec::RowShape> {
        use crate::executor::rowcodec::{ColumnSlot, RowShape};
        let columns = self
            .columns
            .iter()
            .map(|column| match &column.missing {
                None => Ok(ColumnSlot::new(column.id)),
                Some(encoded) => crate::executor::encoding::decode_datum(encoded)
                    .map(|(missing, _)| ColumnSlot::added(column.id, missing)),
            })
            .collect::<crate::executor::ExecutorResult<Vec<_>>>()?;
        RowShape::new(columns)
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Table this index belongs to
    pub table: TableName,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
}

impl IndexDef {
    /// Create a new index definition
    pub fn new(name: impl Into<String>, table: impl Into<TableName>, columns: Vec<String>) -> Self {
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
    /// The database the routine is in. Its body runs with this as the
    /// default database, whatever the caller's is.
    pub database: String,
    /// The routine's name within its database, in lower case (routine
    /// names do not distinguish case).
    pub name: String,
    pub params: Vec<ProcedureParam>,
    pub body_sql: String,
    /// For CREATE FUNCTION: return type (e.g. "BIGINT"). None for procedures.
    pub returns_type: Option<String>,
}

/// View definition
#[derive(Debug, Clone)]
pub struct ViewDef {
    pub name: TableName,
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
    pub table_name: TableName,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    /// The trigger body as a parsed sqlparser Statement
    pub body: sqlparser::ast::Statement,
    /// Original SQL for SHOW CREATE TRIGGER
    pub create_sql: String,
}

impl TriggerDef {
    /// Build a trigger from its `CREATE TRIGGER` statement:
    /// `CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE} ON table
    /// FOR EACH ROW body`. The statement is what is stored; this is how it
    /// is turned back into a trigger when the catalog is rebuilt.
    pub fn from_create_sql(sql: &str) -> Result<Self, String> {
        let sql_clean = sql.trim().trim_end_matches(';');
        let upper = sql_clean.to_uppercase();

        const FOR_EACH_ROW: &str = "FOR EACH ROW";
        let fer_pos = upper
            .find(FOR_EACH_ROW)
            .ok_or_else(|| "CREATE TRIGGER requires FOR EACH ROW".to_string())?;
        let header = sql_clean[..fer_pos].trim();
        let body_sql = sql_clean[fer_pos + FOR_EACH_ROW.len()..].trim();

        // CREATE TRIGGER name BEFORE INSERT ON table
        let tokens: Vec<&str> = header.split_whitespace().collect();
        if tokens.len() < 7 {
            return Err("Invalid CREATE TRIGGER syntax".to_string());
        }
        let name = tokens[2].trim_matches('`').to_string();
        // As written (quotes, database and all): whoever creates the
        // trigger works out which table that is, since the statement's
        // text does not carry the session's database.
        let table_name = TableName::bare(tokens[6]);
        let timing = match tokens[3].to_uppercase().as_str() {
            "BEFORE" => TriggerTiming::Before,
            "AFTER" => TriggerTiming::After,
            other => return Err(format!("Expected BEFORE or AFTER, got '{other}'")),
        };
        let event = match tokens[4].to_uppercase().as_str() {
            "INSERT" => TriggerEvent::Insert,
            "UPDATE" => TriggerEvent::Update,
            "DELETE" => TriggerEvent::Delete,
            other => return Err(format!("Expected INSERT, UPDATE, or DELETE, got '{other}'")),
        };
        let body = crate::sql::Parser::parse_one(body_sql)
            .map_err(|e| format!("Parse error in trigger body: {e}"))?;

        Ok(Self {
            name,
            table_name,
            timing,
            event,
            body,
            create_sql: sql.to_string(),
        })
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
    tables: HashMap<TableName, TableDef>,
    /// How each table of `tables` is stored, kept beside its definition so
    /// that it is worked out once per definition, not once per statement.
    layouts: HashMap<TableName, TableLayout>,
    /// The name of each table of `tables`, by its ID.
    names: HashMap<crate::executor::encoding::TableId, TableName>,
    /// Indexes by name
    indexes: HashMap<String, IndexDef>,
    /// Schema version counter (incremented on DDL operations)
    schema_version: u64,
    /// `schema_version` again, for those who do not hold the catalog.
    version_cell: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Known databases
    databases: HashSet<String>,
    /// Stored procedures by name
    procedures: HashMap<(String, String), ProcedureDef>,
    /// Views by name
    views: HashMap<TableName, ViewDef>,
    /// Triggers by name
    triggers: HashMap<(String, String), TriggerDef>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        // The databases every server has. The rest are created.
        let databases = HashSet::from([
            system_tables::SYSTEM_DATABASE.to_string(),
            system_tables::INFORMATION_SCHEMA.to_string(),
        ]);
        Self {
            tables: HashMap::new(),
            layouts: HashMap::new(),
            names: HashMap::new(),
            indexes: HashMap::new(),
            schema_version: 0,
            version_cell: Default::default(),
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
            catalog.put_table(table_def);
        }
        catalog
    }

    /// Register a table directly without validation (for rebuilding from storage)
    ///
    /// This bypasses the normal create_table checks and is used when
    /// rebuilding the catalog cache from system tables on startup.
    pub fn register_table(&mut self, def: TableDef) {
        self.put_table(def);
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
        self.layouts
            .retain(|name, _| system_tables::is_system_table(name));
        self.names
            .retain(|_, name| system_tables::is_system_table(name));
        self.indexes.clear();
    }

    // ============ Database operations ============

    /// Create a database
    pub fn create_database(&mut self, name: &str) -> Result<(), DatabaseError> {
        if self.databases.contains(name) {
            return Err(DatabaseError::DatabaseExists(name.to_string()));
        }
        self.databases.insert(name.to_string());
        self.bump_version();
        Ok(())
    }

    /// Drop a database
    pub fn drop_database(&mut self, name: &str) -> Result<(), DatabaseError> {
        if !self.databases.remove(name) {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }
        self.bump_version();
        Ok(())
    }

    /// Check if a database exists. (`information_schema` in any case, as
    /// MySQL has it.)
    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.contains(name)
            || name.eq_ignore_ascii_case(system_tables::INFORMATION_SCHEMA)
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
    pub fn get_tables_in_database(&self, db: &str) -> Vec<String> {
        let mut tables: Vec<String> = self
            .tables
            .keys()
            .filter(|name| name.is_in(db))
            .map(|name| name.table.clone())
            .collect();
        tables.sort();
        tables
    }

    /// The views of a database, by their names within it.
    pub fn get_views_in_database(&self, db: &str) -> Vec<String> {
        let mut views: Vec<String> = self
            .views
            .keys()
            .filter(|name| name.is_in(db))
            .map(|name| name.table.clone())
            .collect();
        views.sort();
        views
    }

    /// Register a database directly (for rebuilding from storage)
    pub fn register_database(&mut self, name: String) {
        self.databases.insert(name);
    }

    /// Clear the databases that were created (for re-initialization)
    pub fn clear_user_databases(&mut self) {
        self.databases
            .retain(|name| system_tables::is_system_database(name));
    }

    // ============ Table operations ============

    /// Whether `def` is a definition the catalog can hold: its constraints
    /// name columns it has, and its foreign keys tables and columns that
    /// exist (the table itself counts, for a key that refers to its own
    /// table). Checked before a definition is proposed; what is applied
    /// from the log is taken as it is.
    pub fn validate_table(&self, def: &TableDef) -> CatalogResult<()> {
        // Validate table name
        if def.name.table.is_empty() {
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
                    return Err(CatalogError::ColumnNotFound(
                        def.name.to_string(),
                        col.clone(),
                    ));
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
                let ref_def = if *ref_table == def.name {
                    Some(def)
                } else {
                    self.tables.get(ref_table)
                };
                if let Some(ref_def) = ref_def {
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

        Ok(())
    }

    /// Put a table and its indexes in the catalog as the log says they
    /// are, replacing whatever was known of the table. Cannot fail: what
    /// the log holds was validated before it was proposed.
    pub fn set_table(&mut self, def: TableDef, indexes: Vec<IndexDef>) {
        self.indexes.retain(|_, index| index.table != def.name);
        for index in indexes {
            self.indexes.insert(index.name.clone(), index);
        }
        self.put_table(def);
        self.bump_version();
    }

    /// The table with this ID, if the catalog has it.
    pub fn table_by_id(&self, id: crate::executor::encoding::TableId) -> Option<&TableDef> {
        self.tables.get(self.names.get(&id)?)
    }

    /// Take a table and its indexes out of the catalog, if it is there.
    pub fn unset_table(&mut self, name: &TableName) {
        self.take_table(name);
        self.indexes.retain(|_, index| index.table != *name);
        self.bump_version();
    }

    /// The indexes of a table, by name.
    pub fn table_indexes(&self, table: &TableName) -> Vec<IndexDef> {
        let mut indexes: Vec<IndexDef> = self
            .indexes
            .values()
            .filter(|index| index.table == *table)
            .cloned()
            .collect();
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        indexes
    }

    /// Create a table
    pub fn create_table(&mut self, def: TableDef) -> CatalogResult<()> {
        self.validate_table(&def)?;
        if self.tables.contains_key(&def.name) {
            return Err(CatalogError::TableExists(def.name.to_string()));
        }
        self.put_table(def);
        self.bump_version();
        Ok(())
    }

    /// Replace a table definition atomically (used by ALTER TABLE)
    ///
    /// Swaps the entire table definition after applying ALTER operations.
    /// The old definition is replaced with the new one. If the table name
    /// changed (RENAME), the old entry is removed and the new one inserted.
    pub fn replace_table(&mut self, old_name: &TableName, def: TableDef) {
        if *old_name != def.name {
            self.take_table(old_name);
        }
        self.put_table(def);
        self.bump_version();
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &TableName) -> CatalogResult<()> {
        if self.take_table(name).is_none() {
            return Err(CatalogError::TableNotFound(name.to_string()));
        }
        // Also drop all indexes for this table
        self.indexes.retain(|_, idx| idx.table != *name);
        self.bump_version();
        Ok(())
    }

    /// Get a table definition
    pub fn get_table(&self, name: &TableName) -> Option<&TableDef> {
        self.tables.get(name)
    }

    /// What `name` is now, to tell later whether it is still that: a
    /// statement is planned against tables and views as it finds them, and
    /// run only if they are still as it found them.
    pub fn object_stamp(&self, name: &TableName) -> ObjectStamp {
        use std::hash::{Hash, Hasher};
        if let Some(table) = self.tables.get(name) {
            return ObjectStamp::Table {
                id: table.id,
                version: table.version,
            };
        }
        match self.views.get(name) {
            Some(view) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                view.query_sql.hash(&mut hasher);
                ObjectStamp::View(hasher.finish())
            }
            None => ObjectStamp::Missing,
        }
    }

    /// The catalog has changed.
    fn bump_version(&mut self) {
        self.schema_version += 1;
        self.version_cell
            .store(self.schema_version, std::sync::atomic::Ordering::Release);
    }

    /// The catalog's version, where it can be read without holding the
    /// catalog: it changes whenever anything in the catalog does.
    pub fn version_cell(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        self.version_cell.clone()
    }

    /// How a table is stored: its ID and the shape of its rows.
    pub fn table_layout(&self, name: &TableName) -> Option<&TableLayout> {
        self.layouts.get(name)
    }

    /// Put a table's definition in the catalog, with its layout, in place
    /// of whatever was known under its name.
    fn put_table(&mut self, def: TableDef) {
        // A definition was validated before it got here; one whose columns
        // cannot make a shape (a stored default that does not decode)
        // would have been refused. Should one arrive regardless, the table
        // is known but cannot be read.
        match def.row_shape() {
            Ok(shape) => {
                self.layouts
                    .insert(def.name.clone(), TableLayout::new(def.id, shape));
            }
            Err(e) => {
                tracing::error!(table = %def.name, error = %e, "A table's columns give no row shape");
                self.layouts.remove(&def.name);
            }
        }
        // (A table that takes over a name another had: the other's ID no
        // longer leads anywhere.)
        if let Some(replaced) = self.tables.insert(def.name.clone(), def.clone()) {
            if replaced.id != def.id {
                self.names.remove(&replaced.id);
            }
        }
        self.names.insert(def.id, def.name);
    }

    /// Take a table's definition and layout out of the catalog.
    fn take_table(&mut self, name: &TableName) -> Option<TableDef> {
        self.layouts.remove(name);
        let def = self.tables.remove(name)?;
        if self.names.get(&def.id) == Some(name) {
            self.names.remove(&def.id);
        }
        Some(def)
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &TableName) -> bool {
        self.tables.contains_key(name)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<&TableName> {
        self.tables.keys().collect()
    }

    /// Get all table names as a HashSet (for efficient duplicate checking)
    pub fn table_names(&self) -> std::collections::HashSet<TableName> {
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
            .ok_or_else(|| CatalogError::TableNotFound(def.table.to_string()))?;
        for col in &def.columns {
            if table.get_column(col).is_none() {
                return Err(CatalogError::ColumnNotFound(
                    def.table.to_string(),
                    col.clone(),
                ));
            }
        }

        if self.indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name.clone()));
        }
        self.indexes.insert(def.name.clone(), def);
        self.bump_version();
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> CatalogResult<()> {
        if self.indexes.remove(name).is_none() {
            return Err(CatalogError::IndexNotFound(name.to_string()));
        }
        self.bump_version();
        Ok(())
    }

    /// Get an index definition
    pub fn get_index(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.get(name)
    }

    /// Get all indexes for a table
    pub fn get_indexes_for_table(&self, table: &TableName) -> Vec<&IndexDef> {
        self.indexes
            .values()
            .filter(|idx| idx.table == *table)
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
        let key = (def.database.clone(), def.name.clone());
        if self.procedures.contains_key(&key) {
            return Err(CatalogError::ProcedureExists(def.name.clone()));
        }
        self.procedures.insert(key, def);
        self.bump_version();
        Ok(())
    }

    /// Drop a stored procedure
    pub fn drop_procedure(&mut self, database: &str, name: &str) -> CatalogResult<()> {
        let key = (database.to_string(), name.to_string());
        if self.procedures.remove(&key).is_none() {
            return Err(CatalogError::ProcedureNotFound(name.to_string()));
        }
        self.bump_version();
        Ok(())
    }

    /// Get a stored procedure definition
    pub fn get_procedure(&self, database: &str, name: &str) -> Option<&ProcedureDef> {
        self.procedures
            .get(&(database.to_string(), name.to_string()))
    }

    /// Register a procedure directly (for rebuilding from storage)
    pub fn register_procedure(&mut self, def: ProcedureDef) {
        self.procedures
            .insert((def.database.clone(), def.name.clone()), def);
        self.bump_version();
    }

    /// The stored functions: procedures that return a value.
    pub fn functions(&self) -> impl Iterator<Item = &ProcedureDef> {
        self.procedures
            .values()
            .filter(|p| p.returns_type.is_some())
    }

    /// Check if a procedure exists
    pub fn procedure_exists(&self, database: &str, name: &str) -> bool {
        self.get_procedure(database, name).is_some()
    }

    /// Clear all procedures (for re-initialization)
    pub fn clear_procedures(&mut self) {
        self.procedures.clear();
    }

    /// List all stored procedures
    pub fn list_procedures(&self) -> Vec<&ProcedureDef> {
        self.procedures.values().collect()
    }

    // ============ View operations ============

    /// Create a view
    pub fn create_view(&mut self, def: ViewDef) -> CatalogResult<()> {
        if self.views.contains_key(&def.name) {
            return Err(CatalogError::ViewExists(def.name.to_string()));
        }
        self.views.insert(def.name.clone(), def);
        self.bump_version();
        Ok(())
    }

    /// Create or replace a view
    pub fn replace_view(&mut self, def: ViewDef) {
        self.views.insert(def.name.clone(), def);
        self.bump_version();
    }

    /// Drop a view
    pub fn drop_view(&mut self, name: &TableName, if_exists: bool) -> CatalogResult<()> {
        match self.views.remove(name) {
            Some(_) => {
                self.bump_version();
                Ok(())
            }
            None if if_exists => Ok(()),
            None => Err(CatalogError::ViewNotFound(name.to_string())),
        }
    }

    /// Get a view definition
    pub fn get_view(&self, name: &TableName) -> Option<&ViewDef> {
        self.views.get(name)
    }

    /// Check if a view exists
    pub fn view_exists(&self, name: &TableName) -> bool {
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
    pub fn get_view_names(&self) -> Vec<TableName> {
        self.views.keys().cloned().collect()
    }

    // ============ Trigger operations ============

    /// Create a trigger. It is in its table's database, and named within
    /// it.
    pub fn create_trigger(&mut self, def: TriggerDef) {
        self.triggers
            .insert((def.table_name.database.clone(), def.name.clone()), def);
        self.bump_version();
    }

    /// Drop a trigger
    pub fn drop_trigger(&mut self, database: &str, name: &str) -> CatalogResult<()> {
        let key = (database.to_string(), name.to_string());
        if self.triggers.remove(&key).is_none() {
            return Err(CatalogError::ViewNotFound(format!(
                "Trigger '{}' not found",
                name
            )));
        }
        self.bump_version();
        Ok(())
    }

    /// Whether `database` has a trigger of this name.
    pub fn has_trigger(&self, database: &str, name: &str) -> bool {
        self.triggers
            .contains_key(&(database.to_string(), name.to_string()))
    }

    /// Forget every trigger (before rebuilding them from the system tables).
    pub fn clear_triggers(&mut self) {
        self.triggers.clear();
        self.bump_version();
    }

    /// Forget the triggers of one table (when it is dropped).
    pub fn drop_triggers_of_table(&mut self, table_name: &TableName) {
        self.triggers.retain(|_, t| t.table_name != *table_name);
        self.bump_version();
    }

    /// Get triggers for a specific table, timing, and event
    pub fn get_triggers(
        &self,
        table_name: &TableName,
        timing: TriggerTiming,
        event: TriggerEvent,
    ) -> Vec<&TriggerDef> {
        self.triggers
            .values()
            .filter(|t| t.table_name == *table_name && t.timing == timing && t.event == event)
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
        assert!(catalog.table_exists(&"users".into()));

        // Duplicate should fail
        let table2 = TableDef::new("users");
        assert!(matches!(
            catalog.create_table(table2),
            Err(CatalogError::TableExists(_))
        ));

        // Get table
        let t = catalog.get_table(&"users".into()).unwrap();
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.primary_key(), Some(&["id".to_string()][..]));

        // List tables
        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&&"users".into()));

        // Drop table
        catalog.drop_table(&"users".into()).unwrap();
        assert!(!catalog.table_exists(&"users".into()));

        // Drop non-existent should fail
        assert!(matches!(
            catalog.drop_table(&"users".into()),
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
                ref_table: "users".into(),
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
        let indexes = catalog.get_indexes_for_table(&"users".into());
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
        catalog.drop_table(&"users".into()).unwrap();
        let indexes = catalog.get_indexes_for_table(&"users".into());
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

    #[test]
    fn columns_change_by_definition_alone() {
        let mut table = TableDef::new("t")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("a", DataType::Int))
            .column(ColumnDef::new("b", DataType::Varchar(10)))
            .constraint(Constraint::PrimaryKey(vec!["id".to_string()]))
            .constraint(Constraint::Unique(vec!["a".to_string(), "b".to_string()]));
        let shape = |table: &TableDef| -> Vec<(String, u32)> {
            table
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.id))
                .collect()
        };
        let named = |pairs: &[(&str, u32)]| -> Vec<(String, u32)> {
            pairs
                .iter()
                .map(|(name, id)| (name.to_string(), *id))
                .collect()
        };
        assert_eq!(shape(&table), named(&[("id", 1), ("a", 2), ("b", 3)]));

        // Added columns take the next numbers, wherever they are placed.
        table
            .alter_add_column(ColumnDef::new("c", DataType::Int), &ColumnPlace::Last)
            .unwrap();
        table
            .alter_add_column(ColumnDef::new("d", DataType::Int), &ColumnPlace::First)
            .unwrap();
        table
            .alter_add_column(
                ColumnDef::new("e", DataType::Int),
                &ColumnPlace::After("A".to_string()),
            )
            .unwrap();
        assert_eq!(
            shape(&table),
            named(&[("d", 5), ("id", 1), ("a", 2), ("e", 6), ("b", 3), ("c", 4)])
        );
        assert_eq!(
            table.alter_add_column(ColumnDef::new("B", DataType::Int), &ColumnPlace::Last),
            Err(AlterError::DuplicateColumn("B".to_string()))
        );
        assert!(matches!(
            table.alter_add_column(
                ColumnDef::new("n", DataType::Int).auto_increment(),
                &ColumnPlace::Last
            ),
            Err(AlterError::NeedsRebuild(_))
        ));

        // A dropped column's number is not given out again, and a unique
        // constraint goes on without it.
        table.alter_drop_column("A").unwrap();
        table
            .alter_add_column(ColumnDef::new("a", DataType::Int), &ColumnPlace::Last)
            .unwrap();
        assert_eq!(
            shape(&table),
            named(&[("d", 5), ("id", 1), ("e", 6), ("b", 3), ("c", 4), ("a", 7)])
        );
        assert!(table
            .constraints
            .contains(&Constraint::Unique(vec!["b".to_string()])));
        assert!(matches!(
            table.alter_drop_column("id"),
            Err(AlterError::NeedsRebuild(_))
        ));
        assert_eq!(
            table.alter_drop_column("nope"),
            Err(AlterError::NoSuchColumn("nope".to_string()))
        );

        // A renamed column keeps its number, and the constraints follow.
        table.alter_rename_column("ID", "key").unwrap();
        table.alter_rename_column("b", "bb").unwrap();
        assert_eq!(table.primary_key(), Some(&["key".to_string()][..]));
        assert!(table
            .constraints
            .contains(&Constraint::Unique(vec!["bb".to_string()])));
        assert_eq!(
            table.alter_rename_column("bb", "C"),
            Err(AlterError::DuplicateColumn("C".to_string()))
        );

        // Moved.
        table.alter_move_column("a", &ColumnPlace::First).unwrap();
        table
            .alter_move_column("d", &ColumnPlace::After("c".to_string()))
            .unwrap();
        assert_eq!(
            shape(&table),
            named(&[
                ("a", 7),
                ("key", 1),
                ("e", 6),
                ("bb", 3),
                ("c", 4),
                ("d", 5)
            ])
        );
        assert!(table
            .alter_move_column("a", &ColumnPlace::After("nope".to_string()))
            .is_err());
        assert_eq!(table.columns[0].name, "a");

        // A type the stored values already fit; one they do not.
        table
            .alter_column_type("bb", DataType::Varchar(20))
            .unwrap();
        table.alter_column_type("c", DataType::BigInt).unwrap();
        for (column, data_type) in [
            ("bb", DataType::Varchar(5)),
            ("bb", DataType::Int),
            ("c", DataType::Int),
            ("c", DataType::BigIntUnsigned),
        ] {
            assert!(
                matches!(
                    table.alter_column_type(column, data_type.clone()),
                    Err(AlterError::NeedsRebuild(_))
                ),
                "{column} to {data_type:?}"
            );
        }
        // The row shape follows the columns.
        let ids: Vec<u32> = table
            .row_shape()
            .unwrap()
            .columns()
            .iter()
            .map(|column| column.id)
            .collect();
        assert_eq!(ids, [7, 1, 6, 3, 4, 5]);
    }
}
