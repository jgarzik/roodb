//! System tables for schema persistence
//!
//! Schema metadata is stored in system tables that are replicated via Raft
//! like any other data. The catalog is a cache rebuilt from these tables on startup.

use super::{ColumnDef, Constraint, DataType, IndexDef, TableDef};
use crate::executor::{Datum, Row};

/// System table names
pub const SYSTEM_TABLES: &str = "system.tables";
pub const SYSTEM_COLUMNS: &str = "system.columns";
pub const SYSTEM_INDEXES: &str = "system.indexes";

/// Check if a table name is a system table
pub fn is_system_table(name: &str) -> bool {
    name.starts_with("system.")
}

/// Create the TableDef for system.tables
pub fn tables_table_def() -> TableDef {
    TableDef::new(SYSTEM_TABLES)
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["table_name".to_string()]))
}

/// Create the TableDef for system.columns
pub fn columns_table_def() -> TableDef {
    TableDef::new(SYSTEM_COLUMNS)
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("column_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("ordinal", DataType::Int).nullable(false))
        .column(ColumnDef::new("data_type", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("nullable", DataType::Boolean).nullable(false))
        .column(ColumnDef::new("default_value", DataType::Text).nullable(true))
        .column(ColumnDef::new("auto_increment", DataType::Boolean).nullable(false))
        .constraint(Constraint::PrimaryKey(vec![
            "table_name".to_string(),
            "ordinal".to_string(),
        ]))
}

/// Create the TableDef for system.indexes
pub fn indexes_table_def() -> TableDef {
    TableDef::new(SYSTEM_INDEXES)
        .column(ColumnDef::new("index_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("columns", DataType::Text).nullable(false))
        .column(ColumnDef::new("is_unique", DataType::Boolean).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["index_name".to_string()]))
}

/// Get all system table definitions for bootstrapping
pub fn bootstrap_system_tables() -> Vec<TableDef> {
    vec![tables_table_def(), columns_table_def(), indexes_table_def()]
}

/// Convert a TableDef to a row for system.tables
pub fn table_def_to_tables_row(def: &TableDef) -> Row {
    Row::new(vec![Datum::String(def.name.clone())])
}

/// Convert a TableDef to rows for system.columns (one row per column)
pub fn table_def_to_columns_rows(def: &TableDef) -> Vec<Row> {
    def.columns
        .iter()
        .enumerate()
        .map(|(ordinal, col)| {
            Row::new(vec![
                Datum::String(def.name.clone()),
                Datum::String(col.name.clone()),
                Datum::Int(ordinal as i64),
                Datum::String(data_type_to_string(&col.data_type)),
                Datum::Bool(col.nullable),
                col.default
                    .as_ref()
                    .map(|d| Datum::String(d.clone()))
                    .unwrap_or(Datum::Null),
                Datum::Bool(col.auto_increment),
            ])
        })
        .collect()
}

/// Convert a DataType to its string representation for storage
pub fn data_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::TinyInt => "TINYINT".to_string(),
        DataType::SmallInt => "SMALLINT".to_string(),
        DataType::Int => "INT".to_string(),
        DataType::BigInt => "BIGINT".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Varchar(n) => format!("VARCHAR({})", n),
        DataType::Text => "TEXT".to_string(),
        DataType::Blob => "BLOB".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
    }
}

/// Parse a DataType from its string representation
pub fn string_to_data_type(s: &str) -> Option<DataType> {
    let s = s.to_uppercase();
    if s.starts_with("VARCHAR(") && s.ends_with(')') {
        let len_str = &s[8..s.len() - 1];
        len_str.parse().ok().map(DataType::Varchar)
    } else {
        match s.as_str() {
            "BOOLEAN" => Some(DataType::Boolean),
            "TINYINT" => Some(DataType::TinyInt),
            "SMALLINT" => Some(DataType::SmallInt),
            "INT" => Some(DataType::Int),
            "BIGINT" => Some(DataType::BigInt),
            "FLOAT" => Some(DataType::Float),
            "DOUBLE" => Some(DataType::Double),
            "TEXT" => Some(DataType::Text),
            "BLOB" => Some(DataType::Blob),
            "TIMESTAMP" => Some(DataType::Timestamp),
            _ => None,
        }
    }
}

/// Convert an IndexDef to a row for system.indexes
pub fn index_def_to_row(def: &IndexDef) -> Row {
    Row::new(vec![
        Datum::String(def.name.clone()),
        Datum::String(def.table.clone()),
        Datum::String(def.columns.join(",")),
        Datum::Bool(def.unique),
    ])
}

/// Reconstruct a TableDef from system table rows
///
/// Takes the table_name and all column rows for that table from system.columns
pub fn rows_to_table_def(table_name: &str, column_rows: &[Row]) -> Option<TableDef> {
    let mut def = TableDef::new(table_name);

    // Sort by ordinal to ensure correct column order
    let mut sorted_rows: Vec<&Row> = column_rows.iter().collect();
    sorted_rows.sort_by_key(|row| {
        if let Datum::Int(ord) = &row.values()[2] {
            *ord
        } else {
            0
        }
    });

    for row in sorted_rows {
        let column_name = match &row.values()[1] {
            Datum::String(s) => s.clone(),
            _ => return None,
        };

        let data_type_str = match &row.values()[3] {
            Datum::String(s) => s.clone(),
            _ => return None,
        };

        let data_type = string_to_data_type(&data_type_str)?;

        let nullable = match &row.values()[4] {
            Datum::Bool(b) => *b,
            _ => return None,
        };

        let default = match &row.values()[5] {
            Datum::String(s) => Some(s.clone()),
            Datum::Null => None,
            _ => return None,
        };

        let auto_increment = match &row.values()[6] {
            Datum::Bool(b) => *b,
            _ => return None,
        };

        let mut col_def = ColumnDef::new(column_name, data_type).nullable(nullable);
        if let Some(def_val) = default {
            col_def = col_def.default(def_val);
        }
        if auto_increment {
            col_def = col_def.auto_increment();
        }

        def = def.column(col_def);
    }

    Some(def)
}

/// Reconstruct an IndexDef from a system.indexes row
pub fn row_to_index_def(row: &Row) -> Option<IndexDef> {
    let index_name = match &row.values()[0] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };

    let table_name = match &row.values()[1] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };

    let columns_str = match &row.values()[2] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };

    let is_unique = match &row.values()[3] {
        Datum::Bool(b) => *b,
        _ => return None,
    };

    let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();

    let mut def = IndexDef::new(index_name, table_name, columns);
    if is_unique {
        def = def.unique();
    }

    Some(def)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_roundtrip() {
        let types = vec![
            DataType::Boolean,
            DataType::TinyInt,
            DataType::SmallInt,
            DataType::Int,
            DataType::BigInt,
            DataType::Float,
            DataType::Double,
            DataType::Varchar(255),
            DataType::Varchar(100),
            DataType::Text,
            DataType::Blob,
            DataType::Timestamp,
        ];

        for dt in types {
            let s = data_type_to_string(&dt);
            let parsed = string_to_data_type(&s).expect("should parse");
            assert_eq!(dt, parsed);
        }
    }

    #[test]
    fn test_table_def_roundtrip() {
        let original = TableDef::new("test_table")
            .column(
                ColumnDef::new("id", DataType::Int)
                    .nullable(false)
                    .auto_increment(),
            )
            .column(ColumnDef::new("name", DataType::Varchar(100)).nullable(true))
            .column(ColumnDef::new("value", DataType::Double).default("0.0"));

        // Convert to rows
        let column_rows = table_def_to_columns_rows(&original);

        // Convert back
        let restored = rows_to_table_def("test_table", &column_rows).unwrap();

        assert_eq!(original.name, restored.name);
        assert_eq!(original.columns.len(), restored.columns.len());

        for (orig_col, rest_col) in original.columns.iter().zip(restored.columns.iter()) {
            assert_eq!(orig_col.name, rest_col.name);
            assert_eq!(orig_col.data_type, rest_col.data_type);
            assert_eq!(orig_col.nullable, rest_col.nullable);
            assert_eq!(orig_col.default, rest_col.default);
            assert_eq!(orig_col.auto_increment, rest_col.auto_increment);
        }
    }

    #[test]
    fn test_index_def_roundtrip() {
        let original = IndexDef::new(
            "idx_test",
            "test_table",
            vec!["col1".to_string(), "col2".to_string()],
        )
        .unique();

        let row = index_def_to_row(&original);
        let restored = row_to_index_def(&row).unwrap();

        assert_eq!(original.name, restored.name);
        assert_eq!(original.table, restored.table);
        assert_eq!(original.columns, restored.columns);
        assert_eq!(original.unique, restored.unique);
    }

    #[test]
    fn test_is_system_table() {
        assert!(is_system_table("system.tables"));
        assert!(is_system_table("system.columns"));
        assert!(is_system_table("system.indexes"));
        assert!(!is_system_table("users"));
        assert!(!is_system_table("my_system_table"));
    }

    #[test]
    fn test_bootstrap_system_tables() {
        let tables = bootstrap_system_tables();
        assert_eq!(tables.len(), 3);

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&SYSTEM_TABLES));
        assert!(names.contains(&SYSTEM_COLUMNS));
        assert!(names.contains(&SYSTEM_INDEXES));
    }
}
