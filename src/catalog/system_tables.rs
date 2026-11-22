//! System tables for schema persistence
//!
//! Schema metadata is stored in system tables that are replicated via Raft
//! like any other data. The catalog is a cache rebuilt from these tables on startup.

use super::{ColumnDef, Constraint, DataType, ProcedureDef, ProcedureParam, TableDef, ViewDef};
use crate::executor::encoding::TableId;
use crate::executor::{Datum, Row};

/// System table names
pub const SYSTEM_TABLES: &str = "system.tables";

// Auth system tables
pub const SYSTEM_USERS: &str = "system.users";
pub const SYSTEM_GRANTS: &str = "system.grants";
pub const SYSTEM_ROLES: &str = "system.roles";
pub const SYSTEM_ROLE_GRANTS: &str = "system.role_grants";

// Database registry
pub const SYSTEM_DATABASES: &str = "system.databases";

// Table statistics
pub const SYSTEM_TABLE_STATISTICS: &str = "system.table_statistics";

// Stored procedures
pub const SYSTEM_PROCEDURES: &str = "system.procedures";

// Views
pub const SYSTEM_VIEWS: &str = "system.views";

// Triggers
pub const SYSTEM_TRIGGERS: &str = "system.triggers";

/// Check if a table name is a system table
pub fn is_system_table(name: &str) -> bool {
    name.starts_with("system.")
}

/// Create the TableDef for system.tables
pub fn tables_table_def() -> TableDef {
    // One record per table: its definition and its indexes, encoded (see
    // `catalog::store`).
    TableDef::new(SYSTEM_TABLES)
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("definition", DataType::Blob).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["table_name".to_string()]))
}

/// Create the TableDef for system.users (auth)
pub fn users_table_def() -> TableDef {
    TableDef::new(SYSTEM_USERS)
        .column(ColumnDef::new("username", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("host", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("password_hash", DataType::Varchar(255)).nullable(true))
        .column(ColumnDef::new("auth_plugin", DataType::Varchar(64)).nullable(false))
        .column(ColumnDef::new("ssl_subject", DataType::Varchar(512)).nullable(true))
        .column(ColumnDef::new("ssl_issuer", DataType::Varchar(512)).nullable(true))
        .column(ColumnDef::new("account_locked", DataType::Boolean).nullable(false))
        .column(ColumnDef::new("password_expired", DataType::Boolean).nullable(false))
        .column(ColumnDef::new("created_at", DataType::Timestamp).nullable(true))
        .column(ColumnDef::new("updated_at", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec![
            "username".to_string(),
            "host".to_string(),
        ]))
}

/// Create the TableDef for system.grants (auth)
pub fn grants_table_def() -> TableDef {
    TableDef::new(SYSTEM_GRANTS)
        .column(ColumnDef::new("grantee", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("grantee_host", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("grantee_type", DataType::Varchar(16)).nullable(false))
        .column(ColumnDef::new("privilege", DataType::Varchar(64)).nullable(false))
        .column(ColumnDef::new("object_type", DataType::Varchar(16)).nullable(false))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(true))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(true))
        .column(ColumnDef::new("with_grant_option", DataType::Boolean).nullable(false))
        .column(ColumnDef::new("granted_by", DataType::Varchar(255)).nullable(true))
        .column(ColumnDef::new("granted_at", DataType::Timestamp).nullable(true))
        .constraint(Constraint::Unique(vec![
            "grantee".to_string(),
            "grantee_host".to_string(),
            "privilege".to_string(),
            "object_type".to_string(),
            "database_name".to_string(),
            "table_name".to_string(),
        ]))
}

/// Create the TableDef for system.roles (auth)
pub fn roles_table_def() -> TableDef {
    TableDef::new(SYSTEM_ROLES)
        .column(ColumnDef::new("role_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("created_at", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec!["role_name".to_string()]))
}

/// Create the TableDef for system.role_grants (auth)
pub fn role_grants_table_def() -> TableDef {
    TableDef::new(SYSTEM_ROLE_GRANTS)
        .column(ColumnDef::new("username", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("host", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("role_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("with_admin_option", DataType::Boolean).nullable(false))
        .column(ColumnDef::new("granted_at", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec![
            "username".to_string(),
            "host".to_string(),
            "role_name".to_string(),
        ]))
}

/// Create the TableDef for system.table_statistics
pub fn table_statistics_table_def() -> TableDef {
    TableDef::new(SYSTEM_TABLE_STATISTICS)
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("row_count", DataType::BigInt).nullable(false))
        .column(ColumnDef::new("data_size", DataType::BigInt).nullable(false))
        .column(ColumnDef::new("last_analyzed", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec!["table_name".to_string()]))
}

/// Create the TableDef for system.databases
pub fn databases_table_def() -> TableDef {
    TableDef::new(SYSTEM_DATABASES)
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["database_name".to_string()]))
}

/// Create the TableDef for system.procedures
pub fn procedures_table_def() -> TableDef {
    TableDef::new(SYSTEM_PROCEDURES)
        .column(ColumnDef::new("procedure_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("params_json", DataType::Text).nullable(false))
        .column(ColumnDef::new("body_sql", DataType::Text).nullable(false))
        .column(ColumnDef::new("returns_type", DataType::Varchar(255)).nullable(true))
        .constraint(Constraint::PrimaryKey(vec!["procedure_name".to_string()]))
}

/// Create the TableDef for system.views
pub fn views_table_def() -> TableDef {
    TableDef::new(SYSTEM_VIEWS)
        .column(ColumnDef::new("view_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("query_sql", DataType::Text).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["view_name".to_string()]))
}

/// Get all system table definitions for bootstrapping
/// Create the TableDef for system.triggers. A trigger is stored as the
/// statement that created it.
pub fn triggers_table_def() -> TableDef {
    TableDef::new(SYSTEM_TRIGGERS)
        .column(ColumnDef::new("trigger_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("create_sql", DataType::Text).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["trigger_name".to_string()]))
}

/// Convert a trigger to its system.triggers row.
pub fn trigger_def_to_row(def: &crate::catalog::TriggerDef) -> Row {
    Row::new(vec![
        Datum::String(def.name.clone()),
        Datum::String(def.table_name.clone()),
        Datum::String(def.create_sql.clone()),
    ])
}

/// Rebuild a trigger from its system.triggers row.
pub fn row_to_trigger_def(row: &Row) -> Option<crate::catalog::TriggerDef> {
    match row.values().get(2) {
        Some(Datum::String(create_sql)) => {
            crate::catalog::TriggerDef::from_create_sql(create_sql).ok()
        }
        _ => None,
    }
}

pub fn bootstrap_system_tables() -> Vec<TableDef> {
    // Each has a fixed table ID, by its place here: new tables go at the
    // end, and none is ever removed from the middle.
    [
        tables_table_def(),
        users_table_def(),
        grants_table_def(),
        roles_table_def(),
        role_grants_table_def(),
        databases_table_def(),
        table_statistics_table_def(),
        procedures_table_def(),
        views_table_def(),
        triggers_table_def(),
    ]
    .into_iter()
    .zip(1..)
    .map(|(def, id)| def.with_id(TableId(id)))
    .collect()
}

/// Table IDs below this belong to the system's own tables; the tables
/// users create are numbered from here.
pub const FIRST_USER_TABLE_ID: u64 = 1024;

/// The fixed ID of a system table.
pub fn system_table_id(name: &str) -> TableId {
    use std::collections::HashMap;
    use std::sync::LazyLock;

    static IDS: LazyLock<HashMap<String, TableId>> = LazyLock::new(|| {
        bootstrap_system_tables()
            .into_iter()
            .map(|def| (def.name, def.id))
            .collect()
    });
    *IDS.get(name).expect("a system table")
}

/// Convert a DataType to its string representation for storage
pub fn data_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::TinyInt => "TINYINT".to_string(),
        DataType::SmallInt => "SMALLINT".to_string(),
        DataType::Int => "INT".to_string(),
        DataType::BigInt => "BIGINT".to_string(),
        DataType::BigIntUnsigned => "BIGINT UNSIGNED".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Varchar(n) => format!("VARCHAR({})", n),
        DataType::Text => "TEXT".to_string(),
        DataType::Blob => "BLOB".to_string(),
        DataType::Geometry => "GEOMETRY".to_string(),
        DataType::Bit(n) => format!("BIT({})", n),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
        DataType::Json => "JSON".to_string(),
    }
}

/// Convert a ProcedureDef to a row for system.procedures
pub fn procedure_def_to_row(def: &ProcedureDef) -> Row {
    let params_json = serde_json::to_string(&def.params).unwrap_or_else(|_| "[]".to_string());
    Row::new(vec![
        Datum::String(def.name.clone()),
        Datum::String(params_json),
        Datum::String(def.body_sql.clone()),
        Datum::String(def.returns_type.clone().unwrap_or_default()),
    ])
}

/// Reconstruct a ProcedureDef from a system.procedures row
pub fn row_to_procedure_def(row: &Row) -> Option<ProcedureDef> {
    let name = match &row.values()[0] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };
    let params_json = match &row.values()[1] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };
    let body_sql = match &row.values()[2] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };
    let params: Vec<ProcedureParam> = serde_json::from_str(&params_json).ok()?;
    let returns_type = if row.values().len() > 3 {
        match &row.values()[3] {
            Datum::String(s) if !s.is_empty() => Some(s.clone()),
            _ => None,
        }
    } else {
        None
    };
    Some(ProcedureDef {
        name,
        params,
        body_sql,
        returns_type,
    })
}

/// Convert a ViewDef to a row for system.views
pub fn view_def_to_row(def: &ViewDef) -> Row {
    Row::new(vec![
        Datum::String(def.name.clone()),
        Datum::String(def.query_sql.clone()),
    ])
}

/// Reconstruct a ViewDef from a system.views row
pub fn row_to_view_def(row: &Row) -> Option<ViewDef> {
    let name = match &row.values()[0] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };
    let query_sql = match &row.values()[1] {
        Datum::String(s) => s.clone(),
        _ => return None,
    };
    Some(ViewDef { name, query_sql })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(tables.len(), 10);

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&SYSTEM_TABLES));
        assert!(names.contains(&SYSTEM_USERS));
        assert!(names.contains(&SYSTEM_GRANTS));
        assert!(names.contains(&SYSTEM_ROLES));
        assert!(names.contains(&SYSTEM_ROLE_GRANTS));
        assert!(names.contains(&SYSTEM_DATABASES));
        assert!(names.contains(&SYSTEM_TABLE_STATISTICS));
        assert!(names.contains(&SYSTEM_VIEWS));
        assert!(names.contains(&SYSTEM_PROCEDURES));
        assert!(names.contains(&SYSTEM_TRIGGERS));
    }
}
