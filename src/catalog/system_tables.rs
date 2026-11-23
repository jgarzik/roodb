//! System tables for schema persistence
//!
//! Schema metadata is stored in system tables that are replicated via Raft
//! like any other data. The catalog is a cache rebuilt from these tables on startup.

use super::{
    ColumnDef, Constraint, DataType, ProcedureDef, ProcedureParam, TableDef, TableName, ViewDef,
};
use crate::executor::encoding::TableId;
use crate::executor::{Datum, Row};

/// System table names
pub const SYSTEM_TABLES: &str = "tables";

// Auth system tables
pub const SYSTEM_USERS: &str = "users";
pub const SYSTEM_GRANTS: &str = "grants";
pub const SYSTEM_ROLES: &str = "roles";
pub const SYSTEM_ROLE_GRANTS: &str = "role_grants";

// Database registry
pub const SYSTEM_DATABASES: &str = "databases";

// Table statistics
pub const SYSTEM_TABLE_STATISTICS: &str = "table_statistics";

// Stored procedures
pub const SYSTEM_PROCEDURES: &str = "procedures";

// Views
pub const SYSTEM_VIEWS: &str = "views";

// Triggers
pub const SYSTEM_TRIGGERS: &str = "triggers";

/// Check if a table name is a system table
pub fn is_system_table(name: &TableName) -> bool {
    name.is_in(SYSTEM_DATABASE)
}

/// The database the system's own tables are in, as in MySQL.
pub const SYSTEM_DATABASE: &str = "mysql";

/// The database of tables that describe the others. Nothing is stored in
/// it.
pub const INFORMATION_SCHEMA: &str = "information_schema";

/// Whether `database` is one every server has, that cannot be created or
/// dropped.
pub fn is_system_database(database: &str) -> bool {
    database == SYSTEM_DATABASE || database.eq_ignore_ascii_case(INFORMATION_SCHEMA)
}

/// The name of one of the system's tables (`SYSTEM_TABLES`, ...).
pub fn system_name(table: &str) -> TableName {
    TableName::new(SYSTEM_DATABASE, table)
}

/// Create the TableDef for system.tables
pub fn tables_table_def() -> TableDef {
    // One record per table: its definition and its indexes, encoded (see
    // `catalog::store`).
    TableDef::new(system_name(SYSTEM_TABLES))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("definition", DataType::Blob).nullable(false))
        .constraint(Constraint::PrimaryKey(vec![
            "database_name".to_string(),
            "table_name".to_string(),
        ]))
}

/// Create the TableDef for system.users (auth)
pub fn users_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_USERS))
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
    TableDef::new(system_name(SYSTEM_GRANTS))
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
    TableDef::new(system_name(SYSTEM_ROLES))
        .column(ColumnDef::new("role_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("created_at", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec!["role_name".to_string()]))
}

/// Create the TableDef for system.role_grants (auth)
pub fn role_grants_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_ROLE_GRANTS))
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
    TableDef::new(system_name(SYSTEM_TABLE_STATISTICS))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("row_count", DataType::BigInt).nullable(false))
        .column(ColumnDef::new("data_size", DataType::BigInt).nullable(false))
        .column(ColumnDef::new("last_analyzed", DataType::Timestamp).nullable(true))
        .constraint(Constraint::PrimaryKey(vec![
            "database_name".to_string(),
            "table_name".to_string(),
        ]))
}

/// Create the TableDef for system.databases
pub fn databases_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_DATABASES))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .constraint(Constraint::PrimaryKey(vec!["database_name".to_string()]))
}

/// Create the TableDef for system.procedures
pub fn procedures_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_PROCEDURES))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("procedure_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("params_json", DataType::Text).nullable(false))
        .column(ColumnDef::new("body_sql", DataType::Text).nullable(false))
        .column(ColumnDef::new("returns_type", DataType::Varchar(255)).nullable(true))
        .constraint(Constraint::PrimaryKey(vec![
            "database_name".to_string(),
            "procedure_name".to_string(),
        ]))
}

/// Create the TableDef for system.views
pub fn views_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_VIEWS))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("view_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("query_sql", DataType::Text).nullable(false))
        .constraint(Constraint::PrimaryKey(vec![
            "database_name".to_string(),
            "view_name".to_string(),
        ]))
}

/// Get all system table definitions for bootstrapping
/// Create the TableDef for system.triggers. A trigger is stored as the
/// statement that created it.
pub fn triggers_table_def() -> TableDef {
    TableDef::new(system_name(SYSTEM_TRIGGERS))
        .column(ColumnDef::new("database_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("trigger_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("table_name", DataType::Varchar(255)).nullable(false))
        .column(ColumnDef::new("create_sql", DataType::Text).nullable(false))
        .constraint(Constraint::PrimaryKey(vec![
            "database_name".to_string(),
            "trigger_name".to_string(),
        ]))
}

/// Convert a trigger to its system.triggers row.
pub fn trigger_def_to_row(def: &crate::catalog::TriggerDef) -> Row {
    Row::new(vec![
        Datum::String(def.table_name.database.clone()),
        Datum::String(def.name.clone()),
        Datum::String(def.table_name.table.clone()),
        Datum::String(def.create_sql.clone()),
    ])
}

/// Rebuild a trigger from its system.triggers row.
pub fn row_to_trigger_def(row: &Row) -> Option<crate::catalog::TriggerDef> {
    let [Datum::String(database), Datum::String(_), Datum::String(table), Datum::String(create_sql)] =
        row.values()
    else {
        return None;
    };
    let mut def = crate::catalog::TriggerDef::from_create_sql(create_sql).ok()?;
    // The statement names the table as its author wrote it; the row holds
    // the table it meant, with its database.
    def.table_name = TableName::new(database.clone(), table.clone());
    Some(def)
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
            .map(|def| (def.name.table, def.id))
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
        Datum::String(def.database.clone()),
        Datum::String(def.name.clone()),
        Datum::String(params_json),
        Datum::String(def.body_sql.clone()),
        Datum::String(def.returns_type.clone().unwrap_or_default()),
    ])
}

/// Reconstruct a ProcedureDef from a system.procedures row
pub fn row_to_procedure_def(row: &Row) -> Option<ProcedureDef> {
    let [Datum::String(database), Datum::String(name), Datum::String(params_json), Datum::String(body_sql), returns] =
        row.values()
    else {
        return None;
    };
    let params: Vec<ProcedureParam> = serde_json::from_str(params_json).ok()?;
    let returns_type = match returns {
        Datum::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    Some(ProcedureDef {
        database: database.clone(),
        name: name.clone(),
        params,
        body_sql: body_sql.clone(),
        returns_type,
    })
}

/// Convert a ViewDef to a row for system.views
pub fn view_def_to_row(def: &ViewDef) -> Row {
    Row::new(vec![
        Datum::String(def.name.database.clone()),
        Datum::String(def.name.table.clone()),
        Datum::String(def.query_sql.clone()),
    ])
}

/// Reconstruct a ViewDef from a system.views row
pub fn row_to_view_def(row: &Row) -> Option<ViewDef> {
    let [Datum::String(database), Datum::String(view), Datum::String(query_sql)] = row.values()
    else {
        return None;
    };
    Some(ViewDef {
        name: TableName::new(database.clone(), view.clone()),
        query_sql: query_sql.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_system_table() {
        assert!(is_system_table(&system_name(SYSTEM_TABLES)));
        assert!(is_system_table(&TableName::new("mysql", "anything")));
        assert!(!is_system_table(&TableName::new("shop", "tables")));
        assert!(!is_system_table(&TableName::bare("mysql")));
        assert!(is_system_database("mysql"));
        assert!(is_system_database("INFORMATION_SCHEMA"));
        assert!(!is_system_database("shop"));
    }

    #[test]
    fn test_bootstrap_system_tables() {
        let tables = bootstrap_system_tables();
        assert_eq!(tables.len(), 10);

        let names: Vec<&str> = tables.iter().map(|t| t.name.table.as_str()).collect();
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
