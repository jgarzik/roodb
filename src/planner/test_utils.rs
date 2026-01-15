//! Shared test utilities for planner module tests

use crate::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};

/// Create a test catalog with a "users" table
pub fn test_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    let users = TableDef::new("users")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("name", DataType::Varchar(100)))
        .column(ColumnDef::new("age", DataType::Int))
        .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

    catalog.create_table(users).unwrap();
    catalog
}
