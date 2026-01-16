//! SQL layer integration tests

use roodb::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
use roodb::planner::logical::ResolvedStatement;
use roodb::sql::{Parser, Resolver, SqlError, TypeChecker};
use sqlparser::ast::Statement as SpStatement;

/// Create a test catalog with sample tables
fn test_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    let users = TableDef::new("users")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("name", DataType::Varchar(100)))
        .column(ColumnDef::new("email", DataType::Varchar(255)))
        .column(ColumnDef::new("age", DataType::Int))
        .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

    let orders = TableDef::new("orders")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("user_id", DataType::Int))
        .column(ColumnDef::new("total", DataType::Double))
        .column(ColumnDef::new("status", DataType::Varchar(50)))
        .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

    let products = TableDef::new("products")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("name", DataType::Varchar(200)))
        .column(ColumnDef::new("price", DataType::Double))
        .column(ColumnDef::new("quantity", DataType::Int));

    catalog.create_table(users).unwrap();
    catalog.create_table(orders).unwrap();
    catalog.create_table(products).unwrap();

    catalog
}

/// Helper to parse, resolve, and type check
fn full_pipeline(catalog: &Catalog, sql: &str) -> Result<ResolvedStatement, SqlError> {
    let stmt = Parser::parse_one(sql)?;
    let resolver = Resolver::new(catalog);
    let resolved = resolver.resolve(stmt)?;
    TypeChecker::check(&resolved)?;
    Ok(resolved)
}

// ============ Parser Tests ============

#[test]
fn test_parse_select_basic() {
    let stmt = Parser::parse_one("SELECT id, name FROM users").unwrap();
    assert!(matches!(stmt, SpStatement::Query(_)));
}

#[test]
fn test_parse_select_with_where() {
    let stmt = Parser::parse_one("SELECT * FROM users WHERE id = 1 AND name = 'Alice'").unwrap();
    assert!(matches!(stmt, SpStatement::Query(_)));
}

#[test]
fn test_parse_select_with_join() {
    let stmt = Parser::parse_one(
        "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
    )
    .unwrap();
    assert!(matches!(stmt, SpStatement::Query(_)));
}

#[test]
fn test_parse_select_with_group_by() {
    let stmt = Parser::parse_one(
        "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5",
    )
    .unwrap();
    assert!(matches!(stmt, SpStatement::Query(_)));
}

#[test]
fn test_parse_select_with_order_by_limit() {
    let stmt =
        Parser::parse_one("SELECT * FROM users ORDER BY name ASC, age DESC LIMIT 10 OFFSET 5")
            .unwrap();
    assert!(matches!(stmt, SpStatement::Query(_)));
}

#[test]
fn test_parse_insert() {
    let stmt =
        Parser::parse_one("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')")
            .unwrap();
    assert!(matches!(stmt, SpStatement::Insert(_)));
}

#[test]
fn test_parse_insert_multiple_rows() {
    let stmt = Parser::parse_one(
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .unwrap();
    assert!(matches!(stmt, SpStatement::Insert(_)));
}

#[test]
fn test_parse_update() {
    let stmt = Parser::parse_one("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1").unwrap();
    assert!(matches!(stmt, SpStatement::Update { .. }));
}

#[test]
fn test_parse_delete() {
    let stmt = Parser::parse_one("DELETE FROM users WHERE id = 1").unwrap();
    assert!(matches!(stmt, SpStatement::Delete(_)));
}

#[test]
fn test_parse_create_table() {
    let stmt = Parser::parse_one(
        "CREATE TABLE test (
            id INT NOT NULL,
            name VARCHAR(100),
            active BOOLEAN,
            PRIMARY KEY (id)
        )",
    )
    .unwrap();
    assert!(matches!(stmt, SpStatement::CreateTable(_)));
}

#[test]
fn test_parse_drop_table() {
    let stmt = Parser::parse_one("DROP TABLE IF EXISTS users").unwrap();
    assert!(matches!(stmt, SpStatement::Drop { .. }));
}

#[test]
fn test_parse_create_index() {
    let stmt = Parser::parse_one("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap();
    assert!(matches!(stmt, SpStatement::CreateIndex(_)));
}

// ============ Resolver Tests ============

#[test]
fn test_resolve_simple_select() {
    let catalog = test_catalog();
    let resolved = full_pipeline(&catalog, "SELECT id, name FROM users").unwrap();
    assert!(matches!(resolved, ResolvedStatement::Select(_)));
}

#[test]
fn test_resolve_select_with_alias() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "SELECT u.id, u.name FROM users u").is_ok());
}

#[test]
fn test_resolve_wildcard() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "SELECT * FROM users").is_ok());
}

#[test]
fn test_resolve_qualified_wildcard() {
    let catalog = test_catalog();
    assert!(full_pipeline(
        &catalog,
        "SELECT users.* FROM users JOIN orders ON users.id = orders.user_id"
    )
    .is_ok());
}

#[test]
fn test_resolve_table_not_found() {
    let catalog = test_catalog();
    let result = full_pipeline(&catalog, "SELECT * FROM nonexistent");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[test]
fn test_resolve_column_not_found() {
    let catalog = test_catalog();
    let result = full_pipeline(&catalog, "SELECT nonexistent FROM users");
    assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
}

#[test]
fn test_resolve_ambiguous_column() {
    let catalog = test_catalog();
    let result = full_pipeline(
        &catalog,
        "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(matches!(result, Err(SqlError::AmbiguousColumn(_))));
}

#[test]
fn test_resolve_disambiguated_column() {
    let catalog = test_catalog();
    assert!(full_pipeline(
        &catalog,
        "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id"
    )
    .is_ok());
}

#[test]
fn test_resolve_insert() {
    let catalog = test_catalog();
    let resolved = full_pipeline(
        &catalog,
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')",
    )
    .unwrap();
    assert!(matches!(resolved, ResolvedStatement::Insert { .. }));
}

#[test]
fn test_resolve_update() {
    let catalog = test_catalog();
    let resolved = full_pipeline(&catalog, "UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
    assert!(matches!(resolved, ResolvedStatement::Update { .. }));
}

#[test]
fn test_resolve_delete() {
    let catalog = test_catalog();
    let resolved = full_pipeline(&catalog, "DELETE FROM users WHERE age > 30").unwrap();
    assert!(matches!(resolved, ResolvedStatement::Delete { .. }));
}

// ============ Type Check Tests ============

#[test]
fn test_typecheck_comparison_valid() {
    let catalog = test_catalog();
    // Int to Int comparison
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE id = 1").is_ok());
    // String to String comparison
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE name = 'Alice'").is_ok());
}

#[test]
fn test_typecheck_arithmetic() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "SELECT id + 1 FROM users").is_ok());
    assert!(full_pipeline(&catalog, "SELECT price * quantity FROM products").is_ok());
}

#[test]
fn test_typecheck_insert_valid() {
    let catalog = test_catalog();
    assert!(full_pipeline(
        &catalog,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"
    )
    .is_ok());
}

#[test]
fn test_typecheck_update_valid() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "UPDATE users SET age = 30 WHERE id = 1").is_ok());
    assert!(full_pipeline(&catalog, "UPDATE products SET price = 19.99 WHERE id = 1").is_ok());
}

#[test]
fn test_typecheck_aggregate_functions() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "SELECT COUNT(*) FROM users").is_ok());
    assert!(full_pipeline(&catalog, "SELECT SUM(total) FROM orders").is_ok());
    assert!(full_pipeline(&catalog, "SELECT AVG(age) FROM users").is_ok());
    assert!(full_pipeline(&catalog, "SELECT MIN(price), MAX(price) FROM products").is_ok());
}

#[test]
fn test_typecheck_group_by() {
    let catalog = test_catalog();
    assert!(full_pipeline(
        &catalog,
        "SELECT status, COUNT(*) FROM orders GROUP BY status"
    )
    .is_ok());
}

// ============ Expression Tests ============

#[test]
fn test_parse_expressions() {
    let catalog = test_catalog();

    // IN list
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE id IN (1, 2, 3)").is_ok());

    // BETWEEN
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE age BETWEEN 18 AND 65").is_ok());

    // IS NULL
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE email IS NULL").is_ok());
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE email IS NOT NULL").is_ok());

    // LIKE
    assert!(full_pipeline(&catalog, "SELECT * FROM users WHERE name LIKE 'A%'").is_ok());
}

#[test]
fn test_parse_complex_where() {
    let catalog = test_catalog();
    assert!(full_pipeline(
        &catalog,
        "SELECT * FROM users WHERE (age > 18 AND age < 65) OR name = 'Admin'"
    )
    .is_ok());
}

// ============ Edge Cases ============

#[test]
fn test_parse_error_invalid_sql() {
    let result = Parser::parse_one("SELEKT * FROM users");
    assert!(matches!(result, Err(SqlError::Parse(_))));
}

#[test]
fn test_parse_error_empty() {
    let result = Parser::parse_one("");
    assert!(matches!(result, Err(SqlError::Parse(_))));
}

#[test]
fn test_parse_error_multiple_statements() {
    let result = Parser::parse_one("SELECT * FROM users; SELECT * FROM orders");
    assert!(matches!(result, Err(SqlError::Parse(_))));
}
