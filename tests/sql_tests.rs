//! SQL layer integration tests

use roodb::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
use roodb::sql::{Parser, Resolver, SqlError, Statement, TypeChecker};

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
fn full_pipeline(catalog: &Catalog, sql: &str) -> Result<(), SqlError> {
    let stmt = Parser::parse_one(sql)?;
    let resolver = Resolver::new(catalog);
    let resolved = resolver.resolve(stmt)?;
    TypeChecker::check(&resolved)?;
    Ok(())
}

// ============ Parser Tests ============

#[test]
fn test_parse_select_basic() {
    let stmt = Parser::parse_one("SELECT id, name FROM users").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert_eq!(select.from[0].name, "users");
            assert!(select.filter.is_none());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_with_where() {
    let stmt = Parser::parse_one("SELECT * FROM users WHERE id = 1 AND name = 'Alice'").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert!(select.filter.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_with_join() {
    let stmt = Parser::parse_one(
        "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
    )
    .unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.from.len(), 1);
            assert_eq!(select.joins.len(), 1);
            assert!(select.from[0].alias.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_with_group_by() {
    let stmt =
        Parser::parse_one("SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5")
            .unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.group_by.len(), 1);
            assert!(select.having.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_with_order_by_limit() {
    let stmt =
        Parser::parse_one("SELECT * FROM users ORDER BY name ASC, age DESC LIMIT 10 OFFSET 5")
            .unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.order_by.len(), 2);
            assert!(select.order_by[0].ascending);
            assert!(!select.order_by[1].ascending);
            assert_eq!(select.limit, Some(10));
            assert_eq!(select.offset, Some(5));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_insert() {
    let stmt =
        Parser::parse_one("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')")
            .unwrap();
    match stmt {
        Statement::Insert {
            table,
            columns,
            values,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), vec!["id", "name", "email"]);
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 3);
        }
        _ => panic!("Expected INSERT"),
    }
}

#[test]
fn test_parse_insert_multiple_rows() {
    let stmt = Parser::parse_one(
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .unwrap();
    match stmt {
        Statement::Insert { values, .. } => {
            assert_eq!(values.len(), 3);
        }
        _ => panic!("Expected INSERT"),
    }
}

#[test]
fn test_parse_update() {
    let stmt =
        Parser::parse_one("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1").unwrap();
    match stmt {
        Statement::Update {
            table,
            assignments,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(assignments.len(), 2);
            assert!(filter.is_some());
        }
        _ => panic!("Expected UPDATE"),
    }
}

#[test]
fn test_parse_delete() {
    let stmt = Parser::parse_one("DELETE FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Delete { table, filter } => {
            assert_eq!(table, "users");
            assert!(filter.is_some());
        }
        _ => panic!("Expected DELETE"),
    }
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
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            assert_eq!(name, "test");
            assert_eq!(columns.len(), 3);
            assert!(!columns[0].nullable);
            assert!(columns[1].nullable);
            assert_eq!(constraints.len(), 1);
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_drop_table() {
    let stmt = Parser::parse_one("DROP TABLE IF EXISTS users").unwrap();
    match stmt {
        Statement::DropTable { name, if_exists } => {
            assert_eq!(name, "users");
            assert!(if_exists);
        }
        _ => panic!("Expected DROP TABLE"),
    }
}

#[test]
fn test_parse_create_index() {
    let stmt = Parser::parse_one("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap();
    match stmt {
        Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => {
            assert_eq!(name, "idx_email");
            assert_eq!(table, "users");
            assert_eq!(columns, vec!["email"]);
            assert!(unique);
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

// ============ Resolver Tests ============

#[test]
fn test_resolve_simple_select() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "SELECT id, name FROM users").is_ok());
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
    assert!(full_pipeline(
        &catalog,
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')"
    )
    .is_ok());
}

#[test]
fn test_resolve_update() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "UPDATE users SET name = 'Bob' WHERE id = 1").is_ok());
}

#[test]
fn test_resolve_delete() {
    let catalog = test_catalog();
    assert!(full_pipeline(&catalog, "DELETE FROM users WHERE age > 30").is_ok());
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
