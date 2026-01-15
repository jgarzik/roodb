//! Basic SELECT integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_select_star() {
    let server = TestServer::start("select_star").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_star_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_star_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT * FROM select_star_tbl ORDER BY id")
        .await
        .expect("SELECT * failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE select_star_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_specific_columns() {
    let server = TestServer::start("select_cols").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_cols_tbl (id INT, name VARCHAR(50), age INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO select_cols_tbl (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
    )
    .await
    .expect("INSERT failed");

    // Select only name and age columns
    let rows: Vec<(String, i32)> = conn
        .query("SELECT name, age FROM select_cols_tbl ORDER BY id")
        .await
        .expect("SELECT columns failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("Alice".to_string(), 30));
    assert_eq!(rows[1], ("Bob".to_string(), 25));

    conn.query_drop("DROP TABLE select_cols_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_column_alias() {
    let server = TestServer::start("select_alias").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_alias_tbl (user_id INT, user_name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_alias_tbl (user_id, user_name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT failed");

    // Select with column aliases
    let rows: Vec<(i32, String)> = conn
        .query("SELECT user_id AS id, user_name AS name FROM select_alias_tbl")
        .await
        .expect("SELECT with alias failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE select_alias_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_expression() {
    let server = TestServer::start("select_expr").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_expr_tbl (a INT, b INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_expr_tbl (a, b) VALUES (10, 5)")
        .await
        .expect("INSERT failed");

    // Select with arithmetic expressions
    let rows: Vec<(i32, i32, i32, i32)> = conn
        .query("SELECT a + b, a - b, a * b, a / b FROM select_expr_tbl")
        .await
        .expect("SELECT with expression failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (15, 5, 50, 2)); // 10+5, 10-5, 10*5, 10/5

    conn.query_drop("DROP TABLE select_expr_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_literal() {
    let server = TestServer::start("select_literal").await;
    let mut conn = server.connect().await;

    // We need a table for SELECT (RooDB doesn't support SELECT without FROM)
    conn.query_drop("CREATE TABLE select_lit_tbl (x INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_lit_tbl (x) VALUES (1)")
        .await
        .expect("INSERT failed");

    // Select with literal values
    let rows: Vec<(i32, String, i32)> = conn
        .query("SELECT 42, 'hello', x FROM select_lit_tbl")
        .await
        .expect("SELECT literal failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (42, "hello".to_string(), 1));

    conn.query_drop("DROP TABLE select_lit_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_from_empty_table() {
    let server = TestServer::start("select_empty").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_empty_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // SELECT from empty table should return empty result
    let rows: Vec<(i32,)> = conn
        .query("SELECT * FROM select_empty_tbl")
        .await
        .expect("SELECT from empty table failed");
    assert_eq!(rows.len(), 0);

    conn.query_drop("DROP TABLE select_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_null_values() {
    let server = TestServer::start("select_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_null_tbl (id, name) VALUES (1, 'Alice'), (2, NULL)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, Option<String>)> = conn
        .query("SELECT id, name FROM select_null_tbl ORDER BY id")
        .await
        .expect("SELECT with NULL failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, Some("Alice".to_string()));
    assert_eq!(rows[1].0, 2);
    assert!(rows[1].1.is_none());

    conn.query_drop("DROP TABLE select_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_select_table_alias() {
    let server = TestServer::start("select_tbl_alias").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE select_alias2_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO select_alias2_tbl (id, name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT failed");

    // Select with table alias
    let rows: Vec<(i32, String)> = conn
        .query("SELECT t.id, t.name FROM select_alias2_tbl AS t")
        .await
        .expect("SELECT with table alias failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE select_alias2_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
