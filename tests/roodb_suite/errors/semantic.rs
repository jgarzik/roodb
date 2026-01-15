//! Semantic error tests (table not found, column not found, etc.)

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_table_not_found() {
    let server = TestServer::start("err_no_table").await;
    let mut conn = server.connect().await;

    // SELECT from non-existent table
    let result: Result<Vec<(i32,)>, _> = conn.query("SELECT id FROM nonexistent_table").await;
    assert!(result.is_err(), "Expected table not found error");

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("table") || err_str.contains("not found") || err_str.contains("exist"),
        "Error should mention table: {}",
        err_str
    );

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_column_not_found() {
    let server = TestServer::start("err_no_col").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE err_col_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    // SELECT non-existent column
    let result: Result<Vec<(i32,)>, _> = conn
        .query("SELECT nonexistent_column FROM err_col_tbl")
        .await;
    assert!(result.is_err(), "Expected column not found error");

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("column") || err_str.contains("not found") || err_str.contains("unknown"),
        "Error should mention column: {}",
        err_str
    );

    conn.query_drop("DROP TABLE err_col_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_table_already_exists() {
    let server = TestServer::start("err_tbl_exists").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE err_exists_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Try to create the same table again
    let result: Result<(), _> = conn
        .query_drop("CREATE TABLE err_exists_tbl (id INT)")
        .await;
    assert!(result.is_err(), "Expected table already exists error");

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("exist") || err_str.contains("already"),
        "Error should mention exists: {}",
        err_str
    );

    // Reconnect since server may close connection after error
    drop(conn);
    let mut conn = server.connect().await;

    conn.query_drop("DROP TABLE err_exists_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_nonexistent_table() {
    let server = TestServer::start("err_drop_none").await;
    let mut conn = server.connect().await;

    // Try to drop a table that doesn't exist
    let result: Result<(), _> = conn.query_drop("DROP TABLE nonexistent_drop_tbl").await;
    assert!(result.is_err(), "Expected table not found error on DROP");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_ambiguous_column() {
    let server = TestServer::start("err_ambig_col").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE err_ambig_a (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE a failed");
    conn.query_drop("CREATE TABLE err_ambig_b (id INT, value INT)")
        .await
        .expect("CREATE TABLE b failed");

    // SELECT ambiguous column 'id' without table qualifier
    let result: Result<Vec<(i32,)>, _> =
        conn.query("SELECT id FROM err_ambig_a, err_ambig_b").await;
    assert!(result.is_err(), "Expected ambiguous column error");

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("ambiguous") || err_str.contains("column"),
        "Error should mention ambiguous: {}",
        err_str
    );

    conn.query_drop("DROP TABLE err_ambig_a")
        .await
        .expect("DROP TABLE a failed");
    conn.query_drop("DROP TABLE err_ambig_b")
        .await
        .expect("DROP TABLE b failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_division_by_zero() {
    let server = TestServer::start("err_div_zero").await;
    let mut conn = server.connect().await;

    // Division by zero
    let result: Result<Vec<(i64,)>, _> = conn.query("SELECT 1 / 0").await;
    // Note: Some DBs return NULL, others error. Check behavior.
    // If it's NULL, the query succeeds but with NULL result
    if let Ok(rows) = result {
        if !rows.is_empty() {
            // This is OK - some implementations return NULL or Infinity
        }
    }
    // If it errors, that's also acceptable
    // This test documents the behavior either way

    drop(conn);
    server.shutdown().await;
}
