//! User variable (@var) integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_set_and_select_user_variable() {
    let server = TestServer::start("uvar_basic").await;
    let mut conn = server.connect().await;

    // First verify unset variable is NULL
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @x").await.expect("SELECT @x failed");
    assert_eq!(rows.len(), 1, "Expected 1 row");
    assert_eq!(rows[0].0, None, "Unset variable should be NULL");

    conn.query_drop("SET @x = 42").await.expect("SET @x failed");

    let rows: Vec<(Option<String>,)> = conn.query("SELECT @x").await.expect("SELECT @x failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].0,
        Some("42".to_string()),
        "Variable should be 42 after SET"
    );

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_user_variable_string() {
    let server = TestServer::start("uvar_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("SET @name = 'hello'")
        .await
        .expect("SET @name failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT @name")
        .await
        .expect("SELECT @name failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "hello");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_user_variable_null_default() {
    let server = TestServer::start("uvar_null").await;
    let mut conn = server.connect().await;

    // Unset variable should return NULL
    let rows: Vec<(Option<String>,)> = conn
        .query("SELECT @undefined_var")
        .await
        .expect("SELECT @undefined_var failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, None);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_user_variable_in_expression() {
    let server = TestServer::start("uvar_expr").await;
    let mut conn = server.connect().await;

    conn.query_drop("SET @a = 10").await.expect("SET @a failed");

    conn.query_drop("SET @b = 20").await.expect("SET @b failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT @a + @b")
        .await
        .expect("SELECT @a + @b failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "30");

    drop(conn);
    server.shutdown().await;
}
