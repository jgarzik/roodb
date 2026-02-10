//! AUTO_INCREMENT integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_auto_increment_create_table() {
    let server = TestServer::start("ai_create").await;
    let mut conn = server.connect().await;

    // CREATE TABLE with AUTO_INCREMENT should parse and succeed
    conn.query_drop(
        "CREATE TABLE ai_create_tbl (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), PRIMARY KEY (id))",
    )
    .await
    .expect("CREATE TABLE with AUTO_INCREMENT failed");

    conn.query_drop("DROP TABLE ai_create_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_auto_increment_insert() {
    let server = TestServer::start("ai_insert").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE ai_ins_tbl (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), PRIMARY KEY (id))",
    )
    .await
    .expect("CREATE TABLE failed");

    // INSERT without specifying auto_increment column
    conn.query_drop("INSERT INTO ai_ins_tbl (name) VALUES ('Alice')")
        .await
        .expect("INSERT without id failed");

    conn.query_drop("INSERT INTO ai_ins_tbl (name) VALUES ('Bob')")
        .await
        .expect("INSERT without id 2 failed");

    // Both rows should have auto-generated IDs
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM ai_ins_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    // IDs should be non-zero and distinct
    assert!(rows[0].0 > 0, "First auto ID should be positive");
    assert!(rows[1].0 > 0, "Second auto ID should be positive");
    assert_ne!(rows[0].0, rows[1].0, "Auto IDs should be distinct");
    assert_eq!(rows[0].1, "Alice");
    assert_eq!(rows[1].1, "Bob");

    conn.query_drop("DROP TABLE ai_ins_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_auto_increment_with_explicit_id() {
    let server = TestServer::start("ai_explicit").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE ai_exp_tbl (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), PRIMARY KEY (id))",
    )
    .await
    .expect("CREATE TABLE failed");

    // INSERT with explicit ID should work
    conn.query_drop("INSERT INTO ai_exp_tbl (id, name) VALUES (100, 'Alice')")
        .await
        .expect("INSERT with explicit id failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM ai_exp_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 100);
    assert_eq!(rows[0].1, "Alice");

    conn.query_drop("DROP TABLE ai_exp_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_auto_increment_prepared() {
    let server = TestServer::start("ai_prep").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE ai_prep_tbl (id INT NOT NULL AUTO_INCREMENT, k INT NOT NULL, c VARCHAR(120) NOT NULL, PRIMARY KEY (id))",
    )
    .await
    .expect("CREATE TABLE failed");

    // Use prepared statement INSERT without specifying auto_increment column
    conn.exec_drop(
        "INSERT INTO ai_prep_tbl (k, c) VALUES (?, ?)",
        (100, "first"),
    )
    .await
    .expect("prep INSERT without id failed");

    conn.exec_drop(
        "INSERT INTO ai_prep_tbl (k, c) VALUES (?, ?)",
        (200, "second"),
    )
    .await
    .expect("prep INSERT without id 2 failed");

    let rows: Vec<(i32, i32, String)> = conn
        .query("SELECT id, k, c FROM ai_prep_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!(rows[0].0 > 0);
    assert!(rows[1].0 > 0);
    assert_ne!(rows[0].0, rows[1].0);
    assert_eq!(rows[0].1, 100);
    assert_eq!(rows[1].1, 200);

    conn.query_drop("DROP TABLE ai_prep_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
