//! INSERT integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_insert_single_row() {
    let server = TestServer::start("insert_single").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_single_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_single_tbl (id, name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM insert_single_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE insert_single_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let server = TestServer::start("insert_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_multi_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_multi_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM insert_multi_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (2, 20));
    assert_eq!(rows[2], (3, 30));

    conn.query_drop("DROP TABLE insert_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_named_columns() {
    let server = TestServer::start("insert_named").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_named_tbl (a INT, b INT, c INT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert only some columns in different order
    conn.query_drop("INSERT INTO insert_named_tbl (c, a) VALUES (30, 10)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, Option<i32>, i32)> = conn
        .query("SELECT a, b, c FROM insert_named_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);
    assert!(rows[0].1.is_none()); // b should be NULL
    assert_eq!(rows[0].2, 30);

    conn.query_drop("DROP TABLE insert_named_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_null_values() {
    let server = TestServer::start("insert_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_null_tbl (id, name) VALUES (1, NULL)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, Option<String>)> = conn
        .query("SELECT id, name FROM insert_null_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert!(rows[0].1.is_none());

    conn.query_drop("DROP TABLE insert_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_negative_numbers() {
    let server = TestServer::start("insert_negative").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_neg_tbl (val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_neg_tbl (val) VALUES (-42)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT val FROM insert_neg_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, -42);

    conn.query_drop("DROP TABLE insert_neg_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_empty_string() {
    let server = TestServer::start("insert_empty_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_empty_tbl (name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_empty_tbl (name) VALUES ('')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT name FROM insert_empty_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "");

    conn.query_drop("DROP TABLE insert_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_boolean_values() {
    let server = TestServer::start("insert_bool").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_bool_tbl (active BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_bool_tbl (active) VALUES (true)")
        .await
        .expect("INSERT true failed");
    conn.query_drop("INSERT INTO insert_bool_tbl (active) VALUES (false)")
        .await
        .expect("INSERT false failed");

    let rows: Vec<(bool,)> = conn
        .query("SELECT active FROM insert_bool_tbl ORDER BY active")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!(!rows[0].0); // false first
    assert!(rows[1].0); // true second

    conn.query_drop("DROP TABLE insert_bool_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
