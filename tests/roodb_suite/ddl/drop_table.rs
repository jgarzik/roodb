//! DROP TABLE integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_drop_table_basic() {
    let server = TestServer::start("drop_basic").await;
    let mut conn = server.connect().await;

    // Create and drop
    conn.query_drop("CREATE TABLE drop_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("DROP TABLE drop_tbl")
        .await
        .expect("DROP TABLE failed");

    // Verify table is gone - SELECT should fail
    let result: Result<Vec<(i32,)>, _> = conn.query("SELECT id FROM drop_tbl").await;
    assert!(result.is_err(), "Table should not exist after DROP");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let server = TestServer::start("drop_if_exists").await;
    let mut conn = server.connect().await;

    // Drop non-existent table with IF EXISTS - should not error
    conn.query_drop("DROP TABLE IF EXISTS nonexistent_tbl")
        .await
        .expect("DROP TABLE IF EXISTS failed");

    // Create and drop with IF EXISTS
    conn.query_drop("CREATE TABLE exists_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("DROP TABLE IF EXISTS exists_tbl")
        .await
        .expect("DROP TABLE IF EXISTS failed");

    // Drop again with IF EXISTS - should not error
    conn.query_drop("DROP TABLE IF EXISTS exists_tbl")
        .await
        .expect("Second DROP TABLE IF EXISTS failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_table_with_data() {
    let server = TestServer::start("drop_with_data").await;
    let mut conn = server.connect().await;

    // Create table with data
    conn.query_drop("CREATE TABLE data_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO data_tbl (id, name) VALUES (1, 'one')")
        .await
        .expect("INSERT 1 failed");
    conn.query_drop("INSERT INTO data_tbl (id, name) VALUES (2, 'two')")
        .await
        .expect("INSERT 2 failed");

    // Drop table with data
    conn.query_drop("DROP TABLE data_tbl")
        .await
        .expect("DROP TABLE failed");

    // Verify gone
    let result: Result<Vec<(i32,)>, _> = conn.query("SELECT id FROM data_tbl").await;
    assert!(result.is_err(), "Table should not exist after DROP");

    drop(conn);
    server.shutdown().await;
}
