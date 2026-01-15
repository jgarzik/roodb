//! DROP INDEX integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_drop_index_basic() {
    let server = TestServer::start("drop_idx").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE drop_idx_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("CREATE INDEX idx_name ON drop_idx_tbl (name)")
        .await
        .expect("CREATE INDEX failed");

    conn.query_drop("DROP INDEX idx_name")
        .await
        .expect("DROP INDEX failed");

    // Table should still work after index dropped
    conn.query_drop("INSERT INTO drop_idx_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM drop_idx_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP TABLE drop_idx_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_index_and_recreate() {
    let server = TestServer::start("drop_recreate_idx").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE recreate_idx_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    // Create, drop, and recreate index
    conn.query_drop("CREATE INDEX idx_val ON recreate_idx_tbl (value)")
        .await
        .expect("CREATE INDEX failed");

    conn.query_drop("DROP INDEX idx_val")
        .await
        .expect("DROP INDEX failed");

    conn.query_drop("CREATE INDEX idx_val ON recreate_idx_tbl (value)")
        .await
        .expect("Recreate INDEX failed");

    conn.query_drop("INSERT INTO recreate_idx_tbl (id, value) VALUES (1, 100)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM recreate_idx_tbl WHERE value = 100")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP INDEX idx_val")
        .await
        .expect("DROP INDEX failed");
    conn.query_drop("DROP TABLE recreate_idx_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
