//! CREATE INDEX integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_create_index_basic() {
    let server = TestServer::start("create_idx").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE idx_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("CREATE INDEX idx_name ON idx_tbl (name)")
        .await
        .expect("CREATE INDEX failed");

    // Verify table still works with index
    conn.query_drop("INSERT INTO idx_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM idx_tbl WHERE name = 'test'")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP INDEX idx_name")
        .await
        .expect("DROP INDEX failed");
    conn.query_drop("DROP TABLE idx_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_unique_index() {
    let server = TestServer::start("create_unique_idx").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE unique_idx_tbl (id INT, email VARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("CREATE UNIQUE INDEX idx_email ON unique_idx_tbl (email)")
        .await
        .expect("CREATE UNIQUE INDEX failed");

    conn.query_drop("INSERT INTO unique_idx_tbl (id, email) VALUES (1, 'test@example.com')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, email FROM unique_idx_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP INDEX idx_email")
        .await
        .expect("DROP INDEX failed");
    conn.query_drop("DROP TABLE unique_idx_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_index_multi_column() {
    let server = TestServer::start("create_multi_idx").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE multi_idx_tbl (a INT, b INT, c INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("CREATE INDEX idx_ab ON multi_idx_tbl (a, b)")
        .await
        .expect("CREATE INDEX failed");

    conn.query_drop("INSERT INTO multi_idx_tbl (a, b, c) VALUES (1, 2, 3)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32, i32)> = conn
        .query("SELECT a, b, c FROM multi_idx_tbl WHERE a = 1 AND b = 2")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP INDEX idx_ab")
        .await
        .expect("DROP INDEX failed");
    conn.query_drop("DROP TABLE multi_idx_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
