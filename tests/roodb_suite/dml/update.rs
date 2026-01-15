//! UPDATE integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_update_single_row() {
    let server = TestServer::start("update_single").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_single_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO update_single_tbl (id, name) VALUES (1, 'old')")
        .await
        .expect("INSERT failed");

    conn.query_drop("UPDATE update_single_tbl SET name = 'new' WHERE id = 1")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM update_single_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "new".to_string()));

    conn.query_drop("DROP TABLE update_single_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_update_multiple_rows() {
    let server = TestServer::start("update_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_multi_tbl (id INT, status VARCHAR(20))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO update_multi_tbl (id, status) VALUES (1, 'pending'), (2, 'pending'), (3, 'done')",
    )
    .await
    .expect("INSERT failed");

    conn.query_drop("UPDATE update_multi_tbl SET status = 'processed' WHERE status = 'pending'")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, status FROM update_multi_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "processed".to_string()));
    assert_eq!(rows[1], (2, "processed".to_string()));
    assert_eq!(rows[2], (3, "done".to_string()));

    conn.query_drop("DROP TABLE update_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_update_multiple_columns() {
    let server = TestServer::start("update_multi_col").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_cols_tbl (id INT, a INT, b INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO update_cols_tbl (id, a, b) VALUES (1, 10, 20)")
        .await
        .expect("INSERT failed");

    conn.query_drop("UPDATE update_cols_tbl SET a = 100, b = 200 WHERE id = 1")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, i32, i32)> = conn
        .query("SELECT id, a, b FROM update_cols_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, 100, 200));

    conn.query_drop("DROP TABLE update_cols_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_update_all_rows() {
    let server = TestServer::start("update_all").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_all_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO update_all_tbl (id, value) VALUES (1, 1), (2, 2), (3, 3)")
        .await
        .expect("INSERT failed");

    // UPDATE without WHERE affects all rows
    conn.query_drop("UPDATE update_all_tbl SET value = 0")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM update_all_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 0));
    assert_eq!(rows[1], (2, 0));
    assert_eq!(rows[2], (3, 0));

    conn.query_drop("DROP TABLE update_all_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_update_to_null() {
    let server = TestServer::start("update_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO update_null_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    conn.query_drop("UPDATE update_null_tbl SET name = NULL WHERE id = 1")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, Option<String>)> = conn
        .query("SELECT id, name FROM update_null_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert!(rows[0].1.is_none());

    conn.query_drop("DROP TABLE update_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_update_no_matching_rows() {
    let server = TestServer::start("update_no_match").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE update_nomatch_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO update_nomatch_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    // UPDATE with no matching rows - should succeed but change nothing
    conn.query_drop("UPDATE update_nomatch_tbl SET name = 'changed' WHERE id = 999")
        .await
        .expect("UPDATE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM update_nomatch_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "test".to_string())); // unchanged

    conn.query_drop("DROP TABLE update_nomatch_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
