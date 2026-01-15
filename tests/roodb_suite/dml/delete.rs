//! DELETE integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_delete_single_row() {
    let server = TestServer::start("delete_single").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_single_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO delete_single_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("INSERT failed");

    conn.query_drop("DELETE FROM delete_single_tbl WHERE id = 1")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM delete_single_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE delete_single_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_multiple_rows() {
    let server = TestServer::start("delete_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_multi_tbl (id INT, category VARCHAR(20))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO delete_multi_tbl (id, category) VALUES (1, 'A'), (2, 'A'), (3, 'B')",
    )
    .await
    .expect("INSERT failed");

    conn.query_drop("DELETE FROM delete_multi_tbl WHERE category = 'A'")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, category FROM delete_multi_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (3, "B".to_string()));

    conn.query_drop("DROP TABLE delete_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_all_rows() {
    let server = TestServer::start("delete_all").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_all_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO delete_all_tbl (id) VALUES (1), (2), (3)")
        .await
        .expect("INSERT failed");

    // DELETE without WHERE removes all rows
    conn.query_drop("DELETE FROM delete_all_tbl")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM delete_all_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 0);

    conn.query_drop("DROP TABLE delete_all_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_no_matching_rows() {
    let server = TestServer::start("delete_no_match").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_nomatch_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO delete_nomatch_tbl (id) VALUES (1), (2)")
        .await
        .expect("INSERT failed");

    // DELETE with no matching rows - should succeed but delete nothing
    conn.query_drop("DELETE FROM delete_nomatch_tbl WHERE id = 999")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM delete_nomatch_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);

    conn.query_drop("DROP TABLE delete_nomatch_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_with_in_clause() {
    let server = TestServer::start("delete_in").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_in_tbl (id INT, name VARCHAR(20))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO delete_in_tbl (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    )
    .await
    .expect("INSERT failed");

    conn.query_drop("DELETE FROM delete_in_tbl WHERE id IN (2, 4)")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM delete_in_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (3, "c".to_string()));

    conn.query_drop("DROP TABLE delete_in_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_from_empty_table() {
    let server = TestServer::start("delete_empty").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE delete_empty_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // DELETE from empty table - should succeed
    conn.query_drop("DELETE FROM delete_empty_tbl")
        .await
        .expect("DELETE failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM delete_empty_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 0);

    conn.query_drop("DROP TABLE delete_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
