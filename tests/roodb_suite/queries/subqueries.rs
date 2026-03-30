//! Subquery integration tests.
//!
//! Tests for scalar subqueries in SELECT, WHERE, and IN subqueries.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_scalar_subquery_in_select() {
    let server = TestServer::start("subq_select").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t1 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO subq_t1 (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    // Scalar subquery returning MAX
    let rows: Vec<(i64,)> = conn
        .query("SELECT (SELECT MAX(val) FROM subq_t1)")
        .await
        .expect("scalar subquery in SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 30);

    // Scalar subquery returning MIN
    let rows: Vec<(i64,)> = conn
        .query("SELECT (SELECT MIN(val) FROM subq_t1)")
        .await
        .expect("scalar subquery MIN failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);

    // Scalar subquery returning COUNT
    let rows: Vec<(i64,)> = conn
        .query("SELECT (SELECT COUNT(*) FROM subq_t1)")
        .await
        .expect("scalar subquery COUNT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 3);

    conn.query_drop("DROP TABLE subq_t1")
        .await
        .expect("DROP TABLE failed");
    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_scalar_subquery_in_where() {
    let server = TestServer::start("subq_where").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t2 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO subq_t2 (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    // WHERE col = (SELECT MAX(val) FROM t)
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, val FROM subq_t2 WHERE val = (SELECT MAX(val) FROM subq_t2)")
        .await
        .expect("scalar subquery in WHERE failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (3, 30));

    // WHERE col > (SELECT MIN(val) FROM t)
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, val FROM subq_t2 WHERE val > (SELECT MIN(val) FROM subq_t2) ORDER BY id")
        .await
        .expect("scalar subquery > MIN failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (2, 20));
    assert_eq!(rows[1], (3, 30));

    conn.query_drop("DROP TABLE subq_t2")
        .await
        .expect("DROP TABLE failed");
    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_in_subquery() {
    let server = TestServer::start("subq_in").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t3 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE t3 failed");

    conn.query_drop("CREATE TABLE subq_t4 (id INT PRIMARY KEY, ref_val INT)")
        .await
        .expect("CREATE TABLE t4 failed");

    conn.query_drop("INSERT INTO subq_t3 (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT t3 failed");

    conn.query_drop("INSERT INTO subq_t4 (id, ref_val) VALUES (1, 10), (2, 30)")
        .await
        .expect("INSERT t4 failed");

    // WHERE val IN (SELECT ref_val FROM t4)
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, val FROM subq_t3 WHERE val IN (SELECT ref_val FROM subq_t4) ORDER BY id")
        .await
        .expect("IN subquery failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (3, 30));

    // WHERE val NOT IN (SELECT ref_val FROM t4)
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, val FROM subq_t3 WHERE val NOT IN (SELECT ref_val FROM subq_t4) ORDER BY id")
        .await
        .expect("NOT IN subquery failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, 20));

    conn.query_drop("DROP TABLE subq_t3")
        .await
        .expect("DROP TABLE t3 failed");
    conn.query_drop("DROP TABLE subq_t4")
        .await
        .expect("DROP TABLE t4 failed");
    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_scalar_subquery_no_rows() {
    let server = TestServer::start("subq_empty").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t5 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");

    // Scalar subquery from empty table returns NULL
    let rows: Vec<(Option<i64>,)> = conn
        .query("SELECT (SELECT MAX(val) FROM subq_t5)")
        .await
        .expect("scalar subquery on empty table failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, None); // NULL

    conn.query_drop("DROP TABLE subq_t5")
        .await
        .expect("DROP TABLE failed");
    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_scalar_subquery_with_column() {
    let server = TestServer::start("subq_col").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t6 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO subq_t6 (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    // SELECT col, (SELECT MAX(val) FROM t) as max_val FROM t
    let rows: Vec<(i32, i64)> = conn
        .query("SELECT id, (SELECT MAX(val) FROM subq_t6) FROM subq_t6 ORDER BY id")
        .await
        .expect("scalar subquery alongside column failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 30));
    assert_eq!(rows[1], (2, 30));
    assert_eq!(rows[2], (3, 30));

    conn.query_drop("DROP TABLE subq_t6")
        .await
        .expect("DROP TABLE failed");
    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_nested_scalar_subquery() {
    let server = TestServer::start("subq_nested").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE subq_t7 (id INT PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO subq_t7 (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    )
    .await
    .expect("INSERT failed");

    // Nested scalar subquery: subquery inside a subquery's WHERE clause
    // SELECT (SELECT MAX(val) FROM t WHERE val > (SELECT MIN(val) FROM t))
    let rows: Vec<(i64,)> = conn
        .query("SELECT (SELECT MAX(val) FROM subq_t7 WHERE val > (SELECT MIN(val) FROM subq_t7))")
        .await
        .expect("nested scalar subquery failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 50);

    // Nested: scalar subquery inside another scalar subquery in WHERE
    // Rows where val > (SELECT AVG(val) FROM t WHERE val > (SELECT MIN(val) FROM t))
    // Inner: MIN(val) = 10, so AVG of 20,30,40,50 = 35
    // Outer: val > 35 => 40, 50
    let rows: Vec<(i32, i32)> = conn
        .query(
            "SELECT id, val FROM subq_t7 \
             WHERE val > (SELECT AVG(val) FROM subq_t7 WHERE val > (SELECT MIN(val) FROM subq_t7)) \
             ORDER BY id",
        )
        .await
        .expect("nested subquery in WHERE failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (4, 40));
    assert_eq!(rows[1], (5, 50));

    // Three levels deep: subquery inside subquery inside subquery
    // SELECT (SELECT MAX(val) FROM t WHERE val > (SELECT AVG(val) FROM t WHERE val > (SELECT MIN(val) FROM t)))
    // Inner: MIN(val)=10, middle: AVG of 20,30,40,50 = 35, outer: MAX of 40,50 = 50
    let rows: Vec<(i64,)> = conn
        .query(
            "SELECT (SELECT MAX(val) FROM subq_t7 \
             WHERE val > (SELECT AVG(val) FROM subq_t7 \
             WHERE val > (SELECT MIN(val) FROM subq_t7)))",
        )
        .await
        .expect("3-level nested subquery failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 50);

    conn.query_drop("DROP TABLE subq_t7")
        .await
        .expect("DROP TABLE failed");
    drop(conn);
    server.shutdown().await;
}
