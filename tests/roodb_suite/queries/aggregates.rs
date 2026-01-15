//! Aggregate function integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_count_star() {
    let server = TestServer::start("count_star").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE count_star_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO count_star_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM count_star_tbl")
        .await
        .expect("COUNT(*) failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 3);

    conn.query_drop("DROP TABLE count_star_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_count_column() {
    let server = TestServer::start("count_col").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE count_col_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO count_col_tbl (id, name) VALUES (1, 'Alice'), (2, NULL), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    // COUNT(column) excludes NULL values
    let rows: Vec<(i64,)> = conn
        .query("SELECT COUNT(name) FROM count_col_tbl")
        .await
        .expect("COUNT(column) failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2); // NULL is not counted

    conn.query_drop("DROP TABLE count_col_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_sum() {
    let server = TestServer::start("sum").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE sum_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO sum_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i64,)> = conn
        .query("SELECT SUM(value) FROM sum_tbl")
        .await
        .expect("SUM failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 60);

    conn.query_drop("DROP TABLE sum_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_avg() {
    let server = TestServer::start("avg").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE avg_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO avg_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(f64,)> = conn
        .query("SELECT AVG(value) FROM avg_tbl")
        .await
        .expect("AVG failed");
    assert_eq!(rows.len(), 1);
    assert!((rows[0].0 - 20.0).abs() < 0.01);

    conn.query_drop("DROP TABLE avg_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_min() {
    let server = TestServer::start("min").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE min_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO min_tbl (id, value) VALUES (1, 50), (2, 10), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT MIN(value) FROM min_tbl")
        .await
        .expect("MIN failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);

    conn.query_drop("DROP TABLE min_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_max() {
    let server = TestServer::start("max").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE max_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO max_tbl (id, value) VALUES (1, 50), (2, 10), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT MAX(value) FROM max_tbl")
        .await
        .expect("MAX failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 50);

    conn.query_drop("DROP TABLE max_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_aggregate_with_null() {
    let server = TestServer::start("agg_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE agg_null_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO agg_null_tbl (id, value) VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .expect("INSERT failed");

    // SUM should ignore NULLs
    let sum_rows: Vec<(i64,)> = conn
        .query("SELECT SUM(value) FROM agg_null_tbl")
        .await
        .expect("SUM with NULL failed");
    assert_eq!(sum_rows[0].0, 40);

    // AVG should ignore NULLs
    let avg_rows: Vec<(f64,)> = conn
        .query("SELECT AVG(value) FROM agg_null_tbl")
        .await
        .expect("AVG with NULL failed");
    assert!((avg_rows[0].0 - 20.0).abs() < 0.01); // (10 + 30) / 2

    // COUNT(*) includes NULLs
    let count_star: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM agg_null_tbl")
        .await
        .expect("COUNT(*) failed");
    assert_eq!(count_star[0].0, 3);

    // COUNT(column) excludes NULLs
    let count_col: Vec<(i64,)> = conn
        .query("SELECT COUNT(value) FROM agg_null_tbl")
        .await
        .expect("COUNT(column) failed");
    assert_eq!(count_col[0].0, 2);

    conn.query_drop("DROP TABLE agg_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_aggregate_empty_table() {
    let server = TestServer::start("agg_empty").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE agg_empty_tbl (value INT)")
        .await
        .expect("CREATE TABLE failed");

    // COUNT(*) on empty table = 0
    let count_rows: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM agg_empty_tbl")
        .await
        .expect("COUNT(*) on empty failed");
    assert_eq!(count_rows[0].0, 0);

    // SUM on empty table = NULL
    let sum_rows: Vec<(Option<i64>,)> = conn
        .query("SELECT SUM(value) FROM agg_empty_tbl")
        .await
        .expect("SUM on empty failed");
    assert!(sum_rows[0].0.is_none());

    // AVG on empty table = NULL
    let avg_rows: Vec<(Option<f64>,)> = conn
        .query("SELECT AVG(value) FROM agg_empty_tbl")
        .await
        .expect("AVG on empty failed");
    assert!(avg_rows[0].0.is_none());

    // MIN on empty table = NULL
    let min_rows: Vec<(Option<i32>,)> = conn
        .query("SELECT MIN(value) FROM agg_empty_tbl")
        .await
        .expect("MIN on empty failed");
    assert!(min_rows[0].0.is_none());

    // MAX on empty table = NULL
    let max_rows: Vec<(Option<i32>,)> = conn
        .query("SELECT MAX(value) FROM agg_empty_tbl")
        .await
        .expect("MAX on empty failed");
    assert!(max_rows[0].0.is_none());

    conn.query_drop("DROP TABLE agg_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_multiple_aggregates() {
    let server = TestServer::start("multi_agg").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE multi_agg_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO multi_agg_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i64, i64, f64, i32, i32)> = conn
        .query("SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM multi_agg_tbl")
        .await
        .expect("Multiple aggregates failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 3); // COUNT
    assert_eq!(rows[0].1, 60); // SUM
    assert!((rows[0].2 - 20.0).abs() < 0.01); // AVG
    assert_eq!(rows[0].3, 10); // MIN
    assert_eq!(rows[0].4, 30); // MAX

    conn.query_drop("DROP TABLE multi_agg_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
