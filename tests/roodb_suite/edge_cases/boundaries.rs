//! Boundary condition tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_empty_table() {
    let server = TestServer::start("edge_empty_tbl").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_empty_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    // SELECT from empty table
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM edge_empty_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 0);

    // COUNT on empty table
    let count: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM edge_empty_tbl")
        .await
        .expect("COUNT failed");
    assert_eq!(count[0].0, 0);

    conn.query_drop("DROP TABLE edge_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_many_columns() {
    let server = TestServer::start("edge_many_cols").await;
    let mut conn = server.connect().await;

    // Create table with 20 columns
    let cols: Vec<String> = (1..=20).map(|i| format!("col{} INT", i)).collect();
    let create_sql = format!("CREATE TABLE edge_many_cols_tbl ({})", cols.join(", "));
    conn.query_drop(&create_sql)
        .await
        .expect("CREATE TABLE failed");

    // Insert a row with all columns
    let vals: Vec<String> = (1..=20).map(|i| i.to_string()).collect();
    let insert_sql = format!("INSERT INTO edge_many_cols_tbl VALUES ({})", vals.join(", "));
    conn.query_drop(&insert_sql)
        .await
        .expect("INSERT failed");

    // Select specific columns to verify
    let rows: Vec<(i32, i32, i32)> = conn
        .query("SELECT col1, col10, col20 FROM edge_many_cols_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, 10, 20));

    conn.query_drop("DROP TABLE edge_many_cols_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_many_rows() {
    let server = TestServer::start("edge_many_rows").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_many_rows_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert 100 rows
    for batch in 0..10 {
        let vals: Vec<String> = (0..10)
            .map(|i| {
                let id = batch * 10 + i;
                format!("({}, {})", id, id * 10)
            })
            .collect();
        let insert_sql = format!(
            "INSERT INTO edge_many_rows_tbl (id, value) VALUES {}",
            vals.join(", ")
        );
        conn.query_drop(&insert_sql)
            .await
            .expect("INSERT failed");
    }

    // Verify count
    let count: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM edge_many_rows_tbl")
        .await
        .expect("COUNT failed");
    assert_eq!(count[0].0, 100);

    // Verify ordering and limit
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM edge_many_rows_tbl ORDER BY id LIMIT 5")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (0, 0));
    assert_eq!(rows[4], (4, 40));

    conn.query_drop("DROP TABLE edge_many_rows_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_null_comparisons() {
    let server = TestServer::start("edge_null_cmp").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_null_cmp_tbl (id INT, val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO edge_null_cmp_tbl (id, val) VALUES (1, NULL), (2, 10), (3, NULL)")
        .await
        .expect("INSERT failed");

    // NULL = NULL should NOT match (NULL is not equal to anything)
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM edge_null_cmp_tbl WHERE val = NULL")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 0, "NULL = NULL should match nothing");

    // IS NULL should match
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM edge_null_cmp_tbl WHERE val IS NULL ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 3);

    // IS NOT NULL should match
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM edge_null_cmp_tbl WHERE val IS NOT NULL")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);

    conn.query_drop("DROP TABLE edge_null_cmp_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_boundary_integers() {
    let server = TestServer::start("edge_bound_int").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_bound_int_tbl (small SMALLINT, med INT, big BIGINT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert boundary values
    conn.query_drop(
        "INSERT INTO edge_bound_int_tbl (small, med, big) VALUES
         (-32768, -2147483648, -9223372036854775807),
         (32767, 2147483647, 9223372036854775807)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i16, i32, i64)> = conn
        .query("SELECT small, med, big FROM edge_bound_int_tbl ORDER BY small")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (-32768, -2147483648, -9223372036854775807));
    assert_eq!(rows[1], (32767, 2147483647, 9223372036854775807));

    conn.query_drop("DROP TABLE edge_bound_int_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
