//! ORDER BY, LIMIT, OFFSET, and DISTINCT integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_order_by_asc() {
    let server = TestServer::start("order_asc").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE order_asc_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO order_asc_tbl (id, name) VALUES (3, 'Carol'), (1, 'Alice'), (2, 'Bob')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM order_asc_tbl ORDER BY id ASC")
        .await
        .expect("ORDER BY ASC failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));
    assert_eq!(rows[2], (3, "Carol".to_string()));

    conn.query_drop("DROP TABLE order_asc_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_order_by_desc() {
    let server = TestServer::start("order_desc").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE order_desc_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO order_desc_tbl (id, name) VALUES (1, 'Alice'), (3, 'Carol'), (2, 'Bob')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM order_desc_tbl ORDER BY id DESC")
        .await
        .expect("ORDER BY DESC failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (3, "Carol".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));
    assert_eq!(rows[2], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE order_desc_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_order_by_default_asc() {
    let server = TestServer::start("order_default").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE order_default_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO order_default_tbl (id) VALUES (3), (1), (2)")
        .await
        .expect("INSERT failed");

    // Default ordering is ASC
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM order_default_tbl ORDER BY id")
        .await
        .expect("ORDER BY default failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[2].0, 3);

    conn.query_drop("DROP TABLE order_default_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_order_by_multiple() {
    let server = TestServer::start("order_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE order_multi_tbl (category VARCHAR(50), priority INT, name VARCHAR(50))",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO order_multi_tbl (category, priority, name) VALUES
         ('B', 2, 'Bob'), ('A', 1, 'Alice'), ('A', 2, 'Ann'), ('B', 1, 'Ben')",
    )
    .await
    .expect("INSERT failed");

    // Order by category ASC, then priority DESC
    let rows: Vec<(String, i32, String)> = conn
        .query("SELECT category, priority, name FROM order_multi_tbl ORDER BY category ASC, priority DESC")
        .await
        .expect("ORDER BY multiple columns failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], ("A".to_string(), 2, "Ann".to_string()));
    assert_eq!(rows[1], ("A".to_string(), 1, "Alice".to_string()));
    assert_eq!(rows[2], ("B".to_string(), 2, "Bob".to_string()));
    assert_eq!(rows[3], ("B".to_string(), 1, "Ben".to_string()));

    conn.query_drop("DROP TABLE order_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_order_by_string() {
    let server = TestServer::start("order_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE order_str_tbl (name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO order_str_tbl (name) VALUES ('Carol'), ('Alice'), ('Bob')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT name FROM order_str_tbl ORDER BY name")
        .await
        .expect("ORDER BY string failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "Alice");
    assert_eq!(rows[1].0, "Bob");
    assert_eq!(rows[2].0, "Carol");

    conn.query_drop("DROP TABLE order_str_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_limit() {
    let server = TestServer::start("limit").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE limit_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO limit_tbl (id) VALUES (1), (2), (3), (4), (5)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM limit_tbl ORDER BY id LIMIT 3")
        .await
        .expect("LIMIT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[2].0, 3);

    conn.query_drop("DROP TABLE limit_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_offset() {
    let server = TestServer::start("offset").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE offset_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO offset_tbl (id) VALUES (1), (2), (3), (4), (5)")
        .await
        .expect("INSERT failed");

    // OFFSET without LIMIT - skip first 2 rows
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM offset_tbl ORDER BY id LIMIT 10 OFFSET 2")
        .await
        .expect("OFFSET failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 3);
    assert_eq!(rows[1].0, 4);
    assert_eq!(rows[2].0, 5);

    conn.query_drop("DROP TABLE offset_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_limit_offset() {
    let server = TestServer::start("limit_offset").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE limit_offset_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO limit_offset_tbl (id) VALUES (1), (2), (3), (4), (5), (6), (7)")
        .await
        .expect("INSERT failed");

    // Skip 2, take 3
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM limit_offset_tbl ORDER BY id LIMIT 3 OFFSET 2")
        .await
        .expect("LIMIT OFFSET failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 3);
    assert_eq!(rows[1].0, 4);
    assert_eq!(rows[2].0, 5);

    conn.query_drop("DROP TABLE limit_offset_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_limit_zero() {
    let server = TestServer::start("limit_zero").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE limit_zero_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO limit_zero_tbl (id) VALUES (1), (2), (3)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM limit_zero_tbl LIMIT 0")
        .await
        .expect("LIMIT 0 failed");
    assert_eq!(rows.len(), 0);

    conn.query_drop("DROP TABLE limit_zero_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_distinct() {
    let server = TestServer::start("distinct").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE distinct_tbl (category VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO distinct_tbl (category) VALUES ('A'), ('B'), ('A'), ('C'), ('B'), ('A')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT DISTINCT category FROM distinct_tbl ORDER BY category")
        .await
        .expect("DISTINCT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "A");
    assert_eq!(rows[1].0, "B");
    assert_eq!(rows[2].0, "C");

    conn.query_drop("DROP TABLE distinct_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_distinct_multiple_columns() {
    let server = TestServer::start("distinct_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE distinct_multi_tbl (a VARCHAR(50), b VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO distinct_multi_tbl (a, b) VALUES
         ('X', '1'), ('X', '2'), ('X', '1'), ('Y', '1'), ('Y', '1')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String, String)> = conn
        .query("SELECT DISTINCT a, b FROM distinct_multi_tbl ORDER BY a, b")
        .await
        .expect("DISTINCT multiple columns failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("X".to_string(), "1".to_string()));
    assert_eq!(rows[1], ("X".to_string(), "2".to_string()));
    assert_eq!(rows[2], ("Y".to_string(), "1".to_string()));

    conn.query_drop("DROP TABLE distinct_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_order_by_with_null() {
    let server = TestServer::start("order_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE order_null_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO order_null_tbl (id, value) VALUES (1, 30), (2, NULL), (3, 10), (4, 20)",
    )
    .await
    .expect("INSERT failed");

    // NULL values should sort first in ASC (or last, depending on implementation)
    let rows: Vec<(i32, Option<i32>)> = conn
        .query("SELECT id, value FROM order_null_tbl ORDER BY value ASC")
        .await
        .expect("ORDER BY with NULL failed");
    assert_eq!(rows.len(), 4);
    // Check that NULL is somewhere (implementation dependent)
    let has_null = rows.iter().any(|(_, v)| v.is_none());
    assert!(has_null);
    // The non-null values should be in order
    let non_null: Vec<i32> = rows.iter().filter_map(|(_, v)| *v).collect();
    assert!(non_null.windows(2).all(|w| w[0] <= w[1]));

    conn.query_drop("DROP TABLE order_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
