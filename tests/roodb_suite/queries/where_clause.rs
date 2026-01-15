//! WHERE clause integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_where_equal() {
    let server = TestServer::start("where_eq").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_eq_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_eq_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_eq_tbl WHERE id = 2")
        .await
        .expect("SELECT with WHERE = failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE where_eq_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_not_equal() {
    let server = TestServer::start("where_neq").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_neq_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_neq_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_neq_tbl WHERE id != 2 ORDER BY id")
        .await
        .expect("SELECT with WHERE != failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (3, "Carol".to_string()));

    // Also test <> syntax
    let rows2: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_neq_tbl WHERE id <> 2 ORDER BY id")
        .await
        .expect("SELECT with WHERE <> failed");
    assert_eq!(rows2.len(), 2);

    conn.query_drop("DROP TABLE where_neq_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_less_than() {
    let server = TestServer::start("where_lt").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_lt_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO where_lt_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_lt_tbl WHERE value < 25 ORDER BY id")
        .await
        .expect("SELECT with WHERE < failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (2, 20));

    conn.query_drop("DROP TABLE where_lt_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_less_than_or_equal() {
    let server = TestServer::start("where_lte").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_lte_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO where_lte_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_lte_tbl WHERE value <= 20 ORDER BY id")
        .await
        .expect("SELECT with WHERE <= failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (2, 20));

    conn.query_drop("DROP TABLE where_lte_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_greater_than() {
    let server = TestServer::start("where_gt").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_gt_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO where_gt_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_gt_tbl WHERE value > 15 ORDER BY id")
        .await
        .expect("SELECT with WHERE > failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (2, 20));
    assert_eq!(rows[1], (3, 30));

    conn.query_drop("DROP TABLE where_gt_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_greater_than_or_equal() {
    let server = TestServer::start("where_gte").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_gte_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO where_gte_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_gte_tbl WHERE value >= 20 ORDER BY id")
        .await
        .expect("SELECT with WHERE >= failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (2, 20));
    assert_eq!(rows[1], (3, 30));

    conn.query_drop("DROP TABLE where_gte_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_and() {
    let server = TestServer::start("where_and").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_and_tbl (id INT, a INT, b INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_and_tbl (id, a, b) VALUES (1, 10, 100), (2, 20, 200), (3, 10, 200)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, i32, i32)> = conn
        .query("SELECT id, a, b FROM where_and_tbl WHERE a = 10 AND b = 100")
        .await
        .expect("SELECT with WHERE AND failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, 10, 100));

    conn.query_drop("DROP TABLE where_and_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_or() {
    let server = TestServer::start("where_or").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_or_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO where_or_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_or_tbl WHERE value = 10 OR value = 30 ORDER BY id")
        .await
        .expect("SELECT with WHERE OR failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (3, 30));

    conn.query_drop("DROP TABLE where_or_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_not() {
    let server = TestServer::start("where_not").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_not_tbl (id INT, active BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_not_tbl (id, active) VALUES (1, true), (2, false), (3, true)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, bool)> = conn
        .query("SELECT id, active FROM where_not_tbl WHERE NOT active ORDER BY id")
        .await
        .expect("SELECT with WHERE NOT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, false));

    conn.query_drop("DROP TABLE where_not_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_like() {
    let server = TestServer::start("where_like").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_like_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_like_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alfred')",
    )
    .await
    .expect("INSERT failed");

    // LIKE with prefix pattern
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_like_tbl WHERE name LIKE 'Al%' ORDER BY id")
        .await
        .expect("SELECT with WHERE LIKE prefix failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (3, "Alfred".to_string()));

    // LIKE with suffix pattern
    let rows2: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_like_tbl WHERE name LIKE '%ob' ORDER BY id")
        .await
        .expect("SELECT with WHERE LIKE suffix failed");
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE where_like_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_in() {
    let server = TestServer::start("where_in").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_in_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_in_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_in_tbl WHERE id IN (1, 3, 4) ORDER BY id")
        .await
        .expect("SELECT with WHERE IN failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (3, "Carol".to_string()));
    assert_eq!(rows[2], (4, "Dave".to_string()));

    conn.query_drop("DROP TABLE where_in_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_between() {
    let server = TestServer::start("where_between").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_between_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_between_tbl (id, value) VALUES (1, 5), (2, 10), (3, 15), (4, 20), (5, 25)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM where_between_tbl WHERE value BETWEEN 10 AND 20 ORDER BY id")
        .await
        .expect("SELECT with WHERE BETWEEN failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (2, 10));
    assert_eq!(rows[1], (3, 15));
    assert_eq!(rows[2], (4, 20));

    conn.query_drop("DROP TABLE where_between_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_is_null() {
    let server = TestServer::start("where_is_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_null_tbl (id, name) VALUES (1, 'Alice'), (2, NULL), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM where_null_tbl WHERE name IS NULL")
        .await
        .expect("SELECT with WHERE IS NULL failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);

    conn.query_drop("DROP TABLE where_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_is_not_null() {
    let server = TestServer::start("where_is_not_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_notnull_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_notnull_tbl (id, name) VALUES (1, 'Alice'), (2, NULL), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_notnull_tbl WHERE name IS NOT NULL ORDER BY id")
        .await
        .expect("SELECT with WHERE IS NOT NULL failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (3, "Carol".to_string()));

    conn.query_drop("DROP TABLE where_notnull_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_complex_condition() {
    let server = TestServer::start("where_complex").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_complex_tbl (id INT, a INT, b INT, c INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_complex_tbl (id, a, b, c) VALUES
         (1, 10, 20, 30), (2, 10, 25, 30), (3, 15, 20, 30), (4, 10, 20, 35)",
    )
    .await
    .expect("INSERT failed");

    // Complex condition: (a = 10 AND b = 20) OR c = 35
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM where_complex_tbl WHERE (a = 10 AND b = 20) OR c = 35 ORDER BY id")
        .await
        .expect("SELECT with complex WHERE failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 4);

    conn.query_drop("DROP TABLE where_complex_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_where_string_comparison() {
    let server = TestServer::start("where_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE where_str_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO where_str_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM where_str_tbl WHERE name = 'Bob'")
        .await
        .expect("SELECT with string comparison failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE where_str_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
