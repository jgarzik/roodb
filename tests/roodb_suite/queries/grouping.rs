//! GROUP BY and HAVING integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_group_by_single() {
    let server = TestServer::start("group_single").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_single_tbl (category VARCHAR(50), value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_single_tbl (category, value) VALUES
         ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('B', 50)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String, i64)> = conn
        .query(
            "SELECT category, COUNT(*) FROM group_single_tbl GROUP BY category ORDER BY category",
        )
        .await
        .expect("GROUP BY single column failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("A".to_string(), 2));
    assert_eq!(rows[1], ("B".to_string(), 3));

    conn.query_drop("DROP TABLE group_single_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_multiple() {
    let server = TestServer::start("group_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_multi_tbl (cat1 VARCHAR(50), cat2 VARCHAR(50), value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_multi_tbl (cat1, cat2, value) VALUES
         ('A', 'X', 10), ('A', 'X', 20), ('A', 'Y', 30), ('B', 'X', 40)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String, String, i64)> = conn
        .query("SELECT cat1, cat2, COUNT(*) FROM group_multi_tbl GROUP BY cat1, cat2 ORDER BY cat1, cat2")
        .await
        .expect("GROUP BY multiple columns failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("A".to_string(), "X".to_string(), 2));
    assert_eq!(rows[1], ("A".to_string(), "Y".to_string(), 1));
    assert_eq!(rows[2], ("B".to_string(), "X".to_string(), 1));

    conn.query_drop("DROP TABLE group_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_with_sum() {
    let server = TestServer::start("group_sum").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_sum_tbl (category VARCHAR(50), amount INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_sum_tbl (category, amount) VALUES
         ('Sales', 100), ('Sales', 200), ('Marketing', 50), ('Marketing', 75)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String, i64)> = conn
        .query(
            "SELECT category, SUM(amount) FROM group_sum_tbl GROUP BY category ORDER BY category",
        )
        .await
        .expect("GROUP BY with SUM failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("Marketing".to_string(), 125));
    assert_eq!(rows[1], ("Sales".to_string(), 300));

    conn.query_drop("DROP TABLE group_sum_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_with_avg() {
    let server = TestServer::start("group_avg").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_avg_tbl (category VARCHAR(50), score INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_avg_tbl (category, score) VALUES
         ('A', 80), ('A', 90), ('B', 70), ('B', 80), ('B', 90)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String, f64)> = conn
        .query("SELECT category, AVG(score) FROM group_avg_tbl GROUP BY category ORDER BY category")
        .await
        .expect("GROUP BY with AVG failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "A".to_string());
    assert!((rows[0].1 - 85.0).abs() < 0.01);
    assert_eq!(rows[1].0, "B".to_string());
    assert!((rows[1].1 - 80.0).abs() < 0.01);

    conn.query_drop("DROP TABLE group_avg_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_having() {
    let server = TestServer::start("having").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE having_tbl (category VARCHAR(50), value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO having_tbl (category, value) VALUES
         ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('B', 50), ('C', 60)",
    )
    .await
    .expect("INSERT failed");

    // Only groups with more than 1 row
    let rows: Vec<(String, i64)> = conn
        .query("SELECT category, COUNT(*) FROM having_tbl GROUP BY category HAVING COUNT(*) > 1 ORDER BY category")
        .await
        .expect("HAVING failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("A".to_string(), 2));
    assert_eq!(rows[1], ("B".to_string(), 3));

    conn.query_drop("DROP TABLE having_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_having_with_sum() {
    let server = TestServer::start("having_sum").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE having_sum_tbl (category VARCHAR(50), amount INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO having_sum_tbl (category, amount) VALUES
         ('A', 100), ('A', 50), ('B', 30), ('B', 20), ('C', 200)",
    )
    .await
    .expect("INSERT failed");

    // Only groups where total > 100
    let rows: Vec<(String, i64)> = conn
        .query("SELECT category, SUM(amount) FROM having_sum_tbl GROUP BY category HAVING SUM(amount) > 100 ORDER BY category")
        .await
        .expect("HAVING with SUM failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("A".to_string(), 150));
    assert_eq!(rows[1], ("C".to_string(), 200));

    conn.query_drop("DROP TABLE having_sum_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_with_where() {
    let server = TestServer::start("group_where").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE group_where_tbl (category VARCHAR(50), status VARCHAR(20), value INT)",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_where_tbl (category, status, value) VALUES
         ('A', 'active', 10), ('A', 'inactive', 20), ('A', 'active', 30),
         ('B', 'active', 40), ('B', 'inactive', 50)",
    )
    .await
    .expect("INSERT failed");

    // Filter with WHERE before grouping
    let rows: Vec<(String, i64)> = conn
        .query("SELECT category, SUM(value) FROM group_where_tbl WHERE status = 'active' GROUP BY category ORDER BY category")
        .await
        .expect("GROUP BY with WHERE failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("A".to_string(), 40)); // 10 + 30
    assert_eq!(rows[1], ("B".to_string(), 40)); // 40

    conn.query_drop("DROP TABLE group_where_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_null_values() {
    let server = TestServer::start("group_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_null_tbl (category VARCHAR(50), value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_null_tbl (category, value) VALUES
         ('A', 10), ('A', 20), (NULL, 30), (NULL, 40)",
    )
    .await
    .expect("INSERT failed");

    // NULL values should form their own group
    let rows: Vec<(Option<String>, i64)> = conn
        .query("SELECT category, COUNT(*) FROM group_null_tbl GROUP BY category ORDER BY category")
        .await
        .expect("GROUP BY with NULL failed");
    assert_eq!(rows.len(), 2);
    // NULL group should be first in ORDER BY (depends on implementation)
    // Let's just check we have the right counts
    let null_group = rows.iter().find(|(cat, _)| cat.is_none());
    let a_group = rows.iter().find(|(cat, _)| cat.as_deref() == Some("A"));
    assert!(null_group.is_some());
    assert_eq!(null_group.unwrap().1, 2);
    assert!(a_group.is_some());
    assert_eq!(a_group.unwrap().1, 2);

    conn.query_drop("DROP TABLE group_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_group_by_ordinal() {
    let server = TestServer::start("group_ordinal").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE group_ord_tbl (a INT, b VARCHAR(10), c INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO group_ord_tbl (a, b, c) VALUES
         (1, 'x', 10), (1, 'y', 20), (2, 'x', 30), (2, 'y', 40), (1, 'x', 50)",
    )
    .await
    .expect("INSERT failed");

    // GROUP BY 1 — group by first select column (a)
    let rows: Vec<(i64, i64)> = conn
        .query("SELECT a, SUM(c) FROM group_ord_tbl GROUP BY 1 ORDER BY a")
        .await
        .expect("GROUP BY ordinal 1 failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 80)); // 10 + 20 + 50
    assert_eq!(rows[1], (2, 70)); // 30 + 40

    // GROUP BY with multiple ordinals: GROUP BY 1, 2
    let rows: Vec<(i64, String, i64)> = conn
        .query("SELECT a, b, SUM(c) FROM group_ord_tbl GROUP BY 1, 2 ORDER BY a, b")
        .await
        .expect("GROUP BY ordinal 1, 2 failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, "x".to_string(), 60)); // 10 + 50
    assert_eq!(rows[1], (1, "y".to_string(), 20));
    assert_eq!(rows[2], (2, "x".to_string(), 30));
    assert_eq!(rows[3], (2, "y".to_string(), 40));

    // GROUP BY mixing ordinal and column name: GROUP BY 1, b
    let rows: Vec<(i64, String, i64)> = conn
        .query("SELECT a, b, COUNT(*) FROM group_ord_tbl GROUP BY 1, b ORDER BY a, b")
        .await
        .expect("GROUP BY ordinal + name failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, "x".to_string(), 2));
    assert_eq!(rows[1], (1, "y".to_string(), 1));
    assert_eq!(rows[2], (2, "x".to_string(), 1));
    assert_eq!(rows[3], (2, "y".to_string(), 1));

    // Out-of-range ordinal should error
    let err = conn
        .query_drop("SELECT a, SUM(c) FROM group_ord_tbl GROUP BY 3")
        .await;
    assert!(err.is_err(), "GROUP BY out-of-range ordinal should fail");

    conn.query_drop("DROP TABLE group_ord_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_having_aggregate_is_not_null() {
    let server = TestServer::start("having_agg_isnotnull").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE having_inn_tbl (a VARCHAR(50), b INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO having_inn_tbl (a, b) VALUES
         ('x', 10), ('x', 20), ('y', NULL), ('y', NULL), ('z', 30)",
    )
    .await
    .expect("INSERT failed");

    // HAVING SUM(b) IS NOT NULL — should return groups where SUM(b) is not null
    let rows: Vec<(String, i64)> = conn
        .query(
            "SELECT a, SUM(b) FROM having_inn_tbl GROUP BY a HAVING SUM(b) IS NOT NULL ORDER BY a",
        )
        .await
        .expect("HAVING SUM(b) IS NOT NULL failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("x".to_string(), 30));
    assert_eq!(rows[1], ("z".to_string(), 30));

    // HAVING SUM(b) IS NULL — should return groups where SUM(b) is null
    let rows: Vec<(String, Option<i64>)> = conn
        .query("SELECT a, SUM(b) FROM having_inn_tbl GROUP BY a HAVING SUM(b) IS NULL ORDER BY a")
        .await
        .expect("HAVING SUM(b) IS NULL failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "y".to_string());
    assert_eq!(rows[0].1, None);

    conn.query_drop("DROP TABLE having_inn_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_having_alias_is_null() {
    let server = TestServer::start("having_alias_isnull").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE having_ain_tbl (a VARCHAR(50), b INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO having_ain_tbl (a, b) VALUES
         ('x', 10), ('x', 20), ('y', NULL), ('y', NULL), ('z', 30)",
    )
    .await
    .expect("INSERT failed");

    // HAVING alias IS NULL — using alias 's' for SUM(b)
    let rows: Vec<(String, Option<i64>)> = conn
        .query("SELECT a, SUM(b) AS s FROM having_ain_tbl GROUP BY a HAVING s IS NULL ORDER BY a")
        .await
        .expect("HAVING alias IS NULL failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "y".to_string());
    assert_eq!(rows[0].1, None);

    // HAVING alias IS NOT NULL — using alias 's' for SUM(b)
    let rows: Vec<(String, i64)> = conn
        .query(
            "SELECT a, SUM(b) AS s FROM having_ain_tbl GROUP BY a HAVING s IS NOT NULL ORDER BY a",
        )
        .await
        .expect("HAVING alias IS NOT NULL failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("x".to_string(), 30));
    assert_eq!(rows[1], ("z".to_string(), 30));

    conn.query_drop("DROP TABLE having_ain_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
