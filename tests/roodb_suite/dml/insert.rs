//! INSERT integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_insert_single_row() {
    let server = TestServer::start("insert_single").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_single_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_single_tbl (id, name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM insert_single_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE insert_single_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let server = TestServer::start("insert_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_multi_tbl (id INT, value INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_multi_tbl (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, value FROM insert_multi_tbl ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (2, 20));
    assert_eq!(rows[2], (3, 30));

    conn.query_drop("DROP TABLE insert_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_named_columns() {
    let server = TestServer::start("insert_named").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_named_tbl (a INT, b INT, c INT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert only some columns in different order
    conn.query_drop("INSERT INTO insert_named_tbl (c, a) VALUES (30, 10)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, Option<i32>, i32)> = conn
        .query("SELECT a, b, c FROM insert_named_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);
    assert!(rows[0].1.is_none()); // b should be NULL
    assert_eq!(rows[0].2, 30);

    conn.query_drop("DROP TABLE insert_named_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_null_values() {
    let server = TestServer::start("insert_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_null_tbl (id, name) VALUES (1, NULL)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, Option<String>)> = conn
        .query("SELECT id, name FROM insert_null_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert!(rows[0].1.is_none());

    conn.query_drop("DROP TABLE insert_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_negative_numbers() {
    let server = TestServer::start("insert_negative").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_neg_tbl (val INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_neg_tbl (val) VALUES (-42)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT val FROM insert_neg_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, -42);

    conn.query_drop("DROP TABLE insert_neg_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_empty_string() {
    let server = TestServer::start("insert_empty_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_empty_tbl (name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_empty_tbl (name) VALUES ('')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT name FROM insert_empty_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "");

    conn.query_drop("DROP TABLE insert_empty_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_insert_boolean_values() {
    let server = TestServer::start("insert_bool").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE insert_bool_tbl (active BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO insert_bool_tbl (active) VALUES (true)")
        .await
        .expect("INSERT true failed");
    conn.query_drop("INSERT INTO insert_bool_tbl (active) VALUES (false)")
        .await
        .expect("INSERT false failed");

    let rows: Vec<(bool,)> = conn
        .query("SELECT active FROM insert_bool_tbl ORDER BY active")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!(!rows[0].0); // false first
    assert!(rows[1].0); // true second

    conn.query_drop("DROP TABLE insert_bool_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

/// Regression test: INSERT INTO t2 (i) SELECT i FROM t1 with partial column list
/// must map the selected value to column `i`, not positionally to the first column.
#[tokio::test]
async fn test_insert_select_partial_column_list() {
    let server = TestServer::start("ins_sel_partial").await;
    let mut conn = server.connect().await;

    // Source table
    conn.query_drop("CREATE TABLE t1_isp (i INT)")
        .await
        .expect("CREATE TABLE t1 failed");
    conn.query_drop("INSERT INTO t1_isp (i) VALUES (42), (99)")
        .await
        .expect("INSERT INTO t1 failed");

    // Target table has two columns (a, i); we only insert into column i
    conn.query_drop("CREATE TABLE t2_isp (a INT, i INT)")
        .await
        .expect("CREATE TABLE t2 failed");

    conn.query_drop("INSERT INTO t2_isp (i) SELECT i FROM t1_isp")
        .await
        .expect("INSERT...SELECT with partial columns failed");

    // Verify: column `a` should be NULL, column `i` should have the selected values
    let rows: Vec<(Option<i32>, i32)> = conn
        .query("SELECT a, i FROM t2_isp ORDER BY i")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!(
        rows[0].0.is_none(),
        "column a should be NULL, got {:?}",
        rows[0].0
    );
    assert_eq!(rows[0].1, 42);
    assert!(
        rows[1].0.is_none(),
        "column a should be NULL, got {:?}",
        rows[1].0
    );
    assert_eq!(rows[1].1, 99);

    conn.query_drop("DROP TABLE t1_isp, t2_isp")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

/// Regression test: INSERT IGNORE INTO t1 SELECT * FROM t2
/// must silently skip rows that would cause a duplicate key error.
#[tokio::test]
async fn test_insert_ignore_select_duplicate_key() {
    let server = TestServer::start("ins_ign_sel_dup").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE t1_iisd (a INT PRIMARY KEY)")
        .await
        .expect("CREATE TABLE t1 failed");
    conn.query_drop("INSERT INTO t1_iisd VALUES (1), (2)")
        .await
        .expect("INSERT INTO t1 failed");

    conn.query_drop("CREATE TABLE t2_iisd (a INT PRIMARY KEY)")
        .await
        .expect("CREATE TABLE t2 failed");
    conn.query_drop("INSERT INTO t2_iisd VALUES (2), (3)")
        .await
        .expect("INSERT INTO t2 failed");

    // This should skip the duplicate (2) and insert (3)
    conn.query_drop("INSERT IGNORE INTO t1_iisd SELECT * FROM t2_iisd")
        .await
        .expect("INSERT IGNORE ... SELECT should not error on duplicate key");

    let rows: Vec<(i32,)> = conn
        .query("SELECT a FROM t1_iisd ORDER BY a")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3, "expected 3 rows: 1, 2, 3");
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[2].0, 3);

    conn.query_drop("DROP TABLE t1_iisd, t2_iisd")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

/// Regression test: INSERT with partial column list must apply DEFAULT values
/// for unspecified columns, not leave them as NULL.
/// Bug: INSERT INTO t (c2) VALUES (NULL) with c1 DEFAULT 'y' left c1 as NULL.
#[tokio::test]
async fn test_insert_partial_columns_default_values() {
    let server = TestServer::start("ins_partial_def").await;
    let mut conn = server.connect().await;

    // Create table where c1 has a DEFAULT value
    conn.query_drop(
        "CREATE TABLE t_ipd (pk INT NOT NULL PRIMARY KEY, c1 VARCHAR(10) DEFAULT 'y', c2 INT)",
    )
    .await
    .expect("CREATE TABLE failed");

    // Insert specifying only c2 and pk — c1 should get DEFAULT 'y'
    conn.query_drop("INSERT INTO t_ipd (pk, c2) VALUES (1, NULL)")
        .await
        .expect("INSERT with partial columns failed");

    // Verify c1 got DEFAULT 'y', not NULL
    let rows: Vec<(i32, Option<String>, Option<i32>)> = conn
        .query("SELECT pk, c1, c2 FROM t_ipd WHERE pk = 1")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(
        rows[0].1,
        Some("y".to_string()),
        "c1 should be DEFAULT 'y', not NULL"
    );
    assert_eq!(rows[0].2, None, "c2 should be NULL");

    // Also test with INSERT ... SELECT
    conn.query_drop("CREATE TABLE t_ipd_src (pk INT NOT NULL PRIMARY KEY, val INT)")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO t_ipd_src VALUES (10, 42)")
        .await
        .expect("INSERT failed");

    conn.query_drop("INSERT INTO t_ipd (pk, c2) SELECT pk, val FROM t_ipd_src")
        .await
        .expect("INSERT...SELECT with partial columns failed");

    let rows: Vec<(i32, Option<String>, Option<i32>)> = conn
        .query("SELECT pk, c1, c2 FROM t_ipd WHERE pk = 10")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);
    assert_eq!(
        rows[0].1,
        Some("y".to_string()),
        "c1 should be DEFAULT 'y' via INSERT...SELECT"
    );
    assert_eq!(rows[0].2, Some(42));

    conn.query_drop("DROP TABLE t_ipd, t_ipd_src")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
