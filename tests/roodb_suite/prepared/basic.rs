//! Basic prepared statement tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_prepare_select_with_param() {
    let server = TestServer::start("prep_select").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_sel_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO prep_sel_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .exec("SELECT id, name FROM prep_sel_tbl WHERE id = ?", (1,))
        .await
        .expect("prep exec failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE prep_sel_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_insert() {
    let server = TestServer::start("prep_insert").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_ins_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.exec_drop(
        "INSERT INTO prep_ins_tbl (id, name) VALUES (?, ?)",
        (1, "Alice"),
    )
    .await
    .expect("prep insert failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM prep_ins_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string()));

    conn.query_drop("DROP TABLE prep_ins_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_update() {
    let server = TestServer::start("prep_update").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_upd_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO prep_upd_tbl (id, name) VALUES (1, 'Alice')")
        .await
        .expect("INSERT failed");

    conn.exec_drop("UPDATE prep_upd_tbl SET name = ? WHERE id = ?", ("Bob", 1))
        .await
        .expect("prep update failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM prep_upd_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Bob".to_string()));

    conn.query_drop("DROP TABLE prep_upd_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_delete() {
    let server = TestServer::start("prep_delete").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_del_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO prep_del_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("INSERT failed");

    conn.exec_drop("DELETE FROM prep_del_tbl WHERE id = ?", (1,))
        .await
        .expect("prep delete failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM prep_del_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2, "Bob".to_string()));

    conn.query_drop("DROP TABLE prep_del_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_reexecute() {
    let server = TestServer::start("prep_reexec").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_reexec_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop(
        "INSERT INTO prep_reexec_tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await
    .expect("INSERT failed");

    let stmt = conn
        .prep("SELECT name FROM prep_reexec_tbl WHERE id = ?")
        .await
        .expect("prep failed");

    // Execute same statement multiple times with different params
    let rows: Vec<(String,)> = conn.exec(&stmt, (1,)).await.expect("exec 1 failed");
    assert_eq!(rows[0].0, "Alice");

    let rows: Vec<(String,)> = conn.exec(&stmt, (2,)).await.expect("exec 2 failed");
    assert_eq!(rows[0].0, "Bob");

    let rows: Vec<(String,)> = conn.exec(&stmt, (3,)).await.expect("exec 3 failed");
    assert_eq!(rows[0].0, "Charlie");

    conn.close(stmt).await.expect("close stmt failed");

    conn.query_drop("DROP TABLE prep_reexec_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_null_param() {
    let server = TestServer::start("prep_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_null_tbl (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.exec_drop(
        "INSERT INTO prep_null_tbl (id, name) VALUES (?, ?)",
        (1, Option::<String>::None),
    )
    .await
    .expect("prep insert with null failed");

    let rows: Vec<(i32, Option<String>)> = conn
        .query("SELECT id, name FROM prep_null_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert!(rows[0].1.is_none());

    conn.query_drop("DROP TABLE prep_null_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_multiple_params() {
    let server = TestServer::start("prep_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_multi_tbl (a INT, b INT, c VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop(
        "INSERT INTO prep_multi_tbl (a, b, c) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i32, i32, String)> = conn
        .exec(
            "SELECT a, b, c FROM prep_multi_tbl WHERE a >= ? AND b <= ?",
            (2, 30),
        )
        .await
        .expect("prep exec failed");
    assert_eq!(rows.len(), 2);

    conn.query_drop("DROP TABLE prep_multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_no_params() {
    let server = TestServer::start("prep_noparams").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_nop_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");
    conn.query_drop("INSERT INTO prep_nop_tbl (id) VALUES (1)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .exec("SELECT id FROM prep_nop_tbl", ())
        .await
        .expect("prep exec no params failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    conn.query_drop("DROP TABLE prep_nop_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_prepare_types() {
    let server = TestServer::start("prep_types").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE prep_types_tbl (i INT, f DOUBLE, s VARCHAR(100), b BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.exec_drop(
        "INSERT INTO prep_types_tbl (i, f, s, b) VALUES (?, ?, ?, ?)",
        (42, 3.14f64, "hello", true),
    )
    .await
    .expect("prep insert with types failed");

    let rows: Vec<(i32, f64, String, bool)> = conn
        .exec("SELECT i, f, s, b FROM prep_types_tbl WHERE i = ?", (42,))
        .await
        .expect("prep select failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 42);
    assert!((rows[0].1 - 3.14).abs() < 0.001);
    assert_eq!(rows[0].2, "hello");
    assert!(rows[0].3);

    conn.query_drop("DROP TABLE prep_types_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_sysbench_patterns() {
    let server = TestServer::start("prep_sysbench").await;
    let mut conn = server.connect().await;

    // Create sysbench-like table
    conn.query_drop(
        "CREATE TABLE sbtest_prep (id INT NOT NULL, k INT NOT NULL, c VARCHAR(120) NOT NULL, pad VARCHAR(60) NOT NULL, PRIMARY KEY (id))",
    )
    .await
    .expect("CREATE TABLE failed");

    // INSERT with prepared params
    conn.exec_drop(
        "INSERT INTO sbtest_prep (id, k, c, pad) VALUES (?, ?, ?, ?)",
        (1, 100, "test-data", "padding"),
    )
    .await
    .expect("prep INSERT failed");

    conn.exec_drop(
        "INSERT INTO sbtest_prep (id, k, c, pad) VALUES (?, ?, ?, ?)",
        (2, 200, "more-data", "pad2"),
    )
    .await
    .expect("prep INSERT 2 failed");

    // SELECT c FROM t WHERE id = ?
    let rows: Vec<(String,)> = conn
        .exec("SELECT c FROM sbtest_prep WHERE id = ?", (1,))
        .await
        .expect("prep point select failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "test-data");

    // SELECT c FROM t WHERE id BETWEEN ? AND ?
    let rows: Vec<(String,)> = conn
        .exec("SELECT c FROM sbtest_prep WHERE id BETWEEN ? AND ?", (1, 2))
        .await
        .expect("prep range select failed");
    assert_eq!(rows.len(), 2);

    // SELECT SUM(k) FROM t WHERE id BETWEEN ? AND ?
    let rows: Vec<(f64,)> = conn
        .exec(
            "SELECT SUM(k) FROM sbtest_prep WHERE id BETWEEN ? AND ?",
            (1, 2),
        )
        .await
        .expect("prep sum select failed");
    assert!((rows[0].0 - 300.0).abs() < 0.001);

    // UPDATE t SET k=k+1 WHERE id = ?
    conn.exec_drop("UPDATE sbtest_prep SET k=k+1 WHERE id = ?", (1,))
        .await
        .expect("prep update k failed");

    // UPDATE t SET c=? WHERE id = ?
    conn.exec_drop("UPDATE sbtest_prep SET c = ? WHERE id = ?", ("updated", 1))
        .await
        .expect("prep update c failed");

    // Verify updates
    let rows: Vec<(i32, String)> = conn
        .exec("SELECT k, c FROM sbtest_prep WHERE id = ?", (1,))
        .await
        .expect("prep verify failed");
    assert_eq!(rows[0].0, 101);
    assert_eq!(rows[0].1, "updated");

    // DELETE FROM t WHERE id = ?
    conn.exec_drop("DELETE FROM sbtest_prep WHERE id = ?", (2,))
        .await
        .expect("prep delete failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM sbtest_prep")
        .await
        .expect("SELECT after delete failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    conn.query_drop("DROP TABLE sbtest_prep")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
