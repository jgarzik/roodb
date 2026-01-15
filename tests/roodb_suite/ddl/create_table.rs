//! CREATE TABLE integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_create_table_basic() {
    let server = TestServer::start("create_basic").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE basic_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Verify table exists by inserting and selecting
    conn.query_drop("INSERT INTO basic_tbl (id) VALUES (1)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM basic_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP TABLE basic_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_multi_column() {
    let server = TestServer::start("create_multi").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE multi_tbl (id INT, name VARCHAR(100), active BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO multi_tbl (id, name, active) VALUES (1, 'test', true)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String, bool)> = conn
        .query("SELECT id, name, active FROM multi_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "test".to_string(), true));

    conn.query_drop("DROP TABLE multi_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_all_int_types() {
    let server = TestServer::start("create_int_types").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE int_types (
            t TINYINT,
            s SMALLINT,
            i INT,
            b BIGINT
        )",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO int_types (t, s, i, b) VALUES (127, 32767, 2147483647, 9223372036854775807)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i8, i16, i32, i64)> = conn
        .query("SELECT t, s, i, b FROM int_types")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (127, 32767, 2147483647, 9223372036854775807i64));

    conn.query_drop("DROP TABLE int_types")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_float_types() {
    let server = TestServer::start("create_float_types").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE float_types (
            f FLOAT,
            d DOUBLE
        )",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO float_types (f, d) VALUES (1.5, 123.456789)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(f32, f64)> = conn
        .query("SELECT f, d FROM float_types")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert!((rows[0].0 - 1.5).abs() < 0.01);
    assert!((rows[0].1 - 123.456789).abs() < 0.0001);

    conn.query_drop("DROP TABLE float_types")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_string_types() {
    let server = TestServer::start("create_string_types").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE string_types (
            v VARCHAR(255),
            t TEXT
        )",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO string_types (v, t) VALUES ('varchar val', 'text val')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String, String)> = conn
        .query("SELECT v, t FROM string_types")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], ("varchar val".to_string(), "text val".to_string()));

    conn.query_drop("DROP TABLE string_types")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_not_null() {
    let server = TestServer::start("create_not_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE notnull_tbl (id INT NOT NULL, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    // Insert with NOT NULL column
    conn.query_drop("INSERT INTO notnull_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM notnull_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP TABLE notnull_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_primary_key() {
    let server = TestServer::start("create_pk").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE pk_tbl (id INT PRIMARY KEY, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO pk_tbl (id, name) VALUES (1, 'test')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, name FROM pk_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP TABLE pk_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let server = TestServer::start("create_if_not_exists").await;
    let mut conn = server.connect().await;

    // Create table first time
    conn.query_drop("CREATE TABLE ine_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Create again with IF NOT EXISTS - should not error
    conn.query_drop("CREATE TABLE IF NOT EXISTS ine_tbl (id INT)")
        .await
        .expect("CREATE TABLE IF NOT EXISTS failed");

    // Verify table still works
    conn.query_drop("INSERT INTO ine_tbl (id) VALUES (1)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM ine_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    conn.query_drop("DROP TABLE ine_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
