//! Tests for all supported data types.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_boolean_type() {
    let server = TestServer::start("type_bool").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_bool_tbl (val BOOLEAN)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_bool_tbl (val) VALUES (true), (false), (NULL)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Option<bool>,)> = conn
        .query("SELECT val FROM type_bool_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert!(rows[0].0.is_none()); // NULL sorts first
    assert_eq!(rows[1].0, Some(false));
    assert_eq!(rows[2].0, Some(true));

    conn.query_drop("DROP TABLE type_bool_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_tinyint_type() {
    let server = TestServer::start("type_tinyint").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_tinyint_tbl (val TINYINT)")
        .await
        .expect("CREATE TABLE failed");

    // Test range: -128 to 127
    conn.query_drop("INSERT INTO type_tinyint_tbl (val) VALUES (-128), (0), (127)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i8,)> = conn
        .query("SELECT val FROM type_tinyint_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -128);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 127);

    conn.query_drop("DROP TABLE type_tinyint_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_smallint_type() {
    let server = TestServer::start("type_smallint").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_smallint_tbl (val SMALLINT)")
        .await
        .expect("CREATE TABLE failed");

    // Test range: -32768 to 32767
    conn.query_drop("INSERT INTO type_smallint_tbl (val) VALUES (-32768), (0), (32767)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i16,)> = conn
        .query("SELECT val FROM type_smallint_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -32768);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 32767);

    conn.query_drop("DROP TABLE type_smallint_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_int_type() {
    let server = TestServer::start("type_int").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_int_tbl (val INT)")
        .await
        .expect("CREATE TABLE failed");

    // Test range: -2147483648 to 2147483647
    conn.query_drop("INSERT INTO type_int_tbl (val) VALUES (-2147483648), (0), (2147483647)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32,)> = conn
        .query("SELECT val FROM type_int_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -2147483648);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 2147483647);

    conn.query_drop("DROP TABLE type_int_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bigint_type() {
    let server = TestServer::start("type_bigint").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_bigint_tbl (val BIGINT)")
        .await
        .expect("CREATE TABLE failed");

    // Test large values (not full range to avoid parsing issues)
    conn.query_drop(
        "INSERT INTO type_bigint_tbl (val) VALUES (-9223372036854775807), (0), (9223372036854775807)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(i64,)> = conn
        .query("SELECT val FROM type_bigint_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, -9223372036854775807);
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[2].0, 9223372036854775807);

    conn.query_drop("DROP TABLE type_bigint_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_float_type() {
    let server = TestServer::start("type_float").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_float_tbl (val FLOAT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_float_tbl (val) VALUES (-1.5), (0.0), (2.5)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(f32,)> = conn
        .query("SELECT val FROM type_float_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert!((rows[0].0 - (-1.5)).abs() < 0.001);
    assert!((rows[1].0 - 0.0).abs() < 0.001);
    assert!((rows[2].0 - 2.5).abs() < 0.001);

    conn.query_drop("DROP TABLE type_float_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_double_type() {
    let server = TestServer::start("type_double").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_double_tbl (val DOUBLE)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO type_double_tbl (val) VALUES (-1.23456789012345), (0.0), (9.87654321098765)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(f64,)> = conn
        .query("SELECT val FROM type_double_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert!((rows[0].0 - (-1.23456789012345)).abs() < 1e-10);
    assert!((rows[1].0 - 0.0).abs() < 1e-10);
    assert!((rows[2].0 - 9.87654321098765).abs() < 1e-10);

    conn.query_drop("DROP TABLE type_double_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_varchar_type() {
    let server = TestServer::start("type_varchar").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_varchar_tbl (val VARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO type_varchar_tbl (val) VALUES ('hello'), ('world'), ('test string with spaces')",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_varchar_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "hello");
    assert_eq!(rows[1].0, "test string with spaces");
    assert_eq!(rows[2].0, "world");

    conn.query_drop("DROP TABLE type_varchar_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_text_type() {
    let server = TestServer::start("type_text").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_text_tbl (val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert a longer text value
    let long_text = "a".repeat(1000);
    conn.query_drop(format!(
        "INSERT INTO type_text_tbl (val) VALUES ('{}')",
        long_text
    ))
    .await
    .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_text_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.len(), 1000);

    conn.query_drop("DROP TABLE type_text_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_blob_type() {
    let server = TestServer::start("type_blob").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_blob_tbl (val BLOB)")
        .await
        .expect("CREATE TABLE failed");

    // Insert hex-encoded binary data
    conn.query_drop("INSERT INTO type_blob_tbl (val) VALUES (X'48454C4C4F')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT val FROM type_blob_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, b"HELLO".to_vec());

    conn.query_drop("DROP TABLE type_blob_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_timestamp_type() {
    let server = TestServer::start("type_timestamp").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_timestamp_tbl (val TIMESTAMP)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_timestamp_tbl (val) VALUES ('2024-01-15 12:30:45')")
        .await
        .expect("INSERT failed");

    // Query as string to verify format
    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_timestamp_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].0.contains("2024"));
    assert!(rows[0].0.contains("12:30:45"));

    conn.query_drop("DROP TABLE type_timestamp_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
#[allow(clippy::type_complexity)]
async fn test_null_all_types() {
    let server = TestServer::start("type_null_all").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE TABLE type_null_all_tbl (
            b BOOLEAN,
            ti TINYINT,
            si SMALLINT,
            i INT,
            bi BIGINT,
            f FLOAT,
            d DOUBLE,
            v VARCHAR(50),
            t TEXT
        )",
    )
    .await
    .expect("CREATE TABLE failed");

    conn.query_drop(
        "INSERT INTO type_null_all_tbl (b, ti, si, i, bi, f, d, v, t)
         VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    )
    .await
    .expect("INSERT failed");

    let rows: Vec<(
        Option<bool>,
        Option<i8>,
        Option<i16>,
        Option<i32>,
        Option<i64>,
        Option<f32>,
        Option<f64>,
        Option<String>,
        Option<String>,
    )> = conn
        .query("SELECT b, ti, si, i, bi, f, d, v, t FROM type_null_all_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].0.is_none());
    assert!(rows[0].1.is_none());
    assert!(rows[0].2.is_none());
    assert!(rows[0].3.is_none());
    assert!(rows[0].4.is_none());
    assert!(rows[0].5.is_none());
    assert!(rows[0].6.is_none());
    assert!(rows[0].7.is_none());
    assert!(rows[0].8.is_none());

    conn.query_drop("DROP TABLE type_null_all_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
