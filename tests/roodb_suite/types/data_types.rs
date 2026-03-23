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
async fn test_bit_type() {
    let server = TestServer::start("type_bit").await;
    let mut conn = server.connect().await;

    // BIT(1) — basic boolean-like usage
    conn.query_drop("CREATE TABLE type_bit_tbl (val BIT(1))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_bit_tbl (val) VALUES (1), (0)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT val FROM type_bit_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);

    conn.query_drop("DROP TABLE type_bit_tbl")
        .await
        .expect("DROP TABLE failed");

    // BIT(8) — byte-width
    conn.query_drop("CREATE TABLE type_bit8_tbl (val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_bit8_tbl (val) VALUES (255), (0), (42)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT val FROM type_bit8_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);

    conn.query_drop("DROP TABLE type_bit8_tbl")
        .await
        .expect("DROP TABLE failed");

    // BIT(64) — full width
    conn.query_drop("CREATE TABLE type_bit64_tbl (val BIT(64))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_bit64_tbl (val) VALUES (0), (1), (9999)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT val FROM type_bit64_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);

    conn.query_drop("DROP TABLE type_bit64_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_comparison() {
    let server = TestServer::start("type_bit_cmp").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE bit_cmp (id INT, val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO bit_cmp (id, val) VALUES (1, 10), (2, 20), (3, 5), (4, 255)")
        .await
        .expect("INSERT failed");

    // ORDER BY on BIT column
    let rows: Vec<(i32, Vec<u8>)> = conn
        .query("SELECT id, val FROM bit_cmp ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 4);
    // Should be ordered: 5, 10, 20, 255
    assert_eq!(rows[0].0, 3); // id=3, val=5
    assert_eq!(rows[3].0, 4); // id=4, val=255

    // WHERE bit_col = integer
    let rows: Vec<(i32,)> = conn
        .query("SELECT id FROM bit_cmp WHERE val = 20")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);

    conn.query_drop("DROP TABLE bit_cmp")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_operators() {
    let server = TestServer::start("type_bit_ops").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE bit_ops (a BIT(8), b BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    // 5 = 0b00000101, 3 = 0b00000011
    conn.query_drop("INSERT INTO bit_ops (a, b) VALUES (5, 3)")
        .await
        .expect("INSERT failed");

    // a & b = 1, a | b = 7, a ^ b = 6
    let rows: Vec<(i64, i64, i64)> = conn
        .query("SELECT a & b, a | b, a ^ b FROM bit_ops")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1); // AND
    assert_eq!(rows[0].1, 7); // OR
    assert_eq!(rows[0].2, 6); // XOR

    conn.query_drop("DROP TABLE bit_ops")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_count_function() {
    let server = TestServer::start("type_bit_cnt").await;
    let mut conn = server.connect().await;

    // BIT_COUNT on integer literals
    let rows: Vec<(i64,)> = conn
        .query("SELECT BIT_COUNT(7)")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 3); // 7 = 0b111 → 3 bits

    let rows: Vec<(i64,)> = conn
        .query("SELECT BIT_COUNT(0)")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 0);

    let rows: Vec<(i64,)> = conn
        .query("SELECT BIT_COUNT(255)")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 8); // 255 = 0xFF → 8 bits

    // BIT_COUNT on BIT column
    conn.query_drop("CREATE TABLE bit_cnt (val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO bit_cnt (val) VALUES (15), (1), (0)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i64,)> = conn
        .query("SELECT BIT_COUNT(val) FROM bit_cnt ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 0); // BIT_COUNT(0)
    assert_eq!(rows[1].0, 1); // BIT_COUNT(1)
    assert_eq!(rows[2].0, 4); // BIT_COUNT(15)

    conn.query_drop("DROP TABLE bit_cnt")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_aggregate_functions() {
    let server = TestServer::start("type_bit_agg").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE bit_agg (grp INT, val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    // Group A: 0b1100 (12), 0b1010 (10) → AND=8, OR=14, XOR=6
    // Group B: 0b1111 (15), 0b0001 (1)  → AND=1, OR=15, XOR=14
    conn.query_drop("INSERT INTO bit_agg (grp, val) VALUES (1, 12), (1, 10), (2, 15), (2, 1)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(i32, i64, i64, i64)> = conn
        .query("SELECT grp, BIT_AND(val), BIT_OR(val), BIT_XOR(val) FROM bit_agg GROUP BY grp ORDER BY grp")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);

    // Group 1: AND(12,10)=8, OR(12,10)=14, XOR(12,10)=6
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, 8);
    assert_eq!(rows[0].2, 14);
    assert_eq!(rows[0].3, 6);

    // Group 2: AND(15,1)=1, OR(15,1)=15, XOR(15,1)=14
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, 1);
    assert_eq!(rows[1].2, 15);
    assert_eq!(rows[1].3, 14);

    conn.query_drop("DROP TABLE bit_agg")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_cast() {
    let server = TestServer::start("type_bit_cast").await;
    let mut conn = server.connect().await;

    // CAST integer to BIT
    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT CAST(5 AS BIT(8))")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    // Value 5 as BIT(8) should be 1 byte: 0x05
    assert_eq!(rows[0].0, vec![5u8]);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_null() {
    let server = TestServer::start("type_bit_null").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE bit_null (val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO bit_null (val) VALUES (42), (NULL), (7)")
        .await
        .expect("INSERT failed");

    // NULL should be preserved
    let rows: Vec<(Option<Vec<u8>>,)> = conn
        .query("SELECT val FROM bit_null ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    // NULL sorts first
    assert!(rows[0].0.is_none());

    // BIT_COUNT(NULL) = NULL
    let rows: Vec<(Option<i64>,)> = conn
        .query("SELECT BIT_COUNT(NULL)")
        .await
        .expect("SELECT failed");
    assert!(rows[0].0.is_none());

    conn.query_drop("DROP TABLE bit_null")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bit_arithmetic() {
    let server = TestServer::start("type_bit_arith").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE bit_arith (val BIT(8))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO bit_arith (val) VALUES (10)")
        .await
        .expect("INSERT failed");

    // bit_col + 0 promotes to integer
    let rows: Vec<(i64,)> = conn
        .query("SELECT val + 0 FROM bit_arith")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 10);

    // bit_col + 5
    let rows: Vec<(i64,)> = conn
        .query("SELECT val + 5 FROM bit_arith")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 15);

    // bit_col * 2
    let rows: Vec<(i64,)> = conn
        .query("SELECT val * 2 FROM bit_arith")
        .await
        .expect("SELECT failed");
    assert_eq!(rows[0].0, 20);

    conn.query_drop("DROP TABLE bit_arith")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_character_varying_type() {
    let server = TestServer::start("type_charvar").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_charvar_tbl (val CHARACTER VARYING(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_charvar_tbl (val) VALUES ('hello'), ('world')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_charvar_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "hello");
    assert_eq!(rows[1].0, "world");

    conn.query_drop("DROP TABLE type_charvar_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_nvarchar_type() {
    let server = TestServer::start("type_nvarchar").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_nvarchar_tbl (val NVARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_nvarchar_tbl (val) VALUES ('unicode test')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_nvarchar_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "unicode test");

    conn.query_drop("DROP TABLE type_nvarchar_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_bool_alias_type() {
    let server = TestServer::start("type_bool_alias").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_bool_alias_tbl (val BOOL)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_bool_alias_tbl (val) VALUES (true), (false)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(bool,)> = conn
        .query("SELECT val FROM type_bool_alias_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, false);
    assert_eq!(rows[1].0, true);

    conn.query_drop("DROP TABLE type_bool_alias_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_tinytext_type() {
    let server = TestServer::start("type_tinytext").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_tinytext_tbl (val TINYTEXT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_tinytext_tbl (val) VALUES ('small text')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_tinytext_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "small text");

    conn.query_drop("DROP TABLE type_tinytext_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_mediumblob_type() {
    let server = TestServer::start("type_mediumblob").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_mediumblob_tbl (val MEDIUMBLOB)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_mediumblob_tbl (val) VALUES (X'DEADBEEF')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(Vec<u8>,)> = conn
        .query("SELECT val FROM type_mediumblob_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, vec![0xDE, 0xAD, 0xBE, 0xEF]);

    conn.query_drop("DROP TABLE type_mediumblob_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_clob_type() {
    let server = TestServer::start("type_clob").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_clob_tbl (val CLOB)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_clob_tbl (val) VALUES ('large text object')")
        .await
        .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM type_clob_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "large text object");

    conn.query_drop("DROP TABLE type_clob_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_double_unsigned_type() {
    let server = TestServer::start("type_dbl_uns").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_dbl_uns_tbl (val DOUBLE UNSIGNED)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_dbl_uns_tbl (val) VALUES (3.14), (2.71)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(f64,)> = conn
        .query("SELECT val FROM type_dbl_uns_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!((rows[0].0 - 2.71).abs() < 1e-10);
    assert!((rows[1].0 - 3.14).abs() < 1e-10);

    conn.query_drop("DROP TABLE type_dbl_uns_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_decimal_unsigned_type() {
    let server = TestServer::start("type_dec_uns").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE type_dec_uns_tbl (val DECIMAL(10,2) UNSIGNED)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO type_dec_uns_tbl (val) VALUES (99.99), (0.01)")
        .await
        .expect("INSERT failed");

    let rows: Vec<(f64,)> = conn
        .query("SELECT val FROM type_dec_uns_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 2);
    assert!((rows[0].0 - 0.01).abs() < 0.001);
    assert!((rows[1].0 - 99.99).abs() < 0.001);

    conn.query_drop("DROP TABLE type_dec_uns_tbl")
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
