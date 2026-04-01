//! Hex literal arithmetic and bitwise operation tests.
//!
//! MySQL treats hex literals (0xNN) as unsigned integers in numeric context.
//! These tests verify that RooDB correctly performs implicit Bytes->UnsignedInt
//! coercion for arithmetic and bitwise operations.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_hex_literal_addition() {
    let server = TestServer::start("hex_arith_add").await;
    let mut conn = server.connect().await;

    // 0x41 + 0 = 65 (ASCII 'A' as unsigned int)
    let rows: Vec<(u64,)> = conn
        .query("SELECT 0x41 + 0")
        .await
        .expect("hex addition failed");
    assert_eq!(rows[0].0, 65);

    // 0xFF + 1 = 256
    let rows: Vec<(u64,)> = conn
        .query("SELECT 0xFF + 1")
        .await
        .expect("hex addition failed");
    assert_eq!(rows[0].0, 256);

    // 0x0100 + 0 = 256 (two-byte hex)
    let rows: Vec<(u64,)> = conn
        .query("SELECT 0x0100 + 0")
        .await
        .expect("hex addition failed");
    assert_eq!(rows[0].0, 256);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_hex_literal_bitwise_or() {
    let server = TestServer::start("hex_arith_or").await;
    let mut conn = server.connect().await;

    // 0x41 | 0x0F = 0x4F = 79
    let rows: Vec<(i64,)> = conn
        .query("SELECT 0x41 | 0x0F")
        .await
        .expect("hex bitwise OR failed");
    assert_eq!(rows[0].0, 79);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_hex_literal_bitwise_and() {
    let server = TestServer::start("hex_arith_and").await;
    let mut conn = server.connect().await;

    // 0xFF & 0x0F = 0x0F = 15
    let rows: Vec<(i64,)> = conn
        .query("SELECT 0xFF & 0x0F")
        .await
        .expect("hex bitwise AND failed");
    assert_eq!(rows[0].0, 15);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_hex_literal_shift() {
    let server = TestServer::start("hex_arith_shl").await;
    let mut conn = server.connect().await;

    // 0xFF << 8 = 65280
    let rows: Vec<(u64,)> = conn
        .query("SELECT 0xFF << 8")
        .await
        .expect("hex shift left failed");
    assert_eq!(rows[0].0, 65280);

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_hex_literal_multiplication() {
    let server = TestServer::start("hex_arith_mul").await;
    let mut conn = server.connect().await;

    // 0x0A * 3 = 30
    let rows: Vec<(u64,)> = conn
        .query("SELECT 0x0A * 3")
        .await
        .expect("hex multiplication failed");
    assert_eq!(rows[0].0, 30);

    drop(conn);
    server.shutdown().await;
}
