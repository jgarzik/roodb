//! SQL syntax error tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_syntax_error_missing_keyword() {
    let server = TestServer::start("err_syntax_kw").await;
    let mut conn = server.connect().await;

    // Missing FROM keyword
    let result: Result<Vec<(i32,)>, _> = conn.query("SELECT id users").await;
    assert!(result.is_err(), "Expected syntax error");

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("syntax") || err_str.contains("parse") || err_str.contains("error"),
        "Error should mention syntax/parse: {}",
        err_str
    );

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_syntax_error_invalid_statement() {
    let server = TestServer::start("err_syntax_inv").await;
    let mut conn = server.connect().await;

    // Completely invalid SQL
    let result: Result<(), _> = conn.query_drop("GIBBERISH NONSENSE WORDS").await;
    assert!(result.is_err(), "Expected syntax error");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_syntax_error_unclosed_string() {
    let server = TestServer::start("err_syntax_str").await;
    let mut conn = server.connect().await;

    // Unclosed string literal
    let result: Result<(), _> = conn
        .query_drop("SELECT 'unclosed string")
        .await;
    assert!(result.is_err(), "Expected syntax error for unclosed string");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_syntax_error_missing_values() {
    let server = TestServer::start("err_syntax_val").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE err_syntax_tbl (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // INSERT without VALUES
    let result: Result<(), _> = conn
        .query_drop("INSERT INTO err_syntax_tbl (id)")
        .await;
    assert!(result.is_err(), "Expected syntax error");

    conn.query_drop("DROP TABLE err_syntax_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
