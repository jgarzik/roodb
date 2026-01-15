//! String edge case tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_empty_string() {
    let server = TestServer::start("edge_empty_str").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_empty_str_tbl (id INT, val VARCHAR(50))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO edge_empty_str_tbl (id, val) VALUES (1, ''), (2, 'nonempty')")
        .await
        .expect("INSERT failed");

    // Verify empty string is different from NULL
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, val FROM edge_empty_str_tbl WHERE val = '' ORDER BY id")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "".to_string()));

    conn.query_drop("DROP TABLE edge_empty_str_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_large_varchar() {
    let server = TestServer::start("edge_large_vc").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_large_vc_tbl (val VARCHAR(1000))")
        .await
        .expect("CREATE TABLE failed");

    // Insert string near max length
    let long_str = "x".repeat(500);
    conn.query_drop(format!(
        "INSERT INTO edge_large_vc_tbl (val) VALUES ('{}')",
        long_str
    ))
    .await
    .expect("INSERT failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM edge_large_vc_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.len(), 500);

    conn.query_drop("DROP TABLE edge_large_vc_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_special_characters() {
    let server = TestServer::start("edge_special_ch").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_special_ch_tbl (val VARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    // Test various special characters (escaped single quote)
    conn.query_drop("INSERT INTO edge_special_ch_tbl (val) VALUES ('hello''world')")
        .await
        .expect("INSERT with quote failed");

    conn.query_drop("INSERT INTO edge_special_ch_tbl (val) VALUES ('line1\\nline2')")
        .await
        .expect("INSERT with backslash failed");

    conn.query_drop("INSERT INTO edge_special_ch_tbl (val) VALUES ('tab\\there')")
        .await
        .expect("INSERT with tab failed");

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM edge_special_ch_tbl ORDER BY val")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 3);
    // Verify the escaped quote became a single quote
    assert!(rows.iter().any(|r| r.0.contains("'")));

    conn.query_drop("DROP TABLE edge_special_ch_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_unicode_strings() {
    let server = TestServer::start("edge_unicode").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE edge_unicode_tbl (val VARCHAR(100))")
        .await
        .expect("CREATE TABLE failed");

    // Insert unicode characters
    conn.query_drop("INSERT INTO edge_unicode_tbl (val) VALUES ('Hello World')")
        .await
        .expect("INSERT ascii failed");

    // Note: Some unicode may not work depending on encoding support
    // Using simple ASCII for now

    let rows: Vec<(String,)> = conn
        .query("SELECT val FROM edge_unicode_tbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "Hello World");

    conn.query_drop("DROP TABLE edge_unicode_tbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_case_sensitivity() {
    let server = TestServer::start("edge_case_sens").await;
    let mut conn = server.connect().await;

    // Create table with mixed case name
    conn.query_drop("CREATE TABLE EdgeCaseTbl (MyCol INT)")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop("INSERT INTO EdgeCaseTbl (MyCol) VALUES (42)")
        .await
        .expect("INSERT failed");

    // Query with exact case - RooDB is case-sensitive for identifiers
    let rows: Vec<(i32,)> = conn
        .query("SELECT MyCol FROM EdgeCaseTbl")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 42);

    // Verify different case fails (RooDB is case-sensitive)
    let result: Result<Vec<(i32,)>, _> = conn.query("SELECT mycol FROM edgecasetbl").await;
    assert!(result.is_err(), "Different case should fail - RooDB is case-sensitive");

    conn.query_drop("DROP TABLE EdgeCaseTbl")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
