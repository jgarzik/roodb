//! RooDB integration test suite entry point.
//!
//! Run all integration tests: `cargo test --release suite`
//! Run specific category: `cargo test --release suite::ddl`

mod roodb_suite;
mod test_utils;

use mysql_async::prelude::*;
use roodb_suite::TestServer;

/// Smoke test: verify harness can start server, connect, and run basic SQL.
#[tokio::test]
async fn test_harness_smoke() {
    let server = TestServer::start("harness_smoke").await;
    let mut conn = server.connect().await;

    // Create a test table
    conn.query_drop("CREATE TABLE smoke_test (id INT)")
        .await
        .expect("CREATE TABLE failed");

    // Insert and query
    conn.query_drop("INSERT INTO smoke_test (id) VALUES (42)")
        .await
        .expect("INSERT failed");

    let result: Vec<(i32,)> = conn
        .query("SELECT id FROM smoke_test")
        .await
        .expect("SELECT failed");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 42);

    // Cleanup
    conn.query_drop("DROP TABLE smoke_test")
        .await
        .expect("DROP TABLE failed");

    drop(conn);
    server.shutdown().await;
}
