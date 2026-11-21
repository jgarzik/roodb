//! Prepared statements must behave like text statements when things go
//! wrong, and must run the same side effects.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

/// A failing prepared statement reports an error and leaves the connection
/// usable.
#[tokio::test]
async fn test_prepared_error_keeps_connection() {
    let server = TestServer::start("prep_err_conn").await;
    let mut conn = server.connect().await;

    conn.query_drop("DROP TABLE IF EXISTS prep_err_tbl")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE prep_err_tbl (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.exec_drop("INSERT INTO prep_err_tbl (id, v) VALUES (?, ?)", (1, 10))
        .await
        .expect("first prepared insert");

    let dup = conn
        .exec_drop("INSERT INTO prep_err_tbl (id, v) VALUES (?, ?)", (1, 20))
        .await;
    let err = dup.expect_err("duplicate key must fail");
    assert!(
        matches!(err, mysql_async::Error::Server(_)),
        "expected a server error packet, got: {err}"
    );

    // Same connection, still alive, data unchanged.
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT id, v FROM prep_err_tbl WHERE id = ?", (1,))
        .await
        .expect("connection must survive a failed prepared statement");
    assert_eq!(rows, vec![(1, 10)]);

    conn.query_drop("DROP TABLE prep_err_tbl").await.unwrap();
    drop(conn);
    server.shutdown().await;
}

/// An INSERT trigger fires for a prepared INSERT exactly as for a text one.
#[tokio::test]
async fn test_prepared_insert_fires_trigger() {
    let server = TestServer::start("prep_trigger").await;
    let mut conn = server.connect().await;

    for stmt in [
        "DROP TRIGGER IF EXISTS prep_trg",
        "DROP TABLE IF EXISTS prep_trg_src",
        "DROP TABLE IF EXISTS prep_trg_log",
        "CREATE TABLE prep_trg_src (id INT PRIMARY KEY)",
        "CREATE TABLE prep_trg_log (id INT)",
        "CREATE TRIGGER prep_trg AFTER INSERT ON prep_trg_src FOR EACH ROW \
         INSERT INTO prep_trg_log VALUES (NEW.id)",
    ] {
        conn.query_drop(stmt)
            .await
            .unwrap_or_else(|e| panic!("setup `{stmt}` failed: {e}"));
    }

    conn.query_drop("INSERT INTO prep_trg_src VALUES (1)")
        .await
        .expect("text insert");
    conn.exec_drop("INSERT INTO prep_trg_src VALUES (?)", (2,))
        .await
        .expect("prepared insert");

    let mut logged: Vec<i32> = conn
        .query("SELECT id FROM prep_trg_log")
        .await
        .expect("read trigger log");
    logged.sort_unstable();
    assert_eq!(logged, vec![1, 2], "trigger must fire for both inserts");

    for stmt in [
        "DROP TRIGGER IF EXISTS prep_trg",
        "DROP TABLE prep_trg_src",
        "DROP TABLE prep_trg_log",
    ] {
        conn.query_drop(stmt).await.unwrap();
    }
    drop(conn);
    server.shutdown().await;
}

/// A prepared SET must always be answered; it used to leave the client
/// waiting forever for user variables and sql_mode.
#[tokio::test]
async fn test_prepared_set_is_answered() {
    use std::time::Duration;

    let server = TestServer::start("prep_set").await;
    let mut conn = server.connect().await;

    tokio::time::timeout(
        Duration::from_secs(10),
        conn.exec_drop("SET @prep_set_a = 5", ()),
    )
    .await
    .expect("prepared SET @var must get a response")
    .expect("prepared SET @var without parameters must succeed");

    let value: Option<i64> = conn
        .query_first("SELECT @prep_set_a")
        .await
        .expect("read user variable");
    assert_eq!(value, Some(5));

    // With a bound parameter: either works or is refused, but is answered.
    let answered = tokio::time::timeout(
        Duration::from_secs(10),
        conn.exec_drop("SET @prep_set_b = ?", (7,)),
    )
    .await
    .expect("prepared SET @var = ? must get a response");
    if let Err(e) = answered {
        assert!(
            matches!(e, mysql_async::Error::Server(_)),
            "expected a server error packet, got: {e}"
        );
    }

    // Connection still in sync.
    let one: Option<i64> = conn.query_first("SELECT 1").await.expect("still usable");
    assert_eq!(one, Some(1));

    drop(conn);
    server.shutdown().await;
}

/// COM_RESET_CONNECTION clears prepared statements but keeps the selected
/// database.
#[tokio::test]
async fn test_reset_connection_keeps_database() {
    let server = TestServer::start("prep_reset").await;
    let mut conn = server.connect().await;

    conn.query_drop("USE test").await.expect("USE test");
    let before: Option<String> = conn.query_first("SELECT DATABASE()").await.unwrap();
    assert_eq!(before.as_deref(), Some("test"));

    conn.reset().await.expect("COM_RESET_CONNECTION");

    let after: Option<String> = conn.query_first("SELECT DATABASE()").await.unwrap();
    assert_eq!(after.as_deref(), Some("test"));

    drop(conn);
    server.shutdown().await;
}
