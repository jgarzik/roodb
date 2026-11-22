//! Metadata locks: a statement keeps the tables it uses as they are until
//! its transaction ends, and a schema change waits for that.
//!
//! Each test has a server of its own, so that the lock counts it reads are
//! its own.

use std::time::{Duration, Instant};

use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::test_utils::process::ServerProcess;

async fn connect(server: &ServerProcess) -> Conn {
    Conn::new(server.opts()).await.unwrap_or_else(|e| {
        panic!("connect failed: {e}\n{}", server.log_tail());
    })
}

async fn status(conn: &mut Conn, name: &str) -> usize {
    let row: Option<(String, String)> = conn
        .query_first(format!("SHOW STATUS LIKE '{name}'"))
        .await
        .unwrap();
    row.expect("status variable").1.parse().unwrap()
}

/// Wait until `n` requests for a metadata lock are waiting. The deadline
/// only keeps a broken server from hanging the test.
async fn until_metadata_waits(conn: &mut Conn, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while status(conn, "Roodb_metadata_lock_waits").await < n {
        assert!(
            Instant::now() < deadline,
            "no metadata lock request is waiting"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn error_code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("not a server error: {other:?}"),
    }
}

/// A schema change waits for a transaction that has used the table, and
/// goes ahead when the transaction ends. A statement outside a transaction
/// holds the table only while it runs.
#[tokio::test]
async fn a_schema_change_waits_for_a_transaction_that_used_the_table() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut a = connect(&server).await;
    let mut b = connect(&server).await;
    let mut watch = connect(&server).await;
    a.query_drop("CREATE TABLE mdl_wait (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO mdl_wait VALUES (1, 1)")
        .await
        .unwrap();

    // Nothing is in the lock table for statements that have ended.
    let _: Vec<(i32, i32)> = a.query("SELECT * FROM mdl_wait").await.unwrap();
    assert_eq!(status(&mut watch, "Roodb_metadata_locks").await, 0);

    // A transaction reads the table, and stays open.
    a.query_drop("BEGIN").await.unwrap();
    let _: Vec<(i32, i32)> = a.query("SELECT * FROM mdl_wait").await.unwrap();

    // A schema change that will not wait long gives up...
    b.query_drop("SET lock_wait_timeout = 1").await.unwrap();
    let err = b
        .query_drop("ALTER TABLE mdl_wait ADD COLUMN c INT")
        .await
        .expect_err("the table was changed under a transaction that uses it");
    assert_eq!(error_code(&err), 1205, "{err}");
    // ...and the table is as it was, for the transaction and everyone.
    let rows: Vec<(i32, i32)> = a.query("SELECT * FROM mdl_wait").await.unwrap();
    assert_eq!(rows, [(1, 1)]);

    // One that waits is granted when the transaction ends.
    b.query_drop("SET lock_wait_timeout = 3600").await.unwrap();
    let alter = tokio::spawn(async move {
        b.query_drop("ALTER TABLE mdl_wait ADD COLUMN c INT DEFAULT 5")
            .await
            .map(|()| b)
    });
    until_metadata_waits(&mut watch, 1).await;
    // The transaction still sees the table as it was.
    let rows: Vec<(i32, i32)> = a.query("SELECT * FROM mdl_wait").await.unwrap();
    assert_eq!(rows, [(1, 1)]);
    a.query_drop("COMMIT").await.unwrap();
    let mut b = alter.await.unwrap().expect("ALTER TABLE");
    let rows: Vec<(i32, i32, i32)> = b.query("SELECT * FROM mdl_wait").await.unwrap();
    assert_eq!(rows, [(1, 1, 5)]);
    let rows: Vec<(i32, i32, i32)> = a.query("SELECT * FROM mdl_wait").await.unwrap();
    assert_eq!(rows, [(1, 1, 5)]);

    // Nothing is left held or waiting.
    assert_eq!(status(&mut watch, "Roodb_metadata_locks").await, 0);
    assert_eq!(status(&mut watch, "Roodb_metadata_lock_waits").await, 0);
}

/// A statement that changes the schema commits the open transaction first,
/// as in MySQL: what the transaction did stays, whatever comes after.
#[tokio::test]
async fn a_schema_change_commits_the_open_transaction() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut a = connect(&server).await;
    a.query_drop("CREATE TABLE mdl_commit (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for (n, ddl) in [
        "CREATE TABLE mdl_commit_other (id INT PRIMARY KEY)",
        "ALTER TABLE mdl_commit_other ADD COLUMN c INT",
        "CREATE INDEX mdl_commit_ix ON mdl_commit_other (c)",
        "CREATE VIEW mdl_commit_view AS SELECT id FROM mdl_commit_other",
        "DROP VIEW mdl_commit_view",
        "DROP TABLE mdl_commit_other",
        "CREATE DATABASE mdl_commit_db",
        "DROP DATABASE mdl_commit_db",
    ]
    .into_iter()
    .enumerate()
    {
        a.query_drop("BEGIN").await.unwrap();
        a.query_drop(format!("INSERT INTO mdl_commit VALUES ({n})"))
            .await
            .unwrap();
        a.query_drop(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
        a.query_drop("ROLLBACK").await.unwrap();
        let kept: Vec<i32> = a
            .query(format!("SELECT id FROM mdl_commit WHERE id = {n}"))
            .await
            .unwrap();
        assert_eq!(kept, [n as i32], "{ddl} did not commit the transaction");
    }
    // The schema change may be to the very table the transaction used.
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("INSERT INTO mdl_commit VALUES (100)")
        .await
        .unwrap();
    a.query_drop("ALTER TABLE mdl_commit ADD COLUMN c INT")
        .await
        .unwrap();
    a.query_drop("ROLLBACK").await.unwrap();
    let rows: Vec<(i32, Option<i32>)> = a
        .query("SELECT id, c FROM mdl_commit WHERE id = 100")
        .await
        .unwrap();
    assert_eq!(rows, [(100, None)]);
}

/// Schema changes to one table at the same time are made one after the
/// other: each of them takes effect.
#[tokio::test]
async fn schema_changes_to_one_table_do_not_undo_one_another() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut a = connect(&server).await;
    a.query_drop("CREATE TABLE mdl_ddl (id INT PRIMARY KEY, a INT, b INT)")
        .await
        .unwrap();
    for round in 0..8 {
        let mut one = connect(&server).await;
        let mut two = connect(&server).await;
        let mut three = connect(&server).await;
        let (r1, r2, r3) = tokio::join!(
            one.query_drop(format!("CREATE INDEX mdl_ia{round} ON mdl_ddl (a)")),
            two.query_drop(format!("CREATE INDEX mdl_ib{round} ON mdl_ddl (b)")),
            three.query_drop(format!("ALTER TABLE mdl_ddl ADD COLUMN c{round} INT")),
        );
        r1.unwrap_or_else(|e| panic!("round {round}, first index: {e}"));
        r2.unwrap_or_else(|e| panic!("round {round}, second index: {e}"));
        r3.unwrap_or_else(|e| panic!("round {round}, column: {e}"));
    }
    let indexes: Vec<mysql_async::Row> = a.query("SHOW INDEX FROM mdl_ddl").await.unwrap();
    let mut names: Vec<String> = indexes
        .iter()
        .map(|row| row.get::<String, _>("Key_name").unwrap())
        .filter(|name| name.starts_with("mdl_i"))
        .collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 16, "{names:?}");
    let columns: Vec<mysql_async::Row> = a.query("DESCRIBE mdl_ddl").await.unwrap();
    assert_eq!(columns.len(), 3 + 8);
}

/// A cycle of waits through a metadata lock and a row lock is found: the
/// statement that would close it fails with a deadlock, and its
/// transaction is rolled back.
#[tokio::test]
async fn a_deadlock_through_a_table_and_a_row_is_found() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut a = connect(&server).await;
    let mut b = connect(&server).await;
    let mut c = connect(&server).await;
    let mut watch = connect(&server).await;
    a.query_drop("CREATE TABLE mdl_dl_t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("CREATE TABLE mdl_dl_u (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO mdl_dl_u VALUES (1, 0)")
        .await
        .unwrap();

    // A uses the table t. B holds a row of u.
    a.query_drop("BEGIN").await.unwrap();
    let _: Vec<i32> = a.query("SELECT id FROM mdl_dl_t").await.unwrap();
    b.query_drop("BEGIN").await.unwrap();
    b.query_drop("UPDATE mdl_dl_u SET v = 1 WHERE id = 1")
        .await
        .unwrap();

    // C changes t: it waits for A.
    let change = tokio::spawn(async move {
        c.query_drop("ALTER TABLE mdl_dl_t ADD COLUMN c INT")
            .await
            .map(|()| c)
    });
    until_metadata_waits(&mut watch, 1).await;
    // B now uses t: it waits its turn behind C.
    let reader = tokio::spawn(async move {
        let rows: mysql_async::Result<Vec<i32>> = b.query("SELECT id FROM mdl_dl_t").await;
        rows.map(|_| b)
    });
    until_metadata_waits(&mut watch, 2).await;

    // A asks for B's row: A waits for B, B for C, C for A.
    let err = a
        .query_drop("UPDATE mdl_dl_u SET v = 2 WHERE id = 1")
        .await
        .expect_err("a cycle of waits was not found");
    assert_eq!(error_code(&err), 1213, "{err}");

    // A's transaction is gone, so the others go on in turn.
    let _c = change.await.unwrap().expect("ALTER TABLE");
    let mut b = reader
        .await
        .unwrap()
        .expect("SELECT behind the schema change");
    b.query_drop("COMMIT").await.unwrap();
    let rows: Vec<(i32, i32)> = a.query("SELECT id, v FROM mdl_dl_u").await.unwrap();
    assert_eq!(rows, [(1, 1)]);
    assert_eq!(status(&mut watch, "Roodb_metadata_locks").await, 0);
}

/// `lock_wait_timeout` is the session's, and is what `@@lock_wait_timeout`
/// shows.
#[tokio::test]
async fn the_metadata_lock_wait_is_a_session_setting() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut a = connect(&server).await;
    let mut b = connect(&server).await;
    let shown: Option<u64> = a.query_first("SELECT @@lock_wait_timeout").await.unwrap();
    assert_eq!(shown, Some(31_536_000));
    a.query_drop("SET SESSION lock_wait_timeout = 7")
        .await
        .unwrap();
    let shown: Option<u64> = a.query_first("SELECT @@lock_wait_timeout").await.unwrap();
    assert_eq!(shown, Some(7));
    let shown: Option<u64> = b.query_first("SELECT @@lock_wait_timeout").await.unwrap();
    assert_eq!(shown, Some(31_536_000));
    // The row lock wait is another setting.
    let shown: Option<u64> = a
        .query_first("SELECT @@innodb_lock_wait_timeout")
        .await
        .unwrap();
    assert_eq!(shown, Some(50));
}
