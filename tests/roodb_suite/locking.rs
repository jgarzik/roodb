//! Row locking between connections.
//!
//! Each test runs its own server, so that the lock counts it watches are
//! its own. Nothing here depends on how long anything takes: a test that
//! needs another connection to be waiting asks the server whether it is.

use std::time::{Duration, Instant};

use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::test_utils::process::ServerProcess;

async fn connect(server: &ServerProcess) -> Conn {
    Conn::new(server.opts()).await.unwrap_or_else(|e| {
        panic!("connect failed: {e}\n{}", server.log_tail());
    })
}

async fn start() -> (ServerProcess, Conn, Conn) {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let a = connect(&server).await;
    let b = connect(&server).await;
    (server, a, b)
}

/// Number of lock requests waiting on the server.
async fn waiting(conn: &mut Conn) -> usize {
    let row: Option<(String, String)> = conn
        .query_first("SHOW STATUS LIKE 'Innodb_row_lock_current_waits'")
        .await
        .unwrap();
    row.expect("status variable").1.parse().unwrap()
}

/// Wait until `n` lock requests are waiting. The deadline only keeps a
/// broken server from hanging the test.
async fn until_waiting(conn: &mut Conn, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while waiting(conn).await < n {
        assert!(Instant::now() < deadline, "no lock request is waiting");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn error_code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("not a server error: {other:?}"),
    }
}

/// A writer waits for a row another transaction has changed, then works on
/// what that transaction committed.
#[tokio::test]
async fn a_writer_waits_for_the_row_and_sees_what_was_committed() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE acct (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO acct VALUES (1, 0), (2, 0)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE acct SET v = v + 1 WHERE id = 1")
        .await
        .unwrap();

    // A different row is not held up.
    b.query_drop("UPDATE acct SET v = v + 5 WHERE id = 2")
        .await
        .unwrap();

    let blocked = tokio::spawn(async move {
        b.query_drop("UPDATE acct SET v = v + 10 WHERE id = 1")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    assert!(!blocked.is_finished());

    a.query_drop("UPDATE acct SET v = v + 1 WHERE id = 1")
        .await
        .unwrap();
    a.query_drop("COMMIT").await.unwrap();
    let mut b = blocked.await.unwrap().expect("the waiting update failed");

    let rows: Vec<(i32, i32)> = b.query("SELECT id, v FROM acct ORDER BY id").await.unwrap();
    assert_eq!(rows, [(1, 12), (2, 5)], "an update was lost");
    assert_eq!(waiting(&mut watcher).await, 0);
}

/// A rolled-back transaction lets the waiter through, on the old value.
#[tokio::test]
async fn rollback_and_disconnect_release_locks() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE held (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO held VALUES (1, 100)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("DELETE FROM held WHERE id = 1").await.unwrap();
    let blocked = tokio::spawn(async move {
        b.query_drop("UPDATE held SET v = v + 1 WHERE id = 1")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("ROLLBACK").await.unwrap();
    let mut b = blocked.await.unwrap().unwrap();
    let v: Option<i32> = b
        .query_first("SELECT v FROM held WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(v, Some(101));

    // A session that goes away mid-transaction releases its locks too.
    let mut c = connect(&server).await;
    c.query_drop("BEGIN").await.unwrap();
    c.query_drop("UPDATE held SET v = 0 WHERE id = 1")
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        b.query_drop("UPDATE held SET v = v + 1 WHERE id = 1")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    drop(c.disconnect().await);
    let mut b = blocked.await.unwrap().unwrap();
    let v: Option<i32> = b
        .query_first("SELECT v FROM held WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(v, Some(102), "the abandoned transaction's change was kept");
}

/// Two transactions that each hold what the other wants: one is told so
/// (1213) and rolled back, the other goes on.
#[tokio::test]
async fn a_deadlock_rolls_one_transaction_back() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE dl (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO dl VALUES (1, 0), (2, 0)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    b.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE dl SET v = 1 WHERE id = 1")
        .await
        .unwrap();
    b.query_drop("UPDATE dl SET v = 2 WHERE id = 2")
        .await
        .unwrap();

    let first = tokio::spawn(async move {
        a.query_drop("UPDATE dl SET v = 1 WHERE id = 2")
            .await
            .map(|()| a)
    });
    until_waiting(&mut watcher, 1).await;
    let err = b
        .query_drop("UPDATE dl SET v = 2 WHERE id = 1")
        .await
        .expect_err("the deadlock was not detected");
    assert_eq!(error_code(&err), 1213, "{err}");

    // The victim's transaction is gone, with its earlier change.
    let mut a = first.await.unwrap().expect("the survivor failed");
    a.query_drop("COMMIT").await.unwrap();
    b.query_drop("COMMIT").await.unwrap();
    let rows: Vec<(i32, i32)> = b.query("SELECT id, v FROM dl ORDER BY id").await.unwrap();
    assert_eq!(rows, [(1, 1), (2, 1)]);
}

/// A wait that outlasts innodb_lock_wait_timeout fails with 1205; the
/// transaction it belongs to stays open.
#[tokio::test]
async fn a_lock_wait_times_out() {
    let (_server, mut a, mut b) = start().await;
    a.query_drop("CREATE TABLE slow (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO slow VALUES (1, 0), (2, 0)")
        .await
        .unwrap();
    let default: Option<u64> = b
        .query_first("SELECT @@innodb_lock_wait_timeout")
        .await
        .unwrap();
    assert_eq!(default, Some(50));
    b.query_drop("SET SESSION innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    let set: Option<u64> = b
        .query_first("SELECT @@innodb_lock_wait_timeout")
        .await
        .unwrap();
    assert_eq!(set, Some(1));

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE slow SET v = 1 WHERE id = 1")
        .await
        .unwrap();

    b.query_drop("BEGIN").await.unwrap();
    b.query_drop("UPDATE slow SET v = 2 WHERE id = 2")
        .await
        .unwrap();
    let err = b
        .query_drop("UPDATE slow SET v = 2 WHERE id = 1")
        .await
        .expect_err("the wait did not time out");
    assert_eq!(error_code(&err), 1205, "{err}");
    // Only the statement failed: the transaction and its first change stand.
    b.query_drop("COMMIT").await.unwrap();
    a.query_drop("COMMIT").await.unwrap();
    let rows: Vec<(i32, i32)> = b.query("SELECT id, v FROM slow ORDER BY id").await.unwrap();
    assert_eq!(rows, [(1, 1), (2, 2)]);
}

/// A second insert of a key waits for the first; if the first commits, the
/// second is a duplicate there and then, not at its own commit.
#[tokio::test]
async fn a_duplicate_insert_waits_and_is_refused_by_the_statement() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE uniq (id INT PRIMARY KEY, who VARCHAR(10))")
        .await
        .unwrap();

    // A committed row: refused at once.
    a.query_drop("INSERT INTO uniq VALUES (1, 'a')")
        .await
        .unwrap();
    b.query_drop("BEGIN").await.unwrap();
    let err = b
        .query_drop("INSERT INTO uniq VALUES (1, 'b')")
        .await
        .expect_err("a duplicate key was accepted");
    assert_eq!(error_code(&err), 1062, "{err}");
    b.query_drop("ROLLBACK").await.unwrap();

    // An uncommitted row: the second insert waits to see how it ends.
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("INSERT INTO uniq VALUES (2, 'a')")
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        let result = b.query_drop("INSERT INTO uniq VALUES (2, 'b')").await;
        (b, result)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("COMMIT").await.unwrap();
    let (mut b, result) = blocked.await.unwrap();
    assert_eq!(error_code(&result.expect_err("duplicate accepted")), 1062);

    // And if the first rolls back, the second goes in.
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("INSERT INTO uniq VALUES (3, 'a')")
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        b.query_drop("INSERT INTO uniq VALUES (3, 'b')")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("ROLLBACK").await.unwrap();
    let mut b = blocked.await.unwrap().unwrap();
    let rows: Vec<(i32, String)> = b
        .query("SELECT id, who FROM uniq ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (1, "a".to_string()),
            (2, "a".to_string()),
            (3, "b".to_string())
        ]
    );
}

/// With autocommit off every statement is in a transaction, one after
/// another, and turning autocommit back on commits the open one.
#[tokio::test]
async fn autocommit_off_runs_transactions_back_to_back() {
    let (_server, mut a, mut b) = start().await;
    a.query_drop("CREATE TABLE ac (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("SET autocommit = 0").await.unwrap();
    let mode: Option<u8> = a.query_first("SELECT @@autocommit").await.unwrap();
    assert_eq!(mode, Some(0));

    a.query_drop("INSERT INTO ac VALUES (1)").await.unwrap();
    a.query_drop("COMMIT").await.unwrap();
    // The next statement opens a new transaction: not visible elsewhere
    // until committed, and undone by ROLLBACK.
    a.query_drop("INSERT INTO ac VALUES (2)").await.unwrap();
    let seen: Vec<i32> = b.query("SELECT id FROM ac ORDER BY id").await.unwrap();
    assert_eq!(seen, [1]);
    a.query_drop("ROLLBACK").await.unwrap();
    a.query_drop("INSERT INTO ac VALUES (3)").await.unwrap();
    // Turning autocommit on commits what is pending.
    a.query_drop("SET autocommit = 1").await.unwrap();
    let seen: Vec<i32> = b.query("SELECT id FROM ac ORDER BY id").await.unwrap();
    assert_eq!(seen, [1, 3]);
    let mode: Option<u8> = a.query_first("SELECT @@autocommit").await.unwrap();
    assert_eq!(mode, Some(1));
}
