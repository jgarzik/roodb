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

/// A statement that read the whole table keeps rows from appearing in it
/// until its transaction ends (REPEATABLE READ, the default).
#[tokio::test]
async fn a_scan_keeps_new_rows_out_until_the_transaction_ends() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE ph (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO ph VALUES (1, 1), (2, 2)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE ph SET v = v + 10 WHERE v > 0")
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        b.query_drop("INSERT INTO ph VALUES (3, 3)")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    // Had the row slipped in, this would have changed three rows.
    a.query_drop("UPDATE ph SET v = v + 10 WHERE v > 0")
        .await
        .unwrap();
    assert_eq!(a.affected_rows(), 2);
    a.query_drop("COMMIT").await.unwrap();
    let mut b = blocked.await.unwrap().expect("the insert failed");
    let rows: Vec<(i32, i32)> = b.query("SELECT id, v FROM ph ORDER BY id").await.unwrap();
    assert_eq!(rows, [(1, 21), (2, 22), (3, 3)]);
}

/// Looking for a row that is not there locks the gap it would be in: an
/// insert into that gap waits, one elsewhere does not, and another
/// transaction looking for the same missing row does not wait either.
#[tokio::test]
async fn a_missing_row_locks_its_gap() {
    let (server, mut a, mut b) = start().await;
    let mut c = connect(&server).await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE gap (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO gap VALUES (10, 0), (20, 0), (30, 0)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("DELETE FROM gap WHERE id = 15").await.unwrap();
    assert_eq!(a.affected_rows(), 0);

    // Outside the gap (10, 20): no wait. The rows at its ends are not
    // locked either.
    c.query_drop("INSERT INTO gap VALUES (25, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO gap VALUES (5, 0)").await.unwrap();
    c.query_drop("UPDATE gap SET v = 1 WHERE id = 10")
        .await
        .unwrap();
    c.query_drop("UPDATE gap SET v = 1 WHERE id = 20")
        .await
        .unwrap();
    // Another transaction may look for the same missing row.
    c.query_drop("BEGIN").await.unwrap();
    c.query_drop("UPDATE gap SET v = 1 WHERE id = 15")
        .await
        .unwrap();
    c.query_drop("COMMIT").await.unwrap();

    // Inside the gap: waits, wherever in the gap it is.
    let blocked = tokio::spawn(async move {
        b.query_drop("INSERT INTO gap VALUES (17, 0)")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("ROLLBACK").await.unwrap();
    let mut b = blocked.await.unwrap().expect("the insert failed");
    let ids: Vec<i32> = b.query("SELECT id FROM gap ORDER BY id").await.unwrap();
    assert_eq!(ids, [5, 10, 17, 20, 25, 30]);
}

/// Two transactions that both found a row missing and both go on to insert
/// it: each holds the gap the other needs. One is rolled back.
#[tokio::test]
async fn two_inserts_into_a_gap_both_locked_deadlock() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE gdl (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO gdl VALUES (10), (20)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    b.query_drop("BEGIN").await.unwrap();
    a.query_drop("DELETE FROM gdl WHERE id = 15").await.unwrap();
    b.query_drop("DELETE FROM gdl WHERE id = 15").await.unwrap();
    let first = tokio::spawn(async move {
        a.query_drop("INSERT INTO gdl VALUES (15)")
            .await
            .map(|()| a)
    });
    until_waiting(&mut watcher, 1).await;
    let err = b
        .query_drop("INSERT INTO gdl VALUES (16)")
        .await
        .expect_err("the deadlock was not detected");
    assert_eq!(error_code(&err), 1213, "{err}");
    let mut a = first.await.unwrap().expect("the survivor failed");
    a.query_drop("COMMIT").await.unwrap();
    let ids: Vec<i32> = a.query("SELECT id FROM gdl ORDER BY id").await.unwrap();
    assert_eq!(ids, [10, 15, 20]);
}

/// READ COMMITTED locks only the rows a statement changes: rows it merely
/// looked at are free again, and new rows may appear.
#[tokio::test]
async fn read_committed_locks_only_the_rows_it_changes() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE rc (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO rc VALUES (1, 5), (2, 6)")
        .await
        .unwrap();

    a.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap();
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE rc SET v = 50 WHERE v = 5")
        .await
        .unwrap();

    // A row the scan passed over, and a new row: neither waits.
    b.query_drop("UPDATE rc SET v = 60 WHERE id = 2")
        .await
        .unwrap();
    b.query_drop("INSERT INTO rc VALUES (3, 7)").await.unwrap();
    // The row that was changed does.
    let blocked = tokio::spawn(async move {
        b.query_drop("UPDATE rc SET v = v + 1 WHERE id = 1")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("COMMIT").await.unwrap();
    let mut b = blocked.await.unwrap().unwrap();
    let rows: Vec<(i32, i32)> = b.query("SELECT id, v FROM rc ORDER BY id").await.unwrap();
    assert_eq!(rows, [(1, 51), (2, 60), (3, 7)]);
}

/// SELECT ... FOR UPDATE locks the rows it returns: a writer waits for the
/// reader's transaction, and the reader may then change what it read.
#[tokio::test]
async fn select_for_update_holds_the_rows_it_read() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE fu (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO fu VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    let v: Option<i32> = a
        .query_first("SELECT v FROM fu WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();
    assert_eq!(v, Some(10));

    // Another row is free; a plain read of the locked row does not wait.
    b.query_drop("UPDATE fu SET v = 21 WHERE id = 2")
        .await
        .unwrap();
    let seen: Option<i32> = b
        .query_first("SELECT v FROM fu WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(seen, Some(10));

    let blocked = tokio::spawn(async move {
        b.query_drop("UPDATE fu SET v = v + 1 WHERE id = 1")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("UPDATE fu SET v = 100 WHERE id = 1")
        .await
        .unwrap();
    a.query_drop("COMMIT").await.unwrap();
    let mut b = blocked.await.unwrap().unwrap();
    let v: Option<i32> = b
        .query_first("SELECT v FROM fu WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(v, Some(101));
}

/// A locking read waits for a writer and then returns what the writer
/// committed, even outside a transaction.
#[tokio::test]
async fn a_locking_read_waits_and_reads_the_latest() {
    let (server, mut a, mut b) = start().await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE lr (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO lr VALUES (1, 1)").await.unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("UPDATE lr SET v = 2 WHERE id = 1")
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        let v: Option<i32> = b
            .query_first("SELECT v FROM lr WHERE id = 1 FOR SHARE")
            .await
            .unwrap();
        (b, v)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("COMMIT").await.unwrap();
    let (mut b, v) = blocked.await.unwrap();
    assert_eq!(v, Some(2));
    // Outside a transaction the read's own locks are gone when it returns.
    assert_eq!(waiting(&mut watcher).await, 0);
    a.query_drop("UPDATE lr SET v = 3 WHERE id = 1")
        .await
        .unwrap();
    // The older spelling.
    let v: Option<i32> = b
        .query_first("SELECT v FROM lr WHERE id = 1 LOCK IN SHARE MODE")
        .await
        .unwrap();
    assert_eq!(v, Some(3));
}

/// FOR SHARE readers coexist and keep writers out; NOWAIT fails at once on
/// a locked row and SKIP LOCKED leaves it out.
#[tokio::test]
async fn shared_locks_nowait_and_skip_locked() {
    let (_server, mut a, mut b) = start().await;
    a.query_drop("CREATE TABLE sh (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO sh VALUES (1, 1), (2, 2), (3, 3)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    b.query_drop("BEGIN").await.unwrap();
    let _: Vec<i32> = a
        .query("SELECT v FROM sh WHERE id = 2 FOR SHARE")
        .await
        .unwrap();
    // A second shared reader of the same row does not wait.
    let v: Vec<i32> = b
        .query("SELECT v FROM sh WHERE id = 2 FOR SHARE")
        .await
        .unwrap();
    assert_eq!(v, [2]);
    b.query_drop("COMMIT").await.unwrap();

    // An exclusive request on it cannot be met now.
    b.query_drop("BEGIN").await.unwrap();
    let err = b
        .query_drop("SELECT v FROM sh WHERE id = 2 FOR UPDATE NOWAIT")
        .await
        .expect_err("NOWAIT waited or succeeded");
    assert_eq!(error_code(&err), 3572, "{err}");
    let ids: Vec<i32> = b
        .query("SELECT id FROM sh ORDER BY id FOR UPDATE SKIP LOCKED")
        .await
        .unwrap();
    assert_eq!(ids, [1, 3]);
    b.query_drop("ROLLBACK").await.unwrap();
    a.query_drop("COMMIT").await.unwrap();

    let ids: Vec<i32> = b
        .query("SELECT id FROM sh ORDER BY id FOR UPDATE SKIP LOCKED")
        .await
        .unwrap();
    assert_eq!(ids, [1, 2, 3]);
}

/// A locking read of a range keeps rows from being inserted into it, and a
/// locking read that finds no row keeps that row from being created.
#[tokio::test]
async fn a_locking_read_locks_the_range_it_read() {
    let (server, mut a, mut b) = start().await;
    let mut c = connect(&server).await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE rg (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO rg VALUES (10), (20), (30), (40)")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    let ids: Vec<i32> = a
        .query("SELECT id FROM rg WHERE id BETWEEN 15 AND 25 FOR UPDATE")
        .await
        .unwrap();
    assert_eq!(ids, [20]);
    let none: Vec<i32> = a
        .query("SELECT id FROM rg WHERE id = 35 FOR UPDATE")
        .await
        .unwrap();
    assert!(none.is_empty());

    // Beyond the rows that bound what was read: free.
    c.query_drop("INSERT INTO rg VALUES (5)").await.unwrap();
    c.query_drop("INSERT INTO rg VALUES (45)").await.unwrap();
    // Inside the range, and where the missing row would be: wait.
    let first =
        tokio::spawn(async move { b.query_drop("INSERT INTO rg VALUES (17)").await.map(|()| b) });
    until_waiting(&mut watcher, 1).await;
    let second =
        tokio::spawn(async move { c.query_drop("INSERT INTO rg VALUES (35)").await.map(|()| c) });
    until_waiting(&mut watcher, 2).await;
    // The read gives the same answer for as long as the transaction lasts.
    let ids: Vec<i32> = a
        .query("SELECT id FROM rg WHERE id BETWEEN 15 AND 25 FOR UPDATE")
        .await
        .unwrap();
    assert_eq!(ids, [20]);
    a.query_drop("COMMIT").await.unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let ids: Vec<i32> = a.query("SELECT id FROM rg ORDER BY id").await.unwrap();
    assert_eq!(ids, [5, 10, 17, 20, 30, 35, 40, 45]);
}

/// BEGIN inside a transaction commits it and starts another.
#[tokio::test]
async fn begin_commits_the_open_transaction() {
    let (_server, mut a, mut b) = start().await;
    a.query_drop("CREATE TABLE bg (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("INSERT INTO bg VALUES (1)").await.unwrap();
    // A statement that fails does not end the transaction...
    assert!(a.query_drop("INSERT INTO bg VALUES (1)").await.is_err());
    // ...and beginning again commits what it had done.
    a.query_drop("START TRANSACTION").await.unwrap();
    let seen: Vec<i32> = b.query("SELECT id FROM bg").await.unwrap();
    assert_eq!(seen, [1]);
    a.query_drop("INSERT INTO bg VALUES (2)").await.unwrap();
    a.query_drop("ROLLBACK").await.unwrap();
    let seen: Vec<i32> = b.query("SELECT id FROM bg").await.unwrap();
    assert_eq!(seen, [1]);
}

/// A deleted row still bounds a gap while its marker is stored, so the gap
/// around a missing key reaches only as far as the nearest stored key on
/// each side, deleted or not.
#[tokio::test]
async fn a_deleted_row_bounds_the_gap() {
    let (server, mut a, mut b) = start().await;
    let mut c = connect(&server).await;
    let mut watcher = connect(&server).await;
    a.query_drop("CREATE TABLE dgap (id INT PRIMARY KEY)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO dgap VALUES (10), (20), (30)")
        .await
        .unwrap();
    a.query_drop("DELETE FROM dgap WHERE id = 20")
        .await
        .unwrap();

    a.query_drop("BEGIN").await.unwrap();
    a.query_drop("DELETE FROM dgap WHERE id = 15")
        .await
        .unwrap();
    // Past the deleted row: not in the gap.
    c.query_drop("INSERT INTO dgap VALUES (25)").await.unwrap();
    // Before it: in the gap.
    let blocked = tokio::spawn(async move {
        b.query_drop("INSERT INTO dgap VALUES (17)")
            .await
            .map(|()| b)
    });
    until_waiting(&mut watcher, 1).await;
    a.query_drop("COMMIT").await.unwrap();
    let mut b = blocked.await.unwrap().unwrap();
    let ids: Vec<i32> = b.query("SELECT id FROM dgap ORDER BY id").await.unwrap();
    assert_eq!(ids, [10, 17, 25, 30]);
}
