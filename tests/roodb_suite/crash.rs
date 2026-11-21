//! What survives the server being killed.
//!
//! These run the real server binary on its own data directory and kill it
//! with no warning - nothing is flushed, nothing is closed. Whatever a client
//! was told had committed must be there when the server comes back.

use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::test_utils::process::ServerProcess;

async fn connect(server: &ServerProcess) -> Conn {
    Conn::new(server.opts()).await.unwrap_or_else(|e| {
        panic!("connect failed: {e}\n{}", server.log_tail());
    })
}

/// Every acknowledged statement survives a kill, in both durability modes,
/// and the server keeps working across repeated kills.
#[tokio::test]
async fn committed_work_survives_being_killed() {
    for durability in ["os", "fsync"] {
        let mut server = ServerProcess::init(&[("ROODB_DURABILITY", durability)]);
        server.start();
        let mut conn = connect(&server).await;
        conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        for id in 0..50 {
            conn.query_drop(format!("INSERT INTO t VALUES ({id}, 0)"))
                .await
                .unwrap();
        }
        drop(conn);
        server.kill();

        for round in 1..=3 {
            server.start();
            let mut conn = connect(&server).await;
            let rows: Vec<(i32, i32)> = conn
                .query("SELECT id, v FROM t ORDER BY id")
                .await
                .unwrap_or_else(|e| {
                    panic!("{durability} round {round}: {e}\n{}", server.log_tail())
                });
            assert_eq!(
                rows.len(),
                50 + (round - 1) * 10,
                "{durability} round {round}: committed rows are missing"
            );
            assert!(
                rows.iter()
                    .filter(|(id, _)| *id < 50)
                    .all(|(_, v)| *v == round as i32 - 1),
                "{durability} round {round}: an acknowledged UPDATE was lost"
            );

            // More work in each life: a multi-statement transaction, an
            // update of every old row, and new rows.
            conn.query_drop("BEGIN").await.unwrap();
            conn.query_drop(format!("UPDATE t SET v = {round} WHERE id < 50"))
                .await
                .unwrap();
            for id in 0..10 {
                conn.query_drop(format!("INSERT INTO t VALUES ({}, -1)", round * 1000 + id))
                    .await
                    .unwrap();
            }
            conn.query_drop("COMMIT").await.unwrap();

            // A transaction that never commits must leave no trace.
            conn.query_drop("BEGIN").await.unwrap();
            conn.query_drop("DELETE FROM t WHERE id < 25")
                .await
                .unwrap();
            drop(conn);
            server.kill();
        }

        server.start();
        let mut conn = connect(&server).await;
        let count: i64 = conn
            .query_first("SELECT COUNT(*) FROM t")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 80, "{durability}: final row count");
        // The duplicate-key check still sees rows from earlier lives.
        assert!(conn
            .query_drop("INSERT INTO t VALUES (7, 7)")
            .await
            .is_err());
    }
}

/// Counters that name new rows restart from saved state in a new process.
/// New rows must not land on old ones, and new schema objects must not
/// replace existing ones in the catalog.
#[tokio::test]
async fn generated_ids_do_not_repeat_after_a_kill() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut conn = connect(&server).await;
    conn.query_drop("CREATE TABLE no_pk (a INT, b VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE auto_t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE keep (id INT PRIMARY KEY, name VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO keep VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();
    for i in 0..30 {
        conn.query_drop(format!("INSERT INTO no_pk VALUES ({i}, 'life0')"))
            .await
            .unwrap();
        conn.query_drop(format!("INSERT INTO auto_t (v) VALUES ('life0-{i}')"))
            .await
            .unwrap();
    }
    drop(conn);
    server.kill();

    for life in 1..=3 {
        server.start();
        let mut conn = connect(&server).await;

        let before: i64 = conn
            .query_first("SELECT COUNT(*) FROM no_pk")
            .await
            .unwrap_or_else(|e| panic!("life {life}: {e}\n{}", server.log_tail()))
            .unwrap();
        assert_eq!(before, 30 * life as i64, "life {life}: rows missing");
        for i in 0..30 {
            conn.query_drop(format!("INSERT INTO no_pk VALUES ({i}, 'life{life}')"))
                .await
                .unwrap();
        }
        let after: i64 = conn
            .query_first("SELECT COUNT(*) FROM no_pk")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after,
            before + 30,
            "life {life}: new rows overwrote old ones"
        );

        let highest: i64 = conn
            .query_first("SELECT MAX(id) FROM auto_t")
            .await
            .unwrap()
            .unwrap();
        conn.query_drop(format!("INSERT INTO auto_t (v) VALUES ('life{life}')"))
            .await
            .unwrap();
        let newest: i64 = conn
            .query_first(format!("SELECT id FROM auto_t WHERE v = 'life{life}'"))
            .await
            .unwrap()
            .unwrap();
        assert!(
            newest > highest,
            "life {life}: AUTO_INCREMENT gave {newest}, not above {highest}"
        );

        // New tables get new rows in the system tables.
        conn.query_drop(format!(
            "CREATE TABLE made{life} (id INT PRIMARY KEY, x INT)"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("INSERT INTO made{life} VALUES (1, {life})"))
            .await
            .unwrap();
        let names: Vec<String> = conn
            .query("SELECT name FROM keep ORDER BY id")
            .await
            .unwrap();
        assert_eq!(
            names,
            ["one", "two"],
            "life {life}: an existing table was damaged"
        );
        for earlier in 1..life {
            let x: i64 = conn
                .query_first(format!("SELECT x FROM made{earlier} WHERE id = 1"))
                .await
                .unwrap_or_else(|e| panic!("life {life}: made{earlier} is gone: {e}"))
                .unwrap();
            assert_eq!(x, earlier as i64);
        }
        drop(conn);
        server.kill();
    }
}

/// Killed in the middle of a stream of commits: every commit the client saw
/// acknowledged is there afterwards, and at most the one in flight besides.
#[tokio::test]
async fn a_kill_mid_stream_loses_nothing_acknowledged() {
    for durability in ["os", "fsync"] {
        let mut server = ServerProcess::init(&[("ROODB_DURABILITY", durability)]);
        server.start();
        let mut conn = connect(&server).await;
        conn.query_drop("CREATE TABLE stream (id INT PRIMARY KEY, payload VARCHAR(200))")
            .await
            .unwrap();

        let acknowledged = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(-1));
        let writer = {
            let acknowledged = acknowledged.clone();
            tokio::spawn(async move {
                let payload = "x".repeat(150);
                for id in 0i64.. {
                    let sql = format!("INSERT INTO stream VALUES ({id}, '{payload}')");
                    if conn.query_drop(sql).await.is_err() {
                        return;
                    }
                    acknowledged.store(id, std::sync::atomic::Ordering::SeqCst);
                }
            })
        };
        // Let it run long enough for thousands of commits, then pull the plug.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        server.kill();
        writer.await.unwrap();
        let last_acknowledged = acknowledged.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            last_acknowledged > 100,
            "{durability}: only {last_acknowledged} commits before the kill"
        );

        server.start();
        let mut conn = connect(&server).await;
        let ids: Vec<i64> = conn
            .query("SELECT id FROM stream ORDER BY id")
            .await
            .unwrap_or_else(|e| panic!("{durability}: {e}\n{}", server.log_tail()));
        // Commits are sequential, so what survives is 0..n with no gaps.
        for (expected, id) in ids.iter().enumerate() {
            assert_eq!(*id, expected as i64, "{durability}: gap in surviving rows");
        }
        let survived = ids.len() as i64 - 1;
        assert!(
            survived >= last_acknowledged,
            "{durability}: commit {last_acknowledged} was acknowledged but only up to {survived} survived"
        );
        assert!(
            survived <= last_acknowledged + 1,
            "{durability}: {survived} rows survived but only {last_acknowledged} were sent"
        );
    }
}

/// Asked to stop, the server exits by itself with everything in memory
/// written out to table files, and comes back with it all.
#[cfg(unix)]
#[tokio::test]
async fn a_stop_request_shuts_down_cleanly() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut conn = connect(&server).await;
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100))")
        .await
        .unwrap();
    for id in 0..200 {
        conn.query_drop(format!("INSERT INTO t VALUES ({id}, 'value {id}')"))
            .await
            .unwrap();
    }
    drop(conn);
    let before = server.table_files();

    let status = server.stop();
    assert!(
        status.success(),
        "exit status {status}\n{}",
        server.log_tail()
    );
    let after = server.table_files();
    assert!(
        after.len() > before.len(),
        "a clean stop should flush memory to a table file: {before:?} -> {after:?}"
    );

    server.start();
    let mut conn = connect(&server).await;
    let count: i64 = conn
        .query_first("SELECT COUNT(*) FROM t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 200);
    conn.query_drop("INSERT INTO t VALUES (1000, 'after')")
        .await
        .unwrap();
    drop(conn);
    // And again, then a kill: mixing the two is fine.
    assert!(server.stop().success());
    server.start();
    server.kill();
    server.start();
    let mut conn = connect(&server).await;
    let count: i64 = conn
        .query_first("SELECT COUNT(*) FROM t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 201);
}

/// A table dropped and created again shortly before a kill comes back as the
/// new table. On restart both statements are replayed together; the second
/// CREATE must not be taken for a duplicate of the first.
#[tokio::test]
async fn a_table_recreated_before_a_kill_is_the_new_one() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut conn = connect(&server).await;
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, old_col INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    conn.query_drop("CREATE INDEX idx_old ON t (old_col)")
        .await
        .unwrap();
    conn.query_drop("DROP TABLE t").await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, new_col VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t VALUES (2, 'new')")
        .await
        .unwrap();
    // Create, drop, create again: three operations on one name.
    conn.query_drop("CREATE TABLE u (a INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("DROP TABLE u").await.unwrap();
    conn.query_drop("CREATE TABLE u (b INT PRIMARY KEY, c INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO u VALUES (5, 6)")
        .await
        .unwrap();
    // And one that ends dropped.
    conn.query_drop("CREATE TABLE gone (a INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("DROP TABLE gone").await.unwrap();
    drop(conn);
    server.kill();

    server.start();
    let mut conn = connect(&server).await;
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, new_col FROM t")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the re-created table is not usable: {e}\n{}",
                server.log_tail()
            )
        });
    assert_eq!(rows, [(2, "new".to_string())]);
    let rows: Vec<(i32, i32)> = conn.query("SELECT b, c FROM u").await.unwrap();
    assert_eq!(rows, [(5, 6)]);
    assert!(
        conn.query_drop("SELECT * FROM gone").await.is_err(),
        "a dropped table came back"
    );
    // The schema is whole enough to keep working with.
    conn.query_drop("INSERT INTO t VALUES (3, 'more')")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE gone (z INT PRIMARY KEY)")
        .await
        .unwrap();
}

/// A temporary table's rows belong to its session and are gone with it.
/// After a kill, an ordinary table created under the same name is empty.
#[tokio::test]
async fn temporary_table_rows_do_not_outlive_their_session() {
    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut conn = connect(&server).await;
    conn.query_drop("CREATE TABLE real_t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("CREATE TEMPORARY TABLE tmp (id INT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap();
    for id in 0..100 {
        conn.query_drop(format!("INSERT INTO tmp VALUES ({id}, 'temp')"))
            .await
            .unwrap();
    }
    // Temporary and ordinary rows changed in one transaction.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO real_t VALUES (1, 1)")
        .await
        .unwrap();
    conn.query_drop("UPDATE tmp SET v = 'changed' WHERE id < 10")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM tmp WHERE id >= 90")
        .await
        .unwrap();
    conn.query_drop("COMMIT").await.unwrap();
    let count: i64 = conn
        .query_first("SELECT COUNT(*) FROM tmp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 90);
    let changed: i64 = conn
        .query_first("SELECT COUNT(*) FROM tmp WHERE v = 'changed'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed, 10);
    // A duplicate key in a temporary table is still an error.
    assert!(conn
        .query_drop("INSERT INTO tmp VALUES (5, 'dup')")
        .await
        .is_err());

    // Another session does not see the rows.
    let mut other = connect(&server).await;
    let seen: Result<Vec<(i32, String)>, _> = other.query("SELECT id, v FROM tmp").await;
    if let Ok(rows) = seen {
        assert!(rows.is_empty(), "another session saw temporary rows");
    }
    drop(other);
    drop(conn);
    server.kill();

    server.start();
    let mut conn = connect(&server).await;
    let real: Vec<(i32, i32)> = conn.query("SELECT id, v FROM real_t").await.unwrap();
    assert_eq!(real, [(1, 1)], "the ordinary half of the transaction");
    conn.query_drop("CREATE TABLE tmp (id INT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap_or_else(|e| panic!("{e}\n{}", server.log_tail()));
    let ghosts: i64 = conn
        .query_first("SELECT COUNT(*) FROM tmp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ghosts, 0,
        "rows of a dead session's temporary table came back"
    );
    conn.query_drop("INSERT INTO tmp VALUES (5, 'fresh')")
        .await
        .unwrap();
}
