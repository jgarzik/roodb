//! Clusters of real server processes: nodes that join a running server,
//! receive its data, and follow it.
//!
//! Nothing here waits for a length of time: a test that needs a node to
//! have caught up asks it until it has.

use std::time::{Duration, Instant};

use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::test_utils::process::ServerProcess;

async fn connect(server: &ServerProcess) -> Conn {
    Conn::new(server.opts()).await.unwrap_or_else(|e| {
        panic!("connect failed: {e}\n{}", server.log_tail());
    })
}

/// Run `query` on `conn` until it returns `expected`. The deadline only
/// keeps a broken cluster from hanging the test.
async fn until_rows(conn: &mut Conn, server: &ServerProcess, query: &str, expected: &[i32]) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let rows: Result<Vec<i32>, _> = conn.query(query).await;
        if matches!(&rows, Ok(rows) if rows == expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "`{query}` never returned {expected:?}; last: {rows:?}\n{}",
            server.log_tail()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Two nodes join a running server with nothing but its address. They are
/// given what it already held (including what `roodb_init` wrote: they can
/// be logged in to) and everything written afterwards.
#[tokio::test]
async fn nodes_join_and_receive_the_data() {
    let mut first = ServerProcess::init(&[]);
    first.start();
    let mut primary = connect(&first).await;
    primary
        .query_drop("CREATE TABLE before_join (id INT PRIMARY KEY)")
        .await
        .unwrap();
    primary
        .query_drop("INSERT INTO before_join VALUES (1), (2)")
        .await
        .unwrap();

    let mut second = first.joiner();
    second.start();
    let mut third = first.joiner();
    third.start();

    let mut on_second = connect(&second).await;
    let mut on_third = connect(&third).await;
    let query = "SELECT id FROM before_join ORDER BY id";
    until_rows(&mut on_second, &second, query, &[1, 2]).await;
    until_rows(&mut on_third, &third, query, &[1, 2]).await;

    // What is written now reaches them too, schema changes included.
    primary
        .query_drop("INSERT INTO before_join VALUES (3)")
        .await
        .unwrap();
    primary
        .query_drop("CREATE TABLE after_join (id INT PRIMARY KEY)")
        .await
        .unwrap();
    primary
        .query_drop("INSERT INTO after_join VALUES (10)")
        .await
        .unwrap();
    until_rows(&mut on_second, &second, query, &[1, 2, 3]).await;
    until_rows(&mut on_third, &third, query, &[1, 2, 3]).await;
    let query = "SELECT id FROM after_join";
    until_rows(&mut on_second, &second, query, &[10]).await;
    until_rows(&mut on_third, &third, query, &[10]).await;
}

fn error_code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("not a server error: {other:?}"),
    }
}

/// Start a cluster of three: the first server and two that join it. Returns
/// once both joiners hold what the first one has.
async fn cluster_of_three() -> [ServerProcess; 3] {
    let mut first = ServerProcess::init(&[]);
    first.start();
    let mut primary = connect(&first).await;
    primary
        .query_drop("CREATE TABLE formed (id INT PRIMARY KEY)")
        .await
        .unwrap();
    primary
        .query_drop("INSERT INTO formed VALUES (1)")
        .await
        .unwrap();
    let mut second = first.joiner();
    second.start();
    let mut third = first.joiner();
    third.start();
    for node in [&second, &third] {
        let mut conn = connect(node).await;
        until_rows(&mut conn, node, "SELECT id FROM formed", &[1]).await;
    }
    [first, second, third]
}

/// Which of `nodes` accepts a write: the primary. Tried until one does (a
/// cluster that has just lost its primary takes a moment to choose another).
async fn find_primary(nodes: &[&ServerProcess]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            let Ok(mut conn) = Conn::new(node.opts()).await else {
                continue;
            };
            if conn
                .query_drop("CREATE TABLE IF NOT EXISTS primary_probe (id INT PRIMARY KEY)")
                .await
                .is_ok()
            {
                return index;
            }
        }
        assert!(Instant::now() < deadline, "no node accepts writes");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A secondary answers reads and refuses everything that writes or locks,
/// with the error a read-only MySQL server gives.
#[tokio::test]
async fn a_secondary_is_read_only() {
    let [first, second, _third] = cluster_of_three().await;
    let mut primary = connect(&first).await;
    let mut secondary = connect(&second).await;

    for sql in [
        "INSERT INTO formed VALUES (2)",
        "UPDATE formed SET id = 3 WHERE id = 1",
        "DELETE FROM formed",
        "CREATE TABLE not_here (id INT PRIMARY KEY)",
        "DROP TABLE formed",
        "ALTER TABLE formed ADD COLUMN v INT",
        "SELECT id FROM formed FOR UPDATE",
    ] {
        let err = secondary
            .query_drop(sql)
            .await
            .expect_err("a secondary accepted a write");
        assert_eq!(error_code(&err), 1290, "{sql}: {err}");
    }
    // Reading is fine, in a transaction too.
    secondary.query_drop("BEGIN").await.unwrap();
    let rows: Vec<i32> = secondary.query("SELECT id FROM formed").await.unwrap();
    assert_eq!(rows, [1]);
    secondary.query_drop("COMMIT").await.unwrap();
    // And the primary is not affected.
    primary
        .query_drop("INSERT INTO formed VALUES (2)")
        .await
        .unwrap();
    until_rows(
        &mut secondary,
        &second,
        "SELECT id FROM formed ORDER BY id",
        &[1, 2],
    )
    .await;
}

/// The primary is killed: another node takes over, nothing that was
/// acknowledged is lost, and the old primary comes back as a secondary and
/// catches up.
#[tokio::test]
async fn another_node_takes_over_when_the_primary_dies() {
    let [mut first, second, third] = cluster_of_three().await;
    let mut primary = connect(&first).await;
    primary
        .query_drop("CREATE TABLE kept (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for id in 0..50 {
        primary
            .query_drop(format!("INSERT INTO kept VALUES ({id})"))
            .await
            .unwrap();
    }
    drop(primary);
    first.kill();

    let survivors = [&second, &third];
    let leader = find_primary(&survivors).await;
    let mut primary = connect(survivors[leader]).await;
    let all: Vec<i32> = (0..50).collect();
    until_rows(
        &mut primary,
        survivors[leader],
        "SELECT id FROM kept ORDER BY id",
        &all,
    )
    .await;
    primary
        .query_drop("INSERT INTO kept VALUES (50)")
        .await
        .unwrap();

    // The old primary returns. It remembers its cluster, follows the new
    // primary, and is read-only.
    first.start();
    let mut returned = connect(&first).await;
    let all: Vec<i32> = (0..=50).collect();
    until_rows(
        &mut returned,
        &first,
        "SELECT id FROM kept ORDER BY id",
        &all,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match returned.query_drop("INSERT INTO kept VALUES (99)").await {
            Err(e) if error_code(&e) == 1290 => break,
            // For an instant after starting it has not heard from the
            // primary yet.
            other => assert!(
                Instant::now() < deadline,
                "the returned node is not read-only: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A secondary that was down while the primary went on comes back and
/// catches up by itself.
#[tokio::test]
async fn a_secondary_catches_up_after_a_restart() {
    let [first, mut second, _third] = cluster_of_three().await;
    let mut primary = connect(&first).await;
    second.kill();
    primary
        .query_drop("CREATE TABLE missed (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for id in 0..20 {
        primary
            .query_drop(format!("INSERT INTO missed VALUES ({id})"))
            .await
            .unwrap();
    }
    second.start();
    let mut conn = connect(&second).await;
    let all: Vec<i32> = (0..20).collect();
    until_rows(
        &mut conn,
        &second,
        "SELECT id FROM missed ORDER BY id",
        &all,
    )
    .await;
}

async fn members(conn: &mut Conn) -> Vec<(u64, String, String)> {
    let mut rows: Vec<(u64, String, String)> = conn.query("SHOW CLUSTER MEMBERS").await.unwrap();
    rows.sort();
    rows
}

/// Every node can say who is in the cluster and who the primary is; the
/// primary can take a node out, and that node then stops by itself.
#[tokio::test]
async fn members_are_listed_and_a_node_can_be_removed() {
    let [first, second, mut third] = cluster_of_three().await;
    let mut primary = connect(&first).await;
    let mut secondary = connect(&second).await;

    // Wait until both joiners are voting members, then compare the views.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let seen = members(&mut primary).await;
        let roles: Vec<&str> = seen.iter().map(|(_, _, role)| role.as_str()).collect();
        if roles == ["PRIMARY", "SECONDARY", "SECONDARY"] {
            assert_eq!(
                seen.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                [1, 2, 3]
            );
            assert_eq!(seen[0].1, first.raft_addr());
            break;
        }
        assert!(Instant::now() < deadline, "members never settled: {seen:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while members(&mut secondary).await != members(&mut primary).await {
        assert!(Instant::now() < deadline, "the secondary's view differs");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Only the primary changes the membership, and only for a member.
    let err = secondary
        .query_drop("ALTER CLUSTER REMOVE NODE 3")
        .await
        .expect_err("a secondary removed a node");
    assert_eq!(error_code(&err), 1290, "{err}");
    assert!(primary
        .query_drop("ALTER CLUSTER REMOVE NODE 9")
        .await
        .is_err());
    assert!(primary.query_drop("ALTER CLUSTER REMOVE").await.is_err());

    // Which of the joiners is node 3?
    let third_is_3 = members(&mut primary)
        .await
        .iter()
        .any(|(id, addr, _)| *id == 3 && *addr == third.raft_addr());
    let (mut removed, kept) = if third_is_3 {
        (third, second)
    } else {
        drop(secondary);
        third = second;
        (third, first.joiner())
    };
    let _ = &kept;
    primary
        .query_drop("ALTER CLUSTER REMOVE NODE 3")
        .await
        .unwrap();
    let ids: Vec<u64> = members(&mut primary)
        .await
        .iter()
        .map(|(id, _, _)| *id)
        .collect();
    assert_eq!(ids, [1, 2]);

    // The removed node learns of it and stops.
    let deadline = Instant::now() + Duration::from_secs(120);
    while !removed.has_exited() {
        assert!(
            Instant::now() < deadline,
            "the removed node is still running\n{}",
            removed.log_tail()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The cluster goes on with two.
    primary
        .query_drop("INSERT INTO formed VALUES (5)")
        .await
        .unwrap();
}

/// Rows of a table without a primary key get their keys from the node that
/// inserts them. A node that becomes primary must give out keys no node
/// gave out before: no row may be overwritten.
#[tokio::test]
async fn generated_row_keys_stay_unique_across_a_failover() {
    let [mut first, second, third] = cluster_of_three().await;
    let mut primary = connect(&first).await;
    primary
        .query_drop("CREATE TABLE nokey (n INT)")
        .await
        .unwrap();
    for n in 0..30 {
        primary
            .query_drop(format!("INSERT INTO nokey VALUES ({n})"))
            .await
            .unwrap();
    }
    drop(primary);
    first.kill();

    let survivors = [&second, &third];
    let leader = find_primary(&survivors).await;
    let mut primary = connect(survivors[leader]).await;
    for n in 30..60 {
        primary
            .query_drop(format!("INSERT INTO nokey VALUES ({n})"))
            .await
            .unwrap();
    }
    // The old primary comes back and leads again or not; either way more
    // rows are added, by whoever is primary then.
    first.start();
    let all = [&first, &second, &third];
    let leader = find_primary(&all).await;
    let mut primary = connect(all[leader]).await;
    for n in 60..90 {
        primary
            .query_drop(format!("INSERT INTO nokey VALUES ({n})"))
            .await
            .unwrap();
    }
    let expected: Vec<i32> = (0..90).collect();
    until_rows(
        &mut primary,
        all[leader],
        "SELECT n FROM nokey ORDER BY n",
        &expected,
    )
    .await;
}
