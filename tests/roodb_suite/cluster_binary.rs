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
