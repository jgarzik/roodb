//! AUTO_INCREMENT values: a counter per table, as in MySQL.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

async fn ids(conn: &mut mysql_async::Conn, table: &str) -> Vec<i64> {
    conn.query(format!("SELECT id FROM {table} ORDER BY id"))
        .await
        .unwrap()
}

/// Each table counts for itself, from 1, whatever other tables have done.
#[tokio::test]
async fn every_table_counts_from_one() {
    let server = TestServer::start("ai_from_one").await;
    let mut conn = server.connect().await;
    for table in ["ai_one_a", "ai_one_b"] {
        conn.query_drop(format!(
            "CREATE TABLE {table} (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("INSERT INTO {table} (v) VALUES (10), (20), (30)"))
            .await
            .unwrap();
        conn.query_drop(format!("INSERT INTO {table} (v) VALUES (40)"))
            .await
            .unwrap();
        assert_eq!(ids(&mut conn, table).await, vec![1, 2, 3, 4], "{table}");
    }
    // A table created again under the same name starts over.
    conn.query_drop("DROP TABLE ai_one_a").await.unwrap();
    conn.query_drop(
        "CREATE TABLE ai_one_a (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO ai_one_a (v) VALUES (1)")
        .await
        .unwrap();
    assert_eq!(ids(&mut conn, "ai_one_a").await, vec![1]);
    conn.query_drop("DROP TABLE ai_one_a, ai_one_b")
        .await
        .unwrap();
    server.shutdown().await;
}

/// A value given explicitly moves the counter past it; NULL and 0 ask for
/// the next value; a rolled-back insert does not give its values back.
#[tokio::test]
async fn explicit_values_move_the_counter() {
    let server = TestServer::start("ai_explicit").await;
    let mut conn = server.connect().await;
    conn.query_drop(
        "CREATE TABLE ai_explicit_t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (v) VALUES (1)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (id, v) VALUES (100, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (id, v) VALUES (NULL, 3), (0, 4)")
        .await
        .unwrap();
    // Below the counter: stored as given, the counter stays.
    conn.query_drop("INSERT INTO ai_explicit_t (id, v) VALUES (50, 5)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (v) VALUES (6)")
        .await
        .unwrap();
    assert_eq!(
        ids(&mut conn, "ai_explicit_t").await,
        vec![1, 50, 100, 101, 102, 103]
    );

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (v) VALUES (7)")
        .await
        .unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();
    conn.query_drop("INSERT INTO ai_explicit_t (v) VALUES (8)")
        .await
        .unwrap();
    assert_eq!(
        ids(&mut conn, "ai_explicit_t").await,
        vec![1, 50, 100, 101, 102, 103, 105]
    );
    conn.query_drop("DROP TABLE ai_explicit_t").await.unwrap();
    server.shutdown().await;
}

/// LAST_INSERT_ID() is the first value a statement generated, and stays
/// when a later statement generates none.
#[tokio::test]
async fn last_insert_id_is_the_first_generated_value() {
    let server = TestServer::start("ai_last").await;
    let mut conn = server.connect().await;
    conn.query_drop(
        "CREATE TABLE ai_last_t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO ai_last_t (v) VALUES (1), (2), (3)")
        .await
        .unwrap();
    assert_eq!(conn.last_insert_id(), Some(1));
    let last: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await.unwrap();
    assert_eq!(last, Some(1));
    conn.query_drop("INSERT INTO ai_last_t (v) VALUES (4), (5)")
        .await
        .unwrap();
    let last: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await.unwrap();
    assert_eq!(last, Some(4));
    conn.query_drop("INSERT INTO ai_last_t (id, v) VALUES (20, 6)")
        .await
        .unwrap();
    let last: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await.unwrap();
    assert_eq!(last, Some(4));
    conn.query_drop("DROP TABLE ai_last_t").await.unwrap();
    server.shutdown().await;
}

/// INSERT ... SELECT draws from the same counter.
#[tokio::test]
async fn insert_select_uses_the_table_counter() {
    let server = TestServer::start("ai_select").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ai_sel_src (v INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_sel_src VALUES (7), (8), (9)")
        .await
        .unwrap();
    conn.query_drop(
        "CREATE TABLE ai_sel_dst (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO ai_sel_dst (v) VALUES (1)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_sel_dst (v) SELECT v FROM ai_sel_src")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ai_sel_dst (v) VALUES (2)")
        .await
        .unwrap();
    assert_eq!(ids(&mut conn, "ai_sel_dst").await, vec![1, 2, 3, 4, 5]);
    conn.query_drop("DROP TABLE ai_sel_src, ai_sel_dst")
        .await
        .unwrap();
    server.shutdown().await;
}

/// Sessions inserting at once get distinct, gap-free values.
#[tokio::test]
async fn concurrent_sessions_share_the_counter() {
    let server = TestServer::start("ai_concurrent").await;
    let mut conn = server.connect().await;
    conn.query_drop(
        "CREATE TABLE ai_conc_t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for worker in 0..8 {
        let mut conn = server.connect().await;
        tasks.push(tokio::spawn(async move {
            for n in 0..25 {
                conn.query_drop(format!(
                    "INSERT INTO ai_conc_t (v) VALUES ({})",
                    worker * 100 + n
                ))
                .await
                .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let expected: Vec<i64> = (1..=200).collect();
    assert_eq!(ids(&mut conn, "ai_conc_t").await, expected);
    conn.query_drop("DROP TABLE ai_conc_t").await.unwrap();
    server.shutdown().await;
}
