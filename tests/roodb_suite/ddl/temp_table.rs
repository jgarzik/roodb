//! Temporary tables: owned by one session, invisible to the rest.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

/// Another session cannot write to, or drop, a temporary table it does not
/// own; the owner can drop it alone or among other tables.
#[tokio::test]
async fn test_temporary_table_belongs_to_its_session() {
    let server = TestServer::start("temp_table").await;
    let mut owner = server.connect().await;
    let mut other = server.connect().await;

    owner
        .query_drop("CREATE TEMPORARY TABLE tt_owned (id INT PRIMARY KEY, v INT)")
        .await
        .expect("CREATE TEMPORARY TABLE");
    owner
        .query_drop("CREATE TABLE tt_real (id INT PRIMARY KEY)")
        .await
        .expect("CREATE TABLE");
    owner
        .query_drop("INSERT INTO tt_owned VALUES (1, 10), (2, 20)")
        .await
        .expect("owner insert");

    // To the other session the table does not exist, whatever it tries.
    for sql in [
        "SELECT id, v FROM tt_owned",
        "UPDATE tt_owned SET v = 0",
        "DELETE FROM tt_owned",
        "CREATE INDEX tt_owned_v ON tt_owned (v)",
        "ALTER TABLE tt_owned ADD COLUMN extra INT",
        "CREATE TRIGGER tt_owned_trg AFTER INSERT ON tt_owned FOR EACH ROW \
         INSERT INTO tt_real VALUES (NEW.id)",
    ] {
        assert!(
            other.query_drop(sql).await.is_err(),
            "another session ran: {sql}"
        );
    }
    // Nor does its owner get a stored trigger on it.
    assert!(owner
        .query_drop(
            "CREATE TRIGGER tt_owned_trg AFTER INSERT ON tt_owned FOR EACH ROW \
             INSERT INTO tt_real VALUES (NEW.id)"
        )
        .await
        .is_err());
    assert!(
        other
            .query_drop("INSERT INTO tt_owned VALUES (3, 30)")
            .await
            .is_err(),
        "another session wrote to a temporary table"
    );
    assert!(
        other.query_drop("DROP TABLE tt_owned").await.is_err(),
        "another session dropped a temporary table"
    );
    assert!(
        other
            .query_drop("DROP TABLE tt_real, tt_owned")
            .await
            .is_err(),
        "another session dropped a temporary table in a list"
    );

    // Nothing the other session tried took effect.
    let rows: Vec<(i32, i32)> = owner
        .query("SELECT id, v FROM tt_owned ORDER BY id")
        .await
        .expect("owner select");
    assert_eq!(rows, [(1, 10), (2, 20)]);
    let real: Vec<i32> = owner.query("SELECT id FROM tt_real").await.expect("real");
    assert!(real.is_empty());

    // The owner drops it together with an ordinary table.
    owner
        .query_drop("DROP TABLE tt_owned, tt_real")
        .await
        .expect("owner drop");
    assert!(owner.query_drop("SELECT * FROM tt_owned").await.is_err());
    assert!(owner.query_drop("SELECT * FROM tt_real").await.is_err());

    // The name is free again, for an ordinary table that starts empty.
    other
        .query_drop("CREATE TABLE tt_owned (id INT PRIMARY KEY, v INT)")
        .await
        .expect("reuse name");
    let rows: Vec<(i32, i32)> = other.query("SELECT id, v FROM tt_owned").await.unwrap();
    assert!(rows.is_empty());
    other.query_drop("DROP TABLE tt_owned").await.unwrap();
}

/// A session's temporary tables vanish when it disconnects.
#[tokio::test]
async fn test_temporary_table_is_dropped_on_disconnect() {
    let server = TestServer::start("temp_table").await;
    {
        let mut conn = server.connect().await;
        conn.query_drop("CREATE TEMPORARY TABLE tt_gone (id INT PRIMARY KEY)")
            .await
            .unwrap();
        conn.query_drop("INSERT INTO tt_gone VALUES (1)")
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    }
    let mut conn = server.connect().await;
    // The disconnect is handled asynchronously by the server.
    let mut created = false;
    for _ in 0..100 {
        if conn
            .query_drop("CREATE TABLE tt_gone (id INT PRIMARY KEY)")
            .await
            .is_ok()
        {
            created = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(created, "the temporary table outlived its session");
    let rows: Vec<i32> = conn.query("SELECT id FROM tt_gone").await.unwrap();
    assert!(rows.is_empty());
    conn.query_drop("DROP TABLE tt_gone").await.unwrap();
}

/// A trigger on a system table would fire on every schema change.
#[tokio::test]
async fn test_trigger_on_system_table_is_refused() {
    let server = TestServer::start("temp_table").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE tt_trg_log (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let result = conn
        .query_drop(
            "CREATE TRIGGER tt_sys_trg AFTER INSERT ON system.tables FOR EACH ROW \
             INSERT INTO tt_trg_log VALUES (1)",
        )
        .await;
    assert!(result.is_err(), "a trigger was created on a system table");
    conn.query_drop("DROP TABLE tt_trg_log").await.unwrap();
}
