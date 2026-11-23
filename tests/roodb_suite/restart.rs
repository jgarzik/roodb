//! Data written before a server restart must be fully usable after it,
//! including from inside explicit transactions.

use std::path::PathBuf;
use std::sync::Arc;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};
use parking_lot::RwLock;

use roodb::catalog::Catalog;
use roodb::io::{IoBackend, IoEngine};
use roodb::raft::{Durability, LogStore, LogStoreConfig};
use roodb::server::listener::{start_test_server, ServerHandle};
use roodb::storage::{LsmConfig, LsmEngine, StorageEngine};

use crate::test_utils;

fn data_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "roodb_restart_test_{}_{}",
        name,
        std::process::id()
    ));
    path
}

/// Open the data directory and serve it on a fresh port.
async fn start(dir: &PathBuf, first_boot: bool) -> (ServerHandle, Arc<dyn StorageEngine>, u16) {
    let io = IoEngine::new(IoBackend::from_env().unwrap()).unwrap();
    let log_store = LogStore::open(
        io.clone(),
        LogStoreConfig::for_data_dir(dir, Durability::Os),
    )
    .await
    .expect("open log");
    let config = LsmConfig {
        dir: dir.clone(),
        ..Default::default()
    };
    let storage: Arc<dyn StorageEngine> =
        Arc::new(LsmEngine::open(io, config).await.expect("open storage"));
    if first_boot {
        test_utils::auth::initialize_root_user(&storage, "").await;
    }
    let catalog = Arc::new(RwLock::new(Catalog::with_system_tables()));

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let port = std_listener.local_addr().unwrap().port();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    let handle = start_test_server(
        listener,
        test_utils::certs::test_tls_config(),
        test_utils::certs::test_raft_tls_config(),
        storage.clone(),
        catalog,
        log_store,
    )
    .await
    .expect("start server");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    crate::test_utils::create_test_database_on(port)
        .await
        .expect("create the test database");
    (handle, storage, port)
}

async fn connect(port: u16) -> Conn {
    let opts: Opts = OptsBuilder::default()
        .db_name(Some("test"))
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
        .max_allowed_packet(Some(16_777_216))
        .wait_timeout(Some(28800))
        .into();
    Conn::new(opts).await.expect("connect")
}

#[tokio::test]
async fn test_rows_survive_restart_and_are_visible_in_transactions() {
    let dir = data_dir("txn_visibility");
    let _ = std::fs::remove_dir_all(&dir);

    // First boot: create a table and write rows in many separate
    // transactions, so their transaction IDs run well past 1.
    let (handle, storage, port) = start(&dir, true).await;
    let mut conn = connect(port).await;
    conn.query_drop("CREATE TABLE restart_t (id INT PRIMARY KEY, v INT)")
        .await
        .expect("CREATE TABLE");
    for id in 0..40 {
        conn.query_drop(format!("INSERT INTO restart_t VALUES ({id}, {id})"))
            .await
            .expect("INSERT");
    }
    conn.query_drop("UPDATE restart_t SET v = v + 100 WHERE id < 10")
        .await
        .expect("UPDATE");
    let before: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM restart_t ORDER BY id")
        .await
        .expect("SELECT");
    assert_eq!(before.len(), 40);
    drop(conn);

    handle.shutdown().await;
    storage.close().await.expect("close storage");
    drop(storage);

    // Second boot on the same directory.
    let (handle, storage, port) = start(&dir, false).await;
    let mut conn = connect(port).await;

    // Autocommit read
    let after: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM restart_t ORDER BY id")
        .await
        .expect("SELECT after restart");
    assert_eq!(after, before, "rows changed across the restart");

    // The same read inside an explicit transaction: the first transaction
    // of the new process must see everything committed before it started.
    conn.query_drop("BEGIN").await.expect("BEGIN");
    let in_txn: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM restart_t ORDER BY id")
        .await
        .expect("SELECT in transaction");
    assert_eq!(
        in_txn, before,
        "rows written before the restart are not visible inside a transaction"
    );

    // Writes in that transaction act on the old rows and commit.
    conn.query_drop("UPDATE restart_t SET v = -1 WHERE id >= 30")
        .await
        .expect("UPDATE in transaction");
    conn.query_drop("DELETE FROM restart_t WHERE id < 5")
        .await
        .expect("DELETE in transaction");
    conn.query_drop("COMMIT").await.expect("COMMIT");

    let changed: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM restart_t ORDER BY id")
        .await
        .expect("SELECT after changes");
    assert_eq!(changed.len(), 35, "five rows deleted");
    assert_eq!(changed[0], (5, 105));
    assert!(changed
        .iter()
        .filter(|(id, _)| *id >= 30)
        .all(|(_, v)| *v == -1));

    // New rows and a duplicate-key check against a pre-restart row
    conn.query_drop("INSERT INTO restart_t VALUES (1000, 1)")
        .await
        .expect("INSERT after restart");
    let duplicate = conn
        .query_drop("INSERT INTO restart_t VALUES (20, 0)")
        .await;
    assert!(
        duplicate.is_err(),
        "pre-restart primary key must still conflict"
    );

    drop(conn);
    handle.shutdown().await;
    storage.close().await.expect("close storage");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Everything the catalog holds, and every counter that names new rows, has
/// to pick up where it left off - across more than one restart.
#[tokio::test]
async fn test_schema_and_generated_ids_survive_restarts() {
    let dir = data_dir("schema_and_ids");
    let _ = std::fs::remove_dir_all(&dir);

    let (handle, storage, port) = start(&dir, true).await;
    let mut conn = connect(port).await;
    conn.query_drop("CREATE TABLE with_pk (id INT PRIMARY KEY, name VARCHAR(50))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE no_pk (a INT, b VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE auto_t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("CREATE INDEX idx_name ON with_pk (name)")
        .await
        .unwrap();
    conn.query_drop("CREATE VIEW pk_names AS SELECT name FROM with_pk")
        .await
        .unwrap();
    conn.query_drop("CREATE DATABASE restart_db").await.unwrap();
    conn.query_drop("INSERT INTO with_pk VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();
    for i in 0..20 {
        conn.query_drop(format!("INSERT INTO no_pk VALUES ({i}, 'before')"))
            .await
            .unwrap();
        conn.query_drop(format!("INSERT INTO auto_t (v) VALUES ('before{i}')"))
            .await
            .unwrap();
    }
    let auto_before: Vec<i64> = conn
        .query("SELECT id FROM auto_t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(auto_before.len(), 20);
    drop(conn);
    handle.shutdown().await;
    storage.close().await.unwrap();
    drop(storage);

    for round in 0..2 {
        let (handle, storage, port) = start(&dir, false).await;
        let mut conn = connect(port).await;

        // The schema is all there.
        let names: Vec<String> = conn
            .query("SELECT name FROM pk_names ORDER BY name")
            .await
            .unwrap();
        assert_eq!(names, ["one", "two"], "round {round}: view over a table");
        let databases: Vec<String> = conn.query("SHOW DATABASES").await.unwrap();
        assert!(
            databases.iter().any(|d| d == "restart_db"),
            "round {round}: {databases:?}"
        );

        // Rows without a primary key are keyed by a generated ID. New ones
        // must not land on old ones.
        let before: i64 = conn
            .query_first("SELECT COUNT(*) FROM no_pk")
            .await
            .unwrap()
            .unwrap();
        for i in 0..20 {
            conn.query_drop(format!("INSERT INTO no_pk VALUES ({i}, 'round{round}')"))
                .await
                .unwrap();
        }
        let after: i64 = conn
            .query_first("SELECT COUNT(*) FROM no_pk")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after, before + 20, "round {round}: rows were overwritten");

        // AUTO_INCREMENT continues above every value handed out so far.
        let highest: i64 = conn
            .query_first("SELECT MAX(id) FROM auto_t")
            .await
            .unwrap()
            .unwrap();
        conn.query_drop(format!("INSERT INTO auto_t (v) VALUES ('round{round}')"))
            .await
            .unwrap();
        let newest: i64 = conn
            .query_first(format!("SELECT id FROM auto_t WHERE v = 'round{round}'"))
            .await
            .unwrap()
            .unwrap();
        assert!(newest > highest, "round {round}: {newest} <= {highest}");

        // New schema objects get their own rows in the system tables; they
        // must not replace the rows of existing objects.
        conn.query_drop(format!(
            "CREATE TABLE made_in_round{round} (id INT PRIMARY KEY, x INT)"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "INSERT INTO made_in_round{round} VALUES (1, {round})"
        ))
        .await
        .unwrap();
        let names: Vec<String> = conn
            .query("SELECT name FROM with_pk ORDER BY id")
            .await
            .unwrap();
        assert_eq!(
            names,
            ["one", "two"],
            "round {round}: existing table damaged"
        );
        let auto_now: Vec<i64> = conn
            .query("SELECT id FROM auto_t ORDER BY id")
            .await
            .unwrap();
        assert_eq!(&auto_now[..20], &auto_before[..], "round {round}");

        drop(conn);
        handle.shutdown().await;
        storage.close().await.unwrap();
        drop(storage);
    }

    // One more start: everything made along the way is still there.
    let (handle, storage, port) = start(&dir, false).await;
    let mut conn = connect(port).await;
    for round in 0..2 {
        let x: i64 = conn
            .query_first(format!("SELECT x FROM made_in_round{round} WHERE id = 1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x, round);
    }
    let rows: i64 = conn
        .query_first("SELECT COUNT(*) FROM no_pk")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows, 60);
    drop(conn);
    handle.shutdown().await;
    storage.close().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
