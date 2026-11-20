//! SQL batches over TDS.

use super::{connect, TRANSPORTS};
use crate::roodb_suite::harness::TestServer;

#[tokio::test]
async fn test_ddl_dml_and_rows() {
    let server = TestServer::start("tds_query").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_q_{name}");

        client
            .simple_query(format!("DROP TABLE IF EXISTS {table}"))
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();
        client
            .simple_query(format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(50), score DOUBLE)"
            ))
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();

        let inserted = client
            .execute(
                format!(
                    "INSERT INTO {table} VALUES (1, 'alice', 1.5), (2, 'b\u{f6}b', NULL), \
                     (3, NULL, -2.25)"
                ),
                &[],
            )
            .await;
        // `execute` uses an RPC, which may be refused; fall back to a batch.
        if inserted.is_err() {
            client
                .simple_query(format!(
                    "INSERT INTO {table} VALUES (1, 'alice', 1.5), (2, 'b\u{f6}b', NULL), \
                     (3, NULL, -2.25)"
                ))
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
        }

        let rows = client
            .simple_query(format!("SELECT id, name, score FROM {table} ORDER BY id"))
            .await
            .unwrap_or_else(|e| panic!("{name}: select failed: {e}"))
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.len(), 3, "{name}");
        assert_eq!(rows[0].get::<i32, _>(0), Some(1));
        assert_eq!(rows[0].get::<&str, _>(1), Some("alice"));
        assert_eq!(rows[0].get::<f64, _>(2), Some(1.5));
        assert_eq!(rows[1].get::<&str, _>("name"), Some("b\u{f6}b"));
        assert_eq!(rows[1].get::<f64, _>(2), None);
        assert_eq!(rows[2].get::<&str, _>(1), None);
        assert_eq!(rows[2].get::<f64, _>(2), Some(-2.25));

        client
            .simple_query(format!("DROP TABLE {table}"))
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();
    }
    server.shutdown().await;
}

/// A failing statement returns a SQL Server style error and the session
/// carries on.
#[tokio::test]
async fn test_error_keeps_the_connection() {
    let server = TestServer::start("tds_error").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;

        let err = match client.simple_query("SELECT * FROM tds_no_such_table").await {
            Ok(stream) => stream
                .into_results()
                .await
                .expect_err("select from a missing table must fail"),
            Err(e) => e,
        };
        match err {
            tiberius::error::Error::Server(e) => {
                assert_eq!(e.code(), 208, "{name}: invalid object name, got {e:?}");
            }
            other => panic!("{name}: expected a server error, got: {other}"),
        }

        let row = client
            .simple_query("SELECT 42")
            .await
            .unwrap_or_else(|e| panic!("{name}: connection must survive an error: {e}"))
            .into_row()
            .await
            .unwrap();
        assert!(row.is_some(), "{name}");
    }
    server.shutdown().await;
}

/// A result set far larger than one packet arrives complete.
#[tokio::test]
async fn test_large_result_set_is_streamed() {
    let server = TestServer::start("tds_large").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_big_{name}");
        for sql in [
            format!("DROP TABLE IF EXISTS {table}"),
            format!("CREATE TABLE {table} (id INT PRIMARY KEY, pad VARCHAR(200))"),
        ] {
            client
                .simple_query(sql)
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
        }
        let pad = "x".repeat(180);
        for chunk in 0..10 {
            let values: Vec<String> = (0..200)
                .map(|i| format!("({}, '{pad}')", chunk * 200 + i))
                .collect();
            client
                .simple_query(format!("INSERT INTO {table} VALUES {}", values.join(",")))
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
        }

        let rows = client
            .simple_query(format!("SELECT id, pad FROM {table} ORDER BY id"))
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap_or_else(|e| panic!("{name}: reading 2000 rows failed: {e}"));
        assert_eq!(rows.len(), 2000, "{name}");
        assert_eq!(rows[1999].get::<i32, _>(0), Some(1999));
        assert_eq!(rows[1999].get::<&str, _>(1), Some(pad.as_str()));

        client
            .simple_query(format!("DROP TABLE {table}"))
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();
    }
    server.shutdown().await;
}

/// Transactions through SQL text: rollback discards, commit keeps.
#[tokio::test]
async fn test_transactions() {
    let server = TestServer::start("tds_txn").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_txn_{name}");
        let run = |sql: String| sql;
        for sql in [
            format!("DROP TABLE IF EXISTS {table}"),
            format!("CREATE TABLE {table} (id INT PRIMARY KEY)"),
            run("BEGIN".into()),
            format!("INSERT INTO {table} VALUES (1)"),
            run("ROLLBACK".into()),
            run("BEGIN".into()),
            format!("INSERT INTO {table} VALUES (2)"),
            run("COMMIT".into()),
        ] {
            client
                .simple_query(sql.clone())
                .await
                .unwrap_or_else(|e| panic!("{name}: `{sql}` failed: {e}"))
                .into_results()
                .await
                .unwrap_or_else(|e| panic!("{name}: `{sql}` failed: {e}"));
        }

        let rows = client
            .simple_query(format!("SELECT id FROM {table}"))
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "{name}");
        assert_eq!(rows[0].get::<i32, _>(0), Some(2));

        client
            .simple_query(format!("DROP TABLE {table}"))
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();
    }
    server.shutdown().await;
}
