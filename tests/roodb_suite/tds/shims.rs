//! What drivers send on their own: batches of several statements, T-SQL
//! transaction statements, session options and probes. Also cancelling a
//! running query.

use std::time::Duration;

use super::raw::*;
use super::{connect, TdsClient, TRANSPORTS};
use crate::roodb_suite::harness::TestServer;

async fn run(client: &mut TdsClient, sql: &str) {
    client
        .simple_query(sql.to_string())
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        .into_results()
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// Whether running `sql` reports an error.
async fn fails(client: &mut TdsClient, sql: &str) -> bool {
    match client.simple_query(sql.to_string()).await {
        Ok(stream) => stream.into_results().await.is_err(),
        Err(_) => true,
    }
}

async fn count(client: &mut TdsClient, table: &str) -> usize {
    client
        .simple_query(format!("SELECT id FROM {table}"))
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn test_multi_statement_batches() {
    let server = TestServer::start("tds_shim_batch").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_shim_b_{name}");

        // DDL, DML and two result sets in one batch
        let results = client
            .simple_query(format!(
                "DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(20)); \
                 INSERT INTO {table} VALUES (1, 'a;b'), (2, 'c'); \
                 SELECT id FROM {table} ORDER BY id; \
                 SELECT v FROM {table} WHERE id = 1;"
            ))
            .await
            .unwrap_or_else(|e| panic!("{name}: batch failed: {e}"))
            .into_results()
            .await
            .unwrap_or_else(|e| panic!("{name}: batch failed: {e}"));
        let with_rows: Vec<_> = results.iter().filter(|r| !r.is_empty()).collect();
        assert_eq!(with_rows.len(), 2, "{name}: two result sets");
        assert_eq!(with_rows[0].len(), 2);
        assert_eq!(with_rows[1][0].get::<&str, _>(0), Some("a;b"));

        // The batch stops at the first failing statement.
        let batch = format!(
            "INSERT INTO {table} VALUES (3, 'x'); \
             INSERT INTO {table} VALUES (1, 'duplicate'); \
             INSERT INTO {table} VALUES (4, 'never')"
        );
        assert!(
            fails(&mut client, &batch).await,
            "{name}: duplicate key must fail the batch"
        );
        assert_eq!(count(&mut client, &table).await, 3, "{name}: 1, 2 and 3");

        run(&mut client, &format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

#[tokio::test]
async fn test_tsql_transaction_statements() {
    let server = TestServer::start("tds_shim_txn").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_shim_t_{name}");
        run(&mut client, &format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY)"),
        )
        .await;

        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (1)")).await;
        run(&mut client, "ROLLBACK TRAN").await;
        assert_eq!(count(&mut client, &table).await, 0, "{name}");

        run(&mut client, "BEGIN TRANSACTION").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (2)")).await;
        run(&mut client, "COMMIT TRANSACTION").await;
        assert_eq!(count(&mut client, &table).await, 1, "{name}");

        // Guarded end of transaction: a no-op outside one, effective inside
        run(&mut client, "IF @@TRANCOUNT > 0 ROLLBACK TRAN").await;
        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (3)")).await;
        run(&mut client, "IF @@TRANCOUNT > 0 ROLLBACK TRAN").await;
        assert_eq!(count(&mut client, &table).await, 1, "{name}");

        // Implicit transactions: statements open a transaction on their own
        run(&mut client, "SET IMPLICIT_TRANSACTIONS ON").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (4)")).await;
        run(&mut client, "ROLLBACK").await;
        run(&mut client, "SET IMPLICIT_TRANSACTIONS OFF").await;
        assert_eq!(count(&mut client, &table).await, 1, "{name}");

        // Savepoints are refused rather than turned into a full rollback
        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (5)")).await;
        assert!(
            fails(&mut client, "ROLLBACK TRAN sp1").await,
            "{name}: named rollback must be refused"
        );
        run(&mut client, "COMMIT TRAN").await;
        assert_eq!(count(&mut client, &table).await, 2, "{name}: row 5 kept");

        run(&mut client, &format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

#[tokio::test]
async fn test_session_options_and_probes() {
    let server = TestServer::start("tds_shim_probe").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;

        // The option block drivers send after login
        run(
            &mut client,
            "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; \
             SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; \
             SET QUOTED_IDENTIFIER ON; SET NOCOUNT OFF; SET XACT_ABORT OFF; \
             SET TEXTSIZE 2147483647; SET LOCK_TIMEOUT -1; \
             SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .await;

        let row = client
            .simple_query(
                "SELECT @@SPID, @@TRANCOUNT, DB_NAME(), SUSER_SNAME(), \
                 SERVERPROPERTY('ProductVersion'), SERVERPROPERTY('EngineEdition')",
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: probes failed: {e}"))
            .into_row()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.len(), 6, "{name}");
        let cells: Vec<String> = row.cells().map(|(_, data)| format!("{data:?}")).collect();
        assert!(
            cells[2].contains("default"),
            "{name}: DB_NAME() = {}",
            cells[2]
        );
        assert!(
            cells[3].contains("root"),
            "{name}: SUSER_SNAME() = {}",
            cells[3]
        );
        assert!(cells[4].contains("16.0"), "{name}: version = {}", cells[4]);

        // @@TRANCOUNT follows the transaction state
        run(&mut client, "BEGIN TRAN").await;
        let row = client
            .simple_query("SELECT @@TRANCOUNT")
            .await
            .unwrap()
            .into_row()
            .await
            .unwrap()
            .unwrap();
        let in_txn = format!("{:?}", row.cells().next().unwrap().1);
        assert!(
            in_txn.contains('1'),
            "{name}: @@TRANCOUNT in txn = {in_txn}"
        );
        run(&mut client, "ROLLBACK").await;

        // The server version string names RooDB
        let row = client
            .simple_query("SELECT @@VERSION")
            .await
            .unwrap_or_else(|e| panic!("{name}: @@VERSION failed: {e}"))
            .into_row()
            .await
            .unwrap()
            .unwrap();
        let version = format!("{:?}", row.cells().next().unwrap().1);
        assert!(version.contains("RooDB"), "{name}: @@VERSION = {version}");

        // A parameterised statement may use probes too
        let rows = client
            .query("SELECT @@SPID, @P1", &[&7i32])
            .await
            .unwrap_or_else(|e| panic!("{name}: parameterised probe failed: {e}"))
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "{name}");
    }
    server.shutdown().await;
}

/// The client cancels a query that is still producing rows: the response
/// ends with DONE(attention) and the session carries on.
#[tokio::test]
async fn test_attention_cancels_a_running_query() {
    let server = TestServer::start("tds_shim_cancel").await;
    let mut client = RawTds::login_root(&server).await;
    for sql in [
        "DROP TABLE IF EXISTS tds_cancel_t".to_string(),
        "CREATE TABLE tds_cancel_t (id INT PRIMARY KEY)".to_string(),
        format!(
            "INSERT INTO tds_cancel_t VALUES {}",
            (0..300)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ] {
        let tokens = client.batch(&sql).await;
        assert!(errors(&tokens).is_empty(), "{tokens:?}");
    }

    // 27 million rows if left to run
    let mut payload = all_headers();
    payload.extend_from_slice(&ucs2(
        "SELECT a.id FROM tds_cancel_t a, tds_cancel_t b, tds_cancel_t c",
    ));
    client.send(PKT_SQL_BATCH, STATUS_EOM, &payload).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.send(PKT_ATTENTION, STATUS_EOM, &[]).await;

    let received = tokio::time::timeout(Duration::from_secs(30), client.recv())
        .await
        .expect("cancelled query must end promptly")
        .expect("connection stays open");
    let (kind, tokens) = received;
    assert_eq!(kind, PKT_TABULAR_RESULT);
    // The response ends with DONE carrying the attention acknowledgement.
    let done = &tokens[tokens.len() - 13..];
    assert_eq!(done[0], 0xFD, "last token is DONE");
    let status = u16::from_le_bytes([done[1], done[2]]);
    assert_eq!(status & DONE_ATTN, DONE_ATTN, "status {status:#06x}");
    // Far fewer than 27 million 5-byte rows were sent.
    assert!(tokens.len() < 100 * 1024 * 1024, "{} bytes", tokens.len());

    // The session is in sync afterwards.
    let tokens = client.batch("DROP TABLE tds_cancel_t").await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(final_done(&tokens) & DONE_ATTN, 0);
    server.shutdown().await;
}

/// T-SQL transactions nest: an inner BEGIN TRAN adds a level, its COMMIT
/// takes the level away, only the outermost COMMIT commits, and a ROLLBACK
/// at any level undoes everything.
#[tokio::test]
async fn test_nested_transactions() {
    let server = TestServer::start("tds_shim_nest").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let mut other = connect(&server, encryption).await;
        let table = format!("tds_shim_n_{name}");
        run(&mut client, &format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY)"),
        )
        .await;
        let trancount =
            |rows: Vec<tiberius::Row>| format!("{:?}", rows[0].cells().next().unwrap().1);
        async fn depth(client: &mut TdsClient) -> Vec<tiberius::Row> {
            client
                .simple_query("SELECT @@TRANCOUNT".to_string())
                .await
                .unwrap()
                .into_first_result()
                .await
                .unwrap()
        }

        assert!(trancount(depth(&mut client).await).contains('0'), "{name}");
        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (1)")).await;
        run(&mut client, "BEGIN TRAN").await;
        assert!(trancount(depth(&mut client).await).contains('2'), "{name}");
        run(&mut client, &format!("INSERT INTO {table} VALUES (2)")).await;
        // The inner COMMIT commits nothing.
        run(&mut client, "COMMIT TRAN").await;
        assert!(trancount(depth(&mut client).await).contains('1'), "{name}");
        assert_eq!(count(&mut other, &table).await, 0, "{name}");
        // The outer one commits both rows.
        run(&mut client, "COMMIT TRAN").await;
        assert!(trancount(depth(&mut client).await).contains('0'), "{name}");
        assert_eq!(count(&mut other, &table).await, 2, "{name}");

        // A ROLLBACK inside a nested level undoes the outer work too.
        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (3)")).await;
        run(&mut client, "BEGIN TRAN").await;
        run(&mut client, &format!("INSERT INTO {table} VALUES (4)")).await;
        run(&mut client, "ROLLBACK TRAN").await;
        assert!(trancount(depth(&mut client).await).contains('0'), "{name}");
        assert_eq!(count(&mut client, &table).await, 2, "{name}");

        run(&mut client, &format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}
