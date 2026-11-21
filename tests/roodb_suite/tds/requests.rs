//! Request-level protocol behaviour, exercised with hand-built messages.

use std::time::Duration;

use super::raw::*;
use crate::roodb_suite::harness::TestServer;

const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

#[tokio::test]
async fn test_raw_login_and_batch() {
    let server = TestServer::start("tds_raw_login").await;
    let mut client = RawTds::connect(&server).await;
    let tokens = client.login("root", "", 0x7400_0004).await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert!(
        tokens.contains(&Token::LoginAck {
            tds_version: [0x74, 0x00, 0x00, 0x04]
        }),
        "LOGINACK must report TDS 7.4: {tokens:?}"
    );
    assert_eq!(final_done(&tokens), 0);

    let tokens = client.batch("DROP TABLE IF EXISTS tds_raw_none").await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    server.shutdown().await;
}

/// Drivers on a strict connection send TDS version 8.0 in LOGIN7 and
/// still get LOGINACK 7.4.
#[tokio::test]
async fn test_login_with_tds_8_version_number() {
    let server = TestServer::start("tds_raw_v8").await;
    let mut client = RawTds::connect(&server).await;
    let tokens = client.login("root", "", 0x0800_0000).await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert!(tokens.contains(&Token::LoginAck {
        tds_version: [0x74, 0x00, 0x00, 0x04]
    }));

    // An older protocol version is refused like any other failed login.
    let mut client = RawTds::connect(&server).await;
    let tokens = client.login("root", "", 0x7200_0902).await;
    assert_eq!(errors(&tokens), vec![18456], "{tokens:?}");
    server.shutdown().await;
}

/// A very long user name is echoed in the login error; the response must
/// stay well-formed.
#[tokio::test]
async fn test_long_username_gets_a_well_formed_error() {
    let server = TestServer::start("tds_raw_longuser").await;
    let mut client = RawTds::connect(&server).await;
    let user = "u".repeat(30_000);
    let tokens = client.login(&user, "x", 0x7400_0004).await;
    assert_eq!(errors(&tokens), vec![18456], "{:?}", tokens.last());
    assert_eq!(final_done(&tokens) & DONE_ERROR, DONE_ERROR);
    server.shutdown().await;
}

/// When the server gives up on a session it closes the socket; the client
/// must not be left waiting.
#[tokio::test]
async fn test_protocol_violation_closes_the_connection() {
    let server = TestServer::start("tds_raw_violation").await;

    // A message type that is not a request
    let mut client = RawTds::login_root(&server).await;
    client.send(PKT_BULK_LOAD, STATUS_EOM, b"junk").await;
    assert!(
        client.closed_within(Duration::from_secs(3)).await,
        "bulk load packet"
    );

    // A SQL batch without its header block
    let mut client = RawTds::login_root(&server).await;
    client.send(PKT_SQL_BATCH, STATUS_EOM, &[1, 2]).await;
    assert!(
        client.closed_within(Duration::from_secs(3)).await,
        "truncated ALL_HEADERS"
    );

    // SQL text with an odd number of bytes
    let mut client = RawTds::login_root(&server).await;
    let mut payload = all_headers();
    payload.extend_from_slice(&[b'S', 0, b'E']);
    client.send(PKT_SQL_BATCH, STATUS_EOM, &payload).await;
    assert!(
        client.closed_within(Duration::from_secs(3)).await,
        "odd-length SQL text"
    );

    // Garbage instead of a packet header
    let mut client = RawTds::login_root(&server).await;
    client.send_bytes(&[0xEE; 16]).await;
    assert!(
        client.closed_within(Duration::from_secs(3)).await,
        "unknown packet type"
    );

    server.shutdown().await;
}

/// An attention signal with nothing running is acknowledged.
#[tokio::test]
async fn test_idle_attention_is_acknowledged() {
    let server = TestServer::start("tds_raw_attn").await;
    let mut client = RawTds::login_root(&server).await;

    client.send(PKT_ATTENTION, STATUS_EOM, &[]).await;
    let tokens = client.response().await;
    assert_eq!(final_done(&tokens) & DONE_ATTN, DONE_ATTN, "{tokens:?}");

    // The session carries on.
    let tokens = client.batch("DROP TABLE IF EXISTS tds_raw_none").await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(final_done(&tokens) & DONE_ATTN, 0);
    server.shutdown().await;
}

/// Transaction manager requests, as drivers send for BeginTransaction /
/// Commit / Rollback.
#[tokio::test]
async fn test_transaction_manager_requests() {
    let server = TestServer::start("tds_raw_tm").await;
    let mut client = RawTds::login_root(&server).await;
    for sql in [
        "DROP TABLE IF EXISTS tds_raw_tm_t",
        "CREATE TABLE tds_raw_tm_t (id INT PRIMARY KEY)",
    ] {
        let tokens = client.batch(sql).await;
        assert!(errors(&tokens).is_empty(), "{sql}: {tokens:?}");
    }

    // BEGIN: isolation level 0 (unchanged), empty name
    let tokens = client.transaction_request(TM_BEGIN_XACT, &[0, 0]).await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(env_changes(&tokens), vec![ENV_BEGIN_TRAN]);
    let descriptor = tokens
        .iter()
        .find_map(|t| match t {
            Token::EnvChange { kind, new_value } if *kind == ENV_BEGIN_TRAN => {
                Some(new_value.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(descriptor.len(), 8, "transaction descriptor is 8 bytes");

    client.batch("INSERT INTO tds_raw_tm_t VALUES (1)").await;

    // ROLLBACK: empty name, no flags
    let tokens = client.transaction_request(TM_ROLLBACK_XACT, &[0, 0]).await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(env_changes(&tokens), vec![ENV_ROLLBACK_TRAN]);

    // BEGIN, insert, COMMIT with fBeginXact: commits and opens the next
    client.transaction_request(TM_BEGIN_XACT, &[0, 0]).await;
    client.batch("INSERT INTO tds_raw_tm_t VALUES (2)").await;
    let tokens = client
        .transaction_request(TM_COMMIT_XACT, &[0, 1, 0, 0])
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(
        env_changes(&tokens),
        vec![ENV_COMMIT_TRAN, ENV_BEGIN_TRAN],
        "{tokens:?}"
    );

    // A named rollback is a rollback to a savepoint, which is not
    // supported: it must fail and leave the transaction open, not roll
    // everything back.
    client.batch("INSERT INTO tds_raw_tm_t VALUES (3)").await;
    let mut named = vec![3];
    named.extend_from_slice(&ucs2("sp1"));
    named.push(0);
    let tokens = client.transaction_request(TM_ROLLBACK_XACT, &named).await;
    assert_eq!(errors(&tokens).len(), 1, "{tokens:?}");
    assert!(env_changes(&tokens).is_empty(), "{tokens:?}");

    let tokens = client.transaction_request(TM_COMMIT_XACT, &[0, 0]).await;
    assert_eq!(env_changes(&tokens), vec![ENV_COMMIT_TRAN], "{tokens:?}");

    // Rows 2 and 3 were committed; row 1 was rolled back.
    for (id, expect_duplicate) in [(1, false), (2, true), (3, true)] {
        let tokens = client
            .batch(&format!("INSERT INTO tds_raw_tm_t VALUES ({id})"))
            .await;
        assert_eq!(
            !errors(&tokens).is_empty(),
            expect_duplicate,
            "row {id}: {tokens:?}"
        );
    }

    // An unknown request type is refused, not ignored.
    let tokens = client.transaction_request(99, &[]).await;
    assert_eq!(errors(&tokens).len(), 1, "{tokens:?}");

    client.batch("DROP TABLE tds_raw_tm_t").await;
    server.shutdown().await;
}

/// A pooled connection's next user asks for a reset: the previous user's
/// transaction and database must not leak into it.
#[tokio::test]
async fn test_reset_connection() {
    let server = TestServer::start("tds_raw_reset").await;
    let mut client = RawTds::login_root(&server).await;

    // Leave a transaction open and switch database.
    let tokens = client.batch("USE test").await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    let tokens = client.batch("BEGIN").await;
    assert_eq!(env_changes(&tokens), vec![ENV_BEGIN_TRAN], "{tokens:?}");

    // Reset keeping the transaction: it is still open, so this BEGIN only
    // opens a nested level; no transaction begins or ends.
    let tokens = client
        .batch_with_status("BEGIN", STATUS_RESET_CONNECTION_SKIP_TRAN)
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(
        env_changes(&tokens),
        vec![ENV_RESET_CONNECTION],
        "{tokens:?}"
    );

    // Full reset: the open transaction is rolled back first, so BEGIN
    // succeeds, and the reset is acknowledged before anything else.
    let tokens = client
        .batch_with_status("BEGIN", STATUS_RESET_CONNECTION)
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    let changes = env_changes(&tokens);
    assert_eq!(changes[0], ENV_RESET_CONNECTION, "{tokens:?}");
    assert!(changes.contains(&ENV_BEGIN_TRAN), "{tokens:?}");

    client.batch("ROLLBACK").await;
    server.shutdown().await;
}
