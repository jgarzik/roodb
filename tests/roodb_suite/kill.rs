//! `KILL [CONNECTION | QUERY]`, and a client that goes away in the middle
//! of a statement: the statement stops, and the session's transaction and
//! locks are released.

use std::time::Duration;

use std::sync::Arc;

use mysql_async::prelude::*;
use mysql_async::Conn;
use tiberius::EncryptionLevel;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;

use crate::roodb_suite::tds::connect as tds_connect;
use crate::roodb_suite::tds::raw::accept_any_cert_config;
use crate::roodb_suite::TestServer;

/// Far longer than any of these tests needs when the server behaves; a
/// statement still running after it was not interrupted.
const CEILING: Duration = Duration::from_secs(30);

fn error_code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("expected a server error, got {other:?}"),
    }
}

async fn connection_id(conn: &mut Conn) -> u64 {
    conn.query_first("SELECT CONNECTION_ID()")
        .await
        .unwrap()
        .unwrap()
}

/// Repeat `KILL QUERY id` until `running` ends: a KILL QUERY that arrives
/// before the statement starts has nothing to interrupt.
async fn kill_query_until_done<T>(
    killer: &mut Conn,
    id: u64,
    running: tokio::task::JoinHandle<T>,
) -> T {
    let deadline = tokio::time::Instant::now() + CEILING;
    let mut running = running;
    loop {
        killer.query_drop(format!("KILL QUERY {id}")).await.unwrap();
        match tokio::time::timeout(Duration::from_millis(200), &mut running).await {
            Ok(result) => return result.unwrap(),
            Err(_) => assert!(
                tokio::time::Instant::now() < deadline,
                "KILL QUERY did not interrupt the statement"
            ),
        }
    }
}

/// A row in a fresh table, locked by `holder`'s open transaction.
async fn hold_row_lock(holder: &mut Conn, table: &str) {
    holder
        .query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    holder
        .query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .await
        .unwrap();
    holder
        .query_drop(format!("INSERT INTO {table} VALUES (1, 0)"))
        .await
        .unwrap();
    holder.query_drop("BEGIN").await.unwrap();
    holder
        .query_drop(format!("UPDATE {table} SET v = 1 WHERE id = 1"))
        .await
        .unwrap();
}

/// The row lock `hold_row_lock` took is free again: an update gets it well
/// within the lock wait timeout, and sees the holder's change undone.
async fn row_lock_released(server: &TestServer, table: &str) {
    let mut c = server.connect().await;
    c.query_drop("SET SESSION innodb_lock_wait_timeout = 20")
        .await
        .unwrap();
    c.query_drop(format!("UPDATE {table} SET v = v + 10 WHERE id = 1"))
        .await
        .unwrap_or_else(|e| panic!("the row lock was not released: {e}"));
    let v: Option<i32> = c
        .query_first(format!("SELECT v FROM {table} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(v, Some(10), "the killed transaction was rolled back");
    c.query_drop(format!("DROP TABLE {table}")).await.unwrap();
}

/// KILL QUERY interrupts a SLEEP: it returns 1, the statement reports no
/// error (as in MySQL, SLEEP being all it does), and the session carries on.
#[tokio::test]
async fn test_kill_query_interrupts_sleep() {
    let server = TestServer::start("kill_query").await;
    let mut a = server.connect().await;
    let mut b = server.connect().await;
    let id = connection_id(&mut a).await;

    let sleeper = tokio::spawn(async move {
        let slept: mysql_async::Result<Option<i64>> = a.query_first("SELECT SLEEP(120)").await;
        (slept, a)
    });
    let (slept, mut a) = kill_query_until_done(&mut b, id, sleeper).await;
    assert_eq!(slept.unwrap(), Some(1));

    let v: Option<i64> = a.query_first("SELECT 7").await.unwrap();
    assert_eq!(v, Some(7));
    server.shutdown().await;
}

/// KILL QUERY of the session's own connection interrupts the KILL itself.
#[tokio::test]
async fn test_kill_query_self() {
    let server = TestServer::start("kill_self").await;
    let mut a = server.connect().await;
    let id = connection_id(&mut a).await;
    let e = a.query_drop(format!("KILL QUERY {id}")).await.unwrap_err();
    assert_eq!(error_code(&e), 1317, "{e}");
    let v: Option<i64> = a.query_first("SELECT 8").await.unwrap();
    assert_eq!(v, Some(8), "the session carries on");
    server.shutdown().await;
}

/// KILL of a connection that does not exist.
#[tokio::test]
async fn test_kill_unknown_connection() {
    let server = TestServer::start("kill_unknown").await;
    let mut a = server.connect().await;
    for sql in [
        "KILL 4000000000",
        "KILL QUERY 4000000000",
        "KILL CONNECTION 4000000000",
    ] {
        let e = a.query_drop(sql).await.unwrap_err();
        assert_eq!(error_code(&e), 1094, "{sql}: {e}");
    }
    server.shutdown().await;
}

/// KILL ends an idle connection; its id is then unknown.
#[tokio::test]
async fn test_kill_idle_connection() {
    let server = TestServer::start("kill_idle").await;
    let mut a = server.connect().await;
    let mut b = server.connect().await;
    let id = connection_id(&mut a).await;

    b.query_drop(format!("KILL {id}")).await.unwrap();

    let gone = tokio::time::timeout(CEILING, a.query_drop("SELECT 1"))
        .await
        .expect("the killed connection answered nothing");
    assert!(gone.is_err(), "a killed connection still serves statements");

    // The connection is gone from the server, soon if not at once.
    let deadline = tokio::time::Instant::now() + CEILING;
    loop {
        match b.query_drop(format!("KILL {id}")).await {
            Err(e) if error_code(&e) == 1094 => break,
            _ => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the killed connection lingers"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    server.shutdown().await;
}

/// KILL of a connection inside a transaction rolls it back and releases its
/// locks, though it was in the middle of a statement.
#[tokio::test]
async fn test_kill_connection_releases_locks() {
    let server = TestServer::start("kill_locks").await;
    let mut a = server.connect().await;
    let mut b = server.connect().await;
    let id = connection_id(&mut a).await;
    hold_row_lock(&mut a, "kill_locks_t").await;
    let sleeper = tokio::spawn(async move { a.query_drop("SELECT SLEEP(120)").await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    b.query_drop(format!("KILL CONNECTION {id}")).await.unwrap();
    let ended = tokio::time::timeout(CEILING, sleeper)
        .await
        .expect("the killed connection's statement kept running")
        .unwrap();
    assert!(
        ended.is_err(),
        "the killed connection's statement succeeded"
    );
    row_lock_released(&server, "kill_locks_t").await;
    server.shutdown().await;
}

/// A client that goes away during a statement: the statement stops and the
/// session's locks are released, instead of waiting out the statement.
#[tokio::test]
async fn test_disconnect_during_statement_releases_locks() {
    let server = TestServer::start("kill_disconnect").await;
    let mut a = server.connect().await;
    hold_row_lock(&mut a, "kill_disconnect_t").await;
    // Dropping the task drops the connection: the socket closes with the
    // SLEEP still running.
    let sleeper = tokio::spawn(async move { a.query_drop("SELECT SLEEP(120)").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    sleeper.abort();
    let _ = sleeper.await;

    row_lock_released(&server, "kill_disconnect_t").await;
    server.shutdown().await;
}

/// KILL QUERY reaches a TDS session (the connection ids are shared): its
/// SLEEP returns 1, and no attention acknowledgement is sent, since the
/// client asked for none.
#[tokio::test]
async fn test_kill_query_of_a_tds_session() {
    let server = TestServer::start("kill_tds").await;
    let mut tds = tds_connect(&server, EncryptionLevel::Required).await;
    let spid: i64 = tds
        .simple_query("SELECT @@SPID")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    let mut b = server.connect().await;

    let sleeper = tokio::spawn(async move {
        let row = tds
            .simple_query("SELECT SLEEP(120)")
            .await
            .unwrap()
            .into_row()
            .await
            .unwrap()
            .unwrap();
        let slept: i64 = row.get(0).unwrap();
        (slept, tds)
    });
    let (slept, mut tds) = kill_query_until_done(&mut b, spid as u64, sleeper).await;
    assert_eq!(slept, 1);

    let row = tds
        .simple_query("SELECT 9")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<i64, _>(0), Some(9), "the TDS session carries on");
    server.shutdown().await;
}

/// A TDS client that goes away during a statement releases its locks too.
#[tokio::test]
async fn test_tds_disconnect_during_statement_releases_locks() {
    let server = TestServer::start("kill_tds_disconnect").await;
    let mut setup = server.connect().await;
    setup
        .query_drop("DROP TABLE IF EXISTS kill_tds_disc_t")
        .await
        .unwrap();
    setup
        .query_drop("CREATE TABLE kill_tds_disc_t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    setup
        .query_drop("INSERT INTO kill_tds_disc_t VALUES (1, 0)")
        .await
        .unwrap();

    let mut tds = tds_connect(&server, EncryptionLevel::Required).await;
    for sql in [
        "USE test",
        "BEGIN TRAN",
        "UPDATE kill_tds_disc_t SET v = 1 WHERE id = 1",
    ] {
        tds.simple_query(sql)
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let sleeper = tokio::spawn(async move {
        let _ = tds.simple_query("SELECT SLEEP(120)").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    sleeper.abort();
    let _ = sleeper.await;

    row_lock_released(&server, "kill_tds_disc_t").await;
    server.shutdown().await;
}

/// A RooDB-protocol client that sends hand-built commands: `mysql_async`
/// has no call for COM_PROCESS_KILL.
struct RawMysql {
    stream: TlsStream<TcpStream>,
}

async fn read_packet<S: AsyncRead + Unpin>(stream: &mut S) -> (u8, Vec<u8>) {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await.expect("packet header");
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .expect("packet payload");
    (header[3], payload)
}

async fn write_packet<S: AsyncWrite + Unpin>(stream: &mut S, seq: u8, payload: &[u8]) {
    let len = payload.len() as u32;
    let mut packet = len.to_le_bytes()[..3].to_vec();
    packet.push(seq);
    packet.extend_from_slice(payload);
    stream.write_all(&packet).await.expect("write packet");
    stream.flush().await.expect("flush");
}

impl RawMysql {
    /// Log in as root (no password) through STARTTLS.
    async fn login_root(server: &TestServer) -> Self {
        // CLIENT_LONG_PASSWORD | PROTOCOL_41 | SSL | TRANSACTIONS |
        // SECURE_CONNECTION | PLUGIN_AUTH
        let capabilities: u32 = 0x1 | 0x200 | 0x800 | 0x2000 | 0x8000 | 0x8_0000;
        let mut fixed = capabilities.to_le_bytes().to_vec();
        fixed.extend_from_slice(&(16u32 << 20).to_le_bytes());
        fixed.push(45); // utf8mb4
        fixed.extend_from_slice(&[0; 23]);

        let mut tcp = TcpStream::connect(("127.0.0.1", server.port()))
            .await
            .expect("connect");
        read_packet(&mut tcp).await; // greeting
        write_packet(&mut tcp, 1, &fixed).await; // SSLRequest
        let mut stream = TlsConnector::from(Arc::new(accept_any_cert_config()))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("TLS handshake");

        let mut response = fixed;
        response.extend_from_slice(b"root\0");
        response.push(0); // empty auth response: no password
        response.extend_from_slice(b"mysql_native_password\0");
        write_packet(&mut stream, 2, &response).await;
        let (_, ok) = read_packet(&mut stream).await;
        assert_eq!(ok[0], 0x00, "login failed: {ok:?}");
        Self { stream }
    }

    /// Send COM_PROCESS_KILL; Ok, or the error code.
    async fn process_kill(&mut self, id: u32) -> Result<(), u16> {
        let mut command = vec![0x0C];
        command.extend_from_slice(&id.to_le_bytes());
        write_packet(&mut self.stream, 0, &command).await;
        let (_, reply) = read_packet(&mut self.stream).await;
        match reply[0] {
            0x00 => Ok(()),
            0xFF => Err(u16::from_le_bytes([reply[1], reply[2]])),
            other => panic!("unexpected reply {other:#x}: {reply:?}"),
        }
    }
}

/// COM_PROCESS_KILL, which old clients send for KILL, ends the connection.
#[tokio::test]
async fn test_com_process_kill() {
    let server = TestServer::start("kill_com").await;
    let mut victim = server.connect().await;
    let id = connection_id(&mut victim).await;
    let mut raw = RawMysql::login_root(&server).await;

    assert_eq!(raw.process_kill(4_000_000_000).await, Err(1094));
    assert_eq!(raw.process_kill(id as u32).await, Ok(()));
    let gone = tokio::time::timeout(CEILING, victim.query_drop("SELECT 1"))
        .await
        .expect("the killed connection answered nothing");
    assert!(gone.is_err(), "a killed connection still serves statements");
    server.shutdown().await;
}
