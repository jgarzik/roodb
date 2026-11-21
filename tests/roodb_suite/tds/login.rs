//! TDS transport and login.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{connect, connect_as, TRANSPORTS};
use crate::roodb_suite::harness::TestServer;

#[tokio::test]
async fn test_login_succeeds_on_both_transports() {
    let server = TestServer::start("tds_login").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let row = client
            .simple_query("SELECT 1 + 1")
            .await
            .unwrap_or_else(|e| panic!("{name}: query failed: {e}"))
            .into_row()
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: no row"));
        assert_eq!(row.len(), 1, "{name}");
    }
    server.shutdown().await;
}

/// Wrong password and unknown user are refused with the same SQL Server
/// login error.
#[tokio::test]
async fn test_login_failures_are_uniform() {
    let server = TestServer::start("tds_login_fail").await;
    for (encryption, name) in TRANSPORTS {
        let mut seen = Vec::new();
        for (user, password) in [("root", "definitely-wrong"), ("no_such_user", "x")] {
            let err = match connect_as(&server, encryption, user, password).await {
                Ok(_) => panic!("{name}: login as {user} must fail"),
                Err(e) => e,
            };
            match err {
                tiberius::error::Error::Server(e) => {
                    assert_eq!(e.code(), 18456, "{name}: {e:?}");
                    seen.push(e.message().replace(user, "<user>"));
                }
                other => panic!("{name}: expected server error 18456, got: {other}"),
            }
        }
        assert_eq!(seen[0], seen[1], "{name}: failures must look the same");
    }
    server.shutdown().await;
}

/// Wait for the server to close the connection.
async fn closed_within(stream: &mut TcpStream, limit: Duration) -> bool {
    let mut buf = [0u8; 256];
    let wait = async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => continue,
            }
        }
    };
    tokio::time::timeout(limit, wait).await.is_ok()
}

/// Bytes that are neither a TLS ClientHello nor a PRELOGIN packet get no
/// answer and the connection is closed.
#[tokio::test]
async fn test_non_tds_client_is_dropped() {
    let server = TestServer::start("tds_not_tds").await;
    let mut stream = TcpStream::connect(("127.0.0.1", server.tds_port()))
        .await
        .unwrap();
    stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();

    let mut reply = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut reply)).await;
    assert!(read.is_ok(), "server should close the connection");
    assert!(reply.is_empty(), "server must not answer a non-TDS client");
    server.shutdown().await;
}

/// Build a PRELOGIN packet with the given ENCRYPTION option value.
fn prelogin_packet(encryption: u8) -> Vec<u8> {
    // Options: VERSION (6 bytes), ENCRYPTION (1 byte), terminator
    let mut body = vec![
        0x00, 0x00, 0x0B, 0x00, 0x06, // VERSION at offset 11, length 6
        0x01, 0x00, 0x11, 0x00, 0x01, // ENCRYPTION at offset 17, length 1
        0xFF,
    ];
    body.extend_from_slice(&[18, 0, 0, 0, 0, 0]);
    body.push(encryption);
    let len = ((8 + body.len()) as u16).to_be_bytes();
    let mut packet = vec![0x12, 0x01, len[0], len[1], 0, 0, 1, 0];
    packet.extend(body);
    packet
}

/// A TDS 7.x client that cannot encrypt is told encryption is required
/// and disconnected; it never gets to send credentials.
#[tokio::test]
async fn test_client_without_encryption_is_refused() {
    let server = TestServer::start("tds_no_encrypt").await;
    let mut stream = TcpStream::connect(("127.0.0.1", server.tds_port()))
        .await
        .unwrap();
    stream.write_all(&prelogin_packet(2)).await.unwrap(); // ENCRYPT_NOT_SUP

    let mut header = [0u8; 8];
    stream
        .read_exact(&mut header)
        .await
        .expect("PRELOGIN reply");
    assert_eq!(header[0], 0x04, "reply is a tabular result message");
    let len = u16::from_be_bytes([header[2], header[3]]) as usize;
    let mut body = vec![0u8; len - 8];
    stream.read_exact(&mut body).await.unwrap();

    // Find the ENCRYPTION option in the reply: it must be REQUIRED (3).
    let mut pos = 0;
    let mut encryption = None;
    while body[pos] != 0xFF {
        let offset = u16::from_be_bytes([body[pos + 1], body[pos + 2]]) as usize;
        if body[pos] == 0x01 {
            encryption = Some(body[offset]);
        }
        pos += 5;
    }
    assert_eq!(encryption, Some(3));

    assert!(
        closed_within(&mut stream, Duration::from_secs(3)).await,
        "server should disconnect a client that cannot encrypt"
    );
    server.shutdown().await;
}

/// A cleartext client that asks for login-only encryption (ENCRYPT_OFF)
/// is told the whole session must be encrypted.
#[tokio::test]
async fn test_login_only_encryption_is_upgraded_to_required() {
    let server = TestServer::start("tds_encrypt_off").await;
    let mut stream = TcpStream::connect(("127.0.0.1", server.tds_port()))
        .await
        .unwrap();
    stream.write_all(&prelogin_packet(0)).await.unwrap(); // ENCRYPT_OFF

    let mut header = [0u8; 8];
    stream
        .read_exact(&mut header)
        .await
        .expect("PRELOGIN reply");
    let len = u16::from_be_bytes([header[2], header[3]]) as usize;
    let mut body = vec![0u8; len - 8];
    stream.read_exact(&mut body).await.unwrap();
    let mut pos = 0;
    let mut encryption = None;
    while body[pos] != 0xFF {
        let offset = u16::from_be_bytes([body[pos + 1], body[pos + 2]]) as usize;
        if body[pos] == 0x01 {
            encryption = Some(body[offset]);
        }
        pos += 5;
    }
    assert_eq!(encryption, Some(3), "ENCRYPT_REQ");
    server.shutdown().await;
}

/// A client that connects and says nothing is disconnected.
#[tokio::test]
async fn test_silent_tds_client_is_disconnected() {
    let server = TestServer::start("tds_silent").await;
    let mut stream = TcpStream::connect(("127.0.0.1", server.tds_port()))
        .await
        .unwrap();
    assert!(
        closed_within(&mut stream, Duration::from_secs(20)).await,
        "server should time out a silent TDS client"
    );
    server.shutdown().await;
}

/// An endless PRELOGIN (no end-of-message) is cut off at the size cap.
#[tokio::test]
async fn test_oversized_prelogin_is_rejected() {
    let server = TestServer::start("tds_big_prelogin").await;
    let mut stream = TcpStream::connect(("127.0.0.1", server.tds_port()))
        .await
        .unwrap();
    // 4096-byte PRELOGIN packets with status 0: the message never ends.
    let mut packet = vec![0x12, 0x00, 0x10, 0x00, 0, 0, 1, 0];
    packet.resize(4096, 0);
    for _ in 0..4 {
        if stream.write_all(&packet).await.is_err() {
            break;
        }
    }
    assert!(
        closed_within(&mut stream, Duration::from_secs(3)).await,
        "server should drop a client streaming an endless PRELOGIN"
    );
    server.shutdown().await;
}
