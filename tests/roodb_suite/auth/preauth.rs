//! Limits on what an unauthenticated client can make the server do.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::roodb_suite::harness::TestServer;

/// Connect and consume the plaintext server greeting.
async fn connect_and_read_greeting(server: &TestServer) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
        .await
        .expect("connect");
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .expect("greeting header");
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut greeting = vec![0u8; len];
    stream
        .read_exact(&mut greeting)
        .await
        .expect("greeting body");
    stream
}

/// Wait for the server to close the connection.
async fn closed_within(stream: &mut TcpStream, limit: Duration) -> bool {
    let mut buf = [0u8; 256];
    let wait = async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => continue, // an error packet before the close is fine
            }
        }
    };
    tokio::time::timeout(limit, wait).await.is_ok()
}

/// A pre-auth packet that claims to be huge is refused at once; the server
/// must not sit waiting for (or allocate) 16 MiB from a stranger.
#[tokio::test]
async fn test_oversized_preauth_packet_is_rejected() {
    let server = TestServer::start("preauth_oversized").await;
    let mut stream = connect_and_read_greeting(&server).await;

    // Header: 0xFFFFFF payload bytes, sequence 1. Send only a few of them.
    stream
        .write_all(&[0xFF, 0xFF, 0xFF, 0x01, 0, 0, 0, 0])
        .await
        .expect("send header");

    assert!(
        closed_within(&mut stream, Duration::from_secs(3)).await,
        "server should drop a client announcing a 16 MiB pre-auth packet"
    );
    server.shutdown().await;
}

/// A client that connects and then says nothing is disconnected.
#[tokio::test]
async fn test_silent_client_is_disconnected() {
    let server = TestServer::start("preauth_silent").await;
    let mut stream = connect_and_read_greeting(&server).await;

    assert!(
        closed_within(&mut stream, Duration::from_secs(20)).await,
        "server should time out a client that never completes the handshake"
    );
    server.shutdown().await;
}
