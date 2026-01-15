//! STARTTLS handshake for MySQL protocol
//!
//! Handles the initial plaintext handshake and TLS upgrade sequence.

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use super::auth::SslRequest;
use super::error::{codes, states, ProtocolError, ProtocolResult};
use super::handshake::HandshakeV10;
use super::packet::{read_packet_raw, write_packet_raw};
use super::resultset::encode_err_packet;

/// Perform STARTTLS handshake and return upgraded TLS stream
///
/// Sequence:
/// 1. Send HandshakeV10 (plaintext) with CLIENT_SSL capability
/// 2. Read SSLRequest (plaintext)
/// 3. Verify CLIENT_SSL is set (reject if not)
/// 4. Upgrade to TLS
/// 5. Return TLS stream and scramble for auth
pub async fn starttls_handshake(
    mut stream: TcpStream,
    acceptor: TlsAcceptor,
    connection_id: u32,
) -> ProtocolResult<(TlsStream<TcpStream>, [u8; 20])> {
    info!(connection_id, "Starting STARTTLS handshake");

    // Create and send server greeting (plaintext)
    let greeting = HandshakeV10::new(connection_id);
    let scramble = greeting.scramble();

    let greeting_packet = greeting.encode();
    write_packet_raw(&mut stream, 0, &greeting_packet).await?;
    stream.flush().await?;

    debug!(
        connection_id,
        "Sent server greeting, waiting for SSL request"
    );

    // Read client's SSL request (plaintext)
    let (packet, _seq) = read_packet_raw(&mut stream).await?;

    // Parse as SSL request (first 32 bytes)
    let ssl_request = SslRequest::parse(&packet)?;

    // Require SSL - reject clients that don't request it
    if !ssl_request.has_ssl() {
        warn!(
            connection_id,
            "Client did not request SSL, rejecting connection"
        );

        // Send error packet
        let err = encode_err_packet(
            codes::ER_ACCESS_DENIED,
            states::ACCESS_DENIED,
            "SSL connection required",
        );
        write_packet_raw(&mut stream, 2, &err).await?;
        stream.flush().await?;

        return Err(ProtocolError::AuthFailed(
            "SSL connection required".to_string(),
        ));
    }

    debug!(connection_id, "Client requested SSL, upgrading connection");

    // Upgrade to TLS
    let tls_stream = acceptor.accept(stream).await.map_err(|e| {
        warn!(connection_id, error = %e, "TLS handshake failed");
        ProtocolError::Tls(e.to_string())
    })?;

    info!(connection_id, "TLS handshake completed");

    Ok((tls_stream, scramble))
}
