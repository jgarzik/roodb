//! Connection handler for RooDB client protocol

use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::protocol::roodb::starttls::starttls_handshake;
use crate::protocol::roodb::RooDbConnection;
use crate::server::context::ServerContext;
use crate::server::limits::HANDSHAKE_TIMEOUT;
use crate::server::query::error::ProtocolError;

/// Handle a RooDB client connection with STARTTLS
///
/// Performs STARTTLS handshake, then runs authentication and command loop.
pub async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    acceptor: TlsAcceptor,
    ctx: ServerContext,
) {
    let connection_id = ctx.next_connection_id();
    info!(%peer_addr, connection_id, "Client connected");

    // Disable Nagle's algorithm for low-latency request-response
    if let Err(e) = stream.set_nodelay(true) {
        error!(%peer_addr, connection_id, error = %e, "Failed to set TCP_NODELAY");
    }

    // Greeting, TLS upgrade and login must all finish within the handshake
    // timeout; an unauthenticated client cannot hold a connection open.
    let login = async {
        let (tls_stream, scramble) = starttls_handshake(stream, acceptor, connection_id).await?;
        let mut conn =
            RooDbConnection::new(tls_stream, connection_id, peer_addr.ip(), scramble, &ctx);
        // The handshake response comes over TLS
        conn.complete_handshake().await?;
        Ok::<_, ProtocolError>(conn)
    };
    let mut conn = match tokio::time::timeout(HANDSHAKE_TIMEOUT, login).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => {
            error!(%peer_addr, connection_id, error = %e, "Handshake failed");
            return;
        }
        Err(_) => {
            warn!(%peer_addr, connection_id, "Handshake timed out");
            return;
        }
    };

    // Run command loop
    if let Err(e) = conn.run().await {
        // ConnectionClosed is expected when client disconnects
        let error_str = e.to_string();
        if !error_str.contains("closed") {
            error!(%peer_addr, connection_id, error = %e, "Connection error");
        }
    }

    // Release any advisory locks held by this connection
    crate::server::locks::global_lock_manager().release_all_for_connection(connection_id);

    info!(%peer_addr, connection_id, "Client disconnected");
}
