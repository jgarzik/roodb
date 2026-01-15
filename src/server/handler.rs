//! Connection handler for MySQL protocol

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

use crate::catalog::Catalog;
use crate::protocol::mysql::starttls::starttls_handshake;
use crate::protocol::mysql::MySqlConnection;
use crate::storage::StorageEngine;
use crate::txn::TransactionManager;

/// Handle a MySQL client connection with STARTTLS
///
/// Performs STARTTLS handshake, then runs authentication and command loop.
pub async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    connection_id: u32,
    acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
) {
    info!(%peer_addr, connection_id, "Client connected");

    // Perform STARTTLS handshake
    let (tls_stream, scramble) = match starttls_handshake(stream, acceptor, connection_id).await {
        Ok(result) => result,
        Err(e) => {
            error!(%peer_addr, connection_id, error = %e, "STARTTLS handshake failed");
            return;
        }
    };

    // Create MySQL connection on TLS stream
    let mut conn = MySqlConnection::new_with_scramble(
        tls_stream,
        connection_id,
        scramble,
        storage,
        catalog,
        txn_manager,
    );

    // Complete authentication (handshake response comes over TLS)
    if let Err(e) = conn.complete_handshake().await {
        error!(%peer_addr, connection_id, error = %e, "Authentication failed");
        return;
    }

    // Run command loop
    if let Err(e) = conn.run().await {
        // ConnectionClosed is expected when client disconnects
        let error_str = e.to_string();
        if !error_str.contains("closed") {
            error!(%peer_addr, connection_id, error = %e, "Connection error");
        }
    }

    info!(%peer_addr, connection_id, "Client disconnected");
}
