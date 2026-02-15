//! Connection handler for RooDB client protocol
//!
//! Supports both MySQL-compatible (STARTTLS) and TDS 8.0 (direct TLS) clients.
//! Protocol detection is done by peeking at the first byte of the TCP stream:
//! - 0x16 (TLS ClientHello) → TDS 8.0 client, accept TLS then run TDS protocol
//! - Anything else → MySQL client, send greeting then STARTTLS

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

use crate::catalog::Catalog;
use crate::protocol::roodb::starttls::starttls_handshake;
use crate::protocol::roodb::RooDbConnection;
use crate::protocol::tds::TdsConnection;
use crate::raft::RaftNode;
use crate::storage::StorageEngine;
use crate::txn::TransactionManager;

/// Handle a client connection, auto-detecting MySQL vs TDS protocol.
#[allow(clippy::too_many_arguments)]
pub async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    connection_id: u32,
    acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
    tds_enabled: bool,
) {
    info!(%peer_addr, connection_id, "Client connected");

    // Disable Nagle's algorithm for low-latency request-response
    if let Err(e) = stream.set_nodelay(true) {
        error!(%peer_addr, connection_id, error = %e, "Failed to set TCP_NODELAY");
    }

    // Detect protocol when TDS is enabled.
    // TDS 8.0 clients send TLS ClientHello immediately (first byte 0x16).
    // MySQL clients wait for the server greeting, so no data is available.
    // When TDS is disabled, skip detection entirely — zero added latency.
    let is_tds = if tds_enabled {
        let mut peek_buf = [0u8; 1];
        match tokio::time::timeout(std::time::Duration::from_millis(5), stream.readable()).await {
            Ok(Ok(())) => {
                // Socket is readable — data arrived, peek won't block
                match stream.peek(&mut peek_buf).await {
                    Ok(1) => peek_buf[0] == 0x16,
                    _ => false,
                }
            }
            _ => false, // Timeout (no data from client) → MySQL client
        }
    } else {
        false
    };

    if is_tds {
        // TDS 8.0: Client is initiating TLS directly (with ALPN "tds/8.0")
        handle_tds_connection(
            stream,
            peer_addr,
            connection_id,
            acceptor,
            storage,
            catalog,
            txn_manager,
            raft_node,
        )
        .await;
    } else {
        // MySQL: Client expects server greeting, then STARTTLS
        handle_mysql_connection(
            stream,
            peer_addr,
            connection_id,
            acceptor,
            storage,
            catalog,
            txn_manager,
            raft_node,
        )
        .await;
    }
}

/// Handle a TDS 8.0 connection (direct TLS with ALPN "tds/8.0")
#[allow(clippy::too_many_arguments)]
async fn handle_tds_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    connection_id: u32,
    acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
) {
    info!(%peer_addr, connection_id, "TDS 8.0 client detected, accepting TLS");

    // Accept TLS handshake directly
    let tls_stream = match acceptor.accept(stream).await {
        Ok(s) => s,
        Err(e) => {
            error!(%peer_addr, connection_id, error = %e, "TDS TLS handshake failed");
            return;
        }
    };

    // Create TDS connection handler
    let mut conn = TdsConnection::new(
        tls_stream,
        connection_id,
        peer_addr.ip(),
        storage,
        catalog,
        txn_manager,
        raft_node,
    );

    // Run TDS protocol (PRELOGIN → LOGIN7 → command loop)
    if let Err(e) = conn.run().await {
        let error_str = e.to_string();
        if !error_str.contains("closed") && !error_str.contains("PermissionDenied") {
            error!(%peer_addr, connection_id, error = %e, "TDS connection error");
        }
    }

    info!(%peer_addr, connection_id, "TDS client disconnected");
}

/// Handle a MySQL-compatible connection (STARTTLS)
#[allow(clippy::too_many_arguments)]
async fn handle_mysql_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    connection_id: u32,
    acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
) {
    // Perform STARTTLS handshake
    let (tls_stream, scramble) = match starttls_handshake(stream, acceptor, connection_id).await {
        Ok(result) => result,
        Err(e) => {
            error!(%peer_addr, connection_id, error = %e, "STARTTLS handshake failed");
            return;
        }
    };

    // Create RooDB connection on TLS stream
    let mut conn = RooDbConnection::new_with_scramble(
        tls_stream,
        connection_id,
        peer_addr.ip(),
        scramble,
        storage,
        catalog,
        txn_manager,
        raft_node,
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
