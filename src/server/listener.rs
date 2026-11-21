//! TCP listener and RooDB server

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::catalog::Catalog;
use crate::protocol::tds::{self, connection::TdsAcceptors};
use crate::raft::LogStore;
use crate::raft::RaftNode;
use crate::server::context::ServerContext;
use crate::server::handler::handle_connection;
use crate::storage::{set_node_id, StorageEngine};
use crate::tls::{RaftTlsConfig, TlsConfig};
use crate::txn::TransactionManager;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

/// RooDB server with STARTTLS support
pub struct RooDbServer {
    addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    /// TLS acceptors for the TDS listener
    tds_acceptors: TdsAcceptors,
    /// Address of the TDS listener, if enabled
    tds_addr: Option<SocketAddr>,
    ctx: ServerContext,
}

impl RooDbServer {
    /// Create a new RooDB server
    pub fn new(
        addr: SocketAddr,
        tls_config: TlsConfig,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        // Transaction IDs continue from where the state machine has got to,
        // not from 1: rows already on disk carry earlier IDs.
        let txn_manager = Arc::new(TransactionManager::with_applied_floor(
            raft_node.applied_txn_floor(),
        ));
        Self {
            addr,
            tls_acceptor: TlsAcceptor::from(tls_config.server_config()),
            tds_acceptors: TdsAcceptors::new(&tls_config),
            tds_addr: None,
            ctx: ServerContext::new(storage, catalog, txn_manager, raft_node),
        }
    }

    /// Also accept TDS (SQL Server protocol) clients on `addr`.
    pub fn enable_tds(mut self, addr: SocketAddr) -> Self {
        self.tds_addr = Some(addr);
        self
    }

    /// Get the listen address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the server until the process exits
    pub async fn run(self) -> Result<(), ServerError> {
        self.run_until(std::future::pending()).await
    }

    /// Serve clients until `shutdown` completes, then stop accepting and
    /// return. Connections already open are left to their tasks.
    pub async fn run_until(
        self,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr).await?;
        self.follow_leadership();
        self.accept_loop(listener, shutdown).await
    }

    /// Keep the transaction manager told whether this node is the primary
    /// (the leader of its cluster). When it stops being one, its open
    /// transactions are rolled back: their locks and buffered changes mean
    /// nothing to the node that leads now.
    fn follow_leadership(&self) {
        let txn_manager = self.ctx.txn_manager.clone();
        self.ctx.raft_node.watch_leadership(move |leads| {
            txn_manager.set_leader(leads);
            if leads {
                tracing::info!("This node is the primary");
            } else {
                let aborted = txn_manager.abort_all();
                tracing::info!(aborted, "This node is a secondary: read-only");
            }
        });
    }

    /// Run the server with a pre-bound listener and shutdown signal
    ///
    /// This avoids TOCTOU race conditions when the port is allocated
    /// separately from server startup (e.g., in test harnesses).
    pub async fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ServerError> {
        self.accept_loop(listener, async {
            // A dropped sender also means shutdown.
            let _ = shutdown_rx.await;
        })
        .await
    }

    /// Accept connections until `shutdown` resolves.
    async fn accept_loop(
        self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), ServerError> {
        tracing::info!(addr = %self.addr, "RooDB server listening (STARTTLS enabled)");

        let tds_listener = match self.tds_addr {
            Some(addr) => {
                let listener = TcpListener::bind(addr).await?;
                tracing::info!(%addr, "TDS listener enabled (TLS required)");
                Some(listener)
            }
            None => None,
        };

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let Some((stream, peer_addr)) = accepted(result).await else {
                        continue;
                    };
                    // Handle connection with STARTTLS (TLS upgrade happens in handler)
                    tokio::spawn(handle_connection(
                        stream,
                        peer_addr,
                        self.tls_acceptor.clone(),
                        self.ctx.clone(),
                    ));
                }
                result = accept_optional(&tds_listener) => {
                    let Some((stream, peer_addr)) = accepted(result).await else {
                        continue;
                    };
                    tokio::spawn(tds::connection::serve(
                        stream,
                        peer_addr,
                        self.tds_acceptors.clone(),
                        self.ctx.clone(),
                    ));
                }
                _ = &mut shutdown => {
                    tracing::info!("RooDB server shutting down");
                    return Ok(());
                }
            }
        }
    }
}

/// Pause after a failed accept, so a persistent failure (such as running
/// out of file descriptors) does not spin.
const ACCEPT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// Unwrap an accepted connection. A failed accept is logged and skipped:
/// it concerns one incoming connection (or a temporary resource
/// shortage), not the listener, and must not stop the server.
async fn accepted(
    result: std::io::Result<(tokio::net::TcpStream, SocketAddr)>,
) -> Option<(tokio::net::TcpStream, SocketAddr)> {
    match result {
        Ok(connection) => Some(connection),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to accept a connection");
            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            None
        }
    }
}

/// Accept on a listener that may not be configured; without one this
/// never resolves.
async fn accept_optional(
    listener: &Option<TcpListener>,
) -> std::io::Result<(tokio::net::TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

/// Server handle for testing
pub struct ServerHandle {
    pub addr: SocketAddr,
    pub shutdown_tx: oneshot::Sender<()>,
    raft_node: Arc<RaftNode>,
}

impl ServerHandle {
    /// Stop accepting connections and shut Raft down, with its log written
    /// out. The storage engine is the caller's to close.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(e) = self.raft_node.shutdown().await {
            tracing::warn!(error = %e, "Raft shutdown reported an error");
        }
    }
}

/// Start a server in the background for testing
///
/// Accepts a pre-bound TcpListener to avoid TOCTOU race conditions
/// where the port could be grabbed by another test between allocation and binding.
pub async fn start_test_server(
    listener: TcpListener,
    tls_config: TlsConfig,
    raft_tls_config: RaftTlsConfig,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    log_store: LogStore,
) -> Result<ServerHandle, ServerError> {
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Create Raft node for single-node mode (use port 0 to let OS pick available port)
    // This avoids port conflicts when running tests in parallel
    let raft_addr: SocketAddr = "127.0.0.1:0".parse().expect("valid address");

    let node_id = 1;
    let mut raft_node = RaftNode::new(
        node_id,
        raft_addr,
        raft_tls_config,
        storage.clone(),
        catalog.clone(),
        log_store,
        true,
    )
    .await
    .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    // Set node ID for globally unique row ID generation
    set_node_id(node_id);

    raft_node
        .start_rpc_server()
        .await
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    // Forms the cluster on first start; on later starts resumes it and
    // replays the log. Returns once the node leads and has caught up.
    raft_node
        .bootstrap_single_node()
        .await
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    let raft_node = Arc::new(raft_node);

    let server = RooDbServer::new(addr, tls_config, storage, catalog, raft_node.clone());
    let actual_addr = server.addr();

    tokio::spawn(async move {
        if let Err(e) = server.run_with_listener(listener, shutdown_rx).await {
            tracing::error!(error = %e, "Server error");
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    Ok(ServerHandle {
        addr: actual_addr,
        shutdown_tx,
        raft_node,
    })
}
