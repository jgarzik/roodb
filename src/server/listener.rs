//! TCP listener and RooDB server

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::catalog::Catalog;
use crate::raft::RaftNode;
use crate::server::handler::handle_connection;
use crate::storage::StorageEngine;
use crate::tls::TlsConfig;
use crate::txn::TransactionManager;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(String),
}

/// RooDB server with STARTTLS support
pub struct RooDbServer {
    addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
    next_conn_id: AtomicU32,
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
        let txn_manager = Arc::new(TransactionManager::with_storage(storage.clone()));
        Self {
            addr,
            tls_acceptor: TlsAcceptor::from(tls_config.server_config()),
            storage,
            catalog,
            txn_manager,
            raft_node,
            next_conn_id: AtomicU32::new(1),
        }
    }

    /// Create a new RooDB server with custom transaction manager
    pub fn with_txn_manager(
        addr: SocketAddr,
        tls_config: TlsConfig,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        Self {
            addr,
            tls_acceptor: TlsAcceptor::from(tls_config.server_config()),
            storage,
            catalog,
            txn_manager,
            raft_node,
            next_conn_id: AtomicU32::new(1),
        }
    }

    /// Get the listen address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get the transaction manager
    pub fn txn_manager(&self) -> &Arc<TransactionManager> {
        &self.txn_manager
    }

    /// Run the server (blocking)
    pub async fn run(self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr).await?;
        let storage = self.storage;
        let catalog = self.catalog;
        let txn_manager = self.txn_manager;
        let raft_node = self.raft_node;
        let acceptor = self.tls_acceptor;
        let next_conn_id = Arc::new(self.next_conn_id);

        tracing::info!(addr = %self.addr, "RooDB server listening (STARTTLS enabled)");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let acceptor = acceptor.clone();
            let storage = storage.clone();
            let catalog = catalog.clone();
            let txn_manager = txn_manager.clone();
            let raft_node = raft_node.clone();
            let connection_id = next_conn_id.fetch_add(1, Ordering::Relaxed);

            // Handle connection with STARTTLS (TLS upgrade happens in handler)
            tokio::spawn(async move {
                handle_connection(
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
            });
        }
    }

    /// Run the server with shutdown signal
    pub async fn run_with_shutdown(
        self,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr).await?;
        let storage = self.storage;
        let catalog = self.catalog;
        let txn_manager = self.txn_manager;
        let raft_node = self.raft_node;
        let acceptor = self.tls_acceptor;
        let next_conn_id = Arc::new(self.next_conn_id);

        tracing::info!(addr = %self.addr, "RooDB server listening (STARTTLS enabled)");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer_addr) = result?;
                    let acceptor = acceptor.clone();
                    let storage = storage.clone();
                    let catalog = catalog.clone();
                    let txn_manager = txn_manager.clone();
                    let raft_node = raft_node.clone();
                    let connection_id = next_conn_id.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        handle_connection(
                            stream,
                            peer_addr,
                            connection_id,
                            acceptor,
                            storage,
                            catalog,
                            txn_manager,
                            raft_node,
                        ).await;
                    });
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("RooDB server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Server handle for testing
pub struct ServerHandle {
    pub addr: SocketAddr,
    pub shutdown_tx: oneshot::Sender<()>,
}

impl ServerHandle {
    /// Shutdown the server
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Start a server in the background for testing
pub async fn start_test_server(
    addr: SocketAddr,
    tls_config: TlsConfig,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
) -> Result<ServerHandle, ServerError> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Create Raft node for single-node mode (use a different port for Raft RPC)
    let raft_port = addr.port() + 1000;
    let raft_addr: SocketAddr = format!("127.0.0.1:{}", raft_port)
        .parse()
        .expect("valid raft address");

    let mut raft_node = RaftNode::new(1, raft_addr, tls_config.clone(), Some(storage.clone()))
        .await
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    raft_node
        .start_rpc_server()
        .await
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    raft_node
        .bootstrap_single_node()
        .await
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

    // Wait for leader election to complete (single-node should be immediate)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let raft_node = Arc::new(raft_node);

    let server = RooDbServer::new(addr, tls_config, storage, catalog, raft_node);
    let actual_addr = server.addr();

    tokio::spawn(async move {
        if let Err(e) = server.run_with_shutdown(shutdown_rx).await {
            tracing::error!(error = %e, "Server error");
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    Ok(ServerHandle {
        addr: actual_addr,
        shutdown_tx,
    })
}
