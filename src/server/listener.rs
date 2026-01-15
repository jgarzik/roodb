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
    next_conn_id: AtomicU32,
}

impl RooDbServer {
    /// Create a new RooDB server
    pub fn new(
        addr: SocketAddr,
        tls_config: TlsConfig,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        let txn_manager = Arc::new(TransactionManager::with_storage(storage.clone()));
        Self {
            addr,
            tls_acceptor: TlsAcceptor::from(tls_config.server_config()),
            storage,
            catalog,
            txn_manager,
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
    ) -> Self {
        Self {
            addr,
            tls_acceptor: TlsAcceptor::from(tls_config.server_config()),
            storage,
            catalog,
            txn_manager,
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
        let acceptor = self.tls_acceptor;
        let next_conn_id = Arc::new(self.next_conn_id);

        tracing::info!(addr = %self.addr, "RooDB server listening (STARTTLS enabled)");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let acceptor = acceptor.clone();
            let storage = storage.clone();
            let catalog = catalog.clone();
            let txn_manager = txn_manager.clone();
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

    let server = RooDbServer::new(addr, tls_config, storage, catalog);
    let actual_addr = server.addr();

    tokio::spawn(async move {
        if let Err(e) = server.run_with_shutdown(shutdown_rx).await {
            tracing::error!(error = %e, "Server error");
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    Ok(ServerHandle {
        addr: actual_addr,
        shutdown_tx,
    })
}
