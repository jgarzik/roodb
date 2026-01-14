//! TCP listener and MySQL server

use std::net::SocketAddr;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::raft::RaftNode;
use crate::tls::TlsConfig;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(String),
}

/// MySQL-compatible server
pub struct MySqlServer {
    addr: SocketAddr,
    tls_config: TlsConfig,
    raft_node: RaftNode,
}

impl MySqlServer {
    /// Create a new MySQL server
    pub fn new(addr: SocketAddr, tls_config: TlsConfig, raft_node: RaftNode) -> Self {
        Self {
            addr,
            tls_config,
            raft_node,
        }
    }

    /// Get the listen address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the server (blocking)
    pub async fn run(self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr).await?;
        let acceptor = TlsAcceptor::from(self.tls_config.server_config());
        let raft_node = Arc::new(self.raft_node);

        tracing::info!(addr = %self.addr, "MySQL server listening");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let acceptor = acceptor.clone();
            let _raft_node = raft_node.clone();

            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(_tls_stream) => {
                        tracing::debug!(%peer_addr, "Client connected");
                        // TODO: MySQL protocol handshake and command loop
                        // For Phase 1, we just accept TLS connections
                    }
                    Err(e) => {
                        tracing::warn!(%peer_addr, error = %e, "TLS handshake failed");
                    }
                }
            });
        }
    }

    /// Run the server with shutdown signal
    pub async fn run_with_shutdown(
        self,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.addr).await?;
        let acceptor = TlsAcceptor::from(self.tls_config.server_config());
        let raft_node = Arc::new(self.raft_node);

        tracing::info!(addr = %self.addr, "MySQL server listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer_addr) = result?;
                    let acceptor = acceptor.clone();
                    let _raft_node = raft_node.clone();

                    tokio::spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(_tls_stream) => {
                                tracing::debug!(%peer_addr, "Client connected");
                                // TODO: MySQL protocol handling
                            }
                            Err(e) => {
                                tracing::warn!(%peer_addr, error = %e, "TLS handshake failed");
                            }
                        }
                    });
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("MySQL server shutting down");
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
    raft_node: RaftNode,
) -> Result<ServerHandle, ServerError> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = MySqlServer::new(addr, tls_config, raft_node);
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
