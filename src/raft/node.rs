//! Raft node management

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Config;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcHandler};
use crate::raft::storage::MemStorage;
use crate::raft::types::{Command, CommandResponse, Node, NodeId, Raft};
use crate::tls::TlsConfig;

#[derive(Error, Debug)]
pub enum RaftNodeError {
    #[error("Raft error: {0}")]
    Raft(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Not leader")]
    NotLeader,
}

/// A Raft node in the cluster
pub struct RaftNode {
    id: NodeId,
    addr: SocketAddr,
    raft: Raft,
    storage: MemStorage,
    network: RaftNetworkFactoryImpl,
    tls_config: TlsConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl RaftNode {
    /// Create a new Raft node
    pub async fn new(
        id: NodeId,
        addr: SocketAddr,
        tls_config: TlsConfig,
    ) -> Result<Self, RaftNodeError> {
        let config = Config {
            cluster_name: "roodb".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let config = Arc::new(config);

        let log_storage = MemStorage::new();
        let sm_storage = MemStorage::new();
        let network = RaftNetworkFactoryImpl::new(tls_config.clone());

        let raft = Raft::new(
            id,
            config,
            network.clone(),
            log_storage.clone(),
            sm_storage.clone(),
        )
        .await
        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        Ok(Self {
            id,
            addr,
            raft,
            storage: sm_storage,
            network,
            tls_config,
            shutdown_tx: None,
        })
    }

    /// Get the node ID
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Get the Raft instance
    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    /// Get the storage
    pub fn storage(&self) -> &MemStorage {
        &self.storage
    }

    /// Add a peer node
    pub fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.network.add_node(id, addr);
    }

    /// Bootstrap as a single-node cluster (auto-elect as leader)
    pub async fn bootstrap_single_node(&self) -> Result<(), RaftNodeError> {
        let mut members = BTreeMap::new();
        members.insert(self.id, Node::default());

        self.raft
            .initialize(members)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        tracing::info!(node_id = self.id, "Bootstrapped as single-node cluster");
        Ok(())
    }

    /// Bootstrap a multi-node cluster
    pub async fn bootstrap_cluster(
        &self,
        members: Vec<(NodeId, SocketAddr)>,
    ) -> Result<(), RaftNodeError> {
        let mut member_map = BTreeMap::new();
        for (id, addr) in &members {
            member_map.insert(*id, Node::default());
            self.network.add_node(*id, *addr);
        }

        self.raft
            .initialize(member_map)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        tracing::info!(node_id = self.id, ?members, "Bootstrapped cluster");
        Ok(())
    }

    /// Start the Raft RPC server
    pub async fn start_rpc_server(&mut self) -> Result<(), RaftNodeError> {
        let listener = TcpListener::bind(self.addr).await?;
        let acceptor = TlsAcceptor::from(self.tls_config.server_config());
        let handler = Arc::new(RaftRpcHandler::new(self.raft.clone()));

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        tracing::info!(addr = %self.addr, "Raft RPC server listening");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer_addr)) => {
                                let acceptor = acceptor.clone();
                                let handler = handler.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_rpc_connection(stream, acceptor, handler).await {
                                        tracing::warn!(%peer_addr, error = %e, "RPC connection error");
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Accept error");
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("Raft RPC server shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Write a command to the Raft log
    pub async fn write(&self, cmd: Command) -> Result<CommandResponse, RaftNodeError> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        Ok(resp.data)
    }

    /// Read a value (linearizable read through Raft)
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RaftNodeError> {
        // Ensure we're the leader (linearizable read)
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        Ok(self.storage.get(key))
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    /// Shutdown the node
    pub async fn shutdown(&mut self) -> Result<(), RaftNodeError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.raft
            .shutdown()
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        Ok(())
    }
}

async fn handle_rpc_connection(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    handler: Arc<RaftRpcHandler>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tls_stream = acceptor.accept(stream).await?;

    // Read: [msg_type: u8][len: u32][body]
    let msg_type = tls_stream.read_u8().await?;
    let len = tls_stream.read_u32().await? as usize;

    let mut body = vec![0u8; len];
    tls_stream.read_exact(&mut body).await?;

    // Process request
    let response = handler.handle_request(msg_type, &body).await?;

    // Write response: [len: u32][body]
    tls_stream.write_u32(response.len() as u32).await?;
    tls_stream.write_all(&response).await?;
    tls_stream.flush().await?;

    Ok(())
}
