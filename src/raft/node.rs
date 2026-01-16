//! Raft node management
//!
//! Provides the RaftNode type which wraps OpenRaft and integrates with
//! the SQL execution layer. SQL DML operations collect row changes which
//! are proposed to Raft for replication.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Config;
use parking_lot::RwLock;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::catalog::Catalog;
use crate::raft::lsm_storage::LsmRaftStorage;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcHandler};
use crate::raft::types::{Command, CommandResponse, Node, NodeId, Raft};
use crate::raft::ChangeSet;
use crate::storage::StorageEngine;
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
///
/// Wraps OpenRaft and provides methods for:
/// - Proposing SQL changes for replication
/// - Managing cluster membership
/// - Handling Raft RPC communication
pub struct RaftNode {
    id: NodeId,
    addr: SocketAddr,
    raft: Raft,
    #[allow(dead_code)]
    storage: LsmRaftStorage,
    network: RaftNetworkFactoryImpl,
    tls_config: TlsConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl RaftNode {
    /// Create a new Raft node with storage engine integration
    ///
    /// The storage engine is used for both Raft log persistence and applying
    /// committed changes. All Raft state (log entries, vote, membership) is
    /// persisted to LSM storage for crash recovery.
    pub async fn new(
        id: NodeId,
        addr: SocketAddr,
        tls_config: TlsConfig,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Result<Self, RaftNodeError> {
        let config = Config {
            cluster_name: "roodb".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        };
        let config = Arc::new(config);

        // Create LSM-backed storage that persists all Raft state
        let lsm_storage = LsmRaftStorage::new(storage, catalog)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        let network = RaftNetworkFactoryImpl::new(tls_config.clone());

        let raft = Raft::new(
            id,
            config,
            network.clone(),
            lsm_storage.clone(),
            lsm_storage.clone(),
        )
        .await
        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        Ok(Self {
            id,
            addr,
            raft,
            storage: lsm_storage,
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

    /// Propose a set of row changes through Raft consensus
    ///
    /// This is the main entry point for SQL write operations. After a DML
    /// statement executes and collects row changes, those changes are proposed
    /// to Raft for replication to followers.
    ///
    /// Returns Ok(()) when the changes have been committed (majority ack).
    pub async fn propose_changes(&self, changeset: ChangeSet) -> Result<(), RaftNodeError> {
        if changeset.is_empty() {
            return Ok(());
        }

        let cmd = Command::DataChange(changeset);
        self.raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Write a raw command to the Raft log
    pub async fn write(&self, cmd: Command) -> Result<CommandResponse, RaftNodeError> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        Ok(resp.data)
    }

    /// Check if this node is the leader
    ///
    /// Returns true if this node can accept writes (is the Raft leader).
    /// Non-leaders can still serve reads from local storage.
    pub async fn is_leader(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    /// Ensure linearizable read
    ///
    /// This confirms we are the leader and have the latest committed state.
    /// Call this before returning query results if strong consistency is required.
    pub async fn ensure_linearizable(&self) -> Result<(), RaftNodeError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|e| RaftNodeError::Raft(e.to_string()))
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
