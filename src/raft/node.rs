//! Raft node management
//!
//! Provides the RaftNode type which wraps OpenRaft and integrates with
//! the SQL execution layer. SQL DML operations collect row changes which
//! are proposed to Raft for replication.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::raft::ReadPolicy;
use openraft::{Config, SnapshotPolicy};
use parking_lot::RwLock;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::catalog::Catalog;
use crate::raft::log_store::LogStore;
use crate::raft::lsm_storage::LsmRaftStorage;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcHandler};
use crate::raft::types::{Command, CommandResponse, Node, NodeId, Raft};
use crate::raft::ChangeSet;
use crate::storage::StorageEngine;
use crate::tls::RaftTlsConfig;

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
    network: RaftNetworkFactoryImpl,
    tls_config: RaftTlsConfig,
    shutdown_tx: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    log_store: LogStore,
    single_node: bool,
    /// Highest transaction ID the state machine has applied (or a bound
    /// above it), for the transaction manager.
    applied_txn_floor: Arc<AtomicU64>,
    /// Log size, in bytes, above which a checkpoint is requested.
    log_budget: Arc<AtomicU64>,
    /// The task that requests checkpoints.
    checkpointer: tokio::task::JoinHandle<()>,
    /// The task that keeps row IDs reserved ahead of use.
    row_id_reserver: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// How much log may accumulate before the state machine is checkpointed so
/// that the log can be purged. It bounds both the disk the log uses and the
/// work a restart has to replay.
pub const DEFAULT_LOG_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// How long startup waits for the node to lead and to replay its log.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// How often the log size is looked at.
const CHECKPOINT_POLL: Duration = Duration::from_millis(250);

/// How long to wait for a requested checkpoint to shrink the log before
/// asking again.
const CHECKPOINT_RETRY: Duration = Duration::from_secs(5);

/// Smallest range of row IDs reserved at a time.
const ROW_ID_MIN_BLOCK: u64 = 1 << 16;

/// Largest range of row IDs reserved at a time.
const ROW_ID_MAX_BLOCK: u64 = 1 << 24;

/// How often the supply of reserved row IDs is looked at.
const ROW_ID_POLL: Duration = Duration::from_millis(20);

/// Reserve `block` more row IDs for this node, past both what is reserved
/// and what any waiting allocation needs.
async fn reserve_row_ids(raft: &Raft, node_id: NodeId, block: u64) -> Result<(), RaftNodeError> {
    let (next, limit, wanted) = crate::storage::row_id::generator().demand();
    let upto = next.max(limit.unwrap_or(0)).max(wanted) + block;
    let response = raft
        .client_write(Command::ReserveRowIds { node_id, upto })
        .await
        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
    match response.data {
        CommandResponse::Ok(_) => Ok(()),
        CommandResponse::Error(message) => Err(RaftNodeError::Raft(message)),
    }
}

/// Keep row IDs reserved ahead of use.
///
/// A new range is reserved when less than half of the last one is left, or
/// when an allocation is waiting. Each range is twice what was used since the
/// previous reservation, so a bulk load quickly gets ranges to match while an
/// idle server wastes few IDs on a restart.
async fn row_id_task(raft: Raft, node_id: NodeId) {
    let generator = crate::storage::row_id::generator();
    let mut block = ROW_ID_MIN_BLOCK;
    let mut last_next = generator.demand().0;
    loop {
        tokio::time::sleep(ROW_ID_POLL).await;
        let (next, limit, wanted) = generator.demand();
        let Some(limit) = limit else {
            continue;
        };
        if limit.saturating_sub(next) >= block / 2 && wanted <= limit {
            continue;
        }
        let used = next.saturating_sub(last_next);
        block = (used * 2).clamp(ROW_ID_MIN_BLOCK, ROW_ID_MAX_BLOCK);
        last_next = next;
        match reserve_row_ids(&raft, node_id, block).await {
            Ok(()) => {}
            // Not the leader, or shutting down. A follower hands out no row
            // IDs; if this node becomes leader the next round succeeds.
            Err(_) if raft.is_leader() => return,
            Err(_) => {}
        }
    }
}

/// Ask for a checkpoint whenever the log outgrows its budget.
///
/// A checkpoint flushes the state machine and tells openraft the position
/// that is now on disk; openraft then purges the log up to it. Checkpointing
/// on log size rather than on a count of entries keeps flushes large: each
/// one writes a table, and many small tables mean much compaction.
async fn checkpoint_task(raft: Raft, log: LogStore, budget: Arc<AtomicU64>) {
    loop {
        tokio::time::sleep(CHECKPOINT_POLL).await;
        if log.disk_bytes() <= budget.load(Ordering::Relaxed) {
            continue;
        }
        if raft.trigger().snapshot().await.is_err() {
            // Raft has shut down.
            return;
        }
        // Give the checkpoint and the purge that follows time to take effect.
        let deadline = tokio::time::Instant::now() + CHECKPOINT_RETRY;
        while log.disk_bytes() > budget.load(Ordering::Relaxed)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(CHECKPOINT_POLL).await;
        }
    }
}

impl RaftNode {
    /// Create a new Raft node.
    ///
    /// `log_store` holds the Raft log and the vote. `storage` is what the
    /// state machine applies committed entries to; it also holds the state
    /// machine's own position (last applied entry, membership).
    pub async fn new(
        id: NodeId,
        addr: SocketAddr,
        tls_config: RaftTlsConfig,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        log_store: LogStore,
        single_node: bool,
    ) -> Result<Self, RaftNodeError> {
        let config = if single_node {
            // Single-node: disable all unnecessary Raft overhead
            Config {
                cluster_name: "roodb".to_string(),
                heartbeat_interval: 1,
                election_timeout_min: 50,
                election_timeout_max: 100,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                purge_batch_size: 1000,
                // Checkpoints are requested by log size (see
                // `checkpoint_task`), not by entry count, and with no
                // followers to catch up nothing is kept behind one.
                snapshot_policy: SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 0,
                ..Default::default()
            }
        } else {
            Config {
                cluster_name: "roodb".to_string(),
                election_timeout_min: 150,
                election_timeout_max: 300,
                heartbeat_interval: 50,
                snapshot_policy: SnapshotPolicy::Never,
                ..Default::default()
            }
        };
        let config = Arc::new(config);

        // The state machine applies committed entries to the LSM; the log
        // store holds the entries themselves.
        let state_machine = LsmRaftStorage::new(storage, catalog, single_node, id)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        let applied_txn_floor = state_machine.applied_txn_floor();

        let network = RaftNetworkFactoryImpl::new(tls_config.clone());

        let raft = Raft::new(
            id,
            config,
            network.clone(),
            log_store.clone(),
            state_machine,
        )
        .await
        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;

        let log_budget = Arc::new(AtomicU64::new(DEFAULT_LOG_BUDGET_BYTES));
        let checkpointer = tokio::spawn(checkpoint_task(
            raft.clone(),
            log_store.clone(),
            log_budget.clone(),
        ));

        // Runs on every node: only a leader's reservations succeed, and a
        // follower that becomes leader needs them at once.
        let row_id_reserver = tokio::spawn(row_id_task(raft.clone(), id));

        Ok(Self {
            id,
            addr,
            raft,
            network,
            tls_config,
            shutdown_tx: parking_lot::Mutex::new(None),
            log_store,
            single_node,
            applied_txn_floor,
            log_budget,
            checkpointer,
            row_id_reserver: parking_lot::Mutex::new(Some(row_id_reserver)),
        })
    }

    /// The highest transaction ID the state machine has applied, or a bound
    /// above it. Give this to the transaction manager.
    pub fn applied_txn_floor(&self) -> Arc<AtomicU64> {
        self.applied_txn_floor.clone()
    }

    /// Change how much log may accumulate before a checkpoint is requested.
    pub fn set_log_budget(&self, bytes: u64) {
        self.log_budget.store(bytes, Ordering::Relaxed);
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

    /// Bring a single-node cluster up: form it on first start, resume it on
    /// every later one, and return once this node is leading and its state
    /// machine has caught up with its log.
    pub async fn bootstrap_single_node(&self) -> Result<(), RaftNodeError> {
        let initialized = self
            .raft
            .is_initialized()
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        if !initialized {
            let mut members = BTreeMap::new();
            members.insert(self.id, Node::default());
            self.raft
                .initialize(members)
                .await
                .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
            tracing::info!(node_id = self.id, "Formed a single-node cluster");
        }
        self.wait_until_ready().await
    }

    /// Wait until this node leads and has applied everything in its log that
    /// was committed before it last stopped. Until then reads could see an
    /// older state than clients were shown before the restart.
    ///
    /// A single node holds no elections on a timer, so if it did not come
    /// back as leader by itself it is told to stand.
    pub async fn wait_until_ready(&self) -> Result<(), RaftNodeError> {
        if self.single_node {
            let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
            let mut next_election = tokio::time::Instant::now() + Duration::from_millis(200);
            while !self.raft.is_leader() {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(RaftNodeError::Raft(
                        "single node did not become leader".to_string(),
                    ));
                }
                if now >= next_election {
                    self.raft
                        .trigger()
                        .elect(false)
                        .await
                        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
                    next_election = now + Duration::from_secs(1);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.raft
            .wait_for_recovery(Some(READY_TIMEOUT))
            .await
            .map_err(|e| RaftNodeError::Raft(format!("recovery did not complete: {e}")))?;

        // Row IDs. Every reservation made before the restart has been
        // replayed by now; any ID in those ranges may have been used, so
        // start past them, with a fresh range reserved before serving.
        if self.single_node {
            crate::storage::row_id::generator().skip_reserved();
            reserve_row_ids(&self.raft, self.id, ROW_ID_MIN_BLOCK).await?;
        }
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
        // Update addr with the actual bound address (important when port was 0)
        self.addr = listener.local_addr()?;

        let acceptor = TlsAcceptor::from(self.tls_config.server_config());
        let handler = Arc::new(RaftRpcHandler::new(self.raft.clone()));

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.lock() = Some(shutdown_tx);

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
        let start = std::time::Instant::now();
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        tracing::debug!(target: "roodb::perf", client_write_us = start.elapsed().as_micros() as u64, "raft_propose");

        // Check the state machine apply result for application-level errors
        // (e.g., OCC conflicts, duplicate keys)
        match resp.data {
            CommandResponse::Ok(_) => Ok(()),
            CommandResponse::Error(msg) => Err(RaftNodeError::Raft(msg)),
        }
    }

    /// Write a raw command to the Raft log
    pub async fn write(&self, cmd: Command) -> Result<CommandResponse, RaftNodeError> {
        let start = std::time::Instant::now();
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        tracing::debug!(target: "roodb::perf", client_write_us = start.elapsed().as_micros() as u64, "raft_write");
        Ok(resp.data)
    }

    /// Check if this node is the leader
    ///
    /// Returns true if this node can accept writes (is the Raft leader).
    /// Non-leaders can still serve reads from local storage.
    pub async fn is_leader(&self) -> bool {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .is_ok()
    }

    /// Ensure linearizable read
    ///
    /// This confirms we are the leader and have the latest committed state.
    /// Call this before returning query results if strong consistency is required.
    pub async fn ensure_linearizable(&self) -> Result<(), RaftNodeError> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|_| ())
            .map_err(|e| RaftNodeError::Raft(e.to_string()))
    }

    /// Stop the node: no more RPCs, Raft halted, and everything queued for
    /// the log written out.
    pub async fn shutdown(&self) -> Result<(), RaftNodeError> {
        self.checkpointer.abort();
        if let Some(reserver) = self.row_id_reserver.lock().take() {
            reserver.abort();
        }
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
        self.raft
            .shutdown()
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        self.log_store.close().await;
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

impl Drop for RaftNode {
    fn drop(&mut self) {
        // These tasks hold a handle to Raft and would keep it alive.
        self.checkpointer.abort();
        if let Some(reserver) = self.row_id_reserver.lock().take() {
            reserver.abort();
        }
    }
}
