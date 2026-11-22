//! Raft node management
//!
//! Provides the RaftNode type which wraps OpenRaft and integrates with
//! the SQL execution layer. SQL DML operations collect row changes which
//! are proposed to Raft for replication.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// The storage the state machine applies to.
    storage: Arc<dyn StorageEngine>,
    /// What other nodes dial to reach this one, if not `addr`.
    advertised_addr: Option<String>,
    /// Signalled when this node learns it has been removed from its
    /// cluster.
    removed: Arc<tokio::sync::Notify>,
    /// Whether this node is the only member of its cluster. Alone, it runs
    /// without timers, heartbeats or elections; they are switched on when a
    /// second node is admitted.
    alone: Arc<AtomicBool>,
    /// Highest transaction ID the state machine has applied (or a bound
    /// above it), for the transaction manager.
    applied_txn_floor: Arc<AtomicU64>,
    /// Log size, in bytes, above which a checkpoint is requested.
    log_budget: Arc<AtomicU64>,
    /// The task that requests checkpoints.
    checkpointer: tokio::task::JoinHandle<()>,
    /// The task that keeps row IDs reserved ahead of use.
    row_id_reserver: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Held while a commit makes a reservation of its own, so that a burst
    /// of commits makes one reservation rather than one each.
    row_id_gate: tokio::sync::Mutex<()>,
}

/// What a write is refused with on a node that is not the leader of its
/// cluster: MySQL's message for a read-only server (error 1290).
pub const NOT_PRIMARY: &str =
    "The MySQL server is running with the --super-read-only option so it cannot execute this statement";

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
/// and what has been handed out.
async fn reserve_row_ids(raft: &Raft, node_id: NodeId, block: u64) -> Result<(), RaftNodeError> {
    let (next, limit) = crate::storage::row_id::generator().demand();
    let upto = next.max(limit.unwrap_or(0)) + block;
    let response = raft
        .client_write(Command::ReserveRowIds { node_id, upto })
        .await
        .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
    match response.data {
        CommandResponse::Ok(_) => Ok(()),
        CommandResponse::Error(message) => Err(RaftNodeError::Raft(message)),
    }
}

/// Keep row IDs reserved ahead of use, so that commits rarely have to wait
/// for a reservation.
///
/// A new range is reserved when less than half of the last one is left. Each
/// range is twice what was used since the previous reservation, so a bulk
/// load quickly gets ranges to match while an idle server wastes few IDs on
/// a restart. Runs until the node shuts down; a failed attempt (this node is
/// not the leader, say) is simply tried again.
async fn row_id_task(raft: Raft, node_id: NodeId) {
    let generator = crate::storage::row_id::generator();
    let mut block = ROW_ID_MIN_BLOCK;
    let mut last_next = generator.demand().0;
    loop {
        tokio::time::sleep(ROW_ID_POLL).await;
        // Only a leader's proposal can succeed; a follower hands out no IDs.
        if !raft.is_leader() {
            continue;
        }
        let (next, Some(limit)) = generator.demand() else {
            continue;
        };
        if limit.saturating_sub(next) >= block / 2 {
            continue;
        }
        let used = next.saturating_sub(last_next);
        block = (used * 2).clamp(ROW_ID_MIN_BLOCK, ROW_ID_MAX_BLOCK);
        last_next = next;
        if let Err(e) = reserve_row_ids(&raft, node_id, block).await {
            tracing::debug!(error = %e, "row ID reservation not made; will retry");
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
                // The timing of a cluster: unused while the node is alone
                // (its timers are off), in force once another node joins.
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                // Purge whatever a checkpoint allows. A minimum batch would
                // leave the log over its budget whenever entries are large:
                // few of them fill the budget, and too few to purge.
                purge_batch_size: 1,
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
        let state_machine = LsmRaftStorage::new(storage.clone(), catalog, id)
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

        // A node that restarts as one of several members is not alone,
        // whatever it was started as.
        let alone = {
            use openraft::async_runtime::WatchReceiver;
            let members = raft
                .metrics()
                .borrow_watched()
                .membership_config
                .nodes()
                .count();
            single_node && members <= 1
        };
        if !alone && single_node {
            let runtime = raft.runtime_config();
            runtime.tick(true);
            runtime.heartbeat(true);
            runtime.elect(true);
        }
        let alone = Arc::new(AtomicBool::new(alone));

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
            storage,
            advertised_addr: None,
            removed: Arc::new(tokio::sync::Notify::new()),
            alone,
            applied_txn_floor,
            log_budget,
            checkpointer,
            row_id_reserver: parking_lot::Mutex::new(Some(row_id_reserver)),
            row_id_gate: tokio::sync::Mutex::new(()),
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
            let addr = self
                .advertised_addr
                .clone()
                .unwrap_or_else(|| self.addr.to_string());
            members.insert(self.id, Node::new(addr));
            self.raft
                .initialize(members)
                .await
                .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
            tracing::info!(node_id = self.id, "Formed a single-node cluster");
            self.wait_until_ready().await?;
            return self.seed_log().await;
        }
        self.wait_until_ready().await
    }

    /// Put what storage already holds into the log, as the cluster's first
    /// entry. `roodb_init` wrote it before there was a log; with it in the
    /// log, a node that joins with nothing is brought to the same state by
    /// the log like everything else.
    async fn seed_log(&self) -> Result<(), RaftNodeError> {
        use crate::raft::lsm_storage::RAFT_KEY_PREFIX;
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .storage
            .scan(None, None)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?
            .into_iter()
            .filter(|(key, _)| !key.starts_with(RAFT_KEY_PREFIX))
            .collect();
        self.write(Command::Seed(entries)).await.map(|_| ())
    }

    /// Set the address other nodes are to reach this one at, where it
    /// differs from the address it listens on.
    pub fn set_advertised_addr(&mut self, addr: String) {
        self.advertised_addr = Some(addr);
    }

    /// Whether this node leads its cluster right now, as far as it knows.
    pub fn leads(&self) -> bool {
        self.raft.is_leader()
    }

    /// Call `changed(leads)` now and every time this node starts or stops
    /// leading its cluster, from a task of its own.
    pub fn watch_leadership(self: &Arc<Self>, changed: impl Fn(bool) + Send + 'static) {
        use openraft::async_runtime::WatchReceiver;
        let node = Arc::downgrade(self);
        let mut metrics = self.raft.metrics();
        tokio::spawn(async move {
            let mut led: Option<bool> = None;
            loop {
                let leads = metrics.borrow_watched().state.is_leader();
                if led != Some(leads) {
                    led = Some(leads);
                    if leads {
                        // IDs this node handed out the last time it led may
                        // have been used: go past everything it reserved,
                        // and reserve afresh before any is handed out.
                        let Some(node) = node.upgrade() else { return };
                        crate::storage::row_id::generator().skip_reserved();
                        if let Err(e) = reserve_row_ids(&node.raft, node.id, ROW_ID_MIN_BLOCK).await
                        {
                            tracing::warn!(error = %e, "Could not reserve row IDs on becoming leader");
                        }
                    }
                    changed(leads);
                }
                if metrics.changed().await.is_err() {
                    return;
                }
            }
        });
    }

    /// The nodes of the cluster as this node knows them: ID, address, and
    /// role (`PRIMARY`, `SECONDARY`, or `LEARNER` for a node still being
    /// brought up to date).
    pub fn members(&self) -> Vec<(NodeId, String, &'static str)> {
        use openraft::async_runtime::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let voters: Vec<NodeId> = metrics.membership_config.voter_ids().collect();
        metrics
            .membership_config
            .nodes()
            .map(|(id, node)| {
                let role = if metrics.current_leader == Some(*id) {
                    "PRIMARY"
                } else if voters.contains(id) {
                    "SECONDARY"
                } else {
                    "LEARNER"
                };
                (*id, node.addr.clone(), role)
            })
            .collect()
    }

    /// Take a node out of the cluster. Only the primary can. The node
    /// itself stops serving when it learns of it.
    pub async fn remove_member(&self, id: NodeId) -> Result<(), RaftNodeError> {
        use openraft::ChangeMembers;
        if !self.raft.is_leader() {
            return Err(RaftNodeError::Raft(NOT_PRIMARY.to_string()));
        }
        let members = self.members();
        let Some((_, addr, role)) = members.iter().find(|(member, _, _)| *member == id) else {
            return Err(RaftNodeError::Raft(format!(
                "Node {id} is not a member of the cluster"
            )));
        };
        let ids = std::collections::BTreeSet::from([id]);
        let change = if *role == "LEARNER" {
            ChangeMembers::RemoveNodes(ids)
        } else {
            let voters = members.iter().filter(|(_, _, r)| *r != "LEARNER").count();
            if voters <= 1 {
                return Err(RaftNodeError::Raft(
                    "Cannot remove the only voting node of the cluster".to_string(),
                ));
            }
            ChangeMembers::RemoveVoters(ids)
        };
        self.raft
            .change_membership(change, false)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        tracing::info!(node_id = id, "A node was removed from the cluster");
        // The node is told, so that it stops instead of calling elections
        // nobody will answer.
        if id != self.id {
            let tls_config = self.tls_config.clone();
            let addr = addr.clone();
            tokio::spawn(
                async move { crate::raft::network::notify_removed(&tls_config, &addr).await },
            );
        }
        Ok(())
    }

    /// Hand the primary role to another node. Only the primary can, and
    /// only to a voting member. Returns when the request has been made; the
    /// other node takes over within moments, and this one becomes a
    /// secondary (rolling back its open transactions, as on any change of
    /// primary).
    pub async fn transfer_primary(&self, to: NodeId) -> Result<(), RaftNodeError> {
        if !self.raft.is_leader() {
            return Err(RaftNodeError::Raft(NOT_PRIMARY.to_string()));
        }
        if to == self.id {
            return Ok(());
        }
        match self.members().iter().find(|(id, _, _)| *id == to) {
            Some((_, _, "SECONDARY")) => {}
            Some(_) => {
                return Err(RaftNodeError::Raft(format!(
                    "Node {to} is still being brought up to date and cannot be the primary yet"
                )))
            }
            None => {
                return Err(RaftNodeError::Raft(format!(
                    "Node {to} is not a member of the cluster"
                )))
            }
        }
        self.raft
            .trigger()
            .transfer_leader(to)
            .await
            .map_err(|e| RaftNodeError::Raft(e.to_string()))
    }

    /// Call `removed` once, if this node finds that it has been taken out
    /// of its cluster: it is told so by the leader, or sees a membership
    /// it is no longer part of.
    pub fn watch_removal(self: &Arc<Self>, removed: impl FnOnce() + Send + 'static) {
        use openraft::async_runtime::WatchReceiver;
        let id = self.id;
        let mut metrics = self.raft.metrics();
        let told = self.removed.clone();
        tokio::spawn(async move {
            let seen = async {
                let mut was_member = false;
                loop {
                    let (is_member, any) = {
                        let metrics = metrics.borrow_watched();
                        let mut nodes = metrics.membership_config.nodes();
                        let any = metrics.membership_config.nodes().next().is_some();
                        (nodes.any(|(member, _)| *member == id), any)
                    };
                    if is_member {
                        was_member = true;
                    } else if was_member && any {
                        return true;
                    }
                    if metrics.changed().await.is_err() {
                        return false;
                    }
                }
            };
            let out = tokio::select! {
                out = seen => out,
                () = told.notified() => true,
            };
            if out {
                removed();
            }
        });
    }

    /// Whether this node is the only member of its cluster.
    pub fn is_alone(&self) -> bool {
        self.alone.load(Ordering::SeqCst)
    }

    /// Ask a running cluster to admit a new node whose Raft port is at
    /// `own_addr`, through any of `members`. Returns the node ID the
    /// cluster assigned. Follows a member's pointer to the leader, and
    /// keeps trying for a while: the cluster may be electing.
    pub async fn request_membership(
        tls_config: &RaftTlsConfig,
        members: &[String],
        own_addr: &str,
    ) -> Result<NodeId, RaftNodeError> {
        use crate::raft::network::{request_join, JoinReply};
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        let mut candidates: Vec<String> = members.to_vec();
        let mut last_error = String::from("no member given");
        loop {
            for member in candidates.clone() {
                match request_join(tls_config, &member, own_addr).await {
                    Ok(JoinReply::Joined { node_id }) => return Ok(node_id),
                    Ok(JoinReply::NotLeader {
                        leader: Some(leader),
                    }) => {
                        if !candidates.contains(&leader) {
                            candidates.insert(0, leader);
                        }
                        last_error = format!("{member} is not the leader");
                    }
                    Ok(JoinReply::NotLeader { leader: None }) => {
                        last_error = format!("{member} knows no leader yet");
                    }
                    Ok(JoinReply::Failed(e)) => last_error = e,
                    Err(e) => last_error = e,
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftNodeError::Raft(format!(
                    "could not join the cluster: {last_error}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until this node leads and has applied everything in its log that
    /// was committed before it last stopped. Until then reads could see an
    /// older state than clients were shown before the restart.
    ///
    /// A single node holds no elections on a timer, so if it did not come
    /// back as leader by itself it is told to stand.
    pub async fn wait_until_ready(&self) -> Result<(), RaftNodeError> {
        if !self.is_alone() {
            // One of several: ready when it knows who leads and has applied
            // what it holds. (What it does not hold yet arrives from the
            // leader; a secondary is always a little behind.)
            use openraft::async_runtime::WatchReceiver;
            let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
            let mut metrics = self.raft.metrics();
            // What this node remembers of its cluster is published a moment
            // after it starts. (A node that has just been admitted has
            // nothing to remember yet, and is not kept waiting long.)
            let loaded = metrics.wait_until(|m| m.membership_config.nodes().next().is_some());
            let _ = tokio::time::timeout(Duration::from_secs(2), loaded).await;
            let peers: Vec<String> = self
                .members()
                .into_iter()
                .filter(|(id, _, _)| *id != self.id)
                .map(|(_, addr, _)| addr)
                .collect();
            let removed = || {
                RaftNodeError::Raft(
                    "this node has been removed from its cluster; to bring it back, \
                     empty its data directory and start it with --join"
                        .to_string(),
                )
            };
            // Was this node taken out of the cluster while it was down? It
            // would not know: what it remembers (its membership, even who
            // led) is from before. So it asks before it serves anything.
            if crate::raft::network::removed_from_cluster(&self.tls_config, self.id, &peers).await {
                return Err(removed());
            }
            loop {
                let known = metrics.wait_until(|m| m.current_leader.is_some());
                match tokio::time::timeout(Duration::from_secs(3), known).await {
                    Ok(found) => {
                        found.map_err(|e| RaftNodeError::Raft(e.to_string()))?;
                        break;
                    }
                    Err(_) if tokio::time::Instant::now() >= deadline => {
                        return Err(RaftNodeError::Raft("no leader was found".to_string()));
                    }
                    Err(_) => {}
                }
                // Nobody has claimed to lead this node. Perhaps the cluster
                // is electing; perhaps this node has been taken out of it
                // meanwhile. Ask again.
                if crate::raft::network::removed_from_cluster(&self.tls_config, self.id, &peers)
                    .await
                {
                    return Err(removed());
                }
            }
        }
        if self.is_alone() {
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
        if self.is_alone() {
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
            member_map.insert(*id, Node::new(addr.to_string()));
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
        let handler = Arc::new(RaftRpcHandler::new(
            self.raft.clone(),
            self.alone.clone(),
            self.removed.clone(),
            self.tls_config.clone(),
            self.storage.clone(),
        ));

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
        // Refuse what storage cannot hold before it is committed to the log.
        // (The state machine rejects it too; this saves the round trip.)
        for change in &changeset.changes {
            let value_len =
                change.value.as_ref().map_or(0, Vec::len) + crate::raft::MVCC_HEADER_SIZE;
            crate::storage::lsm::block::check_entry_size(change.key.len(), value_len)
                .map_err(|e| RaftNodeError::Raft(e.to_string()))?;
        }

        // Only the leader writes. Said here, before anything is tried, in
        // the words a client of a read-only MySQL server would get.
        if !self.raft.is_leader() {
            return Err(RaftNodeError::Raft(NOT_PRIMARY.to_string()));
        }
        self.cover_row_ids().await?;
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

    /// Make sure every row ID handed out so far is covered by a recorded
    /// reservation, before changes that may use them are committed. Usually
    /// the background task has reserved ahead and there is nothing to do.
    async fn cover_row_ids(&self) -> Result<(), RaftNodeError> {
        let generator = crate::storage::row_id::generator();
        if generator.uncovered().is_none() {
            return Ok(());
        }
        let _gate = self.row_id_gate.lock().await;
        // Another commit may have made the reservation while we waited.
        if generator.uncovered().is_some() {
            reserve_row_ids(&self.raft, self.id, ROW_ID_MIN_BLOCK).await?;
        }
        Ok(())
    }

    /// Write a raw command to the Raft log
    pub async fn write(&self, cmd: Command) -> Result<CommandResponse, RaftNodeError> {
        if !self.raft.is_leader() {
            return Err(RaftNodeError::Raft(NOT_PRIMARY.to_string()));
        }
        self.cover_row_ids().await?;
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
    // Requests and answers are small and each is waited for: they must go
    // out at once, not sit waiting for more to send with them.
    stream.set_nodelay(true)?;
    let mut tls_stream = acceptor.accept(stream).await?;

    // A peer keeps its connection and sends one request after another.
    loop {
        // Read: [msg_type: u8][len: u32][body]
        let msg_type = match tls_stream.read_u8().await {
            Ok(msg_type) => msg_type,
            // The peer has closed the connection between requests, cleanly
            // or not: either way there is no request to answer.
            Err(_) => return Ok(()),
        };
        let len = tls_stream.read_u32().await? as usize;

        let mut body = vec![0u8; len];
        tls_stream.read_exact(&mut body).await?;

        // Process request. A snapshot's contents follow its request on the
        // connection.
        let response = if RaftRpcHandler::is_snapshot(msg_type) {
            handler.receive_snapshot(&body, &mut tls_stream).await?
        } else {
            handler.handle_request(msg_type, &body).await?
        };

        // Write response: [len: u32][body], as one piece. Written as two,
        // the second would wait for the peer to acknowledge the first, which
        // a peer with nothing to send puts off for tens of milliseconds:
        // longer than the peer waits for an answer.
        let mut framed = Vec::with_capacity(4 + response.len());
        framed.extend_from_slice(&(response.len() as u32).to_be_bytes());
        framed.extend_from_slice(&response);
        tls_stream.write_all(&framed).await?;
        tls_stream.flush().await?;
    }
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
