//! Raft network implementation over TLS

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::AppendResult;
use openraft::OptionalSend;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::raft::types::{
    AppendEntriesRequest, AppendEntriesResponse, Node, NodeId, Snapshot, SnapshotData,
    SnapshotMeta, SnapshotResponse, TypeConfig, Vote, VoteRequest, VoteResponse,
};
use crate::raft::types::{Command, CommandResponse};
use crate::tls::RaftTlsConfig;

/// What a snapshot sent to another node begins with. The contents follow on
/// the same connection as a series of frames, each a length and that many
/// bytes holding a batch of entries in key order, ended by a length of zero;
/// then the receiver answers.
#[derive(Serialize, Deserialize)]
struct SnapshotHeader {
    vote: Vote,
    meta: SnapshotMeta,
}

/// About how much of a snapshot travels in one frame.
const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

/// The largest frame of a snapshot that is accepted: one entry of the
/// largest size storage holds, and room for the rest of a chunk.
const SNAPSHOT_FRAME_LIMIT: usize = crate::storage::lsm::block::MAX_VALUE_SIZE
    + crate::storage::lsm::block::MAX_KEY_SIZE
    + 2 * SNAPSHOT_CHUNK_BYTES;

/// How long a snapshot transfer may make no progress before it is given up.
const SNAPSHOT_STALL: std::time::Duration = std::time::Duration::from_secs(60);

/// Describe a failed RPC: what was being done, to whom, and why it failed.
fn describe(
    target: NodeId,
    addr: Option<&str>,
    operation: &str,
    err: impl std::fmt::Display,
) -> String {
    let addr = addr.unwrap_or("unknown");
    format!("Raft RPC {operation} to node {target} ({addr}): {err}")
}

/// Message types for Raft RPC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MessageType {
    Vote = 1,
    VoteResponse = 2,
    AppendEntries = 3,
    AppendEntriesResponse = 4,
    Snapshot = 5,
    SnapshotResponse = 6,
    Join = 7,
    Removed = 8,
    Members = 9,
    SnapshotStream = 10,
    TransferLeader = 11,
}

/// The least time a request to take over as leader is given to be answered.
const TRANSFER_LEADER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// What a node says when asked who is in the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct MembersReply {
    /// The node answering.
    pub node_id: NodeId,
    /// Who it believes leads.
    pub leader: Option<NodeId>,
    /// The nodes of the membership it has.
    pub members: Vec<NodeId>,
}

/// Whether `addr` (`host:port`) names no particular host: the "any address"
/// a server binds to, or nothing at all.
pub fn is_unspecified(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.is_empty()
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_unspecified())
}

/// Ask the node at `addr` who is in the cluster.
pub async fn ask_members(tls_config: &RaftTlsConfig, addr: &str) -> Result<MembersReply, String> {
    let connection = RaftNetworkConnection {
        target: 0,
        addr: Some(addr.to_string()),
        tls_config: tls_config.clone(),
        stream: tokio::sync::Mutex::new(None),
    };
    connection
        .send_request(MessageType::Members, &(), std::time::Duration::from_secs(5))
        .await
}

/// Whether the node `id` has been taken out of its cluster: some node among
/// `peers` that is itself the leader has a membership without it. Only the
/// leader's word counts: it is the one that knows the current membership.
pub async fn removed_from_cluster(
    tls_config: &RaftTlsConfig,
    id: NodeId,
    peers: &[String],
) -> bool {
    for peer in peers {
        if let Ok(reply) = ask_members(tls_config, peer).await {
            if reply.leader == Some(reply.node_id) && !reply.members.contains(&id) {
                return true;
            }
        }
    }
    false
}

/// Tell the node at `addr` that it has been taken out of the cluster. The
/// leader stops sending to a node once it is out, so without this the node
/// would never hear of it. Tried a few times; a node that cannot be reached
/// is down, and has nothing to stop.
pub async fn notify_removed(tls_config: &RaftTlsConfig, addr: &str) {
    let connection = RaftNetworkConnection {
        target: 0,
        addr: Some(addr.to_string()),
        tls_config: tls_config.clone(),
        stream: tokio::sync::Mutex::new(None),
    };
    for _ in 0..5 {
        let sent: Result<bool, String> = connection
            .send_request(MessageType::Removed, &(), std::time::Duration::from_secs(5))
            .await;
        if sent.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// A node asking to be admitted to the cluster.
#[derive(Serialize, Deserialize)]
struct JoinRequest {
    /// Where the other nodes can reach its Raft port.
    addr: String,
    /// A number the asking process made up when it started. A node asks
    /// again if the answer was lost, with the same number; a node that
    /// lost its data and starts over asks with a new one.
    instance: u64,
}

/// What a member answers to a node asking to join.
#[derive(Debug, Serialize, Deserialize)]
pub enum JoinReply {
    /// Admitted, under this node ID. The data follows through Raft.
    Joined {
        node_id: NodeId,
    },
    /// Ask the leader instead (at this address, if known).
    NotLeader {
        leader: Option<String>,
    },
    /// Will not be done, however often it is asked.
    Refused(String),
    Failed(String),
}

/// Ask the member at `member` to admit a node whose Raft port is at
/// `own_addr`.
pub async fn request_join(
    tls_config: &RaftTlsConfig,
    member: &str,
    own_addr: &str,
    instance: u64,
) -> Result<JoinReply, String> {
    let connection = RaftNetworkConnection {
        target: 0,
        addr: Some(member.to_string()),
        tls_config: tls_config.clone(),
        stream: tokio::sync::Mutex::new(None),
    };
    connection
        .send_request(
            MessageType::Join,
            &JoinRequest {
                addr: own_addr.to_string(),
                instance,
            },
            std::time::Duration::from_secs(30),
        )
        .await
}

impl TryFrom<u8> for MessageType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            1 => Ok(Self::Vote),
            2 => Ok(Self::VoteResponse),
            3 => Ok(Self::AppendEntries),
            4 => Ok(Self::AppendEntriesResponse),
            5 => Ok(Self::Snapshot),
            6 => Ok(Self::SnapshotResponse),
            7 => Ok(Self::Join),
            8 => Ok(Self::Removed),
            9 => Ok(Self::Members),
            10 => Ok(Self::SnapshotStream),
            11 => Ok(Self::TransferLeader),
            _ => Err(()),
        }
    }
}

/// Network factory for creating Raft connections
#[derive(Clone)]
pub struct RaftNetworkFactoryImpl {
    tls_config: RaftTlsConfig,
    nodes: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

impl RaftNetworkFactoryImpl {
    pub fn new(tls_config: RaftTlsConfig) -> Self {
        Self {
            tls_config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_node(&self, id: NodeId, addr: SocketAddr) {
        self.nodes.write().insert(id, addr);
    }

    pub fn remove_node(&self, id: NodeId) {
        self.nodes.write().remove(&id);
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // A node's address is part of the membership every node replicates,
        // so that is where it is looked for. (Addresses given by hand, with
        // `add_node`, serve members recorded without one.)
        let addr = if node.addr.is_empty() {
            self.nodes.read().get(&target).map(SocketAddr::to_string)
        } else {
            Some(node.addr.clone())
        };
        RaftNetworkConnection {
            target,
            addr,
            tls_config: self.tls_config.clone(),
            stream: tokio::sync::Mutex::new(None),
        }
    }
}

type PeerStream = tokio_rustls::client::TlsStream<TcpStream>;

/// A single network connection to a Raft peer
pub struct RaftNetworkConnection {
    target: NodeId,
    /// `host:port`; a host name is resolved each time a connection is made.
    addr: Option<String>,
    /// The connection to the peer, kept between requests. Taken out for the
    /// length of a request and put back when the request has been answered,
    /// so a request that fails or is abandoned half way leaves none behind.
    stream: tokio::sync::Mutex<Option<PeerStream>>,
    tls_config: RaftTlsConfig,
}

impl RaftNetworkConnection {
    /// Open a connection to the peer.
    async fn connect(&self, addr: &str) -> Result<PeerStream, String> {
        let stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        // Each request is waited for: send it at once.
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        let connector = TlsConnector::from(self.tls_config.client_config());
        // Cluster certificates are issued for this name, whatever the address.
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").map_err(|e| e.to_string())?;
        connector
            .connect(server_name, stream)
            .await
            .map_err(|e| e.to_string())
    }

    /// Send one framed request and read the framed reply.
    async fn round_trip(stream: &mut PeerStream, request: &[u8]) -> std::io::Result<Vec<u8>> {
        stream.write_all(request).await?;
        stream.flush().await?;
        // Response: [len: u32][body]
        let len = stream.read_u32().await? as usize;
        let mut response = vec![0u8; len];
        stream.read_exact(&mut response).await?;
        Ok(response)
    }

    /// Send one request and read its reply, giving up after `limit`. A peer
    /// that has stopped answering must not hold up replication to it for
    /// ever: openraft leaves the time limit to the transport.
    async fn send_request<Req, Resp>(
        &self,
        msg_type: MessageType,
        request: &Req,
        limit: std::time::Duration,
    ) -> Result<Resp, String>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        match tokio::time::timeout(limit, self.exchange(msg_type, request)).await {
            Ok(result) => result,
            Err(_) => Err(describe(
                self.target,
                self.addr.as_deref(),
                &format!("{msg_type:?}"),
                format_args!("no reply within {limit:?}"),
            )),
        }
    }

    /// Send one request and read its reply. Any failure is reported as text.
    async fn exchange<Req, Resp>(
        &self,
        msg_type: MessageType,
        request: &Req,
    ) -> Result<Resp, String>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let operation = format!("{:?}", msg_type);
        let fail = |err: &dyn std::fmt::Display| {
            describe(self.target, self.addr.as_deref(), &operation, err)
        };
        let addr = self
            .addr
            .as_deref()
            .ok_or_else(|| fail(&"address not configured"))?;

        // Use the connection kept from the last request, or make one. A kept
        // connection may have been closed by the peer meanwhile (it
        // restarted, say); then the request is made once more on a new one.
        let body = bincode::serialize(request).map_err(|e| fail(&e))?;
        let len = u32::try_from(body.len()).map_err(|_| fail(&"request larger than 4 GiB"))?;
        let mut buf = BytesMut::with_capacity(5 + body.len());
        buf.put_u8(msg_type as u8);
        buf.put_u32(len);
        buf.put_slice(&body);

        let kept = self.stream.lock().await.take();
        let reused = kept.is_some();
        let mut tls_stream = match kept {
            Some(stream) => stream,
            None => self.connect(addr).await.map_err(|e| fail(&e))?,
        };
        let resp_buf = match Self::round_trip(&mut tls_stream, &buf).await {
            Ok(resp) => resp,
            Err(_) if reused => {
                tls_stream = self.connect(addr).await.map_err(|e| fail(&e))?;
                Self::round_trip(&mut tls_stream, &buf)
                    .await
                    .map_err(|e| fail(&e))?
            }
            Err(e) => return Err(fail(&e)),
        };
        *self.stream.lock().await = Some(tls_stream);
        bincode::deserialize(&resp_buf).map_err(|e| fail(&e))
    }
}

fn unreachable(message: String) -> Unreachable<TypeConfig> {
    Unreachable::<TypeConfig>::from_string(message)
}

impl RaftNetworkV2<TypeConfig> for RaftNetworkConnection {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        // The reply on the wire says only success, conflict or a higher vote;
        // the log positions it refers to are the ones we sent.
        let prev_log_id = req.prev_log_id;
        let last_log_id = req.last_log_id();
        let response: AppendEntriesResponse = self
            .send_request(MessageType::AppendEntries, &req, option.soft_ttl())
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))?;
        Ok(response.into_append_result(prev_log_id, last_log_id))
    }

    async fn vote(
        &mut self,
        req: VoteRequest,
        option: RPCOption,
    ) -> Result<VoteResponse, RPCError<TypeConfig>> {
        self.send_request(MessageType::Vote, &req, option.soft_ttl())
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }

    async fn transfer_leader(
        &mut self,
        req: openraft::raft::TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<openraft::raft::TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        // Sent once, by a leader that lets go as it sends: a request that
        // is lost to a slow round trip leaves the choice of the next leader
        // to chance. It is given more time than a heartbeat.
        let limit = option.soft_ttl().max(TRANSFER_LEADER_TIMEOUT);
        self.send_request(MessageType::TransferLeader, &req, limit)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError<TypeConfig>> {
        let SnapshotData::Stored(view) = snapshot.snapshot else {
            return Err(StreamingError::Unreachable(unreachable(
                "this node holds no snapshot contents to send".to_string(),
            )));
        };
        let header = SnapshotHeader {
            vote,
            meta: snapshot.meta,
        };
        // Stop as soon as openraft no longer wants this snapshot sent. The
        // whole transfer may take as long as it takes; only standing still
        // is an error.
        let _ = option;
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            result = self.send_snapshot(&header, view) => {
                result.map_err(|e| StreamingError::Unreachable(unreachable(e)))
            }
        }
    }
}

impl RaftNetworkConnection {
    /// Send a snapshot: its header, then its contents read from `view` a
    /// piece at a time, then wait for the receiver's answer.
    async fn send_snapshot(
        &self,
        header: &SnapshotHeader,
        view: Arc<dyn crate::storage::traits::SnapshotView>,
    ) -> Result<SnapshotResponse, String> {
        let operation = "Snapshot";
        let fail = |err: &dyn std::fmt::Display| {
            describe(self.target, self.addr.as_deref(), operation, err)
        };
        let step = |what: &'static str| {
            move |_| {
                describe(
                    self.target,
                    self.addr.as_deref(),
                    operation,
                    format_args!("stalled while {what}"),
                )
            }
        };
        let addr = self
            .addr
            .as_deref()
            .ok_or_else(|| fail(&"address not configured"))?;
        let stream = tokio::time::timeout(SNAPSHOT_STALL, TcpStream::connect(addr))
            .await
            .map_err(step("connecting"))?
            .map_err(|e| fail(&e))?;
        let connector = TlsConnector::from(self.tls_config.client_config());
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").map_err(|e| fail(&e))?;
        let mut stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| fail(&e))?;

        let body = bincode::serialize(header).map_err(|e| fail(&e))?;
        let mut frame = BytesMut::with_capacity(5 + body.len());
        frame.put_u8(MessageType::SnapshotStream as u8);
        frame.put_u32(body.len() as u32);
        frame.put_slice(&body);
        stream.write_all(&frame).await.map_err(|e| fail(&e))?;

        let mut after: Option<Vec<u8>> = None;
        loop {
            let (entries, done) = view
                .next_chunk(after.as_deref(), SNAPSHOT_CHUNK_BYTES)
                .await
                .map_err(|e| fail(&e))?;
            if let Some((last, _)) = entries.last() {
                after = Some(last.clone());
            }
            if !entries.is_empty() {
                let body = bincode::serialize(&entries).map_err(|e| fail(&e))?;
                let len = u32::try_from(body.len())
                    .map_err(|_| fail(&"snapshot frame larger than 4 GiB"))?;
                let write = async {
                    stream.write_u32(len).await?;
                    stream.write_all(&body).await
                };
                tokio::time::timeout(SNAPSHOT_STALL, write)
                    .await
                    .map_err(step("sending"))?
                    .map_err(|e| fail(&e))?;
            }
            if done {
                break;
            }
        }
        stream.write_u32(0).await.map_err(|e| fail(&e))?;
        stream.flush().await.map_err(|e| fail(&e))?;

        // The receiver switches to the new contents before it answers.
        let read = async {
            let len = stream.read_u32().await? as usize;
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await?;
            Ok::<_, std::io::Error>(body)
        };
        let body = tokio::time::timeout(SNAPSHOT_STALL * 10, read)
            .await
            .map_err(step("waiting for the answer"))?
            .map_err(|e| fail(&e))?;
        bincode::deserialize(&body).map_err(|e| fail(&e))
    }
}

/// RPC server handler - processes incoming Raft messages
pub struct RaftRpcHandler {
    raft: crate::raft::types::Raft,
    /// Set while this node is the only member of its cluster (see
    /// `RaftNode`); cleared here when a second node is admitted.
    alone: Arc<std::sync::atomic::AtomicBool>,
    /// Signalled when this node is told it has been removed.
    removed: Arc<tokio::sync::Notify>,
    /// For asking other nodes whether that is true.
    tls_config: RaftTlsConfig,
    /// Where a snapshot sent to this node is written.
    storage: Arc<dyn crate::storage::StorageEngine>,
    /// Held while a snapshot is being received.
    receiving_snapshot: tokio::sync::Mutex<()>,
    /// Held while a node is being admitted: Raft changes its membership
    /// one step at a time.
    admitting: Arc<tokio::sync::Mutex<()>>,
    /// Nodes admitted by this process: address, the instance that asked,
    /// and the ID it was given.
    admitted: parking_lot::Mutex<Vec<(String, u64, NodeId)>>,
}

impl RaftRpcHandler {
    pub fn new(
        raft: crate::raft::types::Raft,
        alone: Arc<std::sync::atomic::AtomicBool>,
        removed: Arc<tokio::sync::Notify>,
        tls_config: RaftTlsConfig,
        storage: Arc<dyn crate::storage::StorageEngine>,
    ) -> Self {
        Self {
            raft,
            alone,
            removed,
            tls_config,
            storage,
            receiving_snapshot: tokio::sync::Mutex::new(()),
            admitting: Arc::new(tokio::sync::Mutex::new(())),
            admitted: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Whether a request of this type is a snapshot, whose contents follow
    /// the request on the connection (see `receive_snapshot`).
    pub fn is_snapshot(msg_type: u8) -> bool {
        msg_type == MessageType::SnapshotStream as u8
    }

    /// Receive a snapshot: `header` has been read; the contents are read
    /// from `stream`, written out as a replacement for this node's storage,
    /// and handed to Raft, which switches to them. Returns the answer to
    /// send back.
    pub async fn receive_snapshot<S>(&self, header: &[u8], stream: &mut S) -> Result<Bytes, String>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        use openraft::async_runtime::WatchReceiver;
        use tokio::io::AsyncReadExt;
        let header: SnapshotHeader = bincode::deserialize(header).map_err(|e| e.to_string())?;
        // A snapshot from a leader this node has already moved on from
        // would be refused in the end; refuse it before any of it is
        // written.
        if header.vote < self.raft.metrics().borrow_watched().vote {
            return Err("the snapshot comes from a leader that has been superseded".to_string());
        }
        // One at a time: a second sender would only fill the disk.
        let Ok(_receiving) = self.receiving_snapshot.try_lock() else {
            return Err("another snapshot is being received".to_string());
        };
        let mut ingest = self
            .storage
            .clone()
            .ingest()
            .await
            .map_err(|e| e.to_string())?;
        loop {
            let frame = async {
                let len = stream.read_u32().await? as usize;
                // A frame is a batch of whole entries: about a chunk, or
                // one entry if that is larger. Nothing honest is bigger.
                if len > SNAPSHOT_FRAME_LIMIT {
                    return Err(std::io::Error::other("snapshot frame too large"));
                }
                let mut body = vec![0u8; len];
                stream.read_exact(&mut body).await?;
                Ok::<_, std::io::Error>(body)
            };
            let body = tokio::time::timeout(SNAPSHOT_STALL, frame)
                .await
                .map_err(|_| "the snapshot stopped arriving".to_string())?
                .map_err(|e| e.to_string())?;
            if body.is_empty() {
                break;
            }
            let entries: Vec<(Vec<u8>, Vec<u8>)> =
                bincode::deserialize(&body).map_err(|e| e.to_string())?;
            ingest.add(entries).await.map_err(|e| e.to_string())?;
        }
        let snapshot = Snapshot {
            meta: header.meta,
            snapshot: SnapshotData::Received(ingest),
        };
        let response = self
            .raft
            .install_full_snapshot(header.vote, snapshot)
            .await
            .map_err(|e| e.to_string())?;
        let encoded = bincode::serialize(&response).map_err(|e| e.to_string())?;
        Ok(Bytes::from(encoded))
    }

    /// This node and the addresses of the other nodes it knows of.
    fn peers(&self) -> (NodeId, Vec<String>) {
        use openraft::async_runtime::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let peers = metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| **id != metrics.id)
            .map(|(_, node)| node.addr.clone())
            .collect();
        (metrics.id, peers)
    }

    /// Admit a node that asks to join: give it a node ID, add it as a
    /// learner, and make it a voter once it has caught up. Only the leader
    /// can; any other member says who the leader is.
    async fn join(&self, request: JoinRequest) -> JoinReply {
        use openraft::async_runtime::WatchReceiver;

        let (id, leader, membership) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow_watched();
            (
                metrics.id,
                metrics.current_leader,
                metrics.membership_config.clone(),
            )
        };
        if leader != Some(id) {
            let leader = leader.and_then(|leader| {
                membership
                    .nodes()
                    .find(|(node, _)| **node == leader)
                    .map(|(_, node)| node.addr.clone())
            });
            return JoinReply::NotLeader { leader };
        }
        // Asked again by the same process (its first answer may have been
        // lost): the same answer.
        let asked_before = self
            .admitted
            .lock()
            .iter()
            .find(|(addr, instance, _)| *addr == request.addr && *instance == request.instance)
            .map(|(_, _, node_id)| *node_id);
        if let Some(node_id) = asked_before {
            return JoinReply::Joined { node_id };
        }
        // The address belongs to a member, and it is not that member asking
        // again: a node that lost its data, started over. It must not be
        // given the old node's place. The cluster counts that node's vote
        // and what it had acknowledged; a node with nothing in its place
        // can cost the cluster writes it has acknowledged.
        if let Some((node_id, _)) = membership
            .nodes()
            .find(|(_, node)| node.addr == request.addr)
        {
            return JoinReply::Refused(format!(
                "{} is the address of node {node_id}, which is a member already. \
                 If that node has lost its data, remove it first: \
                 ALTER CLUSTER REMOVE NODE {node_id}",
                request.addr
            ));
        }
        // Other nodes are told this node's address as the membership has
        // it. One that means "any address" would have them call themselves.
        let own_addr = membership
            .nodes()
            .find(|(node, _)| **node == id)
            .map(|(_, node)| node.addr.clone())
            .unwrap_or_default();
        if is_unspecified(&own_addr) {
            return JoinReply::Refused(format!(
                "the primary advertises {own_addr:?}, which other nodes cannot reach it at; \
                 restart it with --advertise-host"
            ));
        }

        let node_id = match self.raft.client_write(Command::AllocNodeId).await {
            Ok(response) => match response.data {
                CommandResponse::Ok(Some(bytes)) if bytes.len() == 8 => {
                    u64::from_le_bytes(bytes.try_into().expect("eight bytes"))
                }
                other => return JoinReply::Failed(format!("no node ID was allocated: {other:?}")),
            },
            Err(e) => return JoinReply::Failed(e.to_string()),
        };

        // No longer alone: this node needs its timers, to send heartbeats
        // and to notice if the others stop answering.
        self.alone.store(false, std::sync::atomic::Ordering::SeqCst);
        let runtime = self.raft.runtime_config();
        runtime.tick(true);
        runtime.heartbeat(true);
        runtime.elect(true);

        self.admitted
            .lock()
            .push((request.addr.clone(), request.instance, node_id));

        // The node is told its ID now and starts listening; meanwhile it is
        // sent the log (or a snapshot), and made a voter when it has it all.
        // One node at a time: Raft refuses a change of membership while
        // another is under way. And until it has worked: the node has been
        // told it is in.
        let raft = self.raft.clone();
        let node = Node::new(request.addr.clone());
        let admitting = self.admitting.clone();
        tokio::spawn(async move {
            let _one_at_a_time = admitting.lock().await;
            let voters = std::collections::BTreeSet::from([node_id]);
            let mut added = false;
            loop {
                if !raft.is_leader() {
                    tracing::error!(
                        node_id,
                        "No longer the leader: a joining node was not admitted"
                    );
                    return;
                }
                let step = if added {
                    raft.change_membership(
                        openraft::ChangeMembers::AddVoterIds(voters.clone()),
                        false,
                    )
                    .await
                    .map(|_| ())
                } else {
                    raft.add_learner(node_id, node.clone(), true)
                        .await
                        .map(|_| ())
                };
                match step {
                    Ok(()) if added => {
                        tracing::info!(node_id, "A node joined the cluster");
                        return;
                    }
                    Ok(()) => added = true,
                    Err(e) => {
                        tracing::warn!(node_id, error = %e, "Admitting a node: will try again");
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                }
            }
        });
        JoinReply::Joined { node_id }
    }

    /// Handle an incoming RPC request
    pub async fn handle_request(&self, msg_type: u8, body: &[u8]) -> Result<Bytes, String> {
        let msg_type = MessageType::try_from(msg_type)
            .map_err(|_| format!("Unknown message type: {}", msg_type))?;

        match msg_type {
            MessageType::Vote => {
                let req: VoteRequest = bincode::deserialize(body).map_err(|e| e.to_string())?;
                let resp = self.raft.vote(req).await.map_err(|e| e.to_string())?;
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::AppendEntries => {
                let req: AppendEntriesRequest =
                    bincode::deserialize(body).map_err(|e| e.to_string())?;
                let resp: AppendEntriesResponse = self
                    .raft
                    .append_entries(req)
                    .await
                    .map_err(|e| e.to_string())?
                    .into();
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::TransferLeader => {
                let req: openraft::raft::TransferLeaderRequest<TypeConfig> =
                    bincode::deserialize(body).map_err(|e| e.to_string())?;
                let resp = self
                    .raft
                    .handle_transfer_leader(req)
                    .await
                    .map_err(|e| e.to_string())?;
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::Removed => {
                // The message is a hint, not an order: a stray or forged
                // one must not stop a member. The node asks the leader
                // itself, and stops only if the leader's membership no
                // longer has it.
                let (id, peers) = self.peers();
                let tls_config = self.tls_config.clone();
                let removed = self.removed.clone();
                tokio::spawn(async move {
                    if removed_from_cluster(&tls_config, id, &peers).await {
                        removed.notify_one();
                    }
                });
                let encoded = bincode::serialize(&true).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::Members => {
                use openraft::async_runtime::WatchReceiver;
                let reply = {
                    let metrics = self.raft.metrics();
                    let metrics = metrics.borrow_watched();
                    MembersReply {
                        node_id: metrics.id,
                        leader: metrics.current_leader,
                        members: metrics
                            .membership_config
                            .nodes()
                            .map(|(id, _)| *id)
                            .collect(),
                    }
                };
                let encoded = bincode::serialize(&reply).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::Join => {
                let request: JoinRequest = bincode::deserialize(body).map_err(|e| e.to_string())?;
                let reply = self.join(request).await;
                let encoded = bincode::serialize(&reply).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            _ => Err(format!("Unexpected message type: {:?}", msg_type)),
        }
    }
}
