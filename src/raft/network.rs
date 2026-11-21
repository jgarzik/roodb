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

/// A whole snapshot as it travels between nodes.
#[derive(Serialize, Deserialize)]
struct SnapshotMessage {
    vote: Vote,
    meta: SnapshotMeta,
    data: Vec<u8>,
}

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
    Failed(String),
}

/// Ask the member at `member` to admit a node whose Raft port is at
/// `own_addr`.
pub async fn request_join(
    tls_config: &RaftTlsConfig,
    member: &str,
    own_addr: &str,
) -> Result<JoinReply, String> {
    let connection = RaftNetworkConnection {
        target: 0,
        addr: Some(member.to_string()),
        tls_config: tls_config.clone(),
    };
    connection
        .send_request(
            MessageType::Join,
            &JoinRequest {
                addr: own_addr.to_string(),
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
        }
    }
}

/// A single network connection to a Raft peer
pub struct RaftNetworkConnection {
    target: NodeId,
    /// `host:port`; a host name is resolved each time a connection is made.
    addr: Option<String>,
    tls_config: RaftTlsConfig,
}

impl RaftNetworkConnection {
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

        // Connect with TLS
        let stream = TcpStream::connect(addr).await.map_err(|e| fail(&e))?;
        let connector = TlsConnector::from(self.tls_config.client_config());
        // Cluster certificates are issued for this name, whatever the address.
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").map_err(|e| fail(&e))?;
        let mut tls_stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| fail(&e))?;

        let body = bincode::serialize(request).map_err(|e| fail(&e))?;

        // Send: [msg_type: u8][len: u32][body]
        let len = u32::try_from(body.len()).map_err(|_| fail(&"request larger than 4 GiB"))?;
        let mut buf = BytesMut::with_capacity(5 + body.len());
        buf.put_u8(msg_type as u8);
        buf.put_u32(len);
        buf.put_slice(&body);
        tls_stream.write_all(&buf).await.map_err(|e| fail(&e))?;
        tls_stream.flush().await.map_err(|e| fail(&e))?;

        // Read response: [len: u32][body]
        let mut len_buf = [0u8; 4];
        tls_stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| fail(&e))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; len];
        tls_stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| fail(&e))?;

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

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError<TypeConfig>> {
        let message = SnapshotMessage {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        // Stop as soon as openraft no longer wants this snapshot sent.
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            result = self.send_request(MessageType::Snapshot, &message, option.soft_ttl()) => {
                result.map_err(|e| StreamingError::Unreachable(unreachable(e)))
            }
        }
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
}

impl RaftRpcHandler {
    pub fn new(
        raft: crate::raft::types::Raft,
        alone: Arc<std::sync::atomic::AtomicBool>,
        removed: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            raft,
            alone,
            removed,
        }
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
        // Asked again by a node that is already in (its first answer may
        // have been lost): the same answer.
        if let Some((node_id, _)) = membership
            .nodes()
            .find(|(_, node)| node.addr == request.addr)
        {
            return JoinReply::Joined { node_id: *node_id };
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

        // The node is told its ID now and starts listening; meanwhile it is
        // sent the log (or a snapshot), and made a voter when it has it all.
        let raft = self.raft.clone();
        let node = Node::new(request.addr.clone());
        tokio::spawn(async move {
            if let Err(e) = raft.add_learner(node_id, node, true).await {
                tracing::error!(node_id, error = %e, "A joining node could not be added");
                return;
            }
            let voters = std::collections::BTreeSet::from([node_id]);
            match raft
                .change_membership(openraft::ChangeMembers::AddVoterIds(voters), false)
                .await
            {
                Ok(_) => tracing::info!(node_id, "A node joined the cluster"),
                Err(e) => {
                    tracing::error!(node_id, error = %e, "A joining node could not be made a voter");
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
            MessageType::Snapshot => {
                let message: SnapshotMessage =
                    bincode::deserialize(body).map_err(|e| e.to_string())?;
                let snapshot = Snapshot {
                    meta: message.meta,
                    snapshot: std::io::Cursor::new(message.data),
                };
                let resp = self
                    .raft
                    .install_full_snapshot(message.vote, snapshot)
                    .await
                    .map_err(|e| e.to_string())?;
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::Removed => {
                // Only a node that is really out acts on it: a stray or
                // forged message must not stop a member. The leader sends
                // this after the removal is committed, so by the time it is
                // believed here the cluster no longer counts on this node.
                use openraft::async_runtime::WatchReceiver;
                let still_led = {
                    let metrics = self.raft.metrics();
                    let metrics = metrics.borrow_watched();
                    metrics.state.is_leader()
                };
                if !still_led {
                    self.removed.notify_one();
                }
                let encoded = bincode::serialize(&true).map_err(|e| e.to_string())?;
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
