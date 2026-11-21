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
    addr: Option<SocketAddr>,
    operation: &str,
    err: impl std::fmt::Display,
) -> String {
    let addr = addr.map_or_else(|| "unknown".to_string(), |a| a.to_string());
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

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        let addr = self.nodes.read().get(&target).copied();
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
    addr: Option<SocketAddr>,
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
                self.addr,
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
        let fail = |err: &dyn std::fmt::Display| describe(self.target, self.addr, &operation, err);

        let addr = self.addr.ok_or_else(|| fail(&"address not configured"))?;

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
}

impl RaftRpcHandler {
    pub fn new(raft: crate::raft::types::Raft) -> Self {
        Self { raft }
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
            _ => Err(format!("Unexpected message type: {:?}", msg_type)),
        }
    }
}
