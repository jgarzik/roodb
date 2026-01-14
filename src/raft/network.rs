//! Raft network implementation over TLS

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::raft::types::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Node, NodeId, TypeConfig, VoteRequest, VoteResponse,
};
use crate::tls::TlsConfig;

/// Message types for Raft RPC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MessageType {
    Vote = 1,
    VoteResponse = 2,
    AppendEntries = 3,
    AppendEntriesResponse = 4,
    InstallSnapshot = 5,
    InstallSnapshotResponse = 6,
}

impl TryFrom<u8> for MessageType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            1 => Ok(Self::Vote),
            2 => Ok(Self::VoteResponse),
            3 => Ok(Self::AppendEntries),
            4 => Ok(Self::AppendEntriesResponse),
            5 => Ok(Self::InstallSnapshot),
            6 => Ok(Self::InstallSnapshotResponse),
            _ => Err(()),
        }
    }
}

/// Network factory for creating Raft connections
#[derive(Clone)]
pub struct RaftNetworkFactoryImpl {
    tls_config: TlsConfig,
    nodes: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

impl RaftNetworkFactoryImpl {
    pub fn new(tls_config: TlsConfig) -> Self {
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
    tls_config: TlsConfig,
}

impl RaftNetworkConnection {
    async fn send_request<Req, Resp, E>(
        &self,
        msg_type: MessageType,
        request: &Req,
    ) -> Result<Resp, RPCError<NodeId, Node, RaftError<NodeId, E>>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
        E: std::error::Error,
    {
        let addr = self.addr.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Node {} address not found", self.target),
            ))))
        })?;

        // Connect with TLS
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;

        let connector = TlsConnector::from(self.tls_config.client_config());
        let server_name = "localhost".try_into().unwrap();
        let mut tls_stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;

        // Serialize request
        let body = bincode::serialize(request).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))))
        })?;

        // Send: [msg_type: u8][len: u32][body]
        let mut buf = BytesMut::with_capacity(5 + body.len());
        buf.put_u8(msg_type as u8);
        buf.put_u32(body.len() as u32);
        buf.put_slice(&body);

        tls_stream
            .write_all(&buf)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;
        tls_stream
            .flush()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;

        // Read response: [len: u32][body]
        let mut len_buf = [0u8; 4];
        tls_stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; len];
        tls_stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e))))?;

        // Deserialize response
        let resp: Resp = bincode::deserialize(&resp_buf).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))))
        })?;

        Ok(resp)
    }
}

impl RaftNetwork<TypeConfig> for RaftNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError<NodeId, Node, RaftError<NodeId>>> {
        self.send_request(MessageType::AppendEntries, &req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError<NodeId, Node, RaftError<NodeId>>> {
        self.send_request(MessageType::Vote, &req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.send_request(MessageType::InstallSnapshot, &req).await
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
                let resp = self
                    .raft
                    .append_entries(req)
                    .await
                    .map_err(|e| e.to_string())?;
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            MessageType::InstallSnapshot => {
                let req: InstallSnapshotRequest =
                    bincode::deserialize(body).map_err(|e| e.to_string())?;
                let resp = self
                    .raft
                    .install_snapshot(req)
                    .await
                    .map_err(|e| e.to_string())?;
                let encoded = bincode::serialize(&resp).map_err(|e| e.to_string())?;
                Ok(Bytes::from(encoded))
            }
            _ => Err(format!("Unexpected message type: {:?}", msg_type)),
        }
    }
}
