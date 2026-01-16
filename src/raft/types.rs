//! Type aliases for OpenRaft 0.9

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use super::ChangeSet;

/// Node identifier
pub type NodeId = u64;

/// Node type (address info)
pub type Node = BasicNode;

/// Raft log entry data - commands to be replicated
///
/// SQL operations produce row-level changes that are replicated through Raft.
/// When a log entry is committed, the changes are applied to the local LSM storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Command {
    /// Data changes from SQL DML/DDL operations
    ///
    /// Contains row-level changes collected during transaction execution.
    /// These are applied to the LSM storage when the log entry is committed.
    DataChange(ChangeSet),
    /// No-op for leader election
    Noop,
}

/// Response from applying a command
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResponse {
    /// Success with optional value
    Ok(Option<Vec<u8>>),
    /// Error message
    Error(String),
}

// Use the macro to declare type config properly
openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        Node = Node,
        NodeId = NodeId,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Raft instance type alias
pub type Raft = openraft::Raft<TypeConfig>;

/// Vote type alias
pub type Vote = openraft::Vote<NodeId>;

/// Log ID type alias
pub type LogId = openraft::LogId<NodeId>;

/// Entry type alias
pub type Entry = openraft::Entry<TypeConfig>;

/// Snapshot meta type alias
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;

/// Snapshot type alias
pub type Snapshot = openraft::storage::Snapshot<TypeConfig>;

/// Storage error type alias
pub type StorageError = openraft::StorageError<NodeId>;

/// Stored membership type alias
pub type StoredMembership = openraft::StoredMembership<NodeId, Node>;

/// Membership type alias
pub type Membership = openraft::Membership<NodeId, Node>;

/// Log state type alias
pub type LogState = openraft::storage::LogState<TypeConfig>;

/// RPC types - these use TypeConfig which has associated types
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
/// Response to append entries - uses NodeId directly
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;
pub type VoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type VoteResponse = openraft::raft::VoteResponse<NodeId>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;
