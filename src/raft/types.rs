//! Raft type configuration and aliases

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
    /// Row IDs below `upto` (the node-local counter) are reserved for
    /// `node_id`. A node records this before handing any of them out, so that
    /// after a restart it can resume past every ID it may have used.
    ReserveRowIds { node_id: NodeId, upto: u64 },
}

// ============ MVCC header layout constants ============
// Format: [DB_TRX_ID:8][DB_ROLL_PTR:8][deleted:1][row_data...]

/// Total size of the MVCC header prefix (8 + 8 + 1 = 17 bytes)
pub const MVCC_HEADER_SIZE: usize = 17;

/// Byte offset of the deleted/tombstone flag within the MVCC header
pub const MVCC_DELETED_OFFSET: usize = 16;

/// Check whether an MVCC-encoded row is a tombstone (deleted).
pub fn is_mvcc_tombstone(data: &[u8]) -> bool {
    data.len() > MVCC_DELETED_OFFSET && data[MVCC_DELETED_OFFSET] == 1
}

/// Response from applying a command
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResponse {
    /// Success with optional value
    Ok(Option<Vec<u8>>),
    /// Error message
    Error(String),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::DataChange(changes) => write!(
                f,
                "DataChange(txn {}, {} changes)",
                changes.txn_id,
                changes.changes.len()
            ),
            Command::Noop => f.write_str("Noop"),
            Command::ReserveRowIds { node_id, upto } => {
                write!(f, "ReserveRowIds(node {node_id}, upto {upto})")
            }
        }
    }
}

// Everything not named here takes openraft's default.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        Node = Node,
        NodeId = NodeId,
);

/// What a snapshot's data is carried in.
pub type SnapshotData = Cursor<Vec<u8>>;

/// Raft instance type alias
pub type Raft = openraft::Raft<TypeConfig, crate::raft::LsmRaftStorage>;

/// Vote type alias
pub type Vote = openraft::alias::VoteOf<TypeConfig>;

/// Log ID type alias
pub type LogId = openraft::alias::LogIdOf<TypeConfig>;

/// Entry type alias
pub type Entry = openraft::alias::EntryOf<TypeConfig>;

/// Snapshot meta type alias
pub type SnapshotMeta = openraft::alias::SnapshotMetaOf<TypeConfig>;

/// Snapshot type alias
pub type Snapshot = openraft::alias::SnapshotOf<TypeConfig, SnapshotData>;

/// Stored membership type alias
pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;

/// Log state type alias
pub type LogState = openraft::storage::LogState<TypeConfig>;

/// RPC types
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
