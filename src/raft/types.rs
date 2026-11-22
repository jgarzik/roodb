//! Raft type configuration and aliases

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
    /// Row IDs below `upto` are reserved. The primary records this before
    /// it commits anything that uses them, so that after a restart, or on
    /// another node that becomes primary, IDs continue past every ID that
    /// may have been used. One counter for the cluster: the rows of a
    /// table without a key stay in the order they were inserted, whichever
    /// node inserted them.
    ReserveRowIds { upto: u64 },
    /// What `roodb_init` wrote into the first node's storage before there
    /// was a log: stored keys and values, as they are. It is the first
    /// thing proposed when the cluster is formed, so that the log accounts
    /// for everything in storage and a node that joins later, with nothing,
    /// ends up with the same.
    Seed(Vec<(Vec<u8>, Vec<u8>)>),
    /// Hand out the next node ID. The response carries it. IDs are never
    /// reused, also not after a node has left.
    AllocNodeId,
}

// ============ MVCC header layout constants ============
// Format: [DB_TRX_ID:8][DB_ROLL_PTR:8][deleted:1][row_data...]

/// Total size of the MVCC header prefix (8 + 8 + 1 = 17 bytes)
pub const MVCC_HEADER_SIZE: usize = 17;

/// Byte offset of the deleted/tombstone flag within the MVCC header
pub const MVCC_DELETED_OFFSET: usize = 16;

/// Encode row data with its MVCC header, as rows are stored.
///
/// Format: [DB_TRX_ID:8][DB_ROLL_PTR:8][deleted:1][row_data]
pub fn encode_mvcc_row(txn_id: u64, deleted: bool, data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(MVCC_HEADER_SIZE + data.len());
    result.extend_from_slice(&txn_id.to_le_bytes()); // DB_TRX_ID
    result.extend_from_slice(&0u64.to_le_bytes()); // DB_ROLL_PTR = 0 (no version chain)
    result.push(u8::from(deleted));
    result.extend_from_slice(data);
    result
}

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
            Command::ReserveRowIds { upto } => {
                write!(f, "ReserveRowIds(upto {upto})")
            }
            Command::Seed(entries) => write!(f, "Seed({} entries)", entries.len()),
            Command::AllocNodeId => f.write_str("AllocNodeId"),
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

/// The contents of a snapshot, as far as a node holds them.
pub enum SnapshotData {
    /// No contents. A node that is alone checkpoints by flushing, and no
    /// other node will ever ask for the contents.
    None,
    /// On the node that sends it: a view of its storage at the snapshot's
    /// position. Nothing is copied until the snapshot is sent, and then it
    /// is read and sent a piece at a time.
    Stored(std::sync::Arc<dyn crate::storage::traits::SnapshotView>),
    /// On the node that receives it: the contents, already written out as
    /// a replacement for everything it holds, waiting to be switched to.
    Received(Box<dyn crate::storage::traits::Ingest>),
}

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
