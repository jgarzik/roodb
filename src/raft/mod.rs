//! Raft consensus module using OpenRaft

mod changes;
mod lsm_storage;
mod network;
mod node;
mod types;

// Keep MemStorage available for tests only
#[cfg(test)]
mod storage;

pub use changes::{ChangeOp, ChangeSet, RowChange};
pub use lsm_storage::LsmRaftStorage;
pub use network::{RaftNetworkFactoryImpl, RaftRpcHandler};
pub use node::RaftNode;
pub use types::*;

#[cfg(test)]
pub use storage::MemStorage;
