//! Shared server state handed to every client connection.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::catalog::Catalog;
use crate::raft::RaftNode;
use crate::server::connections::Connections;
use crate::storage::StorageEngine;
use crate::txn::TransactionManager;

/// Server-wide state shared by all listeners and connections.
///
/// Cloning is cheap: every field is reference counted. Connection IDs are
/// allocated from one counter so they stay unique across every listener;
/// advisory locks and session bookkeeping are keyed by them.
#[derive(Clone)]
pub struct ServerContext {
    /// Storage engine
    pub storage: Arc<dyn StorageEngine>,
    /// Schema catalog
    pub catalog: Arc<RwLock<Catalog>>,
    /// Transaction manager
    pub txn_manager: Arc<TransactionManager>,
    /// Raft node for consensus
    pub raft_node: Arc<RaftNode>,
    /// Next connection ID
    next_conn_id: Arc<AtomicU32>,
    /// The open connections, for `KILL`
    pub connections: Arc<Connections>,
}

impl ServerContext {
    /// Create the shared server state.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        Self {
            storage,
            catalog,
            txn_manager,
            raft_node,
            next_conn_id: Arc::new(AtomicU32::new(1)),
            connections: Arc::new(Connections::default()),
        }
    }

    /// Allocate a connection ID, unique across all listeners.
    pub fn next_connection_id(&self) -> u32 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }
}
