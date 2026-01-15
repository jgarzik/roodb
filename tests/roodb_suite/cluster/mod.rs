//! Cluster and replication tests.
//!
//! Note: SQL-level replication through Raft is not yet implemented.
//! The Raft consensus layer is separate from the SQL storage layer.
//! These tests verify basic Raft cluster behavior.
//!
//! For full SQL replication, the storage engine would need to be
//! integrated with Raft to replicate SQL operations across nodes.

mod replication;
