//! RooDB integration test suite.
//!
//! - ddl, dml, queries, types, errors, edge_cases, prepared, auth: SQL through
//!   the `mysql_async` client, against one shared server process
//! - tds: the same server through TDS clients
//! - protocol_tests: the RooDB wire protocol
//! - crash, restart: durability across a kill and a restart
//! - raft_cluster, cluster: Raft nodes inside the test process

mod harness;

pub mod auth;
pub mod cluster;
pub mod cluster_binary;
pub mod crash;
pub mod ddl;
pub mod dml;
pub mod edge_cases;
pub mod errors;
pub mod kill;
pub mod locking;
pub mod metadata_locks;
pub mod prepared;
pub mod protocol_tests;
pub mod queries;
pub mod raft_cluster;
pub mod restart;
pub mod tds;
pub mod types;

pub use harness::TestServer;
