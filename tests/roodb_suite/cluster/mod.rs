//! Cluster and replication tests.
//!
//! These run `RaftNode` instances inside the test process and replicate row
//! changes between them. No test yet runs SQL against a cluster of server
//! processes: the server binary starts as a single node.

mod replication;
