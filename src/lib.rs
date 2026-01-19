//! RooDB - A distributed SQL database written in Rust
//!
//! Features:
//! - Raft consensus for distributed replication
//! - MySQL wire protocol compatibility
//! - io_uring on Linux, POSIX fallback elsewhere

pub mod catalog;
pub mod executor;
pub mod init;
pub mod io;
pub mod planner;
pub mod protocol;
pub mod raft;
pub mod server;
pub mod sql;
pub mod storage;
pub mod tls;
pub mod txn;
