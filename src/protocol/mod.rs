//! Wire protocol implementations
//!
//! Supports RooDB client protocol (MySQL-compatible) and TDS protocol over TLS.

pub mod roodb;
pub mod tds;

pub use roodb::RooDbConnection;
pub use tds::TdsConnection;
