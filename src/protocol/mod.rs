//! Wire protocol implementations
//!
//! - `roodb`: the RooDB client protocol (MySQL-compatible) with STARTTLS
//! - `tds`: the TDS protocol spoken by SQL Server clients

pub mod roodb;
pub mod tds;

pub use roodb::RooDbConnection;
