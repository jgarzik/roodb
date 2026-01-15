//! Wire protocol implementations
//!
//! Currently supports RooDB client protocol over TLS.

pub mod roodb;

pub use roodb::RooDbConnection;
