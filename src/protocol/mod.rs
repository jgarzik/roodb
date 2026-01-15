//! Wire protocol implementations
//!
//! Currently supports MySQL protocol over TLS.

pub mod mysql;

pub use mysql::MySqlConnection;
