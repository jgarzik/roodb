//! TDS (Tabular Data Stream) protocol server implementation
//!
//! Implements the server side of the TDS 8.0 protocol, allowing
//! SQL Server clients to connect to RooDB.

pub mod codec;
pub mod connection;
pub mod login;
pub mod params;
pub mod prelogin;
pub mod token;
pub mod types;

pub use connection::TdsConnection;
