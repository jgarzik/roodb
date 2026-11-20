//! TDS (Tabular Data Stream) protocol server implementation
//!
//! Lets SQL Server clients connect to RooDB. The server speaks the TDS 7.4
//! token stream over two transports: TDS 8.0 ("strict", TLS first) and
//! TDS 7.x with the TLS handshake tunnelled after PRELOGIN. Encryption is
//! mandatory on both.

pub mod codec;
