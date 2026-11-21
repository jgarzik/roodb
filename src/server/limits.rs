//! Limits applied to clients before they have authenticated.
//!
//! Anyone who can reach a listener can open connections, so until login
//! succeeds the server bounds both how long a client may take and how much
//! it may make the server buffer.

use std::time::Duration;

/// Time allowed from accepting a connection to completing authentication
/// (protocol greeting, TLS handshake and login).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest message accepted from an unauthenticated client. Login messages
/// are small: credentials, a database name and client attributes.
pub const MAX_PREAUTH_MESSAGE: usize = 1024 * 1024;
