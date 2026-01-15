//! RooDB handshake protocol
//!
//! Implements the initial handshake sequence (server greeting).

use rand::Rng;

use super::packet::encode_null_terminated_string;
use super::types::charset;

/// Server capabilities we advertise
pub mod capabilities {
    pub const CLIENT_LONG_PASSWORD: u32 = 0x00000001;
    pub const CLIENT_FOUND_ROWS: u32 = 0x00000002;
    pub const CLIENT_LONG_FLAG: u32 = 0x00000004;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;
    pub const CLIENT_NO_SCHEMA: u32 = 0x00000010;
    pub const CLIENT_COMPRESS: u32 = 0x00000020;
    pub const CLIENT_ODBC: u32 = 0x00000040;
    pub const CLIENT_LOCAL_FILES: u32 = 0x00000080;
    pub const CLIENT_IGNORE_SPACE: u32 = 0x00000100;
    pub const CLIENT_PROTOCOL_41: u32 = 0x00000200;
    pub const CLIENT_INTERACTIVE: u32 = 0x00000400;
    pub const CLIENT_SSL: u32 = 0x00000800;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 0x00001000;
    pub const CLIENT_TRANSACTIONS: u32 = 0x00002000;
    pub const CLIENT_RESERVED: u32 = 0x00004000;
    pub const CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;
    pub const CLIENT_MULTI_RESULTS: u32 = 0x00020000;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x00040000;
    pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
    pub const CLIENT_CONNECT_ATTRS: u32 = 0x00100000;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
    pub const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;
}

/// Server status flags
pub mod status_flags {
    pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
    pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
    pub const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;
    pub const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;
    pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
    pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
    pub const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;
    pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    pub const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;
    pub const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;
    pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
}

/// Authentication plugin name
pub const AUTH_PLUGIN_NAME: &str = "mysql_native_password";

/// Server version string
pub const SERVER_VERSION: &str = "8.0.0-RooDB";

/// Initial handshake packet (Protocol::HandshakeV10)
pub struct HandshakeV10 {
    /// Protocol version (always 10)
    pub protocol_version: u8,
    /// Server version string
    pub server_version: String,
    /// Connection ID (thread ID)
    pub connection_id: u32,
    /// First 8 bytes of auth plugin data (scramble)
    pub auth_plugin_data_part1: [u8; 8],
    /// Remaining 12 bytes of auth plugin data
    pub auth_plugin_data_part2: [u8; 12],
    /// Server capability flags (lower 16 bits)
    pub capability_flags_lower: u16,
    /// Server capability flags (upper 16 bits)
    pub capability_flags_upper: u16,
    /// Default character set
    pub character_set: u8,
    /// Server status flags
    pub status_flags: u16,
    /// Auth plugin name
    pub auth_plugin_name: String,
}

impl HandshakeV10 {
    /// Create a new handshake packet with random scramble
    pub fn new(connection_id: u32) -> Self {
        let mut rng = rand::thread_rng();

        // Generate 20-byte scramble (split into two parts)
        let mut auth_plugin_data_part1 = [0u8; 8];
        let mut auth_plugin_data_part2 = [0u8; 12];
        rng.fill(&mut auth_plugin_data_part1);
        rng.fill(&mut auth_plugin_data_part2);

        // Capabilities we support
        // Note: We do NOT advertise CLIENT_DEPRECATE_EOF for maximum compatibility
        let capabilities = capabilities::CLIENT_LONG_PASSWORD
            | capabilities::CLIENT_FOUND_ROWS
            | capabilities::CLIENT_LONG_FLAG
            | capabilities::CLIENT_CONNECT_WITH_DB
            | capabilities::CLIENT_PROTOCOL_41
            | capabilities::CLIENT_SSL // Required for STARTTLS
            | capabilities::CLIENT_TRANSACTIONS
            | capabilities::CLIENT_SECURE_CONNECTION
            | capabilities::CLIENT_PLUGIN_AUTH
            | capabilities::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;

        HandshakeV10 {
            protocol_version: 10,
            server_version: SERVER_VERSION.to_string(),
            connection_id,
            auth_plugin_data_part1,
            auth_plugin_data_part2,
            capability_flags_lower: (capabilities & 0xFFFF) as u16,
            capability_flags_upper: ((capabilities >> 16) & 0xFFFF) as u16,
            character_set: charset::UTF8MB4_GENERAL_CI,
            status_flags: status_flags::SERVER_STATUS_AUTOCOMMIT,
            auth_plugin_name: AUTH_PLUGIN_NAME.to_string(),
        }
    }

    /// Get the full 20-byte scramble
    pub fn scramble(&self) -> [u8; 20] {
        let mut result = [0u8; 20];
        result[..8].copy_from_slice(&self.auth_plugin_data_part1);
        result[8..].copy_from_slice(&self.auth_plugin_data_part2);
        result
    }

    /// Get full capability flags as u32
    pub fn capabilities(&self) -> u32 {
        (self.capability_flags_lower as u32) | ((self.capability_flags_upper as u32) << 16)
    }

    /// Encode the handshake packet
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);

        // Protocol version (1 byte)
        buf.push(self.protocol_version);

        // Server version (null-terminated)
        buf.extend_from_slice(&encode_null_terminated_string(&self.server_version));

        // Connection ID (4 bytes, little-endian)
        buf.extend_from_slice(&self.connection_id.to_le_bytes());

        // Auth plugin data part 1 (8 bytes)
        buf.extend_from_slice(&self.auth_plugin_data_part1);

        // Filler (1 byte)
        buf.push(0x00);

        // Capability flags lower (2 bytes)
        buf.extend_from_slice(&self.capability_flags_lower.to_le_bytes());

        // Character set (1 byte)
        buf.push(self.character_set);

        // Status flags (2 bytes)
        buf.extend_from_slice(&self.status_flags.to_le_bytes());

        // Capability flags upper (2 bytes)
        buf.extend_from_slice(&self.capability_flags_upper.to_le_bytes());

        // Auth plugin data length (1 byte) - 21 = 20 bytes + null terminator
        buf.push(21);

        // Reserved (10 bytes of zeros)
        buf.extend_from_slice(&[0u8; 10]);

        // Auth plugin data part 2 (12 bytes + null terminator = 13 bytes)
        buf.extend_from_slice(&self.auth_plugin_data_part2);
        buf.push(0x00);

        // Auth plugin name (null-terminated)
        buf.extend_from_slice(&encode_null_terminated_string(&self.auth_plugin_name));

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_v10_new() {
        let handshake = HandshakeV10::new(1);
        assert_eq!(handshake.protocol_version, 10);
        assert_eq!(handshake.connection_id, 1);
        assert_eq!(handshake.server_version, SERVER_VERSION);
        assert_eq!(handshake.auth_plugin_name, AUTH_PLUGIN_NAME);
    }

    #[test]
    fn test_handshake_v10_scramble() {
        let handshake = HandshakeV10::new(1);
        let scramble = handshake.scramble();
        assert_eq!(scramble.len(), 20);

        // Scramble should be random (very unlikely to be all zeros)
        assert!(scramble.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_handshake_v10_encode() {
        let handshake = HandshakeV10::new(42);
        let encoded = handshake.encode();

        // Verify protocol version
        assert_eq!(encoded[0], 10);

        // Verify server version starts after protocol version
        assert!(encoded[1..].starts_with(SERVER_VERSION.as_bytes()));
    }

    #[test]
    fn test_capabilities() {
        let handshake = HandshakeV10::new(1);
        let caps = handshake.capabilities();

        // Check key capabilities are set
        assert!(caps & capabilities::CLIENT_PROTOCOL_41 != 0);
        assert!(caps & capabilities::CLIENT_SECURE_CONNECTION != 0);
        assert!(caps & capabilities::CLIENT_PLUGIN_AUTH != 0);
    }
}
