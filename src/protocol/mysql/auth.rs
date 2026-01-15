//! MySQL authentication
//!
//! Implements mysql_native_password authentication.

use sha1::{Digest, Sha1};

use super::error::{ProtocolError, ProtocolResult};
use super::handshake::capabilities;
use super::packet::{decode_length_encoded_string, decode_null_terminated_string};

/// SSL Request packet from client (sent before TLS upgrade)
///
/// This is a truncated HandshakeResponse41 containing only:
/// - Capability flags (with CLIENT_SSL set)
/// - Max packet size
/// - Character set
/// - Reserved bytes
#[derive(Debug)]
pub struct SslRequest {
    /// Client capability flags (must include CLIENT_SSL)
    pub capability_flags: u32,
    /// Maximum packet size client can handle
    pub max_packet_size: u32,
    /// Character set
    pub character_set: u8,
}

impl SslRequest {
    /// Parse an SSL request packet (32 bytes)
    pub fn parse(data: &[u8]) -> ProtocolResult<Self> {
        if data.len() < 32 {
            return Err(ProtocolError::InvalidPacket(
                "SSL request too short".to_string(),
            ));
        }

        // Capability flags (4 bytes)
        let capability_flags = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        // Max packet size (4 bytes)
        let max_packet_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

        // Character set (1 byte)
        let character_set = data[8];

        // Remaining 23 bytes are reserved (zeros)

        Ok(SslRequest {
            capability_flags,
            max_packet_size,
            character_set,
        })
    }

    /// Check if client requested SSL
    pub fn has_ssl(&self) -> bool {
        self.capability_flags & capabilities::CLIENT_SSL != 0
    }
}

/// Handshake response from client (Protocol 4.1)
#[derive(Debug)]
pub struct HandshakeResponse41 {
    /// Client capability flags
    pub capability_flags: u32,
    /// Maximum packet size client can handle
    pub max_packet_size: u32,
    /// Character set
    pub character_set: u8,
    /// Username
    pub username: String,
    /// Auth response (scrambled password)
    pub auth_response: Vec<u8>,
    /// Database name (if CLIENT_CONNECT_WITH_DB)
    pub database: Option<String>,
    /// Auth plugin name (if CLIENT_PLUGIN_AUTH)
    pub auth_plugin_name: Option<String>,
    /// Connection attributes (if CLIENT_CONNECT_ATTRS)
    pub connect_attrs: Vec<(String, String)>,
}

impl HandshakeResponse41 {
    /// Parse a handshake response packet
    pub fn parse(data: &[u8]) -> ProtocolResult<Self> {
        if data.len() < 32 {
            return Err(ProtocolError::InvalidPacket(
                "handshake response too short".to_string(),
            ));
        }

        let mut offset = 0;

        // Capability flags (4 bytes)
        let capability_flags = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Max packet size (4 bytes)
        let max_packet_size = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Character set (1 byte)
        let character_set = data[offset];
        offset += 1;

        // Reserved (23 bytes of zeros)
        offset += 23;

        // Username (null-terminated)
        let (username, consumed) = decode_null_terminated_string(&data[offset..])?;
        offset += consumed;

        // Auth response
        let auth_response =
            if capability_flags & capabilities::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
                // Length-encoded auth response
                let (s, consumed) = decode_length_encoded_string(&data[offset..])?;
                offset += consumed;
                s.into_bytes()
            } else if capability_flags & capabilities::CLIENT_SECURE_CONNECTION != 0 {
                // Length-prefixed auth response (1-byte length)
                if offset >= data.len() {
                    Vec::new()
                } else {
                    let len = data[offset] as usize;
                    offset += 1;
                    if offset + len > data.len() {
                        return Err(ProtocolError::InvalidPacket(
                            "auth response truncated".to_string(),
                        ));
                    }
                    let response = data[offset..offset + len].to_vec();
                    offset += len;
                    response
                }
            } else {
                // Null-terminated auth response
                let (s, consumed) = decode_null_terminated_string(&data[offset..])?;
                offset += consumed;
                s.into_bytes()
            };

        // Database (if CLIENT_CONNECT_WITH_DB)
        let database = if capability_flags & capabilities::CLIENT_CONNECT_WITH_DB != 0
            && offset < data.len()
        {
            let (db, consumed) = decode_null_terminated_string(&data[offset..])?;
            offset += consumed;
            if db.is_empty() {
                None
            } else {
                Some(db)
            }
        } else {
            None
        };

        // Auth plugin name (if CLIENT_PLUGIN_AUTH)
        let auth_plugin_name =
            if capability_flags & capabilities::CLIENT_PLUGIN_AUTH != 0 && offset < data.len() {
                let (name, _consumed) = decode_null_terminated_string(&data[offset..])?;
                // offset += consumed; // Don't need to track further
                if name.is_empty() {
                    None
                } else {
                    Some(name)
                }
            } else {
                None
            };

        // Skip connection attributes for now

        Ok(HandshakeResponse41 {
            capability_flags,
            max_packet_size,
            character_set,
            username,
            auth_response,
            database,
            auth_plugin_name,
            connect_attrs: Vec::new(),
        })
    }

    /// Check if client requested a specific capability
    pub fn has_capability(&self, cap: u32) -> bool {
        self.capability_flags & cap != 0
    }
}

/// Verify mysql_native_password authentication
///
/// The auth response is computed as:
/// SHA1(password) XOR SHA1(scramble || SHA1(SHA1(password)))
pub fn verify_mysql_native_password(scramble: &[u8], password: &str, auth_response: &[u8]) -> bool {
    if password.is_empty() {
        // Empty password = empty auth response
        return auth_response.is_empty();
    }

    if auth_response.len() != 20 {
        return false;
    }

    // Expected response:
    // SHA1(password) XOR SHA1(scramble || SHA1(SHA1(password)))

    // Step 1: SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // Step 2: SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(stage1);
    let stage2 = hasher.finalize();

    // Step 3: SHA1(scramble || stage2)
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(stage2);
    let scrambled = hasher.finalize();

    // Step 4: stage1 XOR scrambled
    let mut expected = [0u8; 20];
    for i in 0..20 {
        expected[i] = stage1[i] ^ scrambled[i];
    }

    expected == auth_response
}

/// Compute mysql_native_password auth response (for testing)
#[cfg(test)]
pub fn compute_auth_response(scramble: &[u8], password: &str) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // Step 1: SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // Step 2: SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(stage1);
    let stage2 = hasher.finalize();

    // Step 3: SHA1(scramble || stage2)
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(stage2);
    let scrambled = hasher.finalize();

    // Step 4: stage1 XOR scrambled
    let mut result = Vec::with_capacity(20);
    for i in 0..20 {
        result.push(stage1[i] ^ scrambled[i]);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_empty_password() {
        let scramble = [0u8; 20];
        assert!(verify_mysql_native_password(&scramble, "", &[]));
        assert!(!verify_mysql_native_password(&scramble, "", &[1, 2, 3]));
    }

    #[test]
    fn test_verify_password() {
        let scramble = b"12345678901234567890";
        let password = "secret";

        let auth_response = compute_auth_response(scramble, password);
        assert_eq!(auth_response.len(), 20);

        // Correct password should verify
        assert!(verify_mysql_native_password(
            scramble,
            password,
            &auth_response
        ));

        // Wrong password should fail
        assert!(!verify_mysql_native_password(
            scramble,
            "wrong",
            &auth_response
        ));

        // Corrupted response should fail
        let mut bad_response = auth_response.clone();
        bad_response[0] ^= 0xff;
        assert!(!verify_mysql_native_password(
            scramble,
            password,
            &bad_response
        ));
    }

    #[test]
    fn test_parse_handshake_response() {
        // Minimal handshake response packet
        let mut packet = vec![0u8; 32];

        // Capability flags (4 bytes) - CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
        let caps = capabilities::CLIENT_PROTOCOL_41 | capabilities::CLIENT_SECURE_CONNECTION;
        packet[0..4].copy_from_slice(&caps.to_le_bytes());

        // Max packet size (4 bytes)
        packet[4..8].copy_from_slice(&(16_777_215u32).to_le_bytes());

        // Character set (1 byte)
        packet[8] = 45;

        // Reserved (23 bytes) - already zeros

        // Username (null-terminated)
        packet.extend_from_slice(b"root\0");

        // Auth response length (1 byte) + empty response
        packet.push(0);

        let response = HandshakeResponse41::parse(&packet).unwrap();
        assert_eq!(response.username, "root");
        assert_eq!(response.capability_flags, caps);
        assert!(response.auth_response.is_empty());
    }
}
