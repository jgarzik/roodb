//! TDS LOGIN7 message parsing.

/// Parse a LOGIN7 message from the client.
///
/// LOGIN7 structure (from MS-TDS spec):
/// - Bytes 0-3: Total length (LE u32)
/// - Bytes 4-7: TDS Version (BE u32)
/// - Bytes 8-11: Packet size (LE u32)
/// - Bytes 12-15: Client prog version (LE u32)
/// - Bytes 16-19: Client PID (LE u32)
/// - Bytes 20-23: Connection ID (LE u32)
/// - Byte 24: OptionFlags1
/// - Byte 25: OptionFlags2
/// - Byte 26: TypeFlags
/// - Byte 27: OptionFlags3
/// - Bytes 28-31: Client timezone (LE i32)
/// - Bytes 32-35: Client LCID (LE u32)
/// - Bytes 36+: Offset/length pairs for variable data
///
/// Offset/Length pairs (each 4 bytes: offset LE u16 + length LE u16):
///   0: HostName
///   1: UserName
///   2: Password
///   3: AppName
///   4: ServerName
///   5: Unused / Extension
///   6: CltIntName (Interface Library)
///   7: Language
///   8: Database
///   9: ClientID (6 bytes raw)
///  10: SSPI
///  11: AtchDBFile
///  12: ChangePassword
///  13: SSPILong (4 bytes raw)

#[derive(Debug)]
pub struct Login7 {
    pub tds_version: u32,
    pub packet_size: u32,
    pub hostname: String,
    pub username: String,
    pub password: String,
    pub app_name: String,
    pub server_name: String,
    pub database: String,
    pub type_flags: u8,
}

#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("LOGIN7 too short: {0} bytes")]
    TooShort(usize),
    #[error("invalid LOGIN7 data")]
    Invalid,
}

/// Decode a UCS-2LE string from a byte slice
fn decode_ucs2(data: &[u8]) -> String {
    let chars: Vec<u16> = data
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&chars)
}

/// Decode the obfuscated LOGIN7 password.
/// Each byte: ((b >> 4) | (b << 4)) ^ 0xA5 to reverse the client's obfuscation.
fn decode_password(data: &[u8]) -> String {
    let deobfuscated: Vec<u8> = data
        .iter()
        .map(|&b| {
            let unswapped = b ^ 0xA5;
            unswapped.rotate_left(4)
        })
        .collect();
    decode_ucs2(&deobfuscated)
}

/// Extract a string field from LOGIN7 using the offset/length pair at the given
/// position in the offset table. `offset_base` is where the offset table starts (byte 36).
fn extract_field(data: &[u8], offset_table_pos: usize, is_password: bool) -> Option<String> {
    let base = 36 + offset_table_pos * 4;
    if base + 4 > data.len() {
        return None;
    }
    let offset = u16::from_le_bytes([data[base], data[base + 1]]) as usize;
    let char_count = u16::from_le_bytes([data[base + 2], data[base + 3]]) as usize;

    if char_count == 0 {
        return Some(String::new());
    }

    let byte_count = char_count * 2; // UCS-2
    if offset + byte_count > data.len() {
        return None;
    }

    let field_data = &data[offset..offset + byte_count];
    if is_password {
        Some(decode_password(field_data))
    } else {
        Some(decode_ucs2(field_data))
    }
}

pub fn parse_login7(data: &[u8]) -> Result<Login7, LoginError> {
    // Minimum size: 36 bytes fixed + at least some offset/length pairs
    if data.len() < 36 {
        return Err(LoginError::TooShort(data.len()));
    }

    let tds_version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let packet_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    let type_flags = data[26];

    let hostname = extract_field(data, 0, false).unwrap_or_default();
    let username = extract_field(data, 1, false).unwrap_or_default();
    let password = extract_field(data, 2, true).unwrap_or_default();
    let app_name = extract_field(data, 3, false).unwrap_or_default();
    let server_name = extract_field(data, 4, false).unwrap_or_default();
    // Skip index 5 (unused/extension) and 6 (library name) and 7 (language)
    let database = extract_field(data, 8, false).unwrap_or_default();

    Ok(Login7 {
        tds_version,
        packet_size,
        hostname,
        username,
        password,
        app_name,
        server_name,
        database,
        type_flags,
    })
}

/// Encode a UCS-2LE string
pub fn encode_ucs2(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(s.len() * 2);
    for c in s.encode_utf16() {
        buf.extend_from_slice(&c.to_le_bytes());
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_decode() {
        // Test the obfuscation round-trip
        let password = "secret";
        let ucs2 = encode_ucs2(password);

        // Obfuscate like the client does
        let obfuscated: Vec<u8> = ucs2.iter().map(|&b| ((b << 4) | (b >> 4)) ^ 0xA5).collect();

        // Deobfuscate
        let decoded = decode_password(&obfuscated);
        assert_eq!(decoded, password);
    }

    #[test]
    fn test_ucs2_roundtrip() {
        let s = "hello";
        let encoded = encode_ucs2(s);
        let decoded = decode_ucs2(&encoded);
        assert_eq!(decoded, s);
    }
}
