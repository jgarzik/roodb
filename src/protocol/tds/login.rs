//! TDS LOGIN7 message parsing.
//!
//! LOGIN7 layout (MS-TDS 2.2.6.4), all integers little-endian:
//! - 0..4   total length
//! - 4..8   TDS version
//! - 8..12  requested packet size
//! - 12..24 client program version, PID, connection ID
//! - 24     OptionFlags1
//! - 25     OptionFlags2 (bit 7: integrated security)
//! - 26     TypeFlags
//! - 27     OptionFlags3 (bit 4: feature extension present)
//! - 28..36 client time zone, LCID
//! - 36..94 offset/length table for the variable fields
//!
//! Offset/length pairs (u16 offset from the start of the message, u16
//! length in UCS-2 characters unless noted): 0 HostName, 1 UserName,
//! 2 Password, 3 AppName, 4 ServerName, 5 Extension (offset of a u32 that
//! locates the feature list, length 4 bytes), 6 CltIntName, 7 Language,
//! 8 Database, then ClientID (6 raw bytes), 9 SSPI (bytes), 10 AtchDBFile,
//! 11 ChangePassword, and cbSSPILong (u32).

use std::fmt;

/// Size of the fixed part of LOGIN7, including the offset/length table
const FIXED_SIZE: usize = 94;
/// Where the offset/length table starts
const TABLE_OFFSET: usize = 36;

/// TDS 7.4, as sent by current drivers on a TDS 7.x connection
pub const TDS_VERSION_7_4: u32 = 0x7400_0004;
/// TDS 8.0, as sent by drivers on a strict connection
pub const TDS_VERSION_8_0: u32 = 0x0800_0000;

/// OptionFlags2: client wants integrated (SSPI) authentication
const FLAG2_INTEGRATED_SECURITY: u8 = 0x80;
/// OptionFlags3: the Extension field locates a feature list
const FLAG3_EXTENSION: u8 = 0x10;
/// Feature list terminator
const FEATURE_TERMINATOR: u8 = 0xFF;
/// Feature ID: federated authentication
pub const FEATURE_FEDAUTH: u8 = 0x02;

/// A parsed LOGIN7 message.
pub struct Login7 {
    /// TDS version the client speaks
    pub tds_version: u32,
    /// Packet size the client asks for
    pub packet_size: u32,
    /// Client machine name
    pub hostname: String,
    /// Login name
    pub username: String,
    /// Password. LOGIN7 only obfuscates it, so this is the cleartext; it
    /// is protected by TLS on the wire and must never be logged.
    pub password: String,
    /// Client application name
    pub app_name: String,
    /// Server name the client asked for
    pub server_name: String,
    /// Initial database (empty for the default)
    pub database: String,
    /// Client asked for integrated (SSPI) authentication
    pub integrated_security: bool,
    /// Feature IDs the client listed in its feature extension
    pub features: Vec<u8>,
}

impl fmt::Debug for Login7 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Login7")
            .field("tds_version", &format_args!("{:#010x}", self.tds_version))
            .field("packet_size", &self.packet_size)
            .field("hostname", &self.hostname)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("app_name", &self.app_name)
            .field("server_name", &self.server_name)
            .field("database", &self.database)
            .field("integrated_security", &self.integrated_security)
            .field("features", &self.features)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum LoginError {
    #[error("LOGIN7 too short: {0} bytes")]
    TooShort(usize),
    #[error("invalid LOGIN7 data")]
    Invalid,
}

/// Decode a UCS-2LE string from a byte slice
pub fn decode_ucs2(data: &[u8]) -> String {
    let chars: Vec<u16> = data
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u16::from_le_bytes(*c))
        .collect();
    String::from_utf16_lossy(&chars)
}

/// Encode a string as UCS-2LE
pub fn encode_ucs2(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(s.len() * 2);
    for c in s.encode_utf16() {
        buf.extend_from_slice(&c.to_le_bytes());
    }
    buf
}

/// Undo the LOGIN7 password obfuscation: each byte was nibble-swapped and
/// then XORed with 0xA5.
fn decode_password(data: &[u8]) -> String {
    let clear: Vec<u8> = data.iter().map(|&b| (b ^ 0xA5).rotate_left(4)).collect();
    decode_ucs2(&clear)
}

fn u16_at(data: &[u8], pos: usize) -> Result<usize, LoginError> {
    let bytes = data.get(pos..pos + 2).ok_or(LoginError::Invalid)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]) as usize)
}

fn u32_at(data: &[u8], pos: usize) -> Result<u32, LoginError> {
    let bytes = data.get(pos..pos + 4).ok_or(LoginError::Invalid)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// The bytes of the character field whose offset/length pair is at byte
/// `pair` of the message. An empty field may have any offset.
fn char_field(data: &[u8], pair: usize) -> Result<&[u8], LoginError> {
    let offset = u16_at(data, pair)?;
    let chars = u16_at(data, pair + 2)?;
    if chars == 0 {
        return Ok(&[]);
    }
    data.get(offset..offset + chars * 2)
        .ok_or(LoginError::Invalid)
}

/// Feature IDs from the feature extension list: repeated
/// (id u8, length u32, data) entries ended by 0xFF.
fn parse_features(data: &[u8], mut pos: usize) -> Result<Vec<u8>, LoginError> {
    let mut features = Vec::new();
    loop {
        let id = *data.get(pos).ok_or(LoginError::Invalid)?;
        if id == FEATURE_TERMINATOR {
            return Ok(features);
        }
        let len = u32_at(data, pos + 1)? as usize;
        let end = pos
            .checked_add(5)
            .and_then(|p| p.checked_add(len))
            .ok_or(LoginError::Invalid)?;
        if end > data.len() {
            return Err(LoginError::Invalid);
        }
        features.push(id);
        pos = end;
    }
}

/// Parse a LOGIN7 message.
pub fn parse_login7(data: &[u8]) -> Result<Login7, LoginError> {
    if data.len() < FIXED_SIZE {
        return Err(LoginError::TooShort(data.len()));
    }
    // The declared length covers the whole message.
    if u32_at(data, 0)? as usize != data.len() {
        return Err(LoginError::Invalid);
    }

    let tds_version = u32_at(data, 4)?;
    let packet_size = u32_at(data, 8)?;
    let flags2 = data[25];
    let flags3 = data[27];

    let pair = |index: usize| TABLE_OFFSET + index * 4;
    let hostname = decode_ucs2(char_field(data, pair(0))?);
    let username = decode_ucs2(char_field(data, pair(1))?);
    let password = decode_password(char_field(data, pair(2))?);
    let app_name = decode_ucs2(char_field(data, pair(3))?);
    let server_name = decode_ucs2(char_field(data, pair(4))?);
    let database = decode_ucs2(char_field(data, pair(8))?);

    // SSPI data follows the 6-byte ClientID; its pair is at index 9 + 6 bytes.
    let sspi_len = u16_at(data, pair(9) + 6 + 2)?;
    let integrated_security = flags2 & FLAG2_INTEGRATED_SECURITY != 0 || sspi_len > 0;

    let features = if flags3 & FLAG3_EXTENSION != 0 {
        // The Extension pair locates a u32 holding the feature list offset.
        let pointer = u16_at(data, pair(5))?;
        let list = u32_at(data, pointer)? as usize;
        parse_features(data, list)?
    } else {
        Vec::new()
    };

    Ok(Login7 {
        tds_version,
        packet_size,
        hostname,
        username,
        password,
        app_name,
        server_name,
        database,
        integrated_security,
        features,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// What a test LOGIN7 message should contain.
    #[derive(Default)]
    pub(crate) struct LoginSpec<'a> {
        pub tds_version: u32,
        pub packet_size: u32,
        pub hostname: &'a str,
        pub username: &'a str,
        pub password: &'a str,
        pub app_name: &'a str,
        pub database: &'a str,
        pub flags2: u8,
        pub sspi: &'a [u8],
        /// Feature (id, data) entries; `Some` sets the extension flag
        pub features: Option<&'a [(u8, &'a [u8])]>,
    }

    /// Encode a LOGIN7 message the way a driver does.
    pub(crate) fn build_login7(spec: &LoginSpec<'_>) -> Vec<u8> {
        let mut msg = vec![0u8; FIXED_SIZE];
        msg[4..8].copy_from_slice(&spec.tds_version.to_le_bytes());
        msg[8..12].copy_from_slice(&spec.packet_size.to_le_bytes());
        msg[25] = spec.flags2;

        fn put_pair(msg: &mut [u8], pos: usize, offset: usize, len: usize) {
            msg[pos..pos + 2].copy_from_slice(&(offset as u16).to_le_bytes());
            msg[pos + 2..pos + 4].copy_from_slice(&(len as u16).to_le_bytes());
        }
        let pair = |index: usize| TABLE_OFFSET + index * 4;

        let obfuscated: Vec<u8> = encode_ucs2(spec.password)
            .iter()
            .map(|&b| b.rotate_left(4) ^ 0xA5)
            .collect();
        let char_fields: [(usize, Vec<u8>, usize); 5] = [
            (
                0,
                encode_ucs2(spec.hostname),
                spec.hostname.encode_utf16().count(),
            ),
            (
                1,
                encode_ucs2(spec.username),
                spec.username.encode_utf16().count(),
            ),
            (2, obfuscated, spec.password.encode_utf16().count()),
            (
                3,
                encode_ucs2(spec.app_name),
                spec.app_name.encode_utf16().count(),
            ),
            (
                8,
                encode_ucs2(spec.database),
                spec.database.encode_utf16().count(),
            ),
        ];
        for (index, bytes, chars) in char_fields {
            let offset = msg.len();
            put_pair(&mut msg, pair(index), offset, chars);
            msg.extend_from_slice(&bytes);
        }

        // SSPI: its pair sits after the 6-byte ClientID
        let offset = msg.len();
        put_pair(&mut msg, pair(9) + 6, offset, spec.sspi.len());
        msg.extend_from_slice(spec.sspi);

        if let Some(features) = spec.features {
            msg[27] |= FLAG3_EXTENSION;
            let pointer = msg.len();
            put_pair(&mut msg, pair(5), pointer, 4);
            let list = (pointer + 4) as u32;
            msg.extend_from_slice(&list.to_le_bytes());
            for (id, data) in features {
                msg.push(*id);
                msg.extend_from_slice(&(data.len() as u32).to_le_bytes());
                msg.extend_from_slice(data);
            }
            msg.push(FEATURE_TERMINATOR);
        }

        let total = msg.len() as u32;
        msg[0..4].copy_from_slice(&total.to_le_bytes());
        msg
    }

    fn typical() -> LoginSpec<'static> {
        LoginSpec {
            tds_version: TDS_VERSION_7_4,
            packet_size: 4096,
            hostname: "client-pc",
            username: "root",
            password: "s3cr\u{e9}t \u{4e16}\u{754c}",
            app_name: "tests",
            database: "test",
            ..Default::default()
        }
    }

    #[test]
    fn parses_all_fields() {
        let login = parse_login7(&build_login7(&typical())).unwrap();
        assert_eq!(login.tds_version, TDS_VERSION_7_4);
        assert_eq!(login.packet_size, 4096);
        assert_eq!(login.hostname, "client-pc");
        assert_eq!(login.username, "root");
        assert_eq!(login.password, "s3cr\u{e9}t \u{4e16}\u{754c}");
        assert_eq!(login.app_name, "tests");
        assert_eq!(login.server_name, "");
        assert_eq!(login.database, "test");
        assert!(!login.integrated_security);
        assert!(login.features.is_empty());
    }

    #[test]
    fn password_obfuscation_matches_the_specification() {
        // MS-TDS: swap the nibbles of each byte, then XOR with 0xA5.
        // 'a' is 0x61 0x00 in UCS-2LE: 0x61 -> 0x16 -> 0xB3, 0x00 -> 0xA5.
        assert_eq!(decode_password(&[0xB3, 0xA5]), "a");
    }

    #[test]
    fn tds_8_version_and_features() {
        let spec = LoginSpec {
            tds_version: TDS_VERSION_8_0,
            features: Some(&[(0x01, &[1, 2, 3]), (0x0A, &[]), (FEATURE_FEDAUTH, &[0])]),
            ..typical()
        };
        let login = parse_login7(&build_login7(&spec)).unwrap();
        assert_eq!(login.tds_version, TDS_VERSION_8_0);
        assert_eq!(login.features, vec![0x01, 0x0A, FEATURE_FEDAUTH]);
    }

    #[test]
    fn integrated_security_is_detected() {
        let by_flag = LoginSpec {
            flags2: FLAG2_INTEGRATED_SECURITY,
            ..typical()
        };
        assert!(
            parse_login7(&build_login7(&by_flag))
                .unwrap()
                .integrated_security
        );

        let by_sspi = LoginSpec {
            sspi: b"NTLMSSP\0",
            ..typical()
        };
        assert!(
            parse_login7(&build_login7(&by_sspi))
                .unwrap()
                .integrated_security
        );
    }

    #[test]
    fn debug_output_never_contains_the_password() {
        let login = parse_login7(&build_login7(&typical())).unwrap();
        let shown = format!("{:?}", login);
        assert!(shown.contains("<redacted>"));
        assert!(!shown.contains("s3cr"));
    }

    #[test]
    fn short_and_inconsistent_messages_are_rejected() {
        assert_eq!(parse_login7(&[]).unwrap_err(), LoginError::TooShort(0));
        assert_eq!(
            parse_login7(&[0u8; FIXED_SIZE - 1]).unwrap_err(),
            LoginError::TooShort(FIXED_SIZE - 1)
        );
        // Declared length disagrees with the message
        let mut msg = build_login7(&typical());
        msg[0] = msg[0].wrapping_add(1);
        assert_eq!(parse_login7(&msg).unwrap_err(), LoginError::Invalid);
    }

    #[test]
    fn fields_pointing_outside_the_message_are_rejected() {
        let good = build_login7(&typical());
        // Corrupt each character field's offset and length in turn.
        for index in [0usize, 1, 2, 3, 4, 8] {
            for byte in 0..4 {
                let mut msg = good.clone();
                msg[TABLE_OFFSET + index * 4 + byte] = 0xFF;
                // Either rejected or parsed; never a panic.
                if let Ok(login) = parse_login7(&msg) {
                    // A field that still parses must lie inside the message.
                    assert!(login.username.len() <= msg.len());
                }
            }
        }
        // Username claims more characters than remain
        let mut msg = good.clone();
        msg[TABLE_OFFSET + 4 + 2] = 0xFF;
        msg[TABLE_OFFSET + 4 + 3] = 0x7F;
        assert_eq!(parse_login7(&msg).unwrap_err(), LoginError::Invalid);
    }

    #[test]
    fn malformed_feature_lists_are_rejected() {
        let spec = LoginSpec {
            features: Some(&[(0x01, &[1, 2, 3])]),
            ..typical()
        };
        let good = build_login7(&spec);

        // Missing terminator
        let mut msg = good[..good.len() - 1].to_vec();
        let total = msg.len() as u32;
        msg[0..4].copy_from_slice(&total.to_le_bytes());
        assert_eq!(parse_login7(&msg).unwrap_err(), LoginError::Invalid);

        // Feature length far beyond the message (would overflow an offset)
        let mut msg = good.clone();
        let len_pos = good.len() - 1 - 3 - 4;
        msg[len_pos..len_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(parse_login7(&msg).unwrap_err(), LoginError::Invalid);
    }

    #[test]
    fn every_truncation_and_byte_flip_is_handled() {
        let spec = LoginSpec {
            features: Some(&[(0x01, &[1, 2, 3]), (FEATURE_FEDAUTH, &[0])]),
            sspi: b"x",
            ..typical()
        };
        let good = build_login7(&spec);
        for len in 0..good.len() {
            let _ = parse_login7(&good[..len]);
        }
        for pos in 0..good.len() {
            for value in [0x00, 0x7F, 0x80, 0xFF] {
                let mut msg = good.clone();
                msg[pos] = value;
                let _ = parse_login7(&msg);
            }
        }
    }

    #[test]
    fn ucs2_round_trip() {
        let s = "h\u{e9}llo \u{1F600}";
        assert_eq!(decode_ucs2(&encode_ucs2(s)), s);
    }
}
