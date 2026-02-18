//! TDS PRELOGIN message parsing and response encoding.

/// PRELOGIN option tokens
const PL_VERSION: u8 = 0x00;
const PL_ENCRYPTION: u8 = 0x01;
const PL_INSTANCE: u8 = 0x02;
const PL_MARS: u8 = 0x04;
const PL_TERMINATOR: u8 = 0xFF;

/// Encryption availability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encryption {
    Off = 0,
    On = 1,
    NotSupported = 2,
    Required = 3,
}

/// Parsed PRELOGIN from client
#[derive(Debug)]
pub struct PreLoginRequest {
    pub version: Option<[u8; 6]>,
    pub encryption: Option<Encryption>,
    pub instance: Option<String>,
    pub mars: Option<bool>,
}

/// Parse a client PRELOGIN message
pub fn parse_prelogin(data: &[u8]) -> PreLoginRequest {
    let mut options = Vec::new();
    let mut pos = 0;

    // Parse option list
    while pos < data.len() {
        let token = data[pos];
        if token == PL_TERMINATOR {
            break;
        }
        if pos + 5 > data.len() {
            break;
        }
        let offset = u16::from_be_bytes([data[pos + 1], data[pos + 2]]) as usize;
        let length = u16::from_be_bytes([data[pos + 3], data[pos + 4]]) as usize;
        options.push((token, offset, length));
        pos += 5;
    }

    let mut req = PreLoginRequest {
        version: None,
        encryption: None,
        instance: None,
        mars: None,
    };

    for (token, offset, length) in options {
        if offset + length > data.len() {
            continue;
        }
        let value = &data[offset..offset + length];

        match token {
            PL_VERSION if length >= 6 => {
                let mut v = [0u8; 6];
                v.copy_from_slice(&value[..6]);
                req.version = Some(v);
            }
            PL_ENCRYPTION if length >= 1 => {
                req.encryption = Some(match value[0] {
                    0 => Encryption::Off,
                    1 => Encryption::On,
                    2 => Encryption::NotSupported,
                    3 => Encryption::Required,
                    _ => Encryption::On,
                });
            }
            PL_INSTANCE if !value.is_empty() => {
                // Instance name (could be UCS-2 or ASCII depending on client)
                req.instance = Some(String::from_utf8_lossy(value).to_string());
            }
            PL_MARS if length >= 1 => {
                req.mars = Some(value[0] != 0);
            }
            _ => {}
        }
    }

    req
}

/// Encode a server PRELOGIN response
pub fn encode_prelogin_response() -> Vec<u8> {
    // Options: VERSION, ENCRYPTION, MARS, TERMINATOR
    // 3 options × 5 bytes each + 1 terminator = 16 bytes header
    let header_len = 3 * 5 + 1; // 16

    // VERSION data: TDS 7.4 = 0x04000074 as big-endian, plus 2 bytes
    let version_data: [u8; 6] = [0x04, 0x00, 0x00, 0x74, 0x00, 0x00];
    let encryption_data: [u8; 1] = [Encryption::On as u8];
    let mars_data: [u8; 1] = [0x00]; // MARS off

    let mut buf = Vec::with_capacity(64);

    // Write option headers
    let mut data_offset = header_len as u16;

    // VERSION
    buf.push(PL_VERSION);
    buf.extend_from_slice(&data_offset.to_be_bytes());
    buf.extend_from_slice(&(version_data.len() as u16).to_be_bytes());
    let version_offset = data_offset;
    data_offset += version_data.len() as u16;

    // ENCRYPTION
    buf.push(PL_ENCRYPTION);
    buf.extend_from_slice(&data_offset.to_be_bytes());
    buf.extend_from_slice(&(encryption_data.len() as u16).to_be_bytes());
    data_offset += encryption_data.len() as u16;

    // MARS
    buf.push(PL_MARS);
    buf.extend_from_slice(&data_offset.to_be_bytes());
    buf.extend_from_slice(&(mars_data.len() as u16).to_be_bytes());

    // Terminator
    buf.push(PL_TERMINATOR);

    // Verify offset alignment
    debug_assert_eq!(buf.len(), header_len);
    let _ = version_offset;

    // Write option data
    buf.extend_from_slice(&version_data);
    buf.extend_from_slice(&encryption_data);
    buf.extend_from_slice(&mars_data);

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_prelogin() {
        let encoded = encode_prelogin_response();
        let parsed = parse_prelogin(&encoded);
        assert!(parsed.version.is_some());
        assert_eq!(parsed.encryption, Some(Encryption::On));
        assert_eq!(parsed.mars, Some(false));
    }
}
