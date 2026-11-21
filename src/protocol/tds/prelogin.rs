//! TDS PRELOGIN message parsing and response encoding.
//!
//! PRELOGIN is the first message on a connection. On a TDS 7.x connection
//! it travels in cleartext and negotiates encryption; on a TDS 8.0
//! connection it is already inside TLS. It carries driver and server
//! version numbers and a few flags, never credentials.

/// PRELOGIN option tokens
const PL_VERSION: u8 = 0x00;
const PL_ENCRYPTION: u8 = 0x01;
const PL_INSTOPT: u8 = 0x02;
const PL_THREADID: u8 = 0x03;
const PL_MARS: u8 = 0x04;
const PL_TERMINATOR: u8 = 0xFF;

/// Size of one option header: token, offset (u16 BE), length (u16 BE)
const OPTION_HEADER_SIZE: usize = 5;

/// Product version the server reports: major, minor, build (u16 BE),
/// sub-build (u16 BE). Drivers enable protocol features by this number;
/// 16.0 is the SQL Server 2022 level, which implies TDS 7.4.
pub const SERVER_VERSION: [u8; 6] = [16, 0, 0x03, 0xE8, 0, 0];

/// Encryption option values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encryption {
    /// Encryption available but off (client: encrypt login only)
    Off = 0,
    /// Encryption on
    On = 1,
    /// Encryption not supported
    NotSupported = 2,
    /// Encryption required
    Required = 3,
}

/// Parsed PRELOGIN from client
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PreLoginRequest {
    /// Driver version
    pub version: Option<[u8; 6]>,
    /// Encryption the client asks for
    pub encryption: Option<Encryption>,
    /// Client wants Multiple Active Result Sets
    pub mars: bool,
}

/// A PRELOGIN message that does not follow the format.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("malformed PRELOGIN message")]
pub struct PreLoginError;

/// Parse a client PRELOGIN message: a list of option headers ended by a
/// terminator, each pointing at its value later in the message.
pub fn parse_prelogin(data: &[u8]) -> Result<PreLoginRequest, PreLoginError> {
    let mut req = PreLoginRequest::default();
    let mut pos = 0;

    loop {
        let token = *data.get(pos).ok_or(PreLoginError)?;
        if token == PL_TERMINATOR {
            return Ok(req);
        }
        let header = data
            .get(pos..pos + OPTION_HEADER_SIZE)
            .ok_or(PreLoginError)?;
        let offset = u16::from_be_bytes([header[1], header[2]]) as usize;
        let length = u16::from_be_bytes([header[3], header[4]]) as usize;
        let value = data.get(offset..offset + length).ok_or(PreLoginError)?;
        pos += OPTION_HEADER_SIZE;

        match token {
            PL_VERSION => {
                let v: [u8; 6] = value.try_into().map_err(|_| PreLoginError)?;
                req.version = Some(v);
            }
            PL_ENCRYPTION => {
                let [b] = value else {
                    return Err(PreLoginError);
                };
                // The high bit (client certificate) does not change the choice.
                req.encryption = Some(match b & 0x7F {
                    0 => Encryption::Off,
                    1 => Encryption::On,
                    2 => Encryption::NotSupported,
                    3 => Encryption::Required,
                    _ => return Err(PreLoginError),
                });
            }
            PL_MARS => {
                let [b] = value else {
                    return Err(PreLoginError);
                };
                req.mars = *b != 0;
            }
            // Instance name, thread id, trace id, federated-auth flag,
            // nonce: nothing the server acts on.
            _ => {}
        }
    }
}

/// Decide the encryption reply for a cleartext (TDS 7.x) PRELOGIN.
///
/// The server always encrypts the whole session. A client that can
/// encrypt is told so; a client that cannot is refused (`None`), and the
/// caller sends `Required` and closes, as the protocol prescribes.
pub fn negotiate_encryption(client: Option<Encryption>) -> Option<Encryption> {
    match client {
        // "Off" means the client would encrypt only the login; insist on
        // the whole session.
        Some(Encryption::Off) => Some(Encryption::Required),
        Some(Encryption::On) | Some(Encryption::Required) => Some(Encryption::On),
        Some(Encryption::NotSupported) | None => None,
    }
}

/// Encode the server PRELOGIN response.
pub fn encode_prelogin_response(encryption: Encryption) -> Vec<u8> {
    let options: [(u8, &[u8]); 5] = [
        (PL_VERSION, &SERVER_VERSION),
        (PL_ENCRYPTION, &[encryption as u8]),
        (PL_INSTOPT, &[0]), // instance name check passed
        (PL_THREADID, &[]),
        (PL_MARS, &[0]), // MARS not supported
    ];

    let header_len = options.len() * OPTION_HEADER_SIZE + 1;
    let mut buf = Vec::with_capacity(header_len + 16);
    let mut offset = header_len as u16;
    for (token, value) in options {
        buf.push(token);
        buf.extend_from_slice(&offset.to_be_bytes());
        buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
        offset += value.len() as u16;
    }
    buf.push(PL_TERMINATOR);
    for (_, value) in options {
        buf.extend_from_slice(value);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a PRELOGIN message from (token, value) options.
    fn build(options: &[(u8, &[u8])]) -> Vec<u8> {
        let header_len = options.len() * OPTION_HEADER_SIZE + 1;
        let mut buf = Vec::new();
        let mut offset = header_len as u16;
        for (token, value) in options {
            buf.push(*token);
            buf.extend_from_slice(&offset.to_be_bytes());
            buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
            offset += value.len() as u16;
        }
        buf.push(PL_TERMINATOR);
        for (_, value) in options {
            buf.extend_from_slice(value);
        }
        buf
    }

    #[test]
    fn parses_typical_client_prelogin() {
        let data = build(&[
            (PL_VERSION, &[18, 3, 0, 1, 0, 0]),
            (PL_ENCRYPTION, &[1]),
            (PL_INSTOPT, b"MSSQLServer\0"),
            (PL_THREADID, &[1, 2, 3, 4]),
            (PL_MARS, &[1]),
            (0x05, &[0u8; 36]), // TRACEID
        ]);
        let req = parse_prelogin(&data).unwrap();
        assert_eq!(req.version, Some([18, 3, 0, 1, 0, 0]));
        assert_eq!(req.encryption, Some(Encryption::On));
        assert!(req.mars);
    }

    #[test]
    fn server_response_round_trips() {
        let encoded = encode_prelogin_response(Encryption::On);
        let parsed = parse_prelogin(&encoded).unwrap();
        assert_eq!(parsed.version, Some(SERVER_VERSION));
        assert_eq!(parsed.encryption, Some(Encryption::On));
        assert!(!parsed.mars);
        // Drivers reject a server whose major version is below 9.
        assert!(SERVER_VERSION[0] >= 9);
    }

    #[test]
    fn client_certificate_bit_is_ignored() {
        let data = build(&[(PL_ENCRYPTION, &[0x81])]);
        assert_eq!(
            parse_prelogin(&data).unwrap().encryption,
            Some(Encryption::On)
        );
    }

    #[test]
    fn malformed_messages_are_rejected() {
        // Empty, and no terminator
        assert_eq!(parse_prelogin(&[]), Err(PreLoginError));
        assert_eq!(parse_prelogin(&[PL_VERSION, 0, 6]), Err(PreLoginError));
        // Value points past the end
        let mut data = build(&[(PL_ENCRYPTION, &[1])]);
        data[2] = 0xFF;
        assert_eq!(parse_prelogin(&data), Err(PreLoginError));
        // Offset + length overflowing u16 range still must not panic
        let data = [PL_VERSION, 0xFF, 0xFF, 0xFF, 0xFF, PL_TERMINATOR];
        assert_eq!(parse_prelogin(&data), Err(PreLoginError));
        // Wrong value sizes and unknown encryption value
        assert_eq!(
            parse_prelogin(&build(&[(PL_VERSION, &[1, 2])])),
            Err(PreLoginError)
        );
        assert_eq!(
            parse_prelogin(&build(&[(PL_ENCRYPTION, &[1, 1])])),
            Err(PreLoginError)
        );
        assert_eq!(
            parse_prelogin(&build(&[(PL_ENCRYPTION, &[9])])),
            Err(PreLoginError)
        );
        assert_eq!(
            parse_prelogin(&build(&[(PL_MARS, &[])])),
            Err(PreLoginError)
        );
    }

    #[test]
    fn every_truncation_is_handled() {
        let data = build(&[
            (PL_VERSION, &[18, 3, 0, 1, 0, 0]),
            (PL_ENCRYPTION, &[1]),
            (PL_MARS, &[0]),
        ]);
        for len in 0..data.len() {
            // Must return (Ok or Err) without panicking.
            let _ = parse_prelogin(&data[..len]);
        }
    }

    #[test]
    fn encryption_is_always_required() {
        assert_eq!(
            negotiate_encryption(Some(Encryption::Off)),
            Some(Encryption::Required)
        );
        assert_eq!(
            negotiate_encryption(Some(Encryption::On)),
            Some(Encryption::On)
        );
        assert_eq!(
            negotiate_encryption(Some(Encryption::Required)),
            Some(Encryption::On)
        );
        assert_eq!(negotiate_encryption(Some(Encryption::NotSupported)), None);
        assert_eq!(negotiate_encryption(None), None);
    }
}
