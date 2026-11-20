//! A minimal hand-rolled TDS client for tests that need to send exactly
//! chosen bytes: malformed requests, status bits, transaction manager
//! requests, attention signals. It uses the TDS 8.0 transport (TLS first),
//! which keeps the framing simple.

use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::roodb_suite::harness::TestServer;

pub const PKT_SQL_BATCH: u8 = 0x01;
pub const PKT_RPC: u8 = 0x03;
pub const PKT_TABULAR_RESULT: u8 = 0x04;
pub const PKT_ATTENTION: u8 = 0x06;
pub const PKT_BULK_LOAD: u8 = 0x07;
pub const PKT_TRANSACTION_MANAGER: u8 = 0x0E;
pub const PKT_LOGIN7: u8 = 0x10;
pub const PKT_PRELOGIN: u8 = 0x12;

pub const STATUS_EOM: u8 = 0x01;
pub const STATUS_RESET_CONNECTION: u8 = 0x08;
pub const STATUS_RESET_CONNECTION_SKIP_TRAN: u8 = 0x10;

pub const DONE_ERROR: u16 = 0x0002;
pub const DONE_ATTN: u16 = 0x0020;

pub const ENV_BEGIN_TRAN: u8 = 8;
pub const ENV_COMMIT_TRAN: u8 = 9;
pub const ENV_ROLLBACK_TRAN: u8 = 10;
pub const ENV_RESET_CONNECTION: u8 = 18;

/// The test server uses a self-signed certificate.
#[derive(Debug)]
struct AcceptAnyCert(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// A token of a server response, as far as these tests need to see it.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Error {
        number: i32,
        message: String,
    },
    Info {
        number: i32,
    },
    EnvChange {
        kind: u8,
        new_value: Vec<u8>,
    },
    LoginAck {
        tds_version: [u8; 4],
    },
    FeatureExtAck,
    Done {
        status: u16,
        row_count: u64,
    },
    ReturnStatus(i32),
    /// Integer output parameter
    ReturnValue {
        name: String,
        value: i32,
    },
    /// First byte of a token this parser does not decode (result sets)
    Other(u8),
}

/// Decode the tokens of a response up to the first one not understood.
pub fn parse_tokens(mut data: &[u8]) -> Vec<Token> {
    let mut tokens = Vec::new();
    while let Some(&kind) = data.first() {
        match kind {
            0xAA | 0xAB | 0xE3 | 0xAD => {
                let len = u16::from_le_bytes([data[1], data[2]]) as usize;
                let body = &data[3..3 + len];
                tokens.push(match kind {
                    0xAA | 0xAB => {
                        let number = i32::from_le_bytes(body[0..4].try_into().unwrap());
                        let chars = u16::from_le_bytes([body[6], body[7]]) as usize;
                        let units: Vec<u16> = body[8..8 + chars * 2]
                            .chunks_exact(2)
                            .map(|c| u16::from_le_bytes([c[0], c[1]]))
                            .collect();
                        let message = String::from_utf16_lossy(&units);
                        if kind == 0xAA {
                            Token::Error { number, message }
                        } else {
                            Token::Info { number }
                        }
                    }
                    0xE3 => {
                        let new_len = body[1] as usize;
                        // String values count UTF-16 units, binary ones bytes
                        let is_binary = matches!(body[0], 7..=10 | 18);
                        let bytes = if is_binary { new_len } else { new_len * 2 };
                        Token::EnvChange {
                            kind: body[0],
                            new_value: body.get(2..2 + bytes).unwrap_or(&[]).to_vec(),
                        }
                    }
                    _ => Token::LoginAck {
                        tds_version: body[1..5].try_into().unwrap(),
                    },
                });
                data = &data[3 + len..];
            }
            0xAE => {
                // Feature acknowledgements until the 0xFF terminator
                let mut pos = 1;
                while data[pos] != 0xFF {
                    let len =
                        u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
                    pos += 5 + len;
                }
                tokens.push(Token::FeatureExtAck);
                data = &data[pos + 1..];
            }
            0x79 => {
                tokens.push(Token::ReturnStatus(i32::from_le_bytes(
                    data[1..5].try_into().unwrap(),
                )));
                data = &data[5..];
            }
            0xAC => {
                // ordinal, name, status, user type, flags, INTN(4), value
                let chars = data[3] as usize;
                let units: Vec<u16> = data[4..4 + chars * 2]
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect();
                let rest = &data[4 + chars * 2..];
                assert_eq!(&rest[7..10], &[0x26, 4, 4], "integer return value");
                tokens.push(Token::ReturnValue {
                    name: String::from_utf16_lossy(&units),
                    value: i32::from_le_bytes(rest[10..14].try_into().unwrap()),
                });
                data = &rest[14..];
            }
            0xFD..=0xFF => {
                tokens.push(Token::Done {
                    status: u16::from_le_bytes([data[1], data[2]]),
                    row_count: u64::from_le_bytes(data[5..13].try_into().unwrap()),
                });
                data = &data[13..];
            }
            other => {
                tokens.push(Token::Other(other));
                break;
            }
        }
    }
    tokens
}

/// Error numbers in a response.
pub fn errors(tokens: &[Token]) -> Vec<i32> {
    tokens
        .iter()
        .filter_map(|t| match t {
            Token::Error { number, .. } => Some(*number),
            _ => None,
        })
        .collect()
}

/// ENVCHANGE types in a response, in order.
pub fn env_changes(tokens: &[Token]) -> Vec<u8> {
    tokens
        .iter()
        .filter_map(|t| match t {
            Token::EnvChange { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect()
}

/// Status of the last DONE token.
pub fn final_done(tokens: &[Token]) -> u16 {
    match tokens.last() {
        Some(Token::Done { status, .. }) => *status,
        other => panic!("response does not end with DONE: {other:?}"),
    }
}

pub fn ucs2(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

/// An RPC parameter: name, status flags, then TYPE_INFO and value bytes.
pub fn rpc_param(name: &str, output: bool, typed_value: &[u8]) -> Vec<u8> {
    let mut p = vec![name.encode_utf16().count() as u8];
    p.extend(ucs2(name));
    p.push(output as u8);
    p.extend_from_slice(typed_value);
    p
}

/// TYPE_INFO and value of an INT parameter (`None` for NULL).
pub fn int_value(value: Option<i32>) -> Vec<u8> {
    let mut v = vec![0x26, 4];
    match value {
        Some(n) => {
            v.push(4);
            v.extend_from_slice(&n.to_le_bytes());
        }
        None => v.push(0),
    }
    v
}

/// TYPE_INFO and value of an NVARCHAR(4000) parameter.
pub fn nvarchar_value(text: &str) -> Vec<u8> {
    let mut v = vec![0xE7, 0x40, 0x1F, 0x09, 0x04, 0xD0, 0x00, 0x34];
    let bytes = ucs2(text);
    v.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    v.extend_from_slice(&bytes);
    v
}

/// ALL_HEADERS with one transaction descriptor header.
pub fn all_headers() -> Vec<u8> {
    let mut h = Vec::with_capacity(22);
    h.extend_from_slice(&22u32.to_le_bytes()); // total length
    h.extend_from_slice(&18u32.to_le_bytes()); // header length
    h.extend_from_slice(&2u16.to_le_bytes()); // transaction descriptor
    h.extend_from_slice(&0u64.to_le_bytes());
    h.extend_from_slice(&1u32.to_le_bytes()); // outstanding requests
    h
}

/// Build a LOGIN7 message for SQL authentication.
pub fn login7(user: &str, password: &str, tds_version: u32) -> Vec<u8> {
    let mut msg = vec![0u8; 94];
    msg[4..8].copy_from_slice(&tds_version.to_le_bytes());
    msg[8..12].copy_from_slice(&4096u32.to_le_bytes());

    let obfuscated: Vec<u8> = ucs2(password)
        .iter()
        .map(|b| b.rotate_left(4) ^ 0xA5)
        .collect();
    // (offset/length pair index, bytes, length in characters)
    let fields = [
        (1usize, ucs2(user), user.encode_utf16().count()),
        (2, obfuscated, password.encode_utf16().count()),
    ];
    for (index, bytes, chars) in fields {
        let pair = 36 + index * 4;
        let offset = msg.len() as u16;
        msg[pair..pair + 2].copy_from_slice(&offset.to_le_bytes());
        msg[pair + 2..pair + 4].copy_from_slice(&(chars as u16).to_le_bytes());
        msg.extend_from_slice(&bytes);
    }
    let total = msg.len() as u32;
    msg[0..4].copy_from_slice(&total.to_le_bytes());
    msg
}

pub struct RawTds {
    stream: TlsStream<TcpStream>,
}

impl RawTds {
    /// Open a TDS 8.0 connection: TLS first, then PRELOGIN.
    pub async fn connect(server: &TestServer) -> Self {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
            .with_no_client_auth();
        config.alpn_protocols = vec![b"tds/8.0".to_vec()];

        let tcp = TcpStream::connect(("127.0.0.1", server.tds_port()))
            .await
            .expect("connect to TDS port");
        let stream = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("TLS handshake");
        let mut client = Self { stream };

        // PRELOGIN with only a VERSION option
        let mut prelogin = vec![0x00, 0x00, 0x06, 0x00, 0x06, 0xFF];
        prelogin.extend_from_slice(&[18, 0, 0, 0, 0, 0]);
        client.send(PKT_PRELOGIN, STATUS_EOM, &prelogin).await;
        let (kind, _) = client.recv().await.expect("PRELOGIN reply");
        assert_eq!(kind, PKT_TABULAR_RESULT);
        client
    }

    /// Connect and log in as root.
    pub async fn login_root(server: &TestServer) -> Self {
        let mut client = Self::connect(server).await;
        let tokens = client.login("root", "", 0x7400_0004).await;
        assert!(errors(&tokens).is_empty(), "login failed: {tokens:?}");
        client
    }

    /// Send LOGIN7 and return the response tokens.
    pub async fn login(&mut self, user: &str, password: &str, tds_version: u32) -> Vec<Token> {
        self.send(PKT_LOGIN7, STATUS_EOM, &login7(user, password, tds_version))
            .await;
        let (_, payload) = self.recv().await.expect("login response");
        parse_tokens(&payload)
    }

    /// Send one message as 4096-byte packets. `status` is OR-ed into the
    /// first packet; the last packet gets end-of-message.
    pub async fn send(&mut self, kind: u8, status: u8, payload: &[u8]) {
        let mut wire = Vec::new();
        let chunks: Vec<&[u8]> = if payload.is_empty() {
            vec![&[][..]]
        } else {
            payload.chunks(4096 - 8).collect()
        };
        for (i, chunk) in chunks.iter().enumerate() {
            let mut flags = if i == 0 { status & !STATUS_EOM } else { 0 };
            if i + 1 == chunks.len() {
                flags |= STATUS_EOM;
            }
            let len = ((8 + chunk.len()) as u16).to_be_bytes();
            wire.extend_from_slice(&[kind, flags, len[0], len[1], 0, 0, (i + 1) as u8, 0]);
            wire.extend_from_slice(chunk);
        }
        self.send_bytes(&wire).await;
    }

    /// Send bytes exactly as given.
    pub async fn send_bytes(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("send");
        self.stream.flush().await.expect("flush");
    }

    /// Read one complete message, or `None` if the server closed the
    /// connection.
    pub async fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut payload = Vec::new();
        loop {
            let mut header = [0u8; 8];
            if self.stream.read_exact(&mut header).await.is_err() {
                return None;
            }
            let len = u16::from_be_bytes([header[2], header[3]]) as usize;
            let mut body = vec![0u8; len - 8];
            if self.stream.read_exact(&mut body).await.is_err() {
                return None;
            }
            payload.extend_from_slice(&body);
            if header[1] & STATUS_EOM != 0 {
                return Some((header[0], payload));
            }
        }
    }

    /// Read a response and decode its tokens.
    pub async fn response(&mut self) -> Vec<Token> {
        let received = tokio::time::timeout(Duration::from_secs(10), self.recv())
            .await
            .expect("timed out waiting for a response");
        let (kind, payload) = received.expect("server closed the connection");
        assert_eq!(kind, PKT_TABULAR_RESULT);
        parse_tokens(&payload)
    }

    /// Run a SQL batch and return the response tokens.
    pub async fn batch(&mut self, sql: &str) -> Vec<Token> {
        self.batch_with_status(sql, 0).await
    }

    /// Run a SQL batch with extra status bits on its first packet.
    pub async fn batch_with_status(&mut self, sql: &str, status: u8) -> Vec<Token> {
        let mut payload = all_headers();
        payload.extend_from_slice(&ucs2(sql));
        self.send(PKT_SQL_BATCH, status, &payload).await;
        self.response().await
    }

    /// Call a system procedure by number and return the response tokens.
    pub async fn rpc(&mut self, proc_id: u16, params: &[Vec<u8>]) -> Vec<Token> {
        let mut payload = all_headers();
        payload.extend_from_slice(&0xFFFFu16.to_le_bytes());
        payload.extend_from_slice(&proc_id.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes()); // option flags
        for param in params {
            payload.extend_from_slice(param);
        }
        self.send(PKT_RPC, STATUS_EOM, &payload).await;
        self.response().await
    }

    /// Send a transaction manager request and return the response tokens.
    pub async fn transaction_request(&mut self, kind: u16, args: &[u8]) -> Vec<Token> {
        let mut payload = all_headers();
        payload.extend_from_slice(&kind.to_le_bytes());
        payload.extend_from_slice(args);
        self.send(PKT_TRANSACTION_MANAGER, STATUS_EOM, &payload)
            .await;
        self.response().await
    }

    /// Whether the server closes the connection within `limit`.
    pub async fn closed_within(&mut self, limit: Duration) -> bool {
        let mut buf = [0u8; 4096];
        let wait = async {
            loop {
                match self.stream.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => continue,
                }
            }
        };
        tokio::time::timeout(limit, wait).await.is_ok()
    }
}
