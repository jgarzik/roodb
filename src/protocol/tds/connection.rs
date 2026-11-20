//! TDS connection handling: transport setup, login and the request loop.
//!
//! Two transports are accepted on the TDS port, told apart by the first
//! byte the client sends:
//!
//! - `0x16`, a TLS ClientHello: TDS 8.0 ("strict"). TLS is established
//!   first and everything, PRELOGIN included, travels inside it.
//! - `0x12`, a PRELOGIN packet: TDS 7.x. PRELOGIN is exchanged in
//!   cleartext, then the TLS handshake is tunnelled in PRELOGIN packets
//!   and the rest of the session is encrypted. A client that cannot
//!   encrypt is refused.
//!
//! Anything else is not TDS and the connection is closed. After login
//! both transports carry the same TDS 7.4 token stream; statement
//! execution is delegated to [`QueryEngine`].

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use crate::server::auth::{self, AuthError};
use crate::server::context::ServerContext;
use crate::server::limits::HANDSHAKE_TIMEOUT;
use crate::server::query::error::{codes, states, ProtocolError};
use crate::server::query::{QueryEngine, QueryError};
use crate::tls::TlsConfig;
use crate::txn::IsolationLevel;

use super::codec::{
    Message, PacketError, PacketType, TdsReader, TdsWriter, DEFAULT_PACKET_SIZE, MIN_PACKET_SIZE,
};
use super::login::{
    decode_ucs2, parse_login7, Login7, FEATURE_FEDAUTH, TDS_VERSION_7_4, TDS_VERSION_8_0,
};
use super::prelogin::{
    encode_prelogin_response, negotiate_encryption, parse_prelogin, Encryption, SERVER_VERSION,
};
use super::sink::TdsSink;
use super::token::{
    put_done, put_envchange_binary, put_envchange_string, put_error, put_featureextack_none,
    put_info, put_loginack, DEFAULT_COLLATION, DONE_ERROR, ENV_BEGIN_TRAN, ENV_COLLATION,
    ENV_COMMIT_TRAN, ENV_DATABASE, ENV_LANGUAGE, ENV_PACKET_SIZE, ENV_ROLLBACK_TRAN, TOKEN_DONE,
};
use super::tunnel::PreloginTlsStream;

/// First byte of a TLS ClientHello record
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// Largest PRELOGIN message accepted
const MAX_PRELOGIN: usize = 4096;
/// Largest LOGIN7 message accepted
const MAX_LOGIN: usize = 64 * 1024;
/// Largest request (SQL batch, RPC) accepted after login
const MAX_REQUEST: usize = 64 * 1024 * 1024;
/// Largest packet size the server agrees to
const MAX_NEGOTIATED_PACKET_SIZE: usize = 16 * 1024;

/// Database reported to clients that did not select one
const DEFAULT_DATABASE: &str = "default";

/// SQL Server error: login failed
const ERROR_LOGIN_FAILED: i32 = 18456;
/// SQL Server error: cannot open the database requested by the login
const ERROR_CANNOT_OPEN_DATABASE: i32 = 4060;

// Transaction manager request types
const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

/// TLS acceptors for the two TDS transports.
#[derive(Clone)]
pub struct TdsAcceptors {
    /// TDS 8.0: TLS first, ALPN `tds/8.0`
    strict: TlsAcceptor,
    /// TDS 7.x: TLS 1.2 tunnelled after PRELOGIN
    legacy: TlsAcceptor,
}

impl TdsAcceptors {
    /// Build the acceptors from the server's TLS configuration.
    pub fn new(tls_config: &TlsConfig) -> Self {
        Self {
            strict: TlsAcceptor::from(tls_config.tds_strict_config()),
            legacy: TlsAcceptor::from(tls_config.tds_legacy_config()),
        }
    }
}

/// An encrypted client stream of either transport.
trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> ClientStream for T {}
type BoxedStream = Box<dyn ClientStream>;
type Sink = TdsSink<WriteHalf<BoxedStream>>;

/// Why a connection ended before or during the request loop.
#[derive(Debug, thiserror::Error)]
enum TdsError {
    #[error(transparent)]
    Packet(#[from] PacketError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol violation: {0}")]
    Protocol(&'static str),
    #[error(transparent)]
    Engine(#[from] ProtocolError),
}

/// Serve one client connection on the TDS port.
pub async fn serve(
    stream: TcpStream,
    peer_addr: SocketAddr,
    acceptors: TdsAcceptors,
    ctx: ServerContext,
) {
    let connection_id = ctx.next_connection_id();
    info!(%peer_addr, connection_id, "TDS client connected");

    if let Err(e) = stream.set_nodelay(true) {
        error!(%peer_addr, connection_id, error = %e, "Failed to set TCP_NODELAY");
    }

    // Transport setup and login must finish within the handshake timeout.
    let login = establish(stream, peer_addr.ip(), connection_id, &acceptors, &ctx);
    let mut conn = match tokio::time::timeout(HANDSHAKE_TIMEOUT, login).await {
        Ok(Ok(Some(conn))) => conn,
        Ok(Ok(None)) => {
            info!(%peer_addr, connection_id, "TDS client refused");
            return;
        }
        Ok(Err(e)) => {
            warn!(%peer_addr, connection_id, error = %e, "TDS handshake failed");
            return;
        }
        Err(_) => {
            warn!(%peer_addr, connection_id, "TDS handshake timed out");
            return;
        }
    };

    match conn.run().await {
        Ok(()) | Err(TdsError::Packet(PacketError::ConnectionClosed)) => {}
        Err(e) => warn!(%peer_addr, connection_id, error = %e, "TDS connection error"),
    }
    conn.close().await;

    crate::server::locks::global_lock_manager().release_all_for_connection(connection_id);
    info!(%peer_addr, connection_id, "TDS client disconnected");
}

/// Read one message of the expected type.
async fn expect_message<R: AsyncRead + Unpin>(
    reader: R,
    expected: PacketType,
    max_len: usize,
) -> Result<Vec<u8>, TdsError> {
    let message = TdsReader::new(reader).read_message(max_len).await?;
    if message.packet_type != expected {
        return Err(TdsError::Packet(PacketError::UnexpectedType {
            expected,
            got: message.packet_type,
        }));
    }
    Ok(message.payload)
}

/// Set up the encrypted transport. Returns `None` when the client was
/// refused (not TDS, or unable to encrypt).
async fn secure_transport(
    mut stream: TcpStream,
    acceptors: &TdsAcceptors,
) -> Result<Option<BoxedStream>, TdsError> {
    let mut first = [0u8; 1];
    if stream.peek(&mut first).await? == 0 {
        return Err(TdsError::Packet(PacketError::ConnectionClosed));
    }

    if first[0] == TLS_HANDSHAKE_RECORD {
        // TDS 8.0: TLS first, then PRELOGIN inside it. The encryption
        // option of that PRELOGIN is meaningless and ignored.
        let mut tls = acceptors.strict.accept(stream).await?;
        let prelogin = expect_message(&mut tls, PacketType::PreLogin, MAX_PRELOGIN).await?;
        parse_prelogin(&prelogin).map_err(|_| TdsError::Protocol("malformed PRELOGIN"))?;
        TdsWriter::new(&mut tls)
            .write_message(
                PacketType::TabularResult,
                &encode_prelogin_response(Encryption::On),
            )
            .await?;
        return Ok(Some(Box::new(tls)));
    }

    if first[0] != PacketType::PreLogin as u8 {
        return Ok(None);
    }

    // TDS 7.x: cleartext PRELOGIN negotiates encryption.
    let prelogin = expect_message(&mut stream, PacketType::PreLogin, MAX_PRELOGIN).await?;
    let request =
        parse_prelogin(&prelogin).map_err(|_| TdsError::Protocol("malformed PRELOGIN"))?;
    let Some(encryption) = negotiate_encryption(request.encryption) else {
        // Tell the client encryption is required, then close.
        TdsWriter::new(&mut stream)
            .write_message(
                PacketType::TabularResult,
                &encode_prelogin_response(Encryption::Required),
            )
            .await?;
        return Ok(None);
    };
    TdsWriter::new(&mut stream)
        .write_message(
            PacketType::TabularResult,
            &encode_prelogin_response(encryption),
        )
        .await?;

    // TLS handshake tunnelled in PRELOGIN packets, then TLS on the socket.
    let mut tls = acceptors
        .legacy
        .accept(PreloginTlsStream::new(stream))
        .await?;
    tls.get_mut().0.finish_handshake()?;
    Ok(Some(Box::new(tls)))
}

/// Send a login failure and end the response.
async fn refuse_login<W: AsyncWrite + Unpin>(
    writer: W,
    number: i32,
    class: u8,
    message: &str,
) -> Result<(), TdsError> {
    let mut tokens = Vec::new();
    put_error(&mut tokens, number, 1, class, message);
    put_done(&mut tokens, TOKEN_DONE, DONE_ERROR, 0);
    TdsWriter::new(writer)
        .write_message(PacketType::TabularResult, &tokens)
        .await?;
    Ok(())
}

/// Packet size to use for a client that asked for `requested` bytes.
fn negotiate_packet_size(requested: u32) -> usize {
    if requested == 0 {
        DEFAULT_PACKET_SIZE
    } else {
        (requested as usize).clamp(MIN_PACKET_SIZE, MAX_NEGOTIATED_PACKET_SIZE)
    }
}

/// Secure the transport and log the client in. Returns `None` when the
/// client was refused.
async fn establish(
    stream: TcpStream,
    client_ip: IpAddr,
    connection_id: u32,
    acceptors: &TdsAcceptors,
    ctx: &ServerContext,
) -> Result<Option<TdsConnection>, TdsError> {
    let Some(mut tls) = secure_transport(stream, acceptors).await? else {
        return Ok(None);
    };

    let payload = expect_message(&mut tls, PacketType::Tds7Login, MAX_LOGIN).await?;
    let login = parse_login7(&payload).map_err(|_| TdsError::Protocol("malformed LOGIN7"))?;
    debug!(connection_id, ?login, "LOGIN7 received");

    // Every refusal an unauthenticated client can provoke reads the same.
    let login_failed = format!("Login failed for user '{}'.", login.username);

    if login.tds_version != TDS_VERSION_7_4 && login.tds_version != TDS_VERSION_8_0 {
        warn!(
            connection_id,
            tds_version = format_args!("{:#010x}", login.tds_version),
            "Unsupported TDS version"
        );
        refuse_login(&mut tls, ERROR_LOGIN_FAILED, 14, &login_failed).await?;
        return Ok(None);
    }
    if login.integrated_security || login.features.contains(&FEATURE_FEDAUTH) {
        warn!(connection_id, "Client requested SSPI or federated login");
        refuse_login(&mut tls, ERROR_LOGIN_FAILED, 14, &login_failed).await?;
        return Ok(None);
    }

    let authenticated =
        auth::authenticate(&ctx.storage, &login.username, client_ip, |stored_hash| {
            auth::verify_cleartext_password(&login.password, stored_hash)
        })
        .await;
    match authenticated {
        Ok(()) => {}
        Err(AuthError::Denied) => {
            refuse_login(&mut tls, ERROR_LOGIN_FAILED, 14, &login_failed).await?;
            return Ok(None);
        }
        Err(AuthError::Internal(e)) => {
            warn!(connection_id, error = %e, "User lookup failed");
            refuse_login(&mut tls, ERROR_LOGIN_FAILED, 14, &login_failed).await?;
            return Ok(None);
        }
    }

    if !login.database.is_empty() && !ctx.catalog.read().database_exists(&login.database) {
        let message = format!(
            "Cannot open database \"{}\" requested by the login. The login failed.",
            login.database
        );
        refuse_login(&mut tls, ERROR_CANNOT_OPEN_DATABASE, 11, &message).await?;
        return Ok(None);
    }

    let (read_half, write_half) = tokio::io::split(tls);
    let cancel = Arc::new(AtomicBool::new(false));
    let sink: Sink = TdsSink::new(write_half, cancel.clone());
    let mut engine = QueryEngine::new(connection_id, client_ip, ctx, Box::new(sink));
    engine.session.set_user(login.username.clone());
    if !login.database.is_empty() {
        engine.session.set_database(Some(login.database.clone()));
    }

    let mut conn = TdsConnection {
        connection_id,
        engine,
        cancel: cancel.clone(),
        requests: spawn_reader(read_half, cancel),
    };
    conn.send_login_response(&login).await?;

    info!(connection_id, username = %login.username, "TDS login succeeded");
    Ok(Some(conn))
}

/// Read requests on their own task, so an attention signal is seen while
/// a statement is still executing.
fn spawn_reader(
    read_half: ReadHalf<BoxedStream>,
    cancel: Arc<AtomicBool>,
) -> mpsc::Receiver<Result<Message, PacketError>> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut reader = TdsReader::new(BufReader::with_capacity(8192, read_half));
        loop {
            let result = reader.read_message(MAX_REQUEST).await;
            let last = result.is_err();
            if matches!(&result, Ok(m) if m.packet_type == PacketType::Attention) {
                cancel.store(true, Ordering::Relaxed);
            }
            if tx.send(result).await.is_err() || last {
                break;
            }
        }
    });
    rx
}

/// A logged-in TDS session.
struct TdsConnection {
    connection_id: u32,
    engine: QueryEngine,
    /// Raised by the reader when the client cancels the running request
    cancel: Arc<AtomicBool>,
    requests: mpsc::Receiver<Result<Message, PacketError>>,
}

/// Session facts the client tracks through ENVCHANGE tokens.
struct Environment {
    database: Option<String>,
    transaction: Option<u64>,
}

/// Skip the ALL_HEADERS block that starts every request: a total length
/// (u32, counting itself) followed by headers.
fn skip_all_headers(payload: &[u8]) -> Result<&[u8], TdsError> {
    let bad = TdsError::Protocol("malformed ALL_HEADERS");
    let Some(len) = payload.get(..4) else {
        return Err(bad);
    };
    let total = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
    if total < 4 {
        return Err(bad);
    }
    payload.get(total..).ok_or(bad)
}

impl TdsConnection {
    /// The TDS encoder this connection installed in the engine.
    fn sink(&mut self) -> &mut Sink {
        self.engine
            .out
            .as_any_mut()
            .downcast_mut()
            .expect("engine sink is the TdsSink this connection created")
    }

    fn environment(&self) -> Environment {
        Environment {
            database: self.engine.session.database.clone(),
            transaction: self.engine.session.current_txn,
        }
    }

    /// Tell the client it is logged in and describe the session.
    async fn send_login_response(&mut self, login: &Login7) -> Result<(), TdsError> {
        let database = self
            .engine
            .session
            .database
            .clone()
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        let packet_size = negotiate_packet_size(login.packet_size);
        let acknowledge_features = !login.features.is_empty();

        let sink = self.sink();
        let tokens = sink.tokens();
        put_envchange_string(tokens, ENV_DATABASE, &database, "master");
        put_info(
            tokens,
            5701,
            2,
            0,
            &format!("Changed database context to '{}'.", database),
        );
        put_envchange_binary(tokens, ENV_COLLATION, &DEFAULT_COLLATION, &[]);
        put_envchange_string(tokens, ENV_LANGUAGE, "us_english", "");
        put_info(
            tokens,
            5703,
            1,
            0,
            "Changed language setting to us_english.",
        );
        put_loginack(
            tokens,
            [
                SERVER_VERSION[0],
                SERVER_VERSION[1],
                SERVER_VERSION[2],
                SERVER_VERSION[3],
            ],
        );
        if acknowledge_features {
            put_featureextack_none(tokens);
        }
        put_envchange_string(
            tokens,
            ENV_PACKET_SIZE,
            &packet_size.to_string(),
            &DEFAULT_PACKET_SIZE.to_string(),
        );
        sink.finish(false).await?;
        // The new size applies from the next message on.
        sink.writer().set_packet_size(packet_size);
        Ok(())
    }

    /// Serve requests until the client disconnects.
    async fn run(&mut self) -> Result<(), TdsError> {
        while let Some(message) = self.requests.recv().await {
            let message = message?;
            match message.packet_type {
                PacketType::Attention => {
                    // Acknowledge unless the request it interrupted
                    // already did.
                    if self.cancel.swap(false, Ordering::Relaxed) {
                        self.sink().finish(true).await?;
                    }
                }
                PacketType::SqlBatch => {
                    let before = self.environment();
                    let sql = self.sql_batch(&message.payload)?;
                    debug!(connection_id = self.connection_id, sql = %sql, "SQL batch");
                    self.execute(&sql).await?;
                    let rolled_back = sql.trim_start().to_uppercase().starts_with("ROLLBACK");
                    self.finish_request(before, rolled_back).await?;
                }
                PacketType::TransactionManager => {
                    let before = self.environment();
                    let rolled_back = self.transaction_request(&message.payload).await?;
                    self.finish_request(before, rolled_back).await?;
                }
                PacketType::Rpc => {
                    let before = self.environment();
                    self.engine.out.error(&QueryError::new(
                        codes::ER_UNKNOWN_COM_ERROR,
                        states::GENERAL_ERROR,
                        "Remote procedure calls are not supported",
                    ));
                    self.finish_request(before, false).await?;
                }
                _ => return Err(TdsError::Protocol("unexpected message type")),
            }
        }
        Ok(())
    }

    /// Execute SQL, reporting results through the sink. An error here is
    /// one the engine could not turn into a statement error; the client is
    /// told before the connection is given up.
    async fn execute(&mut self, sql: &str) -> Result<(), TdsError> {
        if let Err(e) = self.engine.handle_query(sql).await {
            let _ = self.engine.send_error_from_protocol_error(&e).await;
            let _ = self.sink().finish(false).await;
            return Err(e.into());
        }
        Ok(())
    }

    /// The SQL text of a batch request.
    fn sql_batch(&self, payload: &[u8]) -> Result<String, TdsError> {
        let text = skip_all_headers(payload)?;
        if text.len() % 2 != 0 {
            return Err(TdsError::Protocol("SQL text is not UTF-16"));
        }
        Ok(decode_ucs2(text))
    }

    /// Run a transaction manager request. Returns whether it was a
    /// rollback.
    async fn transaction_request(&mut self, payload: &[u8]) -> Result<bool, TdsError> {
        let body = skip_all_headers(payload)?;
        let Some(kind) = body.get(..2) else {
            return Err(TdsError::Protocol("malformed transaction request"));
        };
        let args = &body[2..];

        match u16::from_le_bytes([kind[0], kind[1]]) {
            TM_BEGIN_XACT => {
                // Isolation level byte; 0 keeps the session's level.
                let level = match args.first() {
                    Some(1) => Some(IsolationLevel::ReadUncommitted),
                    Some(2) => Some(IsolationLevel::ReadCommitted),
                    Some(3) | Some(5) => Some(IsolationLevel::RepeatableRead),
                    Some(4) => Some(IsolationLevel::Serializable),
                    _ => None,
                };
                if let Some(level) = level {
                    self.engine.session.set_isolation_level(level);
                }
                self.execute("BEGIN").await?;
                Ok(false)
            }
            TM_COMMIT_XACT => {
                self.execute("COMMIT").await?;
                Ok(false)
            }
            TM_ROLLBACK_XACT => {
                self.execute("ROLLBACK").await?;
                Ok(true)
            }
            _ => {
                self.engine.out.error(&QueryError::new(
                    codes::ER_UNKNOWN_COM_ERROR,
                    states::GENERAL_ERROR,
                    "This transaction manager request is not supported",
                ));
                Ok(false)
            }
        }
    }

    /// Complete the response: report what changed in the session, then
    /// the final DONE, acknowledging a cancellation if one arrived.
    async fn finish_request(
        &mut self,
        before: Environment,
        rolled_back: bool,
    ) -> Result<(), TdsError> {
        let after = self.environment();
        let cancelled = self.cancel.swap(false, Ordering::Relaxed);
        let sink = self.sink();
        let tokens = sink.tokens();

        if before.database != after.database {
            put_envchange_string(
                tokens,
                ENV_DATABASE,
                after.database.as_deref().unwrap_or(DEFAULT_DATABASE),
                before.database.as_deref().unwrap_or(DEFAULT_DATABASE),
            );
        }
        match (before.transaction, after.transaction) {
            (None, Some(new)) => {
                put_envchange_binary(tokens, ENV_BEGIN_TRAN, &new.to_le_bytes(), &[]);
            }
            (Some(old), None) => {
                let kind = if rolled_back {
                    ENV_ROLLBACK_TRAN
                } else {
                    ENV_COMMIT_TRAN
                };
                put_envchange_binary(tokens, kind, &[], &old.to_le_bytes());
            }
            _ => {}
        }

        sink.finish(cancelled).await?;
        Ok(())
    }

    /// Release what the session holds once the client is gone.
    async fn close(&mut self) {
        if self.engine.session.in_transaction() {
            if let Err(e) = self.engine.handle_query("ROLLBACK").await {
                warn!(connection_id = self.connection_id, error = %e, "Rollback on disconnect failed");
            }
        }
        self.engine.cleanup_temp_tables().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_size_negotiation_is_clamped() {
        assert_eq!(negotiate_packet_size(0), DEFAULT_PACKET_SIZE);
        assert_eq!(negotiate_packet_size(4096), 4096);
        assert_eq!(negotiate_packet_size(8000), 8000);
        assert_eq!(negotiate_packet_size(1), MIN_PACKET_SIZE);
        assert_eq!(negotiate_packet_size(u32::MAX), MAX_NEGOTIATED_PACKET_SIZE);
    }

    #[test]
    fn all_headers_are_skipped_with_bounds_checks() {
        // Total length 4: no headers, the rest is the request body
        assert_eq!(skip_all_headers(&[4, 0, 0, 0, 9, 9]).unwrap(), &[9, 9]);
        // One 18-byte transaction descriptor header (total 22)
        let mut payload = vec![22, 0, 0, 0, 18, 0, 0, 0, 2, 0];
        payload.extend_from_slice(&[0u8; 12]);
        payload.extend_from_slice(b"body");
        assert_eq!(skip_all_headers(&payload).unwrap(), b"body");
        // Exactly the headers, empty body
        assert_eq!(skip_all_headers(&[4, 0, 0, 0]).unwrap(), b"");

        // Too short, total below its own size, total past the end
        assert!(skip_all_headers(&[]).is_err());
        assert!(skip_all_headers(&[4, 0, 0]).is_err());
        assert!(skip_all_headers(&[3, 0, 0, 0, 1]).is_err());
        assert!(skip_all_headers(&[9, 0, 0, 0, 1]).is_err());
        assert!(skip_all_headers(&[0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }
}
