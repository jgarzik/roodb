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

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use crate::executor::Datum;
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
use super::params::RpcParam;
use super::prelogin::{
    encode_prelogin_response, negotiate_encryption, parse_prelogin, Encryption, SERVER_VERSION,
};
use super::rpc::{bind_parameters, declared_names, parse_rpc, Procedure, RpcCall};
use super::shims::{split_batch, translate, Action, ShimContext};
use super::sink::TdsSink;
use super::token::{
    put_done, put_envchange_binary, put_envchange_string, put_error, put_featureextack_none,
    put_info, put_loginack, DEFAULT_COLLATION, DONE_ERROR, ENV_BEGIN_TRAN, ENV_COLLATION,
    ENV_COMMIT_TRAN, ENV_DATABASE, ENV_LANGUAGE, ENV_PACKET_SIZE, ENV_RESET_CONNECTION,
    ENV_ROLLBACK_TRAN, TOKEN_DONE,
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
/// Most statements a session may hold prepared at once
const MAX_PREPARED_STATEMENTS: usize = 4096;
/// Most statement text, in bytes, a session may hold prepared at once
const MAX_PREPARED_BYTES: usize = 16 * 1024 * 1024;
/// Statements remembered for sp_executesql before the cache is cleared
const STATEMENT_CACHE_SIZE: usize = 256;
/// Statement text, in bytes, remembered for sp_executesql before the
/// cache is cleared
const STATEMENT_CACHE_BYTES: usize = 4 * 1024 * 1024;
/// Largest statement worth remembering for sp_executesql
const MAX_CACHED_STATEMENT: usize = 64 * 1024;
/// Largest packet size the server agrees to
const MAX_NEGOTIATED_PACKET_SIZE: usize = 16 * 1024;

/// Database reported to clients that did not select one
const DEFAULT_DATABASE: &str = "default";

/// Product version reported to probes, matching PRELOGIN and LOGINACK
const PRODUCT_VERSION: &str = "16.0.1000";

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

    let login_database = engine.session.database.clone();
    let (requests, reader) = spawn_reader(read_half, cancel.clone());
    let mut conn = TdsConnection {
        connection_id,
        engine,
        cancel,
        requests,
        reader,
        login_database,
        prepared: HashMap::new(),
        prepared_bytes: 0,
        next_handle: 1,
        statement_cache: HashMap::new(),
        statement_cache_bytes: 0,
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
) -> (mpsc::Receiver<Result<Message, PacketError>>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1);
    let task = tokio::spawn(async move {
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
    (rx, task)
}

/// A logged-in TDS session.
struct TdsConnection {
    connection_id: u32,
    engine: QueryEngine,
    /// Raised by the reader when the client cancels the running request
    cancel: Arc<AtomicBool>,
    requests: mpsc::Receiver<Result<Message, PacketError>>,
    /// The task reading from the socket. It owns the read half, so the
    /// socket only closes once it has stopped.
    reader: JoinHandle<()>,
    /// Database selected at login, restored by a connection reset
    login_database: Option<String>,
    /// Statements prepared with sp_prepare / sp_prepexec, by handle,
    /// each with the size of its text
    prepared: HashMap<i32, (Prepared, usize)>,
    /// Total text size of `prepared`
    prepared_bytes: usize,
    next_handle: i32,
    /// Statements seen by sp_executesql, keyed by text and parameter
    /// declaration, so repeated executions reuse the cached plan
    statement_cache: HashMap<(String, String), Prepared>,
    /// Total key size of `statement_cache`
    statement_cache_bytes: usize,
}

/// A statement ready to be executed with parameter values.
#[derive(Debug, Clone)]
enum Prepared {
    /// No parameters: runs as plain SQL text
    Text(String),
    /// Parameter references rewritten to placeholders and prepared in the
    /// engine
    Bound {
        /// Engine prepared-statement ID
        statement_id: u32,
        /// Declared parameter names, lower-case
        names: Vec<String>,
        /// For each placeholder, the index of its parameter in `names`
        order: Vec<usize>,
    },
}

impl Drop for TdsConnection {
    fn drop(&mut self) {
        // Without this the reader would keep the socket open, blocked on
        // a read, after the server has given up on the session.
        self.reader.abort();
    }
}

/// Session facts the client tracks through ENVCHANGE tokens.
struct Environment {
    database: Option<String>,
    transaction: Option<u64>,
}

/// Arguments of a transaction manager commit or rollback request.
#[derive(Debug, PartialEq, Eq)]
struct EndTransaction {
    /// A transaction (savepoint) name was given
    named: bool,
    /// Start another transaction afterwards, at this isolation level
    begin_next: Option<u8>,
}

/// Parse the arguments of a commit or rollback request: a name (length
/// byte in UTF-16 units, then the text), a flags byte, and, when the
/// flags ask for a follow-on transaction, its isolation level and name.
fn parse_end_transaction(args: &[u8]) -> Result<EndTransaction, TdsError> {
    let bad = || TdsError::Protocol("malformed transaction request");
    // Old clients send no arguments at all.
    let Some(&name_chars) = args.first() else {
        return Ok(EndTransaction {
            named: false,
            begin_next: None,
        });
    };
    let after_name = 1 + usize::from(name_chars) * 2;
    let rest = args.get(after_name..).ok_or_else(bad)?;
    let flags = rest.first().copied().unwrap_or(0);
    let begin_next = if flags & 0x01 != 0 {
        Some(*rest.get(1).ok_or_else(bad)?)
    } else {
        None
    };
    Ok(EndTransaction {
        named: name_chars != 0,
        begin_next,
    })
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
            // A pooled connection's next user asks for a clean session
            // before its first request runs.
            let reset = message.packet_type != PacketType::Attention && message.resets_connection();
            if reset {
                self.engine
                    .reset_session(message.reset_keeps_transaction())
                    .await;
                self.engine
                    .session
                    .set_database(self.login_database.clone());
                // The engine dropped its prepared statements.
                self.prepared.clear();
                self.prepared_bytes = 0;
                self.statement_cache.clear();
                self.statement_cache_bytes = 0;
                put_envchange_binary(self.sink().tokens(), ENV_RESET_CONNECTION, &[], &[]);
            }
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
                    let rolled_back = self.run_sql(&sql).await?;
                    self.finish_request(before, rolled_back).await?;
                }
                PacketType::TransactionManager => {
                    let before = self.environment();
                    let rolled_back = self.transaction_request(&message.payload).await?;
                    self.finish_request(before, rolled_back).await?;
                }
                PacketType::Rpc => {
                    let before = self.environment();
                    self.rpc_request(&message.payload).await?;
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

    /// Run SQL text as a client sent it: split into statements, apply
    /// the driver shims, and stop at the first statement that fails.
    /// Returns whether a rollback was the last transaction statement run.
    async fn run_sql(&mut self, sql: &str) -> Result<bool, TdsError> {
        let mut rolled_back = false;
        for statement in split_batch(sql) {
            let errors_before = self.sink().error_count();
            let action = {
                let session = &self.engine.session;
                translate(
                    &statement,
                    &ShimContext {
                        connection_id: self.connection_id,
                        in_transaction: session.in_transaction(),
                        database: session.database.as_deref().unwrap_or(DEFAULT_DATABASE),
                        user: &session.user,
                        product_version: PRODUCT_VERSION,
                    },
                )
            };
            match action {
                Action::Run(sql) => {
                    let was_in_transaction = self.engine.session.in_transaction();
                    self.execute(&sql).await?;
                    if was_in_transaction && !self.engine.session.in_transaction() {
                        rolled_back = sql == "ROLLBACK"
                            || sql.trim_start().to_uppercase().starts_with("ROLLBACK");
                    }
                }
                Action::Accept => self.engine.send_ok(0, 0).await?,
                Action::Refuse(message) => self.refuse(codes::ER_UNKNOWN_COM_ERROR, message),
            }
            if self.sink().error_count() != errors_before {
                break;
            }
        }
        Ok(rolled_back)
    }

    /// The SQL text of a batch request.
    fn sql_batch(&self, payload: &[u8]) -> Result<String, TdsError> {
        let text = skip_all_headers(payload)?;
        if text.len() % 2 != 0 {
            return Err(TdsError::Protocol("SQL text is not UTF-16"));
        }
        Ok(decode_ucs2(text))
    }

    /// Report a request the server will not carry out. The session
    /// continues.
    fn refuse(&mut self, code: u16, message: &str) {
        self.engine
            .out
            .error(&QueryError::new(code, states::GENERAL_ERROR, message));
    }

    /// Run the procedure calls of an RPC request.
    async fn rpc_request(&mut self, payload: &[u8]) -> Result<(), TdsError> {
        let body = skip_all_headers(payload)?;
        let calls = match parse_rpc(body, MAX_REQUEST) {
            Ok(calls) => calls,
            Err(e) => {
                self.refuse(codes::ER_UNKNOWN_COM_ERROR, &e.to_string());
                return Ok(());
            }
        };
        for call in calls {
            self.sink().begin_procedure();
            let handle = self.procedure_call(call).await?;
            match handle {
                Some((name, value)) => self.sink().end_procedure(&[(name.as_str(), value)]),
                None => self.sink().end_procedure(&[]),
            }
        }
        Ok(())
    }

    /// Run one procedure call. Returns the statement handle to report as
    /// an output parameter, if the procedure produces one.
    async fn procedure_call(&mut self, call: RpcCall) -> Result<Option<(String, i32)>, TdsError> {
        let text = |param: Option<&RpcParam>| match param.map(|p| &p.value) {
            Some(Datum::String(s)) => Some(s.clone()),
            _ => None,
        };
        let params = &call.params;

        match call.procedure {
            // sp_executesql @stmt, @params, values...
            Procedure::ExecuteSql => {
                let Some(sql) = text(params.first()) else {
                    self.refuse(
                        codes::ER_WRONG_ARGUMENTS,
                        "sp_executesql expects a statement",
                    );
                    return Ok(None);
                };
                let declaration = text(params.get(1)).unwrap_or_default();
                let key = (sql, declaration);
                let prepared = match self.statement_cache.get(&key) {
                    Some(prepared) => prepared.clone(),
                    None => {
                        let Some(prepared) = self.prepare(&key.0, &key.1).await? else {
                            return Ok(None);
                        };
                        let size = key.0.len() + key.1.len();
                        if size <= MAX_CACHED_STATEMENT {
                            if self.statement_cache.len() >= STATEMENT_CACHE_SIZE
                                || self.statement_cache_bytes + size > STATEMENT_CACHE_BYTES
                            {
                                self.clear_statement_cache();
                            }
                            self.statement_cache_bytes += size;
                            self.statement_cache.insert(key, prepared.clone());
                            self.execute_prepared(&prepared, params.get(2..).unwrap_or(&[]))
                                .await?;
                            return Ok(None);
                        }
                        // Too large to remember: run it and release it.
                        self.execute_prepared(&prepared, params.get(2..).unwrap_or(&[]))
                            .await?;
                        if let Prepared::Bound { statement_id, .. } = prepared {
                            self.engine.prepared_stmts.close(statement_id);
                        }
                        return Ok(None);
                    }
                };
                self.execute_prepared(&prepared, params.get(2..).unwrap_or(&[]))
                    .await?;
                Ok(None)
            }

            // sp_prepare @handle OUTPUT, @params, @stmt
            // sp_prepexec @handle OUTPUT, @params, @stmt, values...
            Procedure::Prepare | Procedure::PrepExec => {
                let Some(sql) = text(params.get(2)) else {
                    self.refuse(codes::ER_WRONG_ARGUMENTS, "Expected a statement to prepare");
                    return Ok(None);
                };
                let declaration = text(params.get(1)).unwrap_or_default();
                let size = sql.len() + declaration.len();
                if self.prepared.len() >= MAX_PREPARED_STATEMENTS
                    || self.prepared_bytes + size > MAX_PREPARED_BYTES
                {
                    self.refuse(
                        codes::ER_UNKNOWN_ERROR,
                        "Too many prepared statements on this connection",
                    );
                    return Ok(None);
                }
                let Some(prepared) = self.prepare(&sql, &declaration).await? else {
                    return Ok(None);
                };
                let handle = self.next_handle;
                self.next_handle = self.next_handle.wrapping_add(1).max(1);
                self.prepared.insert(handle, (prepared.clone(), size));
                self.prepared_bytes += size;

                if call.procedure == Procedure::PrepExec {
                    self.execute_prepared(&prepared, params.get(3..).unwrap_or(&[]))
                        .await?;
                }
                let name = match params.first() {
                    Some(p) if !p.name.is_empty() => p.name.clone(),
                    _ => "@handle".to_string(),
                };
                Ok(Some((name, handle)))
            }

            // sp_execute @handle, values...
            Procedure::Execute => {
                let handle = match params.first().map(|p| &p.value) {
                    Some(Datum::Int(h)) => i32::try_from(*h).ok(),
                    _ => None,
                };
                let Some((prepared, _)) = handle.and_then(|h| self.prepared.get(&h)).cloned()
                else {
                    self.refuse(
                        codes::ER_WRONG_ARGUMENTS,
                        "Could not find the prepared statement for this handle",
                    );
                    return Ok(None);
                };
                self.execute_prepared(&prepared, params.get(1..).unwrap_or(&[]))
                    .await?;
                Ok(None)
            }

            // sp_unprepare @handle
            Procedure::Unprepare => {
                let handle = match params.first().map(|p| &p.value) {
                    Some(Datum::Int(h)) => i32::try_from(*h).ok(),
                    _ => None,
                };
                match handle.and_then(|h| self.prepared.remove(&h)) {
                    Some((prepared, size)) => {
                        self.prepared_bytes -= size;
                        if let Prepared::Bound { statement_id, .. } = prepared {
                            self.engine.prepared_stmts.close(statement_id);
                        }
                    }
                    None => self.refuse(
                        codes::ER_WRONG_ARGUMENTS,
                        "Could not find the prepared statement for this handle",
                    ),
                }
                Ok(None)
            }

            Procedure::Other(name) => {
                self.refuse(
                    codes::ER_SP_DOES_NOT_EXIST,
                    &format!("Could not find stored procedure '{}'.", name),
                );
                Ok(None)
            }
        }
    }

    /// Prepare a statement whose parameters are declared in `declaration`.
    /// Returns `None` if the statement was refused (the error is already
    /// reported).
    async fn prepare(
        &mut self,
        sql: &str,
        declaration: &str,
    ) -> Result<Option<Prepared>, TdsError> {
        let names = declared_names(declaration);
        if names.is_empty() {
            return Ok(Some(Prepared::Text(sql.to_string())));
        }
        // Apply the driver shims to the statement text. A statement the
        // shims answer themselves takes no parameters.
        let sql = {
            let session = &self.engine.session;
            let action = translate(
                sql,
                &ShimContext {
                    connection_id: self.connection_id,
                    in_transaction: session.in_transaction(),
                    database: session.database.as_deref().unwrap_or(DEFAULT_DATABASE),
                    user: &session.user,
                    product_version: PRODUCT_VERSION,
                },
            );
            match action {
                Action::Run(sql) => sql,
                Action::Accept | Action::Refuse(_) => {
                    return Ok(Some(Prepared::Text(sql.to_string())))
                }
            }
        };
        let (bound_sql, order) = bind_parameters(&sql, &names);
        match self.engine.prepared_stmts.prepare(&bound_sql) {
            Ok(statement) => Ok(Some(Prepared::Bound {
                statement_id: statement.id,
                names,
                order,
            })),
            Err(e) => {
                self.engine.send_error_from_protocol_error(&e).await?;
                Ok(None)
            }
        }
    }

    /// Execute a prepared statement with the parameter values of a call.
    /// Values are matched to parameters by name, or by position when
    /// unnamed, and bound as typed values.
    async fn execute_prepared(
        &mut self,
        prepared: &Prepared,
        values: &[RpcParam],
    ) -> Result<(), TdsError> {
        let (statement_id, names, order) = match prepared {
            Prepared::Text(sql) => return self.run_sql(sql).await.map(|_| ()),
            Prepared::Bound {
                statement_id,
                names,
                order,
            } => (*statement_id, names, order),
        };

        let mut bound: Vec<Option<Datum>> = vec![None; names.len()];
        let mut next_position = 0;
        for param in values {
            let index = if param.name.is_empty() {
                let index = next_position;
                next_position += 1;
                index
            } else {
                let lower = param.name.to_lowercase();
                match names.iter().position(|n| *n == lower) {
                    Some(index) => index,
                    None => {
                        self.refuse(
                            codes::ER_WRONG_ARGUMENTS,
                            &format!("{} is not a parameter of this statement", param.name),
                        );
                        return Ok(());
                    }
                }
            };
            match bound.get_mut(index) {
                Some(slot) => *slot = Some(param.value.clone()),
                None => {
                    self.refuse(
                        codes::ER_WRONG_ARGUMENTS,
                        "Too many parameter values for this statement",
                    );
                    return Ok(());
                }
            }
        }
        if let Some(missing) = bound.iter().position(Option::is_none) {
            self.refuse(
                codes::ER_WRONG_ARGUMENTS,
                &format!("Parameter {} was not supplied", names[missing]),
            );
            return Ok(());
        }

        // One value per placeholder, in placeholder order
        let args: Vec<Datum> = order
            .iter()
            .map(|index| bound[*index].clone().unwrap_or(Datum::Null))
            .collect();
        if let Err(e) = self.engine.execute_prepared(statement_id, &args).await {
            let _ = self.engine.send_error_from_protocol_error(&e).await;
            let _ = self.sink().finish(false).await;
            return Err(e.into());
        }
        Ok(())
    }

    /// Forget the sp_executesql statement cache, releasing its statements.
    fn clear_statement_cache(&mut self) {
        for (_, prepared) in self.statement_cache.drain() {
            if let Prepared::Bound { statement_id, .. } = prepared {
                self.engine.prepared_stmts.close(statement_id);
            }
        }
        self.statement_cache_bytes = 0;
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
                self.begin_transaction(args.first().copied().unwrap_or(0))
                    .await?;
                Ok(false)
            }
            kind @ (TM_COMMIT_XACT | TM_ROLLBACK_XACT) => {
                let rollback = kind == TM_ROLLBACK_XACT;
                let end = parse_end_transaction(args)?;
                if end.named {
                    // A named commit or rollback targets a savepoint.
                    // Ending the whole transaction instead would silently
                    // discard or keep more than the client asked for.
                    self.engine.out.error(&QueryError::new(
                        codes::ER_UNKNOWN_COM_ERROR,
                        states::GENERAL_ERROR,
                        "Savepoints are not supported",
                    ));
                    return Ok(false);
                }
                self.execute(if rollback { "ROLLBACK" } else { "COMMIT" })
                    .await?;
                if let Some(isolation) = end.begin_next {
                    self.begin_transaction(isolation).await?;
                }
                Ok(rollback)
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

    /// Start a transaction at the isolation level a transaction manager
    /// request names (0 keeps the session's level).
    async fn begin_transaction(&mut self, isolation: u8) -> Result<(), TdsError> {
        let level = match isolation {
            1 => Some(IsolationLevel::ReadUncommitted),
            2 => Some(IsolationLevel::ReadCommitted),
            // Snapshot (5) is served by the repeatable-read snapshot.
            3 | 5 => Some(IsolationLevel::RepeatableRead),
            4 => Some(IsolationLevel::Serializable),
            _ => None,
        };
        if let Some(level) = level {
            self.engine.session.set_isolation_level(level);
        }
        self.execute("BEGIN").await
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
        if before.transaction != after.transaction {
            // The old transaction ended, a new one began, or both.
            if let Some(old) = before.transaction {
                let kind = if rolled_back {
                    ENV_ROLLBACK_TRAN
                } else {
                    ENV_COMMIT_TRAN
                };
                put_envchange_binary(tokens, kind, &[], &old.to_le_bytes());
            }
            if let Some(new) = after.transaction {
                put_envchange_binary(tokens, ENV_BEGIN_TRAN, &new.to_le_bytes(), &[]);
            }
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
    fn end_transaction_arguments() {
        let parsed = |args: &[u8]| parse_end_transaction(args).unwrap();
        let plain = EndTransaction {
            named: false,
            begin_next: None,
        };
        assert_eq!(parsed(&[]), plain);
        assert_eq!(parsed(&[0]), plain);
        assert_eq!(parsed(&[0, 0]), plain);
        // fBeginXact with isolation level 2 and an empty name
        assert_eq!(
            parsed(&[0, 1, 2, 0]),
            EndTransaction {
                named: false,
                begin_next: Some(2)
            }
        );
        // Named: "sp" then flags
        assert_eq!(
            parsed(&[2, b's', 0, b'p', 0, 0]),
            EndTransaction {
                named: true,
                begin_next: None
            }
        );
        // Name longer than the message, and a begin flag with no level
        assert!(parse_end_transaction(&[5, b's', 0]).is_err());
        assert!(parse_end_transaction(&[0, 1]).is_err());
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
