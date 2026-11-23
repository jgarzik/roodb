//! RooDB client protocol implementation
//!
//! Implements the RooDB client wire protocol with STARTTLS support.

pub mod auth;
pub mod binary;
pub mod command;
pub mod error;
pub mod handshake;
pub mod packet;
pub mod prepared;
pub mod resultset;
pub mod sink;
pub mod starttls;
pub mod types;

// Re-export status flags for use by other modules
pub use handshake::status_flags;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::executor::cancel::{self, reason, Interrupt};
use crate::server::auth::AuthError;
use crate::server::context::ServerContext;
use crate::server::limits::MAX_PREAUTH_MESSAGE;
use crate::server::query::{metrics, QueryEngine, ResponseSink};

use self::auth::{verify_native_password_with_hash, HandshakeResponse41};
use self::command::{parse_command, ParsedCommand};
use self::error::{codes, states, ProtocolError, ProtocolResult};
use self::handshake::{capabilities, AUTH_PLUGIN_NAME};
use self::packet::{PacketReader, MAX_TOTAL_PAYLOAD_SIZE};
use self::prepared::{decode_execute_params, encode_prepare_ok, select_column_names};
use self::resultset::{
    default_status, encode_eof_packet, encode_err_packet, encode_ok_packet, ColumnDefinition41,
};
use self::sink::{RooDbSink, RowFormat};
use self::types::ColumnType;

/// RooDB protocol connection: packet framing, handshake, authentication and
/// command decoding. Statement execution is delegated to [`QueryEngine`].
pub struct RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// Packet reader (buffered to reduce syscalls), until the login is
    /// done; then it moves to the reader task (`spawn_reader`).
    reader: Option<PacketReader<BufReader<ReadHalf<S>>>>,
    /// Connection ID
    connection_id: u32,
    /// Scramble from handshake
    scramble: [u8; 20],
    /// Client capabilities
    client_capabilities: u32,
    /// Parameter types last bound per prepared statement; a client may
    /// omit them on later executions (new_params_bound_flag = 0)
    param_types: HashMap<u32, Vec<ColumnType>>,
    /// Statement execution and session state
    engine: QueryEngine,
}

impl<S> RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// Create a RooDB connection on a TLS stream.
    ///
    /// `scramble` is the challenge already sent in the plaintext greeting
    /// during the STARTTLS exchange.
    pub fn new(
        stream: S,
        connection_id: u32,
        client_ip: IpAddr,
        scramble: [u8; 20],
        ctx: &ServerContext,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader = PacketReader::new(BufReader::with_capacity(8192, read_half));
        reader.set_max_payload(MAX_PREAUTH_MESSAGE);
        RooDbConnection {
            reader: Some(reader),
            connection_id,
            scramble,
            client_capabilities: 0,
            param_types: HashMap::new(),
            engine: QueryEngine::new(
                connection_id,
                client_ip,
                ctx,
                Box::new(RooDbSink::new(write_half)),
            ),
        }
    }

    /// The RooDB protocol encoder this connection installed in the engine.
    fn out(&mut self) -> &mut RooDbSink<WriteHalf<S>> {
        self.engine
            .out
            .as_any_mut()
            .downcast_mut()
            .expect("engine sink is the RooDbSink this connection created")
    }

    /// Complete handshake after STARTTLS upgrade
    ///
    /// Reads the full HandshakeResponse41 that the client sends over TLS
    /// after the SSL request was acknowledged.
    pub async fn complete_handshake(&mut self) -> ProtocolResult<()> {
        info!(
            connection_id = self.connection_id,
            "Completing handshake over TLS"
        );

        // Read client's handshake response (sent over TLS)
        // After SSL upgrade, client re-sends with sequence 2
        let reader = self.login_reader();
        reader.set_sequence(2);
        let response_packet = reader.read_packet().await?;

        let response = HandshakeResponse41::parse(&response_packet)?;
        self.client_capabilities = response.capability_flags;
        let deprecate_eof = response.has_capability(capabilities::CLIENT_DEPRECATE_EOF);
        self.out().set_client_capabilities(
            deprecate_eof,
            response.has_capability(capabilities::CLIENT_MULTI_RESULTS),
        );

        debug!(
            connection_id = self.connection_id,
            client_capabilities = format!("{:#010x}", self.client_capabilities),
            deprecate_eof,
            "Client capabilities parsed"
        );

        // Verify authentication
        self.verify_auth(&response).await?;

        // Send OK
        self.out().set_sequence(3);
        self.out()
            .packet(&encode_ok_packet(0, 0, default_status(), 0));
        self.out().flush().await?;

        info!(
            connection_id = self.connection_id,
            username = %self.engine.session.user,
            "Handshake completed over TLS"
        );

        Ok(())
    }

    /// Send authentication error and return error
    async fn send_auth_error(&mut self, message: &str) -> ProtocolResult<()> {
        // The client's handshake response over TLS was sequence 2.
        self.out().set_sequence(3);
        self.out().packet(&encode_err_packet(
            codes::ER_ACCESS_DENIED,
            states::ACCESS_DENIED,
            message,
        ));
        self.out().flush().await?;
        Err(ProtocolError::AuthFailed(message.to_string()))
    }

    /// Verify authentication from handshake response
    async fn verify_auth(&mut self, response: &HandshakeResponse41) -> ProtocolResult<()> {
        let auth_plugin = response
            .auth_plugin_name
            .as_deref()
            .unwrap_or(AUTH_PLUGIN_NAME);

        if auth_plugin != AUTH_PLUGIN_NAME {
            warn!(plugin = auth_plugin, "Unsupported auth plugin");
            return self
                .send_auth_error("Unsupported authentication plugin")
                .await;
        }

        let authenticated = crate::server::auth::authenticate(
            &self.engine.storage,
            &response.username,
            self.engine.client_ip,
            |stored_hash| {
                verify_native_password_with_hash(
                    &self.scramble,
                    stored_hash,
                    &response.auth_response,
                )
            },
        )
        .await;

        match authenticated {
            Ok(()) => {}
            Err(AuthError::Denied) => return self.send_auth_error("Access denied").await,
            Err(AuthError::Internal(e)) => {
                warn!(error = %e, "User lookup failed");
                return Err(ProtocolError::AuthFailed(
                    "Internal error during authentication".to_string(),
                ));
            }
        }

        // Set authenticated user in session
        self.engine.set_user(&response.username);
        self.login_reader().set_max_payload(MAX_TOTAL_PAYLOAD_SIZE);

        // A session may begin in a database, which must be there (an empty
        // name is none).
        if let Some(db) = response.database.as_ref().filter(|db| !db.is_empty()) {
            if !self.engine.catalog.read().database_exists(db) {
                let message = format!("Unknown database '{db}'");
                self.out().set_sequence(3);
                self.out().packet(&encode_err_packet(
                    codes::ER_BAD_DB_ERROR,
                    "42000",
                    &message,
                ));
                self.out().flush().await?;
                return Err(ProtocolError::AuthFailed(message));
            }
            self.engine.session.set_database(Some(db.clone()));
        }

        Ok(())
    }

    /// Run the command loop until the client leaves or the connection
    /// fails. Either way the session ends here: an open transaction is
    /// rolled back and its temporary tables are dropped. (They are named in
    /// the shared catalog; a name left behind could never be used again.)
    ///
    /// Commands are read on a task of their own, so that a client that
    /// goes away is noticed while a statement runs; the connection runs
    /// inside its engine's interruption scope, so that the statement can
    /// stop.
    pub async fn run(&mut self) -> ProtocolResult<()> {
        let interrupt = self.engine.interrupt();
        let reader = self
            .reader
            .take()
            .expect("a connection runs once, after its login");
        let (requests, reader_task) = spawn_reader(reader, interrupt.clone());
        let result =
            cancel::scope(interrupt.clone(), self.command_loop(requests, &interrupt)).await;
        reader_task.abort();
        self.engine.reset_session(false).await;
        result
    }

    /// The packet reader, during the login.
    fn login_reader(&mut self) -> &mut PacketReader<BufReader<ReadHalf<S>>> {
        self.reader
            .as_mut()
            .expect("the login reads before the connection runs")
    }

    async fn command_loop(
        &mut self,
        mut requests: mpsc::Receiver<ProtocolResult<Vec<u8>>>,
        interrupt: &Interrupt,
    ) -> ProtocolResult<()> {
        let mut last_cmd_end = Instant::now();
        loop {
            debug!(
                connection_id = self.connection_id,
                "Waiting for next command"
            );

            // The next command, unless the connection is killed first
            let packet = tokio::select! {
                biased;
                () = interrupt.killed() => {
                    debug!(connection_id = self.connection_id, "Connection killed");
                    return Ok(());
                }
                packet = requests.recv() => packet,
            };
            let packet = match packet {
                Some(Ok(p)) => p,
                Some(Err(ProtocolError::ConnectionClosed)) | None => {
                    debug!(connection_id = self.connection_id, "Client disconnected");
                    return Ok(());
                }
                Some(Err(e)) => return Err(e),
            };

            let read_wait_ns = metrics::elapsed_ns(last_cmd_end);

            // Parse and handle command
            let cmd_start = Instant::now();
            let cmd = parse_command(&packet)?;
            let format = match cmd {
                ParsedCommand::StmtExecute { .. } => RowFormat::Binary,
                _ => RowFormat::Text,
            };
            self.out().start_command(format);

            let handled = self.handle_command(cmd).await;
            // A killed connection ends without its statement's result, as
            // in MySQL: the client sees the connection lost.
            if interrupt.has(reason::KILL_CONNECTION) {
                debug!(connection_id = self.connection_id, "Connection killed");
                return Ok(());
            }
            match handled {
                Ok(true) => {
                    self.out().finish().await?;
                    let cmd_ns = metrics::elapsed_ns(cmd_start);
                    tracing::debug!(
                        target: "roodb::perf",
                        read_wait_us = read_wait_ns / 1000,
                        cmd_us = cmd_ns / 1000,
                        "cmd_loop"
                    );
                    last_cmd_end = Instant::now();
                    continue;
                }
                Ok(false) => {
                    return Ok(()); // Client quit
                }
                Err(e) => {
                    // Try to send error to client
                    warn!(error = %e, "Command error");
                    let _ = self.engine.send_error_from_protocol_error(&e).await;
                    let _ = self.out().finish().await;
                    return Err(e);
                }
            }
        }
    }

    /// Handle a single command, returns false if client quit
    async fn handle_command(&mut self, cmd: ParsedCommand) -> ProtocolResult<bool> {
        match cmd {
            ParsedCommand::Quit => {
                debug!(connection_id = self.connection_id, "COM_QUIT");
                Ok(false)
            }

            ParsedCommand::Ping => {
                debug!(connection_id = self.connection_id, "COM_PING");
                self.engine.send_ok().await?;
                Ok(true)
            }

            ParsedCommand::InitDb(db) => {
                debug!(connection_id = self.connection_id, database = %db, "COM_INIT_DB");
                // Validate database exists
                let exists = {
                    let catalog = self.engine.catalog.read();
                    catalog.database_exists(&db)
                };
                if exists {
                    self.engine.session.set_database(Some(db));
                    self.engine.send_ok().await?;
                } else {
                    self.engine
                        .send_error(
                            codes::ER_BAD_DB_ERROR,
                            "42000",
                            &format!("Unknown database '{}'", db),
                        )
                        .await?;
                }
                Ok(true)
            }

            ParsedCommand::Query(sql) => {
                debug!(connection_id = self.connection_id, sql = %sql, "COM_QUERY");
                self.engine.handle_statement(&sql).await?;
                debug!(connection_id = self.connection_id, "Query completed");
                Ok(true)
            }

            ParsedCommand::StmtPrepare(sql) => {
                debug!(connection_id = self.connection_id, sql = %sql, "COM_STMT_PREPARE");
                self.handle_stmt_prepare(&sql).await?;
                Ok(true)
            }

            ParsedCommand::StmtExecute {
                statement_id,
                raw_payload,
            } => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_EXECUTE"
                );
                self.handle_stmt_execute(statement_id, &raw_payload).await?;
                Ok(true)
            }

            ParsedCommand::StmtClose(statement_id) => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_CLOSE"
                );
                self.engine.prepared_stmts.close(statement_id);
                self.param_types.remove(&statement_id);
                // No response needed for STMT_CLOSE per MySQL protocol
                Ok(true)
            }

            ParsedCommand::StmtReset(statement_id) => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_RESET"
                );
                // No cursor state to reset; just return OK
                self.engine.send_ok().await?;
                Ok(true)
            }

            ParsedCommand::ResetConnection => {
                debug!(connection_id = self.connection_id, "COM_RESET_CONNECTION");
                // Session state is reset as if the client had reconnected:
                // an open transaction is rolled back, temporary tables and
                // prepared statements go, variables return to their defaults.
                // The selected database is kept.
                self.engine.reset_session(false).await;
                self.param_types.clear();
                self.engine.send_ok().await?;
                Ok(true)
            }

            ParsedCommand::ProcessKill(id) => {
                // As KILL CONNECTION id
                match self.engine.kill(u64::from(id), false) {
                    Ok(()) => self.engine.send_ok().await?,
                    Err(e) => self.engine.out.error(&e),
                }
                Ok(true)
            }

            ParsedCommand::Unsupported(cmd) => {
                warn!(connection_id = self.connection_id, command = ?cmd, "Unsupported command");
                self.engine
                    .send_error(
                        codes::ER_UNKNOWN_COM_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Unsupported command: {:?}", cmd),
                    )
                    .await?;
                Ok(true)
            }
        }
    }

    /// Handle COM_STMT_PREPARE: parse SQL, store AST, send COM_STMT_PREPARE_OK.
    async fn handle_stmt_prepare(&mut self, sql: &str) -> ProtocolResult<()> {
        let ps = match self.engine.prepared_stmts.prepare(sql) {
            Ok(ps) => ps,
            Err(e) => {
                return self.engine.send_error_from_protocol_error(&e).await;
            }
        };

        let stmt_id = ps.id;
        let num_params = ps.param_count;

        // Derive column names from AST for SELECT statements.
        // The C MySQL client (libmysqlclient) requires column definitions
        // in the prepare response; without them it hangs waiting for data.
        let col_names = select_column_names(&ps.parsed_stmt).unwrap_or_default();
        let num_columns = col_names.len() as u16;

        // Send COM_STMT_PREPARE_OK
        let ok_packet = encode_prepare_ok(stmt_id, num_columns, num_params);
        self.out().packet(&ok_packet);

        // Send parameter column definitions (type=VARCHAR as placeholder)
        if num_params > 0 {
            let schema = self.engine.session.database.clone().unwrap_or_default();
            for i in 0..num_params {
                let col = crate::planner::logical::OutputColumn {
                    id: i as usize,
                    name: "?".to_string(),
                    data_type: crate::catalog::DataType::Varchar(255),
                    nullable: true,
                };
                let def = ColumnDefinition41::from_output_column(&col, "", &schema);
                self.out().packet(&def.encode());
            }

            // EOF after parameter definitions
            if !self.out().deprecate_eof() {
                let eof = encode_eof_packet(0, default_status());
                self.out().packet(&eof);
            }
        }

        // Send result column definitions (placeholder types for SELECT)
        if num_columns > 0 {
            let schema = self.engine.session.database.clone().unwrap_or_default();
            for (i, name) in col_names.iter().enumerate() {
                let col = crate::planner::logical::OutputColumn {
                    id: i,
                    name: name.clone(),
                    data_type: crate::catalog::DataType::Varchar(255),
                    nullable: true,
                };
                let def = ColumnDefinition41::from_output_column(&col, "", &schema);
                self.out().packet(&def.encode());
            }

            // EOF after column definitions
            if !self.out().deprecate_eof() {
                let eof = encode_eof_packet(0, default_status());
                self.out().packet(&eof);
            }
        }

        Ok(())
    }

    /// Handle COM_STMT_EXECUTE: decode params, substitute into AST, run pipeline.
    async fn handle_stmt_execute(
        &mut self,
        statement_id: u32,
        raw_payload: &[u8],
    ) -> ProtocolResult<()> {
        // Look up statement
        let param_count = match self.engine.prepared_stmts.get(statement_id) {
            Some(ps) => ps.param_count,
            None => {
                return self
                    .engine
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Unknown prepared statement id: {}", statement_id),
                    )
                    .await;
            }
        };

        // Decode binary parameters from payload
        let prev_types = self.param_types.get(&statement_id).map(Vec::as_slice);
        let (params, used_types) = match decode_execute_params(raw_payload, param_count, prev_types)
        {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    connection_id = self.connection_id,
                    statement_id,
                    error = %e,
                    "COM_STMT_EXECUTE parameter decode failed"
                );
                return self.engine.send_error_from_protocol_error(&e).await;
            }
        };

        // Store the used types for subsequent executions with new_params_bound=0
        self.param_types.insert(statement_id, used_types);

        self.engine.execute_prepared(statement_id, &params).await
    }
}

/// Read the client's commands on a task of their own, handing them over
/// one at a time. A read that fails, the client having gone, interrupts
/// the statement then running: nothing would read its result.
fn spawn_reader<R>(
    mut reader: PacketReader<R>,
    interrupt: Arc<Interrupt>,
) -> (mpsc::Receiver<ProtocolResult<Vec<u8>>>, JoinHandle<()>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        loop {
            // After the login every client message is a command, and a
            // command starts a new packet sequence.
            reader.reset_sequence();
            let result = reader.read_packet().await;
            let last = result.is_err();
            if last {
                interrupt.raise(reason::CLIENT_GONE);
            }
            if tx.send(result).await.is_err() || last {
                break;
            }
        }
    });
    (rx, task)
}

#[cfg(test)]
mod tests {
    use super::auth::{compute_password_hash, verify_native_password_with_hash};

    #[test]
    fn test_password_hash_verification() {
        let scramble = b"12345678901234567890";
        let password = "secret123";

        // Compute stored hash (what would be in system.users)
        let stored_hash = compute_password_hash(password);

        // Compute auth response as client would
        let auth_response = super::auth::compute_auth_response(scramble, password);

        // Verify using stored hash
        assert!(verify_native_password_with_hash(
            scramble,
            &stored_hash,
            &auth_response
        ));

        // Wrong password should fail
        let wrong_response = super::auth::compute_auth_response(scramble, "wrongpassword");
        assert!(!verify_native_password_with_hash(
            scramble,
            &stored_hash,
            &wrong_response
        ));
    }

    #[test]
    fn test_empty_password_verification() {
        let scramble = b"12345678901234567890";

        // Empty password should have empty hash and empty auth response
        let stored_hash = compute_password_hash("");
        assert!(stored_hash.is_empty());

        // Empty auth response should match empty password
        assert!(verify_native_password_with_hash(scramble, "", &[]));

        // Non-empty auth response should fail for empty password
        let auth_response = super::auth::compute_auth_response(scramble, "nonempty");
        assert!(!verify_native_password_with_hash(
            scramble,
            "",
            &auth_response
        ));
    }
}
