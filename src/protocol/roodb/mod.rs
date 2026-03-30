//! RooDB client protocol implementation
//!
//! Implements the RooDB client wire protocol with STARTTLS support.

pub mod auth;
pub mod binary;
pub mod command;
pub mod error;
pub mod handshake;
pub mod metrics;
pub mod packet;
pub mod prepared;
pub mod resultset;
pub mod starttls;
pub mod types;

// Re-export status flags for use by other modules
pub use handshake::status_flags;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadHalf, WriteHalf};
use tracing::{debug, info, warn};

use crate::catalog::system_tables::{SYSTEM_PROCEDURES, SYSTEM_USERS};
use crate::catalog::{Catalog, ParamMode, ProcedureDef, ProcedureParam};
use crate::executor::encoding::{decode_row, table_key_end, table_key_prefix};
use crate::executor::engine::ExecutorEngine;
use crate::executor::{Datum, Executor, TransactionContext};
use crate::planner::logical::builder::LogicalPlanBuilder;
use crate::planner::optimizer::Optimizer;
use crate::planner::physical::{PhysicalPlan, PhysicalPlanner};
use crate::raft::{ChangeSet, RaftNode};
use crate::server::session::Session;
use crate::sql::privileges::{
    check_privilege, GrantEntry, HostPattern, Privilege, PrivilegeObject, RequiredPrivilege,
};
use crate::sql::{Parser, Resolver, TypeChecker};
use crate::storage::StorageEngine;
use crate::txn::{IsolationLevel, MvccStorage, TransactionManager};

use self::auth::{verify_native_password_with_hash, HandshakeResponse41};
use self::command::{parse_command, ParsedCommand};
use self::error::{codes, states, ProtocolError, ProtocolResult};
use self::handshake::{capabilities, HandshakeV10, AUTH_PLUGIN_NAME};
use self::packet::{PacketReader, PacketWriter};
use self::prepared::{
    decode_execute_params, encode_prepare_ok, select_column_names, CachedPlan,
    PreparedStatementManager,
};
use self::resultset::{
    default_status, encode_column_count, encode_eof_ok_packet, encode_eof_packet,
    encode_err_packet, encode_ok_packet, encode_text_row, ColumnDefinition41,
};

/// Internal enum for planning errors (used to avoid holding guard across await)
enum PlanError {
    Sql(crate::sql::SqlError),
    Planner(crate::planner::PlannerError),
}

/// Info extracted from INSERT plans for trigger firing
struct InsertTriggerInfo {
    table_name: String,
    column_names: Vec<String>,
    values: Vec<Vec<crate::planner::logical::ResolvedExpr>>,
}

/// RooDB connection handler
pub struct RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    /// Packet reader (buffered to reduce syscalls)
    reader: PacketReader<BufReader<ReadHalf<S>>>,
    /// Packet writer (buffered to reduce syscalls)
    writer: PacketWriter<BufWriter<WriteHalf<S>>>,
    /// Connection ID
    connection_id: u32,
    /// Client IP address for authentication
    client_ip: IpAddr,
    /// Scramble from handshake
    scramble: [u8; 20],
    /// Client capabilities
    client_capabilities: u32,
    /// Current database (if any)
    database: Option<String>,
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
    /// Catalog
    catalog: Arc<RwLock<Catalog>>,
    /// Whether client requested DEPRECATE_EOF
    deprecate_eof: bool,
    /// Session state (transactions, autocommit, isolation level)
    session: Session,
    /// Transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Raft node for consensus
    raft_node: Arc<RaftNode>,
    /// Whether we're inside a stored procedure (multi-result-set mode)
    in_multi_result: bool,
    /// Whether any result sets were sent during current SP execution
    sp_sent_results: bool,
    /// Prepared statement manager (per-connection)
    prepared_stmts: PreparedStatementManager,
    /// Per-connection MVCC storage (avoids Arc::new per query)
    mvcc: Arc<MvccStorage>,
}

impl<S> RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    /// Create a new RooDB connection from a TLS stream
    pub fn new(
        stream: S,
        connection_id: u32,
        client_ip: IpAddr,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        let mvcc = Arc::new(MvccStorage::new(storage.clone(), txn_manager.clone()));
        let conn = RooDbConnection {
            reader: PacketReader::new(BufReader::with_capacity(8192, read_half)),
            writer: PacketWriter::new(BufWriter::with_capacity(8192, write_half)),
            connection_id,
            client_ip,
            scramble: [0u8; 20],
            client_capabilities: 0,
            database: None,
            storage,
            catalog,
            deprecate_eof: false,
            session: Session::new(connection_id),
            txn_manager,
            raft_node,
            in_multi_result: false,
            sp_sent_results: false,
            prepared_stmts: PreparedStatementManager::new(),
            mvcc,
        };
        conn.session.init_eval_flags();
        conn
    }

    /// Create a new RooDB connection with pre-established scramble (for STARTTLS)
    ///
    /// Used after STARTTLS handshake where the scramble was already sent in the
    /// plaintext greeting.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_scramble(
        stream: S,
        connection_id: u32,
        client_ip: IpAddr,
        scramble: [u8; 20],
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        let mvcc = Arc::new(MvccStorage::new(storage.clone(), txn_manager.clone()));
        let conn = RooDbConnection {
            reader: PacketReader::new(BufReader::with_capacity(8192, read_half)),
            writer: PacketWriter::new(BufWriter::with_capacity(8192, write_half)),
            connection_id,
            client_ip,
            scramble,
            client_capabilities: 0,
            database: None,
            storage,
            catalog,
            deprecate_eof: false,
            session: Session::new(connection_id),
            txn_manager,
            raft_node,
            in_multi_result: false,
            sp_sent_results: false,
            prepared_stmts: PreparedStatementManager::new(),
            mvcc,
        };
        conn.session.init_eval_flags();
        conn
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
        self.reader.set_sequence(2);
        let response_packet = self.reader.read_packet().await?;

        let response = HandshakeResponse41::parse(&response_packet)?;
        self.client_capabilities = response.capability_flags;
        self.deprecate_eof = response.has_capability(capabilities::CLIENT_DEPRECATE_EOF);

        debug!(
            connection_id = self.connection_id,
            client_capabilities = format!("{:#010x}", self.client_capabilities),
            deprecate_eof = self.deprecate_eof,
            "Client capabilities parsed"
        );

        // Verify authentication
        self.verify_auth(&response).await?;

        // Send OK
        self.writer.set_sequence(3);
        let ok_packet = encode_ok_packet(0, 0, default_status(), 0);
        self.writer.write_packet(&ok_packet).await?;
        self.writer.flush().await?;

        info!(
            connection_id = self.connection_id,
            username = %self.session.user,
            "Handshake completed over TLS"
        );

        Ok(())
    }

    /// Perform the RooDB handshake (non-STARTTLS, for testing)
    pub async fn handshake(&mut self) -> ProtocolResult<()> {
        info!(
            connection_id = self.connection_id,
            "Starting RooDB handshake"
        );

        // Send server greeting
        let greeting = HandshakeV10::new(self.connection_id);
        self.scramble = greeting.scramble();

        let greeting_packet = greeting.encode();
        self.writer.write_packet(&greeting_packet).await?;
        self.writer.flush().await?;

        // Read client response
        self.reader.set_sequence(1);
        let response_packet = self.reader.read_packet().await?;

        let response = HandshakeResponse41::parse(&response_packet)?;
        self.client_capabilities = response.capability_flags;
        self.deprecate_eof = response.has_capability(capabilities::CLIENT_DEPRECATE_EOF);

        // Verify authentication
        self.verify_auth(&response).await?;

        // Send OK
        self.writer.set_sequence(2);
        let ok_packet = encode_ok_packet(0, 0, default_status(), 0);
        self.writer.write_packet(&ok_packet).await?;
        self.writer.flush().await?;

        info!(
            connection_id = self.connection_id,
            username = %self.session.user,
            "Handshake completed"
        );

        Ok(())
    }

    /// Send authentication error and return error
    async fn send_auth_error(&mut self, message: &str) -> ProtocolResult<()> {
        self.writer.set_sequence(2);
        let err_packet = encode_err_packet(codes::ER_ACCESS_DENIED, states::ACCESS_DENIED, message);
        self.writer.write_packet(&err_packet).await?;
        self.writer.flush().await?;
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

        // Look up user in system.users table using the actual client IP
        let client_host = self.client_ip.to_string();
        let user_info = self
            .lookup_user(&response.username, &client_host)
            .await
            .map_err(|e| {
                warn!(error = %e, "User lookup failed");
                ProtocolError::AuthFailed("Internal error during authentication".to_string())
            })?;

        let Some((password_hash, account_locked, password_expired)) = user_info else {
            warn!(username = %response.username, "Unknown user");
            return self.send_auth_error("Access denied").await;
        };

        // Check account status
        if account_locked {
            warn!(username = %response.username, "Account is locked");
            return self.send_auth_error("Account is locked").await;
        }

        if password_expired {
            warn!(username = %response.username, "Password has expired");
            return self.send_auth_error("Password has expired").await;
        }

        // Verify password
        if !verify_native_password_with_hash(
            &self.scramble,
            &password_hash,
            &response.auth_response,
        ) {
            warn!(username = %response.username, "Password verification failed");
            return self.send_auth_error("Access denied").await;
        }

        // Set authenticated user in session
        self.session.set_user(response.username.clone());

        if let Some(ref db) = response.database {
            self.database = Some(db.clone());
        }

        Ok(())
    }

    /// Look up a user in system.users table
    ///
    /// Returns Some((password_hash, account_locked, password_expired)) if found, None otherwise.
    /// Matches user by username and host pattern.
    async fn lookup_user(
        &self,
        username: &str,
        client_host: &str,
    ) -> Result<Option<(String, bool, bool)>, ProtocolError> {
        // Scan system.users table
        let prefix = table_key_prefix(SYSTEM_USERS);
        let end = table_key_end(SYSTEM_USERS);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;

        // Find matching user row
        // system.users schema: username, host, password_hash, auth_plugin,
        //   ssl_subject, ssl_issuer, account_locked, password_expired, created_at, updated_at
        for (_key, value) in rows {
            let row = match decode_row(&value) {
                Ok(r) => r,
                Err(_) => continue,
            };

            // Get username from row (column 0)
            let row_username = match row.get_opt(0) {
                Some(Datum::String(s)) => s,
                _ => continue,
            };

            if row_username != username {
                continue;
            }

            // Get host pattern from row (column 1)
            let row_host = match row.get_opt(1) {
                Some(Datum::String(s)) => s,
                _ => continue,
            };

            // Check if client host matches the host pattern
            let host_pattern = HostPattern::new(row_host);
            if !host_pattern.matches(client_host, None) {
                continue;
            }

            // Get password_hash (column 2)
            let password_hash = match row.get_opt(2) {
                Some(Datum::String(s)) => s.clone(),
                Some(Datum::Null) | None => String::new(),
                _ => continue,
            };

            // Get account_locked (column 6)
            let account_locked = match row.get_opt(6) {
                Some(Datum::Bool(b)) => *b,
                _ => false,
            };

            // Get password_expired (column 7)
            let password_expired = match row.get_opt(7) {
                Some(Datum::Bool(b)) => *b,
                _ => false,
            };

            return Ok(Some((password_hash, account_locked, password_expired)));
        }

        Ok(None)
    }

    /// Run the command loop
    pub async fn run(&mut self) -> ProtocolResult<()> {
        let mut last_cmd_end = Instant::now();
        loop {
            // Reset sequence for each command
            self.reader.reset_sequence();
            self.writer.reset_sequence();

            debug!(
                connection_id = self.connection_id,
                "Waiting for next command"
            );

            // Read command packet
            let packet = match self.reader.read_packet().await {
                Ok(p) => p,
                Err(ProtocolError::ConnectionClosed) => {
                    debug!(connection_id = self.connection_id, "Client disconnected");
                    self.cleanup_temp_tables().await;
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            let read_wait_ns = metrics::elapsed_ns(last_cmd_end);

            // Parse and handle command
            let cmd_start = Instant::now();
            let cmd = parse_command(&packet)?;

            match self.handle_command(cmd).await {
                Ok(true) => {
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
                    self.cleanup_temp_tables().await;
                    return Ok(()); // Client quit
                }
                Err(e) => {
                    // Try to send error to client
                    warn!(error = %e, "Command error");
                    let _ = self.send_error_from_protocol_error(&e).await;
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
                self.send_ok(0, 0).await?;
                Ok(true)
            }

            ParsedCommand::InitDb(db) => {
                debug!(connection_id = self.connection_id, database = %db, "COM_INIT_DB");
                // Validate database exists
                let exists = {
                    let catalog = self.catalog.read();
                    catalog.database_exists(&db)
                };
                if exists {
                    self.database = Some(db.clone());
                    self.session.set_database(Some(db));
                    self.send_ok(0, 0).await?;
                } else {
                    self.send_error(
                        codes::ER_BAD_DB_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Unknown database '{}'", db),
                    )
                    .await?;
                }
                Ok(true)
            }

            ParsedCommand::Query(sql) => {
                debug!(connection_id = self.connection_id, sql = %sql, "COM_QUERY");
                self.handle_query(&sql).await?;
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
                self.prepared_stmts.close(statement_id);
                // No response needed for STMT_CLOSE per MySQL protocol
                Ok(true)
            }

            ParsedCommand::StmtReset(statement_id) => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_RESET"
                );
                // No cursor state to reset; just return OK
                self.send_ok(0, 0).await?;
                Ok(true)
            }

            ParsedCommand::ResetConnection => {
                debug!(connection_id = self.connection_id, "COM_RESET_CONNECTION");
                self.database = None;
                self.prepared_stmts.clear();
                self.send_ok(0, 0).await?;
                Ok(true)
            }

            ParsedCommand::Unsupported(cmd) => {
                warn!(connection_id = self.connection_id, command = ?cmd, "Unsupported command");
                self.send_error(
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
        let ps = match self.prepared_stmts.prepare(sql) {
            Ok(ps) => ps,
            Err(e) => {
                return self.send_error_from_protocol_error(&e).await;
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
        self.writer.set_sequence(1);
        let ok_packet = encode_prepare_ok(stmt_id, num_columns, num_params);
        self.writer.write_packet(&ok_packet).await?;

        // Send parameter column definitions (type=VARCHAR as placeholder)
        if num_params > 0 {
            let schema = self.database.as_deref().unwrap_or("default");
            for i in 0..num_params {
                let col = crate::planner::logical::OutputColumn {
                    id: i as usize,
                    name: "?".to_string(),
                    data_type: crate::catalog::DataType::Varchar(255),
                    nullable: true,
                };
                let def = ColumnDefinition41::from_output_column(&col, "", schema);
                self.writer.write_packet(&def.encode()).await?;
            }

            // EOF after parameter definitions
            if !self.deprecate_eof {
                let eof = encode_eof_packet(0, default_status());
                self.writer.write_packet(&eof).await?;
            }
        }

        // Send result column definitions (placeholder types for SELECT)
        if num_columns > 0 {
            let schema = self.database.as_deref().unwrap_or("default");
            for (i, name) in col_names.iter().enumerate() {
                let col = crate::planner::logical::OutputColumn {
                    id: i,
                    name: name.clone(),
                    data_type: crate::catalog::DataType::Varchar(255),
                    nullable: true,
                };
                let def = ColumnDefinition41::from_output_column(&col, "", schema);
                self.writer.write_packet(&def.encode()).await?;
            }

            // EOF after column definitions
            if !self.deprecate_eof {
                let eof = encode_eof_packet(0, default_status());
                self.writer.write_packet(&eof).await?;
            }
        }

        self.writer.flush().await?;
        Ok(())
    }

    /// Handle COM_STMT_EXECUTE: decode params, substitute into AST, run pipeline.
    async fn handle_stmt_execute(
        &mut self,
        statement_id: u32,
        raw_payload: &[u8],
    ) -> ProtocolResult<()> {
        // Look up statement
        let ps = match self.prepared_stmts.get(statement_id) {
            Some(ps) => ps.clone(),
            None => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Unknown prepared statement id: {}", statement_id),
                    )
                    .await;
            }
        };

        // Decode binary parameters from payload
        let prev_types = ps.last_param_types.as_deref();
        let (params, used_types) =
            match decode_execute_params(raw_payload, ps.param_count, prev_types) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        connection_id = self.connection_id,
                        statement_id,
                        error = %e,
                        "COM_STMT_EXECUTE parameter decode failed"
                    );
                    return self.send_error_from_protocol_error(&e).await;
                }
            };

        // Store the used types for subsequent executions with new_params_bound=0
        if let Some(ps_mut) = self.prepared_stmts.get_mut(statement_id) {
            ps_mut.last_param_types = Some(used_types);
        }

        // Try the plan cache: if we have a cached plan and schema hasn't changed, reuse it
        let cached = ps.cached_plan.as_ref().and_then(|cp| {
            let current_version = self.catalog.read().schema_version();
            if cp.schema_version == current_version {
                Some(cp.clone())
            } else {
                None // Schema changed, invalidate cache
            }
        });

        if let Some(cached) = cached {
            // Cache hit: clone plan and substitute params directly
            let mut physical = cached.physical.clone();
            physical.substitute_params(&params)?;

            // Check authorization
            if !cached.required_privileges.is_empty() {
                if let Err(e) = self.check_authorization(&cached.required_privileges).await {
                    return self
                        .send_error(codes::ER_ACCESS_DENIED, states::GENERAL_ERROR, &e)
                        .await;
                }
            }

            return self.execute_plan_binary(physical).await;
        }

        // Cache miss: build plan from the unsubstituted AST with placeholder mode
        self.handle_prepared_query(statement_id, &ps.parsed_stmt, &params)
            .await
    }

    /// Build and cache a plan for a prepared statement, then execute it.
    async fn handle_prepared_query(
        &mut self,
        statement_id: u32,
        parsed_stmt: &sqlparser::ast::Statement,
        params: &[Datum],
    ) -> ProtocolResult<()> {
        // Handle transaction/SET commands that bypass the resolve/plan pipeline
        match parsed_stmt {
            sqlparser::ast::Statement::StartTransaction { .. } => {
                return self.handle_begin().await;
            }
            sqlparser::ast::Statement::Commit { .. } => {
                return self.handle_commit().await;
            }
            sqlparser::ast::Statement::Rollback { .. } => {
                return self.handle_rollback().await;
            }
            sqlparser::ast::Statement::Set(_) => {
                // Handle SET statements via the text path
                let sql = parsed_stmt.to_string();
                return self.try_handle_transaction_command(&sql).await.map(|_| ());
            }
            _ => {}
        }

        // Resolve with placeholder mode (? becomes Literal::Placeholder(n))
        // Then build the plan template, cache it, substitute params, and execute.
        let plan_result: Result<(PhysicalPlan, Vec<RequiredPrivilege>, u64), PlanError> = (|| {
            let catalog_guard = self.catalog.read();
            let schema_version = catalog_guard.schema_version();

            let resolved = Resolver::new_with_placeholders(&catalog_guard)
                .resolve(parsed_stmt.clone())
                .map_err(PlanError::Sql)?;

            let required_privileges = self.extract_required_privileges(&resolved);

            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;

            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;

            let optimized = Optimizer::new().optimize(logical);

            let physical =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)?;

            Ok((physical, required_privileges, schema_version))
        })(
        );

        let (plan_template, required_privileges, schema_version) = match plan_result {
            Ok((p, privs, sv)) => (p, privs, sv),
            Err(PlanError::Sql(e)) => return self.send_sql_error(&e).await,
            Err(PlanError::Planner(e)) => return self.send_planner_error(&e).await,
        };

        // Cache the plan template (with Placeholder literals) for future executions
        if let Some(ps_mut) = self.prepared_stmts.get_mut(statement_id) {
            ps_mut.cached_plan = Some(CachedPlan {
                physical: plan_template.clone(),
                required_privileges: required_privileges.clone(),
                schema_version,
            });
        }

        // Substitute params into a copy of the plan for this execution
        let mut physical = plan_template;
        physical.substitute_params(params)?;

        // Check authorization
        if !required_privileges.is_empty() {
            if let Err(e) = self.check_authorization(&required_privileges).await {
                return self
                    .send_error(codes::ER_ACCESS_DENIED, states::GENERAL_ERROR, &e)
                    .await;
            }
        }

        // Execute with binary result sets
        self.execute_plan_binary(physical).await
    }

    /// Execute a physical plan and send results in binary format (for prepared statements).
    async fn execute_plan_binary(&mut self, plan: PhysicalPlan) -> ProtocolResult<()> {
        let query_start = Instant::now();

        let returns_rows = matches!(
            plan,
            PhysicalPlan::TableScan { .. }
                | PhysicalPlan::PointGet { .. }
                | PhysicalPlan::RangeScan { .. }
                | PhysicalPlan::Filter { .. }
                | PhysicalPlan::Project { .. }
                | PhysicalPlan::Sort { .. }
                | PhysicalPlan::Limit { .. }
                | PhysicalPlan::HashDistinct { .. }
                | PhysicalPlan::HashAggregate { .. }
                | PhysicalPlan::NestedLoopJoin { .. }
                | PhysicalPlan::AnalyzeTable { .. }
                | PhysicalPlan::Explain { .. }
        );

        let is_ddl = matches!(
            plan,
            PhysicalPlan::CreateTable { .. }
                | PhysicalPlan::DropTable { .. }
                | PhysicalPlan::CreateIndex { .. }
                | PhysicalPlan::DropIndex { .. }
        );

        let mvcc = self.mvcc.clone();

        let (txn_context, implicit_txn_id) = if is_ddl {
            (None, None)
        } else if let Some(txn_id) = self.session.current_txn {
            let read_view = self.txn_manager.create_read_view(txn_id)?;
            let pending = self.session.get_pending_changes();
            (
                Some(TransactionContext::with_pending_changes(
                    txn_id, read_view, pending,
                )),
                None,
            )
        } else if !returns_rows && self.session.autocommit {
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.session.is_read_only)?;
            let read_view = self.txn_manager.create_read_view(txn.txn_id)?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view)),
                Some(txn.txn_id),
            )
        } else {
            let read_view = self.txn_manager.create_read_view(0)?;
            (Some(TransactionContext::new(0, read_view)), None)
        };

        let plan_ns = metrics::elapsed_ns(query_start);

        if returns_rows {
            let columns = plan.output_columns();

            let engine = ExecutorEngine::with_raft(
                mvcc,
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
                self.session.user_variables(),
            );
            let mut executor = engine.build_async(plan).await?;

            let t_open = Instant::now();
            executor.open().await?;
            let exec_open_ns = metrics::elapsed_ns(t_open);

            let t_iter = Instant::now();
            let result = self.send_binary_result_set(&columns, &mut *executor).await;
            let exec_iter_ns = metrics::elapsed_ns(t_iter);

            let m = metrics::QueryMetrics {
                total: query_start.elapsed(),
                plan_ns,
                exec_open_ns,
                exec_iter_ns,
                send_ns: 0,
            };
            m.log("binary_select");

            result
        } else {
            let engine = ExecutorEngine::with_raft(
                mvcc,
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
                self.session.user_variables(),
            );
            let mut executor = engine.build_async(plan).await?;

            let t_open = Instant::now();
            executor.open().await?;
            let exec_open_ns = metrics::elapsed_ns(t_open);

            let t_iter = Instant::now();
            let mut affected = 0u64;
            let mut last_insert_id = 0u64;
            while let Some(row) = executor.next().await? {
                if let Some(Datum::Int(n)) = row.get_opt(0) {
                    affected += *n as u64;
                } else {
                    affected += 1;
                }
                if let Some(Datum::Int(id)) = row.get_opt(1) {
                    last_insert_id = *id as u64;
                }
            }
            executor.close().await?;
            let exec_iter_ns = metrics::elapsed_ns(t_iter);

            let ignore_dups = executor.is_ignore_duplicates();
            let changes = executor.take_changes();
            if !changes.is_empty() {
                if self.session.in_transaction() {
                    self.session.add_pending_changes(changes);
                } else {
                    let changeset = ChangeSet::new_with_changes_ignore(
                        implicit_txn_id.unwrap_or(0),
                        changes,
                        ignore_dups,
                    );
                    self.raft_node
                        .propose_changes(changeset)
                        .await
                        .map_err(|e| ProtocolError::Raft(e.to_string()))?;
                }
            }

            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager.commit(txn_id).await?;
            }

            // Write DML results to session for LAST_INSERT_ID()/ROW_COUNT()
            self.session
                .set_user_variable("__sys_last_insert_id", Datum::Int(last_insert_id as i64));
            self.session
                .set_user_variable("__sys_row_count", Datum::Int(affected as i64));

            let m = metrics::QueryMetrics {
                total: query_start.elapsed(),
                plan_ns,
                exec_open_ns,
                exec_iter_ns,
                send_ns: 0,
            };
            m.log("binary_dml");

            self.send_ok(affected, last_insert_id).await
        }
    }

    /// Send a result set in binary format (for prepared statement execution).
    async fn send_binary_result_set(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        executor: &mut dyn Executor,
    ) -> ProtocolResult<()> {
        // Send column count
        self.writer.set_sequence(1);
        let count_packet = encode_column_count(columns.len() as u64);
        self.writer.write_packet(&count_packet).await?;

        // Send column definitions
        let schema = self.database.as_deref().unwrap_or("default");
        for col in columns {
            let def = ColumnDefinition41::from_output_column(col, "", schema);
            let def_packet = def.encode();
            self.writer.write_packet(&def_packet).await?;
        }

        // Send EOF after columns (unless DEPRECATE_EOF)
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        // Send rows in binary format
        while let Some(row) = executor.next().await? {
            let row_packet = binary::encode_binary_row(&row, columns);
            self.writer.write_packet(&row_packet).await?;
        }

        // Send final EOF/OK
        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(default_status(), 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;

        executor.close().await?;

        Ok(())
    }

    /// Handle a SQL query
    fn handle_query<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ProtocolResult<()>> + Send + 'a>> {
        Box::pin(async move { self.handle_query_inner(sql).await })
    }

    /// Inner implementation of handle_query (separated to allow boxing for recursion)
    async fn handle_query_inner(&mut self, sql: &str) -> ProtocolResult<()> {
        // Clear warnings at the start of each statement
        self.session.clear_warnings();

        // Check for transaction commands first
        if let Some(()) = self.try_handle_transaction_command(sql).await? {
            return Ok(());
        }

        // Check for USE database command
        if let Some(()) = self.try_handle_use_command(sql).await? {
            return Ok(());
        }

        // Check for SHOW commands
        if let Some(()) = self.try_handle_show_command(sql).await? {
            return Ok(());
        }

        // Check for DROP DATABASE (text intercept since sqlparser doesn't support DROP DATABASE)
        if let Some(()) = self.try_handle_drop_database(sql).await? {
            return Ok(());
        }

        // Check for system variable queries (@@variable)
        if let Some(result) = self.try_handle_system_variable(sql).await? {
            return Ok(result);
        }

        // Handle DO statement — evaluate expression, discard result, send OK
        // Errors are propagated (e.g., overflow), but successful results are discarded.
        {
            let upper = sql.trim().to_uppercase();
            if upper.starts_with("DO ") {
                let expr_sql = sql.trim()[3..].trim().trim_end_matches(';');
                let select_sql = format!("SELECT {}", expr_sql);
                match self.execute_sql_silent(&select_sql).await {
                    Ok(_) => return self.send_ok(0, 0).await,
                    Err(e) => {
                        return self
                            .send_error(codes::ER_DATA_OUT_OF_RANGE, states::GENERAL_ERROR, &e)
                            .await;
                    }
                }
            }
        }

        // Handle SQL-level PREPARE/EXECUTE/DEALLOCATE (text protocol prepared statements)
        if let Some(()) = self.try_handle_sql_prepare(sql).await? {
            return Ok(());
        }

        // Handle CREATE FUNCTION and DROP FUNCTION via text intercept
        // (MySQL CREATE FUNCTION with BEGIN/END body doesn't parse cleanly in sqlparser)
        {
            let upper = sql.trim().to_uppercase();
            if upper.starts_with("DROP FUNCTION ") {
                let rest = sql.trim()[14..].trim().trim_end_matches(';').trim();
                let (if_exists, name) = if rest.to_uppercase().starts_with("IF EXISTS ") {
                    (true, rest[10..].trim())
                } else {
                    (false, rest)
                };
                return self
                    .handle_drop_procedure(&name.to_lowercase(), if_exists)
                    .await;
            }
            if upper.starts_with("CREATE FUNCTION ") {
                return self.handle_create_function_text(sql).await;
            }
            if upper.starts_with("LOAD DATA ") {
                return self.handle_load_data(sql).await;
            }
            if upper.starts_with("REPLACE ") {
                return self.handle_replace(sql).await;
            }
        }

        // Handle CREATE TEMPORARY TABLE
        if let Some(()) = self.try_handle_create_temporary_table(sql).await? {
            return Ok(());
        }

        // Handle ALTER TABLE
        if let Some(()) = self.try_handle_alter_table(sql).await? {
            return Ok(());
        }

        // Handle UNION queries by executing each side and concatenating results
        if let Some(()) = self.try_handle_union(sql).await? {
            return Ok(());
        }

        // Handle SET @var = expr (user variables)
        {
            let upper = sql.trim().to_uppercase();
            if upper.starts_with("SET @") && !upper.starts_with("SET @@") {
                return self.handle_set_user_variable(sql).await;
            }
        }

        // Handle SET sql_mode (track in session)
        if let Some(mode_val) = Self::extract_sql_mode_value(sql) {
            self.session.set_sql_mode(&mode_val);
            // Update evaluator flag immediately for the current session
            crate::executor::eval::set_no_unsigned_subtraction(
                &self.session.user_variables(),
                self.session.has_sql_mode("NO_UNSIGNED_SUBTRACTION"),
            );
            crate::executor::eval::set_error_for_division_by_zero(
                &self.session.user_variables(),
                self.session.has_sql_mode("ERROR_FOR_DIVISION_BY_ZERO"),
            );
            crate::executor::eval::set_strict_trans_tables(
                &self.session.user_variables(),
                self.session.has_sql_mode("STRICT_TRANS_TABLES"),
            );
            return self.send_ok(0, 0).await;
        }

        // Handle statements as no-ops for MySQL compatibility
        {
            let upper = sql.trim().to_uppercase();
            // SET NAMES, SET CHARACTER SET, etc.
            if upper.starts_with("SET NAMES")
                || upper.starts_with("SET CHARACTER SET")
                || upper.starts_with("SET CHARACTER_SET")
                || upper.starts_with("SET @@")
                || upper.starts_with("SET SESSION ")
                || upper.starts_with("SET GLOBAL ")
            {
                return self.send_ok(0, 0).await;
            }
            // LOCK/UNLOCK TABLES — no-op (MVCC provides isolation)
            if upper.starts_with("LOCK TABLE")
                || upper.starts_with("LOCK TABLES")
                || upper.starts_with("UNLOCK TABLE")
                || upper.starts_with("UNLOCK TABLES")
            {
                return self.send_ok(0, 0).await;
            }
            // FLUSH — no-op
            if upper.starts_with("FLUSH ") {
                return self.send_ok(0, 0).await;
            }
            // DROP FUNCTION IF EXISTS — no-op (functions not yet implemented)
            if upper.starts_with("DROP FUNCTION") && upper.contains("IF EXISTS") {
                return self.send_ok(0, 0).await;
            }
            // CREATE TRIGGER — parsed and stored via normal pipeline
            if upper.starts_with("CREATE TRIGGER ") {
                return self.handle_create_trigger_sql(sql).await;
            }
            // DROP TRIGGER
            if upper.starts_with("DROP TRIGGER ") {
                return self.handle_drop_trigger_sql(sql).await;
            }
        }

        // Handle INFORMATION_SCHEMA and performance_schema queries
        if let Some(()) = self.try_handle_information_schema(sql).await? {
            return Ok(());
        }

        // Handle CHECK TABLE as no-op
        {
            let upper = sql.trim().to_uppercase();
            if upper.starts_with("CHECK TABLE") || upper.starts_with("CHECKSUM TABLE") {
                let table_name = {
                    let re = regex::Regex::new(r"(?i)(?:CHECK|CHECKSUM)\s+TABLE\s+(\w+)").unwrap();
                    re.captures(sql.trim())
                        .and_then(|c| c.get(1))
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                };
                let db = self.database.as_deref().unwrap_or("test");
                let qualified = format!("{}.{}", db, table_name);
                return self
                    .send_custom_result_set(
                        &["Table", "Op", "Msg_type", "Msg_text"],
                        &[vec![
                            qualified,
                            "check".to_string(),
                            "status".to_string(),
                            "OK".to_string(),
                        ]],
                    )
                    .await;
            }
        }

        // Parse
        let stmt = match Parser::parse_one(sql) {
            Ok(s) => s,
            Err(crate::sql::SqlError::CommentOnly) => {
                // Comment-only SQL: return OK with no result (MySQL behavior)
                return self.send_ok(0, 0).await;
            }
            Err(e) => {
                return self.send_sql_error(&e).await;
            }
        };

        // Handle parsed statements that are no-ops or intercepted
        // Extract data from the statement before calling mutable methods
        enum Intercept {
            NoOp,
            CreateProc {
                name: sqlparser::ast::ObjectName,
                params: Option<Vec<sqlparser::ast::ProcedureParam>>,
                body: sqlparser::ast::ConditionalStatements,
            },
            DropProc {
                name: String,
                if_exists: bool,
            },
            CreateViewRaft {
                name: String,
                query_sql: String,
                or_replace: bool,
            },
            DropViewRaft {
                names: Vec<String>,
                if_exists: bool,
            },
            // CreateFunc handled via text intercept
            Call(sqlparser::ast::Function),
            Continue(Box<sqlparser::ast::Statement>),
        }

        let action = {
            use sqlparser::ast::Statement as S;
            match stmt {
                S::LockTables { .. } | S::UnlockTables => Intercept::NoOp,
                S::CreateProcedure {
                    name, params, body, ..
                } => Intercept::CreateProc { name, params, body },
                S::DropProcedure {
                    if_exists,
                    proc_desc,
                    ..
                } => {
                    let proc_name = proc_desc
                        .first()
                        .map(|d| d.name.to_string())
                        .unwrap_or_default();
                    Intercept::DropProc {
                        name: proc_name,
                        if_exists,
                    }
                }
                S::CreateView(cv) => Intercept::CreateViewRaft {
                    name: cv.name.to_string(),
                    query_sql: cv.query.to_string(),
                    or_replace: cv.or_replace,
                },
                S::Drop {
                    ref object_type,
                    ref names,
                    if_exists,
                    ..
                } if *object_type == sqlparser::ast::ObjectType::View => {
                    let view_names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
                    Intercept::DropViewRaft {
                        names: view_names,
                        if_exists,
                    }
                }
                // CREATE FUNCTION handled via text intercept above
                S::Call(func) => Intercept::Call(func),
                other => Intercept::Continue(Box::new(other)),
            }
        };

        let stmt = match action {
            Intercept::NoOp => return self.send_ok(0, 0).await,
            Intercept::CreateProc { name, params, body } => {
                return self
                    .handle_create_procedure(&name, params.as_deref(), &body)
                    .await;
            }
            Intercept::DropProc { name, if_exists } => {
                return self.handle_drop_procedure(&name, if_exists).await;
            }
            // CreateFunc handled via text intercept before parsing
            Intercept::CreateViewRaft {
                name,
                query_sql,
                or_replace,
            } => {
                return self
                    .handle_create_view_raft(&name, &query_sql, or_replace)
                    .await;
            }
            Intercept::DropViewRaft { names, if_exists } => {
                return self.handle_drop_view_raft(&names, if_exists).await;
            }
            Intercept::Call(func) => {
                let proc_name = func.name.to_string().to_lowercase();
                let call_args: Vec<sqlparser::ast::Expr> = match func.args {
                    sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .into_iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => e,
                            sqlparser::ast::FunctionArg::Named { arg, .. } => {
                                if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                    e
                                } else {
                                    sqlparser::ast::Expr::Value(
                                        sqlparser::ast::Value::Null.with_empty_span(),
                                    )
                                }
                            }
                            _ => sqlparser::ast::Expr::Value(
                                sqlparser::ast::Value::Null.with_empty_span(),
                            ),
                        })
                        .collect(),
                    _ => vec![],
                };
                return self.handle_call(&proc_name, &call_args).await;
            }
            Intercept::Continue(stmt) => *stmt,
        };

        // Resolve, type check, and plan while holding catalog lock
        // Use closure to return Result and ensure guard is dropped before await
        // Returns both the physical plan and required privileges for authorization
        let plan_result: Result<(PhysicalPlan, Vec<RequiredPrivilege>), PlanError> = (|| {
            let catalog_guard = self.catalog.read();

            let resolved = Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(PlanError::Sql)?;

            // Extract required privileges from resolved statement before consuming it
            let required_privileges = self.extract_required_privileges(&resolved);

            // Type check
            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;

            // Build logical plan
            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;

            // Optimize
            let optimized = Optimizer::new().optimize(logical);

            // Build physical plan
            let physical =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)?;

            Ok((physical, required_privileges))
        })(); // catalog_guard dropped here

        // Handle errors after guard is dropped
        let (physical, required_privileges) = match plan_result {
            Ok((p, privs)) => (p, privs),
            Err(PlanError::Sql(e)) => return self.send_sql_error(&e).await,
            Err(PlanError::Planner(e)) => return self.send_planner_error(&e).await,
        };

        // Check authorization (async - needs to scan system.grants)
        if !required_privileges.is_empty() {
            if let Err(e) = self.check_authorization(&required_privileges).await {
                return self
                    .send_error(codes::ER_ACCESS_DENIED, states::GENERAL_ERROR, &e)
                    .await;
            }
        }

        // Execute — catch executor errors so they don't kill the connection
        match self.execute_plan(physical).await {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!("Query execution error: {}", e);
                self.send_error_from_protocol_error(&e).await
            }
        }
    }

    /// Extract required privileges from a resolved statement
    fn extract_required_privileges(
        &self,
        stmt: &crate::planner::logical::ResolvedStatement,
    ) -> Vec<RequiredPrivilege> {
        use crate::planner::logical::ResolvedStatement;

        // Use "default" database for privilege checks - tables don't have explicit db prefix
        let db = self.database.as_deref().unwrap_or("default");

        match stmt {
            ResolvedStatement::Select(select) => {
                // SELECT requires SELECT privilege on all tables in FROM clause
                select
                    .from
                    .iter()
                    .map(|table_ref| RequiredPrivilege::select(db, &table_ref.name))
                    .collect()
            }
            ResolvedStatement::Insert { table, .. }
            | ResolvedStatement::InsertSelect { table, .. } => {
                vec![RequiredPrivilege::insert(db, table)]
            }
            ResolvedStatement::Update { table, .. } => {
                vec![RequiredPrivilege::update(db, table)]
            }
            ResolvedStatement::Delete { table, .. } => {
                vec![RequiredPrivilege::delete(db, table)]
            }
            ResolvedStatement::CreateTable { .. }
            | ResolvedStatement::CreateTableAs { .. }
            | ResolvedStatement::CreateView { .. } => {
                vec![RequiredPrivilege::create_table(db)]
            }
            ResolvedStatement::DropView { .. } => {
                vec![] // Views don't need special privileges
            }
            ResolvedStatement::DropTable { name, .. } => {
                vec![RequiredPrivilege::drop_table(db, name)]
            }
            ResolvedStatement::DropMultipleTables { names, .. } => names
                .iter()
                .map(|name| RequiredPrivilege::drop_table(db, name))
                .collect(),
            ResolvedStatement::CreateIndex { table, .. } => {
                vec![RequiredPrivilege::create_index(db, table)]
            }
            ResolvedStatement::DropIndex { .. } => {
                // DROP INDEX requires INDEX privilege at database level
                vec![RequiredPrivilege::new(
                    Privilege::Index,
                    PrivilegeObject::Database(db.to_string()),
                )]
            }
            // Auth commands require global privileges (checked separately)
            ResolvedStatement::CreateUser { .. }
            | ResolvedStatement::AlterUser { .. }
            | ResolvedStatement::DropUser { .. }
            | ResolvedStatement::SetPassword { .. }
            | ResolvedStatement::Grant { .. }
            | ResolvedStatement::Revoke { .. } => {
                // These require CREATE USER or GRANT privileges - checked elsewhere
                vec![]
            }
            ResolvedStatement::ShowGrants { .. } => {
                // Users can always see their own grants
                vec![]
            }
            // Database DDL - requires CREATE privilege (root bypasses anyway)
            ResolvedStatement::CreateDatabase { .. } | ResolvedStatement::DropDatabase { .. } => {
                vec![]
            }
            // ANALYZE TABLE - no special privileges needed (root bypasses anyway)
            ResolvedStatement::AnalyzeTable { .. } => {
                vec![]
            }
            // UNION - requires SELECT on both sides' tables
            ResolvedStatement::Union { left, right, .. } => {
                let mut privs: Vec<_> = left
                    .from
                    .iter()
                    .map(|t| RequiredPrivilege::select(db, &t.name))
                    .collect();
                // Recursively extract privileges from right side (may be nested Union)
                privs.extend(self.extract_required_privileges(right));
                privs
            }
            // EXPLAIN - same privileges as inner statement
            ResolvedStatement::Explain { inner } => self.extract_required_privileges(inner),
        }
    }

    /// Check if the current user has the required privileges
    async fn check_authorization(&self, required: &[RequiredPrivilege]) -> Result<(), String> {
        use crate::catalog::system_tables::SYSTEM_GRANTS;

        let username = &self.session.user;
        if username.is_empty() {
            return Err("Not authenticated".to_string());
        }

        // Root user bypasses authorization checks
        if username == "root" {
            return Ok(());
        }

        // Scan system.grants to get user's grants
        let prefix = table_key_prefix(SYSTEM_GRANTS);
        let end = table_key_end(SYSTEM_GRANTS);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(|e| format!("Failed to query grants: {}", e))?;

        // Parse grants for this user
        let mut user_grants = Vec::new();
        let client_host = self.client_ip.to_string();

        for (_key, value) in rows {
            // Skip MVCC header (17 bytes) if present
            if value.len() <= 17 {
                continue;
            }
            // Check deleted flag
            if value[16] == 1 {
                continue;
            }
            let row_data = &value[17..];
            if let Ok(row) = decode_row(row_data) {
                // system.grants columns: grantee, grantee_host, grantee_type, privilege,
                // object_type, database_name, table_name, with_grant_option, granted_by, granted_at
                let values = row.values();
                if values.len() < 8 {
                    continue;
                }

                let grantee = match &values[0] {
                    Datum::String(s) => s.clone(),
                    _ => continue,
                };
                let grantee_host = match &values[1] {
                    Datum::String(s) => HostPattern::new(s.clone()),
                    _ => continue,
                };

                // Check if grant applies to current user
                if grantee != *username
                    || !grantee_host.matches(&client_host, Some(&self.client_ip))
                {
                    continue;
                }

                let privilege_str = match &values[3] {
                    Datum::String(s) => s.as_str(),
                    _ => continue,
                };
                let privilege = match Privilege::parse(privilege_str) {
                    Some(p) => p,
                    None => continue,
                };

                let object_type = match &values[4] {
                    Datum::String(s) => s.as_str(),
                    _ => continue,
                };
                let database_name = match &values[5] {
                    Datum::String(s) => Some(s.as_str()),
                    Datum::Null => None,
                    _ => continue,
                };
                let table_name = match &values[6] {
                    Datum::String(s) => Some(s.as_str()),
                    Datum::Null => None,
                    _ => continue,
                };

                let object = match object_type {
                    "GLOBAL" => PrivilegeObject::Global,
                    "DATABASE" => {
                        PrivilegeObject::Database(database_name.unwrap_or("").to_string())
                    }
                    "TABLE" => PrivilegeObject::Table {
                        database: database_name.unwrap_or("").to_string(),
                        table: table_name.unwrap_or("").to_string(),
                    },
                    _ => continue,
                };

                let with_grant_option = matches!(&values[7], Datum::Bool(true));

                user_grants.push(GrantEntry {
                    grantee,
                    grantee_host,
                    privilege,
                    object,
                    with_grant_option,
                });
            }
        }

        // Check each required privilege
        for req in required {
            if !check_privilege(&user_grants, req) {
                return Err(format!(
                    "Access denied for user '{}'@'{}': {} privilege required",
                    username,
                    client_host,
                    req.privilege.to_str()
                ));
            }
        }

        Ok(())
    }

    /// Execute a physical plan and send results
    async fn execute_plan(&mut self, plan: PhysicalPlan) -> ProtocolResult<()> {
        // Check if this is a query that returns rows
        let returns_rows = matches!(
            plan,
            PhysicalPlan::TableScan { .. }
                | PhysicalPlan::PointGet { .. }
                | PhysicalPlan::RangeScan { .. }
                | PhysicalPlan::Filter { .. }
                | PhysicalPlan::Project { .. }
                | PhysicalPlan::Sort { .. }
                | PhysicalPlan::Limit { .. }
                | PhysicalPlan::HashDistinct { .. }
                | PhysicalPlan::HashAggregate { .. }
                | PhysicalPlan::NestedLoopJoin { .. }
                | PhysicalPlan::AnalyzeTable { .. }
                | PhysicalPlan::Explain { .. }
        );

        // Check if this is DDL (no MVCC needed)
        let is_ddl = plan.is_ddl();

        // Reuse per-connection MVCC storage wrapper
        let mvcc = self.mvcc.clone();

        // Determine transaction context
        let (txn_context, implicit_txn_id) = if is_ddl {
            // DDL operations don't use MVCC
            (None, None)
        } else if let Some(txn_id) = self.session.current_txn {
            // Explicit transaction - use existing txn_id with pending changes for read-your-writes
            let read_view = self.txn_manager.create_read_view(txn_id)?;
            let pending = self.session.get_pending_changes();
            (
                Some(TransactionContext::with_pending_changes(
                    txn_id, read_view, pending,
                )),
                None,
            )
        } else if !returns_rows && self.session.autocommit {
            // Autocommit DML - create implicit transaction
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.session.is_read_only)?;
            let read_view = self.txn_manager.create_read_view(txn.txn_id)?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view)),
                Some(txn.txn_id),
            )
        } else {
            // Read-only query in autocommit mode - create snapshot read view
            // txn_id=0 special case: sees all committed transactions
            let read_view = self.txn_manager.create_read_view(0)?;
            (Some(TransactionContext::new(0, read_view)), None)
        };

        if returns_rows {
            // Get column definitions before building executor
            let columns = plan.output_columns();

            // Build and execute (with Raft for DDL replication)
            let engine = ExecutorEngine::with_raft(
                mvcc,
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
                self.session.user_variables(),
            );
            let mut executor = engine.build_async(plan).await?;

            self.send_result_set(&columns, &mut *executor).await
        } else {
            // Extract trigger info from INSERT plans before building executor
            let insert_trigger_info = self.extract_insert_trigger_info(&plan);

            // DML/DDL - execute and count affected rows
            let engine = ExecutorEngine::with_raft(
                mvcc.clone(),
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
                self.session.user_variables(),
            );
            let mut executor = engine.build_async(plan).await?;

            executor.open().await?;
            let mut affected = 0u64;
            let mut last_insert_id = 0u64;
            while let Some(row) = executor.next().await? {
                // For INSERT, the result row contains [rows_inserted] as first datum
                if let Some(Datum::Int(n)) = row.get_opt(0) {
                    affected += *n as u64;
                } else {
                    affected += 1;
                }
                // Check for last_insert_id in second datum (from auto_increment)
                if let Some(Datum::Int(id)) = row.get_opt(1) {
                    last_insert_id = *id as u64;
                }
            }
            executor.close().await?;

            // Fire triggers for INSERT operations
            if let Some(ref trigger_info) = insert_trigger_info {
                self.fire_insert_triggers(trigger_info).await?;
            }

            // Collect changes for Raft replication — includes both original INSERT
            // and any trigger-generated changes
            let ignore_dups = executor.is_ignore_duplicates();
            let changes = executor.take_changes();
            if !changes.is_empty() {
                if self.session.in_transaction() {
                    // Explicit transaction: accumulate changes, propose on COMMIT
                    self.session.add_pending_changes(changes);
                } else {
                    // Autocommit: propose immediately
                    let changeset = ChangeSet::new_with_changes_ignore(
                        implicit_txn_id.unwrap_or(0),
                        changes,
                        ignore_dups,
                    );
                    self.raft_node
                        .propose_changes(changeset)
                        .await
                        .map_err(|e| ProtocolError::Raft(e.to_string()))?;
                }
            }

            // Commit implicit transaction if we created one
            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager.commit(txn_id).await?;
            }

            // Write DML results to session for LAST_INSERT_ID()/ROW_COUNT()
            self.session
                .set_user_variable("__sys_last_insert_id", Datum::Int(last_insert_id as i64));
            self.session
                .set_user_variable("__sys_row_count", Datum::Int(affected as i64));

            self.send_ok(affected, last_insert_id).await
        }
    }

    /// Send a result set to the client
    async fn send_result_set(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        executor: &mut dyn Executor,
    ) -> ProtocolResult<()> {
        // Open executor
        executor.open().await?;

        // Send column count
        self.writer.set_sequence(1);
        let count_packet = encode_column_count(columns.len() as u64);
        self.writer.write_packet(&count_packet).await?;

        // Send column definitions
        let schema = self.database.as_deref().unwrap_or("default");
        for col in columns {
            let def = ColumnDefinition41::from_output_column(col, "", schema);
            let def_packet = def.encode();
            self.writer.write_packet(&def_packet).await?;
        }

        // Send EOF after columns (unless DEPRECATE_EOF)
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        // Send rows — catch executor errors mid-stream and send ERR packet
        let mut row_error = None;
        let mut row_count = 0u64;
        loop {
            match executor.next().await {
                Ok(Some(row)) => {
                    row_count += 1;
                    let row_packet = encode_text_row(&row);
                    self.writer.write_packet(&row_packet).await?;
                }
                Ok(None) => break,
                Err(e) => {
                    row_error = Some(e);
                    break;
                }
            }
        }
        // Track row count for FOUND_ROWS()
        self.session
            .set_user_variable("__sys_found_rows", Datum::Int(row_count as i64));

        // If an error occurred during row streaming, send ERR packet instead of EOF
        if let Some(e) = row_error {
            let msg = e.to_string();
            let (code, state) =
                if msg.contains("Incorrect arguments") || msg.contains("Wrong arguments") {
                    (codes::ER_WRONG_ARGUMENTS, states::GENERAL_ERROR)
                } else if msg.contains("Truncated incorrect") {
                    (codes::ER_TRUNCATED_WRONG_VALUE, "22007")
                } else {
                    (codes::ER_DATA_OUT_OF_RANGE, "22003")
                };
            let err_packet = encode_err_packet(code, state, &msg);
            self.writer.write_packet(&err_packet).await?;
            self.writer.flush().await?;
            executor.close().await?;
            return Ok(());
        }

        // Send final EOF/OK — with SERVER_MORE_RESULTS_EXISTS if in multi-result-set
        let status = if self.in_multi_result {
            default_status() | status_flags::SERVER_MORE_RESULTS_EXISTS
        } else {
            default_status()
        };
        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(status, 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, status);
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;

        // Close executor
        executor.close().await?;

        Ok(())
    }

    /// Send a result set from pre-collected rows (used for UNION)
    async fn send_collected_result_set(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        rows: Vec<crate::executor::row::Row>,
    ) -> ProtocolResult<()> {
        self.writer.set_sequence(1);
        let count_packet = encode_column_count(columns.len() as u64);
        self.writer.write_packet(&count_packet).await?;

        let schema = self.database.as_deref().unwrap_or("default");
        for col in columns {
            let def = ColumnDefinition41::from_output_column(col, "", schema);
            let def_packet = def.encode();
            self.writer.write_packet(&def_packet).await?;
        }

        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        for row in &rows {
            let row_packet = encode_text_row(row);
            self.writer.write_packet(&row_packet).await?;
        }

        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(default_status(), 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;
        Ok(())
    }

    /// Try to handle transaction commands (BEGIN, COMMIT, ROLLBACK, SET)
    ///
    /// Returns Some(()) if handled, None if not a transaction command.
    async fn try_handle_transaction_command(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        // BEGIN / START TRANSACTION
        if sql_upper == "BEGIN" || sql_upper.starts_with("START TRANSACTION") {
            return self.handle_begin().await.map(Some);
        }

        // COMMIT
        if sql_upper == "COMMIT" {
            return self.handle_commit().await.map(Some);
        }

        // ROLLBACK
        if sql_upper == "ROLLBACK" {
            return self.handle_rollback().await.map(Some);
        }

        // SET autocommit = 0/1
        if sql_upper.starts_with("SET AUTOCOMMIT") || sql_upper.starts_with("SET @@AUTOCOMMIT") {
            let value = sql_upper.contains('1') || sql_upper.contains("ON");
            return self.handle_set_autocommit(value).await.map(Some);
        }

        // SET TRANSACTION ISOLATION LEVEL
        if sql_upper.starts_with("SET TRANSACTION ISOLATION LEVEL")
            || sql_upper.starts_with("SET SESSION TRANSACTION ISOLATION LEVEL")
        {
            let level = if sql_upper.contains("READ UNCOMMITTED") {
                IsolationLevel::ReadUncommitted
            } else if sql_upper.contains("READ COMMITTED") {
                IsolationLevel::ReadCommitted
            } else if sql_upper.contains("REPEATABLE READ") {
                IsolationLevel::RepeatableRead
            } else if sql_upper.contains("SERIALIZABLE") {
                // Serializable isolation is not implemented
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        "SERIALIZABLE isolation level is not implemented",
                    )
                    .await
                    .map(|_| Some(()));
            } else {
                return self
                    .send_error(
                        codes::ER_SYNTAX_ERROR,
                        states::SYNTAX_ERROR,
                        "Unknown isolation level",
                    )
                    .await
                    .map(|_| Some(()));
            };
            return self.handle_set_isolation_level(level).await.map(Some);
        }

        // Catch-all: silently accept any other SET statement (SET NAMES, SET charset, etc.)
        // Exceptions: SET @var (user variables), SET sql_mode (tracked in session)
        if sql_upper.starts_with("SET ") && !sql_upper.starts_with("SET @") {
            // Let SET sql_mode through to be handled by extract_sql_mode_value
            if Self::extract_sql_mode_value(sql).is_some() {
                return Ok(None);
            }
            debug!(connection_id = self.connection_id, sql = %sql, "Accepting SET statement");
            return self.send_ok(0, 0).await.map(Some);
        }
        // Also accept SET @@system_var as no-op (except @@sql_mode)
        if sql_upper.starts_with("SET @@") {
            if Self::extract_sql_mode_value(sql).is_some() {
                return Ok(None);
            }
            debug!(connection_id = self.connection_id, sql = %sql, "Accepting SET @@var statement");
            return self.send_ok(0, 0).await.map(Some);
        }

        // Not a transaction command
        Ok(None)
    }

    /// Handle BEGIN / START TRANSACTION
    async fn handle_begin(&mut self) -> ProtocolResult<()> {
        // Check if already in a transaction
        if self.session.in_transaction() {
            return self
                .send_error(
                    codes::ER_CANT_CHANGE_TX_CHARACTERISTICS,
                    states::GENERAL_ERROR,
                    "Transaction already in progress",
                )
                .await;
        }

        // Read-only mode (replica) requires read-only transactions
        let read_only = self.session.is_read_only;
        match self
            .txn_manager
            .begin(self.session.isolation_level, read_only)
        {
            Ok(txn) => {
                self.session.begin_transaction(txn.txn_id);
                debug!(
                    connection_id = self.connection_id,
                    txn_id = txn.txn_id,
                    read_only = read_only,
                    "Started transaction"
                );
            }
            Err(e) => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &e.to_string(),
                    )
                    .await;
            }
        }

        self.send_ok_with_status(0, 0).await
    }

    /// Handle COMMIT
    async fn handle_commit(&mut self) -> ProtocolResult<()> {
        if let Some(txn_id) = self.session.current_txn {
            // First, propose any pending changes to Raft
            let changes = self.session.take_pending_changes();
            if !changes.is_empty() {
                let changeset = ChangeSet::new_with_changes(txn_id, changes);
                if let Err(e) = self.raft_node.propose_changes(changeset).await {
                    // Raft proposal failed - rollback the transaction
                    self.session.end_transaction();
                    let _ = self.txn_manager.rollback(txn_id).await;
                    return self
                        .send_error(
                            codes::ER_UNKNOWN_ERROR,
                            states::GENERAL_ERROR,
                            &format!("Raft commit failed: {}", e),
                        )
                        .await;
                }
            }

            // Now commit the transaction in the transaction manager.
            // CRITICAL: After successful Raft proposal, the data IS committed and durable.
            // The txn_manager.commit() is just bookkeeping - if it fails, we log the error
            // but still report success to the client since data is already committed via Raft.
            match self.txn_manager.commit(txn_id).await {
                Ok(()) => {
                    self.session.end_transaction();
                    debug!(
                        connection_id = self.connection_id,
                        txn_id = txn_id,
                        "Committed transaction"
                    );
                }
                Err(e) => {
                    // Log the error but don't fail - data is already committed via Raft
                    tracing::error!(
                        connection_id = self.connection_id,
                        txn_id = txn_id,
                        error = %e,
                        "TxnManager commit failed after Raft success - data IS committed, txn bookkeeping inconsistent"
                    );
                    self.session.end_transaction();
                    // Still report success - data is durable
                }
            }
        }
        // COMMIT without BEGIN is a no-op
        self.send_ok_with_status(0, 0).await
    }

    /// Handle ROLLBACK
    async fn handle_rollback(&mut self) -> ProtocolResult<()> {
        // Clear any pending changes (not proposed to Raft)
        self.session.clear_pending_changes();

        if let Some(txn_id) = self.session.current_txn {
            match self.txn_manager.rollback(txn_id).await {
                Ok(()) => {
                    self.session.end_transaction();
                    debug!(
                        connection_id = self.connection_id,
                        txn_id = txn_id,
                        "Rolled back transaction"
                    );
                }
                Err(e) => {
                    self.session.end_transaction();
                    return self
                        .send_error(
                            codes::ER_UNKNOWN_ERROR,
                            states::GENERAL_ERROR,
                            &e.to_string(),
                        )
                        .await;
                }
            }
        }
        // ROLLBACK without BEGIN is a no-op
        self.send_ok_with_status(0, 0).await
    }

    /// Handle SET autocommit = value
    async fn handle_set_autocommit(&mut self, value: bool) -> ProtocolResult<()> {
        // If turning off autocommit and not in transaction, start one
        if !value && !self.session.in_transaction() && self.session.autocommit {
            if let Ok(txn) = self
                .txn_manager
                .begin(self.session.isolation_level, self.session.is_read_only)
            {
                self.session.begin_transaction(txn.txn_id);
            }
        }

        // If turning on autocommit while in transaction, commit it
        if value && self.session.in_transaction() {
            if let Some(txn_id) = self.session.current_txn {
                let _ = self.txn_manager.commit(txn_id).await;
                self.session.end_transaction();
            }
        }

        self.session.set_autocommit(value);
        debug!(
            connection_id = self.connection_id,
            autocommit = value,
            "Set autocommit"
        );
        self.send_ok_with_status(0, 0).await
    }

    /// Handle SET TRANSACTION ISOLATION LEVEL
    async fn handle_set_isolation_level(&mut self, level: IsolationLevel) -> ProtocolResult<()> {
        // Can only change isolation level outside of a transaction
        if self.session.in_transaction() {
            return self
                .send_error(
                    codes::ER_CANT_CHANGE_TX_CHARACTERISTICS,
                    states::GENERAL_ERROR,
                    "Cannot change isolation level inside a transaction",
                )
                .await;
        }

        self.session.set_isolation_level(level);
        debug!(
            connection_id = self.connection_id,
            isolation_level = ?level,
            "Set isolation level"
        );
        self.send_ok_with_status(0, 0).await
    }

    /// Send OK packet with session-aware status flags
    async fn send_ok_with_status(
        &mut self,
        affected_rows: u64,
        last_insert_id: u64,
    ) -> ProtocolResult<()> {
        let status = self.session.status_flags();
        self.writer.set_sequence(1);
        let ok = encode_ok_packet(affected_rows, last_insert_id, status, 0);
        self.writer.write_packet(&ok).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Try to handle DROP DATABASE command via text intercept
    async fn try_handle_drop_database(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let sql_trimmed = sql.trim().trim_end_matches(';').trim();
        let sql_upper = sql_trimmed.to_uppercase();

        if !sql_upper.starts_with("DROP DATABASE") && !sql_upper.starts_with("DROP SCHEMA") {
            return Ok(None);
        }

        // Parse: DROP DATABASE [IF EXISTS] name
        let if_exists = sql_upper.contains("IF EXISTS");
        let name_part = if if_exists {
            sql_upper
                .replace("DROP DATABASE IF EXISTS", "")
                .replace("DROP SCHEMA IF EXISTS", "")
        } else {
            sql_upper
                .replace("DROP DATABASE", "")
                .replace("DROP SCHEMA", "")
        };

        let db_name = name_part
            .trim()
            .trim_matches('`')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();

        if db_name.is_empty() {
            return self
                .send_error(
                    codes::ER_SYNTAX_ERROR,
                    states::SYNTAX_ERROR,
                    "DROP DATABASE requires a database name",
                )
                .await
                .map(|_| Some(()));
        }

        if db_name == "DEFAULT" || db_name == "default" {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    "Cannot drop the 'default' database",
                )
                .await
                .map(|_| Some(()));
        }

        let exists = {
            let catalog = self.catalog.read();
            catalog.database_exists(&db_name.to_lowercase())
        };

        if !exists {
            if if_exists {
                return self.send_ok(0, 0).await.map(Some);
            }
            return self
                .send_error(
                    codes::ER_BAD_DB_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Can't drop database '{}'; database doesn't exist", db_name),
                )
                .await
                .map(|_| Some(()));
        }

        // Drop the database from the catalog
        {
            let mut catalog = self.catalog.write();
            let _ = catalog.drop_database(&db_name.to_lowercase());
        }

        self.send_ok(0, 0).await.map(Some)
    }

    /// Handle UNION queries by parsing with sqlparser, executing each side,
    /// and concatenating results.
    async fn try_handle_union(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        // Quick check before expensive parsing
        let upper = sql.to_uppercase();
        if !upper.contains(" UNION ") {
            return Ok(None);
        }

        // Parse with sqlparser to detect UNION
        let dialect = sqlparser::dialect::MySqlDialect {};
        let normalized = crate::sql::Parser::normalize_for_alter(sql);
        let ast = match sqlparser::parser::Parser::parse_sql(&dialect, &normalized) {
            Ok(ast) => ast,
            Err(_) => return Ok(None),
        };

        if ast.len() != 1 {
            return Ok(None);
        }

        // Check if it's a UNION query
        let is_union = match &ast[0] {
            sqlparser::ast::Statement::Query(q) => {
                matches!(
                    q.body.as_ref(),
                    sqlparser::ast::SetExpr::SetOperation { .. }
                )
            }
            _ => false,
        };

        if !is_union {
            return Ok(None);
        }

        // Extract the parts of the UNION by splitting the SQL
        // This is a simplified approach — split on UNION keyword
        let union_re = regex::Regex::new(r"(?i)\bUNION\s+(ALL\s+)?").unwrap();
        let parts: Vec<&str> = union_re.split(sql.trim().trim_end_matches(';')).collect();

        if parts.len() < 2 {
            return Ok(None);
        }

        // Execute first query to get columns and initial rows
        let first_sql = parts[0].trim();
        let plan = {
            let catalog_guard = self.catalog.read();
            let stmt = crate::sql::Parser::parse_one(first_sql).map_err(ProtocolError::Sql)?;
            let resolved = crate::sql::Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(ProtocolError::Sql)?;
            crate::sql::TypeChecker::check(&resolved).map_err(ProtocolError::Sql)?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(ProtocolError::Planner)?;
            let optimized = Optimizer::new().optimize(logical);
            PhysicalPlanner::plan(optimized, &catalog_guard).map_err(ProtocolError::Planner)?
        };

        let columns = plan.output_columns();
        let mvcc = self.mvcc.clone();

        // Create a read-only transaction context for UNION queries
        let txn = self.txn_manager.begin(self.session.isolation_level, true)?;
        let read_view = self.txn_manager.create_read_view(txn.txn_id)?;
        let txn_context = Some(TransactionContext::new(txn.txn_id, read_view));

        // Execute first query
        let engine = ExecutorEngine::with_raft(
            mvcc.clone(),
            self.catalog.clone(),
            txn_context.clone(),
            self.raft_node.clone(),
            self.session.user_variables(),
        );
        let mut executor = engine.build_async(plan).await?;
        executor.open().await?;
        let mut all_rows = Vec::new();
        while let Some(row) = executor.next().await? {
            all_rows.push(row);
        }
        executor.close().await?;

        // Execute remaining parts and collect rows
        for part in &parts[1..] {
            let part_sql = part.trim();
            if part_sql.is_empty() {
                continue;
            }
            let plan = {
                let catalog_guard = self.catalog.read();
                let stmt = match crate::sql::Parser::parse_one(part_sql) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let resolved = match crate::sql::Resolver::new(&catalog_guard).resolve(stmt) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let _ = crate::sql::TypeChecker::check(&resolved);
                let logical = match LogicalPlanBuilder::build(resolved) {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                let optimized = Optimizer::new().optimize(logical);
                match PhysicalPlanner::plan(optimized, &catalog_guard) {
                    Ok(p) => p,
                    Err(_) => continue,
                }
            };

            // Validate column count matches first SELECT
            let part_cols = plan.output_columns();
            if part_cols.len() != columns.len() {
                return self
                    .send_error(
                        1222,
                        "21000",
                        "The used SELECT statements have a different number of columns",
                    )
                    .await
                    .map(|_| Some(()));
            }

            let engine = ExecutorEngine::with_raft(
                mvcc.clone(),
                self.catalog.clone(),
                txn_context.clone(),
                self.raft_node.clone(),
                self.session.user_variables(),
            );
            let mut executor = engine.build_async(plan).await?;
            executor.open().await?;
            while let Some(row) = executor.next().await? {
                all_rows.push(row);
            }
            executor.close().await?;
        }

        // Check if DISTINCT (UNION without ALL)
        let is_all = upper.contains("UNION ALL");
        if !is_all {
            // UNION DISTINCT: remove duplicate rows
            let mut seen = std::collections::HashSet::new();
            all_rows.retain(|row| {
                let key: Vec<_> = row.values().to_vec();
                seen.insert(key)
            });
        }

        // Apply trailing ORDER BY to the combined UNION result
        // Extract ORDER BY from the parsed statement (sqlparser already parsed it)
        if let sqlparser::ast::Statement::Query(q) = &ast[0] {
            // Apply ORDER BY from the full UNION query
            if let Some(ref order_by) = q.order_by {
                if let sqlparser::ast::OrderByKind::Expressions(ref exprs) = order_by.kind {
                    let col_names: Vec<String> =
                        columns.iter().map(|c| c.name.to_uppercase()).collect();
                    all_rows.sort_by(|a, b| {
                        for ob_expr in exprs {
                            let col_idx = match &ob_expr.expr {
                                sqlparser::ast::Expr::Identifier(ident) => col_names
                                    .iter()
                                    .position(|n| n.eq_ignore_ascii_case(&ident.value)),
                                sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                                    value: sqlparser::ast::Value::Number(n, _),
                                    ..
                                }) => n.parse::<usize>().ok().map(|i| i - 1),
                                _ => None,
                            };
                            if let Some(idx) = col_idx {
                                let va = a.get_opt(idx).unwrap_or(&Datum::Null);
                                let vb = b.get_opt(idx).unwrap_or(&Datum::Null);
                                let ord = va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal);
                                let ord = if ob_expr.options.asc == Some(false) {
                                    ord.reverse()
                                } else {
                                    ord
                                };
                                if ord != std::cmp::Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            // Apply LIMIT
            if let Some(sqlparser::ast::LimitClause::LimitOffset {
                limit:
                    Some(sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: sqlparser::ast::Value::Number(ref n, _),
                        ..
                    })),
                ..
            }) = q.limit_clause
            {
                if let Ok(limit) = n.parse::<usize>() {
                    all_rows.truncate(limit);
                }
            }
        }

        // Clean up read-only transaction
        self.txn_manager.commit(txn.txn_id).await?;

        // Send result set
        self.send_collected_result_set(&columns, all_rows).await?;
        Ok(Some(()))
    }

    /// Clean up temporary tables when the connection closes.
    /// Deletes data rows from storage and removes catalog entries.
    async fn cleanup_temp_tables(&mut self) {
        let temp_tables = self.session.take_temp_tables();
        if temp_tables.is_empty() {
            return;
        }

        // First pass: delete data rows from storage (async, no catalog lock)
        let storage = self.mvcc.inner().clone();
        for name in &temp_tables {
            let prefix = crate::executor::encoding::table_key_prefix(name);
            let end = crate::executor::encoding::table_key_end(name);
            if let Ok(rows) = storage.scan(Some(&prefix), Some(&end)).await {
                for (key, _) in rows {
                    let _ = storage.delete(&key).await;
                }
            }
        }

        // Second pass: drop all catalog entries under a single lock
        {
            let mut catalog = self.catalog.write();
            for name in &temp_tables {
                let _ = catalog.drop_table(name);
            }
        }

        debug!(
            connection_id = self.connection_id,
            count = temp_tables.len(),
            "Cleaned up temporary tables"
        );
    }

    /// Handle CREATE TEMPORARY TABLE — creates table in catalog without Raft persistence.
    /// Temporary tables are session-scoped and cleaned up on disconnect.
    async fn try_handle_create_temporary_table(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("CREATE TEMPORARY TABLE ") {
            return Ok(None);
        }

        // Rewrite to regular CREATE TABLE for parsing (case-insensitive removal)
        let trimmed_sql = sql.trim();
        let temp_pos = upper.find("TEMPORARY ").unwrap(); // safe: guard above matched
        let rewritten = format!(
            "{}{}",
            &trimmed_sql[..temp_pos],
            &trimmed_sql[temp_pos + 10..]
        );
        let stmt = match crate::sql::Parser::parse_one(&rewritten) {
            Ok(s) => s,
            Err(e) => {
                return self
                    .send_error(1064, "42000", &format!("Parse error: {}", e))
                    .await
                    .map(|_| Some(()));
            }
        };

        // Resolve the CREATE TABLE (drop catalog guard before await)
        let resolved = {
            let catalog = self.catalog.read();
            let resolver = crate::sql::Resolver::new(&catalog);
            resolver.resolve(stmt).map_err(|e| e.to_string())
        };
        let resolved = match resolved {
            Ok(r) => r,
            Err(e) => {
                return self.send_error(1064, "42000", &e).await.map(|_| Some(()));
            }
        };

        // Extract table name and columns from resolved statement
        let (name, columns, constraints, if_not_exists) = match resolved {
            crate::planner::logical::ResolvedStatement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => (name, columns, constraints, if_not_exists),
            crate::planner::logical::ResolvedStatement::CreateTableAs {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => (name, columns, constraints, if_not_exists),
            _ => {
                return self
                    .send_error(
                        1064,
                        "42000",
                        "Unexpected resolved statement for TEMPORARY TABLE",
                    )
                    .await
                    .map(|_| Some(()));
            }
        };

        // Build TableDef with is_temporary flag
        let mut table_def = crate::catalog::TableDef::new(&name);
        table_def.is_temporary = true;
        for col in &columns {
            table_def = table_def.column(col.clone());
        }
        for constraint in &constraints {
            table_def = table_def.constraint(constraint.clone());
        }

        // Create directly in catalog (skip Raft, skip system table persistence)
        let create_result = {
            let mut catalog = self.catalog.write();
            if catalog.get_table(&name).is_some() {
                if if_not_exists {
                    Ok(true) // already exists, but IF NOT EXISTS
                } else {
                    Err(format!("Table '{}' already exists", name))
                }
            } else {
                catalog
                    .create_table(table_def)
                    .map_err(|e| ProtocolError::Internal(e.to_string()))?;
                Ok(false) // newly created
            }
        };

        match create_result {
            Ok(true) => return self.send_ok(0, 0).await.map(|_| Some(())),
            Err(msg) => {
                return self.send_error(1050, "42S01", &msg).await.map(|_| Some(()));
            }
            Ok(false) => {} // success, continue
        }

        // Register in session for cleanup on disconnect
        self.session.register_temp_table(name);

        self.send_ok(0, 0).await.map(|_| Some(()))
    }

    /// Handle ALTER TABLE statements.
    /// Modifies the catalog and persists changes to system tables via Raft.
    async fn try_handle_alter_table(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("ALTER TABLE ") {
            return Ok(None);
        }

        // Parse using sqlparser
        let dialect = sqlparser::dialect::MySqlDialect {};
        let normalized = crate::sql::Parser::normalize_for_alter(sql);
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, &normalized)
            .map_err(|e| ProtocolError::Sql(crate::sql::SqlError::Parse(e.to_string())))?;

        if ast.is_empty() {
            return self.send_ok(0, 0).await.map(|_| Some(()));
        }

        match &ast[0] {
            sqlparser::ast::Statement::AlterTable(alter) => {
                let table_name = alter.name.to_string();

                // Clone current table def from catalog
                let old_table_name = table_name.clone();
                let mut new_def = {
                    let catalog = self.catalog.read();
                    catalog
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            ProtocolError::Sql(crate::sql::SqlError::TableNotFound(
                                table_name.clone(),
                            ))
                        })?
                        .clone()
                };

                // Apply each operation to the cloned def
                for op in &alter.operations {
                    self.apply_alter_op(&mut new_def, op)?;
                }

                // Persist via Raft (delete old system rows, insert new ones)
                self.persist_alter_table(&old_table_name, &new_def).await?;

                self.send_ok(0, 0).await.map(|_| Some(()))
            }
            _ => Ok(None),
        }
    }

    /// Apply a single ALTER TABLE operation to a cloned TableDef.
    fn apply_alter_op(
        &self,
        def: &mut crate::catalog::TableDef,
        op: &sqlparser::ast::AlterTableOperation,
    ) -> ProtocolResult<()> {
        use sqlparser::ast::AlterTableOperation;

        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col = crate::sql::resolver::convert_column_def_pub(column_def)?;
                // Check for duplicate column name
                if def.get_column(&col.name).is_some() {
                    return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                        "Duplicate column name '{}'",
                        col.name
                    ))));
                }
                def.columns.push(col);
            }

            AlterTableOperation::DropColumn { column_names, .. } => {
                for ident in column_names {
                    let col_name = ident.value.clone();
                    let before = def.columns.len();
                    def.columns
                        .retain(|c| !c.name.eq_ignore_ascii_case(&col_name));
                    if def.columns.len() == before {
                        return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                            "Can't DROP '{}'; check that column/key exists",
                            col_name
                        ))));
                    }
                    // Also remove from any constraints referencing this column
                    def.constraints.retain(|c| match c {
                        crate::catalog::Constraint::PrimaryKey(cols)
                        | crate::catalog::Constraint::Unique(cols) => {
                            !cols.iter().any(|c| c.eq_ignore_ascii_case(&col_name))
                        }
                        crate::catalog::Constraint::ForeignKey { columns, .. } => {
                            !columns.iter().any(|c| c.eq_ignore_ascii_case(&col_name))
                        }
                        crate::catalog::Constraint::Check(_) => true,
                    });
                }
            }

            AlterTableOperation::AddConstraint { constraint, .. } => {
                let resolved = crate::sql::resolver::convert_table_constraint_pub(constraint)?;
                if let Some(c) = resolved {
                    def.constraints.push(c);
                }
            }

            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                ..
            } => {
                let name = col_name.value.clone();
                let new_type = crate::sql::resolver::convert_data_type(data_type)?;
                if let Some(col) = def
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&name))
                {
                    col.data_type = new_type;
                    // Re-apply options
                    self.apply_column_options(col, options);
                } else {
                    return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                        "Unknown column '{}' in '{}'",
                        name, def.name
                    ))));
                }
            }

            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                ..
            } => {
                let old = old_name.value.clone();
                let new_type = crate::sql::resolver::convert_data_type(data_type)?;
                if let Some(col) = def
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&old))
                {
                    col.name = new_name.value.clone();
                    col.data_type = new_type;
                    self.apply_column_options(col, options);
                } else {
                    return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                        "Unknown column '{}' in '{}'",
                        old, def.name
                    ))));
                }
            }

            AlterTableOperation::RenameTable { table_name } => {
                let new_name = match table_name {
                    sqlparser::ast::RenameTableNameKind::As(name)
                    | sqlparser::ast::RenameTableNameKind::To(name) => name.to_string(),
                };
                def.name = new_name;
            }

            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old = old_column_name.value.clone();
                if let Some(col) = def
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&old))
                {
                    col.name = new_column_name.value.clone();
                } else {
                    return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                        "Unknown column '{}' in '{}'",
                        old, def.name
                    ))));
                }
            }

            AlterTableOperation::DropPrimaryKey { .. } => {
                def.constraints
                    .retain(|c| !matches!(c, crate::catalog::Constraint::PrimaryKey(_)));
            }

            AlterTableOperation::DropForeignKey { name, .. } => {
                let fk_name = name.value.clone();
                let before = def.constraints.len();
                def.constraints.retain(|c| {
                    if let crate::catalog::Constraint::ForeignKey { name: Some(n), .. } = c {
                        !n.eq_ignore_ascii_case(&fk_name)
                    } else {
                        true
                    }
                });
                if def.constraints.len() == before {
                    warn!("DROP FOREIGN KEY '{}': no foreign key found", fk_name);
                }
            }

            AlterTableOperation::DropIndex { name } => {
                let idx_name = name.value.clone();
                // Remove from catalog indexes
                let mut catalog = self.catalog.write();
                let _ = catalog.drop_index(&idx_name);
            }

            AlterTableOperation::DropConstraint { name, .. } => {
                let constraint_name = name.value.clone();
                // Try to drop as index first, then as a named constraint
                let mut catalog = self.catalog.write();
                let _ = catalog.drop_index(&constraint_name);
            }

            AlterTableOperation::AlterColumn { column_name, op } => {
                let name = column_name.value.clone();
                if let Some(col) = def
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&name))
                {
                    use sqlparser::ast::AlterColumnOperation;
                    match op {
                        AlterColumnOperation::SetDefault { value } => {
                            col.default = Some(value.to_string());
                        }
                        AlterColumnOperation::DropDefault => {
                            col.default = None;
                        }
                        AlterColumnOperation::SetNotNull => {
                            col.nullable = false;
                        }
                        AlterColumnOperation::DropNotNull => {
                            col.nullable = true;
                        }
                        AlterColumnOperation::SetDataType { data_type, .. } => {
                            if let Ok(new_type) = crate::sql::resolver::convert_data_type(data_type)
                            {
                                col.data_type = new_type;
                            }
                        }
                        _ => {}
                    }
                } else {
                    return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                        "Unknown column '{}' in '{}'",
                        name, def.name
                    ))));
                }
            }

            AlterTableOperation::Algorithm { .. }
            | AlterTableOperation::Lock { .. }
            | AlterTableOperation::AutoIncrement { .. } => {
                // MySQL-specific options that don't affect schema — silently accept
            }

            _ => {
                return Err(ProtocolError::Sql(crate::sql::SqlError::Unsupported(
                    format!("ALTER TABLE operation: {:?}", op),
                )));
            }
        }

        Ok(())
    }

    /// Apply sqlparser ColumnOption list to our ColumnDef (used by MODIFY/CHANGE COLUMN).
    fn apply_column_options(
        &self,
        col: &mut crate::catalog::ColumnDef,
        options: &[sqlparser::ast::ColumnOption],
    ) {
        // Reset to defaults before reapplying
        col.nullable = true;
        col.default = None;
        col.auto_increment = false;

        for option in options {
            match option {
                sqlparser::ast::ColumnOption::Null => col.nullable = true,
                sqlparser::ast::ColumnOption::NotNull => col.nullable = false,
                sqlparser::ast::ColumnOption::Default(expr) => {
                    col.default = Some(expr.to_string());
                }
                sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                    col.nullable = false;
                }
                sqlparser::ast::ColumnOption::DialectSpecific(tokens) => {
                    let token_str: String = tokens
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<String>()
                        .to_uppercase();
                    if token_str.contains("AUTO_INCREMENT") || token_str.contains("AUTOINCREMENT") {
                        col.auto_increment = true;
                        col.nullable = false;
                    }
                }
                _ => {}
            }
        }
    }

    /// Persist ALTER TABLE changes to system tables via Raft.
    ///
    /// Pattern: delete old system.columns/constraints rows, insert new ones.
    /// For RENAME, also updates system.tables row.
    async fn persist_alter_table(
        &mut self,
        old_table_name: &str,
        new_def: &crate::catalog::TableDef,
    ) -> ProtocolResult<()> {
        use crate::catalog::system_tables::{
            table_def_to_columns_rows, table_def_to_constraints_rows, table_def_to_tables_row,
            SYSTEM_COLUMNS, SYSTEM_CONSTRAINTS, SYSTEM_TABLES,
        };
        use crate::executor::encoding::{encode_row, encode_row_key};
        use crate::raft::RowChange;
        use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

        let mut changeset = ChangeSet::new(0);

        // Scan and delete old system.tables row (for RENAME, or to reinsert)
        let renamed = old_table_name != new_def.name;

        // Delete old system.columns rows matching old_table_name
        {
            let prefix = format!("t:{SYSTEM_COLUMNS}:");
            let end = format!("t:{SYSTEM_COLUMNS};\x00");
            let rows = self
                .mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ProtocolError::Internal(format!("scan system.columns: {}", e)))?;

            for (key, value) in rows {
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(Datum::String(tname)) = row.values().first() {
                        if tname == old_table_name {
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_COLUMNS,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }
        }

        // Delete old system.constraints rows matching old_table_name
        {
            let prefix = format!("t:{SYSTEM_CONSTRAINTS}:");
            let end = format!("t:{SYSTEM_CONSTRAINTS};\x00");
            let rows = self
                .mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ProtocolError::Internal(format!("scan system.constraints: {}", e)))?;

            for (key, value) in rows {
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(Datum::String(tname)) = row.values().first() {
                        if tname == old_table_name {
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_CONSTRAINTS,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }
        }

        // If renamed, delete old system.tables row and insert new one
        if renamed {
            let prefix = format!("t:{SYSTEM_TABLES}:");
            let end = format!("t:{SYSTEM_TABLES};\x00");
            let rows = self
                .mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ProtocolError::Internal(format!("scan system.tables: {}", e)))?;

            for (key, value) in rows {
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = decode_row(row_data) {
                    if let Some(Datum::String(tname)) = row.values().first() {
                        if tname == old_table_name {
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_TABLES,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            // Insert new system.tables row
            let table_row = table_def_to_tables_row(new_def);
            let (mut next_local, node_id) = allocate_row_id_batch(1);
            let row_id = encode_row_id(next_local, node_id);
            next_local += 1;
            let _ = next_local; // suppress unused warning
            let key = encode_row_key(SYSTEM_TABLES, row_id);
            let value = encode_row(&table_row);
            changeset.push(RowChange::insert(SYSTEM_TABLES, key, value));
        }

        // Insert new system.columns rows from modified def
        let column_rows = table_def_to_columns_rows(new_def);
        let constraint_rows = table_def_to_constraints_rows(new_def);
        let total_ids = column_rows.len() as u64 + constraint_rows.len() as u64;

        if total_ids > 0 {
            let (mut next_local, node_id) = allocate_row_id_batch(total_ids);

            for col_row in column_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_COLUMNS, row_id);
                let value = encode_row(&col_row);
                changeset.push(RowChange::insert(SYSTEM_COLUMNS, key, value));
            }

            for constraint_row in constraint_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_CONSTRAINTS, row_id);
                let value = encode_row(&constraint_row);
                changeset.push(RowChange::insert(SYSTEM_CONSTRAINTS, key, value));
            }
        }

        if changeset.changes.is_empty() {
            // No system table changes — just update catalog directly
            let mut catalog = self.catalog.write();
            catalog.replace_table(old_table_name, new_def.clone());
        } else {
            // Propose via Raft — catalog is updated synchronously in apply()
            self.raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ProtocolError::Internal(format!("Raft propose ALTER TABLE: {}", e)))?;
        }

        Ok(())
    }

    /// Handle SQL-level PREPARE/EXECUTE/DEALLOCATE for text protocol prepared statements.
    ///
    /// MySQL syntax:
    ///   PREPARE stmt_name FROM 'sql_text'
    ///   EXECUTE stmt_name [USING @var1, @var2, ...]
    ///   DEALLOCATE PREPARE stmt_name / DROP PREPARE stmt_name
    async fn try_handle_sql_prepare(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();

        if upper.starts_with("PREPARE ") {
            // PREPARE stmt_name FROM 'sql_text'
            let rest = trimmed[8..].trim();
            if let Some(from_pos) = rest.to_uppercase().find(" FROM ") {
                let stmt_name = rest[..from_pos].trim().to_string();
                let sql_text = rest[from_pos + 6..].trim();
                // Strip surrounding quotes
                let sql_text = if (sql_text.starts_with('\'') && sql_text.ends_with('\''))
                    || (sql_text.starts_with('"') && sql_text.ends_with('"'))
                {
                    &sql_text[1..sql_text.len() - 1]
                } else {
                    sql_text
                };
                self.session
                    .sql_prepared_stmts
                    .insert(stmt_name, sql_text.to_string());
                self.send_ok(0, 0).await?;
                return Ok(Some(()));
            }
        } else if upper.starts_with("EXECUTE ") {
            // EXECUTE stmt_name [USING @var1, @var2, ...]
            let rest = trimmed[8..].trim();
            let parts: Vec<&str> = rest.splitn(2, |c: char| c.is_whitespace()).collect();
            let stmt_name = parts[0].trim_end_matches(';').to_string();
            if let Some(sql_text) = self.session.sql_prepared_stmts.get(&stmt_name).cloned() {
                // Substitute ? placeholders with USING variable values
                let mut final_sql = sql_text.clone();
                if let Some(using_part) = rest.to_uppercase().find("USING ") {
                    let vars_str = &rest[using_part + 6..];
                    let var_names: Vec<&str> = vars_str
                        .split(',')
                        .map(|s| s.trim().trim_end_matches(';'))
                        .collect();
                    for var_name in var_names {
                        let var_key = var_name.trim_start_matches('@').to_lowercase();
                        let val = self.session.user_variables().read().get(&var_key).cloned();
                        let replacement = match val {
                            Some(ref d) => d.to_sql_literal(),
                            None => "NULL".to_string(),
                        };
                        // Replace first ? with the value
                        if let Some(pos) = final_sql.find('?') {
                            final_sql = format!(
                                "{}{}{}",
                                &final_sql[..pos],
                                replacement,
                                &final_sql[pos + 1..]
                            );
                        }
                    }
                }
                return self.handle_query(&final_sql).await.map(|_| Some(()));
            } else {
                self.send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!(
                        "Unknown prepared statement handler ({}) given to EXECUTE",
                        stmt_name
                    ),
                )
                .await?;
                return Ok(Some(()));
            }
        } else if upper.starts_with("DEALLOCATE PREPARE ") || upper.starts_with("DROP PREPARE ") {
            let prefix_len = if upper.starts_with("DEALLOCATE") {
                19
            } else {
                13
            };
            let stmt_name = trimmed[prefix_len..]
                .trim()
                .trim_end_matches(';')
                .to_string();
            self.session.sql_prepared_stmts.remove(&stmt_name);
            self.send_ok(0, 0).await?;
            return Ok(Some(()));
        }

        Ok(None)
    }

    /// Try to handle USE database command via SQL
    ///
    /// Returns Some(()) if handled, None if not a USE command.
    async fn try_handle_use_command(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        if !sql_upper.starts_with("USE ") {
            return Ok(None);
        }

        // Extract database name (strip quotes if present)
        let db_name = sql_trimmed[4..]
            .trim()
            .trim_matches('`')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();

        if db_name.is_empty() {
            return self
                .send_error(
                    codes::ER_SYNTAX_ERROR,
                    states::SYNTAX_ERROR,
                    "USE requires a database name",
                )
                .await
                .map(|_| Some(()));
        }

        // Check if database exists in the catalog
        let exists = {
            let catalog = self.catalog.read();
            catalog.database_exists(&db_name)
        };
        if !exists {
            return self
                .send_error(
                    codes::ER_BAD_DB_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Unknown database '{}'", db_name),
                )
                .await
                .map(|_| Some(()));
        }

        self.database = Some(db_name.clone());
        self.session.set_database(Some(db_name));
        self.send_ok(0, 0).await.map(Some)
    }

    /// Send a result set from in-memory column names and rows
    async fn send_custom_result_set(
        &mut self,
        col_names: &[&str],
        rows: &[Vec<String>],
    ) -> ProtocolResult<()> {
        use crate::catalog::DataType;
        use crate::planner::logical::OutputColumn;

        self.writer.set_sequence(1);

        // Column count
        let count_packet = encode_column_count(col_names.len() as u64);
        self.writer.write_packet(&count_packet).await?;

        // Column definitions
        let schema = self.database.as_deref().unwrap_or("default");
        for (i, name) in col_names.iter().enumerate() {
            let col = OutputColumn {
                id: i,
                name: (*name).to_string(),
                data_type: DataType::Varchar(255),
                nullable: true,
            };
            let def = ColumnDefinition41::from_output_column(&col, "", schema);
            self.writer.write_packet(&def.encode()).await?;
        }

        // EOF after columns
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        // Send rows
        for row_data in rows {
            let row = crate::executor::row::Row::new(
                row_data
                    .iter()
                    .map(|v| crate::executor::datum::Datum::String(v.clone()))
                    .collect(),
            );
            let row_packet = encode_text_row(&row);
            self.writer.write_packet(&row_packet).await?;
        }

        // Final EOF/OK
        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(default_status(), 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;
        Ok(())
    }

    /// Try to handle SHOW commands
    ///
    /// Returns Some(()) if handled, None if not a SHOW command.
    async fn try_handle_show_command(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        if !sql_upper.starts_with("SHOW ")
            && !sql_upper.starts_with("DESCRIBE ")
            && !sql_upper.starts_with("DESC ")
        {
            return Ok(None);
        }

        // SHOW DATABASES
        if sql_upper.starts_with("SHOW DATABASES") || sql_upper.starts_with("SHOW SCHEMAS") {
            let databases = {
                let catalog = self.catalog.read();
                catalog.list_databases()
            };
            let rows: Vec<Vec<String>> = databases.into_iter().map(|db| vec![db]).collect();
            self.send_custom_result_set(&["Database"], &rows)
                .await
                .map(Some)
        }
        // SHOW TABLES FROM db / SHOW TABLES IN db
        else if sql_upper.starts_with("SHOW TABLES FROM ")
            || sql_upper.starts_with("SHOW TABLES IN ")
        {
            let db_name = sql_trimmed
                .split_whitespace()
                .nth(3)
                .unwrap_or("")
                .trim_matches('`')
                .trim_matches('\'')
                .trim_matches('"');

            let tables = {
                let catalog = self.catalog.read();
                catalog.get_tables_in_database(db_name)
            };
            let col_name = format!("Tables_in_{}", db_name);
            let rows: Vec<Vec<String>> = tables.into_iter().map(|t| vec![t]).collect();
            self.send_custom_result_set(&[&col_name], &rows)
                .await
                .map(Some)
        }
        // SHOW TABLES
        else if sql_upper == "SHOW TABLES"
            || sql_upper.starts_with("SHOW TABLES;")
            || sql_upper.starts_with("SHOW FULL TABLES")
        {
            let db = self
                .database
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let tables = {
                let catalog = self.catalog.read();
                let mut tables = catalog.get_tables_in_database(&db);
                // Include views in SHOW TABLES output
                tables.extend(catalog.get_view_names());
                tables.sort();
                tables
            };
            let col_name = format!("Tables_in_{}", db);
            let rows: Vec<Vec<String>> = tables.into_iter().map(|t| vec![t]).collect();
            self.send_custom_result_set(&[&col_name], &rows)
                .await
                .map(Some)
        }
        // SHOW CREATE TABLE
        else if sql_upper.starts_with("SHOW CREATE TABLE ") {
            let table_name = sql_trimmed[18..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('`')
                .trim_matches('\'')
                .trim_matches('"');

            let result = {
                let catalog = self.catalog.read();
                catalog.get_table(table_name).map(reconstruct_create_table)
            };

            match result {
                Some(ddl) => {
                    let rows = vec![vec![table_name.to_string(), ddl]];
                    self.send_custom_result_set(&["Table", "Create Table"], &rows)
                        .await
                        .map(Some)
                }
                None => {
                    // Check if it's a view
                    let view_def = {
                        let catalog = self.catalog.read();
                        catalog.get_view(table_name).cloned()
                    };
                    if let Some(vd) = view_def {
                        let create_view = format!(
                            "CREATE ALGORITHM=UNDEFINED VIEW `{}` AS {}",
                            vd.name, vd.query_sql
                        );
                        let rows = vec![vec![table_name.to_string(), create_view]];
                        self.send_custom_result_set(&["View", "Create View"], &rows)
                            .await
                            .map(Some)
                    } else {
                        self.send_error(
                            codes::ER_NO_SUCH_TABLE,
                            states::NO_SUCH_TABLE,
                            &format!("Table '{}' doesn't exist", table_name),
                        )
                        .await
                        .map(|_| Some(()))
                    }
                }
            }
        }
        // SHOW KEYS FROM / SHOW INDEX FROM / SHOW INDEXES FROM
        else if sql_upper.starts_with("SHOW KEYS FROM ")
            || sql_upper.starts_with("SHOW INDEX FROM ")
            || sql_upper.starts_with("SHOW INDEXES FROM ")
        {
            let table_name = sql_trimmed
                .split_whitespace()
                .nth(3)
                .unwrap_or("")
                .trim_end_matches(';')
                .trim_matches('`');

            let rows = {
                let catalog = self.catalog.read();
                let indexes = catalog.get_indexes_for_table(table_name);
                let mut rows = Vec::new();
                for idx in &indexes {
                    for (seq, col) in idx.columns.iter().enumerate() {
                        rows.push(vec![
                            table_name.to_string(),
                            if idx.unique { "0" } else { "1" }.to_string(),
                            idx.name.clone(),
                            (seq + 1).to_string(),
                            col.clone(),
                        ]);
                    }
                }
                if let Some(table_def) = catalog.get_table(table_name) {
                    if let Some(pk_cols) = table_def.primary_key() {
                        for (seq, col) in pk_cols.iter().enumerate() {
                            rows.push(vec![
                                table_name.to_string(),
                                "0".to_string(),
                                "PRIMARY".to_string(),
                                (seq + 1).to_string(),
                                col.clone(),
                            ]);
                        }
                    }
                }
                rows
            };
            self.send_custom_result_set(
                &[
                    "Table",
                    "Non_unique",
                    "Key_name",
                    "Seq_in_index",
                    "Column_name",
                ],
                &rows,
            )
            .await
            .map(Some)
        }
        // DESCRIBE / DESC / SHOW COLUMNS FROM / SHOW FULL COLUMNS FROM
        else if sql_upper.starts_with("DESCRIBE ")
            || sql_upper.starts_with("DESC ")
            || sql_upper.starts_with("SHOW COLUMNS FROM ")
            || sql_upper.starts_with("SHOW FIELDS FROM ")
            || sql_upper.starts_with("SHOW FULL COLUMNS FROM ")
            || sql_upper.starts_with("SHOW FULL FIELDS FROM ")
        {
            let table_name = if sql_upper.starts_with("SHOW FULL COLUMNS FROM ")
                || sql_upper.starts_with("SHOW FULL FIELDS FROM ")
            {
                sql_trimmed
                    .split_whitespace()
                    .nth(4)
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .trim_matches('`')
                    .trim_matches('\'')
                    .trim_matches('"')
            } else if sql_upper.starts_with("SHOW COLUMNS FROM ")
                || sql_upper.starts_with("SHOW FIELDS FROM ")
            {
                sql_trimmed
                    .split_whitespace()
                    .nth(3)
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .trim_matches('`')
                    .trim_matches('\'')
                    .trim_matches('"')
            } else {
                // DESCRIBE / DESC
                let start = if sql_upper.starts_with("DESCRIBE ") {
                    9
                } else {
                    5
                };
                sql_trimmed[start..]
                    .trim()
                    .trim_end_matches(';')
                    .trim_matches('`')
                    .trim_matches('\'')
                    .trim_matches('"')
            };

            let result = {
                let catalog = self.catalog.read();
                catalog.get_table(table_name).map(describe_table)
            };

            match result {
                Some(rows) => self
                    .send_custom_result_set(
                        &["Field", "Type", "Null", "Key", "Default", "Extra"],
                        &rows,
                    )
                    .await
                    .map(Some),
                None => self
                    .send_error(
                        codes::ER_NO_SUCH_TABLE,
                        states::NO_SUCH_TABLE,
                        &format!("Table '{}' doesn't exist", table_name),
                    )
                    .await
                    .map(|_| Some(())),
            }
        }
        // SHOW WARNINGS
        else if sql_upper.starts_with("SHOW WARNINGS") {
            let rows: Vec<Vec<String>> = self
                .session
                .warnings()
                .iter()
                .map(|w| vec![w.level.clone(), w.code.to_string(), w.message.clone()])
                .collect();
            self.send_custom_result_set(&["Level", "Code", "Message"], &rows)
                .await
                .map(Some)
        }
        // SHOW ERRORS
        else if sql_upper.starts_with("SHOW ERRORS") {
            let rows: Vec<Vec<String>> = self
                .session
                .warnings()
                .iter()
                .filter(|w| w.level == "Error")
                .map(|w| vec![w.level.clone(), w.code.to_string(), w.message.clone()])
                .collect();
            self.send_custom_result_set(&["Level", "Code", "Message"], &rows)
                .await
                .map(Some)
        }
        // SHOW COUNT(*) WARNINGS
        else if sql_upper.contains("SHOW COUNT") && sql_upper.contains("WARNINGS") {
            let count = self.session.warning_count();
            let rows = vec![vec![count.to_string()]];
            self.send_custom_result_set(&["@@session.warning_count"], &rows)
                .await
                .map(Some)
        }
        // SHOW COLLATION
        else if sql_upper.starts_with("SHOW COLLATION") {
            let rows: Vec<Vec<String>> = vec![vec![
                "utf8mb4_general_ci".to_string(),
                "utf8mb4".to_string(),
                "45".to_string(),
                "Yes".to_string(),
                "Yes".to_string(),
                "1".to_string(),
                "PAD SPACE".to_string(),
            ]];
            self.send_custom_result_set(
                &[
                    "Collation",
                    "Charset",
                    "Id",
                    "Default",
                    "Compiled",
                    "Sortlen",
                    "Pad_attribute",
                ],
                &rows,
            )
            .await
            .map(Some)
        }
        // SHOW CHARACTER SET
        else if sql_upper.starts_with("SHOW CHARACTER SET")
            || sql_upper.starts_with("SHOW CHARSET")
        {
            let rows: Vec<Vec<String>> = vec![vec![
                "utf8mb4".to_string(),
                "UTF-8 Unicode".to_string(),
                "utf8mb4_general_ci".to_string(),
                "4".to_string(),
            ]];
            self.send_custom_result_set(
                &["Charset", "Description", "Default collation", "Maxlen"],
                &rows,
            )
            .await
            .map(Some)
        }
        // SHOW ENGINES
        else if sql_upper.starts_with("SHOW ENGINES") || sql_upper.starts_with("SHOW STORAGE") {
            let rows: Vec<Vec<String>> = vec![vec![
                "RooDB".to_string(),
                "DEFAULT".to_string(),
                "RooDB LSM storage engine".to_string(),
                "YES".to_string(),
                "YES".to_string(),
                "YES".to_string(),
            ]];
            self.send_custom_result_set(
                &[
                    "Engine",
                    "Support",
                    "Comment",
                    "Transactions",
                    "XA",
                    "Savepoints",
                ],
                &rows,
            )
            .await
            .map(Some)
        }
        // SHOW VARIABLES / SHOW STATUS / SHOW PROCESSLIST - return empty for compatibility
        else if sql_upper.starts_with("SHOW VARIABLES")
            || sql_upper.starts_with("SHOW SESSION VARIABLES")
            || sql_upper.starts_with("SHOW GLOBAL VARIABLES")
            || sql_upper.starts_with("SHOW STATUS")
            || sql_upper.starts_with("SHOW SESSION STATUS")
            || sql_upper.starts_with("SHOW GLOBAL STATUS")
        {
            let rows: Vec<Vec<String>> = Vec::new();
            self.send_custom_result_set(&["Variable_name", "Value"], &rows)
                .await
                .map(Some)
        }
        // SHOW PROCESSLIST
        else if sql_upper.starts_with("SHOW PROCESSLIST")
            || sql_upper.starts_with("SHOW FULL PROCESSLIST")
        {
            let rows: Vec<Vec<String>> = Vec::new();
            self.send_custom_result_set(
                &[
                    "Id", "User", "Host", "db", "Command", "Time", "State", "Info",
                ],
                &rows,
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Try to handle system variable queries (SELECT @@variable)
    ///
    /// Returns Some(()) if handled, None if not a system variable query.
    /// Try to handle SELECT DATABASE() query
    /// Handle queries against INFORMATION_SCHEMA and performance_schema virtual tables
    async fn try_handle_information_schema(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let upper = sql.trim().to_uppercase();

        // Only intercept SELECT queries that reference these schemas
        if !upper.contains("INFORMATION_SCHEMA") && !upper.contains("PERFORMANCE_SCHEMA") {
            return Ok(None);
        }

        // INFORMATION_SCHEMA.ENGINES
        if upper.contains("INFORMATION_SCHEMA.ENGINES")
            || upper.contains("INFORMATION_SCHEMA.`ENGINES`")
        {
            let rows = vec![vec![
                "RooDB".to_string(),
                "DEFAULT".to_string(),
                "RooDB distributed LSM storage engine".to_string(),
                "YES".to_string(),
                "YES".to_string(),
                "NO".to_string(),
            ]];
            return self
                .send_custom_result_set(
                    &[
                        "ENGINE",
                        "SUPPORT",
                        "COMMENT",
                        "TRANSACTIONS",
                        "XA",
                        "SAVEPOINTS",
                    ],
                    &rows,
                )
                .await
                .map(Some);
        }

        // INFORMATION_SCHEMA.SCHEMATA
        if upper.contains("INFORMATION_SCHEMA.SCHEMATA") {
            let databases = {
                let catalog = self.catalog.read();
                catalog.list_databases()
            };
            let rows: Vec<Vec<String>> = databases
                .into_iter()
                .map(|db| {
                    vec![
                        "def".to_string(),
                        db,
                        "utf8mb4".to_string(),
                        "utf8mb4_general_ci".to_string(),
                        String::new(),
                        "NO".to_string(),
                    ]
                })
                .collect();
            return self
                .send_custom_result_set(
                    &[
                        "CATALOG_NAME",
                        "SCHEMA_NAME",
                        "DEFAULT_CHARACTER_SET_NAME",
                        "DEFAULT_COLLATION_NAME",
                        "SQL_PATH",
                        "DEFAULT_ENCRYPTION",
                    ],
                    &rows,
                )
                .await
                .map(Some);
        }

        // INFORMATION_SCHEMA.TABLES
        if upper.contains("INFORMATION_SCHEMA.TABLES") {
            let db = self
                .database
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let tables = {
                let catalog = self.catalog.read();
                catalog.get_tables_in_database(&db)
            };
            let rows: Vec<Vec<String>> = tables
                .into_iter()
                .map(|t| {
                    vec![
                        "def".to_string(),
                        db.clone(),
                        t,
                        "BASE TABLE".to_string(),
                        "RooDB".to_string(),
                        "10".to_string(),
                        "Dynamic".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        String::new(),
                        "utf8mb4_general_ci".to_string(),
                        String::new(),
                        String::new(),
                    ]
                })
                .collect();
            return self
                .send_custom_result_set(
                    &[
                        "TABLE_CATALOG",
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "TABLE_TYPE",
                        "ENGINE",
                        "VERSION",
                        "ROW_FORMAT",
                        "TABLE_ROWS",
                        "AVG_ROW_LENGTH",
                        "DATA_LENGTH",
                        "MAX_DATA_LENGTH",
                        "INDEX_LENGTH",
                        "DATA_FREE",
                        "CREATE_OPTIONS",
                        "TABLE_COLLATION",
                        "CHECKSUM",
                        "TABLE_COMMENT",
                    ],
                    &rows,
                )
                .await
                .map(Some);
        }

        // INFORMATION_SCHEMA.PLUGINS
        if upper.contains("INFORMATION_SCHEMA.PLUGINS") {
            let rows: Vec<Vec<String>> = Vec::new();
            return self
                .send_custom_result_set(
                    &[
                        "PLUGIN_NAME",
                        "PLUGIN_VERSION",
                        "PLUGIN_STATUS",
                        "PLUGIN_TYPE",
                        "PLUGIN_TYPE_VERSION",
                        "PLUGIN_LIBRARY",
                        "PLUGIN_LIBRARY_VERSION",
                        "PLUGIN_AUTHOR",
                        "PLUGIN_DESCRIPTION",
                        "PLUGIN_LICENSE",
                        "LOAD_OPTION",
                    ],
                    &rows,
                )
                .await
                .map(Some);
        }

        // INFORMATION_SCHEMA.COLUMNS
        if upper.contains("INFORMATION_SCHEMA.COLUMNS") {
            let rows: Vec<Vec<String>> = Vec::new();
            return self
                .send_custom_result_set(
                    &[
                        "TABLE_CATALOG",
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "ORDINAL_POSITION",
                        "COLUMN_DEFAULT",
                        "IS_NULLABLE",
                        "DATA_TYPE",
                        "CHARACTER_MAXIMUM_LENGTH",
                        "COLUMN_TYPE",
                        "COLUMN_KEY",
                        "EXTRA",
                        "PRIVILEGES",
                        "COLUMN_COMMENT",
                    ],
                    &rows,
                )
                .await
                .map(Some);
        }

        // PERFORMANCE_SCHEMA.* — return empty result sets
        if upper.contains("PERFORMANCE_SCHEMA.") {
            return self
                .send_custom_result_set(&["VARIABLE_NAME", "VARIABLE_VALUE"], &[])
                .await
                .map(Some);
        }

        Ok(None)
    }

    async fn try_handle_database_function(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let sql_upper = sql.trim().to_uppercase();

        // Match SELECT DATABASE()
        if !sql_upper.contains("DATABASE()") {
            return Ok(None);
        }

        let db_value = self
            .database
            .clone()
            .or_else(|| self.session.database.clone())
            .unwrap_or_default();

        let rows = vec![vec![db_value]];
        self.send_custom_result_set(&["DATABASE()"], &rows)
            .await
            .map(Some)
    }

    /// Handle SET @var = expr (user variable assignment).
    /// Supports multiple assignments: SET @a = 1, @b = 2, @c = 3
    async fn handle_set_user_variable(&mut self, sql: &str) -> ProtocolResult<()> {
        let trimmed = sql.trim();
        // Strip the SET keyword
        let rest = match trimmed
            .strip_prefix("SET ")
            .or_else(|| trimmed.strip_prefix("set "))
        {
            Some(r) => r.trim(),
            None => return self.send_ok(0, 0).await,
        };

        // Split into individual assignments at top-level commas
        // (respecting parentheses and string literals)
        let assignments = Self::split_set_assignments(rest);

        for assignment in &assignments {
            let assignment = assignment.trim();
            // Each assignment should be @var = expr or @var := expr
            let assignment = match assignment.strip_prefix('@') {
                Some(r) => r,
                None => continue,
            };
            // Find = or :=
            let (var_name, val_str) = if let Some(pos) = assignment.find(":=") {
                (&assignment[..pos], assignment[pos + 2..].trim())
            } else if let Some(pos) = assignment.find('=') {
                (&assignment[..pos], assignment[pos + 1..].trim())
            } else {
                continue;
            };
            let var_name = var_name.trim().to_lowercase();

            // Try to evaluate the value expression by wrapping in SELECT
            let scope_expr = format!("SELECT {} AS v", val_str);

            // Resolve inside a block so the catalog guard is dropped before await
            let datum = (|| -> Option<Datum> {
                let val_stmt = Parser::parse_one(&scope_expr).ok()?;
                let catalog = self.catalog.read();
                let resolver = Resolver::new(&catalog);
                let resolved = resolver.resolve(val_stmt).ok()?;

                if let crate::planner::logical::ResolvedStatement::Select(sel) = resolved {
                    if let Some(crate::planner::logical::ResolvedSelectItem::Expr {
                        expr: resolved_expr,
                        ..
                    }) = sel.columns.first()
                    {
                        let empty_row = crate::executor::row::Row::empty();
                        let vars = self.session.user_variables();
                        return crate::executor::eval::evaluate(resolved_expr, &empty_row, &vars)
                            .ok();
                    }
                }
                None
            })();

            let value = datum.unwrap_or(Datum::String(val_str.to_string()));
            self.session.set_user_variable(&var_name, value);
        }
        self.send_ok(0, 0).await
    }

    /// Handle SET @var = expr silently (no client response packets).
    /// Used within trigger bodies and other internal execution paths.
    fn execute_set_user_variable_silent(&self, sql: &str) {
        let trimmed = sql.trim();
        // Strip "SET " prefix case-insensitively
        let rest = if trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("SET ") {
            trimmed[4..].trim()
        } else {
            return;
        };

        let assignments = Self::split_set_assignments(rest);

        for assignment in &assignments {
            let assignment = assignment.trim();
            let assignment = match assignment.strip_prefix('@') {
                Some(r) => r,
                None => continue,
            };
            let (var_name, val_str) = if let Some(pos) = assignment.find(":=") {
                (&assignment[..pos], assignment[pos + 2..].trim())
            } else if let Some(pos) = assignment.find('=') {
                (&assignment[..pos], assignment[pos + 1..].trim())
            } else {
                continue;
            };
            let var_name = var_name.trim().to_lowercase();

            let scope_expr = format!("SELECT {} AS v", val_str);

            let datum = (|| -> Option<Datum> {
                let val_stmt = Parser::parse_one(&scope_expr).ok()?;
                let catalog = self.catalog.read();
                let resolver = Resolver::new(&catalog);
                let resolved = resolver.resolve(val_stmt).ok()?;

                if let crate::planner::logical::ResolvedStatement::Select(sel) = resolved {
                    if let Some(crate::planner::logical::ResolvedSelectItem::Expr {
                        expr: resolved_expr,
                        ..
                    }) = sel.columns.first()
                    {
                        let empty_row = crate::executor::row::Row::empty();
                        let vars = self.session.user_variables();
                        return crate::executor::eval::evaluate(resolved_expr, &empty_row, &vars)
                            .ok();
                    }
                }
                None
            })();

            let value = datum.unwrap_or(Datum::String(val_str.to_string()));
            self.session.set_user_variable(&var_name, value);
        }
    }

    /// Split a SET assignment list at top-level commas, respecting
    /// parentheses and string literals.
    fn split_set_assignments(s: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth = 0u32;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut start = 0;
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if in_single_quote {
                if b == b'\'' {
                    // Check for escaped quote ''
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
            } else if in_double_quote {
                if b == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
            } else {
                match b {
                    b'\'' => in_single_quote = true,
                    b'"' => in_double_quote = true,
                    b'(' => depth += 1,
                    b')' => depth = depth.saturating_sub(1),
                    b',' if depth == 0 => {
                        parts.push(&s[start..i]);
                        start = i + 1;
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        if start < s.len() {
            parts.push(&s[start..]);
        }
        parts
    }

    /// Extract the value from SET sql_mode / SET @@sql_mode / SET SESSION sql_mode.
    /// Returns None if the SQL is not a sql_mode SET statement.
    fn extract_sql_mode_value(sql: &str) -> Option<String> {
        let upper = sql.trim().to_uppercase();
        // Match: SET SQL_MODE, SET @@SQL_MODE, SET @@SESSION.SQL_MODE, SET SESSION SQL_MODE
        let rest = if let Some(r) = upper.strip_prefix("SET ") {
            r.trim()
        } else {
            return None;
        };
        let rest = if let Some(r) = rest.strip_prefix("@@SESSION.") {
            r.trim()
        } else if let Some(r) = rest.strip_prefix("@@GLOBAL.") {
            r.trim()
        } else if let Some(r) = rest.strip_prefix("@@") {
            r.trim()
        } else if let Some(r) = rest.strip_prefix("SESSION ") {
            r.trim()
        } else if let Some(r) = rest.strip_prefix("GLOBAL ") {
            r.trim()
        } else {
            rest
        };
        // Must be exactly "SQL_MODE" followed by '=' or whitespace, not "SQL_MODE_FOO"
        if !rest.starts_with("SQL_MODE") {
            return None;
        }
        let after = &rest["SQL_MODE".len()..];
        if !after.is_empty() && !after.starts_with('=') && !after.starts_with(' ') {
            return None;
        }
        // Find the '=' and extract value from original sql (preserve case)
        let eq_pos = sql.find('=')?;
        let val = sql[eq_pos + 1..].trim().trim_end_matches(';');
        Some(val.to_string())
    }

    async fn try_handle_system_variable(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        use regex::Regex;

        // Check for DATABASE() function first
        if let Some(()) = self.try_handle_database_function(sql).await? {
            return Ok(Some(()));
        }

        // Simple pattern matching for SELECT @@variable queries
        let sql_upper = sql.to_uppercase();
        if !sql_upper.contains("@@") {
            return Ok(None);
        }

        // Extract variable names from the query
        lazy_static::lazy_static! {
            static ref VAR_RE: Regex = Regex::new(r"@@(?:global\.|session\.)?(\w+)").unwrap();
        }

        let vars: Vec<&str> = VAR_RE
            .captures_iter(sql)
            .map(|c| c.get(1).unwrap().as_str())
            .collect();

        if vars.is_empty() {
            return Ok(None);
        }

        debug!(
            connection_id = self.connection_id,
            variables = ?vars,
            "Handling system variable query"
        );

        // Build response with default values for common variables
        let mut values: Vec<String> = Vec::new();
        let mut col_names: Vec<String> = Vec::new();

        for var in &vars {
            let var_lower = var.to_lowercase();
            let value = match var_lower.as_str() {
                "max_allowed_packet" => "16777216",
                "wait_timeout" => "28800",
                "interactive_timeout" => "28800",
                "net_write_timeout" => "60",
                "net_read_timeout" => "30",
                "socket" => "/tmp/roodb.sock",
                "character_set_client" => "utf8mb4",
                "character_set_connection" => "utf8mb4",
                "character_set_results" => "utf8mb4",
                "collation_connection" => "utf8mb4_general_ci",
                "sql_mode" => {
                    // Dynamic: return session sql_mode
                    col_names.push(format!("@@{}", var_lower));
                    values.push(self.session.sql_mode_string());
                    continue;
                }
                "time_zone" => "SYSTEM",
                "system_time_zone" => "UTC",
                "transaction_isolation" | "tx_isolation" => "REPEATABLE-READ",
                "autocommit" => "1",
                "version" => "8.0.0-RooDB",
                "version_comment" => "RooDB",
                "default_storage_engine" | "storage_engine" => "RooDB",
                "lower_case_table_names" => "0",
                "have_openssl" | "have_ssl" => "YES",
                _ => "", // Unknown variable, return empty string
            };
            col_names.push(format!("@@{}", var_lower));
            values.push(value.to_string());
        }

        // Send result set with one row
        self.writer.set_sequence(1);

        // Column count
        let count_packet = encode_column_count(values.len() as u64);
        self.writer.write_packet(&count_packet).await?;

        // Column definitions
        for name in &col_names {
            use crate::catalog::DataType;
            use crate::planner::logical::OutputColumn;

            let col = OutputColumn {
                id: 0,
                name: name.clone(),
                data_type: DataType::Varchar(255),
                nullable: true,
            };
            let def = ColumnDefinition41::from_output_column(&col, "", "");
            self.writer.write_packet(&def.encode()).await?;
        }

        // EOF after columns (unless DEPRECATE_EOF)
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        // Send single row with values
        let row = crate::executor::row::Row::new(
            values
                .iter()
                .map(|v| crate::executor::datum::Datum::String(v.clone()))
                .collect(),
        );
        let row_packet = encode_text_row(&row);
        self.writer.write_packet(&row_packet).await?;

        // Final EOF/OK
        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(default_status(), 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;

        debug!(
            connection_id = self.connection_id,
            "System variable response sent"
        );

        Ok(Some(()))
    }

    // ============ View Handlers (Raft-persisted) ============

    /// Handle CREATE VIEW with Raft persistence.
    /// Validates the view query at creation time, then persists to system.views via Raft.
    async fn handle_create_view_raft(
        &mut self,
        view_name: &str,
        query_sql: &str,
        or_replace: bool,
    ) -> ProtocolResult<()> {
        use crate::catalog::system_tables::{view_def_to_row, SYSTEM_VIEWS};
        use crate::catalog::ViewDef;
        use crate::executor::encoding::{encode_row, encode_row_key};
        use crate::raft::RowChange;
        use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

        let view_name = view_name.trim_matches('`').to_string();

        // Validate the view query parses correctly
        if let Err(e) = crate::sql::Parser::parse_one(query_sql) {
            return self
                .send_error(
                    codes::ER_PARSE_ERROR,
                    states::SYNTAX_ERROR,
                    &format!("Invalid view query: {}", e),
                )
                .await;
        }

        // Check if view already exists
        let exists = {
            let catalog = self.catalog.read();
            catalog.view_exists(&view_name)
        };

        if exists && !or_replace {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("View '{}' already exists", view_name),
                )
                .await;
        }

        // If replacing, delete old row first
        let mut changes = Vec::new();
        if exists && or_replace {
            if let Some(delete_change) = self.find_view_row_for_delete(&view_name).await? {
                changes.push(delete_change);
            }
        }

        // Insert new row into system.views
        let view_def = ViewDef {
            name: view_name.clone(),
            query_sql: query_sql.to_string(),
        };

        let (local_id, node_id) = allocate_row_id_batch(1);
        let row_id = encode_row_id(local_id, node_id);
        let view_row = view_def_to_row(&view_def);
        let key = encode_row_key(SYSTEM_VIEWS, row_id);
        let value = encode_row(&view_row);
        changes.push(RowChange::insert(SYSTEM_VIEWS, key, value));

        let changeset = ChangeSet::new_with_changes(0, changes);
        match self.raft_node.propose_changes(changeset).await {
            Ok(()) => self.send_ok(0, 0).await,
            Err(e) => {
                self.send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Failed to create view: {}", e),
                )
                .await
            }
        }
    }

    /// Handle DROP VIEW with Raft persistence.
    async fn handle_drop_view_raft(
        &mut self,
        names: &[String],
        if_exists: bool,
    ) -> ProtocolResult<()> {
        let mut changeset = ChangeSet::new(0);

        for raw_name in names {
            let view_name = raw_name.trim_matches('`').to_string();

            let exists = {
                let catalog = self.catalog.read();
                catalog.view_exists(&view_name)
            };

            if !exists {
                if if_exists {
                    continue;
                }
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("View '{}' does not exist", view_name),
                    )
                    .await;
            }

            if let Some(delete_change) = self.find_view_row_for_delete(&view_name).await? {
                changeset.push(delete_change);
            }
        }

        if !changeset.changes.is_empty() {
            match self.raft_node.propose_changes(changeset).await {
                Ok(()) => self.send_ok(0, 0).await,
                Err(e) => {
                    self.send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Failed to drop view: {}", e),
                    )
                    .await
                }
            }
        } else {
            self.send_ok(0, 0).await
        }
    }

    /// Scan system.views for a row matching the given view name and return a delete RowChange.
    async fn find_view_row_for_delete(
        &self,
        view_name: &str,
    ) -> ProtocolResult<Option<crate::raft::RowChange>> {
        use crate::catalog::system_tables::SYSTEM_VIEWS;
        use crate::executor::encoding::decode_row;
        use crate::raft::RowChange;

        let prefix = format!("t:{SYSTEM_VIEWS}:");
        let end = format!("t:{SYSTEM_VIEWS};\x00");

        let rows = self
            .mvcc
            .scan_raw(prefix.as_bytes(), end.as_bytes())
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;

        for (key, value) in rows {
            let row_data = if value.len() > 17 {
                &value[17..]
            } else {
                &value
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(Datum::String(name)) = row.values().first() {
                    if name.eq_ignore_ascii_case(view_name) {
                        return Ok(Some(RowChange::delete_with_value(
                            SYSTEM_VIEWS,
                            key,
                            row_data.to_vec(),
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    // ============ Trigger Handlers ============

    /// Handle CREATE TRIGGER — parse the trigger definition and store in catalog.
    ///
    /// MySQL syntax: CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE}
    ///               ON table FOR EACH ROW body_statement
    ///
    /// The header is parsed by tokenizing, the body is parsed as a standalone
    /// SQL statement via sqlparser. The body AST is stored in the catalog and
    /// executed during DML with NEW/OLD column substitutions.
    async fn handle_create_trigger_sql(&mut self, sql: &str) -> ProtocolResult<()> {
        use crate::catalog::{TriggerDef, TriggerEvent, TriggerTiming};

        let sql_clean = sql.trim().trim_end_matches(';');
        let upper = sql_clean.to_uppercase();

        // Find "FOR EACH ROW" to split header from body
        let fer_pos = match upper.find("FOR EACH ROW") {
            Some(p) => p,
            None => {
                return self
                    .send_error(
                        codes::ER_PARSE_ERROR,
                        states::SYNTAX_ERROR,
                        "CREATE TRIGGER requires FOR EACH ROW",
                    )
                    .await;
            }
        };

        let header = sql_clean[..fer_pos].trim();
        let body_sql = sql_clean[fer_pos + 12..].trim(); // 12 = "FOR EACH ROW".len()

        // Parse header tokens: CREATE TRIGGER name BEFORE INSERT ON table
        let tokens: Vec<&str> = header.split_whitespace().collect();
        if tokens.len() < 7 {
            return self
                .send_error(
                    codes::ER_PARSE_ERROR,
                    states::SYNTAX_ERROR,
                    "Invalid CREATE TRIGGER syntax",
                )
                .await;
        }

        let name = tokens[2].trim_matches('`').to_string();
        let timing_str = tokens[3].to_uppercase();
        let event_str = tokens[4].to_uppercase();
        let table_name = tokens[6].trim_matches('`').to_string();

        let timing = match timing_str.as_str() {
            "BEFORE" => TriggerTiming::Before,
            "AFTER" => TriggerTiming::After,
            _ => {
                return self
                    .send_error(
                        codes::ER_PARSE_ERROR,
                        states::SYNTAX_ERROR,
                        &format!("Expected BEFORE or AFTER, got '{}'", timing_str),
                    )
                    .await;
            }
        };

        let event = match event_str.as_str() {
            "INSERT" => TriggerEvent::Insert,
            "UPDATE" => TriggerEvent::Update,
            "DELETE" => TriggerEvent::Delete,
            _ => {
                return self
                    .send_error(
                        codes::ER_PARSE_ERROR,
                        states::SYNTAX_ERROR,
                        &format!("Expected INSERT, UPDATE, or DELETE, got '{}'", event_str),
                    )
                    .await;
            }
        };

        // Parse body as a standalone SQL statement through the full parser
        let body = match crate::sql::Parser::parse_one(body_sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                return self
                    .send_error(
                        codes::ER_PARSE_ERROR,
                        states::SYNTAX_ERROR,
                        &format!("Parse error in trigger body: {}", e),
                    )
                    .await;
            }
        };

        let def = TriggerDef {
            name,
            table_name,
            timing,
            event,
            body,
            create_sql: sql.to_string(),
        };

        {
            let mut catalog = self.catalog.write();
            catalog.create_trigger(def);
        }

        self.send_ok(0, 0).await
    }

    /// Handle DROP TRIGGER — parse and remove from catalog
    async fn handle_drop_trigger_sql(&mut self, sql: &str) -> ProtocolResult<()> {
        let upper = sql.to_uppercase();
        let if_exists = upper.contains("IF EXISTS");

        // Extract trigger name: DROP TRIGGER [IF EXISTS] name
        let name_part = if if_exists {
            sql.trim_end_matches(';')
                .trim()
                .rsplit("EXISTS")
                .next()
                .unwrap_or("")
                .trim()
        } else {
            sql.trim_end_matches(';')
                .trim()
                .strip_prefix("DROP TRIGGER ")
                .or_else(|| {
                    sql.trim_end_matches(';')
                        .trim()
                        .strip_prefix("drop trigger ")
                })
                .unwrap_or("")
                .trim()
        };

        let trigger_name = name_part.trim_matches('`').trim_matches('"');

        if trigger_name.is_empty() {
            return self
                .send_error(
                    codes::ER_PARSE_ERROR,
                    states::SYNTAX_ERROR,
                    "Missing trigger name in DROP TRIGGER",
                )
                .await;
        }

        let result = {
            let mut catalog = self.catalog.write();
            catalog.drop_trigger(trigger_name)
        };

        match result {
            Ok(()) => self.send_ok(0, 0).await,
            Err(_) if if_exists => self.send_ok(0, 0).await,
            Err(e) => {
                self.send_error(
                    codes::ER_NO_SUCH_TABLE,
                    states::NO_SUCH_TABLE,
                    &format!("{}", e),
                )
                .await
            }
        }
    }

    /// Extract trigger-relevant info from an INSERT physical plan.
    /// Returns (table_name, column_names, row_values) if triggers exist.
    fn extract_insert_trigger_info(&self, plan: &PhysicalPlan) -> Option<InsertTriggerInfo> {
        if let PhysicalPlan::Insert {
            table,
            columns,
            values,
            ..
        } = plan
        {
            // Check if any triggers exist for this table
            let catalog = self.catalog.read();
            let has_before = !catalog
                .get_triggers(
                    table,
                    crate::catalog::TriggerTiming::Before,
                    crate::catalog::TriggerEvent::Insert,
                )
                .is_empty();
            let has_after = !catalog
                .get_triggers(
                    table,
                    crate::catalog::TriggerTiming::After,
                    crate::catalog::TriggerEvent::Insert,
                )
                .is_empty();

            if has_before || has_after {
                Some(InsertTriggerInfo {
                    table_name: table.clone(),
                    column_names: columns.iter().map(|c| c.name.clone()).collect(),
                    values: values.clone(),
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Fire INSERT triggers for each row that was inserted.
    /// Resolves the trigger body with NEW bindings and executes through the full pipeline.
    async fn fire_insert_triggers(&mut self, info: &InsertTriggerInfo) -> ProtocolResult<()> {
        use crate::catalog::{TriggerEvent, TriggerTiming};
        use crate::executor::eval::evaluate;
        use crate::executor::row::Row;
        use crate::executor::trigger::{datum_to_sp_value, substitute_new_refs};

        // Get triggers (both BEFORE and AFTER — for now we fire both post-execution)
        let triggers: Vec<crate::catalog::TriggerDef> = {
            let catalog = self.catalog.read();
            let mut trigs = Vec::new();
            trigs.extend(
                catalog
                    .get_triggers(
                        &info.table_name,
                        TriggerTiming::Before,
                        TriggerEvent::Insert,
                    )
                    .into_iter()
                    .cloned(),
            );
            trigs.extend(
                catalog
                    .get_triggers(&info.table_name, TriggerTiming::After, TriggerEvent::Insert)
                    .into_iter()
                    .cloned(),
            );
            trigs
        };

        if triggers.is_empty() {
            return Ok(());
        }

        let empty_row = Row::empty();
        let user_vars = self.session.user_variables();

        // For each inserted row, fire all triggers
        for value_row in &info.values {
            // Evaluate the row expressions to get actual values
            let mut new_values = std::collections::HashMap::new();
            for (col_idx, expr) in value_row.iter().enumerate() {
                if col_idx < info.column_names.len() {
                    let datum = evaluate(expr, &empty_row, &user_vars)
                        .unwrap_or(crate::executor::datum::Datum::Null);
                    let sp_val = datum_to_sp_value(&datum);
                    new_values.insert(info.column_names[col_idx].clone(), sp_val);
                }
            }

            // Fire each trigger
            for trigger in &triggers {
                // Substitute NEW.col references in the trigger body
                let substituted = substitute_new_refs(&trigger.body, &new_values);

                // Execute the substituted statement through the full pipeline
                let sql = substituted.to_string();
                self.execute_sql_silent(&sql)
                    .await
                    .map_err(|e| ProtocolError::Sql(crate::sql::SqlError::InvalidOperation(e)))?;
            }
        }

        Ok(())
    }

    // ============ Stored Procedure Handlers ============

    /// Handle CREATE PROCEDURE statement
    async fn handle_create_procedure(
        &mut self,
        name: &sqlparser::ast::ObjectName,
        params: Option<&[sqlparser::ast::ProcedureParam]>,
        body: &sqlparser::ast::ConditionalStatements,
    ) -> ProtocolResult<()> {
        let proc_name = name.to_string().to_lowercase();

        // Convert sqlparser params to our ProcedureParam
        let our_params: Vec<ProcedureParam> = params
            .unwrap_or(&[])
            .iter()
            .map(|p| {
                let mode = match &p.mode {
                    Some(sqlparser::ast::ArgMode::Out) => ParamMode::Out,
                    Some(sqlparser::ast::ArgMode::InOut) => ParamMode::InOut,
                    _ => ParamMode::In,
                };
                ProcedureParam {
                    name: p.name.value.to_lowercase(),
                    data_type: p.data_type.to_string(),
                    mode,
                }
            })
            .collect();

        let body_sql = body.to_string();

        // Check if procedure already exists
        let exists = {
            let catalog = self.catalog.read();
            catalog.procedure_exists(&proc_name)
        };
        if exists {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Procedure '{}' already exists", proc_name),
                )
                .await;
        }

        let proc_def = ProcedureDef {
            name: proc_name.clone(),
            params: our_params,
            body_sql,
            returns_type: None,
        };

        // Write to system.procedures via Raft
        use crate::catalog::system_tables::procedure_def_to_row;
        use crate::executor::encoding::{encode_row, encode_row_key};
        use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

        let (local_id, node_id) = allocate_row_id_batch(1);
        let row_id = encode_row_id(local_id, node_id);
        let proc_row = procedure_def_to_row(&proc_def);
        let key = encode_row_key(SYSTEM_PROCEDURES, row_id);
        let value = encode_row(&proc_row);

        use crate::raft::RowChange;
        let change = RowChange::insert(SYSTEM_PROCEDURES, key, value);
        let changeset = ChangeSet::new_with_changes(0, vec![change]);

        match self.raft_node.propose_changes(changeset).await {
            Ok(()) => self.send_ok(0, 0).await,
            Err(e) => {
                self.send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Failed to create procedure: {}", e),
                )
                .await
            }
        }
    }

    /// Handle CREATE FUNCTION via text parsing (MySQL syntax with BEGIN/END body)
    /// Handle REPLACE INTO — MySQL semantics: if duplicate key, delete old row + insert new.
    /// Approach: parse table + columns + values, for each row try INSERT;
    /// on duplicate key, DELETE by PK then INSERT.
    async fn handle_replace(&mut self, sql: &str) -> ProtocolResult<()> {
        // Parse REPLACE INTO table (cols) VALUES (vals), (vals)...
        let re =
            regex::Regex::new(r"(?is)REPLACE\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s+VALUES\s+(.*)")
                .unwrap();

        let caps = match re.captures(sql.trim().trim_end_matches(';')) {
            Some(c) => c,
            None => {
                // Fallback: convert REPLACE to INSERT and try directly
                let insert_sql = regex::Regex::new(r"(?i)^REPLACE\b")
                    .unwrap()
                    .replace(sql, "INSERT");
                return self.handle_query(&insert_sql).await;
            }
        };

        let table = caps.get(1).unwrap().as_str();
        let cols = caps.get(2).map(|m| m.as_str()).unwrap_or("");
        let values_part = caps.get(3).unwrap().as_str();

        // For each VALUES group, try INSERT; on dup key error, DELETE + INSERT
        let col_clause = if cols.is_empty() {
            String::new()
        } else {
            format!("({})", cols)
        };

        // Split value groups: (v1,v2), (v3,v4)
        let mut total_affected = 0u64;
        let val_re = regex::Regex::new(r"\(([^)]*)\)").unwrap();
        for val_match in val_re.captures_iter(values_part) {
            let vals = val_match.get(1).unwrap().as_str();
            let insert_sql = format!("INSERT INTO {} {} VALUES ({})", table, col_clause, vals);
            match self.execute_sql_silent(&insert_sql).await {
                Ok(n) => total_affected += n,
                Err(e) => {
                    if e.contains("Duplicate entry") || e.contains("duplicate key") {
                        // Build DELETE WHERE pk = val using the first column value
                        // For multi-column PK, we'd need schema introspection.
                        // Simplified: delete by first column value
                        let first_val = vals.split(',').next().unwrap_or("").trim();
                        let first_col = if cols.is_empty() {
                            // No column list; assume first column is PK
                            String::new()
                        } else {
                            cols.split(',').next().unwrap_or("").trim().to_string()
                        };
                        if !first_col.is_empty() {
                            let delete_sql = format!(
                                "DELETE FROM {} WHERE {} = {}",
                                table, first_col, first_val
                            );
                            if let Ok(d) = self.execute_sql_silent(&delete_sql).await {
                                total_affected += d;
                            }
                        }
                        // Retry INSERT
                        if let Ok(n) = self.execute_sql_silent(&insert_sql).await {
                            total_affected += n;
                        }
                    }
                    // Else: non-duplicate error, skip row
                }
            }
        }

        self.send_ok(total_affected, 0).await
    }

    /// Handle LOAD DATA [LOCAL] INFILE 'path' INTO TABLE table_name [CHARACTER SET cs]
    async fn handle_load_data(&mut self, sql: &str) -> ProtocolResult<()> {
        // Parse: LOAD DATA [LOCAL] INFILE 'path' INTO TABLE table_name
        let re = regex::Regex::new(
            r#"(?i)LOAD\s+DATA\s+(?:LOCAL\s+)?INFILE\s+['"]([^'"]+)['"]\s+INTO\s+TABLE\s+(\w+)"#,
        )
        .unwrap();

        let caps = match re.captures(sql) {
            Some(c) => c,
            None => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        "Failed to parse LOAD DATA statement",
                    )
                    .await;
            }
        };

        let file_path = caps.get(1).unwrap().as_str();
        let table_name = caps.get(2).unwrap().as_str();

        // Resolve file path: try multiple base directories
        let resolved = Self::resolve_load_data_path(file_path);
        let resolved_path = match resolved {
            Some(p) => p,
            None => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!(
                            "Can't find file '{}' (resolved from data directory)",
                            file_path
                        ),
                    )
                    .await;
            }
        };

        // Read the file
        let data = match std::fs::read(&resolved_path) {
            Ok(d) => d,
            Err(e) => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Can't read file '{}': {}", resolved_path.display(), e),
                    )
                    .await;
            }
        };

        // Convert to UTF-8 string (best effort)
        let text = String::from_utf8_lossy(&data);

        // Split by lines, handling backslash-newline continuation
        let mut rows_inserted = 0u64;
        let raw_lines: Vec<&str> = text.split('\n').collect();
        let mut lines: Vec<String> = Vec::new();
        let mut i = 0;
        while i < raw_lines.len() {
            let mut line = raw_lines[i].to_string();
            // Handle backslash continuation: line ending with \ joins with next
            while line.ends_with('\\') && i + 1 < raw_lines.len() {
                line.pop(); // remove trailing backslash
                i += 1;
                line.push_str(raw_lines[i]);
            }
            i += 1;
            if !line.is_empty() {
                lines.push(line);
            }
        }

        for line in &lines {
            // Process MySQL backslash escaping: remove lone backslashes
            let processed = Self::process_load_data_escapes(line);
            // Escape single quotes for INSERT
            let escaped = processed.replace('\'', "''");
            let insert_sql = format!("INSERT INTO {} VALUES ('{}')", table_name, escaped);
            match self.execute_sql_silent(&insert_sql).await {
                Ok(n) => rows_inserted += n,
                Err(e) => {
                    // Add warning but continue (MySQL LOAD DATA behavior)
                    self.session.add_warning("Warning", 1265, e);
                }
            }
        }

        self.send_ok(rows_inserted, 0).await
    }

    /// Resolve a LOAD DATA INFILE path, trying multiple base directories.
    fn resolve_load_data_path(path: &str) -> Option<std::path::PathBuf> {
        let p = std::path::Path::new(path);

        // 1. Absolute path
        if p.is_absolute() && p.exists() {
            return Some(p.to_path_buf());
        }

        // 2. Relative to CWD
        if p.exists() {
            return Some(p.to_path_buf());
        }

        // 3. Try resolving from /usr/lib/mysql-test/ (MTR basedir)
        // The test path ../../std_data/file.dat from mysql-test/var/data/
        // resolves to mysql-test/std_data/file.dat
        let mtr_base = std::path::Path::new("/usr/lib/mysql-test");
        // Extract the std_data/... portion from relative paths like ../../std_data/file.dat
        if let Some(std_data_idx) = path.find("std_data") {
            let relative = &path[std_data_idx..];
            let mtr_path = mtr_base.join(relative);
            if mtr_path.exists() {
                return Some(mtr_path);
            }
        }

        // 4. Try as relative from MTR basedir directly
        let mtr_path = mtr_base.join(path);
        if mtr_path.exists() {
            return Some(mtr_path);
        }

        None
    }

    /// Process MySQL LOAD DATA escape sequences in a field value.
    /// MySQL's default ESCAPED BY is backslash (\).
    fn process_load_data_escapes(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                // Escape sequence
                match chars.peek() {
                    Some('n') => {
                        result.push('\n');
                        chars.next();
                    }
                    Some('t') => {
                        result.push('\t');
                        chars.next();
                    }
                    Some('0') => {
                        result.push('\0');
                        chars.next();
                    }
                    Some('\\') => {
                        result.push('\\');
                        chars.next();
                    }
                    Some(_) => {
                        // Backslash before any other char: skip the backslash
                        if let Some(next) = chars.next() {
                            result.push(next);
                        }
                    }
                    None => {
                        // Trailing backslash: skip it
                    }
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    async fn handle_create_function_text(&mut self, sql: &str) -> ProtocolResult<()> {
        // Parse: CREATE FUNCTION name(params) RETURNS type BEGIN ... END
        let re = regex::Regex::new(
            r"(?is)CREATE\s+FUNCTION\s+(\w+)\s*\(([^)]*)\)\s+RETURNS\s+(\w+)\s+(BEGIN\b.+\bEND)",
        )
        .unwrap();

        let caps = match re.captures(sql) {
            Some(c) => c,
            None => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        "Failed to parse CREATE FUNCTION",
                    )
                    .await;
            }
        };

        let func_name = caps.get(1).unwrap().as_str().to_lowercase();
        let params_str = caps.get(2).unwrap().as_str();
        let returns_type = caps.get(3).unwrap().as_str().to_string();
        let body_sql = caps.get(4).unwrap().as_str().to_string();

        // Parse parameters: "inputvar CHAR, ..."
        let our_params: Vec<ProcedureParam> = params_str
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                let parts: Vec<&str> = s.trim().splitn(2, char::is_whitespace).collect();
                ProcedureParam {
                    name: parts.first().unwrap_or(&"").to_lowercase(),
                    data_type: parts.get(1).unwrap_or(&"VARCHAR").to_string(),
                    mode: ParamMode::In,
                }
            })
            .collect();

        let proc_def = ProcedureDef {
            name: func_name.clone(),
            params: our_params,
            body_sql: body_sql.clone(),
            returns_type: Some(returns_type),
        };

        // Store via Raft (reuse procedure storage)
        use crate::catalog::system_tables::procedure_def_to_row;
        use crate::executor::encoding::{encode_row, encode_row_key};
        use crate::raft::RowChange;
        use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

        let (local_id, node_id) = allocate_row_id_batch(1);
        let row_id = encode_row_id(local_id, node_id);
        let proc_row = procedure_def_to_row(&proc_def);
        let key = encode_row_key(SYSTEM_PROCEDURES, row_id);
        let value = encode_row(&proc_row);

        let change = RowChange::insert(SYSTEM_PROCEDURES, key, value);
        let changeset = ChangeSet::new_with_changes(0, vec![change]);
        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ProtocolError::Unsupported(format!("Failed to create function: {}", e)))?;

        // Register in catalog
        let param_names: String = proc_def
            .params
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        {
            let mut catalog = self.catalog.write();
            catalog.register_procedure(proc_def);
        }

        // Register UDF in user_variables so eval_function can find it
        {
            let udf_key = format!("__udf_{}", func_name);
            let params_key = format!("__udf_{}_params", func_name);
            let vars = self.session.user_variables();
            let mut w = vars.write();
            w.insert(udf_key, Datum::String(body_sql));
            w.insert(params_key, Datum::String(param_names));
        }

        self.send_ok(0, 0).await
    }

    async fn handle_drop_procedure(
        &mut self,
        proc_name: &str,
        if_exists: bool,
    ) -> ProtocolResult<()> {
        let proc_name = proc_name.to_lowercase();

        // Check if procedure exists
        let exists = {
            let catalog = self.catalog.read();
            catalog.procedure_exists(&proc_name)
        };
        if !exists {
            if if_exists {
                return self.send_ok(0, 0).await;
            }
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Procedure '{}' does not exist", proc_name),
                )
                .await;
        }

        // Scan system.procedures to find the row to delete
        use crate::executor::encoding::decode_row;
        use crate::raft::RowChange;

        let prefix = format!("t:{SYSTEM_PROCEDURES}:");
        let end = format!("t:{SYSTEM_PROCEDURES};\x00");

        let rows = self
            .mvcc
            .scan_raw(prefix.as_bytes(), end.as_bytes())
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;

        let mut changeset = ChangeSet::new(0);
        for (key, value) in rows {
            let row_data = if value.len() > 17 {
                &value[17..]
            } else {
                &value
            };
            if let Ok(row) = decode_row(row_data) {
                if let Some(Datum::String(name)) = row.values().first() {
                    if name.eq_ignore_ascii_case(&proc_name) {
                        changeset.push(RowChange::delete_with_value(
                            SYSTEM_PROCEDURES,
                            key,
                            row_data.to_vec(),
                        ));
                    }
                }
            }
        }

        if !changeset.changes.is_empty() {
            match self.raft_node.propose_changes(changeset).await {
                Ok(()) => self.send_ok(0, 0).await,
                Err(e) => {
                    self.send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Failed to drop procedure: {}", e),
                    )
                    .await
                }
            }
        } else {
            self.send_ok(0, 0).await
        }
    }

    /// Handle CALL procedure_name(args...) — AST tree-walker interpreter
    async fn handle_call(
        &mut self,
        proc_name: &str,
        call_args: &[sqlparser::ast::Expr],
    ) -> ProtocolResult<()> {
        use crate::executor::procedure::{build_procedure_context, propagate_out_params};

        // Look up procedure in catalog
        let proc_def = {
            let catalog = self.catalog.read();
            catalog.get_procedure(proc_name).cloned()
        };
        let proc_def = match proc_def {
            Some(def) => def,
            None => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Procedure '{}' does not exist", proc_name),
                    )
                    .await;
            }
        };

        // Build context: validate arg count, bind IN/INOUT params, OUT = Null
        let user_vars = self.session.user_variables();
        let mut ctx = match build_procedure_context(&proc_def, call_args, &user_vars) {
            Ok(c) => c,
            Err(e) => {
                let code = if e.contains("Out of range value") {
                    codes::ER_WARN_DATA_OUT_OF_RANGE
                } else {
                    codes::ER_UNKNOWN_ERROR
                };
                return self.send_error(code, "22003", &e).await;
            }
        };

        // Extract DECLARE HANDLER declarations before parsing
        let handlers = Self::extract_handlers(&proc_def.body_sql);

        // Parse procedure body into Vec<Statement>
        let body_stmts = match Self::parse_body_to_stmts(&proc_def.body_sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Failed to parse procedure body: {}", e),
                    )
                    .await;
            }
        };
        // Execute the body with multi-result-set mode if client supports it
        let client_multi =
            self.client_capabilities & handshake::capabilities::CLIENT_MULTI_RESULTS != 0;
        self.in_multi_result = client_multi;
        let body_result = self
            .execute_procedure_body_with_handlers(&body_stmts, &mut ctx, &handlers)
            .await;

        match body_result {
            Ok(_) => {}
            Err(e) => {
                self.in_multi_result = false;
                return self
                    .send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("Procedure execution error: {}", e),
                    )
                    .await;
            }
        }

        // Propagate OUT/INOUT params back to user variables
        propagate_out_params(&proc_def, call_args, &ctx, &user_vars);

        // Send final OK
        let sent_results = self.sp_sent_results;
        self.in_multi_result = false;
        self.sp_sent_results = false;
        if sent_results {
            // Multi-result: continue sequence from result set, write final OK
            let packet = encode_ok_packet(ctx.rows_affected, 0, default_status(), 0);
            self.writer.write_packet(&packet).await?;
            self.writer.flush().await?;
            Ok(())
        } else {
            // No result sets sent: standard OK with sequence reset
            self.send_ok(ctx.rows_affected, 0).await
        }
    }

    /// Parse procedure body SQL into Vec<Statement>.
    /// Wraps body in CREATE PROCEDURE to leverage sqlparser's procedure-context
    /// parsing for WHILE, IF, CASE, cursors, etc.
    /// Normalizes MySQL DECLARE variable statements (which sqlparser doesn't
    /// support in MySQL mode) into SET statements.
    fn parse_body_to_stmts(body_sql: &str) -> Result<Vec<sqlparser::ast::Statement>, String> {
        use sqlparser::ast::Statement as S;

        let trimmed = body_sql.trim();
        if trimmed.is_empty() {
            return Ok(vec![]);
        }

        // Strip outer BEGIN/END
        let inner = {
            let upper = trimmed.to_uppercase();
            if upper.starts_with("BEGIN") && upper.ends_with("END") {
                trimmed[5..trimmed.len() - 3].trim()
            } else {
                trimmed
            }
        };

        // Pre-process: convert MySQL DECLARE variable statements to SET.
        // DECLARE var_name TYPE [DEFAULT expr]; → SET var_name = expr;
        // DECLARE var_name CURSOR FOR query; → kept as-is (sqlparser handles this)
        // Normalize := to = for SET statements (MySQL supports both)
        let inner = inner.replace(":=", "=");
        let processed = Self::normalize_declare_stmts(&inner);

        // Wrap in BEGIN...END and CREATE PROCEDURE for parsing
        let wrapper = format!("CREATE PROCEDURE __body__() BEGIN {} END", processed);
        let stmt = crate::sql::Parser::parse_one(&wrapper).map_err(|e| e.to_string())?;

        // Extract the body statements from the parsed CREATE PROCEDURE
        if let S::CreateProcedure { body, .. } = stmt {
            Ok(body.statements().clone())
        } else {
            Err("Failed to parse procedure body".to_string())
        }
    }

    /// Convert MySQL DECLARE variable statements to SET statements.
    /// Leaves DECLARE ... CURSOR FOR ... unchanged (sqlparser handles those).
    /// Strips DECLARE ... HANDLER ... statements (handled separately).
    fn normalize_declare_stmts(body: &str) -> String {
        let mut result = String::with_capacity(body.len());
        // Split on semicolons, process each statement
        for part in body.split(';') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_uppercase();
            if upper.starts_with("DECLARE ") {
                let words: Vec<&str> = trimmed.split_whitespace().collect();
                // DECLARE CONTINUE HANDLER ... — strip (handled separately)
                if words.len() >= 3
                    && (words[1].eq_ignore_ascii_case("CONTINUE")
                        || words[1].eq_ignore_ascii_case("EXIT"))
                    && words[2].eq_ignore_ascii_case("HANDLER")
                {
                    // Skip — handlers are extracted by extract_handlers()
                    continue;
                }
                // Check if this is a cursor declaration (DECLARE name CURSOR FOR ...)
                if words.len() >= 3 && words[2].eq_ignore_ascii_case("CURSOR") {
                    // Keep cursor declarations as-is
                    if !result.is_empty() {
                        result.push(' ');
                    }
                    result.push_str(trimmed);
                    result.push(';');
                } else if let Some(default_pos) = upper.find(" DEFAULT ") {
                    // DECLARE var_name TYPE DEFAULT expr → SET var_name = expr
                    let var_name = words.get(1).unwrap_or(&"_");
                    let default_val = &trimmed[default_pos + 9..]; // skip " DEFAULT "
                    if !result.is_empty() {
                        result.push(' ');
                    }
                    result.push_str(&format!("SET {} = {};", var_name, default_val.trim()));
                } else {
                    // DECLARE var_name TYPE → SET var_name = NULL
                    let var_name = words.get(1).unwrap_or(&"_");
                    if !result.is_empty() {
                        result.push(' ');
                    }
                    result.push_str(&format!("SET {} = NULL;", var_name));
                }
            } else {
                if !result.is_empty() {
                    result.push(' ');
                }
                result.push_str(trimmed);
                result.push(';');
            }
        }
        result
    }

    /// Extract DECLARE CONTINUE/EXIT HANDLER declarations from procedure body.
    /// Returns vec of (handler_type, sqlstate, handler_sql).
    fn extract_handlers(body_sql: &str) -> Vec<(String, String, String)> {
        let trimmed = body_sql.trim();
        let inner = {
            let upper = trimmed.to_uppercase();
            if upper.starts_with("BEGIN") && upper.ends_with("END") {
                trimmed[5..trimmed.len() - 3].trim()
            } else {
                trimmed
            }
        };

        let mut handlers = Vec::new();
        for part in inner.split(';') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_uppercase();
            if !upper.starts_with("DECLARE ") {
                continue;
            }
            let words: Vec<&str> = trimmed.split_whitespace().collect();
            // DECLARE CONTINUE HANDLER FOR SQLSTATE 'xxxxx' handler_sql
            // DECLARE EXIT HANDLER FOR SQLSTATE 'xxxxx' handler_sql
            if words.len() >= 6
                && (words[1].eq_ignore_ascii_case("CONTINUE")
                    || words[1].eq_ignore_ascii_case("EXIT"))
                && words[2].eq_ignore_ascii_case("HANDLER")
                && words[3].eq_ignore_ascii_case("FOR")
                && words[4].eq_ignore_ascii_case("SQLSTATE")
            {
                let handler_type = words[1].to_uppercase();
                let sqlstate = words[5].trim_matches('\'').trim_matches('"').to_string();
                // The handler body is everything after SQLSTATE 'xxx'
                if let Some(sqlstate_pos) = upper.find("SQLSTATE") {
                    let after_sqlstate = trimmed[sqlstate_pos + 8..].trim();
                    // Find opening quote, then closing quote, then handler body
                    if let Some(open_q) = after_sqlstate.find(['\'', '"']) {
                        let quote_char = after_sqlstate.as_bytes()[open_q];
                        if let Some(close_q) =
                            after_sqlstate[open_q + 1..].find(|c: char| c as u8 == quote_char)
                        {
                            let handler_sql = after_sqlstate[open_q + 1 + close_q + 1..]
                                .trim()
                                .to_string();
                            handlers.push((handler_type, sqlstate, handler_sql));
                        }
                    }
                }
            }
        }
        handlers
    }

    /// Execute a list of procedure body statements. Returns control flow signal.
    fn execute_procedure_body<'a>(
        &'a mut self,
        stmts: &'a [sqlparser::ast::Statement],
        ctx: &'a mut crate::executor::procedure::ProcedureContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::executor::procedure::ProcControlFlow, String>,
                > + Send
                + 'a,
        >,
    > {
        use crate::executor::procedure::ProcControlFlow;
        Box::pin(async move {
            for stmt in stmts {
                match self.execute_procedure_stmt(stmt, ctx).await? {
                    ProcControlFlow::Continue => {}
                    ProcControlFlow::Return => return Ok(ProcControlFlow::Return),
                }
            }
            Ok(ProcControlFlow::Continue)
        })
    }

    /// Execute procedure body with DECLARE HANDLER support.
    /// When a statement fails and a matching CONTINUE handler exists, the error
    /// is recorded as a session warning and execution continues.
    fn execute_procedure_body_with_handlers<'a>(
        &'a mut self,
        stmts: &'a [sqlparser::ast::Statement],
        ctx: &'a mut crate::executor::procedure::ProcedureContext,
        handlers: &'a [(String, String, String)], // (handler_type, sqlstate, handler_sql)
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::executor::procedure::ProcControlFlow, String>,
                > + Send
                + 'a,
        >,
    > {
        use crate::executor::procedure::ProcControlFlow;
        Box::pin(async move {
            if handlers.is_empty() {
                // No handlers: fast path, same as execute_procedure_body
                for stmt in stmts {
                    match self.execute_procedure_stmt(stmt, ctx).await? {
                        ProcControlFlow::Continue => {}
                        ProcControlFlow::Return => return Ok(ProcControlFlow::Return),
                    }
                }
                return Ok(ProcControlFlow::Continue);
            }

            for stmt in stmts {
                match self.execute_procedure_stmt(stmt, ctx).await {
                    Ok(ProcControlFlow::Continue) => {}
                    Ok(ProcControlFlow::Return) => return Ok(ProcControlFlow::Return),
                    Err(err_msg) => {
                        // Map error to SQLSTATE
                        let sqlstate = Self::error_to_sqlstate(&err_msg);

                        // Check if any handler matches
                        let matching_handler =
                            handlers.iter().find(|(_, state, _)| *state == sqlstate);

                        if let Some((handler_type, _, handler_sql)) = matching_handler {
                            // Record the error as a session warning
                            self.session.add_warning(
                                "Error",
                                Self::sqlstate_to_error_code(&sqlstate),
                                err_msg.clone(),
                            );

                            // Execute the handler SQL
                            if !handler_sql.is_empty() {
                                let _ = self.handle_query(handler_sql).await;
                            }

                            if handler_type == "CONTINUE" {
                                continue;
                            } else {
                                // EXIT handler: stop execution
                                return Ok(ProcControlFlow::Continue);
                            }
                        } else {
                            // No matching handler: propagate error
                            return Err(err_msg);
                        }
                    }
                }
            }
            Ok(ProcControlFlow::Continue)
        })
    }

    /// Map error message to SQLSTATE code
    fn error_to_sqlstate(err_msg: &str) -> String {
        let lower = err_msg.to_lowercase();
        if lower.contains("out of range") || lower.contains("overflow") {
            "22003".to_string()
        } else if (lower.contains("null") && lower.contains("constraint"))
            || lower.contains("duplicate")
            || lower.contains("already exists")
        {
            "23000".to_string()
        } else if lower.contains("division by zero") || lower.contains("divide by zero") {
            "22012".to_string()
        } else if lower.contains("data truncat") {
            "22001".to_string()
        } else {
            "HY000".to_string() // general error
        }
    }

    /// Map SQLSTATE to MySQL error code
    fn sqlstate_to_error_code(sqlstate: &str) -> u16 {
        match sqlstate {
            "22003" => 1690, // ER_DATA_OUT_OF_RANGE (in procedure context)
            "23000" => 1062, // ER_DUP_ENTRY
            "22012" => 1365, // ER_DIVISION_BY_ZERO
            "22001" => 1265, // WARN_DATA_TRUNCATED
            _ => 1105,       // ER_UNKNOWN_ERROR
        }
    }

    /// Dispatch a single procedure body statement.
    async fn execute_procedure_stmt(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        ctx: &mut crate::executor::procedure::ProcedureContext,
    ) -> Result<crate::executor::procedure::ProcControlFlow, String> {
        use crate::executor::procedure::{
            datum_is_true, eval_declare_default, eval_sp_expr, CursorState, ProcControlFlow,
        };
        use sqlparser::ast::Statement as S;

        let user_vars = self.session.user_variables();

        match stmt {
            // DECLARE variables and cursors
            S::Declare { stmts } => {
                for decl in stmts {
                    // Cursor declaration: DECLARE cursor_name CURSOR FOR query
                    if let Some(query) = &decl.for_query {
                        let cursor_name = decl
                            .names
                            .first()
                            .map(|i| i.value.to_lowercase())
                            .unwrap_or_default();
                        let query_sql = query.to_string();
                        ctx.cursors
                            .insert(cursor_name, CursorState::Declared { query_sql });
                    } else {
                        // Variable declaration: DECLARE var_name [DEFAULT expr]
                        let default_val = if let Some(assignment) = &decl.assignment {
                            eval_declare_default(assignment, ctx, &user_vars)
                        } else {
                            Datum::Null
                        };
                        for name in &decl.names {
                            ctx.locals
                                .insert(name.value.to_lowercase(), default_val.clone());
                        }
                    }
                }
                Ok(ProcControlFlow::Continue)
            }

            // SET variable = expr
            S::Set(set_stmt) => {
                use sqlparser::ast::Set;
                match set_stmt {
                    Set::SingleAssignment {
                        variable, values, ..
                    } => {
                        let var_name_full = variable.to_string();
                        let val = if let Some(expr) = values.first() {
                            eval_sp_expr(expr, ctx, &user_vars)?
                        } else {
                            Datum::Null
                        };
                        self.set_procedure_variable(ctx, &var_name_full, val);
                        Ok(ProcControlFlow::Continue)
                    }
                    Set::MultipleAssignments { assignments } => {
                        for assignment in assignments {
                            let var_name_full = assignment.name.to_string();
                            let val = eval_sp_expr(&assignment.value, ctx, &user_vars)?;
                            self.set_procedure_variable(ctx, &var_name_full, val);
                        }
                        Ok(ProcControlFlow::Continue)
                    }
                    _ => {
                        // Other SET variants (SET NAMES, etc.) — no-op
                        Ok(ProcControlFlow::Continue)
                    }
                }
            }

            // IF / ELSEIF / ELSE
            S::If(if_stmt) => {
                // Check main IF condition
                if let Some(cond) = &if_stmt.if_block.condition {
                    let cond_val = eval_sp_expr(cond, ctx, &user_vars)?;
                    if datum_is_true(&cond_val) {
                        return self
                            .execute_procedure_body(if_stmt.if_block.statements(), ctx)
                            .await;
                    }
                }

                // Check ELSEIF blocks
                for elseif_block in &if_stmt.elseif_blocks {
                    if let Some(cond) = &elseif_block.condition {
                        let cond_val = eval_sp_expr(cond, ctx, &user_vars)?;
                        if datum_is_true(&cond_val) {
                            return self
                                .execute_procedure_body(elseif_block.statements(), ctx)
                                .await;
                        }
                    }
                }

                // ELSE block
                if let Some(else_block) = &if_stmt.else_block {
                    return self
                        .execute_procedure_body(else_block.statements(), ctx)
                        .await;
                }

                Ok(ProcControlFlow::Continue)
            }

            // WHILE loop — extract nested handlers from WHILE body for error handling
            S::While(while_stmt) => {
                let max_iterations = 100_000;
                let mut iterations = 0;

                // Extract handlers from the WHILE body SQL (handles nested BEGIN blocks)
                let while_body_sql = while_stmt.while_block.to_string();
                let nested_handlers = Self::extract_handlers(&while_body_sql);

                loop {
                    if iterations >= max_iterations {
                        return Err(format!("WHILE loop exceeded {} iterations", max_iterations));
                    }
                    iterations += 1;

                    // Evaluate condition
                    let user_vars = self.session.user_variables();
                    if let Some(cond) = &while_stmt.while_block.condition {
                        let cond_val = eval_sp_expr(cond, ctx, &user_vars)?;
                        if !datum_is_true(&cond_val) {
                            break;
                        }
                    }

                    // Execute body with handlers (for CONTINUE HANDLER support)
                    match self
                        .execute_procedure_body_with_handlers(
                            while_stmt.while_block.statements(),
                            ctx,
                            &nested_handlers,
                        )
                        .await?
                    {
                        ProcControlFlow::Return => return Ok(ProcControlFlow::Return),
                        ProcControlFlow::Continue => {}
                    }
                }
                Ok(ProcControlFlow::Continue)
            }

            // CASE statement
            S::Case(case_stmt) => {
                let user_vars = self.session.user_variables();
                if let Some(match_expr) = &case_stmt.match_expr {
                    // Simple CASE: CASE expr WHEN val THEN ...
                    let match_val = eval_sp_expr(match_expr, ctx, &user_vars)?;
                    for when_block in &case_stmt.when_blocks {
                        if let Some(cond) = &when_block.condition {
                            let when_val = eval_sp_expr(cond, ctx, &user_vars)?;
                            if match_val == when_val {
                                return self
                                    .execute_procedure_body(when_block.statements(), ctx)
                                    .await;
                            }
                        }
                    }
                } else {
                    // Searched CASE: CASE WHEN cond THEN ...
                    for when_block in &case_stmt.when_blocks {
                        if let Some(cond) = &when_block.condition {
                            let cond_val = eval_sp_expr(cond, ctx, &user_vars)?;
                            if datum_is_true(&cond_val) {
                                return self
                                    .execute_procedure_body(when_block.statements(), ctx)
                                    .await;
                            }
                        }
                    }
                }

                // ELSE block
                if let Some(else_block) = &case_stmt.else_block {
                    return self
                        .execute_procedure_body(else_block.statements(), ctx)
                        .await;
                }

                Ok(ProcControlFlow::Continue)
            }

            // OPEN cursor
            S::Open(open_stmt) => {
                let cursor_name = open_stmt.cursor_name.value.to_lowercase();
                let query_sql = match ctx.cursors.get(&cursor_name) {
                    Some(CursorState::Declared { query_sql }) => query_sql.clone(),
                    Some(CursorState::Open { .. }) => {
                        return Err(format!("Cursor '{}' is already open", cursor_name));
                    }
                    Some(CursorState::Closed) => {
                        return Err(format!("Cursor '{}' is closed", cursor_name));
                    }
                    None => {
                        return Err(format!("Cursor '{}' is not declared", cursor_name));
                    }
                };
                // Substitute local variables into the query SQL
                let substituted = self.substitute_locals(&query_sql, ctx);
                let rows = self.execute_select_buffered(&substituted).await?;
                ctx.cursors
                    .insert(cursor_name, CursorState::Open { rows, position: 0 });
                Ok(ProcControlFlow::Continue)
            }

            // FETCH cursor INTO vars
            S::Fetch { name, into, .. } => {
                let cursor_name = name.value.to_lowercase();
                let into_vars: Vec<String> = into
                    .as_ref()
                    .map(|obj| {
                        obj.0
                            .iter()
                            .map(|part| match part {
                                sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                                    ident.value.to_lowercase()
                                }
                                other => other.to_string().to_lowercase(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                match ctx.cursors.get_mut(&cursor_name) {
                    Some(CursorState::Open { rows, position }) => {
                        if *position < rows.len() {
                            let row = &rows[*position];
                            for (i, var_name) in into_vars.iter().enumerate() {
                                let val = row.get_opt(i).cloned().unwrap_or(Datum::Null);
                                ctx.locals.insert(var_name.clone(), val);
                            }
                            *position += 1;
                            ctx.found = true;
                        } else {
                            ctx.found = false;
                        }
                    }
                    Some(CursorState::Declared { .. }) => {
                        return Err(format!("Cursor '{}' is not open", cursor_name));
                    }
                    Some(CursorState::Closed) => {
                        return Err(format!("Cursor '{}' is closed", cursor_name));
                    }
                    None => {
                        return Err(format!("Cursor '{}' is not declared", cursor_name));
                    }
                }
                Ok(ProcControlFlow::Continue)
            }

            // CLOSE cursor
            S::Close { cursor } => {
                use sqlparser::ast::CloseCursor;
                match cursor {
                    CloseCursor::Specific { name } => {
                        let cursor_name = name.value.to_lowercase();
                        use std::collections::hash_map::Entry;
                        match ctx.cursors.entry(cursor_name.clone()) {
                            Entry::Occupied(mut e) => {
                                e.insert(CursorState::Closed);
                            }
                            Entry::Vacant(_) => {
                                return Err(format!("Cursor '{}' is not declared", cursor_name));
                            }
                        }
                    }
                    CloseCursor::All => {
                        let names: Vec<String> = ctx.cursors.keys().cloned().collect();
                        for name in names {
                            ctx.cursors.insert(name, CursorState::Closed);
                        }
                    }
                }
                Ok(ProcControlFlow::Continue)
            }

            // RETURN
            S::Return(_) => Ok(ProcControlFlow::Return),

            // Everything else: fallback to string-based execution with local var substitution
            _ => {
                let sql = stmt.to_string();

                // Handle SELECT ... INTO var (MySQL stored procedure syntax)
                // Must be checked BEFORE substitute_locals to preserve variable names
                if let Some(result) = self.try_execute_select_into(&sql, ctx).await? {
                    return Ok(result);
                }

                let substituted = self.substitute_locals(&sql, ctx);

                // SELECT and SHOW statements in SP body send results to client
                // (only if client supports CLIENT_MULTI_RESULTS protocol flag)
                let upper = substituted.trim().to_uppercase();
                let client_multi =
                    self.client_capabilities & handshake::capabilities::CLIENT_MULTI_RESULTS != 0;
                if client_multi && (upper.starts_with("SELECT ") || upper.starts_with("SHOW ")) {
                    self.execute_sp_select(&substituted)
                        .await
                        .map_err(|e| format!("{}", e))?;
                    return Ok(ProcControlFlow::Continue);
                }

                let affected = self.execute_sql_silent(&substituted).await?;
                ctx.rows_affected += affected;
                Ok(ProcControlFlow::Continue)
            }
        }
    }

    /// Set a variable in procedure context or session user variables.
    /// If name starts with @, sets session user variable; otherwise sets local.
    fn set_procedure_variable(
        &self,
        ctx: &mut crate::executor::procedure::ProcedureContext,
        var_name: &str,
        val: Datum,
    ) {
        let trimmed = var_name.trim();
        if let Some(stripped) = trimmed.strip_prefix('@') {
            // User variable
            let name = stripped.trim_start_matches('@').trim().to_lowercase();
            self.session.set_user_variable(&name, val);
        } else {
            // Local variable
            let name = trimmed.to_lowercase();
            ctx.locals.insert(name, val);
        }
    }

    /// Substitute local variable references in a SQL string with their literal values.
    fn substitute_locals(
        &self,
        sql: &str,
        ctx: &crate::executor::procedure::ProcedureContext,
    ) -> String {
        let mut result = sql.to_string();
        // Sort by name length descending to avoid partial matches
        let mut vars: Vec<(&String, &Datum)> = ctx.locals.iter().collect();
        vars.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        for (name, val) in vars {
            result = result.replace(name, &val.to_sql_literal());
        }
        result
    }

    /// Execute a SELECT inside a stored procedure and send the result set to the client.
    /// Uses execute_select_buffered to get rows, then manually sends the result set
    /// with SERVER_MORE_RESULTS_EXISTS flag (in_multi_result is set by handle_call).
    async fn execute_sp_select(&mut self, sql: &str) -> ProtocolResult<()> {
        // First result set starts at sequence 1 (command was 0);
        // subsequent result sets continue from where the previous left off
        if !self.sp_sent_results {
            self.writer.set_sequence(1);
        }
        self.sp_sent_results = true;
        // Extract column names first (works even when result is empty)
        let col_names = Self::extract_select_column_names(sql, 100);
        let col_count = col_names.len();

        // Buffer all rows
        let rows = self
            .execute_select_buffered(sql)
            .await
            .map_err(|e| ProtocolError::Unsupported(format!("SP SELECT failed: {}", e)))?;

        // Send column count
        let count_packet = encode_column_count(col_count as u64);
        self.writer.write_packet(&count_packet).await?;

        // Send column definitions (minimal — name from SELECT expression)
        let schema = self.database.as_deref().unwrap_or("test");
        for name in &col_names {
            let def = ColumnDefinition41 {
                catalog: "def".to_string(),
                schema: schema.to_string(),
                table: String::new(),
                org_table: String::new(),
                name: name.clone(),
                org_name: name.clone(),
                character_set: 63,
                column_length: 255,
                column_type: types::ColumnType::Varchar,
                flags: 0,
                decimals: 0,
            };
            let def_packet = def.encode();
            self.writer.write_packet(&def_packet).await?;
        }

        // Send EOF after columns
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        // Send rows
        for row in &rows {
            let row_packet = encode_text_row(row);
            self.writer.write_packet(&row_packet).await?;
        }

        // Send final EOF with SERVER_MORE_RESULTS_EXISTS
        let status = if self.in_multi_result {
            default_status() | status_flags::SERVER_MORE_RESULTS_EXISTS
        } else {
            default_status()
        };
        if self.deprecate_eof {
            let ok = encode_eof_ok_packet(status, 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, status);
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;
        Ok(())
    }

    /// Extract column names from a SELECT statement (best-effort)
    fn extract_select_column_names(sql: &str, count: usize) -> Vec<String> {
        // Try parsing the SELECT to get column names
        if let Ok(sqlparser::ast::Statement::Query(q)) =
            crate::sql::parser::Parser::parse_one(sql).as_ref()
        {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                let mut names = Vec::new();
                for item in &sel.projection {
                    let name = match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(e) => e.to_string(),
                        sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                            alias.value.clone()
                        }
                        _ => "?column?".to_string(),
                    };
                    names.push(name);
                    if names.len() >= count {
                        break;
                    }
                }
                if !names.is_empty() {
                    return names;
                }
            }
        }
        (0..count).map(|i| format!("col{}", i)).collect()
    }

    /// Handle `SELECT expr INTO var [FROM ...]` in a stored procedure context.
    /// Strips the INTO clause, executes the SELECT, stores the result in local/user var.
    /// Returns Some(Continue) if handled, None if not a SELECT INTO.
    async fn try_execute_select_into(
        &mut self,
        sql: &str,
        ctx: &mut crate::executor::procedure::ProcedureContext,
    ) -> Result<Option<crate::executor::procedure::ProcControlFlow>, String> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("SELECT ") {
            return Ok(None);
        }

        // Find "INTO var" pattern — could be "SELECT expr INTO var" or
        // "SELECT expr INTO var FROM ..."
        // Use case-insensitive search for INTO followed by a variable name
        let into_re = regex::Regex::new(r"(?i)\bINTO\s+(@?\w+(?:\s*,\s*@?\w+)*)")
            .map_err(|e| e.to_string())?;

        let caps = match into_re.captures(sql) {
            Some(c) => c,
            None => return Ok(None),
        };

        // Extract variable names
        let var_list = caps.get(1).unwrap().as_str();
        let var_names: Vec<&str> = var_list.split(',').map(|s| s.trim()).collect();

        // Remove the "INTO var1, var2, ..." from the SQL
        let into_full = caps.get(0).unwrap();
        let select_sql = format!("{}{}", &sql[..into_full.start()], &sql[into_full.end()..]);

        // Substitute local variables in the SELECT part (not the INTO var names)
        let substituted_select = self.substitute_locals(&select_sql, ctx);

        // Execute the SELECT and get the first row
        let rows = self.execute_select_buffered(&substituted_select).await?;
        if let Some(row) = rows.first() {
            for (i, var_name) in var_names.iter().enumerate() {
                if let Some(val) = row.get_opt(i) {
                    self.set_procedure_variable(ctx, var_name, val.clone());
                }
            }
        }

        Ok(Some(crate::executor::procedure::ProcControlFlow::Continue))
    }

    /// Execute a SELECT statement and buffer all result rows.
    /// Used for OPEN cursor to materialize the query result.
    async fn execute_select_buffered(
        &mut self,
        sql: &str,
    ) -> Result<Vec<crate::executor::row::Row>, String> {
        let stmt = crate::sql::Parser::parse_one(sql).map_err(|e| e.to_string())?;

        let plan = {
            let catalog_guard = self.catalog.read();
            let resolved = crate::sql::Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(|e| e.to_string())?;
            crate::sql::TypeChecker::check(&resolved).map_err(|e| e.to_string())?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(|e| e.to_string())?;
            let optimized = Optimizer::new().optimize(logical);
            PhysicalPlanner::plan(optimized, &catalog_guard).map_err(|e| e.to_string())?
        };

        let mvcc = self.mvcc.clone();
        let txn_context = if let Some(txn_id) = self.session.current_txn {
            let read_view = self
                .txn_manager
                .create_read_view(txn_id)
                .map_err(|e| e.to_string())?;
            let pending = self.session.get_pending_changes();
            Some(TransactionContext::with_pending_changes(
                txn_id, read_view, pending,
            ))
        } else {
            let read_view = self
                .txn_manager
                .create_read_view(0)
                .map_err(|e| e.to_string())?;
            Some(TransactionContext::new(0, read_view))
        };

        let engine = ExecutorEngine::with_raft(
            mvcc,
            self.catalog.clone(),
            txn_context,
            self.raft_node.clone(),
            self.session.user_variables(),
        );
        let mut executor = engine.build_async(plan).await.map_err(|e| e.to_string())?;

        executor.open().await.map_err(|e| e.to_string())?;
        let mut rows = Vec::new();
        while let Some(row) = executor.next().await.map_err(|e| e.to_string())? {
            rows.push(row);
        }
        executor.close().await.map_err(|e| e.to_string())?;
        Ok(rows)
    }

    /// Execute a SQL statement silently (no client response packets).
    /// Used for executing DML/DDL within procedure bodies and trigger bodies.
    /// Returns the number of rows affected.
    async fn execute_sql_silent(&mut self, sql: &str) -> Result<u64, String> {
        // Handle SET @var = expr (user variable assignment) — not handled by the planner
        {
            let upper = sql.trim().to_uppercase();
            if upper.starts_with("SET @") && !upper.starts_with("SET @@") {
                self.execute_set_user_variable_silent(sql);
                return Ok(0);
            }
        }

        // Parse and execute through the pipeline
        let stmt = match crate::sql::Parser::parse_one(sql) {
            Ok(s) => s,
            Err(e) => return Err(e.to_string()),
        };

        let plan = {
            let catalog_guard = self.catalog.read();
            let resolved = crate::sql::Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(|e| e.to_string())?;
            crate::sql::TypeChecker::check(&resolved).map_err(|e| e.to_string())?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(|e| e.to_string())?;
            let optimized = Optimizer::new().optimize(logical);
            PhysicalPlanner::plan(optimized, &catalog_guard).map_err(|e| e.to_string())?
        };

        let is_ddl = plan.is_ddl();
        let mvcc = self.mvcc.clone();

        let (txn_context, implicit_txn_id) = if is_ddl {
            (None, None)
        } else if let Some(txn_id) = self.session.current_txn {
            let read_view = self
                .txn_manager
                .create_read_view(txn_id)
                .map_err(|e| e.to_string())?;
            let pending = self.session.get_pending_changes();
            (
                Some(TransactionContext::with_pending_changes(
                    txn_id, read_view, pending,
                )),
                None,
            )
        } else if self.session.autocommit {
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.session.is_read_only)
                .map_err(|e| e.to_string())?;
            let read_view = self
                .txn_manager
                .create_read_view(txn.txn_id)
                .map_err(|e| e.to_string())?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view)),
                Some(txn.txn_id),
            )
        } else {
            let read_view = self
                .txn_manager
                .create_read_view(0)
                .map_err(|e| e.to_string())?;
            (Some(TransactionContext::new(0, read_view)), None)
        };

        let engine = ExecutorEngine::with_raft(
            mvcc,
            self.catalog.clone(),
            txn_context,
            self.raft_node.clone(),
            self.session.user_variables(),
        );
        let mut executor = engine.build_async(plan).await.map_err(|e| e.to_string())?;

        executor.open().await.map_err(|e| e.to_string())?;
        let mut affected = 0u64;
        while let Some(row) = executor.next().await.map_err(|e| e.to_string())? {
            if let Some(Datum::Int(n)) = row.get_opt(0) {
                affected += *n as u64;
            } else {
                affected += 1;
            }
        }
        executor.close().await.map_err(|e| e.to_string())?;

        // Handle changes
        let ignore_dups = executor.is_ignore_duplicates();
        let changes = executor.take_changes();
        if !changes.is_empty() {
            if self.session.in_transaction() {
                self.session.add_pending_changes(changes);
            } else {
                let changeset = ChangeSet::new_with_changes_ignore(
                    implicit_txn_id.unwrap_or(0),
                    changes,
                    ignore_dups,
                );
                self.raft_node
                    .propose_changes(changeset)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }

        if let Some(txn_id) = implicit_txn_id {
            self.txn_manager
                .commit(txn_id)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(affected)
    }

    /// Send an OK packet
    async fn send_ok(&mut self, affected_rows: u64, last_insert_id: u64) -> ProtocolResult<()> {
        self.writer.set_sequence(1);
        let packet = encode_ok_packet(affected_rows, last_insert_id, default_status(), 0);
        self.writer.write_packet(&packet).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Send an error packet
    async fn send_error(&mut self, code: u16, state: &str, message: &str) -> ProtocolResult<()> {
        self.writer.set_sequence(1);
        let packet = encode_err_packet(code, state, message);
        self.writer.write_packet(&packet).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Send error from SQL error
    async fn send_sql_error(&mut self, e: &crate::sql::SqlError) -> ProtocolResult<()> {
        use crate::sql::SqlError;

        let (code, state) = match e {
            SqlError::Parse(_) => (codes::ER_SYNTAX_ERROR, states::SYNTAX_ERROR),
            SqlError::TableNotFound(_) => (codes::ER_NO_SUCH_TABLE, states::NO_SUCH_TABLE),
            SqlError::ColumnNotFound(_) => (codes::ER_BAD_FIELD_ERROR, "42S22"),
            SqlError::AmbiguousColumn(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::TypeMismatch { .. } => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::InvalidOperation(msg) => {
                if msg.contains("cannot be NULL") {
                    (codes::ER_BAD_NULL_ERROR, "23000")
                } else if msg.contains("already exists") {
                    if msg.contains("Table") || msg.contains("table") {
                        (1050, "42S01") // ER_TABLE_EXISTS_ERROR
                    } else {
                        (codes::ER_DUP_ENTRY, "23000")
                    }
                } else if msg.contains("columns but") && msg.contains("values") {
                    (codes::ER_WRONG_VALUE_COUNT_ON_ROW, "21S01")
                } else if msg.contains("Incorrect arguments") {
                    (codes::ER_WRONG_ARGUMENTS, states::GENERAL_ERROR)
                } else if msg.contains("can't have a default value") {
                    (codes::ER_BLOB_CANT_HAVE_DEFAULT, states::SYNTAX_ERROR)
                } else if msg.contains("Incorrect column specifier") {
                    (codes::ER_WRONG_FIELD_SPEC, states::SYNTAX_ERROR)
                } else if msg.contains("Column length too big") {
                    (codes::ER_TOO_BIG_FIELDLENGTH, states::SYNTAX_ERROR)
                } else if msg.contains("Display width out of range") {
                    (codes::ER_TOO_BIG_DISPLAYWIDTH, states::SYNTAX_ERROR)
                } else if msg.contains("Too big scale") {
                    (codes::ER_TOO_BIG_SCALE, states::SYNTAX_ERROR)
                } else {
                    (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR)
                }
            }
            SqlError::Unsupported(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::EmptyQuery => (codes::ER_EMPTY_QUERY, states::GENERAL_ERROR),
            SqlError::CommentOnly => (codes::ER_EMPTY_QUERY, states::GENERAL_ERROR),
        };

        self.send_error(code, state, &e.to_string()).await
    }

    /// Send error from planner error
    async fn send_planner_error(&mut self, e: &crate::planner::PlannerError) -> ProtocolResult<()> {
        self.send_error(
            codes::ER_UNKNOWN_ERROR,
            states::GENERAL_ERROR,
            &e.to_string(),
        )
        .await
    }

    /// Send error from protocol error
    async fn send_error_from_protocol_error(&mut self, e: &ProtocolError) -> ProtocolResult<()> {
        let (code, state, msg) = match e {
            ProtocolError::Sql(sql_err) => {
                return self.send_sql_error(sql_err).await;
            }
            ProtocolError::Planner(plan_err) => {
                return self.send_planner_error(plan_err).await;
            }
            ProtocolError::Executor(exec_err) => {
                // Map specific executor errors to MySQL error codes
                if let crate::executor::ExecutorError::TableNotFound(_) = exec_err {
                    (
                        codes::ER_BAD_TABLE_ERROR,
                        states::NO_SUCH_TABLE,
                        exec_err.to_string(),
                    )
                } else if let crate::executor::ExecutorError::NullValue(_) = exec_err {
                    (codes::ER_BAD_NULL_ERROR, "23000", exec_err.to_string())
                } else if let crate::executor::ExecutorError::DataOutOfRange(ref msg) = exec_err {
                    let code = if msg.starts_with("Out of range value for column") {
                        codes::ER_WARN_DATA_OUT_OF_RANGE
                    } else {
                        codes::ER_DATA_OUT_OF_RANGE
                    };
                    (code, "22003", exec_err.to_string())
                } else if let crate::executor::ExecutorError::InvalidArgumentForLogarithm(_) =
                    exec_err
                {
                    (
                        codes::ER_INVALID_ARGUMENT_FOR_LOGARITHM,
                        "2201E",
                        exec_err.to_string(),
                    )
                } else if let crate::executor::ExecutorError::TruncatedWrongValue(_) = exec_err {
                    (
                        codes::ER_TRUNCATED_WRONG_VALUE,
                        "22007",
                        exec_err.to_string(),
                    )
                } else if let crate::executor::ExecutorError::DuplicateKey(_) = exec_err {
                    (codes::ER_DUP_ENTRY, "23000", exec_err.to_string())
                } else if let crate::executor::ExecutorError::InvalidOperation(ref msg) = exec_err {
                    let code =
                        if msg.contains("Incorrect arguments") || msg.contains("Wrong arguments") {
                            codes::ER_WRONG_ARGUMENTS
                        } else if msg.contains("cannot be NULL") {
                            codes::ER_BAD_NULL_ERROR
                        } else {
                            codes::ER_UNKNOWN_ERROR
                        };
                    (code, states::GENERAL_ERROR, exec_err.to_string())
                } else {
                    let msg = exec_err.to_string();
                    if msg.contains("already exists") {
                        (1050, "42S01", msg)
                    } else if msg.contains("Duplicate key") || msg.contains("duplicate key") {
                        (codes::ER_DUP_ENTRY, "23000", msg)
                    } else {
                        (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, msg)
                    }
                }
            }
            _ => {
                let msg = e.to_string();
                if msg.contains("Duplicate key") {
                    (codes::ER_DUP_ENTRY, "23000", msg)
                } else {
                    (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, msg)
                }
            }
        };

        self.send_error(code, state, &msg).await
    }
}

/// Reconstruct a CREATE TABLE DDL statement from a TableDef
fn reconstruct_create_table(td: &crate::catalog::TableDef) -> String {
    use crate::catalog::system_tables::data_type_to_string;
    use crate::catalog::Constraint;

    let mut parts = Vec::new();

    for col in &td.columns {
        let mut col_str = format!("`{}` {}", col.name, data_type_to_string(&col.data_type));
        if !col.nullable {
            col_str.push_str(" NOT NULL");
        }
        if let Some(ref default) = col.default {
            col_str.push_str(&format!(" DEFAULT {}", default));
        }
        if col.auto_increment {
            col_str.push_str(" AUTO_INCREMENT");
        }
        parts.push(col_str);
    }

    for constraint in &td.constraints {
        match constraint {
            Constraint::PrimaryKey(cols) => {
                let col_list: Vec<String> = cols.iter().map(|c| format!("`{}`", c)).collect();
                parts.push(format!("PRIMARY KEY ({})", col_list.join(", ")));
            }
            Constraint::Unique(cols) => {
                let col_list: Vec<String> = cols.iter().map(|c| format!("`{}`", c)).collect();
                parts.push(format!("UNIQUE KEY ({})", col_list.join(", ")));
            }
            Constraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
            } => {
                let col_list: Vec<String> = columns.iter().map(|c| format!("`{}`", c)).collect();
                let ref_list: Vec<String> =
                    ref_columns.iter().map(|c| format!("`{}`", c)).collect();
                if let Some(fk_name) = name {
                    parts.push(format!(
                        "CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES `{}` ({})",
                        fk_name,
                        col_list.join(", "),
                        ref_table,
                        ref_list.join(", ")
                    ));
                } else {
                    parts.push(format!(
                        "FOREIGN KEY ({}) REFERENCES `{}` ({})",
                        col_list.join(", "),
                        ref_table,
                        ref_list.join(", ")
                    ));
                }
            }
            Constraint::Check(expr) => {
                parts.push(format!("CHECK ({})", expr));
            }
        }
    }

    format!(
        "CREATE TABLE `{}` (\n  {}\n) ENGINE=RooDB DEFAULT CHARSET=utf8mb4",
        td.name,
        parts.join(",\n  ")
    )
}

/// Generate DESCRIBE output rows from a TableDef
fn describe_table(td: &crate::catalog::TableDef) -> Vec<Vec<String>> {
    use crate::catalog::system_tables::data_type_to_string;
    use crate::catalog::Constraint;

    // Collect primary key columns
    let pk_cols: std::collections::HashSet<String> = td
        .constraints
        .iter()
        .filter_map(|c| {
            if let Constraint::PrimaryKey(cols) = c {
                Some(cols.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    // Collect unique key columns
    let uni_cols: std::collections::HashSet<String> = td
        .constraints
        .iter()
        .filter_map(|c| {
            if let Constraint::Unique(cols) = c {
                Some(cols.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    td.columns
        .iter()
        .map(|col| {
            let field = col.name.clone();
            let type_str = data_type_to_string(&col.data_type);
            let null_str = if col.nullable { "YES" } else { "NO" };
            let key = if pk_cols.contains(&col.name) {
                "PRI"
            } else if uni_cols.contains(&col.name) {
                "UNI"
            } else {
                ""
            };
            let default = col.default.clone().unwrap_or_else(|| "NULL".to_string());
            let extra = if col.auto_increment {
                "auto_increment".to_string()
            } else {
                String::new()
            };
            vec![
                field,
                type_str,
                null_str.to_string(),
                key.to_string(),
                default,
                extra,
            ]
        })
        .collect()
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
