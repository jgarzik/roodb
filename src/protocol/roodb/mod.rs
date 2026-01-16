//! RooDB client protocol implementation
//!
//! Implements the RooDB client wire protocol with STARTTLS support.

pub mod auth;
pub mod command;
pub mod error;
pub mod handshake;
pub mod packet;
pub mod prepared;
pub mod resultset;
pub mod starttls;
pub mod types;

// Re-export status flags for use by other modules
pub use handshake::status_flags;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::executor::engine::ExecutorEngine;
use crate::executor::{Executor, TransactionContext};
use crate::planner::logical::builder::LogicalPlanBuilder;
use crate::planner::optimizer::Optimizer;
use crate::planner::physical::{PhysicalPlan, PhysicalPlanner};
use crate::raft::{ChangeSet, RaftNode};
use crate::server::session::Session;
use crate::sql::{Parser, Resolver, TypeChecker};
use crate::storage::StorageEngine;
use crate::txn::{IsolationLevel, MvccStorage, TransactionManager};

use self::auth::{verify_native_password, HandshakeResponse41};
use self::command::{parse_command, ParsedCommand};
use self::error::{codes, states, ProtocolError, ProtocolResult};
use self::handshake::{capabilities, HandshakeV10, AUTH_PLUGIN_NAME};
use self::packet::{PacketReader, PacketWriter};
use self::prepared::unsupported_prepared_stmt_error;
use self::resultset::{
    default_status, encode_column_count, encode_eof_packet, encode_err_packet, encode_ok_packet,
    encode_text_row, ColumnDefinition41,
};

/// Hardcoded credentials for MVP
const ROOT_USER: &str = "root";
const ROOT_PASSWORD: &str = "";

/// Internal enum for planning errors (used to avoid holding guard across await)
enum PlanError {
    Sql(crate::sql::SqlError),
    Planner(crate::planner::PlannerError),
}

/// RooDB connection handler
pub struct RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Packet reader
    reader: PacketReader<ReadHalf<S>>,
    /// Packet writer
    writer: PacketWriter<WriteHalf<S>>,
    /// Connection ID
    connection_id: u32,
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
}

impl<S> RooDbConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Create a new RooDB connection from a TLS stream
    pub fn new(
        stream: S,
        connection_id: u32,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        RooDbConnection {
            reader: PacketReader::new(read_half),
            writer: PacketWriter::new(write_half),
            connection_id,
            scramble: [0u8; 20],
            client_capabilities: 0,
            database: None,
            storage,
            catalog,
            deprecate_eof: false,
            session: Session::new(connection_id),
            txn_manager,
            raft_node,
        }
    }

    /// Create a new RooDB connection with pre-established scramble (for STARTTLS)
    ///
    /// Used after STARTTLS handshake where the scramble was already sent in the
    /// plaintext greeting.
    pub fn new_with_scramble(
        stream: S,
        connection_id: u32,
        scramble: [u8; 20],
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
        txn_manager: Arc<TransactionManager>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        RooDbConnection {
            reader: PacketReader::new(read_half),
            writer: PacketWriter::new(write_half),
            connection_id,
            scramble,
            client_capabilities: 0,
            database: None,
            storage,
            catalog,
            deprecate_eof: false,
            session: Session::new(connection_id),
            txn_manager,
            raft_node,
        }
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

        // Verify authentication
        self.verify_auth(&response).await?;

        // Send OK
        self.writer.set_sequence(3);
        let ok_packet = encode_ok_packet(0, 0, default_status(), 0);
        self.writer.write_packet(&ok_packet).await?;
        self.writer.flush().await?;

        info!(
            connection_id = self.connection_id,
            username = ROOT_USER,
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
            username = ROOT_USER,
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

        if response.username != ROOT_USER {
            warn!(username = %response.username, "Unknown user");
            return self.send_auth_error("Access denied").await;
        }

        if !verify_native_password(&self.scramble, ROOT_PASSWORD, &response.auth_response) {
            warn!("Password verification failed");
            return self.send_auth_error("Access denied").await;
        }

        if let Some(ref db) = response.database {
            self.database = Some(db.clone());
        }

        Ok(())
    }

    /// Run the command loop
    pub async fn run(&mut self) -> ProtocolResult<()> {
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
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            // Parse and handle command
            let cmd = parse_command(&packet)?;

            match self.handle_command(cmd).await {
                Ok(true) => continue,       // Continue command loop
                Ok(false) => return Ok(()), // Client quit
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
                // Accept but don't actually switch (single database for MVP)
                self.database = Some(db);
                self.send_ok(0, 0).await?;
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
                let err = unsupported_prepared_stmt_error();
                self.writer.set_sequence(1);
                self.writer.write_packet(&err).await?;
                self.writer.flush().await?;
                Ok(true)
            }

            ParsedCommand::StmtExecute { statement_id } => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_EXECUTE"
                );
                let err = unsupported_prepared_stmt_error();
                self.writer.set_sequence(1);
                self.writer.write_packet(&err).await?;
                self.writer.flush().await?;
                Ok(true)
            }

            ParsedCommand::StmtClose(statement_id) => {
                debug!(
                    connection_id = self.connection_id,
                    statement_id, "COM_STMT_CLOSE"
                );
                // No response needed for STMT_CLOSE
                Ok(true)
            }

            ParsedCommand::ResetConnection => {
                debug!(connection_id = self.connection_id, "COM_RESET_CONNECTION");
                self.database = None;
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

    /// Handle a SQL query
    async fn handle_query(&mut self, sql: &str) -> ProtocolResult<()> {
        // Check for transaction commands first
        if let Some(()) = self.try_handle_transaction_command(sql).await? {
            return Ok(());
        }

        // Check for system variable queries (@@variable)
        if let Some(result) = self.try_handle_system_variable(sql).await? {
            return Ok(result);
        }

        // Parse
        let stmt = match Parser::parse_one(sql) {
            Ok(s) => s,
            Err(e) => {
                return self
                    .send_error(codes::ER_SYNTAX_ERROR, states::SYNTAX_ERROR, &e.to_string())
                    .await;
            }
        };

        // Resolve, type check, and plan while holding catalog lock
        // Use closure to return Result and ensure guard is dropped before await
        let plan_result: Result<PhysicalPlan, PlanError> = (|| {
            let catalog_guard = self.catalog.read();

            let resolved = Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(PlanError::Sql)?;

            // Type check
            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;

            // Build logical plan
            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;

            // Optimize
            let optimized = Optimizer::new().optimize(logical);

            // Build physical plan
            PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)
        })(); // catalog_guard dropped here

        // Handle errors after guard is dropped
        let physical = match plan_result {
            Ok(p) => p,
            Err(PlanError::Sql(e)) => return self.send_sql_error(&e).await,
            Err(PlanError::Planner(e)) => return self.send_planner_error(&e).await,
        };

        // Execute
        self.execute_plan(physical).await
    }

    /// Execute a physical plan and send results
    async fn execute_plan(&mut self, plan: PhysicalPlan) -> ProtocolResult<()> {
        // Check if this is a query that returns rows
        let returns_rows = matches!(
            plan,
            PhysicalPlan::TableScan { .. }
                | PhysicalPlan::Filter { .. }
                | PhysicalPlan::Project { .. }
                | PhysicalPlan::Sort { .. }
                | PhysicalPlan::Limit { .. }
                | PhysicalPlan::HashDistinct { .. }
                | PhysicalPlan::HashAggregate { .. }
                | PhysicalPlan::NestedLoopJoin { .. }
        );

        // Check if this is DDL (no MVCC needed)
        let is_ddl = matches!(
            plan,
            PhysicalPlan::CreateTable { .. }
                | PhysicalPlan::DropTable { .. }
                | PhysicalPlan::CreateIndex { .. }
                | PhysicalPlan::DropIndex { .. }
        );

        // Create MVCC storage wrapper
        let mvcc = Arc::new(MvccStorage::new(
            self.storage.clone(),
            self.txn_manager.clone(),
        ));

        // Determine transaction context
        let (txn_context, implicit_txn_id) = if is_ddl {
            // DDL operations don't use MVCC
            (None, None)
        } else if let Some(txn_id) = self.session.current_txn {
            // Explicit transaction - use existing txn_id
            let read_view = self.txn_manager.create_read_view(txn_id);
            (Some(TransactionContext::new(txn_id, read_view)), None)
        } else if !returns_rows && self.session.autocommit {
            // Autocommit DML - create implicit transaction
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.session.is_read_only)?;
            let read_view = self.txn_manager.create_read_view(txn.txn_id);
            (
                Some(TransactionContext::new(txn.txn_id, read_view)),
                Some(txn.txn_id),
            )
        } else {
            // Read-only query in autocommit mode - create snapshot read view
            // txn_id=0 special case: sees all committed transactions
            let read_view = self.txn_manager.create_read_view(0);
            (Some(TransactionContext::new(0, read_view)), None)
        };

        if returns_rows {
            // Get column definitions before building executor
            let columns = plan.output_columns();

            // Build and execute
            let engine = ExecutorEngine::new(mvcc, self.catalog.clone(), txn_context);
            let mut executor = engine.build(plan)?;

            self.send_result_set(&columns, &mut *executor).await
        } else {
            // DML/DDL - execute and count affected rows
            let engine = ExecutorEngine::new(mvcc, self.catalog.clone(), txn_context);
            let mut executor = engine.build(plan)?;

            executor.open().await?;
            let mut affected = 0u64;
            while executor.next().await?.is_some() {
                affected += 1;
            }
            executor.close().await?;

            // Collect and propose changes to Raft for replication
            let changes = executor.take_changes();
            if !changes.is_empty() {
                let changeset = ChangeSet::new_with_changes(implicit_txn_id.unwrap_or(0), changes);
                self.raft_node
                    .propose_changes(changeset)
                    .await
                    .map_err(|e| ProtocolError::Raft(e.to_string()))?;
            }

            // Commit implicit transaction if we created one
            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager.commit(txn_id).await?;
            }

            self.send_ok(affected, 0).await
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

        // Send rows
        while let Some(row) = executor.next().await? {
            let row_packet = encode_text_row(&row);
            self.writer.write_packet(&row_packet).await?;
        }

        // Send final EOF/OK
        if self.deprecate_eof {
            let ok = encode_ok_packet(0, 0, default_status(), 0);
            self.writer.write_packet(&ok).await?;
        } else {
            let eof = encode_eof_packet(0, default_status());
            self.writer.write_packet(&eof).await?;
        }

        self.writer.flush().await?;

        // Close executor
        executor.close().await?;

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
                IsolationLevel::Serializable
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
        // COMMIT without BEGIN is a no-op
        self.send_ok_with_status(0, 0).await
    }

    /// Handle ROLLBACK
    async fn handle_rollback(&mut self) -> ProtocolResult<()> {
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

    /// Try to handle system variable queries (SELECT @@variable)
    ///
    /// Returns Some(()) if handled, None if not a system variable query.
    async fn try_handle_system_variable(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        use regex::Regex;

        // Simple pattern matching for SELECT @@variable queries
        let sql_upper = sql.to_uppercase();
        if !sql_upper.contains("@@") {
            return Ok(None);
        }

        // Extract variable names from the query
        lazy_static::lazy_static! {
            static ref VAR_RE: Regex = Regex::new(r"@@(\w+)").unwrap();
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
                "sql_mode" => "STRICT_TRANS_TABLES",
                "time_zone" => "SYSTEM",
                "system_time_zone" => "UTC",
                "transaction_isolation" | "tx_isolation" => "REPEATABLE-READ",
                "autocommit" => "1",
                "version" => "8.0.0-RooDB",
                "version_comment" => "RooDB",
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
            let ok = encode_ok_packet(0, 0, default_status(), 0);
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
            SqlError::ColumnNotFound(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::AmbiguousColumn(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::TypeMismatch { .. } => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::InvalidOperation(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
            SqlError::Unsupported(_) => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR),
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
            ProtocolError::Executor(exec_err) => (
                codes::ER_UNKNOWN_ERROR,
                states::GENERAL_ERROR,
                exec_err.to_string(),
            ),
            _ => (
                codes::ER_UNKNOWN_ERROR,
                states::GENERAL_ERROR,
                e.to_string(),
            ),
        };

        self.send_error(code, state, &msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_credentials() {
        assert_eq!(ROOT_USER, "root");
        assert_eq!(ROOT_PASSWORD, "");
    }
}
