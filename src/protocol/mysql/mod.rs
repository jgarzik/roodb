//! MySQL protocol implementation
//!
//! Implements the MySQL wire protocol over TLS connections.

pub mod auth;
pub mod command;
pub mod error;
pub mod handshake;
pub mod packet;
pub mod prepared;
pub mod resultset;
pub mod types;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::executor::engine::ExecutorEngine;
use crate::executor::Executor;
use crate::planner::logical::builder::LogicalPlanBuilder;
use crate::planner::optimizer::Optimizer;
use crate::planner::physical::planner::PhysicalPlanner;
use crate::planner::physical::PhysicalPlan;
use crate::sql::{Parser, Resolver, TypeChecker};
use crate::storage::StorageEngine;

use self::auth::{verify_mysql_native_password, HandshakeResponse41};
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

/// MySQL connection handler
pub struct MySqlConnection<S>
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
}

impl<S> MySqlConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Create a new MySQL connection from a TLS stream
    pub fn new(
        stream: S,
        connection_id: u32,
        storage: Arc<dyn StorageEngine>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        MySqlConnection {
            reader: PacketReader::new(read_half),
            writer: PacketWriter::new(write_half),
            connection_id,
            scramble: [0u8; 20],
            client_capabilities: 0,
            database: None,
            storage,
            catalog,
            deprecate_eof: false,
        }
    }

    /// Perform the MySQL handshake
    pub async fn handshake(&mut self) -> ProtocolResult<()> {
        info!(connection_id = self.connection_id, "Starting MySQL handshake");

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
        let auth_plugin = response
            .auth_plugin_name
            .as_deref()
            .unwrap_or(AUTH_PLUGIN_NAME);

        if auth_plugin != AUTH_PLUGIN_NAME {
            warn!(plugin = auth_plugin, "Unsupported auth plugin");
            return self.send_auth_error("Unsupported authentication plugin").await;
        }

        // Check username and password
        if response.username != ROOT_USER {
            warn!(username = %response.username, "Unknown user");
            return self.send_auth_error("Access denied").await;
        }

        if !verify_mysql_native_password(&self.scramble, ROOT_PASSWORD, &response.auth_response) {
            warn!("Password verification failed");
            return self.send_auth_error("Access denied").await;
        }

        // Store database if provided
        if let Some(db) = response.database {
            self.database = Some(db);
        }

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

    /// Run the command loop
    pub async fn run(&mut self) -> ProtocolResult<()> {
        loop {
            // Reset sequence for each command
            self.reader.reset_sequence();
            self.writer.reset_sequence();

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
                Ok(true) => continue,   // Continue command loop
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
                Ok(true)
            }

            ParsedCommand::StmtPrepare(_) | ParsedCommand::StmtExecute { .. } | ParsedCommand::StmtClose(_) => {
                let err = unsupported_prepared_stmt_error();
                self.writer.set_sequence(1);
                self.writer.write_packet(&err).await?;
                self.writer.flush().await?;
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
        let physical = {
            let catalog_guard = self.catalog.read();

            let resolved = match Resolver::new(&catalog_guard).resolve(stmt) {
                Ok(r) => r,
                Err(e) => {
                    drop(catalog_guard);
                    return self.send_sql_error(&e).await;
                }
            };

            // Type check
            if let Err(e) = TypeChecker::check(&resolved) {
                drop(catalog_guard);
                return self.send_sql_error(&e).await;
            }

            // Build logical plan
            let logical = match LogicalPlanBuilder::build(resolved) {
                Ok(p) => p,
                Err(e) => {
                    drop(catalog_guard);
                    return self.send_planner_error(&e).await;
                }
            };

            // Optimize
            let optimized = Optimizer::new().optimize(logical);

            // Build physical plan
            match PhysicalPlanner::plan(optimized, &catalog_guard) {
                Ok(p) => p,
                Err(e) => {
                    drop(catalog_guard);
                    return self.send_planner_error(&e).await;
                }
            }
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

        if returns_rows {
            // Get column definitions before building executor
            let columns = plan.output_columns();

            // Build and execute
            let engine = ExecutorEngine::new(self.storage.clone(), self.catalog.clone());
            let mut executor = engine.build(plan)?;

            self.send_result_set(&columns, &mut *executor).await
        } else {
            // DML/DDL - execute and count affected rows
            let engine = ExecutorEngine::new(self.storage.clone(), self.catalog.clone());
            let mut executor = engine.build(plan)?;

            executor.open().await?;
            let mut affected = 0u64;
            while executor.next().await?.is_some() {
                affected += 1;
            }
            executor.close().await?;

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
        self.send_error(codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, &e.to_string())
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
                (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, exec_err.to_string())
            }
            _ => (codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, e.to_string()),
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
