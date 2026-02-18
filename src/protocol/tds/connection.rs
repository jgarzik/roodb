//! TDS connection handler - manages the lifecycle of a single TDS client connection.

use parking_lot::RwLock;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::catalog::system_tables::SYSTEM_USERS;
use crate::catalog::Catalog;
use crate::executor::encoding::{decode_row, table_key_end, table_key_prefix};
use crate::executor::engine::ExecutorEngine;
use crate::executor::{Datum, TransactionContext};
use crate::planner::logical::builder::LogicalPlanBuilder;
use crate::planner::optimizer::Optimizer;
use crate::planner::physical::{PhysicalPlan, PhysicalPlanner};
use crate::raft::{ChangeSet, RaftNode};
use crate::server::session::Session;
use crate::sql::{Parser, Resolver, TypeChecker};
use crate::storage::StorageEngine;
use crate::txn::{IsolationLevel, MvccStorage, TransactionManager};

use super::codec::{PacketError, PacketType, TdsReader, TdsWriter};
use super::login::{self, Login7};
use super::prelogin;
use super::token;
use super::types;

type MessageReceiver = mpsc::Receiver<Result<(PacketType, Vec<u8>), PacketError>>;

/// TDS connection handler
pub struct TdsConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Direct reader for pre-login/login phase (before reader task starts).
    reader: Option<TdsReader<BufReader<ReadHalf<S>>>>,
    /// Channel for receiving messages from the reader task (post-login).
    msg_rx: Option<MessageReceiver>,
    /// Cancellation flag set by the reader task when Attention arrives.
    cancelled: Arc<AtomicBool>,
    writer: TdsWriter<BufWriter<WriteHalf<S>>>,
    connection_id: u32,
    client_ip: IpAddr,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    session: Session,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
    mvcc: Arc<MvccStorage>,
    /// Current transaction descriptor (8-byte value sent in ENVCHANGE)
    transaction_descriptor: u64,
    /// Next transaction descriptor to assign
    next_tran_descriptor: u64,
}

impl<S> TdsConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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

        Self {
            reader: Some(TdsReader::new(BufReader::with_capacity(8192, read_half))),
            msg_rx: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            writer: TdsWriter::new(BufWriter::with_capacity(8192, write_half)),
            connection_id,
            client_ip,
            storage,
            catalog,
            session: Session::new(connection_id),
            txn_manager,
            raft_node,
            mvcc,
            transaction_descriptor: 0,
            next_tran_descriptor: 1,
        }
    }

    /// Run the PRELOGIN + LOGIN7 handshake, then enter the command loop.
    pub async fn run(&mut self) -> Result<(), PacketError> {
        // Phase 1: PRELOGIN (direct reader)
        self.handle_prelogin().await?;

        // Phase 2: LOGIN7 (direct reader)
        let login = self.handle_login7().await?;

        // Phase 3: Authenticate and send login response
        self.authenticate_and_respond(login).await?;

        // Phase 4: Start reader task and enter command loop
        self.start_reader_task();
        self.command_loop().await
    }

    /// Read a message using the direct reader (pre-login/login phase).
    async fn read_direct(&mut self) -> Result<(PacketType, Vec<u8>), PacketError> {
        self.reader
            .as_mut()
            .expect("direct reader not available")
            .read_message()
            .await
    }

    async fn handle_prelogin(&mut self) -> Result<(), PacketError> {
        let (pkt_type, payload) = self.read_direct().await?;
        if pkt_type != PacketType::PreLogin {
            return Err(PacketError::UnexpectedType {
                expected: PacketType::PreLogin,
                got: pkt_type,
            });
        }

        let _prelogin = prelogin::parse_prelogin(&payload);
        debug!(
            connection_id = self.connection_id,
            "Received PRELOGIN from client"
        );

        // Send PRELOGIN response
        let response = prelogin::encode_prelogin_response();
        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await?;

        debug!(connection_id = self.connection_id, "Sent PRELOGIN response");
        Ok(())
    }

    async fn handle_login7(&mut self) -> Result<Login7, PacketError> {
        let (pkt_type, payload) = self.read_direct().await?;
        if pkt_type != PacketType::Tds7Login {
            return Err(PacketError::UnexpectedType {
                expected: PacketType::Tds7Login,
                got: pkt_type,
            });
        }

        let login = login::parse_login7(&payload).map_err(|e| {
            PacketError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;

        debug!(
            connection_id = self.connection_id,
            username = %login.username,
            database = %login.database,
            tds_version = format!("0x{:08X}", login.tds_version),
            "Received LOGIN7"
        );

        Ok(login)
    }

    /// Spawn a dedicated reader task that forwards messages via channel
    /// and signals cancellation on Attention packets.
    fn start_reader_task(&mut self) {
        let mut reader = self.reader.take().expect("reader already taken");
        let cancelled = self.cancelled.clone();
        let conn_id = self.connection_id;

        let (msg_tx, msg_rx) = mpsc::channel(4);
        self.msg_rx = Some(msg_rx);

        tokio::spawn(async move {
            loop {
                match reader.read_message().await {
                    Ok((PacketType::Attention, _)) => {
                        debug!(connection_id = conn_id, "Reader task: received ATTENTION");
                        cancelled.store(true, Ordering::Release);
                        // Don't forward Attention as a message — it's a signal only.
                        // The command handler will check the cancelled flag.
                    }
                    Ok(msg) => {
                        if msg_tx.send(Ok(msg)).await.is_err() {
                            break; // Handler dropped, connection closing
                        }
                    }
                    Err(PacketError::ConnectionClosed) => {
                        let _ = msg_tx.send(Err(PacketError::ConnectionClosed)).await;
                        break;
                    }
                    Err(e) => {
                        let _ = msg_tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
    }

    /// Read the next message from the reader task channel.
    async fn recv_message(&mut self) -> Result<(PacketType, Vec<u8>), PacketError> {
        match self
            .msg_rx
            .as_mut()
            .expect("reader task not started")
            .recv()
            .await
        {
            Some(Ok(msg)) => Ok(msg),
            Some(Err(e)) => Err(e),
            None => Err(PacketError::ConnectionClosed),
        }
    }

    /// Check if cancellation was requested (Attention received).
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Clear the cancellation flag.
    fn clear_cancelled(&self) {
        self.cancelled.store(false, Ordering::Release);
    }

    async fn authenticate_and_respond(&mut self, login: Login7) -> Result<(), PacketError> {
        let client_host = self.client_ip.to_string();

        // Look up user
        let user_info = self
            .lookup_user(&login.username, &client_host)
            .await
            .map_err(|e| PacketError::Io(std::io::Error::other(e)))?;

        // Verify password
        match user_info {
            Some((password_hash, account_locked, password_expired)) => {
                if account_locked {
                    return self.send_login_error("Account is locked").await;
                }
                if password_expired {
                    return self.send_login_error("Password has expired").await;
                }

                // For TDS, the password comes in cleartext (decoded from LOGIN7 obfuscation).
                // Verify it against the stored argon2 hash.
                if !self.verify_password(&login.password, &password_hash) {
                    return self.send_login_error("Login failed").await;
                }
            }
            None => {
                return self
                    .send_login_error(&format!("Login failed for user '{}'", login.username))
                    .await;
            }
        }

        // Set session state
        self.session.set_user(login.username.clone());
        let db = if login.database.is_empty() {
            "default".to_string()
        } else {
            login.database.clone()
        };
        self.session.set_database(Some(db.clone()));

        // Send success response
        let response = token::build_login_response(&db);
        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await?;

        info!(
            connection_id = self.connection_id,
            username = %self.session.user,
            database = %db,
            "TDS login successful"
        );

        Ok(())
    }

    /// Verify a plaintext password against a stored SHA1(SHA1(password)) hex hash.
    fn verify_password(&self, password: &str, stored_hash: &str) -> bool {
        crate::init::verify_password(password, stored_hash)
    }

    async fn send_login_error(&mut self, message: &str) -> Result<(), PacketError> {
        let response = token::build_login_error_response(message);
        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await?;
        Err(PacketError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            message.to_string(),
        )))
    }

    /// Look up user in system.users table (same as MySQL protocol handler)
    async fn lookup_user(
        &self,
        username: &str,
        client_host: &str,
    ) -> Result<Option<(String, bool, bool)>, String> {
        use crate::sql::privileges::HostPattern;

        let prefix = table_key_prefix(SYSTEM_USERS);
        let end = table_key_end(SYSTEM_USERS);

        let rows = self
            .storage
            .scan(Some(&prefix), Some(&end))
            .await
            .map_err(|e| format!("Storage error: {e}"))?;

        for (_key, value) in rows {
            let Ok(row) = decode_row(&value) else {
                continue;
            };

            let Some(Datum::String(row_username)) = row.get_opt(0) else {
                continue;
            };

            if row_username != username {
                continue;
            }

            let Some(Datum::String(row_host)) = row.get_opt(1) else {
                continue;
            };

            let host_pattern = HostPattern::new(row_host);
            if !host_pattern.matches(client_host, None) {
                continue;
            }

            let password_hash = match row.get_opt(2) {
                Some(Datum::String(s)) => s.clone(),
                Some(Datum::Null) | None => String::new(),
                _ => continue,
            };

            let account_locked = match row.get_opt(6) {
                Some(Datum::Bool(b)) => *b,
                _ => false,
            };

            let password_expired = match row.get_opt(7) {
                Some(Datum::Bool(b)) => *b,
                _ => false,
            };

            return Ok(Some((password_hash, account_locked, password_expired)));
        }

        Ok(None)
    }

    /// Main command loop after successful login.
    async fn command_loop(&mut self) -> Result<(), PacketError> {
        loop {
            // Clear any stale cancellation before reading next command
            self.clear_cancelled();

            let (pkt_type, payload) = match self.recv_message().await {
                Ok(msg) => msg,
                Err(PacketError::ConnectionClosed) => {
                    debug!(
                        connection_id = self.connection_id,
                        "TDS client disconnected"
                    );
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            match pkt_type {
                PacketType::SqlBatch => {
                    let r = self.handle_sql_batch(&payload).await;
                    if let Err(ref e) = r {
                        warn!(connection_id = self.connection_id, error = %e, "SQL batch failed");
                    }
                    r?;
                }
                PacketType::TransactionManager => {
                    self.handle_transaction_manager(&payload).await?;
                }
                PacketType::Rpc => {
                    self.handle_rpc(&payload).await?;
                }
                other => {
                    warn!(
                        connection_id = self.connection_id,
                        packet_type = ?other,
                        "Unsupported TDS packet type"
                    );
                    let err = token::encode_error(
                        0,
                        1,
                        16,
                        &format!("Unsupported packet type: {other:?}"),
                        "RooDB",
                    );
                    let done = token::encode_done(token::DONE_ERROR, 0, 0);
                    let mut response = err;
                    response.extend_from_slice(&done);
                    self.writer
                        .write_message(PacketType::TabularResult, &response)
                        .await?;
                }
            }

            // If Attention arrived after the command already completed and sent its
            // response, just clear the flag. The client's asyncWaitCancel unblocks
            // when scan() finishes reading the normal response (via onDone), so
            // sending an extra DONE_ATTN would corrupt the protocol stream.
            if self.is_cancelled() {
                self.clear_cancelled();
                debug!(
                    connection_id = self.connection_id,
                    "Cleared late ATTENTION (command already completed)"
                );
            }
        }
    }

    /// Handle an SQL Batch message.
    ///
    /// Format: ALL_HEADERS + UCS-2LE SQL text
    async fn handle_sql_batch(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        // Skip ALL_HEADERS
        if payload.len() < 4 {
            return self.send_tds_error("Invalid SQL batch: too short").await;
        }

        let total_header_len =
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;

        if total_header_len > payload.len() {
            return self
                .send_tds_error("Invalid SQL batch: header length exceeds payload")
                .await;
        }

        // Extract transaction descriptor from ALL_HEADERS if present
        // (client sends its current transaction descriptor)

        // The SQL text starts after ALL_HEADERS, encoded as UCS-2LE
        let sql_data = &payload[total_header_len..];
        let sql = decode_ucs2_str(sql_data);
        let sql = sql.trim();

        debug!(
            connection_id = self.connection_id,
            sql = %sql,
            "TDS SQL Batch"
        );

        if sql.is_empty() {
            let done = token::encode_done(token::DONE_FINAL, 0, 0);
            self.writer
                .write_message(PacketType::TabularResult, &done)
                .await?;
            return Ok(());
        }

        // Check for transaction commands
        let sql_upper = sql.to_uppercase();
        if sql_upper.starts_with("BEGIN TRANSACTION")
            || sql_upper.starts_with("BEGIN TRAN")
            || sql_upper == "BEGIN"
        {
            return self.handle_begin_transaction().await;
        }
        if sql_upper.starts_with("COMMIT") {
            return self.handle_commit_transaction().await;
        }
        if sql_upper.starts_with("ROLLBACK") {
            return self.handle_rollback_transaction().await;
        }

        // Handle SET statements
        if sql_upper.starts_with("SET ") {
            // Accept SET statements silently
            let done = token::encode_done(token::DONE_FINAL, 0, 0);
            self.writer
                .write_message(PacketType::TabularResult, &done)
                .await?;
            return Ok(());
        }

        // Parse and execute SQL
        self.execute_sql(sql).await
    }

    /// Execute a SQL statement and send the result as TDS tokens.
    async fn execute_sql(&mut self, sql: &str) -> Result<(), PacketError> {
        // Parse
        let stmt = match Parser::parse_one(sql) {
            Ok(s) => s,
            Err(e) => {
                return self.send_tds_error(&e.to_string()).await;
            }
        };

        debug!(connection_id = self.connection_id, "SQL parsed");

        // Resolve, type check, and plan
        enum PlanError {
            Sql(crate::sql::SqlError),
            Planner(crate::planner::PlannerError),
        }

        let plan_result: Result<PhysicalPlan, PlanError> = (|| {
            let catalog_guard = self.catalog.read();
            let resolved = Resolver::new(&catalog_guard)
                .resolve(stmt)
                .map_err(PlanError::Sql)?;
            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;
            let optimized = Optimizer::new().optimize(logical);
            PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)
        })();

        let physical = match plan_result {
            Ok(p) => p,
            Err(PlanError::Sql(e)) => {
                return self.send_tds_error(&e.to_string()).await;
            }
            Err(PlanError::Planner(e)) => {
                return self.send_tds_error(&e.to_string()).await;
            }
        };

        debug!(connection_id = self.connection_id, plan = ?physical, "Plan ready");
        let result = self.execute_plan(physical).await;
        debug!(
            connection_id = self.connection_id,
            ok = result.is_ok(),
            err = ?result.as_ref().err(),
            "execute_plan returned"
        );
        result
    }

    /// Execute a physical plan and send TDS result tokens.
    async fn execute_plan(&mut self, plan: PhysicalPlan) -> Result<(), PacketError> {
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
            let read_view = self
                .txn_manager
                .create_read_view(txn_id)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
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
                .begin(self.session.isolation_level, self.session.is_read_only)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            let read_view = self
                .txn_manager
                .create_read_view(txn.txn_id)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view)),
                Some(txn.txn_id),
            )
        } else {
            let read_view = self
                .txn_manager
                .create_read_view(0)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            (Some(TransactionContext::new(0, read_view)), None)
        };

        if returns_rows {
            let columns = plan.output_columns();
            let engine = ExecutorEngine::with_raft(
                mvcc,
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
            );
            debug!(connection_id = self.connection_id, "Building executor");
            let mut executor = engine
                .build(plan)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

            debug!(
                connection_id = self.connection_id,
                "Calling executor.open()"
            );
            executor
                .open()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            debug!(
                connection_id = self.connection_id,
                "executor.open() returned"
            );

            // Build complete response
            let mut response = Vec::with_capacity(4096);

            // COLMETADATA
            response.extend_from_slice(&types::encode_colmetadata(&columns));

            // ROW tokens — check for cancellation between rows
            let mut row_count: u64 = 0;
            let mut cancelled = false;
            while let Some(row) = executor
                .next()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?
            {
                if self.is_cancelled() {
                    debug!(
                        connection_id = self.connection_id,
                        "Query cancelled by ATTENTION after {} rows", row_count
                    );
                    cancelled = true;
                    break;
                }
                response.extend_from_slice(&types::encode_row(row.values(), &columns));
                row_count += 1;
            }

            executor
                .close()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

            if cancelled {
                // Discard buffered response and send DONE with ATTN flag
                self.clear_cancelled();
                let done = token::encode_done(token::DONE_ATTN, 0, 0);
                self.writer
                    .write_message(PacketType::TabularResult, &done)
                    .await?;
            } else {
                // Check for late Attention (arrived after all rows iterated).
                // If the cancel flag is set, use DONE_ATTN so the client's
                // scan() returns MsgCancel → done() → signals onDone.
                let done_status = if self.is_cancelled() {
                    self.clear_cancelled();
                    token::DONE_ATTN | token::DONE_COUNT
                } else {
                    token::DONE_COUNT
                };
                response.extend_from_slice(&token::encode_done(
                    done_status,
                    0x00C1, // SELECT command
                    row_count,
                ));

                debug!(
                    connection_id = self.connection_id,
                    response_len = response.len(),
                    first_bytes = ?&response[..response.len().min(64)],
                    last_bytes = ?&response[response.len().saturating_sub(16)..],
                    "Sending SELECT response"
                );
                self.writer
                    .write_message(PacketType::TabularResult, &response)
                    .await?;
            }
        } else {
            // DML/DDL
            let engine = ExecutorEngine::with_raft(
                mvcc,
                self.catalog.clone(),
                txn_context,
                self.raft_node.clone(),
            );
            let mut executor = engine
                .build(plan)
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

            executor
                .open()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

            let mut affected = 0u64;
            while let Some(row) = executor
                .next()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?
            {
                if let Some(Datum::Int(n)) = row.get_opt(0) {
                    affected += *n as u64;
                } else {
                    affected += 1;
                }
            }
            executor
                .close()
                .await
                .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

            // Commit changes
            let changes = executor.take_changes();
            if !changes.is_empty() {
                if self.session.in_transaction() {
                    self.session.add_pending_changes(changes);
                } else {
                    let changeset =
                        ChangeSet::new_with_changes(implicit_txn_id.unwrap_or(0), changes);
                    self.raft_node
                        .propose_changes(changeset)
                        .await
                        .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
                }
            }

            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager
                    .commit(txn_id)
                    .await
                    .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            }

            // Send DONE with count
            let done = token::encode_done(token::DONE_COUNT, 0x00C1, affected);
            self.writer
                .write_message(PacketType::TabularResult, &done)
                .await?;
        }

        Ok(())
    }

    /// Handle Transaction Manager request.
    async fn handle_transaction_manager(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        // Skip ALL_HEADERS
        if payload.len() < 4 {
            return self.send_tds_error("Invalid transaction request").await;
        }

        let total_header_len =
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;

        if total_header_len + 2 > payload.len() {
            return self.send_tds_error("Invalid transaction request").await;
        }

        let request_type =
            u16::from_le_bytes([payload[total_header_len], payload[total_header_len + 1]]);

        match request_type {
            5 => {
                // TM_BEGIN_XACT
                let iso_level = if total_header_len + 3 <= payload.len() {
                    payload[total_header_len + 2]
                } else {
                    0 // default
                };
                self.handle_begin_transaction_with_iso(iso_level).await
            }
            7 => {
                // TM_COMMIT_XACT
                self.handle_commit_transaction().await
            }
            8 => {
                // TM_ROLLBACK_XACT
                self.handle_rollback_transaction().await
            }
            _ => {
                self.send_tds_error(&format!(
                    "Unsupported transaction manager request: {request_type}"
                ))
                .await
            }
        }
    }

    /// Handle BEGIN TRANSACTION (via SQL or transaction manager).
    async fn handle_begin_transaction(&mut self) -> Result<(), PacketError> {
        self.handle_begin_transaction_with_iso(0).await
    }

    async fn handle_begin_transaction_with_iso(
        &mut self,
        iso_level: u8,
    ) -> Result<(), PacketError> {
        let isolation = match iso_level {
            1 => IsolationLevel::ReadUncommitted,
            2 => IsolationLevel::ReadCommitted,
            3 => IsolationLevel::RepeatableRead,
            _ => self.session.isolation_level,
        };

        let txn = self
            .txn_manager
            .begin(isolation, self.session.is_read_only)
            .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;

        self.session.begin_transaction(txn.txn_id);

        // Assign a transaction descriptor
        let tran_desc = self.next_tran_descriptor;
        self.next_tran_descriptor += 1;
        self.transaction_descriptor = tran_desc;

        // Send ENVCHANGE (begin transaction) + DONE
        let mut response = Vec::with_capacity(64);
        let desc_bytes = tran_desc.to_le_bytes();
        response.extend_from_slice(&token::encode_envchange_binary(
            token::ENV_BEGIN_TRAN,
            &desc_bytes,
            &[],
        ));
        response.extend_from_slice(&token::encode_done(token::DONE_FINAL, 0, 0));

        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await
    }

    /// Handle COMMIT TRANSACTION.
    async fn handle_commit_transaction(&mut self) -> Result<(), PacketError> {
        if let Some(txn_id) = self.session.current_txn {
            let changes = self.session.take_pending_changes();
            if !changes.is_empty() {
                let changeset = ChangeSet::new_with_changes(txn_id, changes);
                self.raft_node
                    .propose_changes(changeset)
                    .await
                    .map_err(|e| PacketError::Io(std::io::Error::other(e.to_string())))?;
            }

            let _ = self.txn_manager.commit(txn_id).await;
            self.session.end_transaction();
        }

        let old_desc = self.transaction_descriptor.to_le_bytes();
        self.transaction_descriptor = 0;

        let mut response = Vec::with_capacity(64);
        response.extend_from_slice(&token::encode_envchange_binary(
            token::ENV_COMMIT_TRAN,
            &[0; 0], // no new transaction
            &old_desc,
        ));
        response.extend_from_slice(&token::encode_done(token::DONE_FINAL, 0, 0));

        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await
    }

    /// Handle ROLLBACK TRANSACTION.
    async fn handle_rollback_transaction(&mut self) -> Result<(), PacketError> {
        self.session.clear_pending_changes();

        if let Some(txn_id) = self.session.current_txn {
            let _ = self.txn_manager.rollback(txn_id).await;
            self.session.end_transaction();
        }

        let old_desc = self.transaction_descriptor.to_le_bytes();
        self.transaction_descriptor = 0;

        let mut response = Vec::with_capacity(64);
        response.extend_from_slice(&token::encode_envchange_binary(
            token::ENV_ROLLBACK_TRAN,
            &[0; 0],
            &old_desc,
        ));
        response.extend_from_slice(&token::encode_done(token::DONE_FINAL, 0, 0));

        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await
    }

    /// Handle RPC request.
    async fn handle_rpc(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        // Skip ALL_HEADERS
        if payload.len() < 4 {
            return self.send_tds_error("Invalid RPC request").await;
        }

        let total_header_len =
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;

        if total_header_len >= payload.len() {
            return self.send_tds_error("Invalid RPC request").await;
        }

        let rpc_data = &payload[total_header_len..];

        // Check if this is a well-known proc (ProcIDSwitch = 0xFFFF)
        if rpc_data.len() >= 4 {
            let proc_id_switch = u16::from_le_bytes([rpc_data[0], rpc_data[1]]);

            if proc_id_switch == 0xFFFF {
                let proc_id = u16::from_le_bytes([rpc_data[2], rpc_data[3]]);
                // let _options = u16::from_le_bytes([rpc_data[4], rpc_data[5]]);

                match proc_id {
                    10 => {
                        // sp_executesql - extract SQL from first NTEXT parameter
                        return self.handle_sp_executesql(&rpc_data[6..]).await;
                    }
                    _ => {
                        return self
                            .send_tds_error(&format!("Unsupported RPC proc ID: {proc_id}"))
                            .await;
                    }
                }
            }
        }

        self.send_tds_error("Unsupported RPC format").await
    }

    /// Handle sp_executesql RPC: extract the SQL and parameters, substitute, and execute.
    async fn handle_sp_executesql(&mut self, param_data: &[u8]) -> Result<(), PacketError> {
        let Some(sql) = super::params::parse_and_substitute(param_data) else {
            return self
                .send_tds_error("Could not parse sp_executesql parameters")
                .await;
        };

        let sql = sql.trim();
        if sql.is_empty() {
            let done = token::encode_done(token::DONE_FINAL, 0, 0);
            self.writer
                .write_message(PacketType::TabularResult, &done)
                .await?;
            return Ok(());
        }

        self.execute_sql(sql).await
    }

    /// Send a TDS error response.
    async fn send_tds_error(&mut self, message: &str) -> Result<(), PacketError> {
        let mut response = Vec::with_capacity(256);
        response.extend_from_slice(&token::encode_error(0, 1, 16, message, "RooDB"));
        response.extend_from_slice(&token::encode_done(token::DONE_ERROR, 0, 0));

        self.writer
            .write_message(PacketType::TabularResult, &response)
            .await
    }
}

/// Decode a UCS-2LE byte slice to a String.
pub fn decode_ucs2_str(data: &[u8]) -> String {
    let chars: Vec<u16> = data
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&chars)
}
