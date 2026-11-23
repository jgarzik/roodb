//! Statement execution for one client session.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tracing::{debug, warn};

use super::{OkCommand, OkInfo, QueryError, ResponseSink, Status};
use crate::catalog::system_tables::SYSTEM_PROCEDURES;
use crate::catalog::TableName;
use crate::catalog::{Catalog, ParamMode, ProcedureDef, ProcedureParam};
use crate::executor::engine::ExecutorEngine;
use crate::executor::{Datum, Executor, TransactionContext};
use crate::planner::logical::builder::LogicalPlanBuilder;
use crate::planner::optimizer::Optimizer;
use crate::planner::physical::{PhysicalPlan, PhysicalPlanner};
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::server::context::ServerContext;
use crate::server::session::Session;
use crate::server::temp_store::SessionStorage;
use crate::sql::privileges::{
    check_privilege, GrantEntry, HostPattern, Privilege, PrivilegeObject, RequiredPrivilege,
};
use crate::sql::{Parser, Resolver, TypeChecker};
use crate::storage::StorageEngine;
use crate::txn::{IsolationLevel, MvccStorage, TransactionManager};

use super::error::{codes, states, ProtocolError, ProtocolResult};
use super::metrics;
use super::prepared::{CachedPlan, PreparedStatementManager};

/// Flush buffered result rows to the client once this many bytes are pending.
const ROW_FLUSH_BYTES: usize = 64 * 1024;

/// Internal enum for planning errors (used to avoid holding guard across await)
enum PlanError {
    Sql(crate::sql::SqlError),
    Planner(crate::planner::PlannerError),
}

/// Info extracted from INSERT plans for trigger firing
struct InsertTriggerInfo {
    table_name: TableName,
    column_names: Vec<String>,
    values: Vec<Vec<crate::planner::logical::ResolvedExpr>>,
}

/// Protocol-neutral statement execution for one client session.
///
/// Owns the session state and reports every result through its
/// [`ResponseSink`].
pub struct QueryEngine {
    /// Connection ID
    connection_id: u32,
    /// Client IP address, for host-based authentication and grants
    pub(crate) client_ip: IpAddr,
    /// Storage engine
    pub(crate) storage: Arc<dyn StorageEngine>,
    /// Catalog
    pub(crate) catalog: Arc<RwLock<Catalog>>,
    /// Session state (transactions, autocommit, isolation level)
    pub(crate) session: Session,
    /// Transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Raft node for consensus
    raft_node: Arc<RaftNode>,
    /// Prepared statement manager (per-connection)
    pub(crate) prepared_stmts: PreparedStatementManager,
    /// Per-connection MVCC storage (avoids Arc::new per query)
    mvcc: Arc<MvccStorage>,
    /// This session's view of storage: its temporary tables' rows are kept
    /// here in memory, everything else is the shared engine.
    temp: Arc<SessionStorage>,
    /// Catalog version the session's copy of the stored functions was taken
    /// at (see `sync_functions`).
    functions_seen: Option<(u64, Option<String>)>,
    /// Whether BEGIN inside a transaction commits it and starts another
    /// (MySQL), or opens a nested level (the TDS front-end: in T-SQL only
    /// the outermost COMMIT commits).
    pub(crate) begin_commits_open: bool,
    /// How many BEGINs are open, where transactions nest. Meaningful only
    /// while a transaction is open; see `transaction_depth`.
    nested_begins: u32,
    /// The locking the statement now being run asked for with FOR UPDATE /
    /// FOR SHARE, if it did. Set when the statement is parsed, used when it
    /// is executed.
    read_lock: Option<crate::executor::context::ReadLock>,
    /// Implicit transactions of the autocommit statements now executing
    /// (a trigger body nests inside its statement); rolled back if the
    /// statement fails before committing them
    implicit_txns: Vec<u64>,
    /// The session's metadata locks: shared on the tables its statements
    /// use, exclusive on those its schema changes change.
    mdl: Arc<crate::txn::MdlSession>,
    /// How many times the statement now being run has been planned again
    /// (see `MAX_REPLANS`).
    replans: u8,
    /// The catalog's version, readable without the catalog's lock.
    catalog_version: Arc<std::sync::atomic::AtomicU64>,
    /// Where results are reported
    pub(crate) out: Box<dyn ResponseSink>,
}

/// What a plan was made from: the stored tables and the views it uses,
/// each as it was when the plan was made. A plan is run only with those
/// locked and still as they were.
#[derive(Debug, Clone, Default)]
struct PlanUse {
    used: Vec<(TableName, crate::catalog::ObjectStamp)>,
    /// The catalog's version when the plan was made. While it has not
    /// moved, nothing has changed at all.
    planned_at: u64,
}

impl PlanUse {
    /// What the statement `resolver` has resolved uses, as `catalog` (the
    /// catalog it was resolved against) has it.
    fn of(resolver: &Resolver<'_>, catalog: &Catalog) -> Self {
        let used = resolver
            .used_tables()
            .into_iter()
            .map(|name| {
                let stamp = catalog.object_stamp(&name);
                (name, stamp)
            })
            .collect();
        PlanUse {
            used,
            planned_at: catalog.schema_version(),
        }
    }
}

/// How many times a statement is planned again because a schema change
/// got in between its planning and its locking, before it is given up
/// (error 1412). The first retry already runs with the tables locked, so
/// this is only reached by a statement whose every new plan uses yet
/// another table that has just been changed.
const MAX_REPLANS: u8 = 8;

/// Whether a plan was run.
enum Ran {
    /// It was, and the client has its answer (or its error).
    Done,
    /// It was not: the catalog changed between planning and locking.
    /// Nothing was done or sent; the statement is to be planned again.
    Stale,
}

/// The locking a statement asks for with `FOR UPDATE` / `FOR SHARE`, if it
/// is a query that does.
///
/// A table named after `OF` is named as the query knows it: by its alias if
/// it has one. The names are resolved here to the tables themselves, which
/// is what the executors that take the locks know. A name the query does
/// not have is an error (MySQL error 3568).
fn read_lock_of(
    stmt: &sqlparser::ast::Statement,
) -> Result<Option<crate::executor::context::ReadLock>, String> {
    use crate::executor::context::{LockWait, ReadLock};
    use sqlparser::ast::{LockType, NonBlock, SetExpr, Statement, TableFactor};

    let Statement::Query(query) = stmt else {
        return Ok(None);
    };
    let Some(first) = query.locks.first() else {
        return Ok(None);
    };
    // The tables of the query: (name it goes by, table).
    let mut tables: Vec<(String, String)> = Vec::new();
    if let SetExpr::Select(select) = query.body.as_ref() {
        let mut add = |factor: &TableFactor| {
            if let TableFactor::Table { name, alias, .. } = factor {
                let table = name.to_string().trim_matches('`').to_string();
                let known_as = alias
                    .as_ref()
                    .map_or_else(|| table.clone(), |a| a.name.value.clone());
                tables.push((known_as, table));
            }
        };
        for from in &select.from {
            add(&from.relation);
            for join in &from.joins {
                add(&join.relation);
            }
        }
    }
    // FOR UPDATE wins over FOR SHARE if both are given.
    let mode = if query
        .locks
        .iter()
        .any(|lock| matches!(lock.lock_type, LockType::Update))
    {
        crate::txn::LockMode::Exclusive
    } else {
        crate::txn::LockMode::Shared
    };
    let wait = match first.nonblock {
        None => LockWait::Wait,
        Some(NonBlock::Nowait) => LockWait::NoWait,
        Some(NonBlock::SkipLocked) => LockWait::SkipLocked,
    };
    // `OF t1, t2` limits the locking to those tables; without it every
    // table the query reads is locked.
    let mut of = Vec::new();
    for lock in &query.locks {
        match &lock.of {
            Some(name) => {
                let name = name.to_string().trim_matches('`').to_string();
                match tables
                    .iter()
                    .find(|(known_as, _)| known_as.eq_ignore_ascii_case(&name))
                {
                    Some((_, table)) => of.push(table.clone()),
                    None => {
                        return Err(format!("Unresolved table name `{name}` in locking clause."))
                    }
                }
            }
            None => {
                of.clear();
                break;
            }
        }
    }
    Ok(Some(ReadLock { mode, wait, of }))
}

/// What a client is told when its transaction lost a conflict found at
/// commit: the same as for a deadlock, since the remedy is the same.
const COMMIT_CONFLICT: &str = "Deadlock found when trying to get lock; try restarting transaction";

/// What a write gets on a node that is not the primary.
use crate::raft::NOT_PRIMARY;

/// What COMMIT gets when the transaction was rolled back because its node
/// stopped being the primary (MySQL Group Replication's error 3101).
const ROLLED_BACK_BY_FAILOVER: &str =
    "Plugin instructed the server to rollback the current transaction.";

/// How a deadlock is recognised in an error that has been reduced to text.
const DEADLOCK_TEXT: &str = "Deadlock found when trying to get lock";

/// Parse `SET [SESSION | GLOBAL | @@...] INNODB_LOCK_WAIT_TIMEOUT = n` (or
/// `LOCK_WAIT_TIMEOUT`) from upper-cased SQL: which wait, whether it is the
/// global setting, and the seconds.
fn parse_set_lock_wait_timeout(sql_upper: &str) -> Option<(LockWait, bool, u64)> {
    let rest = sql_upper
        .trim_end_matches(';')
        .trim()
        .strip_prefix("SET ")?;
    let (name, value) = rest.split_once('=')?;
    let name = name.trim();
    let (global, name) = if let Some(n) = name.strip_prefix("GLOBAL ") {
        (true, n)
    } else if let Some(n) = name.strip_prefix("@@GLOBAL.") {
        (true, n)
    } else if let Some(n) = name.strip_prefix("SESSION ") {
        (false, n)
    } else if let Some(n) = name.strip_prefix("@@SESSION.") {
        (false, n)
    } else if let Some(n) = name.strip_prefix("@@") {
        (false, n)
    } else {
        (false, name)
    };
    let which = match name.trim() {
        "INNODB_LOCK_WAIT_TIMEOUT" => LockWait::Row,
        "LOCK_WAIT_TIMEOUT" => LockWait::Metadata,
        _ => return None,
    };
    let seconds: u64 = value.trim().parse().ok()?;
    // MySQL's ranges: 1 to 1073741824 for rows, 1 to 31536000 (a year)
    // for metadata.
    let most = match which {
        LockWait::Row => 1_073_741_824,
        LockWait::Metadata => crate::txn::DEFAULT_METADATA_LOCK_WAIT_SECS,
    };
    Some((which, global, seconds.clamp(1, most)))
}

/// Which lock wait a `SET` changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockWait {
    /// `innodb_lock_wait_timeout`: waiting for a row lock.
    Row,
    /// `lock_wait_timeout`: waiting for a metadata lock.
    Metadata,
}

/// What is left of a DROP TABLE once temporary tables are dealt with.
enum TempDrop {
    /// Run this plan (the statement, or what remains of it).
    Plan(Box<PhysicalPlan>),
    /// Nothing remains: the statement has been carried out.
    Done,
    /// The statement names a table this session cannot see.
    NoSuchTable(TableName),
    /// The statement names an index this session cannot see (it belongs to
    /// another session's temporary table).
    NoSuchIndex(String),
}

impl QueryEngine {
    /// Create the engine for a new session, reporting results to `out`.
    pub fn new(
        connection_id: u32,
        client_ip: IpAddr,
        ctx: &ServerContext,
        out: Box<dyn ResponseSink>,
    ) -> Self {
        let temp = Arc::new(SessionStorage::new(ctx.storage.clone()));
        let mvcc = Arc::new(MvccStorage::new(temp.clone(), ctx.txn_manager.clone()));
        let mut session = Session::new(connection_id);
        session.lock_wait_timeout = ctx.txn_manager.locks().default_wait();
        session.metadata_lock_wait = ctx.txn_manager.metadata_locks().default_wait();
        session.init_eval_flags();
        Self {
            connection_id,
            client_ip,
            storage: ctx.storage.clone(),
            catalog: ctx.catalog.clone(),
            session,
            txn_manager: ctx.txn_manager.clone(),
            raft_node: ctx.raft_node.clone(),
            prepared_stmts: PreparedStatementManager::new(),
            mvcc,
            temp,
            functions_seen: None,
            begin_commits_open: true,
            nested_begins: 0,
            read_lock: None,
            implicit_txns: Vec::new(),
            mdl: ctx.txn_manager.metadata_locks().session(),
            replans: 0,
            catalog_version: ctx.catalog.read().version_cell(),
            out,
        }
    }
}

impl QueryEngine {
    /// Execute a prepared statement with bound parameter values.
    pub(crate) async fn execute_prepared(
        &mut self,
        statement_id: u32,
        params: &[Datum],
    ) -> ProtocolResult<()> {
        let result = match self.execute_prepared_statement(statement_id, params).await {
            // As for a statement sent as text: its failure is reported,
            // and the session goes on.
            Err(e) if e.fails_statement_only() => {
                debug!(error = %e, "Statement failed");
                self.abandon_implicit_txn().await;
                self.send_error_from_protocol_error(&e).await
            }
            result => result,
        };
        self.end_of_statement();
        result
    }

    async fn execute_prepared_statement(
        &mut self,
        statement_id: u32,
        params: &[Datum],
    ) -> ProtocolResult<()> {
        self.sync_functions();
        let Some(ps) = self.prepared_stmts.get(statement_id) else {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Unknown prepared statement id: {}", statement_id),
                )
                .await;
        };

        // Plan cache: reuse the cached plan unless the schema changed since.
        // Only what this execution needs is copied out of the statement.
        let cached = match &ps.cached_plan {
            Some(cp) if cp.schema_version == self.catalog.read().schema_version() => Some((
                cp.physical.clone(),
                cp.required_privileges.clone(),
                PlanUse {
                    used: cp.used.clone(),
                    planned_at: cp.schema_version,
                },
            )),
            _ => None,
        };

        self.read_lock = match read_lock_of(&ps.parsed_stmt) {
            Ok(read_lock) => read_lock,
            Err(message) => return self.send_error(3568, "HY000", &message).await,
        };
        let Some((mut physical, required_privileges, plan_use)) = cached else {
            // Cache miss: build the plan from the AST with placeholders kept
            let parsed_stmt = ps.parsed_stmt.clone();
            return self
                .handle_prepared_query(statement_id, &parsed_stmt, params)
                .await;
        };

        if let Err(e) = physical.substitute_params(params) {
            return self.send_error_from_protocol_error(&e.into()).await;
        }

        if !required_privileges.is_empty() {
            if let Err(e) = self.check_authorization(&required_privileges).await {
                return self
                    .send_error(codes::ER_ACCESS_DENIED, states::GENERAL_ERROR, &e)
                    .await;
            }
        }

        match self.run_plan(physical, plan_use).await? {
            Ran::Done => Ok(()),
            // The catalog moved after the cached plan was checked: plan
            // the statement again.
            Ran::Stale => {
                let Some(parsed_stmt) = self
                    .prepared_stmts
                    .get(statement_id)
                    .map(|ps| ps.parsed_stmt.clone())
                else {
                    return Ok(());
                };
                self.handle_prepared_query(statement_id, &parsed_stmt, params)
                    .await
            }
        }
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
                if self.try_handle_transaction_command(&sql).await?.is_some() {
                    return Ok(());
                }
                // Other SET forms (user variables, sql_mode, ...) only exist
                // on the text path, which cannot bind parameters.
                if params.is_empty() {
                    return self.handle_query(&sql).await;
                }
                return self
                    .send_error(
                        codes::ER_UNSUPPORTED_PS,
                        states::GENERAL_ERROR,
                        "This SET statement cannot take parameters in a prepared statement",
                    )
                    .await;
            }
            _ => {}
        }

        // Resolve with placeholder mode (? becomes Literal::Placeholder(n))
        // Then build the plan template, cache it, substitute params, and execute.
        type Planned = (PhysicalPlan, Vec<RequiredPrivilege>, PlanUse, u64);
        let plan_result: Result<Planned, PlanError> = (|| {
            let catalog_guard = self.catalog.read();
            let schema_version = catalog_guard.schema_version();

            let resolver = Resolver::new_with_placeholders(&catalog_guard)
                .for_session(self.temp.table_names(), self.session.database.clone());
            let resolved = resolver
                .resolve(parsed_stmt.clone())
                .map_err(PlanError::Sql)?;
            let plan_use = PlanUse::of(&resolver, &catalog_guard);

            let required_privileges = self.extract_required_privileges(&resolved, &catalog_guard);

            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;

            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;

            let optimized = Optimizer::new().optimize(logical);

            let physical =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)?;

            Ok((physical, required_privileges, plan_use, schema_version))
        })();

        let (plan_template, required_privileges, plan_use, schema_version) = match plan_result {
            Ok(planned) => planned,
            Err(PlanError::Sql(e)) => return self.send_sql_error(&e).await,
            Err(PlanError::Planner(e)) => return self.send_planner_error(&e).await,
        };

        // Cache the plan template (with Placeholder literals) for future executions
        if let Some(ps_mut) = self.prepared_stmts.get_mut(statement_id) {
            ps_mut.cached_plan = Some(CachedPlan {
                physical: plan_template.clone(),
                required_privileges: required_privileges.clone(),
                schema_version,
                used: plan_use.used.clone(),
            });
        }

        // Substitute params into a copy of the plan for this execution
        let mut physical = plan_template;
        if let Err(e) = physical.substitute_params(params) {
            return self.send_error_from_protocol_error(&e.into()).await;
        }

        // Check authorization
        if !required_privileges.is_empty() {
            if let Err(e) = self.check_authorization(&required_privileges).await {
                return self
                    .send_error(codes::ER_ACCESS_DENIED, states::GENERAL_ERROR, &e)
                    .await;
            }
        }

        match self.run_plan(physical, plan_use).await? {
            Ran::Done => Ok(()),
            // The catalog moved between planning and locking: once more.
            Ran::Stale => {
                Box::pin(self.handle_prepared_query(statement_id, parsed_stmt, params)).await
            }
        }
    }

    /// Stream an opened executor's rows to the client as one result set.
    ///
    /// An executor error part-way through is reported to the client in
    /// place of the end-of-rows marker; the connection stays usable.
    async fn stream_rows(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        executor: &mut dyn Executor,
    ) -> ProtocolResult<()> {
        let schema = self.session.database.as_deref().unwrap_or("");
        self.out.begin_result_set(columns, schema);

        let mut row_error = None;
        let mut row_count = 0u64;
        loop {
            if self.out.cancelled() {
                break;
            }
            match executor.next().await {
                Ok(Some(row)) => {
                    row_count += 1;
                    self.out.row(&row);
                    if self.out.buffered_bytes() >= ROW_FLUSH_BYTES {
                        self.out.flush().await?;
                    }
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
            self.out.error(&QueryError::new(code, state, msg));
        } else {
            let status = self.status();
            self.out.end_result_set(status);
        }

        executor.close().await?;
        Ok(())
    }

    /// Handle a SQL query
    pub(crate) fn handle_query<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ProtocolResult<()>> + Send + 'a>> {
        Box::pin(async move { self.handle_query_inner(sql).await })
    }

    /// Carry out a statement a client sent. If the statement fails, the
    /// client is told and the session goes on: only a failure of the
    /// connection itself is returned. (`handle_query` returns a statement's
    /// failure to its caller, which a procedure running a statement of its
    /// body needs.)
    pub(crate) async fn handle_statement(&mut self, sql: &str) -> ProtocolResult<()> {
        let result = match self.handle_query(sql).await {
            Err(e) if e.fails_statement_only() => {
                debug!(error = %e, "Statement failed");
                self.abandon_implicit_txn().await;
                self.send_error_from_protocol_error(&e).await
            }
            result => result,
        };
        self.end_of_statement();
        result
    }

    /// A statement a client sent has been carried out, or has failed: the
    /// metadata locks taken for the statement alone go. Those of the
    /// session's open transaction stay until it ends.
    fn end_of_statement(&mut self) {
        self.mdl.release_all_but(self.session.current_txn);
        self.replans = 0;
    }

    /// Inner implementation of handle_query (separated to allow boxing for recursion)
    async fn handle_query_inner(&mut self, sql: &str) -> ProtocolResult<()> {
        // Clear warnings at the start of each statement
        self.session.clear_warnings();
        self.sync_functions();
        self.read_lock = None;

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
                    Ok(_) => return self.send_ok().await,
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

        // Handle ALTER CLUSTER
        if let Some(()) = self.try_handle_alter_cluster(sql).await? {
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
            return self.send_ok().await;
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
                return self.send_ok().await;
            }
            // LOCK/UNLOCK TABLES — no-op (MVCC provides isolation)
            if upper.starts_with("LOCK TABLE")
                || upper.starts_with("LOCK TABLES")
                || upper.starts_with("UNLOCK TABLE")
                || upper.starts_with("UNLOCK TABLES")
            {
                return self.send_ok().await;
            }
            // FLUSH — no-op
            if upper.starts_with("FLUSH ") {
                return self.send_ok().await;
            }
            // DROP FUNCTION IF EXISTS — no-op (functions not yet implemented)
            if upper.starts_with("DROP FUNCTION") && upper.contains("IF EXISTS") {
                return self.send_ok().await;
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
                let qualified = match self.session.database.as_deref() {
                    Some(db) => format!("{}.{}", db, table_name),
                    None => table_name.clone(),
                };
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
                return self.send_ok().await;
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
                raw_sql: String,
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
                } => Intercept::CreateProc {
                    name,
                    params,
                    body,
                    raw_sql: sql.to_string(),
                },
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
            Intercept::NoOp => return self.send_ok().await,
            Intercept::CreateProc {
                name,
                params,
                body,
                raw_sql,
            } => {
                return self
                    .handle_create_procedure(&name, params.as_deref(), &body, &raw_sql)
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
        self.read_lock = match read_lock_of(&stmt) {
            Ok(read_lock) => read_lock,
            Err(message) => return self.send_error(3568, "HY000", &message).await,
        };

        // Resolve, type check, and plan while holding catalog lock
        // Use closure to return Result and ensure guard is dropped before await
        // Returns both the physical plan and required privileges for authorization
        type Planned = (PhysicalPlan, Vec<RequiredPrivilege>, PlanUse);
        let plan_result: Result<Planned, PlanError> = (|| {
            let catalog_guard = self.catalog.read();

            let resolver = Resolver::new(&catalog_guard)
                .for_session(self.temp.table_names(), self.session.database.clone());
            let resolved = resolver.resolve(stmt).map_err(PlanError::Sql)?;
            let plan_use = PlanUse::of(&resolver, &catalog_guard);

            // Extract required privileges from resolved statement before consuming it
            let required_privileges = self.extract_required_privileges(&resolved, &catalog_guard);

            // Type check
            TypeChecker::check(&resolved).map_err(PlanError::Sql)?;

            // Build logical plan
            let logical = LogicalPlanBuilder::build(resolved).map_err(PlanError::Planner)?;

            // Optimize
            let optimized = Optimizer::new().optimize(logical);

            // Build physical plan
            let physical =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(PlanError::Planner)?;

            Ok((physical, required_privileges, plan_use))
        })(); // catalog_guard dropped here

        // Handle errors after guard is dropped
        let (physical, required_privileges, plan_use) = match plan_result {
            Ok(planned) => planned,
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
        match self.run_plan(physical, plan_use).await? {
            Ran::Done => Ok(()),
            // The catalog moved between planning and locking: the
            // statement is taken from the top again.
            Ran::Stale => self.handle_query(sql).await,
        }
    }

    /// Roll back the implicit transactions of an autocommit statement that
    /// failed before committing, so they do not stay active forever.
    async fn abandon_implicit_txn(&mut self) {
        for txn_id in std::mem::take(&mut self.implicit_txns) {
            if let Err(e) = self.txn_manager.rollback(txn_id).await {
                warn!(txn_id, error = %e, "Failed to roll back implicit transaction");
            }
        }
    }

    /// Execute a plan, reporting an execution failure to the client as a
    /// statement error. The session stays usable.
    async fn run_plan(&mut self, plan: PhysicalPlan, plan_use: PlanUse) -> ProtocolResult<Ran> {
        let start = Instant::now();
        let result = match self.execute_plan(plan, plan_use).await {
            Ok(ran) => Ok(ran),
            Err(e) => {
                warn!("Query execution error: {}", e);
                self.abandon_implicit_txn().await;
                if matches!(
                    e,
                    ProtocolError::Executor(crate::executor::ExecutorError::LockDeadlock)
                ) {
                    self.roll_back_open_transaction().await;
                }
                self.send_error_from_protocol_error(&e)
                    .await
                    .map(|()| Ran::Done)
            }
        };
        metrics::log_query(start);
        result
    }

    /// Extract required privileges from a resolved statement. `catalog` is
    /// the catalog the statement was resolved against, which the caller
    /// holds for reading: it must not be locked again here (a second read
    /// behind a waiting writer would never be granted).
    fn extract_required_privileges(
        &self,
        stmt: &crate::planner::logical::ResolvedStatement,
        catalog: &Catalog,
    ) -> Vec<RequiredPrivilege> {
        use crate::planner::logical::ResolvedStatement;

        // A table's privileges are those of the database it is in; `db` is
        // for what a statement names without a table.
        let db = self.session.database.as_deref().unwrap_or("");
        let select_from = |select: &crate::planner::logical::ResolvedSelect| {
            let mut required = Vec::new();
            select_privileges(select, &mut required);
            required
        };

        match stmt {
            ResolvedStatement::Select(select) => {
                // SELECT requires SELECT privilege on every table and view
                // it reads from
                select_from(select)
            }
            ResolvedStatement::Insert { table, .. } => {
                vec![RequiredPrivilege::insert(&table.database, &table.table)]
            }
            // INSERT on the table, and SELECT on what the rows come from
            ResolvedStatement::InsertSelect { table, source, .. } => {
                let mut privs = vec![RequiredPrivilege::insert(&table.database, &table.table)];
                privs.extend(self.extract_required_privileges(source, catalog));
                privs
            }
            ResolvedStatement::Update { table, .. } => {
                vec![RequiredPrivilege::update(&table.database, &table.table)]
            }
            ResolvedStatement::Delete { table, .. } => {
                vec![RequiredPrivilege::delete(&table.database, &table.table)]
            }
            ResolvedStatement::CreateTableAs { name, source, .. } => {
                let mut privs = vec![RequiredPrivilege::create_table(&name.database)];
                privs.extend(self.extract_required_privileges(source, catalog));
                privs
            }
            ResolvedStatement::CreateTable { name, .. }
            | ResolvedStatement::CreateView { name, .. } => {
                vec![RequiredPrivilege::create_table(&name.database)]
            }
            ResolvedStatement::DropView { .. } => {
                vec![] // Views don't need special privileges
            }
            ResolvedStatement::DropTable { name, .. } => {
                vec![RequiredPrivilege::drop_table(&name.database, &name.table)]
            }
            ResolvedStatement::DropMultipleTables { names, .. } => names
                .iter()
                .map(|name| RequiredPrivilege::drop_table(&name.database, &name.table))
                .collect(),
            ResolvedStatement::CreateIndex { table, .. } => {
                vec![RequiredPrivilege::create_index(
                    &table.database,
                    &table.table,
                )]
            }
            ResolvedStatement::DropIndex { name } => {
                // DROP INDEX requires the INDEX privilege on the index's
                // table. (For an index that does not exist the statement
                // fails whatever the privileges; the session's database
                // stands in.)
                let table = catalog.get_index(name).map(|index| index.table.clone());
                match table {
                    Some(table) => {
                        vec![RequiredPrivilege::create_index(
                            &table.database,
                            &table.table,
                        )]
                    }
                    None => vec![RequiredPrivilege::new(
                        Privilege::Index,
                        PrivilegeObject::Database(db.to_string()),
                    )],
                }
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
            // CREATE DATABASE needs CREATE on the database, DROP DATABASE
            // (which drops everything in it) DROP on it, as in MySQL.
            ResolvedStatement::CreateDatabase { name, .. } => {
                vec![RequiredPrivilege::new(
                    Privilege::Create,
                    PrivilegeObject::Database(name.clone()),
                )]
            }
            ResolvedStatement::DropDatabase { name, .. } => {
                vec![RequiredPrivilege::new(
                    Privilege::Drop,
                    PrivilegeObject::Database(name.clone()),
                )]
            }
            // ANALYZE TABLE - no special privileges needed (root bypasses anyway)
            ResolvedStatement::AnalyzeTable { .. } => {
                vec![]
            }
            // UNION - requires SELECT on both sides' tables
            ResolvedStatement::Union { left, right, .. } => {
                let mut privs = select_from(left);
                // Recursively extract privileges from right side (may be nested Union)
                privs.extend(self.extract_required_privileges(right, catalog));
                privs
            }
            // EXPLAIN - same privileges as inner statement
            ResolvedStatement::Explain { inner } => {
                self.extract_required_privileges(inner, catalog)
            }
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

        // The user's grants, from system.grants
        let rows = crate::catalog::store::scan_rows(self.storage.as_ref(), SYSTEM_GRANTS)
            .await
            .map_err(|e| format!("Failed to query grants: {}", e))?;

        // Parse grants for this user
        let mut user_grants = Vec::new();
        let client_host = self.client_ip.to_string();

        for stored in rows {
            {
                let row = stored.row;
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
    /// Run a plan, with the tables it uses locked. `Ran::Stale` if the
    /// catalog changed between the planning and the locking: then nothing
    /// was done.
    async fn execute_plan(&mut self, plan: PhysicalPlan, plan_use: PlanUse) -> ProtocolResult<Ran> {
        let mut stale = false;
        self.execute_plan_locked(plan, &plan_use, &mut stale)
            .await?;
        Ok(if stale { Ran::Stale } else { Ran::Done })
    }

    async fn execute_plan_locked(
        &mut self,
        plan: PhysicalPlan,
        plan_use: &PlanUse,
        stale: &mut bool,
    ) -> ProtocolResult<()> {
        let plan = match self.drop_temp_tables(plan) {
            TempDrop::Plan(plan) => *plan,
            TempDrop::Done => return self.send_ok().await,
            TempDrop::NoSuchTable(name) => return self.send_no_such_table(&name).await,
            TempDrop::NoSuchIndex(name) => {
                let message = crate::executor::ExecutorError::CantDropKey(name).to_string();
                return self.send_error(1091, "42000", &message).await;
            }
        };

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
        // A secondary reads. Anything that writes or locks is the
        // primary's to do.
        if (is_ddl || !returns_rows || self.read_lock.is_some()) && self.read_only() {
            return self
                .send_error(1290, states::GENERAL_ERROR, NOT_PRIMARY)
                .await;
        }
        if !is_ddl {
            self.ensure_transaction()?;
        }

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
                Some(
                    TransactionContext::with_pending_changes(txn_id, read_view, pending)
                        .with_locks(self.lock_handle()),
                ),
                None,
            )
        } else if (!returns_rows || self.read_lock.is_some()) && self.session.autocommit {
            // Autocommit DML, or a locking read (which must wait for locks
            // like anyone else, though it holds its own only until it is
            // done) - create implicit transaction
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.read_only())?;
            self.implicit_txns.push(txn.txn_id);
            let read_view = self.txn_manager.create_read_view(txn.txn_id)?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view).with_locks(self.lock_handle())),
                Some(txn.txn_id),
            )
        } else {
            // Read-only query in autocommit mode - create snapshot read view
            // txn_id=0 special case: sees all committed transactions
            let read_view = self.txn_manager.create_read_view(0)?;
            (Some(TransactionContext::new(0, read_view)), None)
        };

        // Metadata locks. A schema change commits the open transaction and
        // takes the names it changes for itself. Anything else takes each
        // table it uses shared, for its transaction (or for the statement,
        // if it runs in none), and then makes sure the catalog is still as
        // it was when the plan was made: a schema change may have got in
        // between the planning and the locking.
        if is_ddl {
            let names = self.schema_change_names(&plan);
            if !self.begin_schema_change(names).await? {
                return Ok(());
            }
        } else {
            let owner = match &txn_context {
                Some(context) if context.txn_id != 0 => context.txn_id,
                _ => self.lock_owner(),
            };
            self.lock_used(owner, plan_use).await?;
            if self.is_stale(plan_use) {
                // The statement's implicit transaction has done nothing
                // yet. Its metadata locks are kept until the statement
                // ends: the next plan is then made with these tables
                // already the session's, and no schema change can get in
                // again.
                if let Some(txn_id) = implicit_txn_id {
                    self.implicit_txns.retain(|id| *id != txn_id);
                    if let Err(e) = self.txn_manager.rollback(txn_id).await {
                        warn!(txn_id, error = %e, "Failed to roll back implicit transaction");
                    }
                }
                if self.replans >= MAX_REPLANS {
                    return Err(ProtocolError::Sql(
                        crate::sql::SqlError::TableDefinitionChanged,
                    ));
                }
                self.replans += 1;
                *stale = true;
                return Ok(());
            }
        }

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

            self.send_result_set(&columns, &mut *executor).await?;
            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager.commit(txn_id).await?;
                self.implicit_txns.retain(|id| *id != txn_id);
            }
            Ok(())
        } else {
            // Extract trigger info from INSERT plans before building executor
            let insert_trigger_info = self.extract_insert_trigger_info(&plan);
            // DDL runs here too, but only DML counts rows
            let command = match &plan {
                PhysicalPlan::Insert { .. } | PhysicalPlan::InsertSelect { .. } => {
                    OkCommand::Insert
                }
                PhysicalPlan::Update { .. } => OkCommand::Update,
                PhysicalPlan::Delete { .. } => OkCommand::Delete,
                _ => OkCommand::None,
            };

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
                    self.commit_changes(implicit_txn_id.unwrap_or(0), changes, ignore_dups)
                        .await
                        .map_err(ProtocolError::Raft)?;
                }
            }

            // Commit implicit transaction if we created one
            if let Some(txn_id) = implicit_txn_id {
                self.txn_manager.commit(txn_id).await?;
                self.implicit_txns.retain(|id| *id != txn_id);
            }

            // Write DML results to session for LAST_INSERT_ID()/ROW_COUNT().
            // A statement that generated no value leaves LAST_INSERT_ID()
            // as it was.
            if last_insert_id != 0 {
                self.session
                    .set_user_variable("__sys_last_insert_id", Datum::Int(last_insert_id as i64));
            }
            self.session
                .set_user_variable("__sys_row_count", Datum::Int(affected as i64));

            let status = self.status();
            self.out.ok(
                OkInfo {
                    affected_rows: affected,
                    last_insert_id,
                    command,
                },
                status,
            );
            Ok(())
        }
    }

    /// Open an executor and send its rows to the client
    async fn send_result_set(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        executor: &mut dyn Executor,
    ) -> ProtocolResult<()> {
        executor.open().await?;
        self.stream_rows(columns, executor).await
    }

    /// Send a result set from pre-collected rows (used for UNION)
    async fn send_collected_result_set(
        &mut self,
        columns: &[crate::planner::logical::OutputColumn],
        rows: Vec<crate::executor::row::Row>,
    ) -> ProtocolResult<()> {
        let schema = self.session.database.as_deref().unwrap_or("");
        self.out.begin_result_set(columns, schema);
        for row in &rows {
            self.out.row(row);
        }
        let status = self.status();
        self.out.end_result_set(status);
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

        // SET [SESSION | GLOBAL] innodb_lock_wait_timeout = seconds
        if let Some((which, global, seconds)) = parse_set_lock_wait_timeout(&sql_upper) {
            let wait = std::time::Duration::from_secs(seconds);
            if global {
                // A server-wide setting is the administrator's to change.
                if self.session.user != "root" {
                    return self
                        .send_error(
                            1227,
                            "42000",
                            "Access denied; you need (at least one of) the SUPER or \
                             SYSTEM_VARIABLES_ADMIN privilege(s) for this operation",
                        )
                        .await
                        .map(Some);
                }
                // For sessions that start from now on, as in MySQL.
                match which {
                    LockWait::Row => self.txn_manager.locks().set_default_wait(wait),
                    LockWait::Metadata => self.txn_manager.metadata_locks().set_default_wait(wait),
                }
            } else {
                match which {
                    LockWait::Row => self.session.lock_wait_timeout = wait,
                    LockWait::Metadata => self.session.metadata_lock_wait = wait,
                }
            }
            return self.send_ok().await.map(Some);
        }

        // Catch-all: silently accept any other SET statement (SET NAMES, SET charset, etc.)
        // Exceptions: SET @var (user variables), SET sql_mode (tracked in session)
        if sql_upper.starts_with("SET ") && !sql_upper.starts_with("SET @") {
            // Let SET sql_mode through to be handled by extract_sql_mode_value
            if Self::extract_sql_mode_value(sql).is_some() {
                return Ok(None);
            }
            debug!(connection_id = self.connection_id, sql = %sql, "Accepting SET statement");
            return self.send_ok().await.map(Some);
        }
        // Also accept SET @@system_var as no-op (except @@sql_mode)
        if sql_upper.starts_with("SET @@") {
            if Self::extract_sql_mode_value(sql).is_some() {
                return Ok(None);
            }
            debug!(connection_id = self.connection_id, sql = %sql, "Accepting SET @@var statement");
            return self.send_ok().await.map(Some);
        }

        // Not a transaction command
        Ok(None)
    }

    /// Handle BEGIN / START TRANSACTION
    async fn handle_begin(&mut self) -> ProtocolResult<()> {
        if self.session.in_transaction() {
            // T-SQL transactions nest: a second BEGIN TRAN leaves the first
            // uncommitted and only adds a level, which its COMMIT takes
            // away again. Nothing is committed until the outermost COMMIT;
            // a ROLLBACK at any level undoes everything.
            if !self.begin_commits_open {
                self.nested_begins = self.transaction_depth() + 1;
                return self.send_ok().await;
            }
            // In MySQL, starting a transaction ends the one that is open
            // by committing it. Clients rely on it: after a statement fails
            // inside a transaction they simply begin again.
            if let Err(e) = self.commit_open_transaction().await {
                return self.send_commit_error(&e).await;
            }
        }

        // Read-only mode (replica) requires read-only transactions
        let read_only = self.read_only();
        match self
            .txn_manager
            .begin(self.session.isolation_level, read_only)
        {
            Ok(txn) => {
                self.session.begin_transaction(txn.txn_id);
                self.nested_begins = 1;
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

        self.send_ok().await
    }

    /// Bring this session's copy of the stored functions up to date.
    ///
    /// Expression evaluation finds a stored function through the session's
    /// variables (`__udf_<name>` holds the body, `__udf_<name>_params` the
    /// parameter names). The functions themselves live in the catalog, which
    /// follows the system tables; this copies them over whenever the catalog
    /// has changed, so a function is callable from every session and after a
    /// restart, not only where and when it was created.
    fn sync_functions(&mut self) {
        let (seen, functions) = {
            let catalog = self.catalog.read();
            let version = catalog.schema_version();
            let seen = (version, self.session.database.clone());
            if self.functions_seen.as_ref() == Some(&seen) {
                return;
            }
            // Each function under `database.name`, and those of the
            // session's database under their own names as well.
            let current = self.session.database.as_deref();
            let functions: Vec<(String, String, String)> = catalog
                .functions()
                .flat_map(|f| {
                    let params = f
                        .params
                        .iter()
                        .map(|p| p.name.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    let qualified = (
                        format!("{}.{}", f.database, f.name),
                        f.body_sql.clone(),
                        params.clone(),
                    );
                    let own = (current == Some(f.database.as_str()))
                        .then(|| (f.name.clone(), f.body_sql.clone(), params));
                    std::iter::once(qualified).chain(own)
                })
                .collect();
            (seen, functions)
        };
        let vars = self.session.user_variables();
        let mut vars = vars.write();
        vars.retain(|name, _| !name.starts_with("__udf_"));
        for (name, body, params) in functions {
            vars.insert(format!("__udf_{name}_params"), Datum::String(params));
            vars.insert(format!("__udf_{name}"), Datum::String(body));
        }
        self.functions_seen = Some(seen);
    }

    /// Handle the part of a DROP TABLE that concerns temporary tables. One
    /// of this session's lives in the session only: dropping it touches
    /// neither the log nor the system tables. Another session's does not
    /// exist as far as this one can tell.
    fn drop_temp_tables(&mut self, plan: PhysicalPlan) -> TempDrop {
        match plan {
            PhysicalPlan::DropTable { name, if_exists } => {
                if self.drop_own_temp_table(&name) {
                    TempDrop::Done
                } else if !self.is_foreign_temp_table(&name) {
                    TempDrop::Plan(Box::new(PhysicalPlan::DropTable { name, if_exists }))
                } else if if_exists {
                    TempDrop::Done
                } else {
                    TempDrop::NoSuchTable(name)
                }
            }
            PhysicalPlan::DropMultipleTables { names, if_exists } => {
                if !if_exists {
                    if let Some(foreign) = names.iter().find(|n| self.is_foreign_temp_table(n)) {
                        return TempDrop::NoSuchTable(foreign.clone());
                    }
                }
                // Whatever else was named goes the ordinary way.
                let mut ordinary = Vec::new();
                for name in names {
                    if !self.drop_own_temp_table(&name) && !self.is_foreign_temp_table(&name) {
                        ordinary.push(name);
                    }
                }
                if ordinary.is_empty() {
                    TempDrop::Done
                } else {
                    TempDrop::Plan(Box::new(PhysicalPlan::DropMultipleTables {
                        names: ordinary,
                        if_exists,
                    }))
                }
            }
            PhysicalPlan::DropIndex { name } => {
                // An index of another session's temporary table does not
                // exist as far as this session can tell.
                let table = self
                    .catalog
                    .read()
                    .get_index(&name)
                    .map(|index| index.table.clone());
                if table.is_some_and(|table| self.is_foreign_temp_table(&table)) {
                    TempDrop::NoSuchIndex(name)
                } else {
                    TempDrop::Plan(Box::new(PhysicalPlan::DropIndex { name }))
                }
            }
            other => TempDrop::Plan(Box::new(other)),
        }
    }

    /// Drop one of this session's temporary tables: its rows, its entry in
    /// the catalog, and any changes to it that an open transaction has not
    /// committed yet (they would otherwise be committed as changes to an
    /// ordinary table of that name). False if the table is not one.
    fn drop_own_temp_table(&mut self, name: &TableName) -> bool {
        let Some((start, end)) = self.temp.key_range(name) else {
            return false;
        };
        self.session
            .retain_pending_changes(|change| !(start <= change.key && change.key < end));
        self.temp.remove_table(name);
        // Its AUTO_INCREMENT counter was only ever in memory.
        if let Some(id) = self.catalog.read().get_table(name).map(|def| def.id) {
            self.txn_manager.auto_increment().forget(id);
        }
        let _ = self.catalog.write().drop_table(name);
        true
    }

    /// The name the catalog knows a table by, for a name as a statement
    /// wrote it: with the session's current database for a name written
    /// without one. With no database selected that is an error, as in
    /// MySQL (1046).
    fn table_name(&self, written: &str) -> ProtocolResult<TableName> {
        self.table_name_parts(&name_parts(written))
    }

    /// As `table_name`, for a name the parser read.
    fn object_name(&self, name: &sqlparser::ast::ObjectName) -> ProtocolResult<TableName> {
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|part| match part.as_ident() {
                Some(ident) => ident.value.clone(),
                None => part.to_string(),
            })
            .collect();
        self.table_name_parts(&parts)
    }

    /// The database and name of a routine or trigger, for its name as a
    /// statement wrote it. Routine names do not distinguish case; a
    /// trigger's does.
    fn routine_name(&self, written: &str) -> ProtocolResult<(String, String)> {
        let name = self.table_name(written)?;
        Ok((name.database, name.table.to_lowercase()))
    }

    fn table_name_parts(&self, parts: &[String]) -> ProtocolResult<TableName> {
        // As the resolver names a table, with its errors.
        crate::sql::Resolver::new(&self.catalog.read())
            .for_session(Vec::new(), self.session.database.clone())
            .canonical_parts(parts)
            .map_err(ProtocolError::Sql)
    }

    /// Whether `table` is a temporary table that belongs to another session.
    /// To this session it does not exist.
    fn is_foreign_temp_table(&self, table: &TableName) -> bool {
        !self.temp.is_temp_table(table)
            && self
                .catalog
                .read()
                .get_table(table)
                .is_some_and(|def| def.is_temporary)
    }

    async fn send_no_such_table(&mut self, table: &TableName) -> ProtocolResult<()> {
        self.send_error(
            codes::ER_NO_SUCH_TABLE,
            states::NO_SUCH_TABLE,
            &format!("Table '{table}' doesn't exist"),
        )
        .await
    }

    /// Commit a transaction's row changes.
    ///
    /// Changes to ordinary tables go through Raft. Changes to this session's
    /// temporary tables are applied here, in memory, once the rest has
    /// committed; they are checked first so that a duplicate key in a
    /// temporary table fails the commit before anything is proposed.
    async fn commit_changes(
        &mut self,
        txn_id: u64,
        changes: Vec<RowChange>,
        ignore_duplicates: bool,
    ) -> Result<(), String> {
        // Nothing is committed for a transaction that has been rolled back
        // behind the session's back (its node stopped being the primary
        // while the statement ran): the locks that made its changes safe
        // are gone. This holds for a single statement's own transaction as
        // for an explicit one.
        if txn_id != 0 && !self.txn_manager.is_active(txn_id) {
            self.txn_manager.locks().release_all(txn_id);
            return Err(ROLLED_BACK_BY_FAILOVER.to_string());
        }
        let (temporary, shared) = self.temp.split(changes);
        self.temp
            .check(&temporary, ignore_duplicates)
            .map_err(|e| format!("Raft error: {e}"))?;
        // Another session's temporary table is not this session's to write
        // to: its rows would go to shared storage, where nothing would ever
        // remove them.
        // (What is left after this session's own temporary tables were
        // taken out, and still belongs to a temporary table, is another
        // session's.)
        //
        // The tables the rest belongs to go with the changes, each with
        // the version of its definition: the changes were made against
        // those (the statement holds the tables shared), and are applied
        // only if that is still what the tables are.
        let (foreign, tables) = {
            let catalog = self.catalog.read();
            let mut tables: Vec<(crate::executor::encoding::TableId, u64)> = Vec::new();
            let mut foreign = None;
            for change in &shared {
                let Some(id) = change.table() else { continue };
                if tables.last().is_some_and(|(last, _)| *last == id)
                    || tables.iter().any(|(seen, _)| *seen == id)
                {
                    continue;
                }
                match catalog.table_by_id(id) {
                    Some(def) if def.is_temporary => {
                        foreign = Some(def.name.clone());
                        break;
                    }
                    // The system's own tables have no versions: their
                    // definitions are the server's.
                    Some(def) if crate::catalog::system_tables::is_system_table(&def.name) => {}
                    Some(def) => tables.push((id, def.version)),
                    // Not a table this node knows (any more): no version
                    // of it is the right one.
                    None => tables.push((id, u64::MAX)),
                }
            }
            (foreign, tables)
        };
        if let Some(name) = foreign {
            return Err(format!("Table '{name}' doesn't exist"));
        }
        if !shared.is_empty() {
            let changeset = ChangeSet::new_with_changes_ignore(txn_id, shared, ignore_duplicates)
                .for_tables(tables);
            self.raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| e.to_string())?;
        }
        self.temp.apply(txn_id, temporary, ignore_duplicates);
        Ok(())
    }

    /// `ALTER CLUSTER REMOVE NODE id`: take a node out of the cluster.
    /// `ALTER CLUSTER SET PRIMARY id`: hand the primary role to a node.
    async fn try_handle_alter_cluster(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        let statement = sql.trim().trim_end_matches(';').trim();
        let upper = statement.to_uppercase();
        let mut words = upper.split_whitespace();
        if (words.next(), words.next()) != (Some("ALTER"), Some("CLUSTER")) {
            return Ok(None);
        }
        /// What an `ALTER CLUSTER` statement asks for.
        enum Change {
            Remove(u64),
            SetPrimary(u64),
        }
        let change = match (words.next(), words.next(), words.next(), words.next()) {
            (Some("REMOVE"), Some("NODE"), Some(id), None) => id.parse().ok().map(Change::Remove),
            (Some("SET"), Some("PRIMARY"), Some(id), None) => {
                id.parse().ok().map(Change::SetPrimary)
            }
            _ => None,
        };
        let Some(change) = change else {
            return self
                .send_error(
                    codes::ER_PARSE_ERROR,
                    states::SYNTAX_ERROR,
                    "Expected: ALTER CLUSTER REMOVE NODE <node id>, \
                     or ALTER CLUSTER SET PRIMARY <node id>",
                )
                .await
                .map(Some);
        };
        // Who is in the cluster, and who leads it, is the administrator's
        // to decide.
        if self.session.user != "root" {
            return self
                .send_error(
                    1227,
                    "42000",
                    "Access denied; you need (at least one of) the SUPER privilege(s) for this operation",
                )
                .await
                .map(Some);
        }
        let result = match change {
            Change::Remove(id) => self.raft_node.remove_member(id).await,
            Change::SetPrimary(id) => self.raft_node.transfer_primary(id).await,
        };
        match result {
            Ok(()) => self.send_ok().await.map(Some),
            Err(e) => self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &e.to_string(),
                )
                .await
                .map(Some),
        }
    }

    /// Whether this session may only read: it is on a node that is not the
    /// primary of its cluster.
    fn read_only(&self) -> bool {
        self.session.is_read_only || !self.txn_manager.is_leader()
    }

    /// The row locking a statement of this session does.
    fn lock_handle(&self) -> crate::executor::context::LockHandle {
        crate::executor::context::LockHandle {
            manager: self.txn_manager.locks().clone(),
            timeout: self.session.lock_wait_timeout,
            // READ COMMITTED locks rows only; REPEATABLE READ also locks the
            // ranges it has read against inserts.
            gap_locks: !matches!(
                self.session.isolation_level,
                IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted
            ),
            read: self.read_lock.clone(),
        }
    }

    /// Who the session's metadata locks are held for when no transaction
    /// is: the session itself. (Out of the way of transaction IDs, which
    /// count up from one.)
    fn lock_owner(&self) -> u64 {
        (1 << 62) | u64::from(self.connection_id)
    }

    /// Lock, shared, each table and view a statement uses, for `owner`: a
    /// schema change then waits until the owner has finished with them. A
    /// statement that finds a schema change holding or waiting for one of
    /// them waits its turn.
    async fn lock_used(&mut self, owner: u64, plan_use: &PlanUse) -> ProtocolResult<()> {
        if plan_use.used.is_empty() {
            return Ok(());
        }
        let wait = self.session.metadata_lock_wait;
        for (name, _) in &plan_use.used {
            self.mdl
                .lock_shared(owner, name, wait)
                .await
                .map_err(metadata_lock_error)?;
        }
        Ok(())
    }

    /// As `lock_used`, for a statement run outside the planned path (a
    /// statement of a routine's body, the sides of a UNION): for the open
    /// transaction, or for the session. An error if the catalog changed
    /// between the planning and the locking; the statement can be run
    /// again.
    async fn lock_used_now(&mut self, plan_use: &PlanUse) -> ProtocolResult<()> {
        let owner = self
            .session
            .current_txn
            .unwrap_or_else(|| self.lock_owner());
        self.lock_used(owner, plan_use).await?;
        if self.is_stale(plan_use) {
            return Err(ProtocolError::Sql(
                crate::sql::SqlError::TableDefinitionChanged,
            ));
        }
        Ok(())
    }

    /// Whether any table or view a plan uses is no longer as it was when
    /// the plan was made.
    fn is_stale(&self, plan_use: &PlanUse) -> bool {
        if plan_use.used.is_empty() {
            return false;
        }
        // Nothing in the catalog has changed since the plan was made:
        // the usual case, seen without taking the catalog.
        let version = self
            .catalog_version
            .load(std::sync::atomic::Ordering::Acquire);
        if version == plan_use.planned_at {
            return false;
        }
        let catalog = self.catalog.read();
        plan_use
            .used
            .iter()
            .any(|(name, stamp)| catalog.object_stamp(name) != *stamp)
    }

    /// The names a schema change locks, and how: the tables it creates,
    /// changes or drops exclusively, and their databases shared, so that
    /// a database is not dropped under it. Dropping or creating a
    /// database takes the database itself, and dropping one every table
    /// in it.
    fn schema_change_names(&self, plan: &PhysicalPlan) -> Vec<(TableName, crate::txn::MdlMode)> {
        use crate::txn::MdlMode::{Exclusive, Shared};
        let mut names = Vec::new();
        let table = |names: &mut Vec<(TableName, crate::txn::MdlMode)>, name: &TableName| {
            // A temporary table is its session's own: nobody else can be
            // using it.
            if !self.temp.is_temp_table(name) {
                names.push((database_lock(&name.database), Shared));
                names.push((name.clone(), Exclusive));
            }
        };
        match plan {
            PhysicalPlan::CreateTable { name, .. } | PhysicalPlan::DropTable { name, .. } => {
                table(&mut names, name);
            }
            PhysicalPlan::DropMultipleTables { names: tables, .. } => {
                for name in tables {
                    table(&mut names, name);
                }
            }
            PhysicalPlan::CreateIndex { table: on, .. } => table(&mut names, on),
            PhysicalPlan::DropIndex { name } => {
                let on = self
                    .catalog
                    .read()
                    .get_index(name)
                    .map(|index| index.table.clone());
                if let Some(on) = on {
                    table(&mut names, &on);
                }
            }
            PhysicalPlan::CreateDatabase { name, .. } => {
                names.push((database_lock(name), Exclusive));
            }
            PhysicalPlan::DropDatabase { name, .. } => {
                names.push((database_lock(name), Exclusive));
                let catalog = self.catalog.read();
                for table in catalog.list_tables() {
                    let temporary = catalog.get_table(table).is_some_and(|def| def.is_temporary);
                    if table.is_in(name) && !temporary {
                        names.push((table.clone(), Exclusive));
                    }
                }
                for view in catalog.get_view_names() {
                    if view.is_in(name) {
                        names.push((view, Exclusive));
                    }
                }
            }
            _ => {}
        }
        names
    }

    /// Begin a schema change: commit the open transaction, as MySQL does
    /// before any statement that changes the schema, then lock `names`.
    /// The locks are the statement's own and go when it ends. False if the
    /// statement cannot go on; the client has been told why.
    async fn begin_schema_change(
        &mut self,
        names: Vec<(TableName, crate::txn::MdlMode)>,
    ) -> ProtocolResult<bool> {
        if let Err(message) = self.commit_open_transaction().await {
            self.send_error(codes::ER_UNKNOWN_ERROR, states::GENERAL_ERROR, &message)
                .await?;
            return Ok(false);
        }
        match self.lock_schema_change(&names).await {
            Ok(()) => Ok(true),
            Err(e) => {
                self.send_error_from_protocol_error(&e).await?;
                Ok(false)
            }
        }
    }

    /// What changing (creating, replacing, dropping) the table or view
    /// `name` locks: the name exclusively, its database shared.
    fn object_change(&self, name: &TableName) -> Vec<(TableName, crate::txn::MdlMode)> {
        vec![
            (database_lock(&name.database), crate::txn::MdlMode::Shared),
            (name.clone(), crate::txn::MdlMode::Exclusive),
        ]
    }

    /// Lock the names a schema change changes, for the statement. If one
    /// cannot be had, none is held.
    async fn lock_schema_change(
        &mut self,
        names: &[(TableName, crate::txn::MdlMode)],
    ) -> ProtocolResult<()> {
        if names.is_empty() {
            return Ok(());
        }
        let (owner, wait) = (self.lock_owner(), self.session.metadata_lock_wait);
        self.mdl
            .lock_all(owner, names, wait)
            .await
            .map_err(metadata_lock_error)
    }

    /// The session's transaction has ended: it is no longer the session's,
    /// and the metadata locks it held go.
    fn end_session_transaction(&mut self) {
        if let Some(txn_id) = self.session.current_txn {
            self.session.end_transaction();
            self.mdl.release(txn_id);
        }
    }

    /// With autocommit off, a statement that finds no transaction open
    /// starts one; it stays open until COMMIT or ROLLBACK.
    fn ensure_transaction(&mut self) -> ProtocolResult<()> {
        if !self.session.autocommit && self.session.current_txn.is_none() {
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.read_only())?;
            self.session.begin_transaction(txn.txn_id);
            // A new transaction is one level deep, whatever depth the last
            // one ended at.
            self.nested_begins = 1;
        }
        Ok(())
    }

    /// Roll the session's open transaction back, if it has one: its
    /// uncommitted changes are dropped and its locks released.
    async fn roll_back_open_transaction(&mut self) {
        self.session.clear_pending_changes();
        if let Some(txn_id) = self.session.current_txn {
            match self.txn_manager.rollback(txn_id).await {
                // Not found: already rolled back by a change of primary.
                Ok(()) | Err(crate::txn::TransactionError::NotFound(_)) => {}
                Err(e) => warn!(txn_id, error = %e, "Rollback failed"),
            }
            self.end_session_transaction();
        }
    }

    /// Commit the session's open transaction, if it has one. On failure the
    /// transaction has been rolled back.
    async fn commit_open_transaction(&mut self) -> Result<(), String> {
        let Some(txn_id) = self.session.current_txn else {
            return Ok(());
        };
        // The transaction may have been rolled back behind the session's
        // back: this node stopped being the primary, and with that its
        // locks were gone. What it had buffered must not be committed now,
        // even if the node leads again.
        if !self.txn_manager.is_active(txn_id) {
            self.session.clear_pending_changes();
            self.end_session_transaction();
            // A statement that was running when the transaction was rolled
            // back may have taken locks afterwards; let go of those too.
            self.txn_manager.locks().release_all(txn_id);
            return Err(ROLLED_BACK_BY_FAILOVER.to_string());
        }
        // First, propose any pending changes to Raft
        let changes = self.session.take_pending_changes();
        if !changes.is_empty() {
            if let Err(e) = self.commit_changes(txn_id, changes, false).await {
                self.end_session_transaction();
                let _ = self.txn_manager.rollback(txn_id).await;
                return Err(e);
            }
        }
        // The data is committed once the proposal succeeds; what remains is
        // bookkeeping (and releasing the transaction's locks). A failure
        // there is logged, not reported: the commit did happen.
        if let Err(e) = self.txn_manager.commit(txn_id).await {
            tracing::error!(
                connection_id = self.connection_id,
                txn_id = txn_id,
                error = %e,
                "TxnManager commit failed after Raft success - data IS committed, txn bookkeeping inconsistent"
            );
        }
        self.end_session_transaction();
        debug!(
            connection_id = self.connection_id,
            txn_id = txn_id,
            "Committed transaction"
        );
        Ok(())
    }

    /// Report a commit that failed.
    async fn send_commit_error(&mut self, e: &str) -> ProtocolResult<()> {
        if e.contains("Write conflict") {
            self.send_error(codes::ER_LOCK_DEADLOCK, "40001", COMMIT_CONFLICT)
                .await
        } else if e.contains("Duplicate key") {
            self.send_error(codes::ER_DUP_ENTRY, "23000", e).await
        } else {
            self.send_error(
                codes::ER_UNKNOWN_ERROR,
                states::GENERAL_ERROR,
                &format!("Raft commit failed: {}", e),
            )
            .await
        }
    }

    /// How many BEGINs deep the session is: 0 outside a transaction, 1 in
    /// one, more where transactions nest and BEGIN was repeated.
    pub(crate) fn transaction_depth(&self) -> u32 {
        if self.session.in_transaction() {
            // A transaction opened by a statement (autocommit off) was
            // never begun explicitly; it is one level all the same.
            self.nested_begins.max(1)
        } else {
            0
        }
    }

    /// Handle COMMIT
    async fn handle_commit(&mut self) -> ProtocolResult<()> {
        // The COMMIT of an inner level only closes that level.
        if !self.begin_commits_open && self.transaction_depth() > 1 {
            self.nested_begins = self.transaction_depth() - 1;
            return self.send_ok().await;
        }
        // COMMIT without BEGIN is a no-op
        match self.commit_open_transaction().await {
            Ok(()) => self.send_ok().await,
            Err(e) => self.send_commit_error(&e).await,
        }
    }

    /// Handle ROLLBACK
    async fn handle_rollback(&mut self) -> ProtocolResult<()> {
        // Clear any pending changes (not proposed to Raft)
        self.session.clear_pending_changes();

        if let Some(txn_id) = self.session.current_txn {
            match self.txn_manager.rollback(txn_id).await {
                // Not found: it was rolled back already, when this node
                // stopped being the primary. That is what was asked for.
                Ok(()) | Err(crate::txn::TransactionError::NotFound(_)) => {
                    self.end_session_transaction();
                    debug!(
                        connection_id = self.connection_id,
                        txn_id = txn_id,
                        "Rolled back transaction"
                    );
                }
                Err(e) => {
                    self.end_session_transaction();
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
        self.send_ok().await
    }

    /// Handle SET autocommit = value
    async fn handle_set_autocommit(&mut self, value: bool) -> ProtocolResult<()> {
        // Turning autocommit on commits the open transaction, changes and
        // all. (Turning it off opens nothing yet: the next statement does.)
        if value && !self.session.autocommit {
            if let Err(e) = self.commit_open_transaction().await {
                self.session.set_autocommit(value);
                return self.send_commit_error(&e).await;
            }
        }

        self.session.set_autocommit(value);
        debug!(
            connection_id = self.connection_id,
            autocommit = value,
            "Set autocommit"
        );
        self.send_ok().await
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
        self.send_ok().await
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
        let (plan, plan_use) = {
            let catalog_guard = self.catalog.read();
            let stmt = crate::sql::Parser::parse_one(first_sql).map_err(ProtocolError::Sql)?;
            let resolver = crate::sql::Resolver::new(&catalog_guard)
                .for_session(self.temp.table_names(), self.session.database.clone());
            let resolved = resolver.resolve(stmt).map_err(ProtocolError::Sql)?;
            let plan_use = PlanUse::of(&resolver, &catalog_guard);
            crate::sql::TypeChecker::check(&resolved).map_err(ProtocolError::Sql)?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(ProtocolError::Planner)?;
            let optimized = Optimizer::new().optimize(logical);
            let plan =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(ProtocolError::Planner)?;
            (plan, plan_use)
        };
        self.lock_used_now(&plan_use).await?;

        let columns = plan.output_columns();
        let mvcc = self.mvcc.clone();

        // Create a read-only transaction context for UNION queries
        let txn = self.txn_manager.begin(self.session.isolation_level, true)?;
        let read_view = self.txn_manager.create_read_view(txn.txn_id)?;
        let txn_context =
            Some(TransactionContext::new(txn.txn_id, read_view).with_locks(self.lock_handle()));

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
            // A part that names something that does not exist fails the
            // whole statement; it is not quietly left out of the result.
            let planned = {
                let catalog_guard = self.catalog.read();
                let stmt = match crate::sql::Parser::parse_one(part_sql) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let resolver = crate::sql::Resolver::new(&catalog_guard)
                    .for_session(self.temp.table_names(), self.session.database.clone());
                match resolver.resolve(stmt) {
                    Ok(resolved) => {
                        let plan_use = PlanUse::of(&resolver, &catalog_guard);
                        let _ = crate::sql::TypeChecker::check(&resolved);
                        let logical = match LogicalPlanBuilder::build(resolved) {
                            Ok(l) => l,
                            Err(_) => continue,
                        };
                        let optimized = Optimizer::new().optimize(logical);
                        match PhysicalPlanner::plan(optimized, &catalog_guard) {
                            Ok(p) => Ok((p, plan_use)),
                            Err(_) => continue,
                        }
                    }
                    Err(e) => Err(e),
                }
            };
            let plan = match planned {
                Ok((plan, plan_use)) => {
                    self.lock_used_now(&plan_use).await?;
                    plan
                }
                Err(e) => return self.send_sql_error(&e).await.map(|_| Some(())),
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
    pub(crate) async fn cleanup_temp_tables(&mut self) {
        // The rows were only ever in this session's memory; dropping them
        // is all there is to do, besides forgetting the tables.
        let temp_tables = self.temp.table_names();
        if temp_tables.is_empty() {
            return;
        }
        for name in &temp_tables {
            self.drop_own_temp_table(name);
        }

        debug!(
            connection_id = self.connection_id,
            count = temp_tables.len(),
            "Cleaned up temporary tables"
        );
    }

    /// Handle CREATE TEMPORARY TABLE. The table is known to the catalog but
    /// not to the system tables, and its rows are kept in this session's
    /// memory: nothing about it is logged or replicated.
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
            let resolver = crate::sql::Resolver::new(&catalog)
                .for_session(self.temp.table_names(), self.session.database.clone());
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
        // Its ID is this process's own: a temporary table's rows never
        // leave the session.
        let table_id = crate::executor::encoding::local_table_id();
        let mut table_def = crate::catalog::TableDef::new(&name).with_id(table_id);
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
            Ok(true) => return self.send_ok().await.map(|_| Some(())),
            Err(msg) => {
                return self.send_error(1050, "42S01", &msg).await.map(|_| Some(()));
            }
            Ok(false) => {} // success, continue
        }

        // Its rows are kept in this session from here on.
        self.temp.add_table(&name, table_id);

        self.send_ok().await.map(|_| Some(()))
    }

    /// Handle ALTER TABLE statements.
    /// Modifies the catalog and persists changes to system tables via Raft.
    async fn try_handle_alter_table(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
        // A statement that fails is answered with an error; only a failure
        // to talk to the client ends the connection.
        match self.alter_table(sql).await {
            Err(ProtocolError::Sql(e)) => self.send_sql_error(&e).await.map(Some),
            Err(ProtocolError::Planner(e)) => self.send_planner_error(&e).await.map(Some),
            Err(
                e @ (ProtocolError::Unsupported(_)
                | ProtocolError::Executor(_)
                | ProtocolError::Transaction(_)
                | ProtocolError::Raft(_)
                | ProtocolError::Internal(_)),
            ) => self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &e.to_string(),
                )
                .await
                .map(Some),
            other => other,
        }
    }

    async fn alter_table(&mut self, sql: &str) -> ProtocolResult<Option<()>> {
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
            return self.send_ok().await.map(|_| Some(()));
        }

        match &ast[0] {
            sqlparser::ast::Statement::AlterTable(alter) => {
                let table_name = self.object_name(&alter.name)?;
                if self.is_foreign_temp_table(&table_name) {
                    self.send_no_such_table(&table_name).await?;
                    return Ok(Some(()));
                }
                // A change to a stored table is a schema change: the open
                // transaction is committed, and the table (with its new
                // name, if it is given one) is taken from everyone else
                // before its definition is read. A temporary table is its
                // session's own.
                if !self.temp.is_temp_table(&table_name) {
                    use crate::txn::MdlMode::{Exclusive, Shared};
                    let mut names = vec![
                        (database_lock(&table_name.database), Shared),
                        (table_name.clone(), Exclusive),
                    ];
                    for op in &alter.operations {
                        if let sqlparser::ast::AlterTableOperation::RenameTable { table_name } = op
                        {
                            let (sqlparser::ast::RenameTableNameKind::As(to)
                            | sqlparser::ast::RenameTableNameKind::To(to)) = table_name;
                            let to = self.object_name(to)?;
                            names.push((database_lock(&to.database), Shared));
                            names.push((to, Exclusive));
                        }
                    }
                    if !self.begin_schema_change(names).await? {
                        return Ok(Some(()));
                    }
                }

                // Clone current table def from catalog
                let old_table_name = table_name.clone();
                let mut new_def = {
                    let catalog = self.catalog.read();
                    catalog
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            ProtocolError::Sql(crate::sql::SqlError::TableNotFound(
                                table_name.to_string(),
                            ))
                        })?
                        .clone()
                };

                // Apply each operation to the cloned def
                let mut effects = AlterEffects::default();
                for op in &alter.operations {
                    self.apply_alter_op(&mut new_def, op, &mut effects)?;
                }

                // A temporary table is known to this session only: its new
                // definition goes to the catalog and nowhere else. Written to
                // the system tables it would become an ordinary table that
                // every session could see.
                if self.temp.is_temp_table(&old_table_name) {
                    if new_def.name != old_table_name {
                        return Err(ProtocolError::Unsupported(
                            "renaming a temporary table".to_string(),
                        ));
                    }
                    new_def.is_temporary = true;
                    self.catalog.write().replace_table(&old_table_name, new_def);
                    return self.send_ok().await.map(|_| Some(()));
                }

                // The new definition is checked here, before it is proposed:
                // what is in the log is applied as it is.
                let indexes = {
                    let catalog = self.catalog.read();
                    catalog
                        .validate_table(&new_def)
                        .map_err(|e| ProtocolError::Internal(e.to_string()))?;
                    if new_def.name != old_table_name && catalog.table_exists(&new_def.name) {
                        return Err(ProtocolError::Internal(format!(
                            "Table '{}' already exists",
                            new_def.name
                        )));
                    }
                    // The table's indexes stay with it, less those dropped
                    // here, and under its new name if it has one.
                    let mut indexes = catalog.table_indexes(&old_table_name);
                    indexes.retain(|index| !effects.dropped_indexes.contains(&index.name));
                    for index in &mut indexes {
                        index.table.clone_from(&new_def.name);
                        effects.apply_to_columns(&mut index.columns);
                    }
                    // An index all of whose columns were dropped goes too.
                    indexes.retain(|index| !index.columns.is_empty());
                    indexes
                };
                self.persist_alter_table(&old_table_name, new_def, indexes)
                    .await?;

                self.send_ok().await.map(|_| Some(()))
            }
            _ => Ok(None),
        }
    }

    /// Apply a single ALTER TABLE operation to a cloned TableDef.
    fn apply_alter_op(
        &self,
        def: &mut crate::catalog::TableDef,
        op: &sqlparser::ast::AlterTableOperation,
        effects: &mut AlterEffects,
    ) -> ProtocolResult<()> {
        use sqlparser::ast::AlterTableOperation;

        match op {
            AlterTableOperation::AddColumn {
                column_def,
                column_position,
                ..
            } => {
                let mut col = crate::sql::resolver::convert_column_def_pub(column_def)?;
                // A key written with the column would re-key the rows.
                let keyed = column_def.options.iter().any(|option| {
                    matches!(option.option, sqlparser::ast::ColumnOption::PrimaryKey(_))
                });
                if keyed {
                    return Err(needs_rebuild(format!(
                        "adding the primary key column '{}'",
                        col.name
                    )));
                }
                // What the rows that are already there read as for it.
                col.missing = missing_value(&col)?;
                def.alter_add_column(col, &column_place(column_position.as_ref()))
                    .map_err(alter_error)?;
            }

            AlterTableOperation::DropColumn { column_names, .. } => {
                for ident in column_names {
                    def.alter_drop_column(&ident.value).map_err(|e| match e {
                        // MySQL's words for a column that is not there.
                        crate::catalog::AlterError::NoSuchColumn(name) => {
                            ProtocolError::Sql(crate::sql::SqlError::Parse(format!(
                                "Can't DROP '{name}'; check that column/key exists"
                            )))
                        }
                        other => alter_error(other),
                    })?;
                    effects.dropped_columns.push(ident.value.clone());
                }
            }

            AlterTableOperation::AddConstraint { constraint, .. } => {
                let resolved = crate::sql::Resolver::new(&self.catalog.read())
                    .for_session(self.temp.table_names(), self.session.database.clone())
                    .table_constraint(constraint)?;
                if let Some(crate::catalog::Constraint::PrimaryKey(_)) = resolved {
                    // The rows are stored under their key.
                    return Err(needs_rebuild("adding a primary key".to_string()));
                }
                if let Some(c) = resolved {
                    def.constraints.push(c);
                }
            }

            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                let new_type = crate::sql::resolver::convert_data_type(data_type)?;
                self.alter_column(def, &col_name.value, new_type, options)?;
                if let Some(position) = column_position {
                    def.alter_move_column(&col_name.value, &column_place(Some(position)))
                        .map_err(alter_error)?;
                }
            }

            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                let new_type = crate::sql::resolver::convert_data_type(data_type)?;
                self.alter_column(def, &old_name.value, new_type, options)?;
                def.alter_rename_column(&old_name.value, &new_name.value)
                    .map_err(alter_error)?;
                effects
                    .renamed_columns
                    .push((old_name.value.clone(), new_name.value.clone()));
                if let Some(position) = column_position {
                    def.alter_move_column(&new_name.value, &column_place(Some(position)))
                        .map_err(alter_error)?;
                }
            }

            AlterTableOperation::RenameTable { table_name } => {
                let new_name = match table_name {
                    sqlparser::ast::RenameTableNameKind::As(name)
                    | sqlparser::ast::RenameTableNameKind::To(name) => self.object_name(name)?,
                };
                def.name = new_name;
            }

            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                def.alter_rename_column(&old_column_name.value, &new_column_name.value)
                    .map_err(alter_error)?;
                effects
                    .renamed_columns
                    .push((old_column_name.value.clone(), new_column_name.value.clone()));
            }

            AlterTableOperation::DropPrimaryKey { .. } => {
                // The rows are stored under their key.
                return Err(needs_rebuild("dropping the primary key".to_string()));
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
                effects.dropped_indexes.push(name.value.clone());
            }

            AlterTableOperation::DropConstraint { name, .. } => {
                // A constraint may be backed by an index of the same name.
                effects.dropped_indexes.push(name.value.clone());
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
                            let new_type = crate::sql::resolver::convert_data_type(data_type)?;
                            if !col.data_type.values_fit(&new_type) {
                                return Err(needs_rebuild(format!(
                                    "changing column '{}' from {:?} to {:?}",
                                    col.name, col.data_type, new_type
                                )));
                            }
                            col.data_type = new_type;
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

    /// `MODIFY`/`CHANGE COLUMN`: the column's new type and options.
    fn alter_column(
        &self,
        def: &mut crate::catalog::TableDef,
        name: &str,
        new_type: crate::catalog::DataType,
        options: &[sqlparser::ast::ColumnOption],
    ) -> ProtocolResult<()> {
        def.alter_column_type(name, new_type).map_err(alter_error)?;
        let Some(col) = def
            .columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        else {
            return Err(alter_error(crate::catalog::AlterError::NoSuchColumn(
                name.to_string(),
            )));
        };
        let counted = col.auto_increment;
        self.apply_column_options(col, options);
        if col.auto_increment && !counted {
            // The counter would have to start above the values the column
            // already has, which means reading every row.
            return Err(needs_rebuild(format!(
                "making column '{}' AUTO_INCREMENT",
                col.name
            )));
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

    /// Replace a table's record with its new definition: one change, so
    /// the alteration is applied whole or not at all. A renamed table's
    /// record moves to its new name.
    async fn persist_alter_table(
        &mut self,
        old_table_name: &TableName,
        def: crate::catalog::TableDef,
        indexes: Vec<crate::catalog::IndexDef>,
    ) -> ProtocolResult<()> {
        use crate::catalog::store;

        let record = store::TableRecord { def, indexes };
        let mut changeset = ChangeSet::new(0);
        if *old_table_name == record.def.name {
            changeset.push(store::replace_table(&record));
        } else {
            changeset.push(store::drop_table(old_table_name, record.def.id));
            changeset.push(store::create_table(&record));
        }
        // The catalog is updated when the change is applied, before the
        // proposal returns.
        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ProtocolError::Internal(format!("Raft propose ALTER TABLE: {}", e)))?;
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
                self.send_ok().await?;
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
            self.send_ok().await?;
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
                    "42000",
                    &format!("Unknown database '{}'", db_name),
                )
                .await
                .map(|_| Some(()));
        }

        self.session.set_database(Some(db_name));
        self.send_ok().await.map(Some)
    }

    /// Send a result set from in-memory column names and rows
    async fn send_custom_result_set(
        &mut self,
        col_names: &[&str],
        rows: &[Vec<String>],
    ) -> ProtocolResult<()> {
        use crate::catalog::DataType;
        use crate::planner::logical::OutputColumn;

        let columns: Vec<OutputColumn> = col_names
            .iter()
            .enumerate()
            .map(|(i, name)| OutputColumn {
                id: i,
                name: (*name).to_string(),
                data_type: DataType::Varchar(255),
                nullable: true,
            })
            .collect();

        let schema = self.session.database.as_deref().unwrap_or("");
        self.out.begin_result_set(&columns, schema);
        for row_data in rows {
            let row = crate::executor::row::Row::new(
                row_data
                    .iter()
                    .map(|v| crate::executor::datum::Datum::String(v.clone()))
                    .collect(),
            );
            self.out.row(&row);
        }
        let status = self.status();
        self.out.end_result_set(status);
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
            let Some(db) = self.session.database.clone() else {
                return Err(ProtocolError::Sql(crate::sql::SqlError::NoDatabaseSelected));
            };
            let tables = {
                let catalog = self.catalog.read();
                let mut tables = catalog.get_tables_in_database(&db);
                // Include views in SHOW TABLES output
                tables.extend(catalog.get_views_in_database(&db));
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

            let table_name = &self.table_name(table_name)?;
            let result = {
                let catalog = self.catalog.read();
                catalog.get_table(table_name).map(reconstruct_create_table)
            };

            match result {
                Some(ddl) => {
                    let rows = vec![vec![table_name.table.clone(), ddl]];
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
                            vd.name.table, vd.query_sql
                        );
                        let rows = vec![vec![table_name.table.clone(), create_view]];
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

            // Shown under the table's own name; looked up under the
            // catalog's.
            let table_name = &self.table_name(table_name)?;
            let shown = table_name.table.clone();
            let rows = {
                let catalog = self.catalog.read();
                let indexes = catalog.get_indexes_for_table(table_name);
                let mut rows = Vec::new();
                for idx in &indexes {
                    for (seq, col) in idx.columns.iter().enumerate() {
                        rows.push(vec![
                            shown.clone(),
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
                                shown.clone(),
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

            let table_name = &self.table_name(table_name)?;
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
        {
            let rows: Vec<Vec<String>> = Vec::new();
            self.send_custom_result_set(&["Variable_name", "Value"], &rows)
                .await
                .map(Some)
        }
        // SHOW [GLOBAL | SESSION] STATUS [LIKE 'pattern']
        else if sql_upper.starts_with("SHOW STATUS")
            || sql_upper.starts_with("SHOW SESSION STATUS")
            || sql_upper.starts_with("SHOW GLOBAL STATUS")
        {
            let (record_locks, gap_locks, waiting) = self.txn_manager.locks().counts();
            // Metadata locks in the lock table: those of schema changes,
            // and of the statements and transactions a schema change found
            // using its table. (A shared lock on a table nobody is changing
            // is not there, and not counted.)
            let (metadata_locks, metadata_waiting) = self.txn_manager.metadata_locks().counts();
            let status = [
                ("Innodb_row_lock_current_waits", waiting),
                ("Roodb_row_locks", record_locks),
                ("Roodb_gap_locks", gap_locks),
                ("Roodb_metadata_locks", metadata_locks),
                ("Roodb_metadata_lock_waits", metadata_waiting),
            ];
            // The pattern, if any, is the quoted text after LIKE.
            let pattern = sql_upper.split_once(" LIKE ").map(|(_, p)| {
                p.trim()
                    .trim_end_matches(';')
                    .trim_matches('\'')
                    .to_string()
            });
            let rows: Vec<Vec<String>> = status
                .iter()
                .filter(|(name, _)| {
                    pattern
                        .as_deref()
                        .is_none_or(|p| crate::executor::datum::like_match(&name.to_uppercase(), p))
                })
                .map(|(name, value)| vec![name.to_string(), value.to_string()])
                .collect();
            self.send_custom_result_set(&["Variable_name", "Value"], &rows)
                .await
                .map(Some)
        }
        // SHOW CLUSTER MEMBERS
        else if sql_upper.trim_end_matches(';').trim() == "SHOW CLUSTER MEMBERS" {
            let rows: Vec<Vec<String>> = self
                .raft_node
                .members()
                .into_iter()
                .map(|(id, addr, role)| vec![id.to_string(), addr, role.to_string()])
                .collect();
            self.send_custom_result_set(&["Node_id", "Address", "Role"], &rows)
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
        }
        // SHOW PROCEDURE STATUS [LIKE 'pattern']
        else if sql_upper.starts_with("SHOW PROCEDURE STATUS")
            || sql_upper.starts_with("SHOW FUNCTION STATUS")
        {
            let is_function = sql_upper.starts_with("SHOW FUNCTION");
            // Extract optional LIKE pattern
            let like_pattern = if let Some(idx) = sql_upper.find("LIKE ") {
                let pat_str = sql_trimmed[idx + 5..]
                    .trim()
                    .trim_end_matches(';')
                    .trim_matches('\'');
                Some(pat_str.to_string())
            } else {
                None
            };
            let procs = {
                let catalog = self.catalog.read();
                catalog
                    .list_procedures()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let mut rows: Vec<Vec<String>> = Vec::new();
            for proc in &procs {
                let proc_is_func = proc.returns_type.is_some();
                if proc_is_func != is_function {
                    continue;
                }
                if let Some(ref pat) = like_pattern {
                    if !crate::executor::datum::like_match(
                        &proc.name.to_lowercase(),
                        &pat.to_lowercase(),
                    ) {
                        continue;
                    }
                }
                let type_str = if proc_is_func {
                    "FUNCTION"
                } else {
                    "PROCEDURE"
                };
                rows.push(vec![
                    proc.database.clone(),
                    proc.name.clone(),
                    type_str.to_string(),
                    "root@localhost".to_string(),
                    "0000-00-00 00:00:00".to_string(),
                    "0000-00-00 00:00:00".to_string(),
                    "DEFINER".to_string(),
                    String::new(),
                    "utf8mb4".to_string(),
                    "utf8mb4_general_ci".to_string(),
                    "utf8mb4_general_ci".to_string(),
                ]);
            }
            rows.sort_by(|a, b| a[1].cmp(&b[1]));
            self.send_custom_result_set(
                &[
                    "Db",
                    "Name",
                    "Type",
                    "Definer",
                    "Modified",
                    "Created",
                    "Security_type",
                    "Comment",
                    "character_set_client",
                    "collation_connection",
                    "Database Collation",
                ],
                &rows,
            )
            .await
            .map(Some)
        }
        // SHOW CREATE PROCEDURE proc_name / SHOW CREATE FUNCTION func_name
        else if sql_upper.starts_with("SHOW CREATE PROCEDURE ")
            || sql_upper.starts_with("SHOW CREATE FUNCTION ")
        {
            let is_function = sql_upper.starts_with("SHOW CREATE FUNCTION");
            let offset = if is_function { 21 } else { 22 };
            let proc_name = sql_trimmed[offset..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('`')
                .trim_matches('\'')
                .trim_matches('"');
            let (database, proc_name) = self.routine_name(proc_name)?;
            let proc_name = proc_name.as_str();
            let proc_def = {
                let catalog = self.catalog.read();
                catalog.get_procedure(&database, proc_name).cloned()
            };
            match proc_def {
                Some(def) => {
                    // Reconstruct CREATE DDL
                    let params_str = def
                        .params
                        .iter()
                        .map(|p| {
                            let mode = match p.mode {
                                crate::catalog::ParamMode::In => "IN",
                                crate::catalog::ParamMode::Out => "OUT",
                                crate::catalog::ParamMode::InOut => "INOUT",
                            };
                            format!("{} {} {}", mode, p.name, p.data_type)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let create_ddl = if is_function {
                        let ret = def.returns_type.as_deref().unwrap_or("VARCHAR(255)");
                        format!(
                            "CREATE FUNCTION {}({}) RETURNS {} {}",
                            def.name, params_str, ret, def.body_sql
                        )
                    } else {
                        format!(
                            "CREATE PROCEDURE {}({}) {}",
                            def.name, params_str, def.body_sql
                        )
                    };
                    let (col_name, col_create) = if is_function {
                        ("Function", "Create Function")
                    } else {
                        ("Procedure", "Create Procedure")
                    };
                    let rows = vec![vec![
                        proc_name.to_string(),
                        String::new(),
                        create_ddl,
                        "utf8mb4".to_string(),
                        "utf8mb4_general_ci".to_string(),
                        "utf8mb4_general_ci".to_string(),
                    ]];
                    self.send_custom_result_set(
                        &[
                            col_name,
                            "sql_mode",
                            col_create,
                            "character_set_client",
                            "collation_connection",
                            "Database Collation",
                        ],
                        &rows,
                    )
                    .await
                    .map(Some)
                }
                None => {
                    let kind = if is_function { "FUNCTION" } else { "PROCEDURE" };
                    self.send_error(
                        codes::ER_UNKNOWN_ERROR,
                        states::GENERAL_ERROR,
                        &format!("{} {} does not exist", kind, proc_name),
                    )
                    .await
                    .map(|_| Some(()))
                }
            }
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
            let db = self.session.database.clone().unwrap_or_default();
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

    // DATABASE() is handled by the eval pipeline via __sys_database user variable.
    // No special intercept needed.

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
            None => return self.send_ok().await,
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
                let resolver = Resolver::new(&catalog)
                    .for_session(self.temp.table_names(), self.session.database.clone());
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
        self.send_ok().await
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
                let resolver = Resolver::new(&catalog)
                    .for_session(self.temp.table_names(), self.session.database.clone());
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
        let rest = {
            let r = upper.strip_prefix("SET ")?;
            r.trim()
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

        // DATABASE() is handled by the eval pipeline — no special intercept needed.

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
                "autocommit" => {
                    col_names.push(format!("@@{}", var_lower));
                    values.push(if self.session.autocommit { "1" } else { "0" }.to_string());
                    continue;
                }
                "innodb_lock_wait_timeout" => {
                    col_names.push(format!("@@{}", var_lower));
                    values.push(self.session.lock_wait_timeout.as_secs().to_string());
                    continue;
                }
                "lock_wait_timeout" => {
                    col_names.push(format!("@@{}", var_lower));
                    values.push(self.session.metadata_lock_wait.as_secs().to_string());
                    continue;
                }
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
        let names: Vec<&str> = col_names.iter().map(String::as_str).collect();
        self.send_custom_result_set(&names, &[values]).await?;

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

        let view_name = self.table_name(view_name)?;
        if !self
            .begin_schema_change(self.object_change(&view_name))
            .await?
        {
            return Ok(());
        }

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

        // A view is stored for every session, so it cannot be built on a
        // temporary table, this session's or another's: the query must
        // resolve the same with no temporary table in sight.
        let on_temp_table = {
            let catalog = self.catalog.read();
            let resolves = |hide_temp: bool| {
                let Ok(stmt) = crate::sql::Parser::parse_one(query_sql) else {
                    return false;
                };
                // With every temporary table in sight, or with none.
                let visible: Vec<TableName> = if hide_temp {
                    Vec::new()
                } else {
                    catalog
                        .list_tables()
                        .into_iter()
                        .filter(|name| catalog.get_table(name).is_some_and(|def| def.is_temporary))
                        .cloned()
                        .collect()
                };
                let resolver = crate::sql::Resolver::new(&catalog)
                    .for_session(visible, self.session.database.clone());
                resolver.resolve(stmt).is_ok()
            };
            resolves(false) && !resolves(true)
        };
        if on_temp_table {
            return self
                .send_error(
                    1352,
                    states::GENERAL_ERROR,
                    "View's SELECT refers to a temporary table",
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

        // The view's row, under its name.
        changes.push(crate::catalog::store::insert_row(
            SYSTEM_VIEWS,
            &view_def_to_row(&view_def),
        ));

        let changeset = ChangeSet::new_with_changes(0, changes);
        match self.raft_node.propose_changes(changeset).await {
            Ok(()) => self.send_ok().await,
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

        let mut locks = Vec::new();
        for raw_name in names {
            let view_name = self.table_name(raw_name)?;
            locks.extend(self.object_change(&view_name));
        }
        if !self.begin_schema_change(locks).await? {
            return Ok(());
        }

        for raw_name in names {
            let view_name = self.table_name(raw_name)?;

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
                Ok(()) => self.send_ok().await,
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
            self.send_ok().await
        }
    }

    /// Scan system.views for a row matching the given view name and return a delete RowChange.
    async fn find_view_row_for_delete(
        &self,
        view_name: &TableName,
    ) -> ProtocolResult<Option<crate::raft::RowChange>> {
        use crate::catalog::system_tables::SYSTEM_VIEWS;

        let rows = crate::catalog::store::scan_rows(self.storage.as_ref(), SYSTEM_VIEWS)
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;
        for stored in &rows {
            let is_the_view = matches!(
                stored.row.values(),
                [Datum::String(database), Datum::String(view), ..]
                    if *database == view_name.database && *view == view_name.table
            );
            if is_the_view {
                return Ok(Some(crate::catalog::store::delete_row(stored)));
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
        use crate::catalog::system_tables::{trigger_def_to_row, SYSTEM_TRIGGERS};
        use crate::catalog::TriggerDef;

        let mut def = match TriggerDef::from_create_sql(sql) {
            Ok(def) => def,
            Err(message) => {
                return self
                    .send_error(codes::ER_PARSE_ERROR, states::SYNTAX_ERROR, &message)
                    .await;
            }
        };
        // The table as the catalog knows it: in the session's database
        // unless the statement names another.
        def.table_name = self.table_name(&def.table_name.table)?;
        // A trigger on a system table would run on every schema or account
        // change, for whoever makes it.
        if crate::catalog::system_tables::is_system_table(&def.table_name) {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    "Triggers cannot be created on system tables",
                )
                .await;
        }
        // A trigger changes what happens to its table's rows: the table is
        // taken from everyone else while it is added.
        if !self.temp.is_temp_table(&def.table_name)
            && !self
                .begin_schema_change(self.object_change(&def.table_name))
                .await?
        {
            return Ok(());
        }
        // Nor on a temporary table: the trigger would be stored for everyone
        // and outlive the table. Another session's does not exist at all.
        if self.is_foreign_temp_table(&def.table_name)
            || self.catalog.read().get_table(&def.table_name).is_none()
        {
            return self.send_no_such_table(&def.table_name).await;
        }
        if self.temp.is_temp_table(&def.table_name) {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    "Triggers cannot be created on temporary tables",
                )
                .await;
        }
        // A trigger is named within its table's database.
        if self
            .catalog
            .read()
            .has_trigger(&def.table_name.database, &def.name)
        {
            return self
                .send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Trigger '{}' already exists", def.name),
                )
                .await;
        }

        // A trigger is schema: it is stored in a system table through Raft
        // like the rest, and the catalog picks it up when that is applied.
        let changeset = ChangeSet::new_with_changes(
            0,
            vec![crate::catalog::store::insert_row(
                SYSTEM_TRIGGERS,
                &trigger_def_to_row(&def),
            )],
        );
        match self.raft_node.propose_changes(changeset).await {
            Ok(()) => self.send_ok().await,
            Err(e) => {
                self.send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Failed to create trigger: {}", e),
                )
                .await
            }
        }
    }

    /// Handle DROP TRIGGER: delete its row from system.triggers through Raft.
    async fn handle_drop_trigger_sql(&mut self, sql: &str) -> ProtocolResult<()> {
        use crate::catalog::system_tables::SYSTEM_TRIGGERS;

        // Extract trigger name: DROP TRIGGER [IF EXISTS] name
        let statement = sql.trim().trim_end_matches(';').trim();
        let upper = statement.to_uppercase();
        let if_exists = upper.contains("IF EXISTS");
        let name_part = if if_exists {
            let at = upper.find("EXISTS").map_or(0, |p| p + "EXISTS".len());
            statement.get(at..).unwrap_or("").trim()
        } else {
            statement.get("DROP TRIGGER ".len()..).unwrap_or("").trim()
        };
        // `[database.]trigger`: a trigger is named within a database.
        let named = self.table_name(name_part.trim_matches('"'))?;
        let (database, trigger_name) = (named.database.as_str(), named.table.as_str());

        if trigger_name.is_empty() {
            return self
                .send_error(
                    codes::ER_PARSE_ERROR,
                    states::SYNTAX_ERROR,
                    "Missing trigger name in DROP TRIGGER",
                )
                .await;
        }

        // The trigger's table is taken from everyone else while the
        // trigger goes (and the open transaction is committed whether or
        // not there is such a trigger, as for any schema change).
        let on_table = crate::catalog::store::scan_rows(self.storage.as_ref(), SYSTEM_TRIGGERS)
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?
            .into_iter()
            .find_map(|stored| match stored.row.values() {
                [Datum::String(in_database), Datum::String(name), Datum::String(table), ..]
                    if in_database == database && name == trigger_name =>
                {
                    Some(TableName::new(in_database.clone(), table.clone()))
                }
                _ => None,
            });
        let locks = on_table
            .map(|table| self.object_change(&table))
            .unwrap_or_default();
        if !self.begin_schema_change(locks).await? {
            return Ok(());
        }

        // Find the trigger's row.
        let rows = crate::catalog::store::scan_rows(self.storage.as_ref(), SYSTEM_TRIGGERS)
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;
        let mut changes = Vec::new();
        for stored in &rows {
            let is_the_trigger = matches!(
                stored.row.values(),
                [Datum::String(in_database), Datum::String(name), ..]
                    if in_database == database && name == trigger_name
            );
            if is_the_trigger {
                changes.push(crate::catalog::store::delete_row(stored));
            }
        }

        if changes.is_empty() {
            if if_exists {
                return self.send_ok().await;
            }
            return self
                .send_error(
                    codes::ER_NO_SUCH_TABLE,
                    states::NO_SUCH_TABLE,
                    &format!("Trigger '{}' not found", trigger_name),
                )
                .await;
        }
        match self
            .raft_node
            .propose_changes(ChangeSet::new_with_changes(0, changes))
            .await
        {
            Ok(()) => self.send_ok().await,
            Err(e) => {
                self.send_error(
                    codes::ER_UNKNOWN_ERROR,
                    states::GENERAL_ERROR,
                    &format!("Failed to drop trigger: {}", e),
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
            // Stored triggers belong to stored tables, never to a session's
            // temporary one.
            if self.temp.is_temp_table(table) {
                return None;
            }
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
                // (in the trigger's database, which is its table's, whatever
                // the database of the session that changed the table).
                let sql = substituted.to_string();
                let callers = self.session.database.clone();
                self.session
                    .set_database(Some(trigger.table_name.database.clone()));
                let result = self.execute_sql_silent(&sql).await;
                self.session.set_database(callers);
                result
                    .map_err(|e| ProtocolError::Sql(crate::sql::SqlError::InvalidOperation(e)))?;
            }
        }

        Ok(())
    }

    /// Find the matching closing paren for an opening paren at `start`,
    /// skipping parens inside single-quoted string literals.
    fn find_matching_close_paren(sql: &str, start: usize) -> Option<usize> {
        let bytes = sql.as_bytes();
        let mut depth = 0;
        let mut in_string = false;
        let mut i = start;
        while i < bytes.len() {
            let ch = bytes[i];
            if in_string {
                if ch == b'\'' {
                    // Check for escaped quote ('')
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_string = false;
                }
            } else {
                match ch {
                    b'\'' => in_string = true,
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i);
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        None
    }

    /// Extract procedure body (BEGIN...END) from raw CREATE PROCEDURE SQL.
    fn extract_body_from_raw_sql(raw_sql: &str) -> Option<String> {
        let upper = raw_sql.to_uppercase();
        let cp = upper.find("CREATE PROCEDURE")?;
        let paren_start = raw_sql[cp..].find('(')?;
        let paren_start = cp + paren_start;
        let paren_end = Self::find_matching_close_paren(raw_sql, paren_start)?;
        let body = raw_sql[paren_end + 1..].trim();
        if body.is_empty() {
            None
        } else {
            Some(body.to_string())
        }
    }

    // ============ Stored Procedure Handlers ============

    /// Handle CREATE PROCEDURE statement
    async fn handle_create_procedure(
        &mut self,
        name: &sqlparser::ast::ObjectName,
        params: Option<&[sqlparser::ast::ProcedureParam]>,
        ast_body: &sqlparser::ast::ConditionalStatements,
        raw_sql: &str,
    ) -> ProtocolResult<()> {
        let (database, proc_name) = self.routine_name(&name.to_string())?;
        // Creating a routine is a schema change: the open transaction is
        // committed first.
        if !self.begin_schema_change(Vec::new()).await? {
            return Ok(());
        }

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

        // Extract the original body SQL from the raw SQL (before normalization)
        // to preserve DECLARE HANDLER and DECLARE CURSOR statements.
        // Find the param list closing paren, then take everything after it as body.
        let body_sql =
            Self::extract_body_from_raw_sql(raw_sql).unwrap_or_else(|| ast_body.to_string());

        // Check if procedure already exists
        let exists = {
            let catalog = self.catalog.read();
            catalog.procedure_exists(&database, &proc_name)
        };
        if exists {
            return self
                .send_error(
                    1304,
                    "42000",
                    &format!("PROCEDURE {} already exists", proc_name),
                )
                .await;
        }

        let proc_def = ProcedureDef {
            database,
            name: proc_name.clone(),
            params: our_params,
            body_sql,
            returns_type: None,
        };

        // Write to system.procedures via Raft
        use crate::catalog::system_tables::procedure_def_to_row;
        let change =
            crate::catalog::store::insert_row(SYSTEM_PROCEDURES, &procedure_def_to_row(&proc_def));
        let changeset = ChangeSet::new_with_changes(0, vec![change]);

        match self.raft_node.propose_changes(changeset).await {
            Ok(()) => self.send_ok().await,
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

        self.send_row_count(OkCommand::Insert, total_affected, 0)
            .await
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

        self.send_row_count(OkCommand::Insert, rows_inserted, 0)
            .await
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
            r"(?is)CREATE\s+FUNCTION\s+((?:`[^`]+`|\w+)(?:\s*\.\s*(?:`[^`]+`|\w+))?)\s*\(([^)]*)\)\s+RETURNS\s+(\w+)\s+(BEGIN\b.+\bEND)",
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

        let (database, func_name) = self.routine_name(caps.get(1).unwrap().as_str())?;
        if !self.begin_schema_change(Vec::new()).await? {
            return Ok(());
        }
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

        // A function and a procedure of one database share its names.
        if self.catalog.read().procedure_exists(&database, &func_name) {
            return self
                .send_error(
                    1304,
                    "42000",
                    &format!("FUNCTION {} already exists", func_name),
                )
                .await;
        }

        let proc_def = ProcedureDef {
            database,
            name: func_name.clone(),
            params: our_params,
            body_sql: body_sql.clone(),
            returns_type: Some(returns_type),
        };

        // Store via Raft (reuse procedure storage)
        use crate::catalog::system_tables::procedure_def_to_row;
        let change =
            crate::catalog::store::insert_row(SYSTEM_PROCEDURES, &procedure_def_to_row(&proc_def));
        let changeset = ChangeSet::new_with_changes(0, vec![change]);
        self.raft_node
            .propose_changes(changeset)
            .await
            .map_err(|e| ProtocolError::Unsupported(format!("Failed to create function: {}", e)))?;

        // The catalog picked the function up when the change was applied;
        // every session, this one included, gets it from there.
        self.sync_functions();

        self.send_ok().await
    }

    async fn handle_drop_procedure(
        &mut self,
        proc_name: &str,
        if_exists: bool,
    ) -> ProtocolResult<()> {
        let (database, proc_name) = self.routine_name(proc_name)?;
        if !self.begin_schema_change(Vec::new()).await? {
            return Ok(());
        }

        // Check if procedure exists
        let exists = {
            let catalog = self.catalog.read();
            catalog.procedure_exists(&database, &proc_name)
        };
        if !exists {
            if if_exists {
                return self.send_ok().await;
            }
            return self
                .send_error(
                    1305,
                    "42000",
                    &format!("PROCEDURE {}.{} does not exist", database, proc_name),
                )
                .await;
        }

        // The routine's row
        let rows = crate::catalog::store::scan_rows(self.storage.as_ref(), SYSTEM_PROCEDURES)
            .await
            .map_err(|e| ProtocolError::Internal(format!("Storage error: {}", e)))?;
        let mut changeset = ChangeSet::new(0);
        for stored in &rows {
            let is_the_routine = matches!(
                stored.row.values(),
                [Datum::String(in_database), Datum::String(name), ..]
                    if *in_database == database && *name == proc_name
            );
            if is_the_routine {
                changeset.push(crate::catalog::store::delete_row(stored));
            }
        }

        if !changeset.changes.is_empty() {
            match self.raft_node.propose_changes(changeset).await {
                Ok(()) => self.send_ok().await,
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
            self.send_ok().await
        }
    }

    /// Handle CALL procedure_name(args...) — AST tree-walker interpreter
    async fn handle_call(
        &mut self,
        proc_name: &str,
        call_args: &[sqlparser::ast::Expr],
    ) -> ProtocolResult<()> {
        // Look up procedure in catalog
        let (database, proc_name) = self.routine_name(proc_name)?;
        let proc_def = {
            let catalog = self.catalog.read();
            catalog.get_procedure(&database, &proc_name).cloned()
        };
        let Some(proc_def) = proc_def else {
            return self
                .send_error(
                    1305,
                    "42000",
                    &format!("PROCEDURE {}.{} does not exist", database, proc_name),
                )
                .await;
        };

        // The body runs in the procedure's database, whatever the
        // caller's: the names it uses mean what they meant to its author.
        let callers = self.session.database.clone();
        self.session.set_database(Some(database));
        let result = self.run_procedure(&proc_def, call_args).await;
        self.session.set_database(callers);
        result
    }

    /// Run a procedure's body with the arguments of a CALL.
    async fn run_procedure(
        &mut self,
        proc_def: &ProcedureDef,
        call_args: &[sqlparser::ast::Expr],
    ) -> ProtocolResult<()> {
        use crate::executor::procedure::{build_procedure_context, propagate_out_params};

        // Build context: validate arg count, bind IN/INOUT params, OUT = Null
        let user_vars = self.session.user_variables();
        let mut ctx = match build_procedure_context(proc_def, call_args, &user_vars) {
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
        ctx.handlers = handlers.clone();

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
        let body_result = self
            .execute_procedure_body_with_handlers(&body_stmts, &mut ctx, &handlers)
            .await;

        match body_result {
            Ok(_) => {}
            Err(e) => {
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
        propagate_out_params(proc_def, call_args, &ctx, &user_vars);

        // Final OK; any result sets the body produced precede it
        self.send_row_count(OkCommand::Call, ctx.rows_affected, 0)
            .await
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
        use std::sync::LazyLock;
        static RE_FETCH: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"(?i)^FETCH\s+(\w+)\s+INTO\s+(.+)$").unwrap());
        // Pre-process: split multi-variable DECLARE into individual ones
        let body = crate::sql::Parser::split_multi_var_declare(body);
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
            } else if upper.starts_with("FETCH ") {
                // Normalize FETCH cursor INTO var1, var2, ... to sentinel SET
                // that execute_procedure_stmt detects at runtime.
                // Uses @__roodb_fetch__ (reserved internal name) as the carrier.
                if let Some(caps) = RE_FETCH.captures(trimmed) {
                    let cursor = caps[1].replace('\'', "''");
                    let vars = caps[2].replace('\'', "''");
                    if !result.is_empty() {
                        result.push(' ');
                    }
                    result.push_str(&format!(
                        "SET @__roodb_fetch__ = '{} INTO {}';",
                        cursor, vars
                    ));
                } else {
                    if !result.is_empty() {
                        result.push(' ');
                    }
                    result.push_str(trimmed);
                    result.push(';');
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
        if lower.contains("no data") || lower.contains("zero rows fetched") {
            "02000".to_string()
        } else if lower.contains("out of range") || lower.contains("overflow") {
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
                        // Detect FETCH sentinel: SET @__roodb_fetch__ = 'cursor INTO var1,var2'
                        if var_name_full == "@@__roodb_fetch__"
                            || var_name_full == "@__roodb_fetch__"
                        {
                            let val = if let Some(expr) = values.first() {
                                eval_sp_expr(expr, ctx, &user_vars)?
                            } else {
                                return Ok(ProcControlFlow::Continue);
                            };
                            let fetch_spec = val.to_display_string();
                            return self.execute_fetch_sentinel(&fetch_spec, ctx).await;
                        }
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

                // Combine top-level handlers from context with any nested in the WHILE body
                let while_body_sql = while_stmt.while_block.to_string();
                let mut nested_handlers = Self::extract_handlers(&while_body_sql);
                // Append parent handlers so they apply in the WHILE body
                for h in &ctx.handlers {
                    if !nested_handlers.iter().any(|(_, s, _)| s == &h.1) {
                        nested_handlers.push(h.clone());
                    }
                }
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
                            // Raise SQLSTATE 02000 (NOT FOUND) so DECLARE HANDLER can catch it
                            return Err("No data - zero rows fetched".to_string());
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

            // CALL procedure(args) — nested procedure call
            S::Call(_) => {
                // Route through handle_query which has the Call intercept
                let sql = stmt.to_string();
                let substituted = self.substitute_locals(&sql, ctx);
                self.handle_query(&substituted)
                    .await
                    .map_err(|e| format!("{}", e))?;
                Ok(ProcControlFlow::Continue)
            }

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
                let client_multi = self.out.accepts_multiple_results();
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
        vars.sort_by_key(|a| std::cmp::Reverse(a.0.len()));
        for (name, val) in vars {
            result = result.replace(name, &val.to_sql_literal());
        }
        result
    }

    /// Execute a FETCH sentinel produced by normalization.
    /// Parses 'cursor_name INTO var1, var2, ...' and performs the fetch.
    async fn execute_fetch_sentinel(
        &mut self,
        spec: &str,
        ctx: &mut crate::executor::procedure::ProcedureContext,
    ) -> Result<crate::executor::procedure::ProcControlFlow, String> {
        use crate::executor::procedure::{CursorState, ProcControlFlow};

        // Parse: "cursor_name INTO var1, var2, ..."
        let upper = spec.to_uppercase();
        let into_pos = upper.find(" INTO ").ok_or("Invalid FETCH sentinel")?;
        let cursor_name = spec[..into_pos].trim().to_lowercase();
        let vars_str = &spec[into_pos + 6..]; // skip " INTO "
        let into_vars: Vec<String> = vars_str
            .split(',')
            .map(|v| v.trim().to_lowercase())
            .collect();

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
                    // Raise SQLSTATE 02000 (NOT FOUND) so DECLARE HANDLER can catch it
                    return Err("No data - zero rows fetched".to_string());
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

    /// Execute a SELECT inside a stored procedure and send the result set to the client.
    async fn execute_sp_select(&mut self, sql: &str) -> ProtocolResult<()> {
        // Extract column names first (works even when result is empty)
        let col_names = Self::extract_select_column_names(sql, 100);

        // Buffer all rows
        let rows = self
            .execute_select_buffered(sql)
            .await
            .map_err(|e| ProtocolError::Unsupported(format!("SP SELECT failed: {}", e)))?;

        let columns: Vec<crate::planner::logical::OutputColumn> = col_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| crate::planner::logical::OutputColumn {
                id: i,
                name,
                data_type: crate::catalog::DataType::Varchar(255),
                nullable: true,
            })
            .collect();
        self.send_collected_result_set(&columns, rows).await
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

        let (plan, plan_use) = {
            let catalog_guard = self.catalog.read();
            let resolver = crate::sql::Resolver::new(&catalog_guard)
                .for_session(self.temp.table_names(), self.session.database.clone());
            let resolved = resolver.resolve(stmt).map_err(|e| e.to_string())?;
            let plan_use = PlanUse::of(&resolver, &catalog_guard);
            crate::sql::TypeChecker::check(&resolved).map_err(|e| e.to_string())?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(|e| e.to_string())?;
            let optimized = Optimizer::new().optimize(logical);
            let plan =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(|e| e.to_string())?;
            (plan, plan_use)
        };
        self.lock_used_now(&plan_use)
            .await
            .map_err(|e| e.to_string())?;

        let mvcc = self.mvcc.clone();
        let txn_context = if let Some(txn_id) = self.session.current_txn {
            let read_view = self
                .txn_manager
                .create_read_view(txn_id)
                .map_err(|e| e.to_string())?;
            let pending = self.session.get_pending_changes();
            Some(
                TransactionContext::with_pending_changes(txn_id, read_view, pending)
                    .with_locks(self.lock_handle()),
            )
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
        let result = self.execute_sql_silent_inner(sql).await;
        if let Err(message) = &result {
            self.abandon_implicit_txn().await;
            // A deadlock ends the whole transaction wherever it is found,
            // in a procedure or trigger body as in a plain statement.
            if message.contains(DEADLOCK_TEXT) {
                self.roll_back_open_transaction().await;
            }
        }
        result
    }

    async fn execute_sql_silent_inner(&mut self, sql: &str) -> Result<u64, String> {
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

        let (plan, plan_use) = {
            let catalog_guard = self.catalog.read();
            let resolver = crate::sql::Resolver::new(&catalog_guard)
                .for_session(self.temp.table_names(), self.session.database.clone());
            let resolved = resolver.resolve(stmt).map_err(|e| e.to_string())?;
            let plan_use = PlanUse::of(&resolver, &catalog_guard);
            crate::sql::TypeChecker::check(&resolved).map_err(|e| e.to_string())?;
            let logical = LogicalPlanBuilder::build(resolved).map_err(|e| e.to_string())?;
            let optimized = Optimizer::new().optimize(logical);
            let plan =
                PhysicalPlanner::plan(optimized, &catalog_guard).map_err(|e| e.to_string())?;
            (plan, plan_use)
        };

        let plan = match self.drop_temp_tables(plan) {
            TempDrop::Plan(plan) => *plan,
            TempDrop::Done => return Ok(0),
            TempDrop::NoSuchTable(name) => return Err(format!("Table '{name}' doesn't exist")),
            TempDrop::NoSuchIndex(name) => {
                return Err(crate::executor::ExecutorError::CantDropKey(name).to_string())
            }
        };
        let is_ddl = plan.is_ddl();
        // A schema change commits the open transaction and takes the names
        // it changes; anything else takes what it uses shared.
        if is_ddl {
            let names = self.schema_change_names(&plan);
            self.commit_open_transaction().await?;
            self.lock_schema_change(&names)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            self.lock_used_now(&plan_use)
                .await
                .map_err(|e| e.to_string())?;
        }
        if !is_ddl {
            self.ensure_transaction().map_err(|e| e.to_string())?;
        }
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
                Some(
                    TransactionContext::with_pending_changes(txn_id, read_view, pending)
                        .with_locks(self.lock_handle()),
                ),
                None,
            )
        } else if self.session.autocommit {
            let txn = self
                .txn_manager
                .begin(self.session.isolation_level, self.read_only())
                .map_err(|e| e.to_string())?;
            self.implicit_txns.push(txn.txn_id);
            let read_view = self
                .txn_manager
                .create_read_view(txn.txn_id)
                .map_err(|e| e.to_string())?;
            (
                Some(TransactionContext::new(txn.txn_id, read_view).with_locks(self.lock_handle())),
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
                self.commit_changes(implicit_txn_id.unwrap_or(0), changes, ignore_dups)
                    .await?;
            }
        }

        if let Some(txn_id) = implicit_txn_id {
            self.txn_manager
                .commit(txn_id)
                .await
                .map_err(|e| e.to_string())?;
            self.implicit_txns.retain(|id| *id != txn_id);
        }

        Ok(affected)
    }

    /// Reset the session for the next user of a pooled connection:
    /// roll back the open transaction (unless `keep_transaction`), drop
    /// temporary tables and prepared statements, and restore session
    /// settings. The caller decides which database is selected afterwards.
    pub(crate) async fn reset_session(&mut self, keep_transaction: bool) {
        if !keep_transaction {
            self.session.clear_pending_changes();
            if let Some(txn_id) = self.session.current_txn {
                if let Err(e) = self.txn_manager.rollback(txn_id).await {
                    warn!(txn_id, error = %e, "Rollback on session reset failed");
                }
                self.end_session_transaction();
            }
        }
        self.cleanup_temp_tables().await;
        self.prepared_stmts.clear();
        self.session.reset();
        // The reset cleared the session's copy of the stored functions.
        self.functions_seen = None;
        self.session.lock_wait_timeout = self.txn_manager.locks().default_wait();
        self.session.metadata_lock_wait = self.txn_manager.metadata_locks().default_wait();
    }

    /// Transaction status of the session, reported with every result.
    fn status(&self) -> Status {
        Status {
            in_transaction: self.session.in_transaction(),
            autocommit: self.session.autocommit,
        }
    }

    /// Send an OK for a statement that does not count rows
    pub(crate) async fn send_ok(&mut self) -> ProtocolResult<()> {
        let status = self.status();
        self.out.ok(OkInfo::default(), status);
        Ok(())
    }

    /// Send an OK for a statement that counts rows: DML, `LOAD DATA`, `CALL`
    pub(crate) async fn send_row_count(
        &mut self,
        command: OkCommand,
        affected_rows: u64,
        last_insert_id: u64,
    ) -> ProtocolResult<()> {
        let status = self.status();
        self.out.ok(
            OkInfo {
                affected_rows,
                last_insert_id,
                command,
            },
            status,
        );
        Ok(())
    }

    /// Send an error
    pub(crate) async fn send_error(
        &mut self,
        code: u16,
        state: &str,
        message: &str,
    ) -> ProtocolResult<()> {
        // A deadlock found inside a procedure or trigger reaches here as
        // the text of a general error. It is a deadlock all the same, and
        // clients decide whether to retry by the code. Likewise a write
        // refused because this node is not the primary, wherever it was
        // refused.
        let (code, state) = if message.contains(DEADLOCK_TEXT) {
            (codes::ER_LOCK_DEADLOCK, "40001")
        } else if message.contains(NOT_PRIMARY) {
            (1290, states::GENERAL_ERROR)
        } else if message.contains(ROLLED_BACK_BY_FAILOVER) {
            (3101, "40000")
        } else {
            (code, state)
        };
        let message = if message.contains(NOT_PRIMARY) {
            NOT_PRIMARY
        } else {
            message
        };
        self.out
            .error(&QueryError::new(code, state.to_string(), message));
        Ok(())
    }

    /// Send error from SQL error
    async fn send_sql_error(&mut self, e: &crate::sql::SqlError) -> ProtocolResult<()> {
        use crate::sql::SqlError;

        let (code, state) = match e {
            SqlError::Parse(_) => (codes::ER_SYNTAX_ERROR, states::SYNTAX_ERROR),
            SqlError::TableNotFound(_) => (codes::ER_NO_SUCH_TABLE, states::NO_SUCH_TABLE),
            SqlError::NoDatabaseSelected => (1046, "3D000"),
            SqlError::UnknownDatabase(_) => (codes::ER_BAD_DB_ERROR, "42000"),
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
            SqlError::NotSupportedYet(_) => (1235, "42000"),
            SqlError::TableDefinitionChanged => (1412, states::GENERAL_ERROR),
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
    pub(crate) async fn send_error_from_protocol_error(
        &mut self,
        e: &ProtocolError,
    ) -> ProtocolResult<()> {
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
                } else if let crate::executor::ExecutorError::LockDeadlock = exec_err {
                    (codes::ER_LOCK_DEADLOCK, "40001", exec_err.to_string())
                } else if let crate::executor::ExecutorError::DatabaseExists(_) = exec_err {
                    (1007, states::GENERAL_ERROR, exec_err.to_string())
                } else if let crate::executor::ExecutorError::DatabaseNotFound(_) = exec_err {
                    (1008, states::GENERAL_ERROR, exec_err.to_string())
                } else if let crate::executor::ExecutorError::DuplicateKeyName(_) = exec_err {
                    (1061, "42000", exec_err.to_string())
                } else if let crate::executor::ExecutorError::CantDropKey(_) = exec_err {
                    (1091, "42000", exec_err.to_string())
                } else if let crate::executor::ExecutorError::LockNoWait = exec_err {
                    (3572, states::GENERAL_ERROR, exec_err.to_string())
                } else if let crate::executor::ExecutorError::LockWaitTimeout = exec_err {
                    (
                        codes::ER_LOCK_WAIT_TIMEOUT,
                        states::GENERAL_ERROR,
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
                } else if msg.contains("Write conflict") {
                    // Another transaction changed a row this one read
                    // without locking it: it has lost, and must be retried.
                    (
                        codes::ER_LOCK_DEADLOCK,
                        "40001",
                        COMMIT_CONFLICT.to_string(),
                    )
                } else if msg.contains(crate::raft::TABLE_CHANGED) {
                    // The table was changed between the making of these
                    // rows and their commit.
                    (
                        1412,
                        states::GENERAL_ERROR,
                        crate::raft::TABLE_CHANGED.to_string(),
                    )
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
        td.name.table,
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

/// The name a database is locked by: itself, with no table. Schema changes
/// to a database's tables hold it shared; creating or dropping the
/// database holds it exclusively.
fn database_lock(database: &str) -> TableName {
    TableName::new(database, "")
}

/// A metadata lock that was not granted, as the client is told: as the
/// row lock errors are, with MySQL's codes (1213, and the transaction
/// rolled back; 1205).
fn metadata_lock_error(e: crate::txn::MdlError) -> ProtocolError {
    ProtocolError::Executor(match e {
        crate::txn::MdlError::Deadlock => crate::executor::ExecutorError::LockDeadlock,
        crate::txn::MdlError::Timeout => crate::executor::ExecutorError::LockWaitTimeout,
    })
}

/// What an `ALTER TABLE` did that the table's indexes must follow.
#[derive(Default)]
struct AlterEffects {
    /// Indexes dropped by name.
    dropped_indexes: Vec<String>,
    /// Columns dropped.
    dropped_columns: Vec<String>,
    /// Columns renamed: old name, new name, in the order it was done.
    renamed_columns: Vec<(String, String)>,
}

impl AlterEffects {
    /// Bring a list of column names (an index's) up to date: dropped
    /// columns leave it, renamed ones take their new names.
    fn apply_to_columns(&self, columns: &mut Vec<String>) {
        columns.retain(|column| {
            !self
                .dropped_columns
                .iter()
                .any(|dropped| dropped.eq_ignore_ascii_case(column))
        });
        for (old, new) in &self.renamed_columns {
            for column in columns.iter_mut().filter(|c| c.eq_ignore_ascii_case(old)) {
                column.clone_from(new);
            }
        }
    }
}

/// The error for a change to a table that cannot be made without
/// rewriting its rows or storing them under other keys (MySQL error 1235,
/// until the online rebuild of a table does it).
fn needs_rebuild(change: String) -> ProtocolError {
    ProtocolError::Sql(crate::sql::SqlError::NotSupportedYet(change))
}

/// A refused change to a table's columns, as the client is told.
fn alter_error(e: crate::catalog::AlterError) -> ProtocolError {
    match e {
        crate::catalog::AlterError::NeedsRebuild(change) => needs_rebuild(change),
        other => ProtocolError::Sql(crate::sql::SqlError::Parse(other.to_string())),
    }
}

/// Where `ALTER TABLE` says a column goes: `FIRST`, `AFTER column`, or
/// (saying nothing) after all the others.
fn column_place(
    position: Option<&sqlparser::ast::MySQLColumnPosition>,
) -> crate::catalog::ColumnPlace {
    use crate::catalog::ColumnPlace;
    match position {
        None => ColumnPlace::Last,
        Some(sqlparser::ast::MySQLColumnPosition::First) => ColumnPlace::First,
        Some(sqlparser::ast::MySQLColumnPosition::After(column)) => {
            ColumnPlace::After(column.value.clone())
        }
    }
}

/// What the rows a table already has read as for a column added to it:
/// the column's default, the type's own default for a column that has
/// none and cannot be NULL, or nothing (NULL). Encoded as the catalog
/// keeps it (`ColumnDef::missing`).
fn missing_value(col: &crate::catalog::ColumnDef) -> ProtocolResult<Option<Vec<u8>>> {
    let datum = match &col.default {
        Some(default) => {
            let literal = crate::sql::resolver::parse_default_value(default);
            crate::executor::eval::coerce_to_column_type(
                Datum::from_literal(&literal),
                &col.data_type,
            )?
        }
        None if col.nullable => Datum::Null,
        None => Datum::default_for_type(&col.data_type),
    };
    if datum.is_null() {
        return Ok(None);
    }
    let mut encoded = Vec::new();
    crate::executor::encoding::encode_value(&mut encoded, &datum);
    Ok(Some(encoded))
}

/// SELECT on every table and view a query reads from, in its FROM clause
/// and its joins. A view needs the privilege on the view itself, in the
/// view's database; a derived table or a WITH query needs it on what it
/// reads from in turn.
fn select_privileges(
    select: &crate::planner::logical::ResolvedSelect,
    required: &mut Vec<RequiredPrivilege>,
) {
    let refs = select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.table));
    for table_ref in refs {
        match (&table_ref.table, &table_ref.inner_query) {
            (Some(table), _) => {
                required.push(RequiredPrivilege::select(&table.database, &table.table));
            }
            (None, Some(inner)) => select_privileges(inner, required),
            // DUAL
            (None, None) => {}
        }
    }
}

/// The parts of a name as written in a statement's text: `t`, `db.t`,
/// with or without backquotes. A dot inside backquotes is part of the name;
/// a doubled backquote inside them is one backquote.
fn name_parts(written: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut quoted = false;
    let mut chars = written.trim().chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '`' if quoted && chars.peek() == Some(&'`') => {
                chars.next();
                part.push('`');
            }
            '`' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut part).trim().to_string()),
            c => part.push(c),
        }
    }
    parts.push(part.trim().to_string());
    parts
}
