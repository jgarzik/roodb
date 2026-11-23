//! Response sink: how the query layer reports results to a client.

use std::any::Any;
use std::io;

use async_trait::async_trait;

use crate::executor::row::Row;
use crate::planner::logical::OutputColumn;

use super::error::QueryError;

/// Session status reported alongside a completed statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// An explicit transaction is open
    pub in_transaction: bool,
    /// Autocommit is enabled
    pub autocommit: bool,
}

impl Status {
    /// Status of a session with autocommit on and no open transaction.
    pub const AUTOCOMMIT: Status = Status {
        in_transaction: false,
        autocommit: true,
    };
}

/// Outcome of a statement that returns no rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OkInfo {
    /// Rows changed by the statement
    pub affected_rows: u64,
    /// Last auto-generated ID, or 0
    pub last_insert_id: u64,
    /// The kind of statement, when it counts rows: `affected_rows` is
    /// then a count even when it is 0. TDS reports a count, and the
    /// command, only for these.
    pub command: OkCommand,
}

/// The kind of a statement that counts rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OkCommand {
    /// Counts no rows: DDL, `SET`, transaction statements and the like
    #[default]
    None,
    /// `INSERT`, `REPLACE`, `LOAD DATA`
    Insert,
    Update,
    Delete,
    /// `CALL`: the rows its statements changed
    Call,
}

/// Receives the results of executing one client request.
///
/// A request produces a sequence of results, each one either a result set
/// (`begin_result_set`, zero or more `row`, `end_result_set`), an `ok`, or
/// an `error`. An `error` may also arrive in the middle of a result set, in
/// place of `end_result_set`. Stored procedures and multi-statement batches
/// produce more than one result per request.
///
/// The encode methods are synchronous and buffer their output; the query
/// layer calls `flush` when `buffered_bytes` grows large, and the protocol
/// front-end flushes once the request is complete.
#[async_trait]
pub trait ResponseSink: Send + Sync + 'static {
    /// Whether the client can receive more than one result per request.
    /// When it cannot, statements nested in a stored procedure do not send
    /// their result sets.
    fn accepts_multiple_results(&self) -> bool;

    /// Whether the client asked to cancel the request in progress. The
    /// query layer stops producing rows when it sees this.
    fn cancelled(&self) -> bool {
        false
    }

    /// Start a result set with the given columns. `schema` is the current
    /// database, for protocols that report it in column metadata.
    fn begin_result_set(&mut self, columns: &[OutputColumn], schema: &str);

    /// Emit one row of the current result set.
    fn row(&mut self, row: &Row);

    /// Finish the current result set.
    fn end_result_set(&mut self, status: Status);

    /// Report a statement that completed without returning rows.
    fn ok(&mut self, info: OkInfo, status: Status);

    /// Report a failed statement.
    fn error(&mut self, error: &QueryError);

    /// Bytes encoded but not yet written to the client.
    fn buffered_bytes(&self) -> usize;

    /// Write buffered output to the client.
    async fn flush(&mut self) -> io::Result<()>;

    /// The concrete sink, so the protocol front-end that created it can
    /// reach its protocol-specific controls.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
