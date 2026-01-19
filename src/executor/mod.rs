//! Query executor - Volcano iterator model
//!
//! The executor takes a physical plan and executes it using the Volcano
//! iterator model: each operator implements open/next/close methods.

pub mod aggregate;
pub mod auth;
pub mod context;
pub mod datum;
pub mod ddl;
pub mod delete;
pub mod distinct;
pub mod encoding;
pub mod engine;
pub mod error;
pub mod eval;
pub mod filter;
pub mod insert;
pub mod join;
pub mod limit;
pub mod project;
pub mod row;
pub mod scan;
pub mod single_row;
pub mod sort;
pub mod update;

pub use context::TransactionContext;
pub use datum::Datum;
pub use engine::ExecutorEngine;
pub use error::{ExecutorError, ExecutorResult};
pub use row::Row;

use async_trait::async_trait;

use crate::raft::RowChange;

/// Volcano-style iterator executor
///
/// Each operator implements:
/// - `open()`: Initialize the operator
/// - `next()`: Return the next row, or None if exhausted
/// - `close()`: Clean up resources
/// - `take_changes()`: Extract collected row changes (DML executors only)
#[async_trait]
pub trait Executor: Send {
    /// Initialize the executor
    async fn open(&mut self) -> ExecutorResult<()>;

    /// Get the next row, or None if exhausted
    async fn next(&mut self) -> ExecutorResult<Option<Row>>;

    /// Close the executor and release resources
    async fn close(&mut self) -> ExecutorResult<()>;

    /// Take collected row changes for Raft replication
    ///
    /// DML executors (Insert, Update, Delete) collect row changes during
    /// execution. After the executor completes, call this to extract the
    /// changes for proposing to Raft consensus.
    ///
    /// Default implementation returns empty vec (read-only operators).
    fn take_changes(&mut self) -> Vec<RowChange> {
        Vec::new()
    }
}

/// Execution result for DML/DDL operations
#[derive(Debug, Clone)]
pub struct ExecResult {
    /// Number of rows affected
    pub rows_affected: u64,
    /// Optional message
    pub message: Option<String>,
}

impl ExecResult {
    /// Create a new execution result
    pub fn new(rows_affected: u64) -> Self {
        ExecResult {
            rows_affected,
            message: None,
        }
    }

    /// Create an execution result with a message
    pub fn with_message(rows_affected: u64, message: impl Into<String>) -> Self {
        ExecResult {
            rows_affected,
            message: Some(message.into()),
        }
    }
}
