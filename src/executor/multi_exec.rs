//! Multi-executor — runs multiple executors sequentially
//!
//! Used for multi-table DROP TABLE and other compound DDL operations.

use async_trait::async_trait;

use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

use crate::raft::RowChange;

pub struct MultiExecutor {
    executors: Vec<Box<dyn Executor>>,
    current: usize,
}

impl MultiExecutor {
    pub fn new(executors: Vec<Box<dyn Executor>>) -> Self {
        Self {
            executors,
            current: 0,
        }
    }
}

#[async_trait]
impl Executor for MultiExecutor {
    async fn open(&mut self) -> ExecutorResult<()> {
        // Open and run each executor sequentially
        for exec in &mut self.executors {
            exec.open().await?;
            while exec.next().await?.is_some() {}
            exec.close().await?;
        }
        self.current = self.executors.len();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        Ok(None)
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        let mut all_changes = Vec::new();
        for exec in &mut self.executors {
            all_changes.extend(exec.take_changes());
        }
        all_changes
    }
}
