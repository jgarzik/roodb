//! Single row executor (TABLE_DEE)

use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

/// Executor that yields exactly one empty row (TABLE_DEE)
pub struct SingleRow {
    returned: bool,
}

impl SingleRow {
    pub fn new() -> Self {
        SingleRow { returned: false }
    }
}

impl Default for SingleRow {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Executor for SingleRow {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.returned = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.returned {
            Ok(None)
        } else {
            self.returned = true;
            Ok(Some(Row::empty()))
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }
}
