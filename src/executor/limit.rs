//! Limit executor
//!
//! Implements LIMIT and OFFSET for query results.

use async_trait::async_trait;

use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

/// Limit executor
pub struct Limit {
    /// Input executor
    input: Box<dyn Executor>,
    /// Maximum rows to return (None = unlimited)
    limit: Option<u64>,
    /// Rows to skip
    offset: u64,
    /// Number of rows skipped so far
    skipped: u64,
    /// Number of rows returned so far
    returned: u64,
}

impl Limit {
    /// Create a new limit executor
    pub fn new(input: Box<dyn Executor>, limit: Option<u64>, offset: Option<u64>) -> Self {
        Limit {
            input,
            limit,
            offset: offset.unwrap_or(0),
            skipped: 0,
            returned: 0,
        }
    }
}

#[async_trait]
impl Executor for Limit {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.skipped = 0;
        self.returned = 0;
        self.input.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        // Check if we've reached the limit
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                return Ok(None);
            }
        }

        loop {
            match self.input.next().await? {
                Some(row) => {
                    // Skip rows for offset
                    if self.skipped < self.offset {
                        self.skipped += 1;
                        continue;
                    }

                    self.returned += 1;
                    return Ok(Some(row));
                }
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.input.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::datum::Datum;

    struct MockExecutor {
        rows: Vec<Row>,
        position: usize,
    }

    #[async_trait]
    impl Executor for MockExecutor {
        async fn open(&mut self) -> ExecutorResult<()> {
            self.position = 0;
            Ok(())
        }

        async fn next(&mut self) -> ExecutorResult<Option<Row>> {
            if self.position >= self.rows.len() {
                return Ok(None);
            }
            let row = self.rows[self.position].clone();
            self.position += 1;
            Ok(Some(row))
        }

        async fn close(&mut self) -> ExecutorResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_limit_only() {
        let rows: Vec<Row> = (0..10).map(|i| Row::new(vec![Datum::Int(i)])).collect();
        let input = Box::new(MockExecutor { rows, position: 0 });

        let mut limit = Limit::new(input, Some(3), None);
        limit.open().await.unwrap();

        let r1 = limit.next().await.unwrap().unwrap();
        assert_eq!(r1.get(0).unwrap().as_int(), Some(0));

        let r2 = limit.next().await.unwrap().unwrap();
        assert_eq!(r2.get(0).unwrap().as_int(), Some(1));

        let r3 = limit.next().await.unwrap().unwrap();
        assert_eq!(r3.get(0).unwrap().as_int(), Some(2));

        assert!(limit.next().await.unwrap().is_none());

        limit.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_offset_only() {
        let rows: Vec<Row> = (0..5).map(|i| Row::new(vec![Datum::Int(i)])).collect();
        let input = Box::new(MockExecutor { rows, position: 0 });

        let mut limit = Limit::new(input, None, Some(3));
        limit.open().await.unwrap();

        let r1 = limit.next().await.unwrap().unwrap();
        assert_eq!(r1.get(0).unwrap().as_int(), Some(3));

        let r2 = limit.next().await.unwrap().unwrap();
        assert_eq!(r2.get(0).unwrap().as_int(), Some(4));

        assert!(limit.next().await.unwrap().is_none());

        limit.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_limit_and_offset() {
        let rows: Vec<Row> = (0..10).map(|i| Row::new(vec![Datum::Int(i)])).collect();
        let input = Box::new(MockExecutor { rows, position: 0 });

        let mut limit = Limit::new(input, Some(2), Some(5));
        limit.open().await.unwrap();

        let r1 = limit.next().await.unwrap().unwrap();
        assert_eq!(r1.get(0).unwrap().as_int(), Some(5));

        let r2 = limit.next().await.unwrap().unwrap();
        assert_eq!(r2.get(0).unwrap().as_int(), Some(6));

        assert!(limit.next().await.unwrap().is_none());

        limit.close().await.unwrap();
    }
}
