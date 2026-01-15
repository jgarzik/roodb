//! Distinct executor
//!
//! Implements SELECT DISTINCT using a hash set.

use std::collections::HashSet;

use async_trait::async_trait;

use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

/// Hash-based distinct executor
pub struct HashDistinct {
    /// Input executor
    input: Box<dyn Executor>,
    /// Seen rows (for deduplication)
    seen: HashSet<Row>,
}

impl HashDistinct {
    /// Create a new distinct executor
    pub fn new(input: Box<dyn Executor>) -> Self {
        HashDistinct {
            input,
            seen: HashSet::new(),
        }
    }
}

#[async_trait]
impl Executor for HashDistinct {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.seen.clear();
        self.input.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        loop {
            match self.input.next().await? {
                Some(row) => {
                    if self.seen.insert(row.clone()) {
                        // Row was new, return it
                        return Ok(Some(row));
                    }
                    // Row was duplicate, continue
                }
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.seen.clear();
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
    async fn test_distinct() {
        let rows = vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
            Row::new(vec![Datum::Int(1)]), // duplicate
            Row::new(vec![Datum::Int(3)]),
            Row::new(vec![Datum::Int(2)]), // duplicate
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let mut distinct = HashDistinct::new(input);
        distinct.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = distinct.next().await.unwrap() {
            results.push(row.get(0).unwrap().as_int().unwrap());
        }

        assert_eq!(results.len(), 3);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(results.contains(&3));

        distinct.close().await.unwrap();
    }
}
