//! Union executor
//!
//! Concatenates results from two child executors.
//! UNION ALL returns all rows; UNION removes duplicates.

use std::collections::HashSet;

use async_trait::async_trait;

use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

/// Union executor: runs left side, then right side, concatenating results.
/// For UNION (not ALL), deduplicates rows via hash set.
pub struct Union {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_exhausted: bool,
    seen: Option<HashSet<Vec<super::datum::Datum>>>,
}

impl Union {
    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>, all: bool) -> Self {
        Union {
            left,
            right,
            left_exhausted: false,
            seen: if all { None } else { Some(HashSet::new()) },
        }
    }
}

#[async_trait]
impl Executor for Union {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.left.open().await?;
        self.right.open().await?;
        self.left_exhausted = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        loop {
            let row = if !self.left_exhausted {
                match self.left.next().await? {
                    Some(row) => Some(row),
                    None => {
                        self.left_exhausted = true;
                        self.right.next().await?
                    }
                }
            } else {
                self.right.next().await?
            };

            match row {
                Some(row) => {
                    if let Some(ref mut seen) = self.seen {
                        // UNION DISTINCT: skip duplicate rows
                        let key = row.values().to_vec();
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    return Ok(Some(row));
                }
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.left.close().await?;
        self.right.close().await?;
        Ok(())
    }
}
