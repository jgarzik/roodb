//! Sort executor
//!
//! Implements ORDER BY by collecting all rows, sorting them, then emitting.

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;

use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Sort executor
pub struct Sort {
    /// Input executor
    input: Box<dyn Executor>,
    /// Order by expressions with ascending flag
    order_by: Vec<(ResolvedExpr, bool)>,
    /// Collected and sorted rows
    rows: Vec<Row>,
    /// Current position in sorted rows
    position: usize,
}

impl Sort {
    /// Create a new sort executor
    pub fn new(input: Box<dyn Executor>, order_by: Vec<(ResolvedExpr, bool)>) -> Self {
        Sort {
            input,
            order_by,
            rows: Vec::new(),
            position: 0,
        }
    }
}

#[async_trait]
impl Executor for Sort {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.input.open().await?;

        // Collect all rows
        self.rows.clear();
        while let Some(row) = self.input.next().await? {
            self.rows.push(row);
        }

        // Sort rows
        // Note: We need to handle errors during sort, but Rust's sort_by
        // doesn't support fallible comparisons. We evaluate sort keys upfront.
        let order_by = &self.order_by;

        // Precompute sort keys for each row
        let mut keyed_rows: Vec<(Vec<super::datum::Datum>, Row)> =
            Vec::with_capacity(self.rows.len());
        for row in self.rows.drain(..) {
            let mut keys = Vec::with_capacity(order_by.len());
            for (expr, _) in order_by {
                keys.push(eval(expr, &row)?);
            }
            keyed_rows.push((keys, row));
        }

        // Sort by keys
        keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| {
            for (i, (key_a, key_b)) in keys_a.iter().zip(keys_b.iter()).enumerate() {
                let ascending = order_by.get(i).map(|(_, asc)| *asc).unwrap_or(true);
                let cmp = key_a.cmp(key_b);
                let cmp = if ascending { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Extract sorted rows
        self.rows = keyed_rows.into_iter().map(|(_, row)| row).collect();
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
        self.rows.clear();
        self.position = 0;
        self.input.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::executor::datum::Datum;
    use crate::planner::logical::ResolvedColumn;

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
    async fn test_sort_ascending() {
        let rows = vec![
            Row::new(vec![Datum::Int(3)]),
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let order_by = vec![(
            ResolvedExpr::Column(ResolvedColumn {
                table: "t".to_string(),
                name: "c".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            }),
            true, // ascending
        )];

        let mut sort = Sort::new(input, order_by);
        sort.open().await.unwrap();

        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(1)
        );
        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(2)
        );
        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(3)
        );
        assert!(sort.next().await.unwrap().is_none());

        sort.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sort_descending() {
        let rows = vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(3)]),
            Row::new(vec![Datum::Int(2)]),
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let order_by = vec![(
            ResolvedExpr::Column(ResolvedColumn {
                table: "t".to_string(),
                name: "c".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            }),
            false, // descending
        )];

        let mut sort = Sort::new(input, order_by);
        sort.open().await.unwrap();

        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(3)
        );
        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(2)
        );
        assert_eq!(
            sort.next().await.unwrap().unwrap().get(0).unwrap().as_int(),
            Some(1)
        );

        sort.close().await.unwrap();
    }
}
