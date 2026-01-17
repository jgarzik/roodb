//! Join executor
//!
//! Implements nested loop join for all join types.

use async_trait::async_trait;

use crate::planner::logical::{JoinType, ResolvedExpr};

use super::datum::Datum;
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Nested loop join executor
pub struct NestedLoopJoin {
    /// Left input executor
    left: Box<dyn Executor>,
    /// Right input executor
    right: Box<dyn Executor>,
    /// Join type
    join_type: JoinType,
    /// Optional join condition
    condition: Option<ResolvedExpr>,
    /// Materialized right side rows
    right_rows: Vec<Row>,
    /// Current left row
    current_left: Option<Row>,
    /// Current position in right rows
    right_position: usize,
    /// Number of columns in left input (for outer joins)
    left_width: usize,
    /// Number of columns in right input (for outer joins)
    right_width: usize,
    /// For left/full join: whether current left row matched any right row
    left_matched: bool,
    /// For right/full join: which right rows have been matched
    right_matched: Vec<bool>,
    /// For right/full join: position for emitting unmatched right rows
    unmatched_right_pos: usize,
    /// Whether we've finished the main join loop
    main_loop_done: bool,
}

impl NestedLoopJoin {
    /// Create a new nested loop join executor
    ///
    /// `left_width` and `right_width` are the expected output widths.
    /// These are needed to create null-padded rows for outer joins
    /// even when one side is empty.
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        condition: Option<ResolvedExpr>,
        left_width: usize,
        right_width: usize,
    ) -> Self {
        NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            right_rows: Vec::new(),
            current_left: None,
            right_position: 0,
            left_width,
            right_width,
            left_matched: false,
            right_matched: Vec::new(),
            unmatched_right_pos: 0,
            main_loop_done: false,
        }
    }

    /// Create a null-padded row of the given width
    fn null_row(width: usize) -> Row {
        Row::new(vec![Datum::Null; width])
    }

    /// Check if the join condition matches for the combined row
    fn matches(&self, combined: &Row) -> ExecutorResult<bool> {
        match &self.condition {
            Some(cond) => {
                let result = eval(cond, combined)?;
                Ok(result.as_bool().unwrap_or(false))
            }
            None => Ok(true), // No condition = cross join
        }
    }
}

#[async_trait]
impl Executor for NestedLoopJoin {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.left.open().await?;
        self.right.open().await?;

        // Materialize right side
        self.right_rows.clear();
        while let Some(row) = self.right.next().await? {
            self.right_rows.push(row);
        }

        // Initialize right_matched for right/full joins
        self.right_matched = vec![false; self.right_rows.len()];

        // Get first left row
        self.current_left = self.left.next().await?;

        self.right_position = 0;
        self.left_matched = false;
        self.unmatched_right_pos = 0;
        self.main_loop_done = false;

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        // Main nested loop
        if !self.main_loop_done {
            while let Some(ref left_row) = self.current_left.clone() {
                while self.right_position < self.right_rows.len() {
                    let right_row = &self.right_rows[self.right_position];
                    self.right_position += 1;

                    let combined = Row::concat_ref(left_row, right_row);

                    if self.matches(&combined)? {
                        self.left_matched = true;
                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            self.right_matched[self.right_position - 1] = true;
                        }
                        return Ok(Some(combined));
                    }
                }

                // Finished scanning right side for this left row
                // For left/full outer join, emit unmatched left row
                if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let null_right = Self::null_row(self.right_width);
                    let combined = Row::concat_ref(left_row, &null_right);

                    // Move to next left row before returning
                    self.current_left = self.left.next().await?;
                    self.right_position = 0;
                    self.left_matched = false;

                    return Ok(Some(combined));
                }

                // Move to next left row
                self.current_left = self.left.next().await?;
                self.right_position = 0;
                self.left_matched = false;
            }

            self.main_loop_done = true;
        }

        // For right/full outer join, emit unmatched right rows
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            while self.unmatched_right_pos < self.right_rows.len() {
                let pos = self.unmatched_right_pos;
                self.unmatched_right_pos += 1;

                if !self.right_matched[pos] {
                    let null_left = Self::null_row(self.left_width);
                    let right_row = self.right_rows[pos].clone();
                    return Ok(Some(null_left.concat(right_row)));
                }
            }
        }

        Ok(None)
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.right_rows.clear();
        self.current_left = None;
        self.right_matched.clear();
        self.left.close().await?;
        self.right.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn test_cross_join() {
        let left_rows = vec![Row::new(vec![Datum::Int(1)]), Row::new(vec![Datum::Int(2)])];
        let right_rows = vec![
            Row::new(vec![Datum::String("a".to_string())]),
            Row::new(vec![Datum::String("b".to_string())]),
        ];

        let left = Box::new(MockExecutor {
            rows: left_rows,
            position: 0,
        });
        let right = Box::new(MockExecutor {
            rows: right_rows,
            position: 0,
        });

        let mut join = NestedLoopJoin::new(left, right, JoinType::Cross, None, 1, 1);
        join.open().await.unwrap();

        let mut count = 0;
        while join.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4); // 2 x 2 = 4

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_inner_join() {
        use crate::catalog::DataType;
        use crate::planner::logical::{BinaryOp, ResolvedColumn};

        // Left: (id, name)
        let left_rows = vec![
            Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]),
            Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]),
            Row::new(vec![Datum::Int(3), Datum::String("carol".to_string())]),
        ];
        // Right: (user_id, order)
        let right_rows = vec![
            Row::new(vec![Datum::Int(1), Datum::String("order1".to_string())]),
            Row::new(vec![Datum::Int(1), Datum::String("order2".to_string())]),
            Row::new(vec![Datum::Int(2), Datum::String("order3".to_string())]),
        ];

        let left = Box::new(MockExecutor {
            rows: left_rows,
            position: 0,
        });
        let right = Box::new(MockExecutor {
            rows: right_rows,
            position: 0,
        });

        // Condition: left.id = right.user_id (columns 0 and 2 in combined)
        let condition = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "l".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            })),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "r".to_string(),
                name: "user_id".to_string(),
                index: 2,
                data_type: DataType::Int,
                nullable: false,
            })),
            result_type: DataType::Boolean,
        };

        let mut join = NestedLoopJoin::new(left, right, JoinType::Inner, Some(condition), 2, 2);
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 3); // alice matches 2, bob matches 1, carol matches 0

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_left_join() {
        use crate::catalog::DataType;
        use crate::planner::logical::{BinaryOp, ResolvedColumn};

        let left_rows = vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
            Row::new(vec![Datum::Int(3)]), // no match
        ];
        let right_rows = vec![
            Row::new(vec![Datum::Int(1), Datum::String("a".to_string())]),
            Row::new(vec![Datum::Int(2), Datum::String("b".to_string())]),
        ];

        let left = Box::new(MockExecutor {
            rows: left_rows,
            position: 0,
        });
        let right = Box::new(MockExecutor {
            rows: right_rows,
            position: 0,
        });

        let condition = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "l".to_string(),
                name: "id".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
            })),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "r".to_string(),
                name: "id".to_string(),
                index: 1,
                data_type: DataType::Int,
                nullable: false,
            })),
            result_type: DataType::Boolean,
        };

        let mut join = NestedLoopJoin::new(left, right, JoinType::Left, Some(condition), 1, 2);
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 3); // 1 matches, 2 matches, 3 gets nulls

        // Check that row 3 has NULL in right columns
        let row3 = &results[2];
        assert_eq!(row3.get(0).unwrap().as_int(), Some(3));
        assert!(row3.get(1).unwrap().is_null());
        assert!(row3.get(2).unwrap().is_null());

        join.close().await.unwrap();
    }
}
