//! Filter executor
//!
//! Filters rows based on a predicate expression.

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;

use crate::server::session::UserVariables;

use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Filter executor
pub struct Filter {
    /// Input executor
    input: Box<dyn Executor>,
    /// Filter predicate
    predicate: ResolvedExpr,
    /// User variables
    user_variables: UserVariables,
}

impl Filter {
    /// Create a new filter executor
    pub fn new(
        input: Box<dyn Executor>,
        predicate: ResolvedExpr,
        user_variables: UserVariables,
    ) -> Self {
        Filter {
            input,
            predicate,
            user_variables,
        }
    }
}

#[async_trait]
impl Executor for Filter {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.input.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        loop {
            match self.input.next().await? {
                Some(row) => {
                    let result = evaluate(&self.predicate, &row, &self.user_variables)?;
                    if result.as_bool().unwrap_or(false) {
                        return Ok(Some(row));
                    }
                    // Row didn't match, continue to next
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
    use crate::catalog::DataType;
    use crate::executor::datum::Datum;
    use crate::planner::logical::{BinaryOp, Literal, ResolvedColumn};
    use crate::server::session::UserVariables;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

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
    async fn test_filter() {
        let rows = vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
            Row::new(vec![Datum::Int(3)]),
        ];

        let input = Box::new(MockExecutor { rows, position: 0 });

        // Filter: column > 1
        let predicate = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "t".to_string(),
                name: "c".to_string(),
                index: 0,
                data_type: DataType::Int,
                nullable: false,
                default_value: None,
            })),
            op: BinaryOp::Gt,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            result_type: DataType::Boolean,
        };

        let mut filter = Filter::new(input, predicate, empty_vars());
        filter.open().await.unwrap();

        // Should get 2 and 3 only
        let r1 = filter.next().await.unwrap();
        assert_eq!(r1.unwrap().get(0).unwrap().as_int(), Some(2));

        let r2 = filter.next().await.unwrap();
        assert_eq!(r2.unwrap().get(0).unwrap().as_int(), Some(3));

        let r3 = filter.next().await.unwrap();
        assert!(r3.is_none());

        filter.close().await.unwrap();
    }
}
