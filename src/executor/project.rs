//! Project executor
//!
//! Projects (transforms) rows by evaluating expressions.

use async_trait::async_trait;

use crate::planner::logical::ResolvedExpr;

use crate::server::session::UserVariables;

use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use super::Executor;

/// Project executor
pub struct Project {
    /// Input executor
    input: Box<dyn Executor>,
    /// Expressions to evaluate (with aliases)
    expressions: Vec<(ResolvedExpr, String)>,
    /// User variables
    user_variables: UserVariables,
}

impl Project {
    /// Create a new project executor
    pub fn new(
        input: Box<dyn Executor>,
        expressions: Vec<(ResolvedExpr, String)>,
        user_variables: UserVariables,
    ) -> Self {
        Project {
            input,
            expressions,
            user_variables,
        }
    }
}

#[async_trait]
impl Executor for Project {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.input.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        match self.input.next().await? {
            Some(input_row) => {
                let mut values = Vec::with_capacity(self.expressions.len());
                for (expr, _alias) in &self.expressions {
                    let datum = evaluate(expr, &input_row, &self.user_variables)?;
                    values.push(datum);
                }
                Ok(Some(Row::new(values)))
            }
            None => Ok(None),
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
    use crate::planner::logical::{BinaryOp, ResolvedColumn};

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

    use crate::server::session::UserVariables;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[tokio::test]
    async fn test_project() {
        let rows = vec![
            Row::new(vec![Datum::Int(1), Datum::Int(10)]),
            Row::new(vec![Datum::Int(2), Datum::Int(20)]),
        ];

        let input = Box::new(MockExecutor { rows, position: 0 });

        // Project: col0, col0 + col1
        let expressions = vec![
            (
                ResolvedExpr::Column(ResolvedColumn {
                    table: "t".to_string(),
                    name: "a".to_string(),
                    index: 0,
                    data_type: DataType::Int,
                    nullable: false,
                    default_value: None,
                    is_outer_ref: false,
                }),
                "a".to_string(),
            ),
            (
                ResolvedExpr::BinaryOp {
                    left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                        table: "t".to_string(),
                        name: "a".to_string(),
                        index: 0,
                        data_type: DataType::Int,
                        nullable: false,
                        default_value: None,
                        is_outer_ref: false,
                    })),
                    op: BinaryOp::Add,
                    right: Box::new(ResolvedExpr::Column(ResolvedColumn {
                        table: "t".to_string(),
                        name: "b".to_string(),
                        index: 1,
                        data_type: DataType::Int,
                        nullable: false,
                        default_value: None,
                        is_outer_ref: false,
                    })),
                    result_type: DataType::BigInt,
                },
                "sum".to_string(),
            ),
        ];

        let mut project = Project::new(input, expressions, empty_vars());
        project.open().await.unwrap();

        let r1 = project.next().await.unwrap().unwrap();
        assert_eq!(r1.len(), 2);
        assert_eq!(r1.get(0).unwrap().as_int(), Some(1));
        assert_eq!(r1.get(1).unwrap().as_int(), Some(11));

        let r2 = project.next().await.unwrap().unwrap();
        assert_eq!(r2.get(0).unwrap().as_int(), Some(2));
        assert_eq!(r2.get(1).unwrap().as_int(), Some(22));

        project.close().await.unwrap();
    }
}
