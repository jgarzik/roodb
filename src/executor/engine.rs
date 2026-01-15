//! Executor engine
//!
//! Builds executor trees from physical plans.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::catalog::Catalog;
use crate::planner::PhysicalPlan;
use crate::txn::MvccStorage;

use super::aggregate::HashAggregate;
use super::context::TransactionContext;
use super::ddl::{CreateIndex, CreateTable, DropIndex, DropTable};
use super::delete::Delete;
use super::distinct::HashDistinct;
use super::error::ExecutorResult;
use super::filter::Filter;
use super::insert::Insert;
use super::join::NestedLoopJoin;
use super::limit::Limit;
use super::project::Project;
use super::scan::TableScan;
use super::sort::Sort;
use super::update::Update;
use super::Executor;

/// Executor engine - builds executors from physical plans
pub struct ExecutorEngine {
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Catalog
    catalog: Arc<RwLock<Catalog>>,
    /// Transaction context (None for DDL-only or read-only without txn)
    txn_context: Option<TransactionContext>,
}

impl ExecutorEngine {
    /// Create a new executor engine
    pub fn new(
        mvcc: Arc<MvccStorage>,
        catalog: Arc<RwLock<Catalog>>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        ExecutorEngine {
            mvcc,
            catalog,
            txn_context,
        }
    }

    /// Build an executor tree from a physical plan
    pub fn build(&self, plan: PhysicalPlan) -> ExecutorResult<Box<dyn Executor>> {
        self.build_node(plan)
    }

    fn build_node(&self, plan: PhysicalPlan) -> ExecutorResult<Box<dyn Executor>> {
        match plan {
            PhysicalPlan::TableScan {
                table,
                columns: _,
                filter,
            } => Ok(Box::new(TableScan::new(
                table,
                filter,
                self.mvcc.clone(),
                self.txn_context.clone(),
            ))),

            PhysicalPlan::Filter { input, predicate } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Filter::new(input_exec, predicate)))
            }

            PhysicalPlan::Project { input, expressions } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Project::new(input_exec, expressions)))
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_exec = self.build_node(*left)?;
                let right_exec = self.build_node(*right)?;
                Ok(Box::new(NestedLoopJoin::new(
                    left_exec, right_exec, join_type, condition,
                )))
            }

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(HashAggregate::new(
                    input_exec, group_by, aggregates,
                )))
            }

            PhysicalPlan::Sort { input, order_by } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Sort::new(input_exec, order_by)))
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Limit::new(input_exec, limit, offset)))
            }

            PhysicalPlan::HashDistinct { input } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(HashDistinct::new(input_exec)))
            }

            PhysicalPlan::Insert {
                table,
                columns,
                values,
            } => Ok(Box::new(Insert::new(
                table,
                columns,
                values,
                self.mvcc.clone(),
                self.txn_context.clone(),
            ))),

            PhysicalPlan::Update {
                table,
                assignments,
                filter,
            } => Ok(Box::new(Update::new(
                table,
                assignments,
                filter,
                self.mvcc.clone(),
                self.txn_context.clone(),
            ))),

            PhysicalPlan::Delete { table, filter } => Ok(Box::new(Delete::new(
                table,
                filter,
                self.mvcc.clone(),
                self.txn_context.clone(),
            ))),

            PhysicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => Ok(Box::new(CreateTable::new(
                name,
                columns,
                constraints,
                if_not_exists,
                self.catalog.clone(),
            ))),

            PhysicalPlan::DropTable { name, if_exists } => Ok(Box::new(DropTable::new(
                name,
                if_exists,
                self.catalog.clone(),
            ))),

            PhysicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => Ok(Box::new(CreateIndex::new(
                name,
                table,
                columns,
                unique,
                self.catalog.clone(),
            ))),

            PhysicalPlan::DropIndex { name } => {
                Ok(Box::new(DropIndex::new(name, self.catalog.clone())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, DataType, TableDef};
    use crate::executor::datum::Datum;
    use crate::executor::encoding::{encode_row, encode_row_key};
    use crate::executor::row::Row;
    use crate::planner::logical::expr::OutputColumn;
    use crate::sql::{Literal, ResolvedColumn, ResolvedExpr};
    use crate::storage::traits::KeyValue;
    use crate::storage::{StorageEngine, StorageResult};
    use crate::txn::TransactionManager;
    use std::sync::Mutex;

    struct MockStorage {
        data: Mutex<Vec<KeyValue>>,
    }

    impl MockStorage {
        fn new(initial: Vec<KeyValue>) -> Self {
            MockStorage {
                data: Mutex::new(initial),
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let data = self.data.lock().unwrap();
            Ok(data.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
            let mut data = self.data.lock().unwrap();
            if let Some(entry) = data.iter_mut().find(|(k, _)| k == key) {
                entry.1 = value.to_vec();
            } else {
                data.push((key.to_vec(), value.to_vec()));
            }
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> StorageResult<()> {
            let mut data = self.data.lock().unwrap();
            data.retain(|(k, _)| k != key);
            Ok(())
        }

        async fn scan(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> StorageResult<Vec<KeyValue>> {
            let data = self.data.lock().unwrap();
            let filtered: Vec<_> = data
                .iter()
                .filter(|(k, _)| {
                    let after_start = start.map_or(true, |s| k.as_slice() >= s);
                    let before_end = end.map_or(true, |e| k.as_slice() < e);
                    after_start && before_end
                })
                .cloned()
                .collect();
            Ok(filtered)
        }

        async fn flush(&self) -> StorageResult<()> {
            Ok(())
        }

        async fn close(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    fn setup_test_env() -> (ExecutorEngine, Arc<MvccStorage>) {
        let row1 = Row::new(vec![Datum::Int(1), Datum::String("alice".to_string())]);
        let row2 = Row::new(vec![Datum::Int(2), Datum::String("bob".to_string())]);
        let row3 = Row::new(vec![Datum::Int(3), Datum::String("carol".to_string())]);

        let initial = vec![
            (encode_row_key("users", 1), encode_row(&row1)),
            (encode_row_key("users", 2), encode_row(&row2)),
            (encode_row_key("users", 3), encode_row(&row3)),
        ];

        let storage = Arc::new(MockStorage::new(initial));
        let txn_manager = Arc::new(TransactionManager::new());
        let mvcc = Arc::new(MvccStorage::new(
            storage as Arc<dyn StorageEngine>,
            txn_manager,
        ));
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Add table to catalog
        {
            let mut cat = catalog.write();
            cat.create_table(
                TableDef::new("users")
                    .column(ColumnDef::new("id", DataType::Int).nullable(false))
                    .column(ColumnDef::new("name", DataType::Varchar(100))),
            )
            .unwrap();
        }

        // No transaction context for simple tests (legacy behavior)
        let engine = ExecutorEngine::new(mvcc.clone(), catalog, None);
        (engine, mvcc)
    }

    #[tokio::test]
    async fn test_build_table_scan() {
        let (engine, _) = setup_test_env();

        let plan = PhysicalPlan::TableScan {
            table: "users".to_string(),
            columns: vec![
                OutputColumn {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                OutputColumn {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Varchar(100),
                    nullable: true,
                },
            ],
            filter: None,
        };

        let mut exec = engine.build(plan).unwrap();
        exec.open().await.unwrap();

        let mut count = 0;
        while exec.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        exec.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_build_project_filter() {
        let (engine, _) = setup_test_env();

        // SELECT id FROM users WHERE id > 1
        let scan = PhysicalPlan::TableScan {
            table: "users".to_string(),
            columns: vec![
                OutputColumn {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                OutputColumn {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Varchar(100),
                    nullable: true,
                },
            ],
            filter: Some(ResolvedExpr::BinaryOp {
                left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                    table: "users".to_string(),
                    name: "id".to_string(),
                    index: 0,
                    data_type: DataType::Int,
                    nullable: false,
                })),
                op: crate::sql::BinaryOp::Gt,
                right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
                result_type: DataType::Boolean,
            }),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![(
                ResolvedExpr::Column(ResolvedColumn {
                    table: "users".to_string(),
                    name: "id".to_string(),
                    index: 0,
                    data_type: DataType::Int,
                    nullable: false,
                }),
                "id".to_string(),
            )],
        };

        let mut exec = engine.build(project).unwrap();
        exec.open().await.unwrap();

        let mut ids = Vec::new();
        while let Some(row) = exec.next().await.unwrap() {
            ids.push(row.get(0).unwrap().as_int().unwrap());
        }

        assert_eq!(ids, vec![2, 3]);

        exec.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_build_limit() {
        let (engine, _) = setup_test_env();

        let scan = PhysicalPlan::TableScan {
            table: "users".to_string(),
            columns: vec![OutputColumn {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
            }],
            filter: None,
        };

        let limit = PhysicalPlan::Limit {
            input: Box::new(scan),
            limit: Some(2),
            offset: Some(1),
        };

        let mut exec = engine.build(limit).unwrap();
        exec.open().await.unwrap();

        let mut count = 0;
        while exec.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);

        exec.close().await.unwrap();
    }
}
