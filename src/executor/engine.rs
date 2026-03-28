//! Executor engine
//!
//! Builds executor trees from physical plans.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::catalog::Catalog;
use crate::planner::PhysicalPlan;
use crate::raft::RaftNode;
use crate::server::session::UserVariables;
use crate::txn::MvccStorage;

use super::aggregate::HashAggregate;
use super::analyze::AnalyzeTable;
use super::auth::{AlterUser, CreateUser, DropUser, Grant, Revoke, SetPassword, ShowGrants};
use super::context::TransactionContext;
use super::ddl::{
    CreateDatabase, CreateIndex, CreateTable, CreateTableAs, CreateTableAsParams, DropDatabase,
    DropIndex, DropTable, Materialize,
};
use super::delete::Delete;
use super::distinct::HashDistinct;
use super::error::{ExecutorError, ExecutorResult};
use super::explain_exec::ExplainExecutor;
use super::filter::Filter;
use super::hash_join::HashJoin;
use super::insert::Insert;
use super::insert_select::InsertSelect;
use super::join::NestedLoopJoin;
use super::limit::Limit;
use super::point_get::PointGet;
use super::project::Project;
use super::range_scan::RangeScan;
use super::scan::TableScan;
use super::single_row::SingleRow;
use super::sort::Sort;
use super::union::Union;
use super::update::{Update, UpdateParams};
use super::Executor;

/// Executor engine - builds executors from physical plans
pub struct ExecutorEngine {
    /// MVCC-aware storage
    mvcc: Arc<MvccStorage>,
    /// Catalog
    catalog: Arc<RwLock<Catalog>>,
    /// Transaction context (None for DDL-only or read-only without txn)
    txn_context: Option<TransactionContext>,
    /// Optional Raft node for replication (DDL goes through Raft)
    raft_node: Option<Arc<RaftNode>>,
    /// User variables
    user_variables: UserVariables,
}

impl ExecutorEngine {
    /// Create a new executor engine
    pub fn new(
        mvcc: Arc<MvccStorage>,
        catalog: Arc<RwLock<Catalog>>,
        txn_context: Option<TransactionContext>,
        user_variables: UserVariables,
    ) -> Self {
        ExecutorEngine {
            mvcc,
            catalog,
            txn_context,
            raft_node: None,
            user_variables,
        }
    }

    /// Create a new executor engine with Raft support for DDL replication
    pub fn with_raft(
        mvcc: Arc<MvccStorage>,
        catalog: Arc<RwLock<Catalog>>,
        txn_context: Option<TransactionContext>,
        raft_node: Arc<RaftNode>,
        user_variables: UserVariables,
    ) -> Self {
        ExecutorEngine {
            mvcc,
            catalog,
            txn_context,
            raft_node: Some(raft_node),
            user_variables,
        }
    }

    /// Build an executor tree from a physical plan
    pub fn build(&self, plan: PhysicalPlan) -> ExecutorResult<Box<dyn Executor>> {
        self.build_node(plan)
    }

    fn build_node(&self, plan: PhysicalPlan) -> ExecutorResult<Box<dyn Executor>> {
        match plan {
            PhysicalPlan::SingleRow => Ok(Box::new(SingleRow::new())),

            PhysicalPlan::TableScan {
                table,
                columns,
                filter,
            } => {
                let schema_types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();
                Ok(Box::new(TableScan::with_schema_types(
                    table,
                    filter,
                    self.mvcc.clone(),
                    self.txn_context.clone(),
                    self.user_variables.clone(),
                    schema_types,
                )))
            }

            PhysicalPlan::PointGet {
                table,
                columns: _,
                key_value,
            } => Ok(Box::new(PointGet::new(
                table,
                key_value,
                self.mvcc.clone(),
                self.txn_context.clone(),
                self.user_variables.clone(),
            ))),

            PhysicalPlan::RangeScan {
                table,
                columns: _,
                start_key,
                end_key,
                inclusive_start,
                inclusive_end,
                remaining_filter,
            } => Ok(Box::new(RangeScan::new(
                table,
                super::range_scan::RangeScanBounds {
                    start_expr: start_key,
                    end_expr: end_key,
                    inclusive_start,
                    inclusive_end,
                    remaining_filter,
                },
                self.mvcc.clone(),
                self.txn_context.clone(),
                self.user_variables.clone(),
            ))),

            PhysicalPlan::Filter { input, predicate } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Filter::new(
                    input_exec,
                    predicate,
                    self.user_variables.clone(),
                )))
            }

            PhysicalPlan::Project { input, expressions } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Project::new(
                    input_exec,
                    expressions,
                    self.user_variables.clone(),
                )))
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
            } => {
                // Get output widths before building child executors
                let left_width = left.output_columns().len();
                let right_width = right.output_columns().len();

                let left_exec = self.build_node(*left)?;
                let right_exec = self.build_node(*right)?;
                Ok(Box::new(NestedLoopJoin::new(
                    left_exec,
                    right_exec,
                    join_type,
                    condition,
                    left_width,
                    right_width,
                    self.user_variables.clone(),
                )))
            }

            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                condition,
            } => {
                let left_width = left.output_columns().len();
                let right_width = right.output_columns().len();

                let left_exec = self.build_node(*left)?;
                let right_exec = self.build_node(*right)?;
                Ok(Box::new(HashJoin::new(
                    left_exec,
                    right_exec,
                    join_type,
                    left_keys,
                    right_keys,
                    condition,
                    left_width,
                    right_width,
                    self.user_variables.clone(),
                )))
            }

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(HashAggregate::new(
                    input_exec,
                    group_by,
                    aggregates,
                    self.user_variables.clone(),
                )))
            }

            PhysicalPlan::Sort { input, order_by } => {
                let input_exec = self.build_node(*input)?;
                Ok(Box::new(Sort::new(
                    input_exec,
                    order_by,
                    self.user_variables.clone(),
                )))
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

            PhysicalPlan::Union { left, right, all } => {
                let left_exec = self.build_node(*left)?;
                let right_exec = self.build_node(*right)?;
                Ok(Box::new(Union::new(left_exec, right_exec, all)))
            }

            PhysicalPlan::Insert {
                table,
                columns,
                values,
                auto_increment_indices,
                pk_column_indices,
                ignore,
            } => Ok(Box::new(Insert::new(
                table,
                columns,
                values,
                self.txn_context.clone(),
                auto_increment_indices,
                pk_column_indices,
                self.user_variables.clone(),
                ignore,
            ))),

            PhysicalPlan::InsertSelect {
                table,
                columns,
                source,
                auto_increment_indices,
                pk_column_indices,
                ignore,
            } => {
                let source_exec = self.build_node(*source)?;
                Ok(Box::new(InsertSelect::new(
                    table,
                    columns,
                    source_exec,
                    self.txn_context.clone(),
                    auto_increment_indices,
                    pk_column_indices,
                    ignore,
                )))
            }

            PhysicalPlan::Update {
                table,
                assignments,
                filter,
                key_value,
                order_by,
                limit,
            } => Ok(Box::new(Update::new(UpdateParams {
                table,
                assignments,
                filter,
                key_value,
                order_by,
                limit,
                mvcc: self.mvcc.clone(),
                txn_context: self.txn_context.clone(),
                user_variables: self.user_variables.clone(),
            }))),

            PhysicalPlan::Delete {
                table,
                filter,
                key_value,
            } => Ok(Box::new(Delete::new(
                table,
                filter,
                key_value,
                self.mvcc.clone(),
                self.txn_context.clone(),
                self.user_variables.clone(),
            ))),

            PhysicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(CreateTable::with_raft(
                        name,
                        columns,
                        constraints,
                        if_not_exists,
                        self.catalog.clone(),
                        raft_node.clone(),
                    )))
                } else {
                    Ok(Box::new(CreateTable::new(
                        name,
                        columns,
                        constraints,
                        if_not_exists,
                        self.catalog.clone(),
                    )))
                }
            }

            PhysicalPlan::CreateTableAs {
                name,
                columns,
                constraints,
                if_not_exists,
                source,
            } => {
                let source_exec = self.build_node(*source)?;
                Ok(Box::new(CreateTableAs::new(CreateTableAsParams {
                    name,
                    columns,
                    constraints,
                    if_not_exists,
                    catalog: self.catalog.clone(),
                    raft_node: self.raft_node.clone(),
                    source: source_exec,
                    txn_context: self.txn_context.clone(),
                })))
            }

            PhysicalPlan::Materialize { input } => {
                let inner = self.build_node(*input)?;
                Ok(Box::new(Materialize::new(inner)))
            }

            // CreateView and DropView are handled at the protocol level (Raft path),
            // they should never reach the executor engine.
            PhysicalPlan::CreateView { .. } | PhysicalPlan::DropView { .. } => {
                Err(ExecutorError::Internal(
                    "CreateView/DropView should be handled at protocol level".to_string(),
                ))
            }

            PhysicalPlan::DropTable { name, if_exists } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(DropTable::with_raft(
                        name,
                        if_exists,
                        self.catalog.clone(),
                        raft_node.clone(),
                        self.mvcc.clone(),
                    )))
                } else {
                    Ok(Box::new(DropTable::new(
                        name,
                        if_exists,
                        self.catalog.clone(),
                    )))
                }
            }

            PhysicalPlan::DropMultipleTables { names, if_exists } => {
                let mut executors: Vec<Box<dyn Executor>> = Vec::new();
                for name in names {
                    if let Some(ref raft_node) = self.raft_node {
                        executors.push(Box::new(DropTable::with_raft(
                            name,
                            if_exists,
                            self.catalog.clone(),
                            raft_node.clone(),
                            self.mvcc.clone(),
                        )));
                    } else {
                        executors.push(Box::new(DropTable::new(
                            name,
                            if_exists,
                            self.catalog.clone(),
                        )));
                    }
                }
                Ok(Box::new(super::multi_exec::MultiExecutor::new(executors)))
            }

            PhysicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(CreateIndex::with_raft(
                        name,
                        table,
                        columns,
                        unique,
                        self.catalog.clone(),
                        raft_node.clone(),
                    )))
                } else {
                    Ok(Box::new(CreateIndex::new(
                        name,
                        table,
                        columns,
                        unique,
                        self.catalog.clone(),
                    )))
                }
            }

            PhysicalPlan::DropIndex { name } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(DropIndex::with_raft(
                        name,
                        self.catalog.clone(),
                        raft_node.clone(),
                        self.mvcc.clone(),
                    )))
                } else {
                    Ok(Box::new(DropIndex::new(name, self.catalog.clone())))
                }
            }

            PhysicalPlan::CreateDatabase {
                name,
                if_not_exists,
            } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(CreateDatabase::with_raft(
                        name,
                        if_not_exists,
                        self.catalog.clone(),
                        raft_node.clone(),
                    )))
                } else {
                    Ok(Box::new(CreateDatabase::new(
                        name,
                        if_not_exists,
                        self.catalog.clone(),
                    )))
                }
            }

            PhysicalPlan::DropDatabase { name, if_exists } => {
                if let Some(ref raft_node) = self.raft_node {
                    Ok(Box::new(DropDatabase::with_raft(
                        name,
                        if_exists,
                        self.catalog.clone(),
                        raft_node.clone(),
                        self.mvcc.clone(),
                    )))
                } else {
                    Ok(Box::new(DropDatabase::new(
                        name,
                        if_exists,
                        self.catalog.clone(),
                    )))
                }
            }

            // ============ Auth Operations ============
            PhysicalPlan::CreateUser {
                username,
                host,
                password,
                if_not_exists,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(CreateUser::new(
                    username,
                    host,
                    password,
                    if_not_exists,
                    raft_node,
                    self.mvcc.inner().clone(),
                )))
            }

            PhysicalPlan::DropUser {
                username,
                host,
                if_exists,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(DropUser::new(
                    username,
                    host,
                    if_exists,
                    raft_node,
                    self.mvcc.inner().clone(),
                )))
            }

            PhysicalPlan::Grant {
                privileges,
                object,
                grantee,
                grantee_host,
                with_grant_option,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(Grant::new(
                    privileges,
                    object,
                    grantee,
                    grantee_host,
                    with_grant_option,
                    raft_node,
                )))
            }

            PhysicalPlan::Revoke {
                privileges,
                object,
                grantee,
                grantee_host,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(Revoke::new(
                    privileges,
                    object,
                    grantee,
                    grantee_host,
                    raft_node,
                    self.mvcc.inner().clone(),
                )))
            }

            PhysicalPlan::ShowGrants { for_user } => Ok(Box::new(ShowGrants::new(
                for_user,
                self.mvcc.inner().clone(),
            ))),

            PhysicalPlan::AlterUser {
                username,
                host,
                password,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(AlterUser::new(
                    username,
                    host,
                    password,
                    raft_node,
                    self.mvcc.inner().clone(),
                )))
            }

            PhysicalPlan::SetPassword {
                username,
                host,
                password,
            } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("Auth operations require Raft".to_string())
                })?;
                Ok(Box::new(SetPassword::new(
                    username,
                    host,
                    password,
                    raft_node,
                    self.mvcc.inner().clone(),
                )))
            }

            PhysicalPlan::AnalyzeTable { table } => {
                let raft_node = self.raft_node.clone().ok_or_else(|| {
                    ExecutorError::Internal("ANALYZE TABLE requires Raft".to_string())
                })?;
                Ok(Box::new(AnalyzeTable::new(
                    table,
                    self.mvcc.clone(),
                    self.catalog.clone(),
                    raft_node,
                )))
            }

            PhysicalPlan::Explain { inner } => Ok(Box::new(ExplainExecutor::new(*inner))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, DataType, TableDef};
    use crate::executor::datum::Datum;
    use crate::executor::encoding::{encode_pk_key, encode_row};
    use crate::executor::row::Row;
    use crate::planner::logical::expr::OutputColumn;
    use crate::planner::logical::{BinaryOp, Literal, ResolvedColumn, ResolvedExpr};
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
                    let after_start = start.is_none_or(|s| k.as_slice() >= s);
                    let before_end = end.is_none_or(|e| k.as_slice() < e);
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
            (encode_pk_key("users", &[Datum::Int(1)]), encode_row(&row1)),
            (encode_pk_key("users", &[Datum::Int(2)]), encode_row(&row2)),
            (encode_pk_key("users", &[Datum::Int(3)]), encode_row(&row3)),
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
        let user_variables = UserVariables::default();
        let engine = ExecutorEngine::new(mvcc.clone(), catalog, None, user_variables);
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
                op: BinaryOp::Gt,
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
