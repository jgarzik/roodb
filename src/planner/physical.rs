//! Physical plan representation and planner
//!
//! Physical plans represent how a query will actually be executed,
//! including specific algorithm choices (e.g., nested loop vs hash join).

use crate::catalog::{Catalog, ColumnDef, Constraint};
use crate::planner::error::PlannerResult;
use crate::planner::logical::expr::{AggregateFunc, OutputColumn};
use crate::planner::logical::LogicalPlan;
use crate::planner::logical::{JoinType, ResolvedColumn, ResolvedExpr};

/// Physical plan node
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Full table scan
    TableScan {
        table: String,
        columns: Vec<OutputColumn>,
        /// Optional filter to apply during scan
        filter: Option<ResolvedExpr>,
    },

    /// Filter rows based on a predicate
    Filter {
        input: Box<PhysicalPlan>,
        predicate: ResolvedExpr,
    },

    /// Project columns
    Project {
        input: Box<PhysicalPlan>,
        /// (expression, alias)
        expressions: Vec<(ResolvedExpr, String)>,
    },

    /// Nested loop join
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<ResolvedExpr>,
    },

    /// Hash-based aggregation
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<ResolvedExpr>,
        /// (aggregate function, output alias)
        aggregates: Vec<(AggregateFunc, String)>,
    },

    /// In-memory sort
    Sort {
        input: Box<PhysicalPlan>,
        /// (expression, ascending)
        order_by: Vec<(ResolvedExpr, bool)>,
    },

    /// Limit rows returned
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<u64>,
        offset: Option<u64>,
    },

    /// Hash-based distinct
    HashDistinct { input: Box<PhysicalPlan> },

    // ============ DML Operations ============
    /// INSERT rows into a table
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
    },

    /// UPDATE rows in a table
    Update {
        table: String,
        /// (column, new value)
        assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
        filter: Option<ResolvedExpr>,
    },

    /// DELETE rows from a table
    Delete {
        table: String,
        filter: Option<ResolvedExpr>,
    },

    // ============ DDL Operations ============
    /// CREATE TABLE
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
    },

    /// DROP TABLE
    DropTable { name: String, if_exists: bool },

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
    },

    /// DROP INDEX
    DropIndex { name: String },
}

impl PhysicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            PhysicalPlan::TableScan { columns, .. } => columns.clone(),

            PhysicalPlan::Filter { input, .. } => input.output_columns(),

            PhysicalPlan::Project { expressions, .. } => expressions
                .iter()
                .enumerate()
                .map(|(i, (expr, alias))| OutputColumn {
                    id: i,
                    name: alias.clone(),
                    data_type: expr.data_type(),
                    nullable: expr.is_nullable(),
                })
                .collect(),

            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let mut cols = left.output_columns();
                let offset = cols.len();
                for mut col in right.output_columns() {
                    col.id += offset;
                    cols.push(col);
                }
                cols
            }

            PhysicalPlan::HashAggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut cols: Vec<OutputColumn> = group_by
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| OutputColumn {
                        id: i,
                        name: format!("group_{}", i),
                        data_type: expr.data_type(),
                        nullable: expr.is_nullable(),
                    })
                    .collect();
                let offset = cols.len();
                for (i, (agg, alias)) in aggregates.iter().enumerate() {
                    cols.push(OutputColumn {
                        id: offset + i,
                        name: alias.clone(),
                        data_type: agg.result_type.clone(),
                        nullable: true,
                    });
                }
                cols
            }

            PhysicalPlan::Sort { input, .. } => input.output_columns(),
            PhysicalPlan::Limit { input, .. } => input.output_columns(),
            PhysicalPlan::HashDistinct { input } => input.output_columns(),

            // DML/DDL operations don't produce query output
            PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. }
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. } => vec![],
        }
    }

    /// Check if this is a DDL statement
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            PhysicalPlan::CreateTable { .. }
                | PhysicalPlan::DropTable { .. }
                | PhysicalPlan::CreateIndex { .. }
                | PhysicalPlan::DropIndex { .. }
        )
    }

    /// Check if this is a DML statement
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. }
        )
    }
}

/// Physical planner - converts logical plans to physical plans
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    /// Convert a logical plan to a physical plan
    pub fn plan(logical: LogicalPlan, _catalog: &Catalog) -> PlannerResult<PhysicalPlan> {
        Self::plan_node(logical)
    }

    /// Plan a single logical node
    fn plan_node(logical: LogicalPlan) -> PlannerResult<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table,
                columns,
                filter,
            } => Ok(PhysicalPlan::TableScan {
                table,
                columns,
                filter,
            }),

            LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter {
                input: Box::new(Self::plan_node(*input)?),
                predicate,
            }),

            LogicalPlan::Project { input, expressions } => Ok(PhysicalPlan::Project {
                input: Box::new(Self::plan_node(*input)?),
                expressions,
            }),

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                // For now, always use nested loop join
                // Future: could choose hash join based on cost/statistics
                Ok(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(Self::plan_node(*left)?),
                    right: Box::new(Self::plan_node(*right)?),
                    join_type,
                    condition,
                })
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => Ok(PhysicalPlan::HashAggregate {
                input: Box::new(Self::plan_node(*input)?),
                group_by,
                aggregates,
            }),

            LogicalPlan::Sort { input, order_by } => Ok(PhysicalPlan::Sort {
                input: Box::new(Self::plan_node(*input)?),
                order_by,
            }),

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(PhysicalPlan::Limit {
                input: Box::new(Self::plan_node(*input)?),
                limit,
                offset,
            }),

            LogicalPlan::Distinct { input } => Ok(PhysicalPlan::HashDistinct {
                input: Box::new(Self::plan_node(*input)?),
            }),

            // DML passthrough
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => Ok(PhysicalPlan::Insert {
                table,
                columns,
                values,
            }),

            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => Ok(PhysicalPlan::Update {
                table,
                assignments,
                filter,
            }),

            LogicalPlan::Delete { table, filter } => Ok(PhysicalPlan::Delete { table, filter }),

            // DDL passthrough
            LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            }),

            LogicalPlan::DropTable { name, if_exists } => {
                Ok(PhysicalPlan::DropTable { name, if_exists })
            }

            LogicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => Ok(PhysicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            }),

            LogicalPlan::DropIndex { name } => Ok(PhysicalPlan::DropIndex { name }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::LogicalPlanBuilder;
    use crate::planner::test_utils::test_catalog;
    use crate::sql::{Parser, Resolver, TypeChecker};

    #[test]
    fn test_physical_plan_select() {
        let catalog = test_catalog();
        let sql = "SELECT id, name FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let physical = PhysicalPlanner::plan(logical, &catalog).unwrap();

        // Should produce: Project -> Filter -> TableScan
        match physical {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                }
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_physical_plan_insert() {
        let catalog = test_catalog();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let physical = PhysicalPlanner::plan(logical, &catalog).unwrap();

        assert!(matches!(physical, PhysicalPlan::Insert { .. }));
    }
}
