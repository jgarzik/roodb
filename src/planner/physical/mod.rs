//! Physical plan representation
//!
//! Physical plans represent how a query will actually be executed,
//! including specific algorithm choices (e.g., nested loop vs hash join).

pub mod planner;

pub use planner::PhysicalPlanner;

use crate::catalog::{ColumnDef, Constraint};
use crate::sql::{JoinType, ResolvedColumn, ResolvedExpr};

use crate::planner::logical::expr::{AggregateFunc, OutputColumn};

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
    HashDistinct {
        input: Box<PhysicalPlan>,
    },

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
    DropTable {
        name: String,
        if_exists: bool,
    },

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
    },

    /// DROP INDEX
    DropIndex {
        name: String,
    },
}

impl PhysicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            PhysicalPlan::TableScan { columns, .. } => columns.clone(),

            PhysicalPlan::Filter { input, .. } => input.output_columns(),

            PhysicalPlan::Project { expressions, .. } => {
                expressions
                    .iter()
                    .enumerate()
                    .map(|(i, (expr, alias))| OutputColumn {
                        id: i,
                        name: alias.clone(),
                        data_type: expr.data_type(),
                        nullable: expr.is_nullable(),
                    })
                    .collect()
            }

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
