//! Logical plan representation
//!
//! Logical plans represent the structure of a query before physical
//! implementation details are decided.

pub mod builder;
pub mod expr;

pub use builder::LogicalPlanBuilder;
pub use expr::{AggregateFunc, ColumnId, OutputColumn};

use crate::catalog::{ColumnDef, Constraint};
use crate::sql::{JoinType, ResolvedColumn, ResolvedExpr};

/// Logical plan node
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan - read all rows from a table
    Scan {
        table: String,
        columns: Vec<OutputColumn>,
        /// Optional pushed-down filter predicate
        filter: Option<ResolvedExpr>,
    },

    /// Filter rows based on a predicate
    Filter {
        input: Box<LogicalPlan>,
        predicate: ResolvedExpr,
    },

    /// Project columns (SELECT list)
    Project {
        input: Box<LogicalPlan>,
        /// (expression, alias)
        expressions: Vec<(ResolvedExpr, String)>,
    },

    /// Join two inputs
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<ResolvedExpr>,
    },

    /// Aggregate with optional grouping
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<ResolvedExpr>,
        /// (aggregate function, output alias)
        aggregates: Vec<(AggregateFunc, String)>,
    },

    /// Sort rows
    Sort {
        input: Box<LogicalPlan>,
        /// (expression, ascending)
        order_by: Vec<(ResolvedExpr, bool)>,
    },

    /// Limit rows returned
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<u64>,
        offset: Option<u64>,
    },

    /// Remove duplicate rows
    Distinct {
        input: Box<LogicalPlan>,
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

    // ============ DDL Operations (passthrough) ============

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

impl LogicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            LogicalPlan::Scan { columns, .. } => columns.clone(),

            LogicalPlan::Filter { input, .. } => input.output_columns(),

            LogicalPlan::Project { expressions, .. } => {
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

            LogicalPlan::Join { left, right, .. } => {
                let mut cols = left.output_columns();
                let offset = cols.len();
                for mut col in right.output_columns() {
                    col.id += offset;
                    cols.push(col);
                }
                cols
            }

            LogicalPlan::Aggregate {
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
                        nullable: true, // Aggregates can be NULL for empty groups
                    });
                }
                cols
            }

            LogicalPlan::Sort { input, .. } => input.output_columns(),
            LogicalPlan::Limit { input, .. } => input.output_columns(),
            LogicalPlan::Distinct { input } => input.output_columns(),

            // DML operations don't produce output columns for query purposes
            LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. } => vec![],

            // DDL operations don't produce output columns
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. } => vec![],
        }
    }

    /// Check if this is a DDL statement
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
        )
    }

    /// Check if this is a DML statement
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        )
    }
}
