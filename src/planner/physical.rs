//! Physical plan representation and planner
//!
//! Physical plans represent how a query will actually be executed,
//! including specific algorithm choices (e.g., nested loop vs hash join).

use crate::catalog::{Catalog, ColumnDef, Constraint};
use crate::executor::datum::Datum;
use crate::planner::error::PlannerResult;
use crate::planner::logical::expr::{AggregateFunc, OutputColumn};
use crate::planner::logical::{Literal, LogicalPlan};
use crate::planner::logical::{BinaryOp, JoinType, ResolvedColumn, ResolvedExpr};
use crate::sql::privileges::{HostPattern, Privilege, PrivilegeObject};

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

    /// Point lookup by primary key (O(log n) instead of O(n) scan)
    PointGet {
        table: String,
        columns: Vec<OutputColumn>,
        /// Primary key value to look up
        key_value: ResolvedExpr,
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

    /// Single empty row (TABLE_DEE)
    SingleRow,

    // ============ DML Operations ============
    /// INSERT rows into a table
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
        /// Column indices that are auto_increment (value generated if NULL)
        auto_increment_indices: Vec<usize>,
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

    // ============ Auth Operations ============
    /// CREATE USER
    CreateUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
        if_not_exists: bool,
    },

    /// DROP USER
    DropUser {
        username: String,
        host: HostPattern,
        if_exists: bool,
    },

    /// ALTER USER
    AlterUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
    },

    /// SET PASSWORD
    SetPassword {
        username: String,
        host: HostPattern,
        password: String,
    },

    /// GRANT privileges
    Grant {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
        with_grant_option: bool,
    },

    /// REVOKE privileges
    Revoke {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
    },

    /// SHOW GRANTS
    ShowGrants {
        for_user: Option<(String, HostPattern)>,
    },
}

impl PhysicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            PhysicalPlan::TableScan { columns, .. } => columns.clone(),

            PhysicalPlan::PointGet { columns, .. } => columns.clone(),

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
            PhysicalPlan::SingleRow => vec![],

            // DML/DDL operations don't produce query output
            PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. }
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. } => vec![],

            // Auth operations don't produce query output columns
            PhysicalPlan::CreateUser { .. }
            | PhysicalPlan::DropUser { .. }
            | PhysicalPlan::AlterUser { .. }
            | PhysicalPlan::SetPassword { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::ShowGrants { .. } => vec![],
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

    /// Substitute `Literal::Placeholder(n)` with concrete values from `params`.
    /// Used for prepared statement plan caching — clone the cached plan, then substitute.
    pub fn substitute_params(&mut self, params: &[Datum]) {
        match self {
            PhysicalPlan::TableScan { filter, .. } => {
                if let Some(f) = filter {
                    substitute_expr(f, params);
                }
            }
            PhysicalPlan::PointGet { key_value, .. } => {
                substitute_expr(key_value, params);
            }
            PhysicalPlan::Filter { input, predicate } => {
                input.substitute_params(params);
                substitute_expr(predicate, params);
            }
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                input.substitute_params(params);
                for (expr, _) in expressions {
                    substitute_expr(expr, params);
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            } => {
                left.substitute_params(params);
                right.substitute_params(params);
                if let Some(c) = condition {
                    substitute_expr(c, params);
                }
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                input.substitute_params(params);
                for expr in group_by {
                    substitute_expr(expr, params);
                }
            }
            PhysicalPlan::Sort {
                input, order_by, ..
            } => {
                input.substitute_params(params);
                for (expr, _) in order_by {
                    substitute_expr(expr, params);
                }
            }
            PhysicalPlan::Limit { input, .. } => {
                input.substitute_params(params);
            }
            PhysicalPlan::HashDistinct { input } => {
                input.substitute_params(params);
            }
            PhysicalPlan::Insert { values, .. } => {
                for row in values {
                    for expr in row {
                        substitute_expr(expr, params);
                    }
                }
            }
            PhysicalPlan::Update {
                assignments,
                filter,
                ..
            } => {
                for (_, expr) in assignments {
                    substitute_expr(expr, params);
                }
                if let Some(f) = filter {
                    substitute_expr(f, params);
                }
            }
            PhysicalPlan::Delete { filter, .. } => {
                if let Some(f) = filter {
                    substitute_expr(f, params);
                }
            }
            // DDL/Auth operations have no expression parameters
            PhysicalPlan::SingleRow
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateUser { .. }
            | PhysicalPlan::DropUser { .. }
            | PhysicalPlan::AlterUser { .. }
            | PhysicalPlan::SetPassword { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::ShowGrants { .. } => {}
        }
    }
}

/// Replace `Literal::Placeholder(n)` in a `ResolvedExpr` with concrete values.
fn substitute_expr(expr: &mut ResolvedExpr, params: &[Datum]) {
    match expr {
        ResolvedExpr::Literal(lit) => {
            if let Literal::Placeholder(idx) = lit {
                let datum = if *idx < params.len() {
                    &params[*idx]
                } else {
                    &Datum::Null
                };
                *lit = datum_to_literal(datum);
            }
        }
        ResolvedExpr::BinaryOp { left, right, .. } => {
            substitute_expr(left, params);
            substitute_expr(right, params);
        }
        ResolvedExpr::UnaryOp { expr: inner, .. } => {
            substitute_expr(inner, params);
        }
        ResolvedExpr::Function { args, .. } => {
            for arg in args {
                substitute_expr(arg, params);
            }
        }
        ResolvedExpr::IsNull { expr: inner, .. } => {
            substitute_expr(inner, params);
        }
        ResolvedExpr::InList { expr, list, .. } => {
            substitute_expr(expr, params);
            for item in list {
                substitute_expr(item, params);
            }
        }
        ResolvedExpr::Between {
            expr, low, high, ..
        } => {
            substitute_expr(expr, params);
            substitute_expr(low, params);
            substitute_expr(high, params);
        }
        ResolvedExpr::Column(_) => {}
    }
}

/// Convert a Datum to a Literal for plan substitution.
fn datum_to_literal(datum: &Datum) -> Literal {
    match datum {
        Datum::Null => Literal::Null,
        Datum::Bool(b) => Literal::Boolean(*b),
        Datum::Int(i) => Literal::Integer(*i),
        Datum::Float(f) => Literal::Float(*f),
        Datum::String(s) => Literal::String(s.clone()),
        Datum::Bytes(b) => Literal::Blob(b.clone()),
        Datum::Timestamp(t) => Literal::Integer(*t),
    }
}

/// Physical planner - converts logical plans to physical plans
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    /// Convert a logical plan to a physical plan
    pub fn plan(logical: LogicalPlan, catalog: &Catalog) -> PlannerResult<PhysicalPlan> {
        Self::plan_node(logical, catalog)
    }

    /// Check if a filter expression is a point lookup on the primary key.
    /// Returns the key value expression if it is.
    pub fn extract_point_get(
        table_name: &str,
        filter: &ResolvedExpr,
        catalog: &Catalog,
    ) -> Option<ResolvedExpr> {
        // Get PK columns from catalog
        let table_def = catalog.get_table(table_name)?;
        let pk_cols = table_def.primary_key()?;
        if pk_cols.len() != 1 {
            return None; // Only single-column PK supported
        }
        let pk_col_name = &pk_cols[0];
        let pk_col_index = table_def.get_column_index(pk_col_name)?;

        // Match filter: Column(pk) = Literal or Literal = Column(pk)
        if let ResolvedExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } = filter
        {
            // Check Column = Literal
            if let ResolvedExpr::Column(col) = left.as_ref() {
                if col.index == pk_col_index
                    && matches!(right.as_ref(), ResolvedExpr::Literal(_))
                {
                    return Some(right.as_ref().clone());
                }
            }
            // Check Literal = Column
            if let ResolvedExpr::Column(col) = right.as_ref() {
                if col.index == pk_col_index
                    && matches!(left.as_ref(), ResolvedExpr::Literal(_))
                {
                    return Some(left.as_ref().clone());
                }
            }
        }

        None
    }

    /// Plan a single logical node
    fn plan_node(logical: LogicalPlan, catalog: &Catalog) -> PlannerResult<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table,
                columns,
                filter,
            } => {
                // Note: PointGet optimization is not currently used because
                // storage keys are auto-generated row IDs, not PK values.
                // PointGet would require a PK→row_key index to work.
                Ok(PhysicalPlan::TableScan {
                    table,
                    columns,
                    filter,
                })
            }

            LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter {
                input: Box::new(Self::plan_node(*input, catalog)?),
                predicate,
            }),

            LogicalPlan::Project { input, expressions } => Ok(PhysicalPlan::Project {
                input: Box::new(Self::plan_node(*input, catalog)?),
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
                    left: Box::new(Self::plan_node(*left, catalog)?),
                    right: Box::new(Self::plan_node(*right, catalog)?),
                    join_type,
                    condition,
                })
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => Ok(PhysicalPlan::HashAggregate {
                input: Box::new(Self::plan_node(*input, catalog)?),
                group_by,
                aggregates,
            }),

            LogicalPlan::Sort { input, order_by } => Ok(PhysicalPlan::Sort {
                input: Box::new(Self::plan_node(*input, catalog)?),
                order_by,
            }),

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(PhysicalPlan::Limit {
                input: Box::new(Self::plan_node(*input, catalog)?),
                limit,
                offset,
            }),

            LogicalPlan::Distinct { input } => Ok(PhysicalPlan::HashDistinct {
                input: Box::new(Self::plan_node(*input, catalog)?),
            }),

            LogicalPlan::SingleRow => Ok(PhysicalPlan::SingleRow),

            // DML passthrough
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                // Look up auto_increment columns from catalog
                let auto_increment_indices = if let Some(table_def) = catalog.get_table(&table) {
                    columns
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| {
                            table_def
                                .get_column(&col.name)
                                .is_some_and(|cd| cd.auto_increment)
                        })
                        .map(|(i, _)| i)
                        .collect()
                } else {
                    vec![]
                };
                Ok(PhysicalPlan::Insert {
                    table,
                    columns,
                    values,
                    auto_increment_indices,
                })
            }

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

            // Auth operations - passthrough
            LogicalPlan::CreateUser {
                username,
                host,
                password,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateUser {
                username,
                host,
                password,
                if_not_exists,
            }),
            LogicalPlan::DropUser {
                username,
                host,
                if_exists,
            } => Ok(PhysicalPlan::DropUser {
                username,
                host,
                if_exists,
            }),
            LogicalPlan::AlterUser {
                username,
                host,
                password,
            } => Ok(PhysicalPlan::AlterUser {
                username,
                host,
                password,
            }),
            LogicalPlan::SetPassword {
                username,
                host,
                password,
            } => Ok(PhysicalPlan::SetPassword {
                username,
                host,
                password,
            }),
            LogicalPlan::Grant {
                privileges,
                object,
                grantee,
                grantee_host,
                with_grant_option,
            } => Ok(PhysicalPlan::Grant {
                privileges,
                object,
                grantee,
                grantee_host,
                with_grant_option,
            }),
            LogicalPlan::Revoke {
                privileges,
                object,
                grantee,
                grantee_host,
            } => Ok(PhysicalPlan::Revoke {
                privileges,
                object,
                grantee,
                grantee_host,
            }),
            LogicalPlan::ShowGrants { for_user } => Ok(PhysicalPlan::ShowGrants { for_user }),
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
