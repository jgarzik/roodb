//! Physical plan representation and planner
//!
//! Physical plans represent how a query will actually be executed,
//! including specific algorithm choices (e.g., nested loop vs hash join).

use crate::catalog::{Catalog, ColumnDef, Constraint, DataType};
use crate::executor::datum::Datum;
use crate::planner::error::{PlannerError, PlannerResult};
use crate::planner::logical::expr::{AggregateFunc, OutputColumn};
use crate::planner::logical::{BinaryOp, JoinType, ResolvedColumn, ResolvedExpr};
use crate::planner::logical::{Literal, LogicalPlan};
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

    /// Range scan by primary key bounds (scans only matching PK range instead of full table)
    RangeScan {
        table: String,
        columns: Vec<OutputColumn>,
        /// Start PK bound (None = unbounded start)
        start_key: Option<ResolvedExpr>,
        /// End PK bound (None = unbounded end)
        end_key: Option<ResolvedExpr>,
        /// Whether start bound is inclusive
        inclusive_start: bool,
        /// Whether end bound is inclusive
        inclusive_end: bool,
        /// Any remaining filter predicates that couldn't be pushed to storage bounds
        remaining_filter: Option<ResolvedExpr>,
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

    /// Nested loop join (fallback for non-equi joins and cross joins)
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<ResolvedExpr>,
    },

    /// Hash join for equi-joins (O(n+m) vs O(n*m) nested loop)
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        /// Equi-join key column indices in the left input
        left_keys: Vec<usize>,
        /// Equi-join key column indices in the right input
        right_keys: Vec<usize>,
        /// Remaining non-equi condition (evaluated on combined row)
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
        /// Primary key column indices for PK-based storage keys
        pk_column_indices: Vec<usize>,
    },

    /// UPDATE rows in a table
    Update {
        table: String,
        /// (column, new value)
        assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
        filter: Option<ResolvedExpr>,
        /// PK value for PointGet fast path (O(1) instead of full scan)
        key_value: Option<ResolvedExpr>,
    },

    /// DELETE rows from a table
    Delete {
        table: String,
        filter: Option<ResolvedExpr>,
        /// PK value for PointGet fast path (O(1) instead of full scan)
        key_value: Option<ResolvedExpr>,
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

    /// CREATE DATABASE
    CreateDatabase { name: String, if_not_exists: bool },

    /// DROP DATABASE
    DropDatabase { name: String, if_exists: bool },

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

    /// ANALYZE TABLE
    AnalyzeTable { table: String },
}

impl PhysicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            PhysicalPlan::TableScan { columns, .. } => columns.clone(),

            PhysicalPlan::PointGet { columns, .. } => columns.clone(),

            PhysicalPlan::RangeScan { columns, .. } => columns.clone(),

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

            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. } => {
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
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateDatabase { .. }
            | PhysicalPlan::DropDatabase { .. } => vec![],

            // Auth operations don't produce query output columns
            PhysicalPlan::CreateUser { .. }
            | PhysicalPlan::DropUser { .. }
            | PhysicalPlan::AlterUser { .. }
            | PhysicalPlan::SetPassword { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::ShowGrants { .. } => vec![],

            // ANALYZE TABLE returns a result set (columns handled by executor)
            PhysicalPlan::AnalyzeTable { .. } => vec![
                OutputColumn {
                    id: 0,
                    name: "Table".to_string(),
                    data_type: DataType::Varchar(255),
                    nullable: false,
                },
                OutputColumn {
                    id: 1,
                    name: "Op".to_string(),
                    data_type: DataType::Varchar(255),
                    nullable: false,
                },
                OutputColumn {
                    id: 2,
                    name: "Msg_type".to_string(),
                    data_type: DataType::Varchar(255),
                    nullable: false,
                },
                OutputColumn {
                    id: 3,
                    name: "Msg_text".to_string(),
                    data_type: DataType::Varchar(255),
                    nullable: false,
                },
            ],
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
                | PhysicalPlan::CreateDatabase { .. }
                | PhysicalPlan::DropDatabase { .. }
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
    pub fn substitute_params(&mut self, params: &[Datum]) -> PlannerResult<()> {
        match self {
            PhysicalPlan::TableScan { filter, .. } => {
                if let Some(f) = filter {
                    substitute_expr(f, params)?;
                }
            }
            PhysicalPlan::PointGet { key_value, .. } => {
                substitute_expr(key_value, params)?;
            }
            PhysicalPlan::RangeScan {
                start_key,
                end_key,
                remaining_filter,
                ..
            } => {
                if let Some(sk) = start_key {
                    substitute_expr(sk, params)?;
                }
                if let Some(ek) = end_key {
                    substitute_expr(ek, params)?;
                }
                if let Some(rf) = remaining_filter {
                    substitute_expr(rf, params)?;
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                input.substitute_params(params)?;
                substitute_expr(predicate, params)?;
            }
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                input.substitute_params(params)?;
                for (expr, _) in expressions {
                    substitute_expr(expr, params)?;
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            }
            | PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                ..
            } => {
                left.substitute_params(params)?;
                right.substitute_params(params)?;
                if let Some(c) = condition {
                    substitute_expr(c, params)?;
                }
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                input.substitute_params(params)?;
                for expr in group_by {
                    substitute_expr(expr, params)?;
                }
            }
            PhysicalPlan::Sort {
                input, order_by, ..
            } => {
                input.substitute_params(params)?;
                for (expr, _) in order_by {
                    substitute_expr(expr, params)?;
                }
            }
            PhysicalPlan::Limit { input, .. } => {
                input.substitute_params(params)?;
            }
            PhysicalPlan::HashDistinct { input } => {
                input.substitute_params(params)?;
            }
            PhysicalPlan::Insert { values, .. } => {
                for row in values {
                    for expr in row {
                        substitute_expr(expr, params)?;
                    }
                }
            }
            PhysicalPlan::Update {
                assignments,
                filter,
                key_value,
                ..
            } => {
                for (_, expr) in assignments {
                    substitute_expr(expr, params)?;
                }
                if let Some(f) = filter {
                    substitute_expr(f, params)?;
                }
                if let Some(kv) = key_value {
                    substitute_expr(kv, params)?;
                }
            }
            PhysicalPlan::Delete {
                filter, key_value, ..
            } => {
                if let Some(f) = filter {
                    substitute_expr(f, params)?;
                }
                if let Some(kv) = key_value {
                    substitute_expr(kv, params)?;
                }
            }
            // DDL/Auth operations have no expression parameters
            PhysicalPlan::SingleRow
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateDatabase { .. }
            | PhysicalPlan::DropDatabase { .. }
            | PhysicalPlan::CreateUser { .. }
            | PhysicalPlan::DropUser { .. }
            | PhysicalPlan::AlterUser { .. }
            | PhysicalPlan::SetPassword { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::ShowGrants { .. }
            | PhysicalPlan::AnalyzeTable { .. } => {}
        }
        Ok(())
    }
}

/// Replace `Literal::Placeholder(n)` in a `ResolvedExpr` with concrete values.
fn substitute_expr(expr: &mut ResolvedExpr, params: &[Datum]) -> PlannerResult<()> {
    match expr {
        ResolvedExpr::Literal(lit) => {
            if let Literal::Placeholder(idx) = lit {
                if *idx >= params.len() {
                    return Err(PlannerError::InvalidPlan(format!(
                        "placeholder index {} out of range for {} parameters",
                        idx,
                        params.len()
                    )));
                }
                *lit = datum_to_literal(&params[*idx]);
            }
        }
        ResolvedExpr::BinaryOp { left, right, .. } => {
            substitute_expr(left, params)?;
            substitute_expr(right, params)?;
        }
        ResolvedExpr::UnaryOp { expr: inner, .. } => {
            substitute_expr(inner, params)?;
        }
        ResolvedExpr::Function { args, .. } => {
            for arg in args {
                substitute_expr(arg, params)?;
            }
        }
        ResolvedExpr::IsNull { expr: inner, .. } => {
            substitute_expr(inner, params)?;
        }
        ResolvedExpr::InList { expr, list, .. } => {
            substitute_expr(expr, params)?;
            for item in list {
                substitute_expr(item, params)?;
            }
        }
        ResolvedExpr::Between {
            expr, low, high, ..
        } => {
            substitute_expr(expr, params)?;
            substitute_expr(low, params)?;
            substitute_expr(high, params)?;
        }
        ResolvedExpr::Column(_) => {}
    }
    Ok(())
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

/// Extract equi-join key pairs from a join condition.
///
/// Returns `(equi_keys, remaining_condition)` where:
/// - `equi_keys` is a vec of `(left_col_index, right_col_index)` pairs
///   (right indices are relative to the right input, i.e., offset by left_width)
/// - `remaining_condition` is the non-equi portion of the condition, if any
fn extract_equi_keys(
    condition: &ResolvedExpr,
    left_width: usize,
) -> (Vec<(usize, usize)>, Option<ResolvedExpr>) {
    let mut equi_keys = Vec::new();
    let mut remaining_parts = Vec::new();
    collect_equi_keys(condition, left_width, &mut equi_keys, &mut remaining_parts);

    let remaining = if remaining_parts.is_empty() {
        None
    } else {
        Some(
            remaining_parts
                .into_iter()
                .reduce(|acc, part| ResolvedExpr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(part),
                    result_type: DataType::Boolean,
                })
                .unwrap(),
        )
    };

    (equi_keys, remaining)
}

/// Recursively collect equi-join key pairs from AND-connected conditions.
fn collect_equi_keys(
    expr: &ResolvedExpr,
    left_width: usize,
    equi_keys: &mut Vec<(usize, usize)>,
    remaining: &mut Vec<ResolvedExpr>,
) {
    match expr {
        ResolvedExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_equi_keys(left, left_width, equi_keys, remaining);
            collect_equi_keys(right, left_width, equi_keys, remaining);
        }
        ResolvedExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            // Check if this is Column(left_side) = Column(right_side)
            if let (ResolvedExpr::Column(lc), ResolvedExpr::Column(rc)) =
                (left.as_ref(), right.as_ref())
            {
                if lc.index < left_width && rc.index >= left_width {
                    equi_keys.push((lc.index, rc.index - left_width));
                    return;
                }
                if rc.index < left_width && lc.index >= left_width {
                    equi_keys.push((rc.index, lc.index - left_width));
                    return;
                }
            }
            // Not an equi-join on left/right columns
            remaining.push(expr.clone());
        }
        _ => {
            remaining.push(expr.clone());
        }
    }
}

/// Result of extracting PK range bounds from a filter expression
pub struct RangeBounds {
    pub start_key: Option<ResolvedExpr>,
    pub end_key: Option<ResolvedExpr>,
    pub inclusive_start: bool,
    pub inclusive_end: bool,
    pub remaining_filter: Option<ResolvedExpr>,
}

/// Helper enum for PK range bound extraction
enum BoundType {
    GtEq(ResolvedExpr),
    Gt(ResolvedExpr),
    LtEq(ResolvedExpr),
    Lt(ResolvedExpr),
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
                if col.index == pk_col_index && matches!(right.as_ref(), ResolvedExpr::Literal(_)) {
                    return Some(right.as_ref().clone());
                }
            }
            // Check Literal = Column
            if let ResolvedExpr::Column(col) = right.as_ref() {
                if col.index == pk_col_index && matches!(left.as_ref(), ResolvedExpr::Literal(_)) {
                    return Some(left.as_ref().clone());
                }
            }
        }

        None
    }

    /// Check if a filter expression is a range scan on the primary key.
    pub fn extract_range_scan(
        table_name: &str,
        filter: &ResolvedExpr,
        catalog: &Catalog,
    ) -> Option<RangeBounds> {
        // Get PK columns from catalog
        let table_def = catalog.get_table(table_name)?;
        let pk_cols = table_def.primary_key()?;
        if pk_cols.len() != 1 {
            return None; // Only single-column PK supported
        }
        let pk_col_name = &pk_cols[0];
        let pk_col_index = table_def.get_column_index(pk_col_name)?;

        // Match BETWEEN: Column(pk) BETWEEN low AND high
        if let ResolvedExpr::Between {
            expr,
            low,
            high,
            negated,
        } = filter
        {
            if *negated {
                return None;
            }
            if let ResolvedExpr::Column(col) = expr.as_ref() {
                if col.index == pk_col_index {
                    return Some(RangeBounds {
                        start_key: Some(low.as_ref().clone()),
                        end_key: Some(high.as_ref().clone()),
                        inclusive_start: true,
                        inclusive_end: true,
                        remaining_filter: None,
                    });
                }
            }
        }

        // Match AND of two comparisons on PK
        if let ResolvedExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } = filter
        {
            let mut start_key = None;
            let mut end_key = None;
            let mut inclusive_start = false;
            let mut inclusive_end = false;
            let mut remaining_parts = Vec::new();

            for part in [left.as_ref(), right.as_ref()] {
                if let Some(bound_type) = Self::extract_pk_bound(part, pk_col_index) {
                    match bound_type {
                        BoundType::GtEq(v) => {
                            start_key = Some(v);
                            inclusive_start = true;
                        }
                        BoundType::Gt(v) => {
                            start_key = Some(v);
                            inclusive_start = false;
                        }
                        BoundType::LtEq(v) => {
                            end_key = Some(v);
                            inclusive_end = true;
                        }
                        BoundType::Lt(v) => {
                            end_key = Some(v);
                            inclusive_end = false;
                        }
                    }
                } else {
                    remaining_parts.push(part.clone());
                }
            }

            if start_key.is_some() || end_key.is_some() {
                let remaining_filter = if remaining_parts.is_empty() {
                    None
                } else if remaining_parts.len() == 1 {
                    Some(remaining_parts.remove(0))
                } else {
                    Some(ResolvedExpr::BinaryOp {
                        left: Box::new(remaining_parts.remove(0)),
                        op: BinaryOp::And,
                        right: Box::new(remaining_parts.remove(0)),
                        result_type: DataType::Boolean,
                    })
                };
                return Some(RangeBounds {
                    start_key,
                    end_key,
                    inclusive_start,
                    inclusive_end,
                    remaining_filter,
                });
            }
        }

        // Match single comparison on PK (half-bounded range)
        if let Some(bound_type) = Self::extract_pk_bound(filter, pk_col_index) {
            let rb = match bound_type {
                BoundType::GtEq(v) => RangeBounds {
                    start_key: Some(v),
                    end_key: None,
                    inclusive_start: true,
                    inclusive_end: false,
                    remaining_filter: None,
                },
                BoundType::Gt(v) => RangeBounds {
                    start_key: Some(v),
                    end_key: None,
                    inclusive_start: false,
                    inclusive_end: false,
                    remaining_filter: None,
                },
                BoundType::LtEq(v) => RangeBounds {
                    start_key: None,
                    end_key: Some(v),
                    inclusive_start: false,
                    inclusive_end: true,
                    remaining_filter: None,
                },
                BoundType::Lt(v) => RangeBounds {
                    start_key: None,
                    end_key: Some(v),
                    inclusive_start: false,
                    inclusive_end: false,
                    remaining_filter: None,
                },
            };
            return Some(rb);
        }

        None
    }

    /// Extract a PK bound from a comparison expression
    fn extract_pk_bound(expr: &ResolvedExpr, pk_col_index: usize) -> Option<BoundType> {
        if let ResolvedExpr::BinaryOp {
            left, op, right, ..
        } = expr
        {
            // Column op Literal
            if let ResolvedExpr::Column(col) = left.as_ref() {
                if col.index == pk_col_index {
                    match op {
                        BinaryOp::GtEq => {
                            return Some(BoundType::GtEq(right.as_ref().clone()));
                        }
                        BinaryOp::Gt => {
                            return Some(BoundType::Gt(right.as_ref().clone()));
                        }
                        BinaryOp::LtEq => {
                            return Some(BoundType::LtEq(right.as_ref().clone()));
                        }
                        BinaryOp::Lt => {
                            return Some(BoundType::Lt(right.as_ref().clone()));
                        }
                        _ => {}
                    }
                }
            }
            // Literal op Column (reversed)
            if let ResolvedExpr::Column(col) = right.as_ref() {
                if col.index == pk_col_index {
                    match op {
                        BinaryOp::GtEq => {
                            return Some(BoundType::LtEq(left.as_ref().clone()));
                        }
                        BinaryOp::Gt => {
                            return Some(BoundType::Lt(left.as_ref().clone()));
                        }
                        BinaryOp::LtEq => {
                            return Some(BoundType::GtEq(left.as_ref().clone()));
                        }
                        BinaryOp::Lt => {
                            return Some(BoundType::Gt(left.as_ref().clone()));
                        }
                        _ => {}
                    }
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
                // Try to convert to PointGet if filter is PK equality lookup
                if let Some(ref f) = filter {
                    if let Some(key_value) = Self::extract_point_get(&table, f, catalog) {
                        return Ok(PhysicalPlan::PointGet {
                            table,
                            columns,
                            key_value,
                        });
                    }
                    // Try to convert to RangeScan if filter is PK range
                    if let Some(rb) = Self::extract_range_scan(&table, f, catalog) {
                        return Ok(PhysicalPlan::RangeScan {
                            table,
                            columns,
                            start_key: rb.start_key,
                            end_key: rb.end_key,
                            inclusive_start: rb.inclusive_start,
                            inclusive_end: rb.inclusive_end,
                            remaining_filter: rb.remaining_filter,
                        });
                    }
                }
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
                let left_plan = Box::new(Self::plan_node(*left, catalog)?);
                let right_plan = Box::new(Self::plan_node(*right, catalog)?);
                let left_width = left_plan.output_columns().len();

                // Try to extract equi-join keys for hash join
                if join_type != JoinType::Cross {
                    if let Some(ref cond) = condition {
                        let (equi_keys, remaining) = extract_equi_keys(cond, left_width);
                        if !equi_keys.is_empty() {
                            let (left_keys, right_keys): (Vec<_>, Vec<_>) =
                                equi_keys.into_iter().unzip();
                            return Ok(PhysicalPlan::HashJoin {
                                left: left_plan,
                                right: right_plan,
                                join_type,
                                left_keys,
                                right_keys,
                                condition: remaining,
                            });
                        }
                    }
                }

                // Fallback to nested loop join
                Ok(PhysicalPlan::NestedLoopJoin {
                    left: left_plan,
                    right: right_plan,
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
                // Look up auto_increment and PK columns from catalog
                let (auto_increment_indices, pk_column_indices) = if let Some(table_def) =
                    catalog.get_table(&table)
                {
                    let auto_inc: Vec<usize> = columns
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| {
                            table_def
                                .get_column(&col.name)
                                .is_some_and(|cd| cd.auto_increment)
                        })
                        .map(|(i, _)| i)
                        .collect();
                    let pk_indices: Vec<usize> = if let Some(pk_cols) = table_def.primary_key() {
                        pk_cols
                            .iter()
                            .filter_map(|pk_name| columns.iter().position(|c| c.name == *pk_name))
                            .collect()
                    } else {
                        vec![]
                    };
                    (auto_inc, pk_indices)
                } else {
                    (vec![], vec![])
                };
                Ok(PhysicalPlan::Insert {
                    table,
                    columns,
                    values,
                    auto_increment_indices,
                    pk_column_indices,
                })
            }

            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                let key_value = filter
                    .as_ref()
                    .and_then(|f| Self::extract_point_get(&table, f, catalog));
                Ok(PhysicalPlan::Update {
                    table,
                    assignments,
                    filter,
                    key_value,
                })
            }

            LogicalPlan::Delete { table, filter } => {
                let key_value = filter
                    .as_ref()
                    .and_then(|f| Self::extract_point_get(&table, f, catalog));
                Ok(PhysicalPlan::Delete {
                    table,
                    filter,
                    key_value,
                })
            }

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

            // Database DDL passthrough
            LogicalPlan::CreateDatabase {
                name,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateDatabase {
                name,
                if_not_exists,
            }),

            LogicalPlan::DropDatabase { name, if_exists } => {
                Ok(PhysicalPlan::DropDatabase { name, if_exists })
            }

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

            LogicalPlan::AnalyzeTable { table } => Ok(PhysicalPlan::AnalyzeTable { table }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::LogicalPlanBuilder;
    use crate::planner::optimizer::Optimizer;
    use crate::planner::test_utils::test_catalog;
    use crate::sql::{Parser, Resolver, TypeChecker};

    /// Helper: parse SQL through the full pipeline (optimizer included)
    fn plan_sql(catalog: &Catalog, sql: &str) -> PhysicalPlan {
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();
        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimized = Optimizer::new().optimize(logical);
        PhysicalPlanner::plan(optimized, catalog).unwrap()
    }

    #[test]
    fn test_physical_plan_select() {
        let catalog = test_catalog();
        let physical = plan_sql(&catalog, "SELECT id, name FROM users WHERE age > 18");

        // Should produce: Project -> TableScan(filter: ...)
        match physical {
            PhysicalPlan::Project { input, .. } => {
                assert!(
                    matches!(
                        *input,
                        PhysicalPlan::TableScan {
                            filter: Some(_),
                            ..
                        }
                    ),
                    "Expected TableScan with pushed-down filter"
                );
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_physical_plan_range_scan_between() {
        let catalog = test_catalog();
        let physical = plan_sql(
            &catalog,
            "SELECT id, name FROM users WHERE id BETWEEN 10 AND 20",
        );

        // Should produce: Project -> RangeScan (BETWEEN on PK)
        match physical {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::RangeScan {
                    ref table,
                    inclusive_start,
                    inclusive_end,
                    ref remaining_filter,
                    ..
                } => {
                    assert_eq!(table, "users");
                    assert!(inclusive_start);
                    assert!(inclusive_end);
                    assert!(remaining_filter.is_none());
                }
                _ => panic!("Expected RangeScan, got {:?}", *input),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_physical_plan_range_scan_comparison() {
        let catalog = test_catalog();
        let physical = plan_sql(&catalog, "SELECT id FROM users WHERE id >= 5 AND id < 15");

        // Should produce: Project -> RangeScan (comparisons on PK)
        match physical {
            PhysicalPlan::Project { input, .. } => {
                match *input {
                    PhysicalPlan::RangeScan {
                        inclusive_start,
                        inclusive_end,
                        ..
                    } => {
                        assert!(inclusive_start); // >= is inclusive
                        assert!(!inclusive_end); // < is exclusive
                    }
                    _ => panic!("Expected RangeScan, got {:?}", *input),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_physical_plan_no_range_scan_non_pk() {
        let catalog = test_catalog();
        let physical = plan_sql(&catalog, "SELECT id FROM users WHERE age BETWEEN 18 AND 65");

        // age is NOT the PK, so should NOT use RangeScan
        match physical {
            PhysicalPlan::Project { input, .. } => {
                assert!(
                    !matches!(*input, PhysicalPlan::RangeScan { .. }),
                    "Should NOT use RangeScan for non-PK column"
                );
            }
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
