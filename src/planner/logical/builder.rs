//! Logical plan builder
//!
//! Converts resolved SQL statements into logical query plans.

use super::{
    BinaryOp, JoinType, Literal, ResolvedColumn, ResolvedExpr, ResolvedSelect, ResolvedSelectItem,
    ResolvedStatement, ResolvedTableRef, UnaryOp,
};
use crate::catalog::DataType;

use super::expr::{AggregateFunc, OutputColumn};
use super::LogicalPlan;
use crate::planner::error::PlannerResult;

/// Builder for logical query plans
pub struct LogicalPlanBuilder;

impl LogicalPlanBuilder {
    /// Build a logical plan from a resolved statement
    pub fn build(stmt: ResolvedStatement) -> PlannerResult<LogicalPlan> {
        match stmt {
            ResolvedStatement::Select(select) => Self::build_select(select),
            ResolvedStatement::Union { left, right, all } => {
                let left_plan = Self::build_select(*left)?;
                let right_plan = Self::build_select(*right)?;
                Ok(LogicalPlan::Union {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                })
            }
            ResolvedStatement::Insert {
                table,
                columns,
                values,
                ignore,
            } => Ok(LogicalPlan::Insert {
                table,
                columns,
                values,
                ignore,
            }),
            ResolvedStatement::InsertSelect {
                table,
                columns,
                source,
                column_map,
                ignore,
            } => {
                let source_plan = Self::build(*source)?;
                Ok(LogicalPlan::InsertSelect {
                    table,
                    columns,
                    source: Box::new(source_plan),
                    column_map,
                    ignore,
                })
            }
            ResolvedStatement::Update {
                table,
                assignments,
                filter,
                order_by,
                limit,
                ..
            } => {
                let assigns = assignments
                    .into_iter()
                    .map(|a| (a.column, a.value))
                    .collect();
                Ok(LogicalPlan::Update {
                    table,
                    assignments: assigns,
                    filter,
                    order_by,
                    limit,
                })
            }
            ResolvedStatement::Delete {
                table,
                filter,
                order_by,
                limit,
                ..
            } => Ok(LogicalPlan::Delete {
                table,
                filter,
                order_by,
                limit,
            }),
            ResolvedStatement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => Ok(LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            }),
            ResolvedStatement::CreateTableAs {
                name,
                columns,
                constraints,
                if_not_exists,
                source,
            } => {
                let source_plan = Self::build(*source)?;
                Ok(LogicalPlan::CreateTableAs {
                    name,
                    columns,
                    constraints,
                    if_not_exists,
                    source: Box::new(source_plan),
                })
            }
            ResolvedStatement::DropTable { name, if_exists } => {
                Ok(LogicalPlan::DropTable { name, if_exists })
            }
            ResolvedStatement::CreateView {
                name,
                query_sql,
                or_replace,
            } => Ok(LogicalPlan::CreateView {
                name,
                query_sql,
                or_replace,
            }),
            ResolvedStatement::DropView { name, if_exists } => {
                Ok(LogicalPlan::DropView { name, if_exists })
            }
            // Multi-table DROP: plan as first table, remaining handled by executor
            ResolvedStatement::DropMultipleTables { names, if_exists } => {
                // Build first drop; executor will handle all of them
                Ok(LogicalPlan::DropMultipleTables { names, if_exists })
            }
            ResolvedStatement::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => Ok(LogicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            }),
            ResolvedStatement::DropIndex { name } => Ok(LogicalPlan::DropIndex { name }),

            // Auth statements - passthrough to logical plan
            ResolvedStatement::CreateUser {
                username,
                host,
                password,
                if_not_exists,
            } => Ok(LogicalPlan::CreateUser {
                username,
                host,
                password,
                if_not_exists,
            }),
            ResolvedStatement::DropUser {
                username,
                host,
                if_exists,
            } => Ok(LogicalPlan::DropUser {
                username,
                host,
                if_exists,
            }),
            ResolvedStatement::AlterUser {
                username,
                host,
                password,
            } => Ok(LogicalPlan::AlterUser {
                username,
                host,
                password,
            }),
            ResolvedStatement::SetPassword {
                username,
                host,
                password,
            } => Ok(LogicalPlan::SetPassword {
                username,
                host,
                password,
            }),
            ResolvedStatement::Grant {
                privileges,
                object,
                grantee,
                grantee_host,
                with_grant_option,
            } => Ok(LogicalPlan::Grant {
                privileges,
                object,
                grantee,
                grantee_host,
                with_grant_option,
            }),
            ResolvedStatement::Revoke {
                privileges,
                object,
                grantee,
                grantee_host,
            } => Ok(LogicalPlan::Revoke {
                privileges,
                object,
                grantee,
                grantee_host,
            }),
            ResolvedStatement::ShowGrants { for_user } => Ok(LogicalPlan::ShowGrants { for_user }),

            ResolvedStatement::AnalyzeTable { table } => Ok(LogicalPlan::AnalyzeTable { table }),

            ResolvedStatement::Explain { inner } => {
                let inner_plan = Self::build(*inner)?;
                Ok(LogicalPlan::Explain {
                    inner: Box::new(inner_plan),
                })
            }

            // Database DDL - passthrough
            ResolvedStatement::CreateDatabase {
                name,
                if_not_exists,
            } => Ok(LogicalPlan::CreateDatabase {
                name,
                if_not_exists,
            }),
            ResolvedStatement::DropDatabase { name, if_exists } => {
                Ok(LogicalPlan::DropDatabase { name, if_exists })
            }
        }
    }

    /// Build logical plan for SELECT statement
    fn build_select(select: ResolvedSelect) -> PlannerResult<LogicalPlan> {
        // 1. Build scan for each table in FROM
        let mut plan = Self::build_from(&select.from)?;

        // 1b. Apply JOINs
        for join in &select.joins {
            let right = Self::table_scan(&join.table);
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: join.join_type,
                condition: join.condition.clone(),
            };
        }

        // 2. Apply WHERE filter
        if let Some(filter) = select.filter {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: filter,
            };
        }

        // 3. Apply GROUP BY / aggregates
        let has_aggregates = Self::has_aggregates(&select.columns);
        if !select.group_by.is_empty() || has_aggregates {
            // Collect all aggregates from SELECT columns AND HAVING clause
            let all_aggs = Self::collect_all_aggregates(&select.columns, select.having.as_ref());

            plan = Self::build_aggregate(
                plan,
                &select.group_by,
                &select.columns,
                select.having.as_ref(),
            )?;

            // After aggregation, apply HAVING (with transformed aggregates)
            if let Some(having) = select.having {
                // Transform aggregate expressions in HAVING to column references
                let transformed_having =
                    Self::transform_having_expr(&having, &select.group_by, &all_aggs);
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: transformed_having,
                };
            }

            // After aggregation, build projection that maps to aggregate output columns
            plan = Self::build_post_aggregate_project(plan, &select.columns, &select.group_by)?;
        } else {
            // 4. For non-aggregate queries: ORDER BY before Project
            // so ORDER BY can access columns not in the SELECT list.
            if !select.order_by.is_empty() {
                let order_by: Vec<_> = select
                    .order_by
                    .iter()
                    .map(|item| (item.expr.clone(), item.ascending))
                    .collect();
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by,
                };
            }

            // Apply projection (SELECT list)
            plan = Self::build_project(plan, &select.columns)?;
        }

        // 5. Apply DISTINCT
        if select.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        // 6. Apply ORDER BY for aggregate queries (after projection)
        if !select.order_by.is_empty() && (has_aggregates || !select.group_by.is_empty()) {
            let transformed_order_by: Vec<_> = select
                .order_by
                .iter()
                .map(|item| {
                    let transformed_expr =
                        Self::transform_to_output_columns(&item.expr, &select.columns);
                    (transformed_expr, item.ascending)
                })
                .collect();
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: transformed_order_by,
            };
        }

        // 7. Apply LIMIT/OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: select.limit,
                offset: select.offset,
            };
        }

        Ok(plan)
    }

    /// Build scan nodes for FROM clause
    fn build_from(tables: &[ResolvedTableRef]) -> PlannerResult<LogicalPlan> {
        if tables.is_empty() {
            return Ok(LogicalPlan::SingleRow);
        }

        let mut plan = Self::build_table_source(&tables[0])?;

        for table in &tables[1..] {
            let right = Self::build_table_source(table)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: JoinType::Cross,
                condition: None,
            };
        }

        Ok(plan)
    }

    /// Build a plan node for a single table source (physical table or derived table)
    fn build_table_source(table: &ResolvedTableRef) -> PlannerResult<LogicalPlan> {
        if let Some(inner_query) = &table.inner_query {
            let alias = table.alias.clone().unwrap_or_else(|| table.name.clone());
            let inner_plan = Self::build_select(*inner_query.clone())?;
            Ok(LogicalPlan::DerivedTable {
                input: Box::new(inner_plan),
                alias,
            })
        } else {
            Ok(Self::table_scan(table))
        }
    }

    /// Create a table scan node
    fn table_scan(table: &ResolvedTableRef) -> LogicalPlan {
        let columns = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, data_type, nullable))| OutputColumn {
                id: i,
                name: name.clone(),
                data_type: data_type.clone(),
                nullable: *nullable,
            })
            .collect();

        LogicalPlan::Scan {
            table: table.name.clone(),
            columns,
            filter: None,
        }
    }

    /// Check if select items contain aggregates
    fn has_aggregates(columns: &[ResolvedSelectItem]) -> bool {
        columns.iter().any(|item| match item {
            ResolvedSelectItem::Expr { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedSelectItem::Columns(_) => false,
        })
    }

    /// Check if a function name is a known aggregate function
    fn is_aggregate_name(name: &str) -> bool {
        matches!(
            name,
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
                | "STDDEV"
                | "STD"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "ANY_VALUE"
                | "GROUP_CONCAT"
        )
    }

    /// Check if expression contains aggregate functions
    fn expr_has_aggregate(expr: &ResolvedExpr) -> bool {
        match expr {
            ResolvedExpr::Function { name, args, .. } => {
                let upper = name.to_uppercase();
                if Self::is_aggregate_name(&upper) {
                    true
                } else {
                    args.iter().any(Self::expr_has_aggregate)
                }
            }
            ResolvedExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            ResolvedExpr::UnaryOp { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedExpr::IsNull { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedExpr::Cast { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_has_aggregate(e))
                    || conditions.iter().any(Self::expr_has_aggregate)
                    || results.iter().any(Self::expr_has_aggregate)
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_aggregate(e))
            }
            ResolvedExpr::InList { expr, list, .. } => {
                Self::expr_has_aggregate(expr) || list.iter().any(Self::expr_has_aggregate)
            }
            ResolvedExpr::Between {
                expr, low, high, ..
            } => {
                Self::expr_has_aggregate(expr)
                    || Self::expr_has_aggregate(low)
                    || Self::expr_has_aggregate(high)
            }
            ResolvedExpr::BooleanTest { expr, .. } => Self::expr_has_aggregate(expr),
            // Scalar subqueries are self-contained; aggregates inside don't affect the outer query
            ResolvedExpr::ScalarSubquery { .. } => false,
            ResolvedExpr::InSubquery { expr, .. } => Self::expr_has_aggregate(expr),
            // EXISTS subqueries are self-contained
            ResolvedExpr::ExistsSubquery { .. } => false,
            _ => false,
        }
    }

    /// Build aggregate node
    fn build_aggregate(
        input: LogicalPlan,
        group_by: &[ResolvedExpr],
        columns: &[ResolvedSelectItem],
        having: Option<&ResolvedExpr>,
    ) -> PlannerResult<LogicalPlan> {
        let mut aggregates: Vec<(AggregateFunc, String)> = Vec::new();

        // Recursively extract aggregate functions from all select items,
        // including those nested in arithmetic expressions like max(big)-1.
        for (idx, item) in columns.iter().enumerate() {
            if let ResolvedSelectItem::Expr { expr, alias } = item {
                Self::collect_aggregates(expr, idx, alias.as_deref(), &mut aggregates);
            }
        }

        // Also collect aggregates from HAVING clause
        if let Some(having_expr) = having {
            let next_idx = columns.len();
            Self::collect_aggregates(having_expr, next_idx, None, &mut aggregates);
        }

        Ok(LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: group_by.to_vec(),
            aggregates,
        })
    }

    /// Recursively collect aggregate functions from an expression tree.
    /// Deduplicates so that `max(big)` appearing twice produces one aggregate.
    fn collect_aggregates(
        expr: &ResolvedExpr,
        idx: usize,
        alias: Option<&str>,
        out: &mut Vec<(AggregateFunc, String)>,
    ) {
        if let Some(agg) = Self::extract_aggregate(expr) {
            // Check if this aggregate is already collected (dedup)
            let already = out
                .iter()
                .any(|(a, _)| Self::aggregates_match_func(a, &agg));
            if !already {
                let name = alias
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| format!("agg_{}", idx));
                out.push((agg, name));
            }
            return; // Don't recurse into the aggregate's own args
        }
        // Recurse into sub-expressions to find nested aggregates
        match expr {
            ResolvedExpr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates(left, idx, None, out);
                Self::collect_aggregates(right, idx, None, out);
            }
            ResolvedExpr::UnaryOp { expr: inner, .. } => {
                Self::collect_aggregates(inner, idx, None, out);
            }
            ResolvedExpr::Function { args, .. } => {
                for arg in args {
                    Self::collect_aggregates(arg, idx, None, out);
                }
            }
            ResolvedExpr::Cast { expr: inner, .. } => {
                Self::collect_aggregates(inner, idx, None, out);
            }
            ResolvedExpr::IsNull { expr: inner, .. } => {
                Self::collect_aggregates(inner, idx, None, out);
            }
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    Self::collect_aggregates(op, idx, None, out);
                }
                for cond in conditions {
                    Self::collect_aggregates(cond, idx, None, out);
                }
                for res in results {
                    Self::collect_aggregates(res, idx, None, out);
                }
                if let Some(el) = else_result {
                    Self::collect_aggregates(el, idx, None, out);
                }
            }
            ResolvedExpr::InList { expr, list, .. } => {
                Self::collect_aggregates(expr, idx, None, out);
                for item in list {
                    Self::collect_aggregates(item, idx, None, out);
                }
            }
            ResolvedExpr::Between {
                expr, low, high, ..
            } => {
                Self::collect_aggregates(expr, idx, None, out);
                Self::collect_aggregates(low, idx, None, out);
                Self::collect_aggregates(high, idx, None, out);
            }
            ResolvedExpr::BooleanTest { expr, .. } => {
                Self::collect_aggregates(expr, idx, None, out);
            }
            // InSubquery: collect aggregates from the outer expr only (subquery is self-contained)
            ResolvedExpr::InSubquery { expr, .. } => {
                Self::collect_aggregates(expr, idx, None, out);
            }
            // ScalarSubquery and ExistsSubquery are self-contained, no outer aggregates to collect
            _ => {}
        }
    }

    /// Check if two AggregateFunc instances represent the same aggregate.
    fn aggregates_match_func(a: &AggregateFunc, b: &AggregateFunc) -> bool {
        a.name == b.name
            && a.distinct == b.distinct
            && a.args.len() == b.args.len()
            && a.args
                .iter()
                .zip(b.args.iter())
                .all(|(x, y)| Self::exprs_equal(x, y))
    }

    /// Extract aggregate function from expression
    fn extract_aggregate(expr: &ResolvedExpr) -> Option<AggregateFunc> {
        match expr {
            ResolvedExpr::Function {
                name,
                args,
                distinct,
                result_type,
                separator,
            } => {
                let upper = name.to_uppercase();
                if Self::is_aggregate_name(&upper) {
                    let mut agg =
                        AggregateFunc::new(upper, args.clone(), *distinct, result_type.clone());
                    agg.separator = separator.clone();
                    Some(agg)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Build projection node
    fn build_project(
        input: LogicalPlan,
        columns: &[ResolvedSelectItem],
    ) -> PlannerResult<LogicalPlan> {
        let mut expressions = Vec::new();

        for (idx, item) in columns.iter().enumerate() {
            match item {
                ResolvedSelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| Self::expr_name(expr, idx));
                    expressions.push((expr.clone(), name));
                }
                ResolvedSelectItem::Columns(cols) => {
                    // Expanded wildcard - add each column
                    for col in cols {
                        let expr = ResolvedExpr::Column(col.clone());
                        expressions.push((expr, col.name.clone()));
                    }
                }
            }
        }

        Ok(LogicalPlan::Project {
            input: Box::new(input),
            expressions,
        })
    }

    /// Build projection after aggregate node
    ///
    /// The aggregate output is: [group_by_cols..., aggregate_results...]
    /// We need to transform the original expressions to reference these output columns.
    fn build_post_aggregate_project(
        input: LogicalPlan,
        columns: &[ResolvedSelectItem],
        group_by: &[ResolvedExpr],
    ) -> PlannerResult<LogicalPlan> {
        // First, collect all aggregates in order (same as build_aggregate)
        let mut all_aggs: Vec<(AggregateFunc, String)> = Vec::new();
        for (idx, item) in columns.iter().enumerate() {
            if let ResolvedSelectItem::Expr { expr, alias } = item {
                Self::collect_aggregates(expr, idx, alias.as_deref(), &mut all_aggs);
            }
        }

        let mut expressions = Vec::new();

        for (idx, item) in columns.iter().enumerate() {
            match item {
                ResolvedSelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| Self::expr_name(expr, idx));

                    if Self::expr_has_aggregate(expr) {
                        // Rewrite the expression: replace aggregate sub-expressions
                        // with column references to the aggregate output
                        let rewritten = Self::rewrite_agg_refs(expr, group_by, &all_aggs);
                        expressions.push((rewritten, name));
                    } else {
                        // Non-aggregate expression - check if it's a group-by column
                        if let Some(gb_idx) = Self::find_in_group_by(expr, group_by) {
                            let result_type = Self::get_expr_type(expr);
                            let col_ref = ResolvedExpr::Column(ResolvedColumn {
                                table: String::new(),
                                name: name.clone(),
                                index: gb_idx,
                                data_type: result_type,
                                nullable: true,
                                default_value: None,
                            });
                            expressions.push((col_ref, name));
                        } else {
                            expressions.push((expr.clone(), name));
                        }
                    }
                }
                ResolvedSelectItem::Columns(cols) => {
                    for col in cols {
                        let expr = ResolvedExpr::Column(col.clone());
                        expressions.push((expr, col.name.clone()));
                    }
                }
            }
        }

        Ok(LogicalPlan::Project {
            input: Box::new(input),
            expressions,
        })
    }

    /// Rewrite an expression by replacing aggregate function calls with
    /// column references to the aggregate output, and group-by column
    /// references with their output indices. For example,
    /// `max(big) - 1` becomes `Column(agg_idx) - 1`.
    fn rewrite_agg_refs(
        expr: &ResolvedExpr,
        group_by: &[ResolvedExpr],
        all_aggs: &[(AggregateFunc, String)],
    ) -> ResolvedExpr {
        let group_by_len = group_by.len();

        // If this expression itself is a direct aggregate, replace it
        if let Some(agg) = Self::extract_aggregate(expr) {
            if let Some(pos) = all_aggs
                .iter()
                .position(|(a, _)| Self::aggregates_match_func(a, &agg))
            {
                let col_idx = group_by_len + pos;
                let result_type = Self::get_aggregate_result_type(expr);
                return ResolvedExpr::Column(ResolvedColumn {
                    table: String::new(),
                    name: all_aggs[pos].1.clone(),
                    index: col_idx,
                    data_type: result_type,
                    nullable: true,
                    default_value: None,
                });
            }
        }

        // If this is a column reference that matches a group-by expression,
        // rewrite it to point to the group-by output index.
        if let Some(gb_idx) = Self::find_in_group_by(expr, group_by) {
            let result_type = Self::get_expr_type(expr);
            return ResolvedExpr::Column(ResolvedColumn {
                table: String::new(),
                name: format!("gb_{}", gb_idx),
                index: gb_idx,
                data_type: result_type,
                nullable: true,
                default_value: None,
            });
        }

        // Recurse into sub-expressions
        match expr {
            ResolvedExpr::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => ResolvedExpr::BinaryOp {
                left: Box::new(Self::rewrite_agg_refs(left, group_by, all_aggs)),
                op: *op,
                right: Box::new(Self::rewrite_agg_refs(right, group_by, all_aggs)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::UnaryOp {
                op,
                expr: inner,
                result_type,
            } => ResolvedExpr::UnaryOp {
                op: *op,
                expr: Box::new(Self::rewrite_agg_refs(inner, group_by, all_aggs)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::Cast {
                expr: inner,
                target_type,
            } => ResolvedExpr::Cast {
                expr: Box::new(Self::rewrite_agg_refs(inner, group_by, all_aggs)),
                target_type: target_type.clone(),
            },
            ResolvedExpr::IsNull {
                expr: inner,
                negated,
            } => ResolvedExpr::IsNull {
                expr: Box::new(Self::rewrite_agg_refs(inner, group_by, all_aggs)),
                negated: *negated,
            },
            ResolvedExpr::Function {
                name,
                args,
                distinct,
                result_type,
                separator,
            } => {
                // Guard: do not recurse into aggregate function args.
                // Aggregate args are evaluated pre-aggregation, not post-.
                // If we reach here, the aggregate was not found in all_aggs
                // (should not happen with correct exprs_equal), return as-is.
                if Self::extract_aggregate(expr).is_some() {
                    return expr.clone();
                }
                ResolvedExpr::Function {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|a| Self::rewrite_agg_refs(a, group_by, all_aggs))
                        .collect(),
                    distinct: *distinct,
                    result_type: result_type.clone(),
                    separator: separator.clone(),
                }
            }
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                result_type,
            } => ResolvedExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::rewrite_agg_refs(e, group_by, all_aggs))),
                conditions: conditions
                    .iter()
                    .map(|c| Self::rewrite_agg_refs(c, group_by, all_aggs))
                    .collect(),
                results: results
                    .iter()
                    .map(|r| Self::rewrite_agg_refs(r, group_by, all_aggs))
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(Self::rewrite_agg_refs(e, group_by, all_aggs))),
                result_type: result_type.clone(),
            },
            ResolvedExpr::InList {
                expr,
                list,
                negated,
            } => ResolvedExpr::InList {
                expr: Box::new(Self::rewrite_agg_refs(expr, group_by, all_aggs)),
                list: list
                    .iter()
                    .map(|e| Self::rewrite_agg_refs(e, group_by, all_aggs))
                    .collect(),
                negated: *negated,
            },
            ResolvedExpr::Between {
                expr,
                low,
                high,
                negated,
            } => ResolvedExpr::Between {
                expr: Box::new(Self::rewrite_agg_refs(expr, group_by, all_aggs)),
                low: Box::new(Self::rewrite_agg_refs(low, group_by, all_aggs)),
                high: Box::new(Self::rewrite_agg_refs(high, group_by, all_aggs)),
                negated: *negated,
            },
            ResolvedExpr::BooleanTest { expr, test } => ResolvedExpr::BooleanTest {
                expr: Box::new(Self::rewrite_agg_refs(expr, group_by, all_aggs)),
                test: *test,
            },
            // Subqueries are self-contained, pass through
            ResolvedExpr::ScalarSubquery { .. } => expr.clone(),
            ResolvedExpr::InSubquery {
                expr: inner,
                query,
                negated,
            } => ResolvedExpr::InSubquery {
                expr: Box::new(Self::rewrite_agg_refs(inner, group_by, all_aggs)),
                query: query.clone(),
                negated: *negated,
            },
            ResolvedExpr::ExistsSubquery { .. } => expr.clone(),
            // For other expression types (Column, Literal, etc.), pass through
            other => other.clone(),
        }
    }

    /// Get the result type for an aggregate expression
    fn get_aggregate_result_type(expr: &ResolvedExpr) -> DataType {
        match expr {
            ResolvedExpr::Function {
                name, result_type, ..
            } => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    "COUNT" => DataType::BigInt,
                    "SUM" => DataType::Double,
                    "AVG" => DataType::Double,
                    "STDDEV" | "STD" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
                    | "VAR_SAMP" => DataType::Double,
                    _ => result_type.clone(),
                }
            }
            _ => DataType::Int,
        }
    }

    /// Get the type of an expression
    fn get_expr_type(expr: &ResolvedExpr) -> DataType {
        match expr {
            ResolvedExpr::Column(col) => col.data_type.clone(),
            ResolvedExpr::Literal(lit) => match lit {
                Literal::Integer(_) => DataType::BigInt,
                Literal::UnsignedInteger(_) => DataType::BigIntUnsigned,
                Literal::Float(_) => DataType::Double,
                Literal::String(_) => DataType::Text,
                Literal::Boolean(_) => DataType::Boolean,
                Literal::Null => DataType::Int,
                Literal::Blob(_) => DataType::Blob,
                Literal::Decimal(value, scale) => {
                    let digits = if *value == 0 {
                        1
                    } else {
                        value.unsigned_abs().ilog10() as u8 + 1
                    };
                    DataType::Decimal {
                        precision: digits.max(*scale),
                        scale: *scale,
                    }
                }
                Literal::Placeholder(_) => DataType::BigInt,
            },
            ResolvedExpr::Function { result_type, .. } => result_type.clone(),
            ResolvedExpr::BinaryOp { result_type, .. } => result_type.clone(),
            _ => DataType::Int,
        }
    }

    /// Find if an expression matches one in the group-by list
    fn find_in_group_by(expr: &ResolvedExpr, group_by: &[ResolvedExpr]) -> Option<usize> {
        group_by
            .iter()
            .enumerate()
            .find_map(|(idx, gb_expr)| Self::exprs_equal(expr, gb_expr).then_some(idx))
    }

    /// Check if two expressions are structurally equal
    fn exprs_equal(a: &ResolvedExpr, b: &ResolvedExpr) -> bool {
        match (a, b) {
            (ResolvedExpr::Column(ca), ResolvedExpr::Column(cb)) => {
                ca.table == cb.table && ca.name == cb.name && ca.index == cb.index
            }
            (ResolvedExpr::Literal(la), ResolvedExpr::Literal(lb)) => la == lb,
            (
                ResolvedExpr::BinaryOp {
                    left: la,
                    op: oa,
                    right: ra,
                    ..
                },
                ResolvedExpr::BinaryOp {
                    left: lb,
                    op: ob,
                    right: rb,
                    ..
                },
            ) => oa == ob && Self::exprs_equal(la, lb) && Self::exprs_equal(ra, rb),
            (
                ResolvedExpr::UnaryOp {
                    op: oa, expr: ea, ..
                },
                ResolvedExpr::UnaryOp {
                    op: ob, expr: eb, ..
                },
            ) => oa == ob && Self::exprs_equal(ea, eb),
            (
                ResolvedExpr::Function {
                    name: na, args: aa, ..
                },
                ResolvedExpr::Function {
                    name: nb, args: ab, ..
                },
            ) => {
                na == nb
                    && aa.len() == ab.len()
                    && aa
                        .iter()
                        .zip(ab.iter())
                        .all(|(x, y)| Self::exprs_equal(x, y))
            }
            (
                ResolvedExpr::Cast {
                    expr: ea,
                    target_type: ta,
                },
                ResolvedExpr::Cast {
                    expr: eb,
                    target_type: tb,
                },
            ) => ta == tb && Self::exprs_equal(ea, eb),
            (
                ResolvedExpr::Case {
                    operand: oa,
                    conditions: ca,
                    results: ra,
                    else_result: ea,
                    ..
                },
                ResolvedExpr::Case {
                    operand: ob,
                    conditions: cb,
                    results: rb,
                    else_result: eb,
                    ..
                },
            ) => {
                let operands_eq = match (oa.as_ref(), ob.as_ref()) {
                    (Some(a), Some(b)) => Self::exprs_equal(a, b),
                    (None, None) => true,
                    _ => false,
                };
                let else_eq = match (ea.as_ref(), eb.as_ref()) {
                    (Some(a), Some(b)) => Self::exprs_equal(a, b),
                    (None, None) => true,
                    _ => false,
                };
                operands_eq
                    && ca.len() == cb.len()
                    && ra.len() == rb.len()
                    && ca
                        .iter()
                        .zip(cb.iter())
                        .all(|(x, y)| Self::exprs_equal(x, y))
                    && ra
                        .iter()
                        .zip(rb.iter())
                        .all(|(x, y)| Self::exprs_equal(x, y))
                    && else_eq
            }
            (
                ResolvedExpr::IsNull {
                    expr: ea,
                    negated: na,
                },
                ResolvedExpr::IsNull {
                    expr: eb,
                    negated: nb,
                },
            ) => na == nb && Self::exprs_equal(ea, eb),
            (
                ResolvedExpr::InList {
                    expr: ea,
                    list: la,
                    negated: na,
                },
                ResolvedExpr::InList {
                    expr: eb,
                    list: lb,
                    negated: nb,
                },
            ) => {
                na == nb
                    && Self::exprs_equal(ea, eb)
                    && la.len() == lb.len()
                    && la
                        .iter()
                        .zip(lb.iter())
                        .all(|(x, y)| Self::exprs_equal(x, y))
            }
            (
                ResolvedExpr::Between {
                    expr: ea,
                    low: la,
                    high: ha,
                    negated: na,
                },
                ResolvedExpr::Between {
                    expr: eb,
                    low: lb,
                    high: hb,
                    negated: nb,
                },
            ) => {
                na == nb
                    && Self::exprs_equal(ea, eb)
                    && Self::exprs_equal(la, lb)
                    && Self::exprs_equal(ha, hb)
            }
            (
                ResolvedExpr::BooleanTest { expr: ea, test: ta },
                ResolvedExpr::BooleanTest { expr: eb, test: tb },
            ) => ta == tb && Self::exprs_equal(ea, eb),
            // ExistsSubquery: compare by negation (queries are opaque here)
            (
                ResolvedExpr::ExistsSubquery { negated: na, .. },
                ResolvedExpr::ExistsSubquery { negated: nb, .. },
            ) => na == nb,
            _ => false,
        }
    }

    /// Collect all aggregates from SELECT columns and HAVING clause
    fn collect_all_aggregates(
        columns: &[ResolvedSelectItem],
        having: Option<&ResolvedExpr>,
    ) -> Vec<(AggregateFunc, String)> {
        let mut aggs = Vec::new();
        for (idx, item) in columns.iter().enumerate() {
            if let ResolvedSelectItem::Expr { expr, alias } = item {
                Self::collect_aggregates(expr, idx, alias.as_deref(), &mut aggs);
            }
        }
        if let Some(having_expr) = having {
            let next_idx = columns.len();
            Self::collect_aggregates(having_expr, next_idx, None, &mut aggs);
        }
        aggs
    }

    /// Transform HAVING expression using the full aggregates list
    fn transform_having_expr(
        expr: &ResolvedExpr,
        group_by: &[ResolvedExpr],
        all_aggs: &[(AggregateFunc, String)],
    ) -> ResolvedExpr {
        // Check if this expression is a direct aggregate
        if let Some(agg) = Self::extract_aggregate(expr) {
            if let Some(pos) = all_aggs
                .iter()
                .position(|(a, _)| Self::aggregates_match_func(a, &agg))
            {
                let col_idx = group_by.len() + pos;
                let result_type = Self::get_aggregate_result_type(expr);
                return ResolvedExpr::Column(ResolvedColumn {
                    table: String::new(),
                    name: all_aggs[pos].1.clone(),
                    index: col_idx,
                    data_type: result_type,
                    nullable: true,
                    default_value: None,
                });
            }
        }
        // Recurse into sub-expressions
        match expr {
            ResolvedExpr::Function {
                name,
                args,
                distinct,
                result_type,
                separator,
            } => {
                // Guard: do not recurse into aggregate function args
                if Self::extract_aggregate(expr).is_some() {
                    return expr.clone();
                }
                ResolvedExpr::Function {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|a| Self::transform_having_expr(a, group_by, all_aggs))
                        .collect(),
                    distinct: *distinct,
                    result_type: result_type.clone(),
                    separator: separator.clone(),
                }
            }
            ResolvedExpr::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => ResolvedExpr::BinaryOp {
                left: Box::new(Self::transform_having_expr(left, group_by, all_aggs)),
                op: *op,
                right: Box::new(Self::transform_having_expr(right, group_by, all_aggs)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::UnaryOp {
                op,
                expr: inner,
                result_type,
            } => ResolvedExpr::UnaryOp {
                op: *op,
                expr: Box::new(Self::transform_having_expr(inner, group_by, all_aggs)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::IsNull {
                expr: inner,
                negated,
            } => ResolvedExpr::IsNull {
                expr: Box::new(Self::transform_having_expr(inner, group_by, all_aggs)),
                negated: *negated,
            },
            ResolvedExpr::Column(col) => {
                if let Some(gb_idx) = Self::find_in_group_by(expr, group_by) {
                    ResolvedExpr::Column(ResolvedColumn {
                        table: String::new(),
                        name: format!("gb_{}", gb_idx),
                        index: gb_idx,
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                        default_value: None,
                    })
                } else {
                    expr.clone()
                }
            }
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                result_type,
            } => ResolvedExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::transform_having_expr(e, group_by, all_aggs))),
                conditions: conditions
                    .iter()
                    .map(|c| Self::transform_having_expr(c, group_by, all_aggs))
                    .collect(),
                results: results
                    .iter()
                    .map(|r| Self::transform_having_expr(r, group_by, all_aggs))
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(Self::transform_having_expr(e, group_by, all_aggs))),
                result_type: result_type.clone(),
            },
            ResolvedExpr::InList {
                expr,
                list,
                negated,
            } => ResolvedExpr::InList {
                expr: Box::new(Self::transform_having_expr(expr, group_by, all_aggs)),
                list: list
                    .iter()
                    .map(|e| Self::transform_having_expr(e, group_by, all_aggs))
                    .collect(),
                negated: *negated,
            },
            ResolvedExpr::Between {
                expr,
                low,
                high,
                negated,
            } => ResolvedExpr::Between {
                expr: Box::new(Self::transform_having_expr(expr, group_by, all_aggs)),
                low: Box::new(Self::transform_having_expr(low, group_by, all_aggs)),
                high: Box::new(Self::transform_having_expr(high, group_by, all_aggs)),
                negated: *negated,
            },
            ResolvedExpr::BooleanTest { expr, test } => ResolvedExpr::BooleanTest {
                expr: Box::new(Self::transform_having_expr(expr, group_by, all_aggs)),
                test: *test,
            },
            // Subqueries are self-contained, pass through
            ResolvedExpr::ScalarSubquery { .. } => expr.clone(),
            ResolvedExpr::InSubquery {
                expr: inner,
                query,
                negated,
            } => ResolvedExpr::InSubquery {
                expr: Box::new(Self::transform_having_expr(inner, group_by, all_aggs)),
                query: query.clone(),
                negated: *negated,
            },
            ResolvedExpr::ExistsSubquery { .. } => expr.clone(),
            other => other.clone(),
        }
    }

    /// Check if two aggregate expressions match (same function and arguments)
    fn aggregates_match(a: &ResolvedExpr, b: &ResolvedExpr) -> bool {
        match (a, b) {
            (
                ResolvedExpr::Function {
                    name: name_a,
                    args: args_a,
                    ..
                },
                ResolvedExpr::Function {
                    name: name_b,
                    args: args_b,
                    ..
                },
            ) => {
                if name_a.to_uppercase() != name_b.to_uppercase() {
                    return false;
                }
                if args_a.len() != args_b.len() {
                    return false;
                }
                args_a.iter().zip(args_b.iter()).all(|(aa, ab)| {
                    // Simple structural match for arguments
                    Self::exprs_equal(aa, ab)
                        || (matches!(aa, ResolvedExpr::Literal(Literal::Null))
                            && matches!(ab, ResolvedExpr::Literal(Literal::Null)))
                })
            }
            _ => false,
        }
    }

    /// Generate a name for an expression
    pub fn expr_name(expr: &ResolvedExpr, _idx: usize) -> String {
        Self::expr_to_sql(expr)
    }

    /// Reconstruct SQL-like text from a resolved expression for column naming.
    /// MySQL uses the original SQL text as the column name for expressions.
    fn expr_to_sql(expr: &ResolvedExpr) -> String {
        match expr {
            ResolvedExpr::Column(col) => col.name.clone(),
            ResolvedExpr::Function { name, args, .. } => {
                let arg_strs: Vec<String> = args.iter().map(Self::expr_to_sql).collect();
                // Undo internal name mangling for display; preserve original casing
                let display_name = if let Some(stripped) = name.strip_prefix("_ROODB_") {
                    stripped.to_string()
                } else {
                    name.clone()
                };
                format!("{}({})", display_name, arg_strs.join(","))
            }
            ResolvedExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::UnsignedInteger(u) => u.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => format!("'{}'", s),
                Literal::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                Literal::Null => "NULL".to_string(),
                Literal::Blob(_) => "blob".to_string(),
                Literal::Decimal(value, scale) => {
                    crate::executor::datum::format_decimal(*value, *scale)
                }
                Literal::Placeholder(n) => format!("?{}", n),
            },
            ResolvedExpr::BinaryOp {
                left, op, right, ..
            } => {
                let op_str = match op {
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::Eq => "=",
                    BinaryOp::NotEq => "<>",
                    BinaryOp::Lt => "<",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Like => "LIKE",
                    BinaryOp::NotLike => "NOT LIKE",
                    BinaryOp::BitwiseOr => "|",
                    BinaryOp::BitwiseAnd => "&",
                    BinaryOp::BitwiseXor => "^",
                    BinaryOp::ShiftLeft => "<<",
                    BinaryOp::ShiftRight => ">>",
                    BinaryOp::IntDiv => "DIV",
                    BinaryOp::Xor => "XOR",
                    BinaryOp::Spaceship => "<=>",
                    BinaryOp::Assign => ":=",
                };
                format!(
                    "{} {} {}",
                    Self::expr_to_sql(left),
                    op_str,
                    Self::expr_to_sql(right)
                )
            }
            ResolvedExpr::UnaryOp { op, expr, .. } => {
                let op_str = match op {
                    UnaryOp::Not => "NOT ",
                    UnaryOp::Neg => "-",
                    UnaryOp::Plus => "",
                    UnaryOp::BitwiseNot => "~",
                };
                format!("{}{}", op_str, Self::expr_to_sql(expr))
            }
            ResolvedExpr::IsNull { expr, negated } => {
                if *negated {
                    format!("{} IS NOT NULL", Self::expr_to_sql(expr))
                } else {
                    format!("{} IS NULL", Self::expr_to_sql(expr))
                }
            }
            ResolvedExpr::Cast {
                expr, target_type, ..
            } => {
                format!(
                    "CAST({} AS {})",
                    Self::expr_to_sql(expr),
                    target_type.sql_name()
                )
            }
            ResolvedExpr::UserVariable { name } => format!("@{}", name),
            ResolvedExpr::Case { .. } => "CASE".to_string(),
            ResolvedExpr::BooleanTest { expr, test } => {
                format!("{} IS {:?}", Self::expr_to_sql(expr), test)
            }
            ResolvedExpr::InList {
                expr,
                list,
                negated,
            } => {
                let expr_sql = Self::expr_to_sql(expr);
                let neg = if *negated { " NOT" } else { "" };
                let items: Vec<String> = list.iter().map(Self::expr_to_sql).collect();
                format!("{}{} IN ({})", expr_sql, neg, items.join(", "))
            }
            ResolvedExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let neg = if *negated { " NOT" } else { "" };
                format!(
                    "{}{} BETWEEN {} AND {}",
                    Self::expr_to_sql(expr),
                    neg,
                    Self::expr_to_sql(low),
                    Self::expr_to_sql(high)
                )
            }
            ResolvedExpr::ScalarSubquery { .. } => "(SELECT ...)".to_string(),
            ResolvedExpr::InSubquery { expr, negated, .. } => {
                let neg = if *negated { " NOT" } else { "" };
                format!("{}{} IN (SELECT ...)", Self::expr_to_sql(expr), neg)
            }
            ResolvedExpr::ExistsSubquery { negated, .. } => {
                if *negated {
                    "NOT EXISTS (SELECT ...)".to_string()
                } else {
                    "EXISTS (SELECT ...)".to_string()
                }
            }
        }
    }

    /// Transform an expression to use output column indices from the projection
    ///
    /// ORDER BY expressions use input (joined) column indices, but after projection
    /// the row only has output columns. This function maps input column references
    /// to their corresponding output column indices.
    fn transform_to_output_columns(
        expr: &ResolvedExpr,
        columns: &[ResolvedSelectItem],
    ) -> ResolvedExpr {
        // First check if the whole expression matches a SELECT item (handles
        // aggregate functions like sum(b) referenced by alias in ORDER BY/HAVING)
        if Self::expr_has_aggregate(expr) {
            let mut output_idx = 0usize;
            for item in columns {
                if let ResolvedSelectItem::Expr {
                    expr: sel_expr,
                    alias,
                } = item
                {
                    if Self::aggregates_match(expr, sel_expr) {
                        let name = alias.clone().unwrap_or_default();
                        return ResolvedExpr::Column(ResolvedColumn {
                            table: String::new(),
                            name,
                            index: output_idx,
                            data_type: expr.data_type(),
                            nullable: true,
                            default_value: None,
                        });
                    }
                    output_idx += 1;
                }
            }
        }
        match expr {
            ResolvedExpr::Column(col) => {
                // Find this column in the SELECT list
                let mut output_idx = 0usize;
                for item in columns {
                    match item {
                        ResolvedSelectItem::Expr { expr: sel_expr, .. } => {
                            if let ResolvedExpr::Column(sel_col) = sel_expr {
                                // Match by table and name (or just name if table is empty)
                                if (col.table.is_empty()
                                    || sel_col.table.is_empty()
                                    || col.table == sel_col.table)
                                    && col.name == sel_col.name
                                {
                                    // Found match - use output index
                                    return ResolvedExpr::Column(ResolvedColumn {
                                        table: col.table.clone(),
                                        name: col.name.clone(),
                                        index: output_idx,
                                        data_type: col.data_type.clone(),
                                        nullable: col.nullable,
                                        default_value: None,
                                    });
                                }
                            }
                            output_idx += 1;
                        }
                        ResolvedSelectItem::Columns(cols) => {
                            // Wildcard expansion - check each column
                            for expanded_col in cols {
                                if (col.table.is_empty()
                                    || expanded_col.table.is_empty()
                                    || col.table == expanded_col.table)
                                    && col.name == expanded_col.name
                                {
                                    return ResolvedExpr::Column(ResolvedColumn {
                                        table: col.table.clone(),
                                        name: col.name.clone(),
                                        index: output_idx,
                                        data_type: col.data_type.clone(),
                                        nullable: col.nullable,
                                        default_value: None,
                                    });
                                }
                                output_idx += 1;
                            }
                        }
                    }
                }
                // Column not found in SELECT - keep original (may cause error at runtime)
                expr.clone()
            }
            ResolvedExpr::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => ResolvedExpr::BinaryOp {
                left: Box::new(Self::transform_to_output_columns(left, columns)),
                op: *op,
                right: Box::new(Self::transform_to_output_columns(right, columns)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::UnaryOp {
                op,
                expr: inner,
                result_type,
            } => ResolvedExpr::UnaryOp {
                op: *op,
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::Function {
                name,
                args,
                distinct,
                result_type,
                separator,
            } => ResolvedExpr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::transform_to_output_columns(a, columns))
                    .collect(),
                distinct: *distinct,
                result_type: result_type.clone(),
                separator: separator.clone(),
            },
            ResolvedExpr::IsNull {
                expr: inner,
                negated,
            } => ResolvedExpr::IsNull {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                negated: *negated,
            },
            ResolvedExpr::Cast {
                expr: inner,
                target_type,
            } => ResolvedExpr::Cast {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                target_type: target_type.clone(),
            },
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                result_type,
            } => ResolvedExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::transform_to_output_columns(e, columns))),
                conditions: conditions
                    .iter()
                    .map(|c| Self::transform_to_output_columns(c, columns))
                    .collect(),
                results: results
                    .iter()
                    .map(|r| Self::transform_to_output_columns(r, columns))
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(Self::transform_to_output_columns(e, columns))),
                result_type: result_type.clone(),
            },
            ResolvedExpr::InList {
                expr: inner,
                list,
                negated,
            } => ResolvedExpr::InList {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                list: list
                    .iter()
                    .map(|e| Self::transform_to_output_columns(e, columns))
                    .collect(),
                negated: *negated,
            },
            ResolvedExpr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => ResolvedExpr::Between {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                low: Box::new(Self::transform_to_output_columns(low, columns)),
                high: Box::new(Self::transform_to_output_columns(high, columns)),
                negated: *negated,
            },
            ResolvedExpr::BooleanTest { expr: inner, test } => ResolvedExpr::BooleanTest {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                test: *test,
            },
            // Subqueries are self-contained, pass through
            ResolvedExpr::ScalarSubquery { .. } => expr.clone(),
            ResolvedExpr::InSubquery {
                expr: inner,
                query,
                negated,
            } => ResolvedExpr::InSubquery {
                expr: Box::new(Self::transform_to_output_columns(inner, columns)),
                query: query.clone(),
                negated: *negated,
            },
            ResolvedExpr::ExistsSubquery { .. } => expr.clone(),
            // Other expression types pass through unchanged
            _ => expr.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::test_utils::test_catalog;
    use crate::sql::{Parser, Resolver, TypeChecker};

    #[test]
    fn test_build_simple_select() {
        let catalog = test_catalog();
        let sql = "SELECT id, name FROM users";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let plan = LogicalPlanBuilder::build(resolved).unwrap();

        // Should be: Project -> Scan
        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    #[test]
    fn test_build_select_with_filter() {
        let catalog = test_catalog();
        let sql = "SELECT id FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let plan = LogicalPlanBuilder::build(resolved).unwrap();

        // Should be: Project -> Filter -> Scan
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Filter { .. }));
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_build_insert() {
        let catalog = test_catalog();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let plan = LogicalPlanBuilder::build(resolved).unwrap();
        assert!(matches!(plan, LogicalPlan::Insert { .. }));
    }
}
