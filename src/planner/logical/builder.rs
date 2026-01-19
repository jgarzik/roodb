//! Logical plan builder
//!
//! Converts resolved SQL statements into logical query plans.

use super::{
    JoinType, Literal, ResolvedColumn, ResolvedExpr, ResolvedSelect, ResolvedSelectItem,
    ResolvedStatement, ResolvedTableRef,
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
            ResolvedStatement::Insert {
                table,
                columns,
                values,
            } => Ok(LogicalPlan::Insert {
                table,
                columns,
                values,
            }),
            ResolvedStatement::Update {
                table,
                assignments,
                filter,
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
                })
            }
            ResolvedStatement::Delete { table, filter, .. } => {
                Ok(LogicalPlan::Delete { table, filter })
            }
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
            ResolvedStatement::DropTable { name, if_exists } => {
                Ok(LogicalPlan::DropTable { name, if_exists })
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
            plan = Self::build_aggregate(plan, &select.group_by, &select.columns)?;

            // After aggregation, apply HAVING (with transformed aggregates)
            if let Some(having) = select.having {
                // Transform aggregate expressions in HAVING to column references
                let transformed_having =
                    Self::transform_aggregates_in_expr(&having, &select.columns, &select.group_by);
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: transformed_having,
                };
            }

            // After aggregation, build projection that maps to aggregate output columns
            plan = Self::build_post_aggregate_project(plan, &select.columns, &select.group_by)?;
        } else {
            // 4. Apply projection (SELECT list) for non-aggregate queries
            plan = Self::build_project(plan, &select.columns)?;
        }

        // 5. Apply DISTINCT
        if select.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        // 6. Apply ORDER BY (with transformed column references)
        if !select.order_by.is_empty() {
            // Transform ORDER BY expressions to reference projection output columns
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

        // Create scan for first table
        let mut plan = Self::table_scan(&tables[0]);

        // Cross join with remaining tables
        for table in &tables[1..] {
            let right = Self::table_scan(table);
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: JoinType::Cross,
                condition: None,
            };
        }

        Ok(plan)
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

    /// Check if expression contains aggregate functions
    fn expr_has_aggregate(expr: &ResolvedExpr) -> bool {
        match expr {
            ResolvedExpr::Function { name, .. } => {
                let upper = name.to_uppercase();
                matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            ResolvedExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            ResolvedExpr::UnaryOp { expr, .. } => Self::expr_has_aggregate(expr),
            _ => false,
        }
    }

    /// Build aggregate node
    fn build_aggregate(
        input: LogicalPlan,
        group_by: &[ResolvedExpr],
        columns: &[ResolvedSelectItem],
    ) -> PlannerResult<LogicalPlan> {
        let mut aggregates = Vec::new();

        // Extract aggregate functions from select items
        for (idx, item) in columns.iter().enumerate() {
            if let ResolvedSelectItem::Expr { expr, alias } = item {
                if let Some(agg) = Self::extract_aggregate(expr) {
                    let name = alias.clone().unwrap_or_else(|| format!("agg_{}", idx));
                    aggregates.push((agg, name));
                }
            }
        }

        Ok(LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: group_by.to_vec(),
            aggregates,
        })
    }

    /// Extract aggregate function from expression
    fn extract_aggregate(expr: &ResolvedExpr) -> Option<AggregateFunc> {
        match expr {
            ResolvedExpr::Function {
                name,
                args,
                distinct,
                result_type,
            } => {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    Some(AggregateFunc::new(
                        upper,
                        args.clone(),
                        *distinct,
                        result_type.clone(),
                    ))
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
        let mut expressions = Vec::new();
        let mut agg_idx = 0usize; // Track which aggregate we're on

        for (idx, item) in columns.iter().enumerate() {
            match item {
                ResolvedSelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| Self::expr_name(expr, idx));

                    // Check if this expression is an aggregate function
                    if Self::expr_has_aggregate(expr) {
                        // Map to aggregate result column (after group_by columns)
                        let col_idx = group_by.len() + agg_idx;
                        let result_type = Self::get_aggregate_result_type(expr);
                        let col_ref = ResolvedExpr::Column(ResolvedColumn {
                            table: String::new(),
                            name: name.clone(),
                            index: col_idx,
                            data_type: result_type,
                            nullable: true,
                        });
                        expressions.push((col_ref, name));
                        agg_idx += 1;
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
                            });
                            expressions.push((col_ref, name));
                        } else {
                            // Expression not in GROUP BY - pass through
                            // (should be an error in strict SQL, but let's allow it)
                            expressions.push((expr.clone(), name));
                        }
                    }
                }
                ResolvedSelectItem::Columns(cols) => {
                    // Wildcards shouldn't appear in aggregate queries typically
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
                Literal::Integer(_) => DataType::BigInt, // Use BigInt for integer literals (safer for large values)
                Literal::Float(_) => DataType::Double,
                Literal::String(_) => DataType::Text,
                Literal::Boolean(_) => DataType::Boolean,
                Literal::Null => DataType::Int, // NULL is polymorphic, default to Int
                Literal::Blob(_) => DataType::Blob,
            },
            ResolvedExpr::Function { result_type, .. } => result_type.clone(),
            ResolvedExpr::BinaryOp { result_type, .. } => result_type.clone(),
            _ => DataType::Int,
        }
    }

    /// Find if an expression matches one in the group-by list
    fn find_in_group_by(expr: &ResolvedExpr, group_by: &[ResolvedExpr]) -> Option<usize> {
        for (idx, gb_expr) in group_by.iter().enumerate() {
            if Self::exprs_equal(expr, gb_expr) {
                return Some(idx);
            }
        }
        None
    }

    /// Check if two expressions are structurally equal
    fn exprs_equal(a: &ResolvedExpr, b: &ResolvedExpr) -> bool {
        match (a, b) {
            (ResolvedExpr::Column(ca), ResolvedExpr::Column(cb)) => {
                ca.table == cb.table && ca.name == cb.name && ca.index == cb.index
            }
            (ResolvedExpr::Literal(la), ResolvedExpr::Literal(lb)) => la == lb,
            _ => false, // Simplified - could be more thorough
        }
    }

    /// Transform aggregate expressions in an expression tree to column references
    /// Used for HAVING clauses where aggregates need to reference aggregate output
    fn transform_aggregates_in_expr(
        expr: &ResolvedExpr,
        columns: &[ResolvedSelectItem],
        group_by: &[ResolvedExpr],
    ) -> ResolvedExpr {
        match expr {
            ResolvedExpr::Function { name, args, .. } => {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    // This is an aggregate function - find its index in the aggregate output
                    if let Some(agg_idx) = Self::find_aggregate_index(expr, columns) {
                        let col_idx = group_by.len() + agg_idx;
                        let result_type = Self::get_aggregate_result_type(expr);
                        return ResolvedExpr::Column(ResolvedColumn {
                            table: String::new(),
                            name: upper,
                            index: col_idx,
                            data_type: result_type,
                            nullable: true,
                        });
                    }
                }
                // Non-aggregate function - transform arguments
                ResolvedExpr::Function {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|a| Self::transform_aggregates_in_expr(a, columns, group_by))
                        .collect(),
                    distinct: false,
                    result_type: Self::get_expr_type(expr),
                }
            }
            ResolvedExpr::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => ResolvedExpr::BinaryOp {
                left: Box::new(Self::transform_aggregates_in_expr(left, columns, group_by)),
                op: *op,
                right: Box::new(Self::transform_aggregates_in_expr(right, columns, group_by)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::UnaryOp {
                op,
                expr: inner,
                result_type,
            } => ResolvedExpr::UnaryOp {
                op: *op,
                expr: Box::new(Self::transform_aggregates_in_expr(inner, columns, group_by)),
                result_type: result_type.clone(),
            },
            ResolvedExpr::Column(col) => {
                // Check if this column is in the group-by list
                if let Some(gb_idx) = Self::find_in_group_by(expr, group_by) {
                    ResolvedExpr::Column(ResolvedColumn {
                        table: String::new(),
                        name: col.name.clone(),
                        index: gb_idx,
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                    })
                } else {
                    expr.clone()
                }
            }
            // Other expressions pass through unchanged
            _ => expr.clone(),
        }
    }

    /// Find the index of an aggregate function in the SELECT list
    fn find_aggregate_index(
        target: &ResolvedExpr,
        columns: &[ResolvedSelectItem],
    ) -> Option<usize> {
        let mut agg_idx = 0usize;
        for item in columns {
            if let ResolvedSelectItem::Expr { expr, .. } = item {
                if Self::expr_has_aggregate(expr) {
                    if Self::aggregates_match(target, expr) {
                        return Some(agg_idx);
                    }
                    agg_idx += 1;
                }
            }
        }
        None
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
    fn expr_name(expr: &ResolvedExpr, idx: usize) -> String {
        match expr {
            ResolvedExpr::Column(col) => col.name.clone(),
            ResolvedExpr::Function { name, .. } => name.clone(),
            ResolvedExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => s.clone(),
                Literal::Boolean(b) => b.to_string(),
                Literal::Null => "NULL".to_string(),
                Literal::Blob(_) => "blob".to_string(),
            },
            _ => format!("expr_{}", idx),
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
            } => ResolvedExpr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::transform_to_output_columns(a, columns))
                    .collect(),
                distinct: *distinct,
                result_type: result_type.clone(),
            },
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
