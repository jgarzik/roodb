//! Logical plan builder
//!
//! Converts resolved SQL statements into logical query plans.

use crate::sql::{
    JoinType, Literal, ResolvedExpr, ResolvedOrderByItem, ResolvedSelect, ResolvedSelectItem,
    ResolvedStatement, ResolvedTableRef,
};

use super::expr::{AggregateFunc, OutputColumn};
use super::LogicalPlan;
use crate::planner::error::{PlannerError, PlannerResult};

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
        }
    }

    /// Build logical plan for SELECT statement
    fn build_select(select: ResolvedSelect) -> PlannerResult<LogicalPlan> {
        // 1. Build scan for each table in FROM
        let mut plan = Self::build_from(&select.from)?;

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

            // After aggregation, apply HAVING
            if let Some(having) = select.having {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: having,
                };
            }
        }

        // 4. Apply projection (SELECT list)
        plan = Self::build_project(plan, &select.columns)?;

        // 5. Apply DISTINCT
        if select.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        // 6. Apply ORDER BY
        if !select.order_by.is_empty() {
            plan = Self::build_sort(plan, &select.order_by)?;
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
            return Err(PlannerError::InvalidPlan(
                "SELECT requires at least one table".to_string(),
            ));
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

    /// Build sort node
    fn build_sort(
        input: LogicalPlan,
        order_by: &[ResolvedOrderByItem],
    ) -> PlannerResult<LogicalPlan> {
        let order = order_by
            .iter()
            .map(|item| (item.expr.clone(), item.ascending))
            .collect();

        Ok(LogicalPlan::Sort {
            input: Box::new(input),
            order_by: order,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
    use crate::sql::{Parser, Resolver, TypeChecker};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let users = TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("name", DataType::Varchar(100)))
            .column(ColumnDef::new("age", DataType::Int))
            .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

        catalog.create_table(users).unwrap();
        catalog
    }

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
