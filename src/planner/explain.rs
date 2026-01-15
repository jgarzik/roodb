//! EXPLAIN output formatting
//!
//! Formats query plans for display to users.

use std::fmt::Write;

use crate::planner::physical::PhysicalPlan;

/// Format a physical plan for EXPLAIN output
pub struct ExplainOutput;

impl ExplainOutput {
    /// Format a physical plan as a string
    pub fn format(plan: &PhysicalPlan) -> String {
        let mut output = String::new();
        Self::format_node(plan, 0, &mut output);
        output
    }

    fn format_node(plan: &PhysicalPlan, indent: usize, out: &mut String) {
        let prefix = "  ".repeat(indent);

        match plan {
            PhysicalPlan::TableScan {
                table,
                columns,
                filter,
            } => {
                let col_names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
                writeln!(out, "{}TableScan: {} [{}]", prefix, table, col_names.join(", ")).unwrap();
                if let Some(f) = filter {
                    writeln!(out, "{}  filter: {:?}", prefix, f).unwrap();
                }
            }

            PhysicalPlan::Filter { input, predicate } => {
                writeln!(out, "{}Filter: {:?}", prefix, predicate).unwrap();
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::Project { input, expressions } => {
                let aliases: Vec<_> = expressions.iter().map(|(_, a)| a.as_str()).collect();
                writeln!(out, "{}Project: [{}]", prefix, aliases.join(", ")).unwrap();
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
            } => {
                writeln!(out, "{}NestedLoopJoin: {:?}", prefix, join_type).unwrap();
                if let Some(cond) = condition {
                    writeln!(out, "{}  condition: {:?}", prefix, cond).unwrap();
                }
                writeln!(out, "{}  left:", prefix).unwrap();
                Self::format_node(left, indent + 2, out);
                writeln!(out, "{}  right:", prefix).unwrap();
                Self::format_node(right, indent + 2, out);
            }

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let agg_names: Vec<_> = aggregates.iter().map(|(a, _)| a.name.as_str()).collect();
                writeln!(out, "{}HashAggregate", prefix).unwrap();
                if !group_by.is_empty() {
                    writeln!(out, "{}  group by: {} expressions", prefix, group_by.len()).unwrap();
                }
                if !aggregates.is_empty() {
                    writeln!(out, "{}  aggregates: [{}]", prefix, agg_names.join(", ")).unwrap();
                }
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::Sort { input, order_by } => {
                let dirs: Vec<_> = order_by
                    .iter()
                    .map(|(_, asc)| if *asc { "ASC" } else { "DESC" })
                    .collect();
                writeln!(out, "{}Sort: [{}]", prefix, dirs.join(", ")).unwrap();
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let mut parts = Vec::new();
                if let Some(l) = limit {
                    parts.push(format!("limit={}", l));
                }
                if let Some(o) = offset {
                    parts.push(format!("offset={}", o));
                }
                writeln!(out, "{}Limit: {}", prefix, parts.join(", ")).unwrap();
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::HashDistinct { input } => {
                writeln!(out, "{}HashDistinct", prefix).unwrap();
                Self::format_node(input, indent + 1, out);
            }

            PhysicalPlan::Insert { table, values, .. } => {
                writeln!(out, "{}Insert: {} ({} rows)", prefix, table, values.len()).unwrap();
            }

            PhysicalPlan::Update { table, filter, .. } => {
                writeln!(out, "{}Update: {}", prefix, table).unwrap();
                if let Some(f) = filter {
                    writeln!(out, "{}  filter: {:?}", prefix, f).unwrap();
                }
            }

            PhysicalPlan::Delete { table, filter } => {
                writeln!(out, "{}Delete: {}", prefix, table).unwrap();
                if let Some(f) = filter {
                    writeln!(out, "{}  filter: {:?}", prefix, f).unwrap();
                }
            }

            PhysicalPlan::CreateTable { name, .. } => {
                writeln!(out, "{}CreateTable: {}", prefix, name).unwrap();
            }

            PhysicalPlan::DropTable { name, .. } => {
                writeln!(out, "{}DropTable: {}", prefix, name).unwrap();
            }

            PhysicalPlan::CreateIndex { name, table, .. } => {
                writeln!(out, "{}CreateIndex: {} on {}", prefix, name, table).unwrap();
            }

            PhysicalPlan::DropIndex { name } => {
                writeln!(out, "{}DropIndex: {}", prefix, name).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
    use crate::planner::logical::LogicalPlanBuilder;
    use crate::planner::optimizer::Optimizer;
    use crate::planner::physical::PhysicalPlanner;
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
    fn test_explain_simple_select() {
        let catalog = test_catalog();
        let sql = "SELECT id, name FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimized = Optimizer::new().optimize(logical);
        let physical = PhysicalPlanner::plan(optimized, &catalog).unwrap();

        let explain = ExplainOutput::format(&physical);
        assert!(explain.contains("Project"));
        assert!(explain.contains("TableScan"));
        assert!(explain.contains("users"));
    }

    #[test]
    fn test_explain_with_limit() {
        let catalog = test_catalog();
        let sql = "SELECT * FROM users LIMIT 10";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimized = Optimizer::new().optimize(logical);
        let physical = PhysicalPlanner::plan(optimized, &catalog).unwrap();

        let explain = ExplainOutput::format(&physical);
        assert!(explain.contains("Limit"));
        assert!(explain.contains("limit=10"));
    }
}
