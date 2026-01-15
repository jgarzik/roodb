//! Physical planner
//!
//! Converts logical plans into physical (execution-ready) plans.

use crate::catalog::Catalog;
use crate::planner::error::PlannerResult;
use crate::planner::logical::LogicalPlan;

use super::PhysicalPlan;

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
    use crate::catalog::{ColumnDef, Constraint, DataType, TableDef};
    use crate::planner::logical::LogicalPlanBuilder;
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
