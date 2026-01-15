//! Cost model for query planning
//!
//! Estimates the cost of executing a physical plan. Currently uses
//! placeholder values; future versions will use table statistics.

use crate::catalog::Catalog;
use crate::planner::physical::PhysicalPlan;

/// Cost estimate for a plan
#[derive(Debug, Clone, Default)]
pub struct Cost {
    /// Estimated number of rows produced
    pub rows: f64,
    /// CPU cost (arbitrary units)
    pub cpu: f64,
    /// I/O cost (arbitrary units)
    pub io: f64,
}

impl Cost {
    /// Calculate total cost (I/O weighted higher than CPU)
    pub fn total(&self) -> f64 {
        self.cpu + self.io * 10.0
    }

    /// Create a zero cost
    pub fn zero() -> Self {
        Self::default()
    }
}

/// Cost estimator for physical plans
pub struct CostEstimator;

impl CostEstimator {
    /// Placeholder row count for tables (no statistics available)
    const DEFAULT_TABLE_ROWS: f64 = 1000.0;

    /// Placeholder selectivity for filters
    const DEFAULT_SELECTIVITY: f64 = 0.1;

    /// Estimate the cost of executing a physical plan
    pub fn estimate(plan: &PhysicalPlan, _catalog: &Catalog) -> Cost {
        Self::estimate_node(plan)
    }

    fn estimate_node(plan: &PhysicalPlan) -> Cost {
        match plan {
            PhysicalPlan::TableScan { filter, .. } => {
                let rows = if filter.is_some() {
                    Self::DEFAULT_TABLE_ROWS * Self::DEFAULT_SELECTIVITY
                } else {
                    Self::DEFAULT_TABLE_ROWS
                };
                Cost {
                    rows,
                    cpu: rows,
                    io: rows / 100.0, // Assume 100 rows per I/O
                }
            }

            PhysicalPlan::Filter { input, .. } => {
                let input_cost = Self::estimate_node(input);
                Cost {
                    rows: input_cost.rows * Self::DEFAULT_SELECTIVITY,
                    cpu: input_cost.cpu + input_cost.rows,
                    io: input_cost.io,
                }
            }

            PhysicalPlan::Project { input, .. } => {
                let input_cost = Self::estimate_node(input);
                Cost {
                    rows: input_cost.rows,
                    cpu: input_cost.cpu + input_cost.rows * 0.1, // Small CPU overhead
                    io: input_cost.io,
                }
            }

            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let left_cost = Self::estimate_node(left);
                let right_cost = Self::estimate_node(right);
                let rows = left_cost.rows * right_cost.rows * Self::DEFAULT_SELECTIVITY;
                Cost {
                    rows,
                    // NLJ scans right for each left row
                    cpu: left_cost.cpu + left_cost.rows * right_cost.cpu,
                    io: left_cost.io + left_cost.rows * right_cost.io,
                }
            }

            PhysicalPlan::HashAggregate { input, .. } => {
                let input_cost = Self::estimate_node(input);
                // Assume ~10% of rows are unique groups
                let rows = (input_cost.rows * 0.1).max(1.0);
                Cost {
                    rows,
                    cpu: input_cost.cpu + input_cost.rows, // Hash table operations
                    io: input_cost.io,
                }
            }

            PhysicalPlan::Sort { input, .. } => {
                let input_cost = Self::estimate_node(input);
                let n = input_cost.rows;
                Cost {
                    rows: n,
                    // O(n log n) for sorting
                    cpu: input_cost.cpu + n * (n.ln().max(1.0)),
                    io: input_cost.io,
                }
            }

            PhysicalPlan::Limit { input, limit, .. } => {
                let input_cost = Self::estimate_node(input);
                let rows = match limit {
                    Some(l) => (*l as f64).min(input_cost.rows),
                    None => input_cost.rows,
                };
                Cost {
                    rows,
                    cpu: input_cost.cpu,
                    io: input_cost.io,
                }
            }

            PhysicalPlan::HashDistinct { input } => {
                let input_cost = Self::estimate_node(input);
                // Assume ~50% of rows are unique
                let rows = input_cost.rows * 0.5;
                Cost {
                    rows,
                    cpu: input_cost.cpu + input_cost.rows,
                    io: input_cost.io,
                }
            }

            // DML/DDL have minimal query cost
            PhysicalPlan::Insert { values, .. } => Cost {
                rows: values.len() as f64,
                cpu: values.len() as f64,
                io: 1.0,
            },

            PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => Cost {
                rows: Self::DEFAULT_TABLE_ROWS * Self::DEFAULT_SELECTIVITY,
                cpu: Self::DEFAULT_TABLE_ROWS,
                io: Self::DEFAULT_TABLE_ROWS / 100.0,
            },

            PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. } => Cost::zero(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, Constraint, DataType, TableDef};
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
    fn test_cost_estimate_simple_scan() {
        let catalog = test_catalog();
        let sql = "SELECT * FROM users";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimized = Optimizer::new().optimize(logical);
        let physical = PhysicalPlanner::plan(optimized, &catalog).unwrap();

        let cost = CostEstimator::estimate(&physical, &catalog);
        assert!(cost.rows > 0.0);
        assert!(cost.cpu > 0.0);
    }

    #[test]
    fn test_cost_estimate_filter_reduces_rows() {
        let catalog = test_catalog();
        let sql = "SELECT * FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimized = Optimizer::new().optimize(logical);
        let physical = PhysicalPlanner::plan(optimized, &catalog).unwrap();

        let cost = CostEstimator::estimate(&physical, &catalog);
        // Filter should reduce estimated rows
        assert!(cost.rows < CostEstimator::DEFAULT_TABLE_ROWS);
    }
}
