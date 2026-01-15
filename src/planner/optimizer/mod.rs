//! Query optimizer
//!
//! Applies optimization rules to logical plans to improve execution efficiency.

pub mod rules;

pub use rules::{FilterMerge, OptimizationRule, PredicatePushdown};

use crate::planner::logical::LogicalPlan;

/// Query optimizer that applies a sequence of optimization rules
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer with default rules
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(FilterMerge),
                Box::new(PredicatePushdown),
            ],
        }
    }

    /// Create an optimizer with custom rules
    pub fn with_rules(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self { rules }
    }

    /// Optimize a logical plan by applying all rules
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut current = plan;
        for rule in &self.rules {
            current = rule.apply(current);
        }
        current
    }

    /// Get the names of all optimization rules
    pub fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|r| r.name()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
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
    fn test_optimizer_pushes_filter_to_scan() {
        let catalog = test_catalog();
        let sql = "SELECT id FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();
        let resolver = Resolver::new(&catalog);
        let resolved = resolver.resolve(stmt).unwrap();
        TypeChecker::check(&resolved).unwrap();

        let logical = LogicalPlanBuilder::build(resolved).unwrap();
        let optimizer = Optimizer::new();
        let optimized = optimizer.optimize(logical);

        // After optimization, filter should be pushed into scan
        // Plan should be: Project -> Scan (with filter)
        match optimized {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Scan { filter, .. } => {
                    assert!(filter.is_some(), "Filter should be pushed to scan");
                }
                _ => panic!("Expected Scan after optimization"),
            },
            _ => panic!("Expected Project"),
        }
    }
}
