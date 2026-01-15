//! Optimization rules
//!
//! Rules that transform logical plans to improve execution efficiency.

use crate::catalog::DataType;
use crate::planner::logical::LogicalPlan;
use crate::sql::{BinaryOp, ResolvedExpr};

/// Optimization rule trait
pub trait OptimizationRule: Send + Sync {
    /// Rule name for debugging
    fn name(&self) -> &'static str;

    /// Apply the rule to a logical plan
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

/// Push filter predicates down through the plan tree
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        self.transform(plan)
    }
}

impl PredicatePushdown {
    fn transform(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Push filter into scan
            LogicalPlan::Filter { input, predicate } => {
                let transformed_input = self.transform(*input);
                match transformed_input {
                    // Merge filter into scan if scan has no filter
                    LogicalPlan::Scan {
                        table,
                        columns,
                        filter: None,
                    } => LogicalPlan::Scan {
                        table,
                        columns,
                        filter: Some(predicate),
                    },
                    // Merge filter into scan if scan already has filter (AND them)
                    LogicalPlan::Scan {
                        table,
                        columns,
                        filter: Some(existing),
                    } => LogicalPlan::Scan {
                        table,
                        columns,
                        filter: Some(ResolvedExpr::BinaryOp {
                            left: Box::new(existing),
                            op: BinaryOp::And,
                            right: Box::new(predicate),
                            result_type: DataType::Boolean,
                        }),
                    },
                    // Otherwise keep the filter
                    other => LogicalPlan::Filter {
                        input: Box::new(other),
                        predicate,
                    },
                }
            }

            // Recursively transform children
            LogicalPlan::Project { input, expressions } => LogicalPlan::Project {
                input: Box::new(self.transform(*input)),
                expressions,
            },

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => LogicalPlan::Join {
                left: Box::new(self.transform(*left)),
                right: Box::new(self.transform(*right)),
                join_type,
                condition,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.transform(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.transform(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.transform(*input)),
                limit,
                offset,
            },

            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(self.transform(*input)),
            },

            // Leaf nodes and DML/DDL pass through unchanged
            other => other,
        }
    }
}

/// Merge consecutive filter nodes
pub struct FilterMerge;

impl OptimizationRule for FilterMerge {
    fn name(&self) -> &'static str {
        "filter_merge"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        self.transform(plan)
    }
}

impl FilterMerge {
    fn transform(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Merge consecutive filters
            LogicalPlan::Filter { input, predicate } => {
                let transformed_input = self.transform(*input);
                match transformed_input {
                    LogicalPlan::Filter {
                        input: inner_input,
                        predicate: inner_predicate,
                    } => {
                        // Combine predicates with AND
                        LogicalPlan::Filter {
                            input: inner_input,
                            predicate: ResolvedExpr::BinaryOp {
                                left: Box::new(inner_predicate),
                                op: BinaryOp::And,
                                right: Box::new(predicate),
                                result_type: DataType::Boolean,
                            },
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(other),
                        predicate,
                    },
                }
            }

            // Recursively transform children
            LogicalPlan::Project { input, expressions } => LogicalPlan::Project {
                input: Box::new(self.transform(*input)),
                expressions,
            },

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => LogicalPlan::Join {
                left: Box::new(self.transform(*left)),
                right: Box::new(self.transform(*right)),
                join_type,
                condition,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.transform(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.transform(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.transform(*input)),
                limit,
                offset,
            },

            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(self.transform(*input)),
            },

            // Leaf nodes and DML/DDL pass through unchanged
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::expr::OutputColumn;
    use crate::sql::{Literal, ResolvedColumn};

    fn make_scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "test".to_string(),
            columns: vec![OutputColumn {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
            }],
            filter: None,
        }
    }

    fn make_filter(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate: ResolvedExpr::BinaryOp {
                left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                    table: "test".to_string(),
                    name: "id".to_string(),
                    index: 0,
                    data_type: DataType::Int,
                    nullable: false,
                })),
                op: BinaryOp::Gt,
                right: Box::new(ResolvedExpr::Literal(Literal::Integer(10))),
                result_type: DataType::Boolean,
            },
        }
    }

    #[test]
    fn test_predicate_pushdown_into_scan() {
        let scan = make_scan();
        let filter = make_filter(scan);

        let rule = PredicatePushdown;
        let result = rule.apply(filter);

        // Filter should be pushed into scan
        match result {
            LogicalPlan::Scan { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected Scan with filter"),
        }
    }

    #[test]
    fn test_filter_merge() {
        let scan = make_scan();
        let filter1 = make_filter(scan);
        let filter2 = make_filter(filter1);

        let rule = FilterMerge;
        let result = rule.apply(filter2);

        // Should be single filter with AND predicate
        match result {
            LogicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
                assert!(matches!(
                    predicate,
                    ResolvedExpr::BinaryOp {
                        op: BinaryOp::And,
                        ..
                    }
                ));
            }
            _ => panic!("Expected Filter"),
        }
    }
}
