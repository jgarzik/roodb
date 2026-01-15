//! Query planner
//!
//! Transforms parsed and resolved SQL statements into executable query plans.
//!
//! ## Pipeline
//!
//! ```text
//! ResolvedStatement
//!   → LogicalPlanBuilder::build() → LogicalPlan
//!   → Optimizer::optimize() → LogicalPlan (optimized)
//!   → PhysicalPlanner::plan() → PhysicalPlan
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use roodb::planner::{LogicalPlanBuilder, Optimizer, PhysicalPlanner};
//!
//! let logical = LogicalPlanBuilder::build(resolved_stmt)?;
//! let optimized = Optimizer::new().optimize(logical);
//! let physical = PhysicalPlanner::plan(optimized, &catalog)?;
//! ```

pub mod cost;
pub mod error;
pub mod explain;
pub mod logical;
pub mod optimizer;
pub mod physical;

#[cfg(test)]
pub mod test_utils;

pub use cost::{Cost, CostEstimator};
pub use error::{PlannerError, PlannerResult};
pub use explain::ExplainOutput;
pub use logical::{LogicalPlan, LogicalPlanBuilder};
pub use optimizer::Optimizer;
pub use physical::{PhysicalPlan, PhysicalPlanner};
