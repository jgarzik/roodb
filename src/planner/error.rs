//! Planner error types

use std::fmt;

/// Planner error
#[derive(Debug, Clone)]
pub enum PlannerError {
    /// Invalid plan structure
    InvalidPlan(String),
    /// Unsupported operation
    UnsupportedOperation(String),
    /// Internal error
    Internal(String),
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::InvalidPlan(msg) => write!(f, "Invalid plan: {}", msg),
            PlannerError::UnsupportedOperation(msg) => write!(f, "Unsupported operation: {}", msg),
            PlannerError::Internal(msg) => write!(f, "Internal planner error: {}", msg),
        }
    }
}

impl std::error::Error for PlannerError {}

/// Result type for planner operations
pub type PlannerResult<T> = Result<T, PlannerError>;
