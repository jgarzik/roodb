//! Logical expression types for the query planner

use crate::catalog::DataType;
use crate::sql::ResolvedExpr;

/// Unique ID for columns in the plan
pub type ColumnId = usize;

/// Output column from a plan node
#[derive(Debug, Clone)]
pub struct OutputColumn {
    /// Unique ID within the plan
    pub id: ColumnId,
    /// Column name (or alias)
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether the column can be NULL
    pub nullable: bool,
}

/// Aggregate function specification
#[derive(Debug, Clone)]
pub struct AggregateFunc {
    /// Function name (COUNT, SUM, AVG, MIN, MAX)
    pub name: String,
    /// Arguments to the function
    pub args: Vec<ResolvedExpr>,
    /// Whether DISTINCT is specified
    pub distinct: bool,
    /// Result type of the aggregate
    pub result_type: DataType,
}

impl AggregateFunc {
    /// Create a new aggregate function
    pub fn new(name: impl Into<String>, args: Vec<ResolvedExpr>, distinct: bool, result_type: DataType) -> Self {
        Self {
            name: name.into(),
            args,
            distinct,
            result_type,
        }
    }
}
