//! Logical plan representation
//!
//! Logical plans represent the structure of a query before physical
//! implementation details are decided.

pub mod builder;
pub mod expr;

pub use builder::LogicalPlanBuilder;
pub use expr::{AggregateFunc, ColumnId, OutputColumn};

use crate::catalog::{ColumnDef, Constraint, DataType};
use crate::sql::privileges::{HostPattern, Privilege, PrivilegeObject};

// ============ Operator types (shared with resolver/executor) ============

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // String
    Like,
    NotLike,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

/// JOIN type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Literal value
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
}

// ============ Resolved types (after name resolution) ============

/// Resolved column reference with metadata
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    pub table: String,
    pub name: String,
    pub index: usize,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Resolved expression with type information
#[derive(Debug, Clone)]
pub enum ResolvedExpr {
    /// Resolved column reference
    Column(ResolvedColumn),
    /// Literal value
    Literal(Literal),
    /// Binary operation with result type
    BinaryOp {
        left: Box<ResolvedExpr>,
        op: BinaryOp,
        right: Box<ResolvedExpr>,
        result_type: DataType,
    },
    /// Unary operation with result type
    UnaryOp {
        op: UnaryOp,
        expr: Box<ResolvedExpr>,
        result_type: DataType,
    },
    /// Function call with result type
    Function {
        name: String,
        args: Vec<ResolvedExpr>,
        distinct: bool,
        result_type: DataType,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<ResolvedExpr>,
        negated: bool,
    },
    /// IN (list)
    InList {
        expr: Box<ResolvedExpr>,
        list: Vec<ResolvedExpr>,
        negated: bool,
    },
    /// BETWEEN
    Between {
        expr: Box<ResolvedExpr>,
        low: Box<ResolvedExpr>,
        high: Box<ResolvedExpr>,
        negated: bool,
    },
}

impl ResolvedExpr {
    /// Get the data type of this expression
    pub fn data_type(&self) -> DataType {
        match self {
            ResolvedExpr::Column(col) => col.data_type.clone(),
            ResolvedExpr::Literal(lit) => match lit {
                Literal::Null => DataType::Int, // NULL is polymorphic, default to Int
                Literal::Boolean(_) => DataType::Boolean,
                Literal::Integer(_) => DataType::BigInt,
                Literal::Float(_) => DataType::Double,
                Literal::String(_) => DataType::Text,
                Literal::Blob(_) => DataType::Blob,
            },
            ResolvedExpr::BinaryOp { result_type, .. } => result_type.clone(),
            ResolvedExpr::UnaryOp { result_type, .. } => result_type.clone(),
            ResolvedExpr::Function { result_type, .. } => result_type.clone(),
            ResolvedExpr::IsNull { .. } => DataType::Boolean,
            ResolvedExpr::InList { .. } => DataType::Boolean,
            ResolvedExpr::Between { .. } => DataType::Boolean,
        }
    }

    /// Check if this expression is nullable
    pub fn is_nullable(&self) -> bool {
        match self {
            ResolvedExpr::Column(col) => col.nullable,
            ResolvedExpr::Literal(Literal::Null) => true,
            ResolvedExpr::Literal(_) => false,
            ResolvedExpr::BinaryOp { left, right, .. } => left.is_nullable() || right.is_nullable(),
            ResolvedExpr::UnaryOp { expr, .. } => expr.is_nullable(),
            ResolvedExpr::Function { args, .. } => args.iter().any(|a| a.is_nullable()),
            ResolvedExpr::IsNull { .. } => false,
            ResolvedExpr::InList { expr, list, .. } => {
                expr.is_nullable() || list.iter().any(|e| e.is_nullable())
            }
            ResolvedExpr::Between {
                expr, low, high, ..
            } => expr.is_nullable() || low.is_nullable() || high.is_nullable(),
        }
    }
}

/// Resolved SELECT item
#[derive(Debug, Clone)]
pub enum ResolvedSelectItem {
    /// Single expression
    Expr {
        expr: ResolvedExpr,
        alias: Option<String>,
    },
    /// Expanded wildcard (list of columns)
    Columns(Vec<ResolvedColumn>),
}

/// Resolved table reference
#[derive(Debug, Clone)]
pub struct ResolvedTableRef {
    pub name: String,
    pub alias: Option<String>,
    pub columns: Vec<(String, DataType, bool)>, // (name, type, nullable)
}

/// Resolved ORDER BY item
#[derive(Debug, Clone)]
pub struct ResolvedOrderByItem {
    pub expr: ResolvedExpr,
    pub ascending: bool,
}

/// Resolved JOIN clause
#[derive(Debug, Clone)]
pub struct ResolvedJoin {
    pub table: ResolvedTableRef,
    pub join_type: JoinType,
    pub condition: Option<ResolvedExpr>,
}

/// Resolved assignment for UPDATE
#[derive(Debug, Clone)]
pub struct ResolvedAssignment {
    pub column: ResolvedColumn,
    pub value: ResolvedExpr,
}

/// Resolved SELECT statement
#[derive(Debug, Clone)]
pub struct ResolvedSelect {
    pub distinct: bool,
    pub columns: Vec<ResolvedSelectItem>,
    pub from: Vec<ResolvedTableRef>,
    pub joins: Vec<ResolvedJoin>,
    pub filter: Option<ResolvedExpr>,
    pub group_by: Vec<ResolvedExpr>,
    pub having: Option<ResolvedExpr>,
    pub order_by: Vec<ResolvedOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Resolved statement
#[derive(Debug, Clone)]
pub enum ResolvedStatement {
    /// CREATE TABLE (passthrough, no resolution needed)
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
    },
    /// DROP TABLE
    DropTable { name: String, if_exists: bool },
    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>, // (name, index)
        unique: bool,
    },
    /// DROP INDEX
    DropIndex { name: String },
    /// INSERT with resolved columns
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
    },
    /// UPDATE with resolved assignments
    Update {
        table: String,
        table_columns: Vec<(String, DataType, bool)>,
        assignments: Vec<ResolvedAssignment>,
        filter: Option<ResolvedExpr>,
    },
    /// DELETE with resolved filter
    Delete {
        table: String,
        table_columns: Vec<(String, DataType, bool)>,
        filter: Option<ResolvedExpr>,
    },
    /// SELECT with resolved references
    Select(ResolvedSelect),

    // ============ Auth/User Management ============
    /// CREATE USER 'name'@'host' IDENTIFIED BY 'password'
    CreateUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
        if_not_exists: bool,
    },
    /// ALTER USER 'name'@'host' IDENTIFIED BY 'password'
    AlterUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
    },
    /// DROP USER 'name'@'host'
    DropUser {
        username: String,
        host: HostPattern,
        if_exists: bool,
    },
    /// SET PASSWORD FOR 'name'@'host' = 'password'
    SetPassword {
        username: String,
        host: HostPattern,
        password: String,
    },
    /// GRANT privilege ON object TO 'user'@'host' [WITH GRANT OPTION]
    Grant {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
        with_grant_option: bool,
    },
    /// REVOKE privilege ON object FROM 'user'@'host'
    Revoke {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
    },
    /// SHOW GRANTS [FOR 'user'@'host']
    ShowGrants {
        /// If None, show grants for current user
        for_user: Option<(String, HostPattern)>,
    },
}

/// Logical plan node
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan - read all rows from a table
    Scan {
        table: String,
        columns: Vec<OutputColumn>,
        /// Optional pushed-down filter predicate
        filter: Option<ResolvedExpr>,
    },

    /// Filter rows based on a predicate
    Filter {
        input: Box<LogicalPlan>,
        predicate: ResolvedExpr,
    },

    /// Project columns (SELECT list)
    Project {
        input: Box<LogicalPlan>,
        /// (expression, alias)
        expressions: Vec<(ResolvedExpr, String)>,
    },

    /// Join two inputs
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<ResolvedExpr>,
    },

    /// Aggregate with optional grouping
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<ResolvedExpr>,
        /// (aggregate function, output alias)
        aggregates: Vec<(AggregateFunc, String)>,
    },

    /// Sort rows
    Sort {
        input: Box<LogicalPlan>,
        /// (expression, ascending)
        order_by: Vec<(ResolvedExpr, bool)>,
    },

    /// Limit rows returned
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<u64>,
        offset: Option<u64>,
    },

    /// Remove duplicate rows
    Distinct { input: Box<LogicalPlan> },

    // ============ DML Operations ============
    /// INSERT rows into a table
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
    },

    /// UPDATE rows in a table
    Update {
        table: String,
        /// (column, new value)
        assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
        filter: Option<ResolvedExpr>,
    },

    /// DELETE rows from a table
    Delete {
        table: String,
        filter: Option<ResolvedExpr>,
    },

    // ============ DDL Operations (passthrough) ============
    /// CREATE TABLE
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
    },

    /// DROP TABLE
    DropTable { name: String, if_exists: bool },

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
    },

    /// DROP INDEX
    DropIndex { name: String },

    // ============ Auth Operations ============
    /// CREATE USER
    CreateUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
        if_not_exists: bool,
    },

    /// DROP USER
    DropUser {
        username: String,
        host: HostPattern,
        if_exists: bool,
    },

    /// ALTER USER
    AlterUser {
        username: String,
        host: HostPattern,
        password: Option<String>,
    },

    /// SET PASSWORD
    SetPassword {
        username: String,
        host: HostPattern,
        password: String,
    },

    /// GRANT privileges
    Grant {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
        with_grant_option: bool,
    },

    /// REVOKE privileges
    Revoke {
        privileges: Vec<Privilege>,
        object: PrivilegeObject,
        grantee: String,
        grantee_host: HostPattern,
    },

    /// SHOW GRANTS
    ShowGrants {
        for_user: Option<(String, HostPattern)>,
    },
}

impl LogicalPlan {
    /// Get the output columns of this plan node
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match self {
            LogicalPlan::Scan { columns, .. } => columns.clone(),

            LogicalPlan::Filter { input, .. } => input.output_columns(),

            LogicalPlan::Project { expressions, .. } => expressions
                .iter()
                .enumerate()
                .map(|(i, (expr, alias))| OutputColumn {
                    id: i,
                    name: alias.clone(),
                    data_type: expr.data_type(),
                    nullable: expr.is_nullable(),
                })
                .collect(),

            LogicalPlan::Join { left, right, .. } => {
                let mut cols = left.output_columns();
                let offset = cols.len();
                for mut col in right.output_columns() {
                    col.id += offset;
                    cols.push(col);
                }
                cols
            }

            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut cols: Vec<OutputColumn> = group_by
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| OutputColumn {
                        id: i,
                        name: format!("group_{}", i),
                        data_type: expr.data_type(),
                        nullable: expr.is_nullable(),
                    })
                    .collect();
                let offset = cols.len();
                for (i, (agg, alias)) in aggregates.iter().enumerate() {
                    cols.push(OutputColumn {
                        id: offset + i,
                        name: alias.clone(),
                        data_type: agg.result_type.clone(),
                        nullable: true, // Aggregates can be NULL for empty groups
                    });
                }
                cols
            }

            LogicalPlan::Sort { input, .. } => input.output_columns(),
            LogicalPlan::Limit { input, .. } => input.output_columns(),
            LogicalPlan::Distinct { input } => input.output_columns(),

            // DML operations don't produce output columns for query purposes
            LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. } => vec![],

            // DDL operations don't produce output columns
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. } => vec![],

            // Auth operations don't produce output columns (except ShowGrants which returns rows)
            LogicalPlan::CreateUser { .. }
            | LogicalPlan::DropUser { .. }
            | LogicalPlan::AlterUser { .. }
            | LogicalPlan::SetPassword { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::ShowGrants { .. } => vec![],
        }
    }

    /// Check if this is a DDL statement
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
        )
    }

    /// Check if this is a DML statement
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        )
    }
}
