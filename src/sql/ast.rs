//! Internal AST types
//!
//! These types represent parsed SQL statements in a form that's easier
//! to work with than the sqlparser AST.

use crate::catalog::{ColumnDef, Constraint, DataType};

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

/// Expression (before resolution)
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference (optionally qualified with table name)
    Column { table: Option<String>, name: String },
    /// Literal value
    Literal(Literal),
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },
    /// IN (list)
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// Scalar subquery
    Subquery(Box<SelectStatement>),
    /// Wildcard (SELECT *)
    Wildcard,
    /// Qualified wildcard (SELECT table.*)
    QualifiedWildcard(String),
}

/// SELECT item (column in SELECT list)
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// Expression with optional alias
    Expr { expr: Expr, alias: Option<String> },
    /// Wildcard (*)
    Wildcard,
    /// Qualified wildcard (table.*)
    QualifiedWildcard(String),
}

/// Table reference in FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
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

/// JOIN clause
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub table: TableRef,
    pub join_type: JoinType,
    pub condition: Option<Expr>,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Assignment for UPDATE
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

/// SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
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
        columns: Vec<String>,
        unique: bool,
    },
    /// DROP INDEX
    DropIndex { name: String },
    /// INSERT
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    /// UPDATE
    Update {
        table: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    },
    /// DELETE
    Delete { table: String, filter: Option<Expr> },
    /// SELECT
    Select(SelectStatement),
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
}
