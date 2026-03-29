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
    // Bitwise
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    // Integer division
    IntDiv,
    // Logical
    Xor,
    // NULL-safe comparison
    Spaceship,
    // Assignment (:=)
    Assign,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    Plus,
    BitwiseNot,
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

/// Boolean test type for IS TRUE / IS FALSE / IS UNKNOWN predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanTestType {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

/// Literal value
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    UnsignedInteger(u64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    /// Fixed-point decimal (unscaled value, scale)
    Decimal(i128, u8),
    /// Parameter placeholder (index into params array) for prepared statement plan caching
    Placeholder(usize),
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
    /// Column DEFAULT value expression (as string), from ColumnDef.default
    pub default_value: Option<String>,
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
    /// IS TRUE / IS FALSE / IS UNKNOWN predicates
    BooleanTest {
        expr: Box<ResolvedExpr>,
        test: BooleanTestType,
    },
    /// CAST(expr AS type)
    Cast {
        expr: Box<ResolvedExpr>,
        target_type: DataType,
    },
    /// User variable reference (@var)
    UserVariable { name: String },
    /// CASE expression (simple and searched)
    Case {
        /// Simple CASE: the operand to compare against; None for searched CASE
        operand: Option<Box<ResolvedExpr>>,
        /// WHEN conditions
        conditions: Vec<ResolvedExpr>,
        /// THEN results (parallel to conditions)
        results: Vec<ResolvedExpr>,
        /// Optional ELSE result
        else_result: Option<Box<ResolvedExpr>>,
        /// Inferred result type
        result_type: DataType,
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
                Literal::UnsignedInteger(_) => DataType::BigIntUnsigned,
                Literal::Float(_) => DataType::Double,
                Literal::String(_) => DataType::Text,
                Literal::Blob(_) => DataType::Blob,
                Literal::Decimal(value, scale) => {
                    let digits = if *value == 0 {
                        1
                    } else {
                        value.unsigned_abs().ilog10() as u8 + 1
                    };
                    DataType::Decimal {
                        precision: digits.max(*scale),
                        scale: *scale,
                    }
                }
                Literal::Placeholder(_) => DataType::BigInt, // Placeholder type determined at substitution
            },
            ResolvedExpr::BinaryOp { result_type, .. } => result_type.clone(),
            ResolvedExpr::UnaryOp { result_type, .. } => result_type.clone(),
            ResolvedExpr::Function { result_type, .. } => result_type.clone(),
            ResolvedExpr::IsNull { .. } => DataType::Boolean,
            ResolvedExpr::InList { .. } => DataType::Boolean,
            ResolvedExpr::Between { .. } => DataType::Boolean,
            ResolvedExpr::BooleanTest { .. } => DataType::Boolean,
            ResolvedExpr::Cast { target_type, .. } => target_type.clone(),
            ResolvedExpr::UserVariable { .. } => DataType::Text,
            ResolvedExpr::Case { result_type, .. } => result_type.clone(),
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
            ResolvedExpr::BooleanTest { .. } => false, // Always returns true/false, never NULL
            ResolvedExpr::Cast { expr, .. } => expr.is_nullable(),
            ResolvedExpr::UserVariable { .. } => true,
            ResolvedExpr::Case {
                results,
                else_result,
                ..
            } => {
                // Case is nullable if any result is nullable or there's no ELSE
                else_result.is_none() || results.iter().any(|r| r.is_nullable())
            }
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
    /// Set for derived tables (subquery in FROM) and view expansions
    pub inner_query: Option<Box<ResolvedSelect>>,
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
    /// DROP TABLE t1, t2, t3 (multiple tables)
    DropMultipleTables { names: Vec<String>, if_exists: bool },
    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>, // (name, index)
        unique: bool,
    },
    /// DROP INDEX
    DropIndex { name: String },
    /// CREATE DATABASE
    CreateDatabase { name: String, if_not_exists: bool },
    /// DROP DATABASE
    DropDatabase { name: String, if_exists: bool },
    /// INSERT with resolved columns
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
        ignore: bool,
    },
    /// INSERT ... SELECT with resolved source query
    InsertSelect {
        table: String,
        columns: Vec<ResolvedColumn>,
        source: Box<ResolvedStatement>,
        /// Maps source column index → target column index for partial column lists.
        /// None when all columns are targeted (1:1 mapping).
        column_map: Option<Vec<usize>>,
        ignore: bool,
    },
    /// UPDATE with resolved assignments
    Update {
        table: String,
        table_columns: Vec<(String, DataType, bool)>,
        assignments: Vec<ResolvedAssignment>,
        filter: Option<ResolvedExpr>,
        order_by: Vec<(ResolvedExpr, bool)>, // (expr, ascending)
        limit: Option<usize>,
    },
    /// DELETE with resolved filter
    Delete {
        table: String,
        table_columns: Vec<(String, DataType, bool)>,
        filter: Option<ResolvedExpr>,
        order_by: Vec<(ResolvedExpr, bool)>, // (expr, ascending)
        limit: Option<usize>,
    },
    /// SELECT with resolved references
    Select(ResolvedSelect),
    /// UNION of two SELECTs
    Union {
        left: Box<ResolvedSelect>,
        right: Box<ResolvedSelect>,
        all: bool,
    },
    /// CREATE TABLE ... SELECT (CTAS) — source can be Select or Union
    CreateTableAs {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        source: Box<ResolvedStatement>,
    },

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
    /// ANALYZE TABLE
    AnalyzeTable { table: String },
    /// EXPLAIN statement
    Explain { inner: Box<ResolvedStatement> },
    /// CREATE VIEW
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },
    /// DROP VIEW
    DropView { name: String, if_exists: bool },
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

    /// UNION of two query plans
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },

    /// Single empty row (TABLE_DEE - for expression-only queries)
    SingleRow,

    /// Derived table (subquery in FROM) — materializes inner plan
    DerivedTable {
        input: Box<LogicalPlan>,
        alias: String,
    },

    // ============ DML Operations ============
    /// INSERT rows into a table
    Insert {
        table: String,
        columns: Vec<ResolvedColumn>,
        values: Vec<Vec<ResolvedExpr>>,
        ignore: bool,
    },

    /// INSERT ... SELECT — insert rows from a source query
    InsertSelect {
        table: String,
        columns: Vec<ResolvedColumn>,
        source: Box<LogicalPlan>,
        /// Maps source column index → target column index for partial column lists.
        /// None when all columns are targeted (1:1 mapping).
        column_map: Option<Vec<usize>>,
        ignore: bool,
    },

    /// UPDATE rows in a table
    Update {
        table: String,
        /// (column, new value)
        assignments: Vec<(ResolvedColumn, ResolvedExpr)>,
        filter: Option<ResolvedExpr>,
        order_by: Vec<(ResolvedExpr, bool)>,
        limit: Option<usize>,
    },

    /// DELETE rows from a table
    Delete {
        table: String,
        filter: Option<ResolvedExpr>,
        order_by: Vec<(ResolvedExpr, bool)>,
        limit: Option<usize>,
    },

    // ============ DDL Operations (passthrough) ============
    /// CREATE TABLE
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
    },

    /// CREATE TABLE ... SELECT
    CreateTableAs {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        source: Box<LogicalPlan>,
    },

    /// DROP TABLE
    DropTable { name: String, if_exists: bool },

    /// DROP TABLE t1, t2, t3 (multiple tables)
    DropMultipleTables { names: Vec<String>, if_exists: bool },

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
    },

    /// DROP INDEX
    DropIndex { name: String },

    /// CREATE DATABASE
    CreateDatabase { name: String, if_not_exists: bool },

    /// DROP DATABASE
    DropDatabase { name: String, if_exists: bool },

    /// CREATE VIEW
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },

    /// DROP VIEW
    DropView { name: String, if_exists: bool },

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

    /// ANALYZE TABLE
    AnalyzeTable { table: String },

    /// EXPLAIN — wraps the inner plan for EXPLAIN output
    Explain { inner: Box<LogicalPlan> },
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
            LogicalPlan::SingleRow => vec![],
            LogicalPlan::DerivedTable { input, .. } => input.output_columns(),

            // DML operations don't produce output columns for query purposes
            LogicalPlan::Insert { .. }
            | LogicalPlan::InsertSelect { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. } => vec![],

            // DDL operations don't produce output columns
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateTableAs { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::DropMultipleTables { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateDatabase { .. }
            | LogicalPlan::DropDatabase { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. } => vec![],

            // Auth operations don't produce output columns (except ShowGrants which returns rows)
            LogicalPlan::CreateUser { .. }
            | LogicalPlan::DropUser { .. }
            | LogicalPlan::AlterUser { .. }
            | LogicalPlan::SetPassword { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::ShowGrants { .. } => vec![],

            // ANALYZE TABLE returns a result set (handled by executor)
            LogicalPlan::AnalyzeTable { .. } => vec![],

            // UNION: output columns from left side
            LogicalPlan::Union { left, .. } => left.output_columns(),

            // EXPLAIN returns MySQL-format result set (handled by executor)
            LogicalPlan::Explain { .. } => vec![],
        }
    }

    /// Check if this is a DDL statement
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::DropMultipleTables { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
                | LogicalPlan::CreateDatabase { .. }
                | LogicalPlan::DropDatabase { .. }
        )
    }

    /// Check if this is a DML statement
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Insert { .. }
                | LogicalPlan::InsertSelect { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
        )
    }
}
