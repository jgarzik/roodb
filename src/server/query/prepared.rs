//! Prepared statements: parse once, plan once, bind parameters per execution.
//!
//! The SQL is parsed into an AST at prepare time. On first execution the
//! plan is built with `?` placeholders left in place and cached; each
//! execution substitutes the bound values into a copy of that plan.

use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{self as sp, Expr, Statement, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::planner::physical::PhysicalPlan;
use crate::sql::privileges::RequiredPrivilege;

use super::error::{ProtocolError, ProtocolResult};

// ── CachedPlan ──────────────────────────────────────────────────────

/// A cached physical plan from a previous execution of this prepared statement.
#[derive(Debug, Clone)]
pub struct CachedPlan {
    /// The physical plan (with Placeholder literals for parameters)
    pub physical: PhysicalPlan,
    /// Required privileges for authorization
    pub required_privileges: Vec<RequiredPrivilege>,
    /// Schema version when this plan was cached (invalidate on DDL)
    pub schema_version: u64,
    /// The stored tables and the views the statement uses, each as it was
    /// when the plan was made: what it locks before it runs, and checks.
    pub used: Vec<(crate::catalog::TableName, crate::catalog::ObjectStamp)>,
}

// ── PreparedStatement ───────────────────────────────────────────────

/// A prepared statement: parsed AST + metadata.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Unique statement ID
    pub id: u32,
    /// Original SQL text
    pub sql: String,
    /// Parsed AST (cloned on each execute)
    pub parsed_stmt: Statement,
    /// Number of `?` parameter placeholders
    pub param_count: u16,
    /// Cached physical plan from first execution (reused on subsequent)
    pub cached_plan: Option<CachedPlan>,
}

// ── PreparedStatementManager ────────────────────────────────────────

/// Manages the lifetime of prepared statements for a single connection.
pub struct PreparedStatementManager {
    statements: HashMap<u32, PreparedStatement>,
    next_id: u32,
}

impl PreparedStatementManager {
    /// Create a new (empty) manager.
    pub fn new() -> Self {
        Self {
            statements: HashMap::new(),
            next_id: 1,
        }
    }

    /// Parse `sql`, count placeholders, store, and return the metadata.
    pub fn prepare(&mut self, sql: &str) -> ProtocolResult<&PreparedStatement> {
        let dialect = MySqlDialect {};
        let mut ast = SqlParser::parse_sql(&dialect, sql)
            .map_err(|e| ProtocolError::Sql(crate::sql::SqlError::Parse(e.to_string())))?;
        if ast.len() != 1 {
            return Err(ProtocolError::Sql(crate::sql::SqlError::Parse(
                "Expected exactly one statement".to_string(),
            )));
        }
        let stmt = ast.remove(0);
        let param_count = count_placeholders(&stmt);

        let id = self.next_id;
        self.next_id += 1;

        let ps = PreparedStatement {
            id,
            sql: sql.to_string(),
            parsed_stmt: stmt,
            param_count,
            cached_plan: None,
        };
        self.statements.insert(id, ps);
        Ok(self.statements.get(&id).unwrap())
    }

    /// Look up by ID.
    pub fn get(&self, id: u32) -> Option<&PreparedStatement> {
        self.statements.get(&id)
    }

    /// Look up by ID (mutable).
    pub fn get_mut(&mut self, id: u32) -> Option<&mut PreparedStatement> {
        self.statements.get_mut(&id)
    }

    /// Close (deallocate) a statement.
    pub fn close(&mut self, id: u32) -> bool {
        self.statements.remove(&id).is_some()
    }

    /// Drop all statements (used on COM_RESET_CONNECTION).
    pub fn clear(&mut self) {
        self.statements.clear();
    }
}

impl Default for PreparedStatementManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Placeholder counting ────────────────────────────────────────────

/// Count `?` placeholders in a parsed statement using the visitor API.
pub fn count_placeholders(stmt: &Statement) -> u16 {
    use sqlparser::ast::{Visit, Visitor};

    struct Counter(u16);
    impl Visitor for Counter {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if let Expr::Value(vws) = expr {
                if let Value::Placeholder(s) = &vws.value {
                    if s == "?" {
                        self.0 += 1;
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut c = Counter(0);
    let _ = stmt.visit(&mut c);
    c.0
}

/// Extract the projected column names from a parsed SELECT statement.
///
/// Returns `Some(names)` for SELECT with explicit items (not `*`),
/// or `None` if the column count cannot be determined statically
/// (e.g. `SELECT *` requires catalog lookup).
pub fn select_column_names(stmt: &Statement) -> Option<Vec<String>> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };

    let select = match query.body.as_ref() {
        sp::SetExpr::Select(s) => s,
        _ => return None,
    };

    let mut names = Vec::new();
    for item in &select.projection {
        match item {
            sp::SelectItem::UnnamedExpr(expr) => {
                names.push(expr.to_string());
            }
            sp::SelectItem::ExprWithAlias { alias, .. } => {
                names.push(alias.value.clone());
            }
            // Wildcard / QualifiedWildcard need catalog → give up
            _ => return None,
        }
    }
    Some(names)
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_prepare_and_get() {
        let mut mgr = PreparedStatementManager::new();
        let ps = mgr.prepare("SELECT * FROM t WHERE id = ?").unwrap();
        assert_eq!(ps.id, 1);
        assert_eq!(ps.param_count, 1);
        assert!(mgr.get(1).is_some());
        assert!(mgr.get(99).is_none());
    }

    #[test]
    fn test_manager_close() {
        let mut mgr = PreparedStatementManager::new();
        let _ = mgr.prepare("SELECT 1").unwrap();
        assert!(mgr.close(1));
        assert!(!mgr.close(1)); // already closed
        assert!(mgr.get(1).is_none());
    }

    #[test]
    fn test_manager_clear() {
        let mut mgr = PreparedStatementManager::new();
        let _ = mgr.prepare("SELECT 1").unwrap();
        let _ = mgr.prepare("SELECT 2").unwrap();
        mgr.clear();
        assert!(mgr.get(1).is_none());
        assert!(mgr.get(2).is_none());
    }

    #[test]
    fn test_count_placeholders() {
        let dialect = MySqlDialect {};
        let stmts =
            SqlParser::parse_sql(&dialect, "SELECT * FROM t WHERE a = ? AND b = ?").unwrap();
        assert_eq!(count_placeholders(&stmts[0]), 2);
    }

    #[test]
    fn test_count_placeholders_none() {
        let dialect = MySqlDialect {};
        let stmts = SqlParser::parse_sql(&dialect, "SELECT 1").unwrap();
        assert_eq!(count_placeholders(&stmts[0]), 0);
    }
}
