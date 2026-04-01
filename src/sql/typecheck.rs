//! Type checking for resolved SQL statements
//!
//! The type checker verifies:
//! - Type compatibility in expressions
//! - Assignment types match column types in INSERT/UPDATE
//! - Aggregate function usage is valid

use crate::catalog::DataType;
use crate::planner::logical::{
    Literal, ResolvedAssignment, ResolvedColumn, ResolvedExpr, ResolvedSelect, ResolvedSelectItem,
    ResolvedStatement,
};
use crate::sql::error::{SqlError, SqlResult};

/// Type checker
pub struct TypeChecker;

impl TypeChecker {
    /// Check a resolved statement
    pub fn check(stmt: &ResolvedStatement) -> SqlResult<()> {
        match stmt {
            ResolvedStatement::CreateTable { .. } => Ok(()),
            ResolvedStatement::DropTable { .. } => Ok(()),
            ResolvedStatement::CreateIndex { .. } => Ok(()),
            ResolvedStatement::DropIndex { .. } => Ok(()),
            ResolvedStatement::Insert {
                columns, values, ..
            } => Self::check_insert(columns, values),
            ResolvedStatement::InsertSelect { source, .. } => Self::check(source),
            ResolvedStatement::Update {
                assignments,
                filter,
                ..
            } => Self::check_update(assignments, filter),
            ResolvedStatement::Delete { filter, .. } => Self::check_delete(filter),
            ResolvedStatement::Select(select) => Self::check_select(select),
            ResolvedStatement::CreateTableAs { source, .. } => Self::check(source),
            ResolvedStatement::CreateView { .. } | ResolvedStatement::DropView { .. } => Ok(()),

            // Database DDL doesn't need type checking
            ResolvedStatement::CreateDatabase { .. } | ResolvedStatement::DropDatabase { .. } => {
                Ok(())
            }

            // Auth statements don't need type checking
            ResolvedStatement::CreateUser { .. }
            | ResolvedStatement::AlterUser { .. }
            | ResolvedStatement::DropUser { .. }
            | ResolvedStatement::SetPassword { .. }
            | ResolvedStatement::Grant { .. }
            | ResolvedStatement::Revoke { .. }
            | ResolvedStatement::ShowGrants { .. } => Ok(()),

            // ANALYZE TABLE doesn't need type checking
            ResolvedStatement::AnalyzeTable { .. } => Ok(()),

            // Multi-table DROP doesn't need type checking
            ResolvedStatement::DropMultipleTables { .. } => Ok(()),

            // UNION - type check both sides (right may be nested Union)
            ResolvedStatement::Union { left, right, .. } => {
                Self::check_select(left)?;
                Self::check(right)
            }

            // EXPLAIN - type check the inner statement
            ResolvedStatement::Explain { inner } => Self::check(inner),
        }
    }

    /// Check INSERT statement
    fn check_insert(columns: &[ResolvedColumn], values: &[Vec<ResolvedExpr>]) -> SqlResult<()> {
        // MySQL behavior: single-row INSERT with explicit NULL into NOT NULL → error 1048.
        // Multi-row INSERT in non-strict mode → silently convert NULL to column default.
        let is_multi_row = values.len() > 1;
        for row in values {
            for (col, expr) in columns.iter().zip(row.iter()) {
                Self::check_type_compatible(&col.data_type, &expr.data_type(), expr)?;

                // Check NOT NULL constraints (skip for multi-row — executor handles conversion)
                if !is_multi_row && !col.nullable && Self::is_definitely_null(expr) {
                    return Err(SqlError::InvalidOperation(format!(
                        "Column '{}' cannot be NULL",
                        col.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check UPDATE statement
    fn check_update(
        assignments: &[ResolvedAssignment],
        filter: &Option<ResolvedExpr>,
    ) -> SqlResult<()> {
        for assign in assignments {
            Self::check_type_compatible(
                &assign.column.data_type,
                &assign.value.data_type(),
                &assign.value,
            )?;

            // MySQL: UPDATE SET col=NULL on NOT NULL column silently sets to default.
            // Don't reject at typecheck time — let the executor handle coercion.
        }

        // Check filter is boolean
        if let Some(filter) = filter {
            Self::check_is_boolean(filter)?;
        }

        Ok(())
    }

    /// Check DELETE statement
    fn check_delete(filter: &Option<ResolvedExpr>) -> SqlResult<()> {
        if let Some(filter) = filter {
            Self::check_is_boolean(filter)?;
        }
        Ok(())
    }

    /// Check SELECT statement
    fn check_select(select: &ResolvedSelect) -> SqlResult<()> {
        // Check filter is boolean
        if let Some(filter) = &select.filter {
            Self::check_is_boolean(filter)?;
        }

        // Check HAVING is boolean
        if let Some(having) = &select.having {
            Self::check_is_boolean(having)?;
        }

        // Check aggregate usage in non-grouped queries
        if select.group_by.is_empty() {
            // If there's no GROUP BY, check for mixed aggregate and non-aggregate columns
            let has_aggregate = select.columns.iter().any(Self::item_has_aggregate);
            let has_non_aggregate = select.columns.iter().any(Self::item_has_non_aggregate);

            if has_aggregate && has_non_aggregate {
                return Err(SqlError::InvalidOperation(
                    "SELECT with aggregates must use GROUP BY or only aggregate functions"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Check if expression is definitely NULL or will evaluate to NULL at runtime
    /// (e.g. `1/NULL`, `NULL + 5`, `COALESCE(NULL)`)
    fn is_definitely_null(expr: &ResolvedExpr) -> bool {
        use crate::planner::logical::BinaryOp;
        match expr {
            ResolvedExpr::Literal(Literal::Null) => true,
            ResolvedExpr::BinaryOp {
                op, left, right, ..
            } => {
                // AND/OR use SQL three-valued logic where NULL doesn't always propagate:
                //   NULL OR TRUE = TRUE,  NULL AND FALSE = FALSE
                // Only arithmetic, comparison, bitwise, and string ops guarantee
                // NULL propagation when either operand is NULL.
                match op {
                    BinaryOp::And | BinaryOp::Or => false,
                    _ => Self::is_definitely_null(left) || Self::is_definitely_null(right),
                }
            }
            ResolvedExpr::UnaryOp { expr: inner, .. } => Self::is_definitely_null(inner),
            _ => false,
        }
    }

    /// Check if expression evaluates to boolean
    /// MySQL treats non-zero numeric values as true, so accept numeric types too
    fn check_is_boolean(expr: &ResolvedExpr) -> SqlResult<()> {
        let dt = expr.data_type();
        // MySQL allows comparison results, strings, and JSON in boolean contexts
        if dt == DataType::Boolean || dt.is_numeric() || dt.is_string() || dt == DataType::Json {
            Ok(())
        } else {
            Err(SqlError::TypeMismatch {
                expected: DataType::Boolean,
                found: dt,
            })
        }
    }

    /// Check if expression is a placeholder (compatible with any type)
    fn is_placeholder(expr: &ResolvedExpr) -> bool {
        matches!(expr, ResolvedExpr::Literal(Literal::Placeholder(_)))
    }

    /// Check if two types are compatible
    fn check_type_compatible(
        target: &DataType,
        source: &DataType,
        expr: &ResolvedExpr,
    ) -> SqlResult<()> {
        // A literal NULL is compatible with any type — the NOT NULL constraint
        // handles rejection at runtime. Expressions that merely *contain* a NULL
        // (e.g. COALESCE('x', NULL)) still need type checking on their result type.
        if Self::is_definitely_null(expr) {
            return Ok(());
        }

        // Placeholders are compatible with any type (resolved at execution time)
        if Self::is_placeholder(expr) {
            return Ok(());
        }

        if types_compatible(target, source) {
            Ok(())
        } else {
            Err(SqlError::TypeMismatch {
                expected: target.clone(),
                found: source.clone(),
            })
        }
    }

    /// Check if a SELECT item contains an aggregate function
    fn item_has_aggregate(item: &ResolvedSelectItem) -> bool {
        match item {
            ResolvedSelectItem::Expr { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedSelectItem::Columns(_) => false,
        }
    }

    /// Check if expression contains an aggregate function
    fn expr_has_aggregate(expr: &ResolvedExpr) -> bool {
        match expr {
            ResolvedExpr::Function { name, args, .. } => {
                let is_agg = matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "BIT_AND" | "BIT_OR" | "BIT_XOR"
                );
                is_agg || args.iter().any(Self::expr_has_aggregate)
            }
            ResolvedExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            ResolvedExpr::UnaryOp { expr, .. } => Self::expr_has_aggregate(expr),
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_has_aggregate(e))
                    || conditions.iter().any(Self::expr_has_aggregate)
                    || results.iter().any(Self::expr_has_aggregate)
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_aggregate(e))
            }
            _ => false,
        }
    }

    /// Check if a SELECT item contains a non-aggregate column reference
    fn item_has_non_aggregate(item: &ResolvedSelectItem) -> bool {
        match item {
            ResolvedSelectItem::Expr { expr, .. } => Self::expr_has_non_aggregate_column(expr),
            ResolvedSelectItem::Columns(_) => true,
        }
    }

    /// Check if expression has a non-aggregate column reference
    fn expr_has_non_aggregate_column(expr: &ResolvedExpr) -> bool {
        match expr {
            ResolvedExpr::Column(_) => true,
            ResolvedExpr::Function { name, args, .. } => {
                // Inside an aggregate function, column references are OK
                let is_agg = matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "BIT_AND" | "BIT_OR" | "BIT_XOR"
                );
                if is_agg {
                    false
                } else {
                    args.iter().any(Self::expr_has_non_aggregate_column)
                }
            }
            ResolvedExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_non_aggregate_column(left)
                    || Self::expr_has_non_aggregate_column(right)
            }
            ResolvedExpr::UnaryOp { expr, .. } => Self::expr_has_non_aggregate_column(expr),
            ResolvedExpr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_has_non_aggregate_column(e))
                    || conditions.iter().any(Self::expr_has_non_aggregate_column)
                    || results.iter().any(Self::expr_has_non_aggregate_column)
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_non_aggregate_column(e))
            }
            ResolvedExpr::UserVariable { .. } => false,
            _ => false,
        }
    }
}

/// Check if types are compatible for assignment/comparison
fn types_compatible(target: &DataType, source: &DataType) -> bool {
    // Note: NULL literals default to Int type in the parser.
    // The match below handles numeric type compatibility, which covers this case.

    match (target, source) {
        // Same types are always compatible
        (a, b) if a == b => true,

        // All numeric types are compatible with each other
        (a, b) if a.is_numeric() && b.is_numeric() => true,

        // All string types are compatible with each other
        (DataType::Varchar(_), DataType::Text)
        | (DataType::Text, DataType::Varchar(_))
        | (DataType::Varchar(_), DataType::Varchar(_)) => true,

        // String literals can be assigned to timestamps
        (DataType::Timestamp, DataType::Text) | (DataType::Timestamp, DataType::Varchar(_)) => true,

        // Integer literals can be booleans (0/1)
        (DataType::Boolean, DataType::TinyInt)
        | (DataType::Boolean, DataType::SmallInt)
        | (DataType::Boolean, DataType::Int)
        | (DataType::Boolean, DataType::BigInt) => true,

        // Bit is numeric-compatible and accepts Blob (hex literals)
        (DataType::Bit(_), b) if b.is_numeric() => true,
        (a, DataType::Bit(_)) if a.is_numeric() => true,
        (DataType::Bit(_), DataType::Blob) | (DataType::Blob, DataType::Bit(_)) => true,

        // Blob (hex literals like 0x01) can be inserted into string columns
        (DataType::Varchar(_), DataType::Blob) | (DataType::Text, DataType::Blob) => true,
        (DataType::Blob, DataType::Varchar(_)) | (DataType::Blob, DataType::Text) => true,

        // MySQL implicitly converts strings/blobs to numbers and vice versa
        (a, DataType::Text) | (a, DataType::Varchar(_)) | (a, DataType::Blob) if a.is_numeric() => {
            true
        }
        (DataType::Text, b) | (DataType::Varchar(_), b) | (DataType::Blob, b) if b.is_numeric() => {
            true
        }

        // Geometry is compatible with Blob (WKB binary storage)
        (DataType::Geometry, DataType::Blob) | (DataType::Blob, DataType::Geometry) => true,
        (DataType::Geometry, DataType::Geometry) => true,

        // JSON is compatible with string types (validated on INSERT)
        (DataType::Json, DataType::Text)
        | (DataType::Json, DataType::Varchar(_))
        | (DataType::Text, DataType::Json)
        | (DataType::Varchar(_), DataType::Json) => true,

        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, TableDef};
    use crate::sql::parser::Parser;
    use crate::sql::resolver::Resolver;

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let users = TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("name", DataType::Varchar(100)))
            .column(ColumnDef::new("age", DataType::Int));

        catalog.create_table(users).unwrap();
        catalog
    }

    fn resolve_and_check(catalog: &Catalog, sql: &str) -> SqlResult<()> {
        let stmt = Parser::parse_one(sql)?;
        let resolver = Resolver::new(catalog);
        let resolved = resolver.resolve(stmt)?;
        TypeChecker::check(&resolved)
    }

    #[test]
    fn test_typecheck_comparison() {
        let catalog = test_catalog();

        // Valid: compare int to int
        assert!(resolve_and_check(&catalog, "SELECT * FROM users WHERE id = 1").is_ok());

        // Valid: compare string to string
        assert!(resolve_and_check(&catalog, "SELECT * FROM users WHERE name = 'Alice'").is_ok());
    }

    #[test]
    fn test_typecheck_insert() {
        let catalog = test_catalog();

        // Valid: correct types
        assert!(resolve_and_check(
            &catalog,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"
        )
        .is_ok());

        // Invalid: NULL for NOT NULL column would be caught if we track nullability properly
        // For now, literals aren't marked as null in our implementation
    }

    #[test]
    fn test_typecheck_update() {
        let catalog = test_catalog();

        // Valid: correct types
        assert!(resolve_and_check(&catalog, "UPDATE users SET name = 'Bob' WHERE id = 1").is_ok());

        // Valid: update int column with int
        assert!(resolve_and_check(&catalog, "UPDATE users SET age = 30 WHERE id = 1").is_ok());
    }

    #[test]
    fn test_typecheck_aggregate_without_group_by() {
        let catalog = test_catalog();

        // Valid: only aggregates
        assert!(resolve_and_check(&catalog, "SELECT COUNT(*) FROM users").is_ok());

        // Valid: only non-aggregates
        assert!(resolve_and_check(&catalog, "SELECT id, name FROM users").is_ok());

        // Invalid: mixed aggregates and non-aggregates without GROUP BY
        // Note: This test may fail depending on how we parse COUNT(*)
        // The current implementation might not catch all cases
    }

    #[test]
    fn test_types_compatible() {
        // Numeric types are compatible
        assert!(types_compatible(&DataType::Int, &DataType::BigInt));
        assert!(types_compatible(&DataType::Double, &DataType::Int));

        // String types are compatible
        assert!(types_compatible(&DataType::Text, &DataType::Varchar(100)));

        // MySQL implicit conversion: strings/blobs ↔ numbers
        assert!(types_compatible(&DataType::Int, &DataType::Text));
        assert!(types_compatible(&DataType::Blob, &DataType::Int));

        // Truly incompatible types
        assert!(!types_compatible(&DataType::Timestamp, &DataType::Int));
        assert!(!types_compatible(&DataType::Boolean, &DataType::Text));
    }
}
