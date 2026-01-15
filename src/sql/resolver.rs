//! Name resolution against the catalog
//!
//! The resolver takes parsed SQL statements and resolves:
//! - Table names to table definitions
//! - Column names to column definitions with type information
//! - Validates that referenced tables and columns exist

use std::collections::HashMap;

use crate::catalog::{Catalog, DataType};
use crate::sql::ast::*;
use crate::sql::error::{SqlError, SqlResult};

/// Name resolver
pub struct Resolver<'a> {
    catalog: &'a Catalog,
}

impl<'a> Resolver<'a> {
    /// Create a new resolver
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Resolve a statement
    pub fn resolve(&self, stmt: Statement) -> SqlResult<ResolvedStatement> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => Ok(ResolvedStatement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            }),
            Statement::DropTable { name, if_exists } => {
                Ok(ResolvedStatement::DropTable { name, if_exists })
            }
            Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => {
                // Resolve column indices
                let table_def = self
                    .catalog
                    .get_table(&table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

                let mut resolved_cols = Vec::new();
                for col_name in columns {
                    let idx = table_def
                        .get_column_index(&col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    resolved_cols.push((col_name, idx));
                }

                Ok(ResolvedStatement::CreateIndex {
                    name,
                    table,
                    columns: resolved_cols,
                    unique,
                })
            }
            Statement::DropIndex { name } => Ok(ResolvedStatement::DropIndex { name }),
            Statement::Insert {
                table,
                columns,
                values,
            } => self.resolve_insert(table, columns, values),
            Statement::Update {
                table,
                assignments,
                filter,
            } => self.resolve_update(table, assignments, filter),
            Statement::Delete { table, filter } => self.resolve_delete(table, filter),
            Statement::Select(select) => self.resolve_select(select),
        }
    }

    /// Resolve INSERT statement
    fn resolve_insert(
        &self,
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    ) -> SqlResult<ResolvedStatement> {
        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        // Build column list (explicit or all columns)
        let specified_columns =
            columns.unwrap_or_else(|| table_def.columns.iter().map(|c| c.name.clone()).collect());

        // Resolve specified columns and get their indices
        let mut column_indices = Vec::new();
        for col_name in &specified_columns {
            let col_def = table_def
                .get_column(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let idx = table_def.get_column_index(col_name).unwrap();

            // Check if column is NOT NULL and not provided a default
            // (we'll fill in NULL for missing columns later, so this is a check)
            column_indices.push((idx, col_def.nullable, col_name.clone()));
        }

        // Build scope for expression resolution
        let scope = Scope::single_table(&table, table_def);

        // Resolve all columns for the output (always includes all table columns)
        let mut resolved_columns = Vec::new();
        for (idx, col_def) in table_def.columns.iter().enumerate() {
            resolved_columns.push(ResolvedColumn {
                table: table.clone(),
                name: col_def.name.clone(),
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
            });
        }

        // Resolve values - expand to full rows
        let mut resolved_values = Vec::new();
        for row in values {
            if row.len() != specified_columns.len() {
                return Err(SqlError::InvalidOperation(format!(
                    "INSERT has {} columns but {} values",
                    specified_columns.len(),
                    row.len()
                )));
            }

            // Start with NULL for all columns
            let mut full_row: Vec<ResolvedExpr> = table_def
                .columns
                .iter()
                .map(|_| ResolvedExpr::Literal(Literal::Null))
                .collect();

            // Fill in the specified values at their correct positions
            for (value_idx, expr) in row.iter().enumerate() {
                let (col_idx, nullable, col_name) = &column_indices[value_idx];
                let resolved_expr = self.resolve_expr(expr, &scope)?;

                // Check NOT NULL constraint for non-NULL values
                if !nullable && matches!(resolved_expr, ResolvedExpr::Literal(Literal::Null)) {
                    return Err(SqlError::InvalidOperation(format!(
                        "Column '{}' cannot be NULL",
                        col_name
                    )));
                }

                full_row[*col_idx] = resolved_expr;
            }

            // Check NOT NULL constraints for omitted columns
            for (idx, col_def) in table_def.columns.iter().enumerate() {
                if !col_def.nullable
                    && matches!(full_row[idx], ResolvedExpr::Literal(Literal::Null))
                {
                    // Check if this column was specified
                    let was_specified = column_indices.iter().any(|(i, _, _)| *i == idx);
                    if !was_specified {
                        return Err(SqlError::InvalidOperation(format!(
                            "Column '{}' cannot be NULL and was not specified",
                            col_def.name
                        )));
                    }
                }
            }

            resolved_values.push(full_row);
        }

        Ok(ResolvedStatement::Insert {
            table,
            columns: resolved_columns,
            values: resolved_values,
        })
    }

    /// Resolve UPDATE statement
    fn resolve_update(
        &self,
        table: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    ) -> SqlResult<ResolvedStatement> {
        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        let scope = Scope::single_table(&table, table_def);

        // Get table column info
        let table_columns: Vec<_> = table_def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect();

        // Resolve assignments
        let mut resolved_assignments = Vec::new();
        for assign in assignments {
            let col_def = table_def
                .get_column(&assign.column)
                .ok_or_else(|| SqlError::ColumnNotFound(assign.column.clone()))?;
            let idx = table_def.get_column_index(&assign.column).unwrap();

            let column = ResolvedColumn {
                table: table.clone(),
                name: assign.column.clone(),
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
            };
            let value = self.resolve_expr(&assign.value, &scope)?;

            resolved_assignments.push(ResolvedAssignment { column, value });
        }

        // Resolve filter
        let resolved_filter = filter
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        Ok(ResolvedStatement::Update {
            table,
            table_columns,
            assignments: resolved_assignments,
            filter: resolved_filter,
        })
    }

    /// Resolve DELETE statement
    fn resolve_delete(&self, table: String, filter: Option<Expr>) -> SqlResult<ResolvedStatement> {
        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        let scope = Scope::single_table(&table, table_def);

        // Get table column info
        let table_columns: Vec<_> = table_def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect();

        // Resolve filter
        let resolved_filter = filter
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        Ok(ResolvedStatement::Delete {
            table,
            table_columns,
            filter: resolved_filter,
        })
    }

    /// Resolve SELECT statement
    fn resolve_select(&self, select: SelectStatement) -> SqlResult<ResolvedStatement> {
        // Build scope from FROM clause
        let mut scope = Scope::new();

        for table_ref in &select.from {
            let table_def = self
                .catalog
                .get_table(&table_ref.name)
                .ok_or_else(|| SqlError::TableNotFound(table_ref.name.clone()))?;

            let alias = table_ref
                .alias
                .clone()
                .unwrap_or_else(|| table_ref.name.clone());
            scope.add_table(&alias, &table_ref.name, table_def);
        }

        // Add joined tables to scope
        for join in &select.joins {
            let table_def = self
                .catalog
                .get_table(&join.table.name)
                .ok_or_else(|| SqlError::TableNotFound(join.table.name.clone()))?;

            let alias = join
                .table
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone());
            scope.add_table(&alias, &join.table.name, table_def);
        }

        // Resolve columns
        let mut resolved_columns = Vec::new();
        for item in &select.columns {
            resolved_columns.push(self.resolve_select_item(item, &scope)?);
        }

        // Build resolved from list
        let resolved_from: Vec<_> = select
            .from
            .iter()
            .map(|t| {
                let table_def = self.catalog.get_table(&t.name).unwrap();
                ResolvedTableRef {
                    name: t.name.clone(),
                    alias: t.alias.clone(),
                    columns: table_def
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                        .collect(),
                }
            })
            .collect();

        // Resolve filter
        let resolved_filter = select
            .filter
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        // Resolve GROUP BY
        let mut resolved_group_by = Vec::new();
        for expr in &select.group_by {
            resolved_group_by.push(self.resolve_expr(expr, &scope)?);
        }

        // Resolve HAVING
        let resolved_having = select
            .having
            .as_ref()
            .map(|h| self.resolve_expr(h, &scope))
            .transpose()?;

        // Resolve ORDER BY
        let mut resolved_order_by = Vec::new();
        for item in &select.order_by {
            resolved_order_by.push(ResolvedOrderByItem {
                expr: self.resolve_expr(&item.expr, &scope)?,
                ascending: item.ascending,
            });
        }

        Ok(ResolvedStatement::Select(ResolvedSelect {
            distinct: select.distinct,
            columns: resolved_columns,
            from: resolved_from,
            filter: resolved_filter,
            group_by: resolved_group_by,
            having: resolved_having,
            order_by: resolved_order_by,
            limit: select.limit,
            offset: select.offset,
        }))
    }

    /// Resolve SELECT item
    fn resolve_select_item(
        &self,
        item: &SelectItem,
        scope: &Scope,
    ) -> SqlResult<ResolvedSelectItem> {
        match item {
            SelectItem::Expr { expr, alias } => Ok(ResolvedSelectItem::Expr {
                expr: self.resolve_expr(expr, scope)?,
                alias: alias.clone(),
            }),
            SelectItem::Wildcard => {
                // Expand to all columns from all tables
                let mut columns = Vec::new();
                for (table_alias, table_info) in &scope.tables {
                    for (idx, (name, data_type, nullable)) in table_info.columns.iter().enumerate()
                    {
                        columns.push(ResolvedColumn {
                            table: table_alias.clone(),
                            name: name.clone(),
                            index: idx,
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        });
                    }
                }
                Ok(ResolvedSelectItem::Columns(columns))
            }
            SelectItem::QualifiedWildcard(table) => {
                let table_info = scope
                    .tables
                    .get(table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

                let columns: Vec<_> = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, data_type, nullable))| ResolvedColumn {
                        table: table.clone(),
                        name: name.clone(),
                        index: idx,
                        data_type: data_type.clone(),
                        nullable: *nullable,
                    })
                    .collect();

                Ok(ResolvedSelectItem::Columns(columns))
            }
        }
    }

    /// Resolve expression
    fn resolve_expr(&self, expr: &Expr, scope: &Scope) -> SqlResult<ResolvedExpr> {
        match expr {
            Expr::Column { table, name } => self.resolve_column(table.as_deref(), name, scope),
            Expr::Literal(lit) => Ok(ResolvedExpr::Literal(lit.clone())),
            Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_expr(left, scope)?;
                let resolved_right = self.resolve_expr(right, scope)?;
                let result_type = infer_binary_result_type(*op, &resolved_left, &resolved_right)?;
                Ok(ResolvedExpr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: *op,
                    right: Box::new(resolved_right),
                    result_type,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                let result_type = infer_unary_result_type(*op, &resolved_expr)?;
                Ok(ResolvedExpr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved_expr),
                    result_type,
                })
            }
            Expr::Function {
                name,
                args,
                distinct,
            } => {
                let mut resolved_args = Vec::new();
                for arg in args {
                    resolved_args.push(self.resolve_expr(arg, scope)?);
                }
                let result_type = infer_function_result_type(name, &resolved_args)?;
                Ok(ResolvedExpr::Function {
                    name: name.clone(),
                    args: resolved_args,
                    distinct: *distinct,
                    result_type,
                })
            }
            Expr::IsNull { expr, negated } => Ok(ResolvedExpr::IsNull {
                expr: Box::new(self.resolve_expr(expr, scope)?),
                negated: *negated,
            }),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                let mut resolved_list = Vec::new();
                for item in list {
                    resolved_list.push(self.resolve_expr(item, scope)?);
                }
                Ok(ResolvedExpr::InList {
                    expr: Box::new(resolved_expr),
                    list: resolved_list,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(ResolvedExpr::Between {
                expr: Box::new(self.resolve_expr(expr, scope)?),
                low: Box::new(self.resolve_expr(low, scope)?),
                high: Box::new(self.resolve_expr(high, scope)?),
                negated: *negated,
            }),
            Expr::Wildcard => {
                // Wildcard in expression context is only valid in COUNT(*)
                // Return a placeholder that gets handled in function resolution
                Ok(ResolvedExpr::Literal(Literal::Null))
            }
            Expr::QualifiedWildcard(_) | Expr::Subquery(_) => {
                Err(SqlError::Unsupported("Expression type".to_string()))
            }
        }
    }

    /// Resolve column reference
    fn resolve_column(
        &self,
        table: Option<&str>,
        name: &str,
        scope: &Scope,
    ) -> SqlResult<ResolvedExpr> {
        if let Some(table_name) = table {
            // Qualified column reference
            let table_info = scope
                .tables
                .get(table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?;

            let (idx, col_info) = table_info
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _, _))| n == name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.to_string()))?;

            Ok(ResolvedExpr::Column(ResolvedColumn {
                table: table_name.to_string(),
                name: name.to_string(),
                index: idx,
                data_type: col_info.1.clone(),
                nullable: col_info.2,
            }))
        } else {
            // Unqualified column reference - search all tables
            let mut found: Option<(String, usize, DataType, bool)> = None;

            for (table_alias, table_info) in &scope.tables {
                if let Some((idx, col_info)) = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _, _))| n == name)
                {
                    if found.is_some() {
                        return Err(SqlError::AmbiguousColumn(name.to_string()));
                    }
                    found = Some((table_alias.clone(), idx, col_info.1.clone(), col_info.2));
                }
            }

            match found {
                Some((table, index, data_type, nullable)) => {
                    Ok(ResolvedExpr::Column(ResolvedColumn {
                        table,
                        name: name.to_string(),
                        index,
                        data_type,
                        nullable,
                    }))
                }
                None => Err(SqlError::ColumnNotFound(name.to_string())),
            }
        }
    }
}

/// Scope for name resolution
struct Scope {
    tables: HashMap<String, TableInfo>,
}

struct TableInfo {
    #[allow(dead_code)]
    real_name: String,
    columns: Vec<(String, DataType, bool)>, // (name, type, nullable)
}

impl Scope {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn single_table(name: &str, table_def: &crate::catalog::TableDef) -> Self {
        let mut scope = Self::new();
        scope.add_table(name, name, table_def);
        scope
    }

    fn add_table(&mut self, alias: &str, real_name: &str, table_def: &crate::catalog::TableDef) {
        self.tables.insert(
            alias.to_string(),
            TableInfo {
                real_name: real_name.to_string(),
                columns: table_def
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                    .collect(),
            },
        );
    }
}

/// Infer result type of binary operation
fn infer_binary_result_type(
    op: BinaryOp,
    left: &ResolvedExpr,
    right: &ResolvedExpr,
) -> SqlResult<DataType> {
    match op {
        // Comparison operators always return boolean
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::Like
        | BinaryOp::NotLike => Ok(DataType::Boolean),

        // Logical operators return boolean
        BinaryOp::And | BinaryOp::Or => Ok(DataType::Boolean),

        // Arithmetic operators - use wider type
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            let left_type = left.data_type();
            let right_type = right.data_type();
            Ok(wider_numeric_type(&left_type, &right_type))
        }
    }
}

/// Infer result type of unary operation
fn infer_unary_result_type(op: UnaryOp, expr: &ResolvedExpr) -> SqlResult<DataType> {
    match op {
        UnaryOp::Not => Ok(DataType::Boolean),
        UnaryOp::Neg => Ok(expr.data_type()),
    }
}

/// Infer result type of function
fn infer_function_result_type(name: &str, args: &[ResolvedExpr]) -> SqlResult<DataType> {
    match name.to_uppercase().as_str() {
        "COUNT" => Ok(DataType::BigInt),
        "SUM" => {
            if args.is_empty() {
                Ok(DataType::BigInt)
            } else {
                Ok(args[0].data_type())
            }
        }
        "AVG" => Ok(DataType::Double),
        "MIN" | "MAX" => {
            if args.is_empty() {
                Ok(DataType::Int)
            } else {
                Ok(args[0].data_type())
            }
        }
        "COALESCE" | "IFNULL" | "NULLIF" => {
            if args.is_empty() {
                Ok(DataType::Int)
            } else {
                Ok(args[0].data_type())
            }
        }
        "CONCAT" | "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "SUBSTRING" | "SUBSTR" => {
            Ok(DataType::Text)
        }
        "LENGTH" | "CHAR_LENGTH" => Ok(DataType::BigInt),
        "ABS" | "CEIL" | "FLOOR" | "ROUND" => {
            if args.is_empty() {
                Ok(DataType::Double)
            } else {
                Ok(args[0].data_type())
            }
        }
        "NOW" | "CURRENT_TIMESTAMP" => Ok(DataType::Timestamp),
        _ => Err(SqlError::InvalidOperation(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

/// Get the wider of two numeric types
fn wider_numeric_type(a: &DataType, b: &DataType) -> DataType {
    match (a, b) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
        (DataType::BigInt, _) | (_, DataType::BigInt) => DataType::BigInt,
        (DataType::Int, _) | (_, DataType::Int) => DataType::Int,
        (DataType::SmallInt, _) | (_, DataType::SmallInt) => DataType::SmallInt,
        _ => DataType::Int,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableDef};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let users = TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("name", DataType::Varchar(100)))
            .column(ColumnDef::new("email", DataType::Varchar(255)));

        let orders = TableDef::new("orders")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("user_id", DataType::Int))
            .column(ColumnDef::new("total", DataType::Double));

        catalog.create_table(users).unwrap();
        catalog.create_table(orders).unwrap();

        catalog
    }

    #[test]
    fn test_resolve_select() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = crate::sql::parser::Parser::parse_one("SELECT id, name FROM users WHERE id = 1")
            .unwrap();

        let resolved = resolver.resolve(stmt).unwrap();
        match resolved {
            ResolvedStatement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert!(select.filter.is_some());
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_resolve_table_not_found() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = crate::sql::parser::Parser::parse_one("SELECT * FROM nonexistent").unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::TableNotFound(_))));
    }

    #[test]
    fn test_resolve_column_not_found() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = crate::sql::parser::Parser::parse_one("SELECT nonexistent FROM users").unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
    }

    #[test]
    fn test_resolve_ambiguous_column() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        // Both users and orders have 'id' column
        let stmt = crate::sql::parser::Parser::parse_one(
            "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::AmbiguousColumn(_))));
    }

    #[test]
    fn test_resolve_qualified_column() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = crate::sql::parser::Parser::parse_one(
            "SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        let result = resolver.resolve(stmt);

        assert!(result.is_ok());
    }
}
