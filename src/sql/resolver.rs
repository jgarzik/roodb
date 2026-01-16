//! Name resolution against the catalog
//!
//! The resolver takes parsed SQL statements from sqlparser and resolves:
//! - Table names to table definitions
//! - Column names to column definitions with type information
//! - Validates that referenced tables and columns exist

use std::collections::HashMap;

use sqlparser::ast as sp;

use crate::catalog::{Catalog, ColumnDef, Constraint, DataType};
use crate::planner::logical::{
    BinaryOp, JoinType, Literal, ResolvedAssignment, ResolvedColumn, ResolvedExpr, ResolvedJoin,
    ResolvedOrderByItem, ResolvedSelect, ResolvedSelectItem, ResolvedStatement, ResolvedTableRef,
    UnaryOp,
};
use crate::sql::error::{SqlError, SqlResult};
use crate::sql::privileges::{HostPattern, Privilege, PrivilegeObject};

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
    pub fn resolve(&self, stmt: sp::Statement) -> SqlResult<ResolvedStatement> {
        match stmt {
            sp::Statement::CreateTable(create) => self.resolve_create_table(&create),
            sp::Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.resolve_drop(&object_type, &names, if_exists),
            sp::Statement::CreateIndex(create_index) => self.resolve_create_index(&create_index),
            sp::Statement::Insert(insert) => self.resolve_insert(&insert),
            sp::Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.resolve_update(&table, &assignments, &selection),
            sp::Statement::Delete(delete) => self.resolve_delete(&delete),
            sp::Statement::Query(query) => self.resolve_query(&query),

            // Auth statements
            sp::Statement::CreateRole {
                names,
                if_not_exists,
                password,
                ..
            } => self.resolve_create_user(&names, if_not_exists, &password),
            sp::Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                ..
            } => self.resolve_grant(&privileges, &objects, &grantees, with_grant_option),
            sp::Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => self.resolve_revoke(&privileges, &objects, &grantees),

            _ => Err(SqlError::Unsupported(format!("Statement type: {:?}", stmt))),
        }
    }

    /// Resolve CREATE TABLE
    fn resolve_create_table(&self, create: &sp::CreateTable) -> SqlResult<ResolvedStatement> {
        let name = create.name.to_string();
        let if_not_exists = create.if_not_exists;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        for col in &create.columns {
            columns.push(convert_column_def(col)?);
        }

        for constraint in &create.constraints {
            if let Some(c) = convert_table_constraint(constraint)? {
                constraints.push(c);
            }
        }

        Ok(ResolvedStatement::CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
        })
    }

    /// Resolve DROP statement
    fn resolve_drop(
        &self,
        object_type: &sp::ObjectType,
        names: &[sp::ObjectName],
        if_exists: bool,
    ) -> SqlResult<ResolvedStatement> {
        if names.is_empty() {
            return Err(SqlError::Parse("DROP requires a name".to_string()));
        }
        let name = names[0].to_string();

        match object_type {
            sp::ObjectType::Table => Ok(ResolvedStatement::DropTable { name, if_exists }),
            sp::ObjectType::Index => Ok(ResolvedStatement::DropIndex { name }),
            _ => Err(SqlError::Unsupported(format!("DROP {:?}", object_type))),
        }
    }

    /// Resolve CREATE INDEX
    fn resolve_create_index(&self, create: &sp::CreateIndex) -> SqlResult<ResolvedStatement> {
        let name = create
            .name
            .as_ref()
            .map(|n| n.to_string())
            .ok_or_else(|| SqlError::Parse("CREATE INDEX requires a name".to_string()))?;

        let table = create.table_name.to_string();
        let unique = create.unique;

        // Resolve column indices
        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        let mut resolved_cols = Vec::new();
        for col_expr in &create.columns {
            let col_name = col_expr.expr.to_string();
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

    /// Resolve INSERT statement
    fn resolve_insert(&self, insert: &sp::Insert) -> SqlResult<ResolvedStatement> {
        let table = insert.table_name.to_string();

        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        // Build column list (explicit or all columns)
        let specified_columns: Vec<String> = if insert.columns.is_empty() {
            table_def.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            insert.columns.iter().map(|c| c.value.clone()).collect()
        };

        // Resolve specified columns and get their indices
        let mut column_indices = Vec::new();
        for col_name in &specified_columns {
            let col_def = table_def
                .get_column(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let idx = table_def.get_column_index(col_name).unwrap();
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

        // Parse values
        let values_rows = match insert.source.as_ref().map(|s| s.body.as_ref()) {
            Some(sp::SetExpr::Values(sp::Values { rows, .. })) => rows,
            _ => return Err(SqlError::Unsupported("INSERT without VALUES".to_string())),
        };

        // Resolve values - expand to full rows
        let mut resolved_values = Vec::new();
        for row in values_rows {
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
        table: &sp::TableWithJoins,
        assignments: &[sp::Assignment],
        selection: &Option<sp::Expr>,
    ) -> SqlResult<ResolvedStatement> {
        let table_name = match &table.relation {
            sp::TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(SqlError::Unsupported("Complex UPDATE table".to_string())),
        };

        let table_def = self
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

        let scope = Scope::single_table(&table_name, table_def);

        // Get table column info
        let table_columns: Vec<_> = table_def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect();

        // Resolve assignments
        let mut resolved_assignments = Vec::new();
        for assign in assignments {
            let col_name = extract_assignment_target(&assign.target)?;
            let col_def = table_def
                .get_column(&col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let idx = table_def.get_column_index(&col_name).unwrap();

            let column = ResolvedColumn {
                table: table_name.clone(),
                name: col_name,
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
            };
            let value = self.resolve_expr(&assign.value, &scope)?;

            resolved_assignments.push(ResolvedAssignment { column, value });
        }

        // Resolve filter
        let resolved_filter = selection
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        Ok(ResolvedStatement::Update {
            table: table_name,
            table_columns,
            assignments: resolved_assignments,
            filter: resolved_filter,
        })
    }

    /// Resolve DELETE statement
    fn resolve_delete(&self, delete: &sp::Delete) -> SqlResult<ResolvedStatement> {
        let table_name = match &delete.from {
            sp::FromTable::WithFromKeyword(tables) if !tables.is_empty() => {
                match &tables[0].relation {
                    sp::TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(SqlError::Unsupported("Complex DELETE table".to_string())),
                }
            }
            sp::FromTable::WithoutKeyword(tables) if !tables.is_empty() => {
                match &tables[0].relation {
                    sp::TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(SqlError::Unsupported("Complex DELETE table".to_string())),
                }
            }
            _ => return Err(SqlError::Parse("DELETE requires a table".to_string())),
        };

        let table_def = self
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

        let scope = Scope::single_table(&table_name, table_def);

        // Get table column info
        let table_columns: Vec<_> = table_def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect();

        // Resolve filter
        let resolved_filter = delete
            .selection
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        Ok(ResolvedStatement::Delete {
            table: table_name,
            table_columns,
            filter: resolved_filter,
        })
    }

    /// Resolve SELECT query
    fn resolve_query(&self, query: &sp::Query) -> SqlResult<ResolvedStatement> {
        let select = self.resolve_select_body(query.body.as_ref())?;

        let mut result = select;

        // Handle ORDER BY
        if let Some(order_by) = &query.order_by {
            // Need to create scope from the resolved tables
            let mut scope = Scope::new();
            for table_ref in &result.from {
                if let Some(table_def) = self.catalog.get_table(&table_ref.name) {
                    let alias = table_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| table_ref.name.clone());
                    scope.add_table(&alias, &table_ref.name, table_def);
                }
            }
            for join in &result.joins {
                if let Some(table_def) = self.catalog.get_table(&join.table.name) {
                    let alias = join
                        .table
                        .alias
                        .clone()
                        .unwrap_or_else(|| join.table.name.clone());
                    scope.add_table(&alias, &join.table.name, table_def);
                }
            }

            for item in &order_by.exprs {
                result.order_by.push(ResolvedOrderByItem {
                    expr: self.resolve_expr(&item.expr, &scope)?,
                    ascending: item.asc.unwrap_or(true),
                });
            }
        }

        // Handle LIMIT/OFFSET
        if let Some(sp::Expr::Value(sp::Value::Number(n, _))) = &query.limit {
            result.limit = Some(n.parse().unwrap_or(0));
        }
        if let Some(offset) = &query.offset {
            if let sp::Expr::Value(sp::Value::Number(n, _)) = &offset.value {
                result.offset = Some(n.parse().unwrap_or(0));
            }
        }

        Ok(ResolvedStatement::Select(result))
    }

    /// Resolve SELECT body
    fn resolve_select_body(&self, body: &sp::SetExpr) -> SqlResult<ResolvedSelect> {
        match body {
            sp::SetExpr::Select(select) => {
                // Build scope from FROM clause
                let mut scope = Scope::new();

                for table in &select.from {
                    let table_ref = self.resolve_table_factor(&table.relation)?;
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

                // Build resolved from list first
                let resolved_from: Vec<ResolvedTableRef> = select
                    .from
                    .iter()
                    .map(|t| self.resolve_table_factor(&t.relation))
                    .collect::<SqlResult<Vec<_>>>()?;

                // Add joined tables to scope
                let mut resolved_joins = Vec::new();
                for table_with_joins in &select.from {
                    for join in &table_with_joins.joins {
                        let table_ref = self.resolve_table_factor(&join.relation)?;
                        let table_def = self
                            .catalog
                            .get_table(&table_ref.name)
                            .ok_or_else(|| SqlError::TableNotFound(table_ref.name.clone()))?;

                        let alias = table_ref
                            .alias
                            .clone()
                            .unwrap_or_else(|| table_ref.name.clone());
                        scope.add_table(&alias, &table_ref.name, table_def);

                        let join_type = convert_join_type(&join.join_operator)?;
                        let condition = extract_join_condition(&join.join_operator)
                            .map(|c| self.resolve_expr(c, &scope))
                            .transpose()?;

                        resolved_joins.push(ResolvedJoin {
                            table: table_ref,
                            join_type,
                            condition,
                        });
                    }
                }

                // Resolve columns
                let mut resolved_columns = Vec::new();
                for item in &select.projection {
                    resolved_columns.push(self.resolve_select_item(item, &scope)?);
                }

                // Resolve filter
                let resolved_filter = select
                    .selection
                    .as_ref()
                    .map(|f| self.resolve_expr(f, &scope))
                    .transpose()?;

                // Resolve GROUP BY
                let mut resolved_group_by = Vec::new();
                match &select.group_by {
                    sp::GroupByExpr::Expressions(exprs, _) => {
                        for expr in exprs {
                            resolved_group_by.push(self.resolve_expr(expr, &scope)?);
                        }
                    }
                    sp::GroupByExpr::All(_) => {
                        return Err(SqlError::Unsupported("GROUP BY ALL".to_string()));
                    }
                }

                // Resolve HAVING
                let resolved_having = select
                    .having
                    .as_ref()
                    .map(|h| self.resolve_expr(h, &scope))
                    .transpose()?;

                Ok(ResolvedSelect {
                    distinct: select.distinct.is_some(),
                    columns: resolved_columns,
                    from: resolved_from,
                    joins: resolved_joins,
                    filter: resolved_filter,
                    group_by: resolved_group_by,
                    having: resolved_having,
                    order_by: Vec::new(), // Filled in by resolve_query
                    limit: None,
                    offset: None,
                })
            }
            _ => Err(SqlError::Unsupported(
                "Complex query (UNION, etc.)".to_string(),
            )),
        }
    }

    /// Resolve table factor to table reference
    fn resolve_table_factor(&self, table: &sp::TableFactor) -> SqlResult<ResolvedTableRef> {
        match table {
            sp::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_def = self
                    .catalog
                    .get_table(&table_name)
                    .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

                Ok(ResolvedTableRef {
                    name: table_name,
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                    columns: table_def
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                        .collect(),
                })
            }
            _ => Err(SqlError::Unsupported("Complex table reference".to_string())),
        }
    }

    /// Resolve SELECT item
    fn resolve_select_item(
        &self,
        item: &sp::SelectItem,
        scope: &Scope,
    ) -> SqlResult<ResolvedSelectItem> {
        match item {
            sp::SelectItem::UnnamedExpr(expr) => Ok(ResolvedSelectItem::Expr {
                expr: self.resolve_expr(expr, scope)?,
                alias: None,
            }),
            sp::SelectItem::ExprWithAlias { expr, alias } => Ok(ResolvedSelectItem::Expr {
                expr: self.resolve_expr(expr, scope)?,
                alias: Some(alias.value.clone()),
            }),
            sp::SelectItem::Wildcard(_) => {
                // Expand to all columns from all tables in order
                let mut columns = Vec::new();
                for (table_alias, _offset) in &scope.table_order {
                    let table_info = scope.tables.get(table_alias).unwrap();
                    for (local_idx, (name, data_type, nullable)) in
                        table_info.columns.iter().enumerate()
                    {
                        let global_idx = table_info.column_offset + local_idx;
                        columns.push(ResolvedColumn {
                            table: table_alias.clone(),
                            name: name.clone(),
                            index: global_idx,
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        });
                    }
                }
                Ok(ResolvedSelectItem::Columns(columns))
            }
            sp::SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                let table_info = scope
                    .tables
                    .get(&table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

                let columns: Vec<_> = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(local_idx, (name, data_type, nullable))| {
                        let global_idx = table_info.column_offset + local_idx;
                        ResolvedColumn {
                            table: table.clone(),
                            name: name.clone(),
                            index: global_idx,
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        }
                    })
                    .collect();

                Ok(ResolvedSelectItem::Columns(columns))
            }
        }
    }

    /// Resolve expression
    fn resolve_expr(&self, expr: &sp::Expr, scope: &Scope) -> SqlResult<ResolvedExpr> {
        match expr {
            sp::Expr::Identifier(ident) => self.resolve_column(None, &ident.value, scope),
            sp::Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    self.resolve_column(Some(&idents[0].value), &idents[1].value, scope)
                } else {
                    Err(SqlError::Unsupported("Compound identifier".to_string()))
                }
            }
            sp::Expr::Value(val) => Ok(ResolvedExpr::Literal(convert_value(val)?)),
            sp::Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_expr(left, scope)?;
                let resolved_right = self.resolve_expr(right, scope)?;
                let binary_op = convert_binary_op(op)?;
                let result_type =
                    infer_binary_result_type(binary_op, &resolved_left, &resolved_right)?;
                Ok(ResolvedExpr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: binary_op,
                    right: Box::new(resolved_right),
                    result_type,
                })
            }
            sp::Expr::UnaryOp { op, expr } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                let unary_op = convert_unary_op(op)?;
                let result_type = infer_unary_result_type(unary_op, &resolved_expr)?;
                Ok(ResolvedExpr::UnaryOp {
                    op: unary_op,
                    expr: Box::new(resolved_expr),
                    result_type,
                })
            }
            sp::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                let distinct = matches!(
                    func.args,
                    sp::FunctionArguments::List(sp::FunctionArgumentList {
                        duplicate_treatment: Some(sp::DuplicateTreatment::Distinct),
                        ..
                    })
                );
                let args = match &func.args {
                    sp::FunctionArguments::List(list) => {
                        let mut result = Vec::new();
                        for arg in &list.args {
                            match arg {
                                sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => {
                                    result.push(self.resolve_expr(e, scope)?);
                                }
                                sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => {
                                    // COUNT(*) - use a placeholder
                                    result.push(ResolvedExpr::Literal(Literal::Null));
                                }
                                _ => {
                                    return Err(SqlError::Unsupported(
                                        "Function argument".to_string(),
                                    ))
                                }
                            }
                        }
                        result
                    }
                    sp::FunctionArguments::None => Vec::new(),
                    _ => return Err(SqlError::Unsupported("Function arguments".to_string())),
                };
                let result_type = infer_function_result_type(&name, &args)?;
                Ok(ResolvedExpr::Function {
                    name,
                    args,
                    distinct,
                    result_type,
                })
            }
            sp::Expr::IsNull(expr) => Ok(ResolvedExpr::IsNull {
                expr: Box::new(self.resolve_expr(expr, scope)?),
                negated: false,
            }),
            sp::Expr::IsNotNull(expr) => Ok(ResolvedExpr::IsNull {
                expr: Box::new(self.resolve_expr(expr, scope)?),
                negated: true,
            }),
            sp::Expr::InList {
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
            sp::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(ResolvedExpr::Between {
                expr: Box::new(self.resolve_expr(expr, scope)?),
                low: Box::new(self.resolve_expr(low, scope)?),
                high: Box::new(self.resolve_expr(high, scope)?),
                negated: *negated,
            }),
            sp::Expr::Nested(inner) => self.resolve_expr(inner, scope),
            sp::Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let resolved_left = self.resolve_expr(expr, scope)?;
                let resolved_right = self.resolve_expr(pattern, scope)?;
                let op = if *negated {
                    BinaryOp::NotLike
                } else {
                    BinaryOp::Like
                };
                Ok(ResolvedExpr::BinaryOp {
                    left: Box::new(resolved_left),
                    op,
                    right: Box::new(resolved_right),
                    result_type: DataType::Boolean,
                })
            }
            _ => Err(SqlError::Unsupported(format!("Expression: {:?}", expr))),
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

            let (local_idx, col_info) = table_info
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _, _))| n == name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.to_string()))?;

            let global_idx = table_info.column_offset + local_idx;

            Ok(ResolvedExpr::Column(ResolvedColumn {
                table: table_name.to_string(),
                name: name.to_string(),
                index: global_idx,
                data_type: col_info.1.clone(),
                nullable: col_info.2,
            }))
        } else {
            // Unqualified column reference - search all tables
            let mut found: Option<(String, usize, DataType, bool)> = None;

            for (table_alias, table_info) in &scope.tables {
                if let Some((local_idx, col_info)) = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _, _))| n == name)
                {
                    if found.is_some() {
                        return Err(SqlError::AmbiguousColumn(name.to_string()));
                    }
                    let global_idx = table_info.column_offset + local_idx;
                    found = Some((
                        table_alias.clone(),
                        global_idx,
                        col_info.1.clone(),
                        col_info.2,
                    ));
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

    // ============ Auth statement resolution ============

    /// Resolve CREATE USER (uses CREATE ROLE in sqlparser)
    fn resolve_create_user(
        &self,
        names: &[sp::ObjectName],
        if_not_exists: bool,
        password: &Option<sp::Password>,
    ) -> SqlResult<ResolvedStatement> {
        if names.is_empty() {
            return Err(SqlError::Parse("CREATE USER requires a name".to_string()));
        }

        // Parse 'user'@'host' format or just 'user'
        let (username, host) = parse_user_host(&names[0])?;

        // Extract password from Password enum
        let pwd = password.as_ref().and_then(|p| match p {
            sp::Password::Password(expr) => extract_string_literal(expr),
            sp::Password::NullPassword => None,
        });

        Ok(ResolvedStatement::CreateUser {
            username,
            host,
            password: pwd,
            if_not_exists,
        })
    }

    /// Resolve GRANT statement
    fn resolve_grant(
        &self,
        privileges: &sp::Privileges,
        objects: &sp::GrantObjects,
        grantees: &[sp::Ident],
        with_grant_option: bool,
    ) -> SqlResult<ResolvedStatement> {
        let resolved_privileges = convert_privileges(privileges)?;
        let object = convert_grant_objects(objects)?;

        if grantees.is_empty() {
            return Err(SqlError::Parse(
                "GRANT requires at least one grantee".to_string(),
            ));
        }

        // Parse first grantee's 'user'@'host' format
        let (grantee, grantee_host) = parse_grantee(&grantees[0])?;

        Ok(ResolvedStatement::Grant {
            privileges: resolved_privileges,
            object,
            grantee,
            grantee_host,
            with_grant_option,
        })
    }

    /// Resolve REVOKE statement
    fn resolve_revoke(
        &self,
        privileges: &sp::Privileges,
        objects: &sp::GrantObjects,
        grantees: &[sp::Ident],
    ) -> SqlResult<ResolvedStatement> {
        let resolved_privileges = convert_privileges(privileges)?;
        let object = convert_grant_objects(objects)?;

        if grantees.is_empty() {
            return Err(SqlError::Parse(
                "REVOKE requires at least one grantee".to_string(),
            ));
        }

        // Parse first grantee's 'user'@'host' format
        let (grantee, grantee_host) = parse_grantee(&grantees[0])?;

        Ok(ResolvedStatement::Revoke {
            privileges: resolved_privileges,
            object,
            grantee,
            grantee_host,
        })
    }
}

// ============ Auth helpers ============

/// Parse 'user'@'host' format from ObjectName
fn parse_user_host(name: &sp::ObjectName) -> SqlResult<(String, HostPattern)> {
    // ObjectName is a Vec<Ident> - typically just one element for users
    // MySQL format: 'user'@'host' - sqlparser may not parse this directly
    // so we handle common cases
    let full_name = name.to_string();

    // Check for @host pattern
    if let Some(at_pos) = full_name.find('@') {
        let username = full_name[..at_pos].trim_matches('\'').to_string();
        let host = full_name[at_pos + 1..].trim_matches('\'').to_string();
        Ok((username, HostPattern::new(host)))
    } else {
        // No host specified - default to '%' (any host)
        let username = full_name.trim_matches('\'').to_string();
        Ok((username, HostPattern::any()))
    }
}

/// Parse grantee identifier (may include host)
fn parse_grantee(ident: &sp::Ident) -> SqlResult<(String, HostPattern)> {
    let full_name = ident.value.clone();

    // Check for @host pattern
    if let Some(at_pos) = full_name.find('@') {
        let username = full_name[..at_pos].trim_matches('\'').to_string();
        let host = full_name[at_pos + 1..].trim_matches('\'').to_string();
        Ok((username, HostPattern::new(host)))
    } else {
        // No host specified - default to '%' (any host)
        Ok((full_name, HostPattern::any()))
    }
}

/// Extract string literal from expression
fn extract_string_literal(expr: &sp::Expr) -> Option<String> {
    match expr {
        sp::Expr::Value(sp::Value::SingleQuotedString(s)) => Some(s.clone()),
        sp::Expr::Value(sp::Value::DoubleQuotedString(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Convert sqlparser Privileges to our Privilege type
fn convert_privileges(privs: &sp::Privileges) -> SqlResult<Vec<Privilege>> {
    match privs {
        sp::Privileges::All { .. } => Ok(vec![Privilege::All]),
        sp::Privileges::Actions(actions) => {
            let mut result = Vec::new();
            for action in actions {
                result.push(convert_single_privilege(action)?);
            }
            if result.is_empty() {
                Err(SqlError::Unsupported(
                    "No recognized privileges".to_string(),
                ))
            } else {
                Ok(result)
            }
        }
    }
}

/// Convert a single privilege action
fn convert_single_privilege(action: &sp::Action) -> SqlResult<Privilege> {
    match action {
        sp::Action::Select { .. } => Ok(Privilege::Select),
        sp::Action::Insert { .. } => Ok(Privilege::Insert),
        sp::Action::Update { .. } => Ok(Privilege::Update),
        sp::Action::Delete => Ok(Privilege::Delete),
        sp::Action::Create => Ok(Privilege::Create),
        sp::Action::Truncate => Ok(Privilege::Delete), // Map truncate to delete privilege
        other => Err(SqlError::Unsupported(format!(
            "Unsupported privilege: {:?}",
            other
        ))),
    }
}

/// Convert grant objects to PrivilegeObject
fn convert_grant_objects(objects: &sp::GrantObjects) -> SqlResult<PrivilegeObject> {
    match objects {
        sp::GrantObjects::AllTablesInSchema { schemas } => {
            // Database-level: db.*
            if schemas.is_empty() {
                Ok(PrivilegeObject::Global)
            } else {
                let db_name = schemas[0].to_string();
                Ok(PrivilegeObject::Database(db_name))
            }
        }
        sp::GrantObjects::Tables(tables) => {
            // Table-level: db.table
            if tables.is_empty() {
                Ok(PrivilegeObject::Global)
            } else {
                let table_name = tables[0].to_string();
                // Parse db.table format
                if let Some(dot_pos) = table_name.find('.') {
                    let db = table_name[..dot_pos].to_string();
                    let table = table_name[dot_pos + 1..].to_string();
                    Ok(PrivilegeObject::Table {
                        database: db,
                        table,
                    })
                } else {
                    // No database specified - treat as table in current database
                    Ok(PrivilegeObject::Table {
                        database: String::new(),
                        table: table_name,
                    })
                }
            }
        }
        sp::GrantObjects::AllSequencesInSchema { .. } => {
            // Not supported - treat as global
            Ok(PrivilegeObject::Global)
        }
        sp::GrantObjects::Sequences(_) => {
            // Not supported - treat as global
            Ok(PrivilegeObject::Global)
        }
        sp::GrantObjects::Schemas(_) => {
            // Schema-level - treat as database-level
            Ok(PrivilegeObject::Global)
        }
    }
}

// ============ Scope ============

/// Scope for name resolution
struct Scope {
    tables: HashMap<String, TableInfo>,
    table_order: Vec<(String, usize)>, // (alias, column_offset)
    total_columns: usize,
}

struct TableInfo {
    columns: Vec<(String, DataType, bool)>, // (name, type, nullable)
    column_offset: usize,
}

impl Scope {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_order: Vec::new(),
            total_columns: 0,
        }
    }

    fn single_table(name: &str, table_def: &crate::catalog::TableDef) -> Self {
        let mut scope = Self::new();
        scope.add_table(name, name, table_def);
        scope
    }

    fn add_table(&mut self, alias: &str, _real_name: &str, table_def: &crate::catalog::TableDef) {
        let column_offset = self.total_columns;
        let num_columns = table_def.columns.len();

        self.tables.insert(
            alias.to_string(),
            TableInfo {
                columns: table_def
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                    .collect(),
                column_offset,
            },
        );

        self.table_order.push((alias.to_string(), column_offset));
        self.total_columns += num_columns;
    }
}

// ============ Conversion helpers ============

/// Convert column definition
fn convert_column_def(col: &sp::ColumnDef) -> SqlResult<ColumnDef> {
    let name = col.name.value.clone();
    let data_type = convert_data_type(&col.data_type)?;

    let mut col_def = ColumnDef::new(name, data_type);

    for option in &col.options {
        match &option.option {
            sp::ColumnOption::Null => {
                col_def = col_def.nullable(true);
            }
            sp::ColumnOption::NotNull => {
                col_def = col_def.nullable(false);
            }
            sp::ColumnOption::Default(expr) => {
                col_def = col_def.default(expr.to_string());
            }
            sp::ColumnOption::Unique { is_primary, .. } => {
                if *is_primary {
                    col_def = col_def.nullable(false);
                }
            }
            _ => {}
        }
    }

    Ok(col_def)
}

/// Convert data type
pub fn convert_data_type(dt: &sp::DataType) -> SqlResult<DataType> {
    match dt {
        sp::DataType::Boolean => Ok(DataType::Boolean),
        sp::DataType::TinyInt(_) => Ok(DataType::TinyInt),
        sp::DataType::SmallInt(_) => Ok(DataType::SmallInt),
        sp::DataType::Int(_) | sp::DataType::Integer(_) => Ok(DataType::Int),
        sp::DataType::BigInt(_) => Ok(DataType::BigInt),
        sp::DataType::Float(_) | sp::DataType::Real => Ok(DataType::Float),
        sp::DataType::Double | sp::DataType::DoublePrecision => Ok(DataType::Double),
        sp::DataType::Varchar(len) => {
            let n = extract_varchar_length(len).unwrap_or(255);
            Ok(DataType::Varchar(n))
        }
        sp::DataType::Char(len) => {
            let n = extract_varchar_length(len).unwrap_or(1);
            Ok(DataType::Varchar(n))
        }
        sp::DataType::Text => Ok(DataType::Text),
        sp::DataType::Blob(_) | sp::DataType::Binary(_) | sp::DataType::Varbinary(_) => {
            Ok(DataType::Blob)
        }
        sp::DataType::Timestamp(_, _) | sp::DataType::Datetime(_) => Ok(DataType::Timestamp),
        _ => Err(SqlError::Unsupported(format!("Data type: {:?}", dt))),
    }
}

/// Extract length from VARCHAR/CHAR specification
fn extract_varchar_length(len: &Option<sp::CharacterLength>) -> Option<u32> {
    len.as_ref().map(|l| match l {
        sp::CharacterLength::IntegerLength { length, .. } => *length as u32,
        sp::CharacterLength::Max => 65535,
    })
}

/// Convert table constraint
fn convert_table_constraint(constraint: &sp::TableConstraint) -> SqlResult<Option<Constraint>> {
    match constraint {
        sp::TableConstraint::PrimaryKey { columns, .. } => {
            let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
            Ok(Some(Constraint::PrimaryKey(cols)))
        }
        sp::TableConstraint::Unique { columns, .. } => {
            let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
            Ok(Some(Constraint::Unique(cols)))
        }
        sp::TableConstraint::ForeignKey {
            columns,
            foreign_table,
            referred_columns,
            ..
        } => {
            let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
            let ref_cols: Vec<String> = referred_columns.iter().map(|c| c.value.clone()).collect();
            Ok(Some(Constraint::ForeignKey {
                columns: cols,
                ref_table: foreign_table.to_string(),
                ref_columns: ref_cols,
            }))
        }
        sp::TableConstraint::Check { expr, .. } => Ok(Some(Constraint::Check(expr.to_string()))),
        _ => Ok(None),
    }
}

/// Extract column name from assignment target
fn extract_assignment_target(target: &sp::AssignmentTarget) -> SqlResult<String> {
    match target {
        sp::AssignmentTarget::ColumnName(names) => Ok(names
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".")),
        sp::AssignmentTarget::Tuple(_) => {
            Err(SqlError::Unsupported("Tuple assignment".to_string()))
        }
    }
}

/// Convert JOIN type
fn convert_join_type(join_op: &sp::JoinOperator) -> SqlResult<JoinType> {
    match join_op {
        sp::JoinOperator::Inner(_) => Ok(JoinType::Inner),
        sp::JoinOperator::LeftOuter(_) => Ok(JoinType::Left),
        sp::JoinOperator::RightOuter(_) => Ok(JoinType::Right),
        sp::JoinOperator::FullOuter(_) => Ok(JoinType::Full),
        sp::JoinOperator::CrossJoin => Ok(JoinType::Cross),
        _ => Err(SqlError::Unsupported("Join type".to_string())),
    }
}

/// Extract JOIN condition
fn extract_join_condition(join_op: &sp::JoinOperator) -> Option<&sp::Expr> {
    match join_op {
        sp::JoinOperator::Inner(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::LeftOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::RightOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::FullOuter(sp::JoinConstraint::On(expr)) => Some(expr),
        _ => None,
    }
}

/// Convert literal value
fn convert_value(val: &sp::Value) -> SqlResult<Literal> {
    match val {
        sp::Value::Null => Ok(Literal::Null),
        sp::Value::Boolean(b) => Ok(Literal::Boolean(*b)),
        sp::Value::Number(n, _) => {
            if n.contains('.') {
                Ok(Literal::Float(n.parse().unwrap_or(0.0)))
            } else {
                Ok(Literal::Integer(n.parse().unwrap_or(0)))
            }
        }
        sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => {
            Ok(Literal::String(s.clone()))
        }
        sp::Value::HexStringLiteral(s) => {
            let bytes = hex_decode(s).unwrap_or_default();
            Ok(Literal::Blob(bytes))
        }
        _ => Err(SqlError::Unsupported(format!("Value: {:?}", val))),
    }
}

/// Hex decoding helper
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

/// Convert binary operator
fn convert_binary_op(op: &sp::BinaryOperator) -> SqlResult<BinaryOp> {
    match op {
        sp::BinaryOperator::Plus => Ok(BinaryOp::Add),
        sp::BinaryOperator::Minus => Ok(BinaryOp::Sub),
        sp::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        sp::BinaryOperator::Divide => Ok(BinaryOp::Div),
        sp::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
        sp::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        sp::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        sp::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        sp::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        sp::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        sp::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        sp::BinaryOperator::And => Ok(BinaryOp::And),
        sp::BinaryOperator::Or => Ok(BinaryOp::Or),
        _ => Err(SqlError::Unsupported(format!("Binary operator: {:?}", op))),
    }
}

/// Convert unary operator
fn convert_unary_op(op: &sp::UnaryOperator) -> SqlResult<UnaryOp> {
    match op {
        sp::UnaryOperator::Not => Ok(UnaryOp::Not),
        sp::UnaryOperator::Minus => Ok(UnaryOp::Neg),
        _ => Err(SqlError::Unsupported(format!("Unary operator: {:?}", op))),
    }
}

// ============ Type inference ============

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
    use crate::sql::parser::Parser;

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let users = crate::catalog::TableDef::new("users")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("name", DataType::Varchar(100)))
            .column(ColumnDef::new("email", DataType::Varchar(255)));

        let orders = crate::catalog::TableDef::new("orders")
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

        let stmt = Parser::parse_one("SELECT id, name FROM users WHERE id = 1").unwrap();

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

        let stmt = Parser::parse_one("SELECT * FROM nonexistent").unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::TableNotFound(_))));
    }

    #[test]
    fn test_resolve_column_not_found() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = Parser::parse_one("SELECT nonexistent FROM users").unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
    }

    #[test]
    fn test_resolve_ambiguous_column() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        // Both users and orders have 'id' column
        let stmt =
            Parser::parse_one("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
                .unwrap();
        let result = resolver.resolve(stmt);

        assert!(matches!(result, Err(SqlError::AmbiguousColumn(_))));
    }

    #[test]
    fn test_resolve_qualified_column() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = Parser::parse_one(
            "SELECT users.id, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        let result = resolver.resolve(stmt);

        assert!(result.is_ok());
    }
}
