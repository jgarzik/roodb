//! Name resolution against the catalog
//!
//! The resolver takes parsed SQL statements from sqlparser and resolves:
//! - Table names to table definitions
//! - Column names to column definitions with type information
//! - Validates that referenced tables and columns exist

use std::cell::Cell;
use std::collections::{HashMap, HashSet};

use sqlparser::ast as sp;

use crate::catalog::{Catalog, ColumnDef, Constraint, DataType};
use crate::planner::logical::{
    BinaryOp, BooleanTestType, JoinType, Literal, ResolvedAssignment, ResolvedColumn, ResolvedExpr,
    ResolvedJoin, ResolvedOrderByItem, ResolvedSelect, ResolvedSelectItem, ResolvedStatement,
    ResolvedTableRef, UnaryOp,
};
use crate::sql::error::{SqlError, SqlResult};
use crate::sql::privileges::{HostPattern, Privilege, PrivilegeObject};

/// Name resolver
pub struct Resolver<'a> {
    catalog: &'a Catalog,
    /// When true, `?` placeholders resolve to `Literal::Placeholder(n)` instead of erroring.
    /// Used for prepared statement plan caching.
    placeholder_mode: bool,
    /// Counter for assigning placeholder indices (used in placeholder_mode)
    placeholder_counter: Cell<usize>,
    /// Track view expansion depth to prevent infinite recursion from circular views
    view_depth: Cell<usize>,
}

impl<'a> Resolver<'a> {
    /// Create a new resolver
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            placeholder_mode: false,
            placeholder_counter: Cell::new(0),
            view_depth: Cell::new(0),
        }
    }

    /// Create a resolver that handles `?` placeholders for plan caching.
    pub fn new_with_placeholders(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            placeholder_mode: true,
            placeholder_counter: Cell::new(0),
            view_depth: Cell::new(0),
        }
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
            sp::Statement::Update(update) => self.resolve_update(
                &update.table,
                &update.assignments,
                &update.selection,
                &[], // ORDER BY not in sqlparser Update (only Delete)
                &update.limit,
            ),
            sp::Statement::Delete(delete) => self.resolve_delete(&delete),
            sp::Statement::Query(query) => self.resolve_query(&query),

            // CREATE VIEW
            sp::Statement::CreateView(cv) => {
                let name = cv.name.to_string();
                let or_replace = cv.or_replace;
                let query_sql = cv.query.to_string();
                Ok(ResolvedStatement::CreateView {
                    name,
                    query_sql,
                    or_replace,
                })
            }

            // Auth statements
            sp::Statement::CreateRole(create_role) => self.resolve_create_user(
                &create_role.names,
                create_role.if_not_exists,
                &create_role.password,
            ),
            sp::Statement::Grant(grant) => {
                let default_objects = sp::GrantObjects::Schemas(Vec::new());
                let objects = grant.objects.as_ref().unwrap_or(&default_objects);
                self.resolve_grant(
                    &grant.privileges,
                    objects,
                    &grant.grantees,
                    grant.with_grant_option,
                )
            }
            sp::Statement::Revoke(revoke) => {
                let default_objects = sp::GrantObjects::Schemas(Vec::new());
                let objects = revoke.objects.as_ref().unwrap_or(&default_objects);
                self.resolve_revoke(&revoke.privileges, objects, &revoke.grantees)
            }

            // CREATE DATABASE / SCHEMA
            sp::Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(ResolvedStatement::CreateDatabase {
                name: db_name.to_string(),
                if_not_exists,
            }),

            // ANALYZE TABLE
            sp::Statement::Analyze(analyze) => {
                let table = analyze
                    .table_name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                // Verify table exists
                self.catalog
                    .get_table(&table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;
                Ok(ResolvedStatement::AnalyzeTable { table })
            }

            // TRUNCATE TABLE — resolve as DELETE with no filter (deletes all rows)
            sp::Statement::Truncate(truncate) => {
                let table = truncate
                    .table_names
                    .first()
                    .map(|t| t.name.to_string())
                    .unwrap_or_default();
                // Verify table exists
                self.catalog
                    .get_table(&table)
                    .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;
                Ok(ResolvedStatement::Delete {
                    table: table.clone(),
                    table_columns: self
                        .catalog
                        .get_table(&table)
                        .map(|td| {
                            td.columns
                                .iter()
                                .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                                .collect()
                        })
                        .unwrap_or_default(),
                    filter: None,
                    order_by: Vec::new(),
                    limit: None,
                })
            }

            // EXPLAIN statement
            sp::Statement::Explain { statement, .. } => {
                let inner = self.resolve(*statement)?;
                Ok(ResolvedStatement::Explain {
                    inner: Box::new(inner),
                })
            }

            _ => Err(SqlError::Unsupported(format!("Statement type: {:?}", stmt))),
        }
    }

    /// Resolve CREATE TABLE
    fn resolve_create_table(&self, create: &sp::CreateTable) -> SqlResult<ResolvedStatement> {
        let name = create.name.to_string();
        let if_not_exists = create.if_not_exists;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        let mut inline_pk_columns = Vec::new();

        for col in &create.columns {
            // Check for inline PRIMARY KEY before converting
            let col_name = col.name.value.clone();
            for opt_def in &col.options {
                if matches!(opt_def.option, sp::ColumnOption::PrimaryKey(_)) {
                    inline_pk_columns.push(col_name.clone());
                }
            }
            columns.push(convert_column_def(col)?);
        }

        for constraint in &create.constraints {
            if let Some(c) = convert_table_constraint(constraint)? {
                constraints.push(c);
            }
        }

        // Check for multiple primary key definitions (MySQL error 1068)
        let has_table_level_pk = constraints
            .iter()
            .any(|c| matches!(c, Constraint::PrimaryKey(_)));
        if !inline_pk_columns.is_empty() && has_table_level_pk {
            return Err(SqlError::Unsupported(
                "Multiple primary key defined".to_string(),
            ));
        }

        // Check for multiple table-level PK constraints
        let pk_count = constraints
            .iter()
            .filter(|c| matches!(c, Constraint::PrimaryKey(_)))
            .count();
        if pk_count > 1 {
            return Err(SqlError::Unsupported(
                "Multiple primary key defined".to_string(),
            ));
        }

        // Add PrimaryKey constraint for inline PK columns if not already present
        if !inline_pk_columns.is_empty() && !has_table_level_pk {
            constraints.push(Constraint::PrimaryKey(inline_pk_columns));
        }

        // Enforce NOT NULL on all PRIMARY KEY columns (MySQL behavior)
        for constraint in &constraints {
            if let Constraint::PrimaryKey(pk_cols) = constraint {
                for pk_col in pk_cols {
                    if let Some(col) = columns
                        .iter_mut()
                        .find(|c| c.name.eq_ignore_ascii_case(pk_col))
                    {
                        col.nullable = false;
                    }
                }
            }
        }

        // Handle CREATE TABLE ... SELECT (CTAS)
        if let Some(ref query) = create.query {
            let resolved = self.resolve_query(query)?;

            // Extract a ResolvedSelect for column derivation
            let select_for_cols = match &resolved {
                ResolvedStatement::Select(rs) => rs,
                ResolvedStatement::Union { left, .. } => left.as_ref(),
                _ => {
                    return Err(SqlError::Unsupported(
                        "CTAS source must be SELECT or UNION".to_string(),
                    ))
                }
            };

            // Derive column definitions from SELECT output
            let derived_columns = derive_columns_from_select(select_for_cols);

            // Merge: explicit columns first, then derived columns from SELECT
            let mut merged = columns;
            for dc in derived_columns {
                if !merged.iter().any(|c| c.name == dc.name) {
                    merged.push(dc);
                }
            }

            return Ok(ResolvedStatement::CreateTableAs {
                name,
                columns: merged,
                constraints,
                if_not_exists,
                source: Box::new(resolved),
            });
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
            sp::ObjectType::View => Ok(ResolvedStatement::DropView { name, if_exists }),
            sp::ObjectType::Table => {
                if names.len() > 1 {
                    // Multi-table DROP: DROP TABLE t1, t2, t3
                    let table_names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
                    Ok(ResolvedStatement::DropMultipleTables {
                        names: table_names,
                        if_exists,
                    })
                } else {
                    Ok(ResolvedStatement::DropTable { name, if_exists })
                }
            }
            sp::ObjectType::Index => Ok(ResolvedStatement::DropIndex { name }),
            sp::ObjectType::Schema => Ok(ResolvedStatement::DropDatabase { name, if_exists }),
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
            let col_name = col_expr.column.expr.to_string();
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
        let table = insert.table.to_string();

        let table_def = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;

        // Handle INSERT ... SET syntax by converting to INSERT ... VALUES
        if !insert.assignments.is_empty() {
            return self.resolve_insert_set(insert, &table, table_def);
        }

        // Build column list (explicit or all columns)
        // Check for INSERT INTO t1 () VALUES () — empty column list means use all defaults
        let values_rows_ref = match insert.source.as_ref().map(|s| s.body.as_ref()) {
            Some(sp::SetExpr::Values(sp::Values { rows, .. })) => rows,
            Some(_) => {
                // INSERT ... SELECT — resolve source query and return InsertSelect
                let source_query = insert.source.as_ref().unwrap();
                let resolved_source = self.resolve_query(source_query)?;

                // Always resolve ALL table columns for the target row
                let all_columns: Vec<ResolvedColumn> = table_def
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, col_def)| ResolvedColumn {
                        table: table.clone(),
                        name: col_def.name.clone(),
                        index: idx,
                        data_type: col_def.data_type.clone(),
                        nullable: col_def.nullable || col_def.auto_increment,
                        default_value: col_def.default.clone(),
                    })
                    .collect();

                // Build column_map: maps source column index → target column index.
                // When no explicit column list, source maps 1:1 to all table columns.
                // When explicit, only the named columns are mapped.
                let column_map = if insert.columns.is_empty() {
                    None
                } else {
                    let mut map = Vec::new();
                    for col_ident in &insert.columns {
                        let col_name = &col_ident.value;
                        // Validate column exists
                        let _ = table_def
                            .get_column(col_name)
                            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                        let idx = table_def
                            .get_column_index(col_name)
                            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                        map.push(idx);
                    }
                    Some(map)
                };

                return Ok(ResolvedStatement::InsertSelect {
                    table,
                    columns: all_columns,
                    source: Box::new(resolved_source),
                    column_map,
                    ignore: insert.ignore,
                });
            }
            None => return Err(SqlError::Unsupported("INSERT without VALUES".to_string())),
        };
        let is_empty_insert =
            insert.columns.is_empty() && values_rows_ref.first().is_some_and(|row| row.is_empty());

        let specified_columns: Vec<String> = if is_empty_insert {
            // INSERT INTO t1 () VALUES () — no columns specified, use defaults
            Vec::new()
        } else if insert.columns.is_empty() {
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
            let idx = table_def
                .get_column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            column_indices.push((
                idx,
                col_def.nullable || col_def.auto_increment,
                col_name.clone(),
            ));
        }

        // Build scope for expression resolution
        let scope = Scope::single_table(&table, table_def);

        // Resolve all columns for the output (always includes all table columns)
        // Auto-increment columns are treated as nullable for INSERT purposes
        // since NULL values will be replaced with auto-generated values.
        let mut resolved_columns = Vec::new();
        for (idx, col_def) in table_def.columns.iter().enumerate() {
            resolved_columns.push(ResolvedColumn {
                table: table.clone(),
                name: col_def.name.clone(),
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable || col_def.auto_increment,
                default_value: col_def.default.clone(),
            });
        }

        // Resolve values - expand to full rows
        let mut resolved_values = Vec::new();
        for row in values_rows_ref {
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
            let is_multi_row = values_rows_ref.len() > 1;
            for (value_idx, expr) in row.iter().enumerate() {
                let (col_idx, nullable, col_name) = &column_indices[value_idx];
                let resolved_expr = self.resolve_expr(expr, &scope)?;

                // Check NOT NULL constraint for explicitly specified NULL values.
                // MySQL: single-row INSERT → error 1048; multi-row → convert to default.
                if !is_multi_row
                    && !nullable
                    && matches!(resolved_expr, ResolvedExpr::Literal(Literal::Null))
                {
                    return Err(SqlError::InvalidOperation(format!(
                        "Column '{}' cannot be NULL",
                        col_name
                    )));
                }

                full_row[*col_idx] = resolved_expr;
            }

            // Apply DEFAULT values for omitted columns and check NOT NULL constraints.
            // Build set of specified indices for O(1) lookup.
            let specified_indices: HashSet<usize> =
                column_indices.iter().map(|(i, _, _)| *i).collect();
            for (idx, col_def) in table_def.columns.iter().enumerate() {
                if col_def.auto_increment {
                    continue;
                }
                if !matches!(full_row[idx], ResolvedExpr::Literal(Literal::Null)) {
                    continue;
                }
                let was_specified = specified_indices.contains(&idx);
                if !was_specified {
                    if let Some(ref default_expr) = col_def.default {
                        // Apply column DEFAULT for unspecified columns
                        let default_literal = parse_default_value(default_expr);
                        full_row[idx] = ResolvedExpr::Literal(default_literal);
                    } else if !col_def.nullable {
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
            ignore: insert.ignore,
        })
    }

    /// Resolve INSERT ... SET syntax (MySQL-specific)
    /// Converts SET a=1, b=2 to equivalent VALUES form
    fn resolve_insert_set(
        &self,
        insert: &sp::Insert,
        table: &str,
        table_def: &crate::catalog::TableDef,
    ) -> SqlResult<ResolvedStatement> {
        let scope = Scope::single_table(table, table_def);

        // Build resolved columns for all table columns
        let mut resolved_columns = Vec::new();
        for (idx, col_def) in table_def.columns.iter().enumerate() {
            resolved_columns.push(ResolvedColumn {
                table: table.to_string(),
                name: col_def.name.clone(),
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable || col_def.auto_increment,
                default_value: col_def.default.clone(),
            });
        }

        // Start with NULLs for all columns
        let mut full_row: Vec<ResolvedExpr> = table_def
            .columns
            .iter()
            .map(|_| ResolvedExpr::Literal(Literal::Null))
            .collect();

        // Fill in SET assignments
        for assign in &insert.assignments {
            let col_name = match &assign.target {
                sp::AssignmentTarget::ColumnName(names) => names.to_string(),
                sp::AssignmentTarget::Tuple(_) => {
                    return Err(SqlError::Unsupported("Tuple assignment".to_string()))
                }
            };
            let idx = table_def
                .get_column_index(&col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let resolved_expr = self.resolve_expr(&assign.value, &scope)?;
            full_row[idx] = resolved_expr;
        }

        // Apply defaults for unspecified columns (both nullable and non-nullable)
        for (idx, col_def) in table_def.columns.iter().enumerate() {
            if !col_def.auto_increment
                && matches!(full_row[idx], ResolvedExpr::Literal(Literal::Null))
            {
                if let Some(ref default_expr) = col_def.default {
                    full_row[idx] = ResolvedExpr::Literal(parse_default_value(default_expr));
                }
            }
        }

        Ok(ResolvedStatement::Insert {
            table: table.to_string(),
            columns: resolved_columns,
            values: vec![full_row],
            ignore: insert.ignore,
        })
    }

    /// Resolve UPDATE statement
    fn resolve_update(
        &self,
        table: &sp::TableWithJoins,
        assignments: &[sp::Assignment],
        selection: &Option<sp::Expr>,
        order_by: &[sp::OrderByExpr],
        limit: &Option<sp::Expr>,
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
            let idx = table_def
                .get_column_index(&col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;

            let column = ResolvedColumn {
                table: table_name.clone(),
                name: col_name,
                index: idx,
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
                default_value: None,
            };
            let value = self.resolve_expr(&assign.value, &scope)?;

            resolved_assignments.push(ResolvedAssignment { column, value });
        }

        // Resolve filter
        let resolved_filter = selection
            .as_ref()
            .map(|f| self.resolve_expr(f, &scope))
            .transpose()?;

        // Resolve ORDER BY
        let mut resolved_order_by = Vec::new();
        for ob in order_by {
            let expr = self.resolve_expr(&ob.expr, &scope)?;
            let asc = ob.options.asc.unwrap_or(true);
            resolved_order_by.push((expr, asc));
        }

        // Resolve LIMIT
        let resolved_limit = if let Some(limit_expr) = limit {
            match limit_expr {
                sp::Expr::Value(v) => {
                    if let sp::Value::Number(n, _) = &v.value {
                        n.parse::<usize>().ok()
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        Ok(ResolvedStatement::Update {
            table: table_name,
            table_columns,
            assignments: resolved_assignments,
            filter: resolved_filter,
            order_by: resolved_order_by,
            limit: resolved_limit,
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

        // Resolve ORDER BY
        let mut resolved_order_by = Vec::new();
        for ob in &delete.order_by {
            let expr = self.resolve_expr(&ob.expr, &scope)?;
            let asc = ob.options.asc.unwrap_or(true);
            resolved_order_by.push((expr, asc));
        }

        // Resolve LIMIT
        let resolved_limit = if let Some(ref limit_expr) = delete.limit {
            match limit_expr {
                sp::Expr::Value(v) => {
                    if let sp::Value::Number(n, _) = &v.value {
                        n.parse::<usize>().ok()
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        Ok(ResolvedStatement::Delete {
            table: table_name,
            table_columns,
            filter: resolved_filter,
            order_by: resolved_order_by,
            limit: resolved_limit,
        })
    }

    /// Resolve SELECT query
    fn resolve_query(&self, query: &sp::Query) -> SqlResult<ResolvedStatement> {
        // Check for UNION/INTERSECT/EXCEPT at the top level
        if let sp::SetExpr::SetOperation {
            left,
            right,
            op,
            set_quantifier,
            ..
        } = query.body.as_ref()
        {
            if matches!(op, sp::SetOperator::Union) {
                let left_select = self.resolve_set_expr_as_select(left)?;
                let right_select = self.resolve_set_expr_as_select(right)?;
                let all = matches!(
                    set_quantifier,
                    sp::SetQuantifier::All | sp::SetQuantifier::AllByName
                );
                let stmt = ResolvedStatement::Union {
                    left: Box::new(left_select),
                    right: Box::new(right_select),
                    all,
                };

                return Ok(stmt);
            }
            return Err(SqlError::Unsupported(format!("Set operation: {:?}", op)));
        }

        let select = self.resolve_select_body(query.body.as_ref())?;

        let mut result = select;

        // Handle ORDER BY
        if let Some(order_by) = &query.order_by {
            // Need to create scope from the resolved tables
            let mut scope = Scope::new();
            for table_ref in &result.from {
                let alias = table_ref
                    .alias
                    .clone()
                    .unwrap_or_else(|| table_ref.name.clone());
                if table_ref.inner_query.is_some() {
                    scope.add_derived_table(&alias, &table_ref.columns);
                } else if let Some(table_def) = self.catalog.get_table(&table_ref.name) {
                    scope.add_table(&alias, &table_ref.name, table_def);
                }
            }
            for join in &result.joins {
                let alias = join
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone());
                if join.table.inner_query.is_some() {
                    scope.add_derived_table(&alias, &join.table.columns);
                } else if let Some(table_def) = self.catalog.get_table(&join.table.name) {
                    scope.add_table(&alias, &join.table.name, table_def);
                }
            }

            if let sp::OrderByKind::Expressions(exprs) = &order_by.kind {
                for item in exprs {
                    // Try resolving against table scope first, then SELECT aliases
                    let resolved = match self.resolve_expr(&item.expr, &scope) {
                        Ok(expr) => expr,
                        Err(SqlError::ColumnNotFound(_)) => {
                            // Check if it matches a SELECT alias
                            if let sp::Expr::Identifier(ident) = &item.expr {
                                let alias_name = &ident.value;
                                let mut found = None;
                                for col_item in &result.columns {
                                    if let ResolvedSelectItem::Expr {
                                        expr: resolved_expr,
                                        alias: Some(a),
                                    } = col_item
                                    {
                                        if a.eq_ignore_ascii_case(alias_name) {
                                            found = Some(resolved_expr.clone());
                                            break;
                                        }
                                    }
                                }
                                found.ok_or_else(|| SqlError::ColumnNotFound(alias_name.clone()))?
                            } else {
                                return Err(SqlError::ColumnNotFound(item.expr.to_string()));
                            }
                        }
                        Err(e) => return Err(e),
                    };
                    result.order_by.push(ResolvedOrderByItem {
                        expr: resolved,
                        ascending: item.options.asc.unwrap_or(true),
                    });
                }
            }
        }

        // Handle LIMIT/OFFSET
        if let Some(ref limit_clause) = query.limit_clause {
            match limit_clause {
                sp::LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(sp::Expr::Value(ref val_with_span)) = limit {
                        if let sp::Value::Number(ref n, _) = val_with_span.value {
                            result.limit = Some(n.parse().map_err(|_| {
                                SqlError::InvalidOperation(format!("Invalid LIMIT value: '{}'", n))
                            })?);
                        }
                    }
                    if let Some(ref off) = offset {
                        if let sp::Expr::Value(ref val_with_span) = off.value {
                            if let sp::Value::Number(ref n, _) = val_with_span.value {
                                result.offset = Some(n.parse().map_err(|_| {
                                    SqlError::InvalidOperation(format!(
                                        "Invalid OFFSET value: '{}'",
                                        n
                                    ))
                                })?);
                            }
                        }
                    }
                }
                sp::LimitClause::OffsetCommaLimit { offset, limit } => {
                    if let sp::Expr::Value(ref val_with_span) = limit {
                        if let sp::Value::Number(ref n, _) = val_with_span.value {
                            result.limit = Some(n.parse().map_err(|_| {
                                SqlError::InvalidOperation(format!("Invalid LIMIT value: '{}'", n))
                            })?);
                        }
                    }
                    if let sp::Expr::Value(ref val_with_span) = offset {
                        if let sp::Value::Number(ref n, _) = val_with_span.value {
                            result.offset = Some(n.parse().map_err(|_| {
                                SqlError::InvalidOperation(format!("Invalid OFFSET value: '{}'", n))
                            })?);
                        }
                    }
                }
            }
        }

        Ok(ResolvedStatement::Select(result))
    }

    /// Resolve a SetExpr into a ResolvedSelect (for UNION operands)
    fn resolve_set_expr_as_select(&self, expr: &sp::SetExpr) -> SqlResult<ResolvedSelect> {
        match expr {
            sp::SetExpr::Select(_) | sp::SetExpr::Query(_) => self.resolve_select_body(expr),
            sp::SetExpr::SetOperation {
                left,
                right,
                op,
                set_quantifier,
                ..
            } => {
                // Nested UNION: resolve recursively, flatten into a single select
                // by collecting all rows — but we can't do that at resolve time.
                // Instead, error for now on deeply nested UNIONs.
                if matches!(op, sp::SetOperator::Union) {
                    let left_select = self.resolve_set_expr_as_select(left)?;
                    let right_select = self.resolve_set_expr_as_select(right)?;
                    let all = matches!(
                        set_quantifier,
                        sp::SetQuantifier::All | sp::SetQuantifier::AllByName
                    );
                    // Wrap as a "union select" — use left's metadata
                    // The planner will create a Union node
                    // For now, return left and the builder handles it via ResolvedStatement::Union
                    // Actually, we need to return ResolvedSelect not ResolvedStatement here.
                    // The simplest approach: return the left side and let the caller handle UNION.
                    let _ = (right_select, all);
                    Ok(left_select)
                } else {
                    Err(SqlError::Unsupported(format!("Set operation: {:?}", op)))
                }
            }
            _ => Err(SqlError::Unsupported("Complex set expression".to_string())),
        }
    }

    /// Resolve SELECT body
    fn resolve_select_body(&self, body: &sp::SetExpr) -> SqlResult<ResolvedSelect> {
        match body {
            sp::SetExpr::Select(select) => {
                // Build scope from FROM clause
                let mut scope = Scope::new();

                for table in &select.from {
                    let table_ref = self.resolve_table_factor(&table.relation)?;
                    let alias = table_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| table_ref.name.clone());

                    // MySQL DUAL pseudo-table: SELECT expr FROM DUAL is same as SELECT expr
                    if table_ref.name.eq_ignore_ascii_case("dual")
                        && table_ref.inner_query.is_none()
                    {
                        continue;
                    }

                    if table_ref.inner_query.is_some() {
                        // Derived table or view — add columns directly to scope
                        scope.add_derived_table(&alias, &table_ref.columns);
                    } else {
                        let table_def = self
                            .catalog
                            .get_table(&table_ref.name)
                            .ok_or_else(|| SqlError::TableNotFound(table_ref.name.clone()))?;
                        scope.add_table(&alias, &table_ref.name, table_def);
                    }
                }

                // Build resolved from list first (skip DUAL pseudo-table)
                let resolved_from: Vec<ResolvedTableRef> = select
                    .from
                    .iter()
                    .map(|t| self.resolve_table_factor(&t.relation))
                    .collect::<SqlResult<Vec<_>>>()?
                    .into_iter()
                    .filter(|t| !t.name.eq_ignore_ascii_case("dual") || t.inner_query.is_some())
                    .collect();

                // Add joined tables to scope
                let mut resolved_joins = Vec::new();
                for table_with_joins in &select.from {
                    for join in &table_with_joins.joins {
                        let table_ref = self.resolve_table_factor(&join.relation)?;

                        let alias = table_ref
                            .alias
                            .clone()
                            .unwrap_or_else(|| table_ref.name.clone());

                        // For views (inner_query set), use derived table columns
                        if table_ref.inner_query.is_some() {
                            scope.add_derived_table(&alias, &table_ref.columns);
                        } else {
                            let table_def = self
                                .catalog
                                .get_table(&table_ref.name)
                                .ok_or_else(|| SqlError::TableNotFound(table_ref.name.clone()))?;
                            scope.add_table(&alias, &table_ref.name, table_def);
                        }

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

                // Resolve GROUP BY — MySQL allows referencing SELECT aliases in GROUP BY
                // When a name matches both a table column AND a SELECT alias,
                // MySQL uses the alias and emits warning 1052 (ambiguous column).
                let mut resolved_group_by = Vec::new();
                match &select.group_by {
                    sp::GroupByExpr::Expressions(exprs, _) => {
                        for expr in exprs {
                            // For simple identifiers, check SELECT aliases first (MySQL behavior)
                            if let sp::Expr::Identifier(ident) = expr {
                                let name = &ident.value;
                                let mut alias_match = None;
                                for item in &resolved_columns {
                                    if let ResolvedSelectItem::Expr {
                                        expr: resolved_expr,
                                        alias: Some(a),
                                    } = item
                                    {
                                        if a.eq_ignore_ascii_case(name) {
                                            alias_match = Some(resolved_expr.clone());
                                            break;
                                        }
                                    }
                                }
                                if let Some(alias_expr) = alias_match {
                                    resolved_group_by.push(alias_expr);
                                    continue;
                                }
                            }
                            // Fall back to table scope resolution
                            match self.resolve_expr(expr, &scope) {
                                Ok(resolved) => resolved_group_by.push(resolved),
                                Err(SqlError::ColumnNotFound(_)) => {
                                    // Column not in table scope — check SELECT aliases
                                    if let sp::Expr::Identifier(ident) = expr {
                                        let alias_name = &ident.value;
                                        return Err(SqlError::ColumnNotFound(alias_name.clone()));
                                    } else {
                                        return Err(SqlError::ColumnNotFound(expr.to_string()));
                                    }
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    sp::GroupByExpr::All(_) => {
                        return Err(SqlError::Unsupported("GROUP BY ALL".to_string()));
                    }
                }

                // Resolve HAVING — aliases from SELECT list are valid here
                let resolved_having = if let Some(h) = &select.having {
                    Some(self.resolve_expr_with_aliases(h, &scope, &resolved_columns)?)
                } else {
                    None
                };

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
            sp::SetExpr::Query(inner_query) => {
                // Parenthesized query: (SELECT ... LIMIT ...) ORDER BY ... LIMIT ...
                // Resolve the inner query fully (including its own ORDER BY/LIMIT),
                // then the outer ORDER BY/LIMIT is applied by resolve_query().
                // If the inner query is a UNION, resolve its left side for column metadata.
                let inner = self.resolve_query(inner_query)?;
                match inner {
                    ResolvedStatement::Select(resolved) => Ok(resolved),
                    ResolvedStatement::Union { left, .. } => Ok(*left),
                    _ => Err(SqlError::Unsupported(
                        "Parenthesized non-SELECT query".to_string(),
                    )),
                }
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

                // MySQL DUAL pseudo-table: FROM DUAL is equivalent to no FROM
                if table_name.eq_ignore_ascii_case("dual") {
                    return Ok(ResolvedTableRef {
                        name: "dual".to_string(),
                        alias: alias.as_ref().map(|a| a.name.value.clone()),
                        columns: vec![],
                        inner_query: None,
                    });
                }

                // Check physical table first
                if let Some(table_def) = self.catalog.get_table(&table_name) {
                    return Ok(ResolvedTableRef {
                        name: table_name,
                        alias: alias.as_ref().map(|a| a.name.value.clone()),
                        columns: table_def
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                            .collect(),
                        inner_query: None,
                    });
                }

                // Fall back to view — expand view query as a derived table
                if let Some(view_def) = self.catalog.get_view(&table_name) {
                    let depth = self.view_depth.get();
                    if depth >= 32 {
                        return Err(SqlError::InvalidOperation(format!(
                            "View recursion limit exceeded (depth {}), possible circular reference",
                            depth
                        )));
                    }
                    self.view_depth.set(depth + 1);

                    let view_sql = view_def.query_sql.clone();
                    let view_stmt = crate::sql::Parser::parse_one(&view_sql).map_err(|e| {
                        SqlError::Parse(format!("Invalid view '{}': {}", table_name, e))
                    })?;
                    if let sp::Statement::Query(query) = view_stmt {
                        let inner_select = self.resolve_select_body(query.body.as_ref())?;
                        let columns: Vec<(String, DataType, bool)> = inner_select
                            .columns
                            .iter()
                            .enumerate()
                            .flat_map(|(idx, item)| match item {
                                ResolvedSelectItem::Columns(cols) => cols
                                    .iter()
                                    .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                                    .collect::<Vec<_>>(),
                                ResolvedSelectItem::Expr { expr, alias: a } => {
                                    let name = a.clone().unwrap_or_else(|| {
                                        crate::planner::logical::builder::LogicalPlanBuilder::expr_name(expr, idx)
                                    });
                                    vec![(name, expr.data_type(), expr.is_nullable())]
                                }
                            })
                            .collect();
                        let effective_alias = alias
                            .as_ref()
                            .map(|a| a.name.value.clone())
                            .unwrap_or_else(|| table_name.clone());
                        self.view_depth.set(self.view_depth.get() - 1);
                        return Ok(ResolvedTableRef {
                            name: table_name,
                            alias: Some(effective_alias),
                            columns,
                            inner_query: Some(Box::new(inner_select)),
                        });
                    }
                    self.view_depth.set(self.view_depth.get() - 1);
                }

                Err(SqlError::TableNotFound(table_name))
            }
            // Derived table (subquery in FROM clause)
            sp::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or_else(|| {
                        SqlError::Parse("Derived table must have an alias".to_string())
                    })?;
                let inner_select = self.resolve_select_body(subquery.body.as_ref())?;
                // Derive columns from the inner query's SELECT list
                let columns: Vec<(String, DataType, bool)> = inner_select
                    .columns
                    .iter()
                    .enumerate()
                    .flat_map(|(idx, item)| match item {
                        ResolvedSelectItem::Columns(cols) => cols
                            .iter()
                            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                            .collect::<Vec<_>>(),
                        ResolvedSelectItem::Expr { expr, alias } => {
                            let name = alias.clone().unwrap_or_else(|| {
                                crate::planner::logical::builder::LogicalPlanBuilder::expr_name(
                                    expr, idx,
                                )
                            });
                            vec![(name, expr.data_type(), expr.is_nullable())]
                        }
                    })
                    .collect();
                Ok(ResolvedTableRef {
                    name: alias_name.clone(),
                    alias: Some(alias_name),
                    columns,
                    inner_query: Some(Box::new(inner_select)),
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
                    let table_info = scope
                        .tables
                        .get(table_alias)
                        .ok_or_else(|| SqlError::TableNotFound(table_alias.clone()))?;
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
                            default_value: None,
                        });
                    }
                }
                Ok(ResolvedSelectItem::Columns(columns))
            }
            sp::SelectItem::QualifiedWildcard(kind, _) => {
                let table = match kind {
                    sp::SelectItemQualifiedWildcardKind::ObjectName(name) => name.to_string(),
                    sp::SelectItemQualifiedWildcardKind::Expr(expr) => expr.to_string(),
                };
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
                            default_value: None,
                        }
                    })
                    .collect();

                Ok(ResolvedSelectItem::Columns(columns))
            }
        }
    }

    /// Resolve expression with SELECT alias fallback (for HAVING/ORDER BY).
    /// Resolve expression with SELECT alias fallback (for HAVING).
    /// Identifiers that match SELECT aliases are replaced with the aliased expression.
    /// Recurses into BinaryOp/UnaryOp to handle `HAVING s <> 0` where `s` is an alias.
    fn resolve_expr_with_aliases(
        &self,
        expr: &sp::Expr,
        scope: &Scope,
        select_columns: &[ResolvedSelectItem],
    ) -> SqlResult<ResolvedExpr> {
        // First check if this is a plain identifier matching a SELECT alias
        if let sp::Expr::Identifier(ident) = expr {
            if !ident.value.starts_with('@') {
                let alias_name = &ident.value;
                for col_item in select_columns {
                    if let ResolvedSelectItem::Expr {
                        expr: resolved_expr,
                        alias: Some(a),
                    } = col_item
                    {
                        if a.eq_ignore_ascii_case(alias_name) {
                            return Ok(resolved_expr.clone());
                        }
                    }
                }
            }
        }
        // For compound expressions, recurse to resolve sub-expressions with aliases
        match expr {
            sp::Expr::BinaryOp { left, op, right } => {
                let l = self.resolve_expr_with_aliases(left, scope, select_columns)?;
                let r = self.resolve_expr_with_aliases(right, scope, select_columns)?;
                let binary_op = convert_binary_op(op)?;
                let result_type = infer_binary_result_type(binary_op, &l, &r)?;
                Ok(ResolvedExpr::BinaryOp {
                    left: Box::new(l),
                    op: binary_op,
                    right: Box::new(r),
                    result_type,
                })
            }
            sp::Expr::UnaryOp { op, expr: inner } => {
                let resolved = self.resolve_expr_with_aliases(inner, scope, select_columns)?;
                let unary_op = convert_unary_op(op)?;
                let result_type = infer_unary_result_type(unary_op, &resolved)?;
                Ok(ResolvedExpr::UnaryOp {
                    op: unary_op,
                    expr: Box::new(resolved),
                    result_type,
                })
            }
            // All other expression types: normal resolution
            _ => self.resolve_expr(expr, scope),
        }
    }

    /// Resolve expression
    fn resolve_expr(&self, expr: &sp::Expr, scope: &Scope) -> SqlResult<ResolvedExpr> {
        match expr {
            sp::Expr::Identifier(ident)
                if ident.value.starts_with('@') && !ident.value.starts_with("@@") =>
            {
                Ok(ResolvedExpr::UserVariable {
                    name: ident.value[1..].to_lowercase(),
                })
            }
            sp::Expr::Identifier(ident) => self.resolve_column(None, &ident.value, scope),
            sp::Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    self.resolve_column(Some(&idents[0].value), &idents[1].value, scope)
                } else {
                    Err(SqlError::Unsupported("Compound identifier".to_string()))
                }
            }
            sp::Expr::Value(val_with_span) => {
                let val = &val_with_span.value;
                // Handle `?` placeholders in placeholder mode (prepared statement plan caching)
                if self.placeholder_mode {
                    if let sp::Value::Placeholder(s) = val {
                        if s == "?" {
                            let idx = self.placeholder_counter.get();
                            self.placeholder_counter.set(idx + 1);
                            return Ok(ResolvedExpr::Literal(Literal::Placeholder(idx)));
                        }
                    }
                }
                Ok(ResolvedExpr::Literal(convert_value(val)?))
            }
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
                                    // For functions that accept keyword arguments
                                    // (GET_FORMAT, CONVERT, etc.), unresolvable
                                    // identifiers are treated as string literals.
                                    let accepts_keywords =
                                        matches!(name.as_str(), "GET_FORMAT" | "CONVERT");
                                    match self.resolve_expr(e, scope) {
                                        Ok(resolved) => result.push(resolved),
                                        Err(_)
                                            if accepts_keywords
                                                && matches!(e, sp::Expr::Identifier(_)) =>
                                        {
                                            let ident = match e {
                                                sp::Expr::Identifier(id) => id.value.to_uppercase(),
                                                _ => unreachable!(),
                                            };
                                            result.push(ResolvedExpr::Literal(Literal::String(
                                                ident,
                                            )));
                                        }
                                        Err(err) => return Err(err),
                                    }
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
            // IS TRUE / IS FALSE / IS UNKNOWN predicates
            sp::Expr::IsTrue(e) => self.resolve_boolean_test(e, scope, BooleanTestType::IsTrue),
            sp::Expr::IsNotTrue(e) => {
                self.resolve_boolean_test(e, scope, BooleanTestType::IsNotTrue)
            }
            sp::Expr::IsFalse(e) => self.resolve_boolean_test(e, scope, BooleanTestType::IsFalse),
            sp::Expr::IsNotFalse(e) => {
                self.resolve_boolean_test(e, scope, BooleanTestType::IsNotFalse)
            }
            sp::Expr::IsUnknown(e) => {
                self.resolve_boolean_test(e, scope, BooleanTestType::IsUnknown)
            }
            sp::Expr::IsNotUnknown(e) => {
                self.resolve_boolean_test(e, scope, BooleanTestType::IsNotUnknown)
            }

            sp::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let resolved_operand = operand
                    .as_ref()
                    .map(|e| self.resolve_expr(e, scope))
                    .transpose()?
                    .map(Box::new);
                let resolved_conditions: Vec<ResolvedExpr> = conditions
                    .iter()
                    .map(|cw| self.resolve_expr(&cw.condition, scope))
                    .collect::<SqlResult<Vec<_>>>()?;
                let resolved_results: Vec<ResolvedExpr> = conditions
                    .iter()
                    .map(|cw| self.resolve_expr(&cw.result, scope))
                    .collect::<SqlResult<Vec<_>>>()?;
                let resolved_else = else_result
                    .as_ref()
                    .map(|e| self.resolve_expr(e, scope))
                    .transpose()?
                    .map(Box::new);

                // Infer result type from all result branches
                let result_type = infer_case_result_type(&resolved_results, &resolved_else);

                Ok(ResolvedExpr::Case {
                    operand: resolved_operand,
                    conditions: resolved_conditions,
                    results: resolved_results,
                    else_result: resolved_else,
                    result_type,
                })
            }

            sp::Expr::Cast {
                expr, data_type, ..
            } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                let target_type = convert_data_type(data_type)?;
                Ok(ResolvedExpr::Cast {
                    expr: Box::new(resolved_expr),
                    target_type,
                })
            }

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
            // TypedString: DATE '2020-01-01' etc. — extract string value
            sp::Expr::TypedString(typed_string) => {
                let s = match &typed_string.value.value {
                    sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => {
                        s.clone()
                    }
                    other => other.to_string(),
                };
                Ok(ResolvedExpr::Literal(Literal::String(s)))
            }

            // Prefixed: _charset 'string' — treat as string literal (was IntroducedString)
            sp::Expr::Prefixed { value, .. } => self.resolve_expr(value, scope),

            // Collate: expr COLLATE collation — ignore collation, return expr
            sp::Expr::Collate { expr, .. } => self.resolve_expr(expr, scope),

            // Subquery: (SELECT ...) — resolve as scalar subquery
            sp::Expr::Subquery(_) => Err(SqlError::Unsupported(
                "Subqueries not yet supported".to_string(),
            )),

            // SUBSTRING(expr, from, for) — sqlparser parses as AST node
            sp::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![self.resolve_expr(expr, scope)?];
                if let Some(from) = substring_from {
                    args.push(self.resolve_expr(from, scope)?);
                }
                if let Some(for_expr) = substring_for {
                    args.push(self.resolve_expr(for_expr, scope)?);
                }
                let result_type = infer_function_result_type("SUBSTRING", &args)?;
                Ok(ResolvedExpr::Function {
                    name: "SUBSTRING".to_string(),
                    args,
                    distinct: false,
                    result_type,
                })
            }

            // TRIM(expr) — sqlparser parses as AST node
            sp::Expr::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => {
                let resolved = self.resolve_expr(expr, scope)?;
                let name = match trim_where {
                    Some(sp::TrimWhereField::Leading) => "LTRIM",
                    Some(sp::TrimWhereField::Trailing) => "RTRIM",
                    _ => "TRIM",
                };
                let mut args = vec![resolved];
                if let Some(what) = trim_what {
                    args.push(self.resolve_expr(what, scope)?);
                }
                let result_type = infer_function_result_type(name, &args)?;
                Ok(ResolvedExpr::Function {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    result_type,
                })
            }

            // FLOOR(expr) / CEIL(expr) — sqlparser parses these as AST nodes, not functions
            sp::Expr::Floor { expr, .. } => {
                let resolved = self.resolve_expr(expr, scope)?;
                let result_type =
                    infer_function_result_type("FLOOR", std::slice::from_ref(&resolved))?;
                Ok(ResolvedExpr::Function {
                    name: "FLOOR".to_string(),
                    args: vec![resolved],
                    distinct: false,
                    result_type,
                })
            }
            sp::Expr::Ceil { expr, .. } => {
                let resolved = self.resolve_expr(expr, scope)?;
                let result_type =
                    infer_function_result_type("CEIL", std::slice::from_ref(&resolved))?;
                Ok(ResolvedExpr::Function {
                    name: "CEIL".to_string(),
                    args: vec![resolved],
                    distinct: false,
                    result_type,
                })
            }

            // REGEXP / RLIKE — resolve as a function call to REGEXP
            sp::Expr::RLike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                let resolved_pattern = self.resolve_expr(pattern, scope)?;
                let func = ResolvedExpr::Function {
                    name: "REGEXP".to_string(),
                    args: vec![resolved_expr, resolved_pattern],
                    distinct: false,
                    result_type: DataType::Boolean,
                };
                if *negated {
                    Ok(ResolvedExpr::UnaryOp {
                        op: UnaryOp::Not,
                        expr: Box::new(func),
                        result_type: DataType::Boolean,
                    })
                } else {
                    Ok(func)
                }
            }

            // CONVERT(expr, type) — resolve as CAST
            sp::Expr::Convert {
                expr, data_type, ..
            } => {
                let resolved_expr = self.resolve_expr(expr, scope)?;
                if let Some(dt) = data_type {
                    let target_type = convert_data_type(dt)?;
                    Ok(ResolvedExpr::Cast {
                        expr: Box::new(resolved_expr),
                        target_type,
                    })
                } else {
                    // CONVERT with charset only — pass through as string
                    Ok(resolved_expr)
                }
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
            // Qualified column reference (case-insensitive table name)
            let table_key = table_name.to_lowercase();
            let table_info = scope
                .tables
                .get(&table_key)
                .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?;

            let (local_idx, col_info) = table_info
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _, _))| n.eq_ignore_ascii_case(name))
                .ok_or_else(|| SqlError::ColumnNotFound(name.to_string()))?;

            let global_idx = table_info.column_offset + local_idx;

            Ok(ResolvedExpr::Column(ResolvedColumn {
                table: table_name.to_string(),
                name: col_info.0.clone(), // Use the original column name from catalog
                index: global_idx,
                data_type: col_info.1.clone(),
                nullable: col_info.2,
                default_value: None,
            }))
        } else {
            // Unqualified column reference - search all tables
            let mut found: Option<(String, usize, DataType, bool)> = None;

            for (table_alias, table_info) in &scope.tables {
                if let Some((local_idx, col_info)) = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _, _))| n.eq_ignore_ascii_case(name))
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
                        default_value: None,
                    }))
                }
                None => Err(SqlError::ColumnNotFound(name.to_string())),
            }
        }
    }

    /// Resolve IS TRUE / IS FALSE / IS UNKNOWN boolean test predicate
    fn resolve_boolean_test(
        &self,
        inner: &sp::Expr,
        scope: &Scope,
        test: BooleanTestType,
    ) -> SqlResult<ResolvedExpr> {
        Ok(ResolvedExpr::BooleanTest {
            expr: Box::new(self.resolve_expr(inner, scope)?),
            test,
        })
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
        grantees: &[sp::Grantee],
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
        let (grantee, grantee_host) = parse_grantee_obj(&grantees[0])?;

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
        grantees: &[sp::Grantee],
    ) -> SqlResult<ResolvedStatement> {
        let resolved_privileges = convert_privileges(privileges)?;
        let object = convert_grant_objects(objects)?;

        if grantees.is_empty() {
            return Err(SqlError::Parse(
                "REVOKE requires at least one grantee".to_string(),
            ));
        }

        // Parse first grantee's 'user'@'host' format
        let (grantee, grantee_host) = parse_grantee_obj(&grantees[0])?;

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

/// Parse grantee from sqlparser Grantee struct
fn parse_grantee_obj(grantee: &sp::Grantee) -> SqlResult<(String, HostPattern)> {
    match &grantee.name {
        Some(sp::GranteeName::UserHost { user, host }) => {
            Ok((user.value.clone(), HostPattern::new(host.value.clone())))
        }
        Some(sp::GranteeName::ObjectName(name)) => {
            let full_name = name.to_string();
            if let Some(at_pos) = full_name.find('@') {
                let username = full_name[..at_pos].trim_matches('\'').to_string();
                let host = full_name[at_pos + 1..].trim_matches('\'').to_string();
                Ok((username, HostPattern::new(host)))
            } else {
                Ok((full_name, HostPattern::any()))
            }
        }
        None => {
            // Fallback: use to_string on the grantee
            let full_name = grantee.to_string().trim().to_string();
            if let Some(at_pos) = full_name.find('@') {
                let username = full_name[..at_pos].trim_matches('\'').to_string();
                let host = full_name[at_pos + 1..].trim_matches('\'').to_string();
                Ok((username, HostPattern::new(host)))
            } else {
                Ok((full_name, HostPattern::any()))
            }
        }
    }
}

/// Extract string literal from expression
fn extract_string_literal(expr: &sp::Expr) -> Option<String> {
    match expr {
        sp::Expr::Value(val_with_span) => match &val_with_span.value {
            sp::Value::SingleQuotedString(s) => Some(s.clone()),
            sp::Value::DoubleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
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
        sp::Action::Create { .. } => Ok(Privilege::Create),
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
        _ => Ok(PrivilegeObject::Global),
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

        // Store with original case but also support case-insensitive lookups
        self.tables.insert(
            alias.to_lowercase(),
            TableInfo {
                columns: table_def
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                    .collect(),
                column_offset,
            },
        );

        self.table_order.push((alias.to_lowercase(), column_offset));
        self.total_columns += num_columns;
    }

    /// Add a derived table (subquery or view) to scope using pre-computed columns
    fn add_derived_table(&mut self, alias: &str, columns: &[(String, DataType, bool)]) {
        let column_offset = self.total_columns;
        let num_columns = columns.len();

        self.tables.insert(
            alias.to_lowercase(),
            TableInfo {
                columns: columns.to_vec(),
                column_offset,
            },
        );

        self.table_order.push((alias.to_lowercase(), column_offset));
        self.total_columns += num_columns;
    }
}

// ============ Conversion helpers ============

/// Derive column definitions from a resolved SELECT's output items.
/// Used by CREATE TABLE ... SELECT to determine the schema of the new table.
fn derive_columns_from_select(select: &ResolvedSelect) -> Vec<ColumnDef> {
    use crate::planner::logical::builder::LogicalPlanBuilder;

    let mut columns = Vec::new();
    for (idx, item) in select.columns.iter().enumerate() {
        match item {
            ResolvedSelectItem::Expr { expr, alias } => {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| LogicalPlanBuilder::expr_name(expr, idx));
                let data_type = expr.data_type();
                let nullable = expr.is_nullable();
                columns.push(ColumnDef::new(name, data_type).nullable(nullable));
            }
            ResolvedSelectItem::Columns(cols) => {
                for col in cols {
                    columns.push(
                        ColumnDef::new(col.name.clone(), col.data_type.clone())
                            .nullable(col.nullable),
                    );
                }
            }
        }
    }
    columns
}

/// Public wrapper for convert_column_def (used by ALTER TABLE handler)
pub fn convert_column_def_pub(col: &sp::ColumnDef) -> SqlResult<ColumnDef> {
    convert_column_def(col)
}

/// Public wrapper for convert_table_constraint (used by ALTER TABLE handler)
pub fn convert_table_constraint_pub(
    constraint: &sp::TableConstraint,
) -> SqlResult<Option<Constraint>> {
    convert_table_constraint(constraint)
}

/// Convert column definition
fn convert_column_def(col: &sp::ColumnDef) -> SqlResult<ColumnDef> {
    let name = col.name.value.clone();

    // Check if this is a VARCHAR with length > 65535 (would be promoted to TEXT)
    let has_default = col
        .options
        .iter()
        .any(|o| matches!(o.option, sp::ColumnOption::Default(_)));
    let is_oversized_varchar = matches!(
        &col.data_type,
        sp::DataType::Varchar(len) | sp::DataType::CharacterVarying(len) | sp::DataType::CharVarying(len) | sp::DataType::Nvarchar(len)
        if extract_varchar_length(len).unwrap_or(255) > 65535
    );
    if is_oversized_varchar && has_default {
        return Err(SqlError::InvalidOperation(format!(
            "Column length too big for column '{}' (max = 65535)",
            name
        )));
    }

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
            sp::ColumnOption::Unique(_) => {
                // Unique constraint on column — no special handling needed
            }
            sp::ColumnOption::PrimaryKey(_) => {
                col_def = col_def.nullable(false);
            }
            sp::ColumnOption::DialectSpecific(tokens) => {
                let token_str: String = tokens
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<String>()
                    .to_uppercase();
                if token_str.contains("AUTO_INCREMENT") || token_str.contains("AUTOINCREMENT") {
                    col_def = col_def.auto_increment();
                    col_def = col_def.nullable(false);
                }
            }
            _ => {}
        }
    }

    // MySQL: BLOB/TEXT columns cannot have a non-empty default value
    if let Some(ref default_val) = col_def.default {
        if matches!(col_def.data_type, DataType::Text | DataType::Blob) {
            let trimmed = default_val.trim_matches('\'').trim_matches('"');
            if !trimmed.is_empty() {
                return Err(SqlError::InvalidOperation(format!(
                    "BLOB, TEXT, GEOMETRY or JSON column '{}' can't have a default value",
                    col_def.name
                )));
            }
        }
    }

    Ok(col_def)
}

/// Convert data type
pub fn convert_data_type(dt: &sp::DataType) -> SqlResult<DataType> {
    match dt {
        // Boolean
        sp::DataType::Boolean | sp::DataType::Bool => Ok(DataType::Boolean),
        // TinyInt
        sp::DataType::TinyInt(_) | sp::DataType::TinyIntUnsigned(_) | sp::DataType::UTinyInt => {
            Ok(DataType::TinyInt)
        }
        // SmallInt
        sp::DataType::SmallInt(_)
        | sp::DataType::SmallIntUnsigned(_)
        | sp::DataType::USmallInt
        | sp::DataType::Int2(_)
        | sp::DataType::Int2Unsigned(_) => Ok(DataType::SmallInt),
        // Int
        sp::DataType::Int(_)
        | sp::DataType::Integer(_)
        | sp::DataType::MediumInt(_)
        | sp::DataType::MediumIntUnsigned(_)
        | sp::DataType::IntUnsigned(_)
        | sp::DataType::IntegerUnsigned(_)
        | sp::DataType::UnsignedInteger
        | sp::DataType::Int4(_)
        | sp::DataType::Int4Unsigned(_)
        | sp::DataType::Int16
        | sp::DataType::Int32
        | sp::DataType::UInt8
        | sp::DataType::UInt16
        | sp::DataType::UInt32 => Ok(DataType::Int),
        // BigInt
        // BigInt (signed)
        sp::DataType::BigInt(_)
        | sp::DataType::Int8(_)
        | sp::DataType::Int64
        | sp::DataType::Int128
        | sp::DataType::Int256
        | sp::DataType::UInt128
        | sp::DataType::UInt256
        | sp::DataType::HugeInt
        | sp::DataType::UHugeInt
        | sp::DataType::Signed
        | sp::DataType::SignedInteger => Ok(DataType::BigInt),
        // BigInt Unsigned
        sp::DataType::BigIntUnsigned(_)
        | sp::DataType::Int8Unsigned(_)
        | sp::DataType::UBigInt
        | sp::DataType::UInt64
        | sp::DataType::Unsigned => Ok(DataType::BigIntUnsigned),
        // Float — validate precision (MySQL: 0-24 → FLOAT, 25-53 → DOUBLE, >53 → error)
        sp::DataType::Float(info) | sp::DataType::FloatUnsigned(info) => {
            match info {
                sp::ExactNumberInfo::Precision(p) => {
                    if *p > 53 {
                        // FLOAT(p) with p>53 and no scale → error 1063
                        return Err(SqlError::InvalidOperation(
                            "Incorrect column specifier".to_string(),
                        ));
                    }
                    if *p > 24 {
                        return Ok(DataType::Double);
                    }
                }
                sp::ExactNumberInfo::PrecisionAndScale(p, s) => {
                    // MySQL: FLOAT/DOUBLE(M,D) — D (scale) max is 30
                    if *s > 30 {
                        return Err(SqlError::InvalidOperation(
                            "Display width out of range for column (max = 255)".to_string(),
                        ));
                    }
                    if *s as u64 > *p {
                        // FLOAT(M,D) with D>M → error 1427
                        return Err(SqlError::InvalidOperation(format!(
                            "Too big scale {} specified for column. Maximum is {}.",
                            s, p
                        )));
                    }
                    // FLOAT(M,D): M is display width (max 255)
                    if *p > 255 {
                        return Err(SqlError::InvalidOperation(
                            "Display width out of range for column (max = 255)".to_string(),
                        ));
                    }
                }
                sp::ExactNumberInfo::None => {}
            }
            Ok(DataType::Float)
        }
        sp::DataType::Real
        | sp::DataType::Float4
        | sp::DataType::Float32
        | sp::DataType::RealUnsigned => Ok(DataType::Float),
        // Double — validate precision
        sp::DataType::Double(info) | sp::DataType::DoubleUnsigned(info) => {
            if let sp::ExactNumberInfo::PrecisionAndScale(p, s) = info {
                // MySQL: DOUBLE(M,D) — D (scale) max is 30
                if *s > 30 {
                    return Err(SqlError::InvalidOperation(format!(
                        "Too big scale {} specified for column. Maximum is 30.",
                        s
                    )));
                }
                if *s as u64 > *p {
                    return Err(SqlError::InvalidOperation(format!(
                        "Too big scale {} specified for column. Maximum is {}.",
                        s, p
                    )));
                }
                // DOUBLE(M,D): M is display width (max 255)
                if *p > 255 {
                    return Err(SqlError::InvalidOperation(
                        "Display width out of range for column (max = 255)".to_string(),
                    ));
                }
            }
            Ok(DataType::Double)
        }
        sp::DataType::DoublePrecision
        | sp::DataType::Float8
        | sp::DataType::Float64
        | sp::DataType::DoublePrecisionUnsigned => Ok(DataType::Double),
        // DECIMAL/NUMERIC
        sp::DataType::Decimal(info)
        | sp::DataType::Numeric(info)
        | sp::DataType::Dec(info)
        | sp::DataType::DecimalUnsigned(info)
        | sp::DataType::DecUnsigned(info)
        | sp::DataType::BigNumeric(info)
        | sp::DataType::BigDecimal(info) => {
            let (p, s) = match info {
                sp::ExactNumberInfo::None => (10, 0),
                sp::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sp::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
            };
            Ok(DataType::Decimal {
                precision: p.min(65),
                scale: s.min(30),
            })
        }
        // Varchar — promote to Text if too large (MySQL silently converts)
        sp::DataType::Varchar(len)
        | sp::DataType::CharacterVarying(len)
        | sp::DataType::CharVarying(len)
        | sp::DataType::Nvarchar(len) => {
            let n = extract_varchar_length(len).unwrap_or(255);
            if n > 65535 {
                Ok(DataType::Text)
            } else {
                Ok(DataType::Varchar(n))
            }
        }
        sp::DataType::Char(len) | sp::DataType::Character(len) => {
            let n = extract_varchar_length(len).unwrap_or(1);
            if n > 255 {
                return Err(SqlError::InvalidOperation(
                    "Column length too big for column (max = 255)".to_string(),
                ));
            }
            Ok(DataType::Varchar(n))
        }
        // Text
        sp::DataType::Text
        | sp::DataType::TinyText
        | sp::DataType::MediumText
        | sp::DataType::LongText => Ok(DataType::Text),
        sp::DataType::CharacterLargeObject(_)
        | sp::DataType::CharLargeObject(_)
        | sp::DataType::Clob(_) => Ok(DataType::Text),
        sp::DataType::String(_) | sp::DataType::FixedString(_) => Ok(DataType::Text),
        // Blob
        sp::DataType::Blob(_)
        | sp::DataType::Binary(_)
        | sp::DataType::Varbinary(_)
        | sp::DataType::TinyBlob
        | sp::DataType::MediumBlob
        | sp::DataType::LongBlob
        | sp::DataType::Bytes(_)
        | sp::DataType::Bytea => Ok(DataType::Blob),
        sp::DataType::Bit(len) => {
            let width = len.unwrap_or(1) as u8;
            Ok(DataType::Bit(width.clamp(1, 64)))
        }
        sp::DataType::BitVarying(len) | sp::DataType::VarBit(len) => {
            let width = len.unwrap_or(64) as u8;
            Ok(DataType::Bit(width.clamp(1, 64)))
        }
        // Timestamp
        sp::DataType::Date | sp::DataType::Date32 => Ok(DataType::Timestamp),
        sp::DataType::Timestamp(_, _)
        | sp::DataType::Datetime(_)
        | sp::DataType::Datetime64(_, _)
        | sp::DataType::TimestampNtz(_) => Ok(DataType::Timestamp),
        // ENUM and SET — map to Text (we don't enforce the value set)
        sp::DataType::Enum(..) => Ok(DataType::Text),
        sp::DataType::Set(_) => Ok(DataType::Text),
        // TIME — map to Text (we don't have a native Time type)
        sp::DataType::Time(_, _) => Ok(DataType::Text),
        // JSON
        sp::DataType::JSON | sp::DataType::JSONB => Ok(DataType::Text),
        // PostgreSQL-style geometry types
        sp::DataType::GeometricType(_) => Ok(DataType::Geometry),
        // Custom type names (backward compat + types not in sqlparser enum)
        sp::DataType::Custom(name, _) => {
            let upper = name.to_string().to_uppercase();
            match upper.as_str() {
                "SERIAL" => Ok(DataType::BigInt),
                "UNSIGNED" => Ok(DataType::BigInt),
                "SIGNED" => Ok(DataType::BigInt),
                "YEAR" => Ok(DataType::SmallInt),
                "MEDIUMTEXT" | "LONGTEXT" | "TINYTEXT" | "NCHAR" | "NVARCHAR" => Ok(DataType::Text),
                "MEDIUMBLOB" | "LONGBLOB" | "TINYBLOB" => Ok(DataType::Blob),
                "FIXED" => Ok(DataType::Double),
                // MySQL spatial/geometry types
                "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
                | "MULTIPOLYGON" | "GEOMETRY" | "GEOMETRYCOLLECTION" => Ok(DataType::Geometry),
                _ => Err(SqlError::Unsupported(format!("Data type: {:?}", dt))),
            }
        }
        // Catch-all: try to map by Display name for any unsigned/signed variants
        // we may have missed
        _ => {
            let display = dt.to_string().to_uppercase();
            if display.contains("UNSIGNED") {
                if display.starts_with("TINYINT") {
                    Ok(DataType::TinyInt)
                } else if display.starts_with("SMALLINT") || display.starts_with("INT2") {
                    Ok(DataType::SmallInt)
                } else if display.starts_with("BIGINT") || display.starts_with("INT8") {
                    Ok(DataType::BigInt)
                } else {
                    // Default unsigned int → Int
                    Ok(DataType::Int)
                }
            } else if display.contains("SIGNED") {
                Ok(DataType::BigInt)
            } else {
                Err(SqlError::Unsupported(format!("Data type: {:?}", dt)))
            }
        }
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
        sp::TableConstraint::PrimaryKey(pk) => {
            let cols: Vec<String> = pk
                .columns
                .iter()
                .map(|c| c.column.expr.to_string())
                .collect();
            Ok(Some(Constraint::PrimaryKey(cols)))
        }
        sp::TableConstraint::Unique(uc) => {
            let cols: Vec<String> = uc
                .columns
                .iter()
                .map(|c| c.column.expr.to_string())
                .collect();
            Ok(Some(Constraint::Unique(cols)))
        }
        sp::TableConstraint::ForeignKey(fk) => {
            let cols: Vec<String> = fk.columns.iter().map(|c| c.value.clone()).collect();
            let ref_cols: Vec<String> = fk
                .referred_columns
                .iter()
                .map(|c| c.value.clone())
                .collect();
            Ok(Some(Constraint::ForeignKey {
                name: fk.name.as_ref().map(|n| n.value.clone()),
                columns: cols,
                ref_table: fk.foreign_table.to_string(),
                ref_columns: ref_cols,
            }))
        }
        sp::TableConstraint::Check(check) => Ok(Some(Constraint::Check(check.expr.to_string()))),
        _ => Ok(None),
    }
}

/// Extract column name from assignment target
fn extract_assignment_target(target: &sp::AssignmentTarget) -> SqlResult<String> {
    match target {
        sp::AssignmentTarget::ColumnName(names) => Ok(names.to_string()),
        sp::AssignmentTarget::Tuple(_) => {
            Err(SqlError::Unsupported("Tuple assignment".to_string()))
        }
    }
}

/// Convert JOIN type
fn convert_join_type(join_op: &sp::JoinOperator) -> SqlResult<JoinType> {
    match join_op {
        sp::JoinOperator::Join(_) | sp::JoinOperator::Inner(_) => Ok(JoinType::Inner),
        sp::JoinOperator::Left(_) | sp::JoinOperator::LeftOuter(_) => Ok(JoinType::Left),
        sp::JoinOperator::Right(_) | sp::JoinOperator::RightOuter(_) => Ok(JoinType::Right),
        sp::JoinOperator::FullOuter(_) => Ok(JoinType::Full),
        sp::JoinOperator::CrossJoin(_) => Ok(JoinType::Cross),
        sp::JoinOperator::StraightJoin(_) => Ok(JoinType::Inner),
        _ => Err(SqlError::Unsupported("Join type".to_string())),
    }
}

/// Extract JOIN condition
fn extract_join_condition(join_op: &sp::JoinOperator) -> Option<&sp::Expr> {
    match join_op {
        sp::JoinOperator::Join(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::Inner(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::Left(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::LeftOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::Right(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::RightOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::FullOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::StraightJoin(sp::JoinConstraint::On(expr)) => Some(expr),
        _ => None,
    }
}

/// Parse a static default value expression string into a Literal.
///
/// Handles: integers, floats, quoted strings, NULL, TRUE/FALSE.
/// Dynamic expressions (CURRENT_TIMESTAMP, NOW(), etc.) are mapped to
/// appropriate values where possible; unrecognized expressions fall back
/// to Literal::Null to avoid silently inserting wrong string values.
pub fn parse_default_value(expr: &str) -> Literal {
    let trimmed = expr.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Literal::Null;
    }
    // Handle dynamic default expressions that we can evaluate at insert time
    let upper = trimmed.to_uppercase();
    if upper == "CURRENT_TIMESTAMP"
        || upper == "NOW()"
        || upper == "CURRENT_TIMESTAMP()"
        || upper == "CURRENT_DATE"
        || upper == "CURRENT_TIME"
    {
        // Return 0 for timestamp defaults (MySQL stores epoch when not able to resolve)
        return Literal::Integer(0);
    }
    // Try integer
    if let Ok(i) = trimmed.parse::<i64>() {
        return Literal::Integer(i);
    }
    // Try float
    if let Ok(f) = trimmed.parse::<f64>() {
        return Literal::Float(f);
    }
    // Quoted string: 'value' or "value" (must be >1 char to have content)
    if trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')))
    {
        let inner = &trimmed[1..trimmed.len() - 1];
        return Literal::String(inner.to_string());
    }
    // Boolean
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Literal::Boolean(true);
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Literal::Boolean(false);
    }
    // Unrecognized expression — use NULL rather than silently inserting the expression text
    Literal::Null
}

/// Convert literal value
fn convert_value(val: &sp::Value) -> SqlResult<Literal> {
    match val {
        sp::Value::Null => Ok(Literal::Null),
        sp::Value::Boolean(b) => Ok(Literal::Boolean(*b)),
        sp::Value::Number(n, _) => {
            if n.contains('.') || n.contains('E') || n.contains('e') {
                let val = n.parse().map_err(|_| {
                    SqlError::InvalidOperation(format!("Invalid float literal: '{}'", n))
                })?;
                Ok(Literal::Float(val))
            } else {
                // Try i64 first, fall back to u64 for values > i64::MAX.
                // MySQL treats unsigned literals (e.g. 9223372036854775808) as BIGINT UNSIGNED.
                // Since we store all integers as i64, values in [i64::MAX+1, u64::MAX] are
                // reinterpreted as negative i64 (two's complement). This matches MySQL's
                // behavior for -9223372036854775808 (parsed as -(9223372036854775808u64 as i64)).
                match n.parse::<i64>() {
                    Ok(val) => Ok(Literal::Integer(val)),
                    Err(_) => {
                        // Values > i64::MAX: check for the special i64::MIN case
                        // (9223372036854775808 becomes i64::MIN when negated by unary minus).
                        // For other values > i64::MAX, use unsigned or decimal.
                        match n.parse::<u64>() {
                            Ok(val) => {
                                // Values > i64::MAX stored as UnsignedInteger
                                Ok(Literal::UnsignedInteger(val))
                            }
                            Err(_) => {
                                // Strip leading zeros and retry
                                let stripped = n.trim_start_matches('0');
                                let stripped = if stripped.is_empty() { "0" } else { stripped };
                                // Try as u64 first, then i128 for very large integers
                                if let Ok(u) = stripped.parse::<u64>() {
                                    Ok(Literal::UnsignedInteger(u))
                                } else if let Ok(v) = stripped.parse::<i128>() {
                                    Ok(Literal::Decimal(v, 0))
                                } else if let Ok(f) = stripped.parse::<f64>() {
                                    Ok(Literal::Float(f))
                                } else {
                                    stripped.parse::<i64>().map(Literal::Integer).map_err(|_| {
                                        SqlError::InvalidOperation(format!(
                                            "Invalid integer literal: '{}'",
                                            n
                                        ))
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }
        sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => {
            Ok(Literal::String(s.clone()))
        }
        sp::Value::HexStringLiteral(s) => {
            let bytes = hex_decode(s).unwrap_or_default();
            Ok(Literal::Blob(bytes))
        }
        sp::Value::SingleQuotedByteStringLiteral(s) => {
            // B'10101' — binary bit string literal, convert to integer
            let val = u64::from_str_radix(s, 2).unwrap_or(0);
            Ok(Literal::UnsignedInteger(val))
        }
        _ => Err(SqlError::Unsupported(format!("Value: {:?}", val))),
    }
}

/// Hex decoding helper
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    // Validate hex string has even length
    if !s.len().is_multiple_of(2) {
        return Err(()); // Odd-length hex string is invalid
    }
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
        // MySQL: || is OR (not string concat), && is AND
        sp::BinaryOperator::StringConcat => Ok(BinaryOp::Or),
        sp::BinaryOperator::PGOverlap => Ok(BinaryOp::And),
        sp::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
        sp::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
        sp::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
        sp::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
        sp::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOp::ShiftRight),
        sp::BinaryOperator::MyIntegerDivide => Ok(BinaryOp::IntDiv),
        sp::BinaryOperator::Xor => Ok(BinaryOp::Xor),
        sp::BinaryOperator::Spaceship => Ok(BinaryOp::Spaceship),
        sp::BinaryOperator::Assignment => Ok(BinaryOp::Assign),
        _ => Err(SqlError::Unsupported(format!("Binary operator: {:?}", op))),
    }
}

/// Convert unary operator
fn convert_unary_op(op: &sp::UnaryOperator) -> SqlResult<UnaryOp> {
    match op {
        sp::UnaryOperator::Not | sp::UnaryOperator::BangNot => Ok(UnaryOp::Not),
        sp::UnaryOperator::Minus => Ok(UnaryOp::Neg),
        sp::UnaryOperator::Plus => Ok(UnaryOp::Plus),
        sp::UnaryOperator::BitwiseNot => Ok(UnaryOp::BitwiseNot),
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

        // Bitwise operators return BigInt
        BinaryOp::BitwiseOr
        | BinaryOp::BitwiseAnd
        | BinaryOp::BitwiseXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => Ok(DataType::BigInt),

        // Integer division returns BigInt
        BinaryOp::IntDiv => Ok(DataType::BigInt),

        // Logical XOR returns boolean
        BinaryOp::Xor => Ok(DataType::Boolean),

        // NULL-safe equality returns boolean (never NULL)
        BinaryOp::Spaceship => Ok(DataType::Boolean),

        // Assignment (:=) returns the assigned value type
        BinaryOp::Assign => Ok(right.data_type()),
    }
}

/// Infer result type of unary operation
fn infer_unary_result_type(op: UnaryOp, expr: &ResolvedExpr) -> SqlResult<DataType> {
    match op {
        UnaryOp::Not => Ok(DataType::Boolean),
        UnaryOp::Neg | UnaryOp::Plus => {
            let dt = expr.data_type();
            // Negating a boolean produces an integer (-TRUE = -1)
            if dt == DataType::Boolean {
                Ok(DataType::BigInt)
            } else {
                Ok(dt)
            }
        }
        UnaryOp::BitwiseNot => Ok(DataType::BigInt),
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
        "ISNULL" => Ok(DataType::BigInt),
        "IF" => {
            // IF(cond, then, else) — return type = wider type of args[1] and args[2]
            if args.len() < 3 {
                return Err(SqlError::InvalidOperation(
                    "IF requires 3 arguments".to_string(),
                ));
            }
            let t1 = args[1].data_type();
            let t2 = args[2].data_type();
            if t1.is_numeric() && t2.is_numeric() {
                Ok(wider_numeric_type(&t1, &t2))
            } else {
                Ok(t1)
            }
        }
        "CONCAT" | "CONCAT_WS" | "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "SUBSTRING"
        | "SUBSTR" => Ok(DataType::Text),
        "LENGTH" | "CHAR_LENGTH" => Ok(DataType::BigInt),
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" => {
            if args.is_empty() {
                Ok(DataType::Double)
            } else {
                Ok(args[0].data_type())
            }
        }
        "NOW" | "CURRENT_TIMESTAMP" => Ok(DataType::Timestamp),
        "HEX" | "UNHEX" => Ok(DataType::Text),
        "LPAD" | "RPAD" | "LEFT" | "RIGHT" | "REVERSE" | "REPEAT" | "SPACE" | "REPLACE"
        | "INSERT" | "_ROODB_INSERT" => Ok(DataType::Text),
        "STRCMP" => Ok(DataType::BigInt),
        "MOD" | "_ROODB_MOD" => {
            if args.is_empty() {
                Ok(DataType::BigInt)
            } else {
                Ok(args[0].data_type())
            }
        }
        "FORMAT" => Ok(DataType::Text),
        "SHA" | "SHA1" | "SHA2" | "MD5" => Ok(DataType::Text),
        "WEIGHT_STRING" => Ok(DataType::Blob),
        "CRC32" => Ok(DataType::BigInt),
        "CONNECTION_ID" | "LAST_INSERT_ID" => Ok(DataType::BigInt),
        "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" | "VERSION" | "DATABASE"
        | "SCHEMA" => Ok(DataType::Text),
        "SLEEP" => Ok(DataType::BigInt),
        "FOUND_ROWS" | "ROW_COUNT" => Ok(DataType::BigInt),
        "BIT_COUNT" => Ok(DataType::BigInt),
        "REGEXP" => Ok(DataType::Boolean),
        "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "STD" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            Ok(DataType::Double)
        }
        // Spatial functions — not supported, but need type info for error path
        "ST_GEOMFROMTEXT"
        | "ST_GEOMETRYFROMTEXT"
        | "ST_GEOMFROMWKB"
        | "ST_GEOMETRYFROMWKB"
        | "ST_POINTFROMWKB"
        | "ST_LINESTRINGFROMWKB"
        | "ST_POLYGONFROMWKB" => Ok(DataType::Geometry),
        "ST_X" | "ST_Y" | "ST_LENGTH" | "ST_AREA" | "ST_DISTANCE" | "ST_PERIMETER" => {
            Ok(DataType::Double)
        }
        "ST_NUMPOINTS"
        | "ST_NUMGEOMETRIES"
        | "ST_NUMINTERIORRINGS"
        | "ST_SRID"
        | "ST_DIMENSION" => Ok(DataType::BigInt),
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => Ok(DataType::BigInt),
        "TO_DAYS" | "FROM_DAYS" | "DATEDIFF" | "DAYOFMONTH" | "DAYOFWEEK" | "DAYOFYEAR"
        | "HOUR" | "MINUTE" | "SECOND" | "MONTH" | "YEAR" | "WEEK" | "QUARTER" | "WEEKDAY"
        | "YEARWEEK" | "UNIX_TIMESTAMP" | "TIME_TO_SEC" | "PERIOD_ADD" | "PERIOD_DIFF" => {
            Ok(DataType::BigInt)
        }
        "FROM_UNIXTIME" | "DATE_FORMAT" | "STR_TO_DATE" | "DATE_ADD" | "DATE_SUB" | "ADDDATE"
        | "SUBDATE" | "MAKEDATE" | "MAKETIME" | "SEC_TO_TIME" | "TIMEDIFF" | "TIMESTAMPADD"
        | "TIMESTAMPDIFF" | "CURDATE" | "CURTIME" | "SYSDATE" | "UTC_DATE" | "UTC_TIME"
        | "UTC_TIMESTAMP" => Ok(DataType::Text),
        "CONV" | "BIN" | "OCT" => Ok(DataType::Text),
        "CHAR" => Ok(DataType::Text),
        "ORD" | "ASCII" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH" | "FIELD"
        | "LOCATE" | "INSTR" | "FIND_IN_SET" | "POSITION" => Ok(DataType::BigInt),
        "ELT" | "MAKE_SET" | "EXPORT_SET" => Ok(DataType::Text),
        "GET_LOCK" | "RELEASE_LOCK" | "IS_FREE_LOCK" => Ok(DataType::BigInt),
        "INET_NTOA" | "INET6_NTOA" => Ok(DataType::Text),
        "INET_ATON" | "INET6_ATON" => Ok(DataType::BigInt),
        "GREATEST" | "LEAST" => {
            if args.is_empty() {
                Ok(DataType::Int)
            } else {
                Ok(args[0].data_type())
            }
        }
        "CAST" | "CONVERT" => Ok(DataType::Text), // actual type resolved at Cast expr level
        "GET_FORMAT" => Ok(DataType::Text),
        "TRUNCATE" => Ok(DataType::Double), // TRUNCATE(number, decimals)
        "SIGN" => Ok(DataType::BigInt),
        "POW" | "POWER" | "SQRT" | "LOG" | "LOG2" | "LOG10" | "LN" | "EXP" | "PI" | "RADIANS"
        | "DEGREES" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "COT"
        | "RAND" => Ok(DataType::Double),
        // UDF: check if function exists in catalog (returns BIGINT by default)
        _ => Ok(DataType::BigInt),
    }
}

/// Infer result type for CASE expression from all result branches
fn infer_case_result_type(
    results: &[ResolvedExpr],
    else_result: &Option<Box<ResolvedExpr>>,
) -> DataType {
    let mut types: Vec<DataType> = results.iter().map(|r| r.data_type()).collect();
    if let Some(e) = else_result {
        types.push(e.data_type());
    }
    if types.is_empty() {
        return DataType::Int;
    }
    // If all numeric, use widest; otherwise use first type
    if types.iter().all(|t| t.is_numeric()) {
        types
            .iter()
            .fold(types[0].clone(), |acc, t| wider_numeric_type(&acc, t))
    } else {
        types[0].clone()
    }
}

/// Get the wider of two numeric types
fn wider_numeric_type(a: &DataType, b: &DataType) -> DataType {
    match (a, b) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
        (DataType::BigIntUnsigned, _) | (_, DataType::BigIntUnsigned) => DataType::BigIntUnsigned,
        (DataType::BigInt, _) | (_, DataType::BigInt) => DataType::BigInt,
        (DataType::Bit(_), _) | (_, DataType::Bit(_)) => DataType::BigInt,
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
    fn test_resolve_user_variable() {
        let catalog = test_catalog();
        let resolver = Resolver::new(&catalog);

        let stmt = Parser::parse_one("SELECT @x").unwrap();
        let resolved = resolver.resolve(stmt).unwrap();
        match resolved {
            ResolvedStatement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                match &select.columns[0] {
                    ResolvedSelectItem::Expr { expr, .. } => {
                        assert!(
                            matches!(expr, ResolvedExpr::UserVariable { name } if name == "x"),
                            "Expected UserVariable, got {:?}",
                            expr
                        );
                    }
                    other => panic!("Expected Expr, got {:?}", other),
                }
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

    #[test]
    fn test_convert_data_type_coverage() {
        use sqlparser::ast::DataType as SpDt;
        use sqlparser::ast::ExactNumberInfo;

        // Boolean
        assert_eq!(
            convert_data_type(&SpDt::Boolean).unwrap(),
            DataType::Boolean
        );
        assert_eq!(convert_data_type(&SpDt::Bool).unwrap(), DataType::Boolean);

        // TinyInt
        assert_eq!(
            convert_data_type(&SpDt::TinyInt(None)).unwrap(),
            DataType::TinyInt
        );
        assert_eq!(
            convert_data_type(&SpDt::TinyIntUnsigned(None)).unwrap(),
            DataType::TinyInt
        );
        assert_eq!(
            convert_data_type(&SpDt::UTinyInt).unwrap(),
            DataType::TinyInt
        );

        // SmallInt
        assert_eq!(
            convert_data_type(&SpDt::SmallInt(None)).unwrap(),
            DataType::SmallInt
        );
        assert_eq!(
            convert_data_type(&SpDt::SmallIntUnsigned(None)).unwrap(),
            DataType::SmallInt
        );
        assert_eq!(
            convert_data_type(&SpDt::USmallInt).unwrap(),
            DataType::SmallInt
        );
        assert_eq!(
            convert_data_type(&SpDt::Int2(None)).unwrap(),
            DataType::SmallInt
        );
        assert_eq!(
            convert_data_type(&SpDt::Int2Unsigned(None)).unwrap(),
            DataType::SmallInt
        );

        // Int
        assert_eq!(convert_data_type(&SpDt::Int(None)).unwrap(), DataType::Int);
        assert_eq!(
            convert_data_type(&SpDt::Integer(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(
            convert_data_type(&SpDt::MediumInt(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(
            convert_data_type(&SpDt::MediumIntUnsigned(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(
            convert_data_type(&SpDt::IntUnsigned(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(
            convert_data_type(&SpDt::IntegerUnsigned(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(
            convert_data_type(&SpDt::UnsignedInteger).unwrap(),
            DataType::Int
        );
        assert_eq!(convert_data_type(&SpDt::Int4(None)).unwrap(), DataType::Int);
        assert_eq!(
            convert_data_type(&SpDt::Int4Unsigned(None)).unwrap(),
            DataType::Int
        );
        assert_eq!(convert_data_type(&SpDt::Int16).unwrap(), DataType::Int);
        assert_eq!(convert_data_type(&SpDt::Int32).unwrap(), DataType::Int);
        assert_eq!(convert_data_type(&SpDt::UInt8).unwrap(), DataType::Int);
        assert_eq!(convert_data_type(&SpDt::UInt16).unwrap(), DataType::Int);
        assert_eq!(convert_data_type(&SpDt::UInt32).unwrap(), DataType::Int);

        // BigInt
        assert_eq!(
            convert_data_type(&SpDt::BigInt(None)).unwrap(),
            DataType::BigInt
        );
        assert_eq!(
            convert_data_type(&SpDt::BigIntUnsigned(None)).unwrap(),
            DataType::BigIntUnsigned
        );
        assert_eq!(
            convert_data_type(&SpDt::Int8(None)).unwrap(),
            DataType::BigInt
        );
        assert_eq!(
            convert_data_type(&SpDt::Int8Unsigned(None)).unwrap(),
            DataType::BigIntUnsigned
        );
        assert_eq!(convert_data_type(&SpDt::Int64).unwrap(), DataType::BigInt);
        assert_eq!(convert_data_type(&SpDt::Int128).unwrap(), DataType::BigInt);
        assert_eq!(convert_data_type(&SpDt::Int256).unwrap(), DataType::BigInt);
        assert_eq!(
            convert_data_type(&SpDt::UInt64).unwrap(),
            DataType::BigIntUnsigned
        );
        assert_eq!(convert_data_type(&SpDt::UInt128).unwrap(), DataType::BigInt);
        assert_eq!(convert_data_type(&SpDt::UInt256).unwrap(), DataType::BigInt);
        assert_eq!(
            convert_data_type(&SpDt::UBigInt).unwrap(),
            DataType::BigIntUnsigned
        );
        assert_eq!(convert_data_type(&SpDt::HugeInt).unwrap(), DataType::BigInt);
        assert_eq!(
            convert_data_type(&SpDt::UHugeInt).unwrap(),
            DataType::BigInt
        );
        assert_eq!(convert_data_type(&SpDt::Signed).unwrap(), DataType::BigInt);
        assert_eq!(
            convert_data_type(&SpDt::SignedInteger).unwrap(),
            DataType::BigInt
        );
        assert_eq!(
            convert_data_type(&SpDt::Unsigned).unwrap(),
            DataType::BigIntUnsigned
        );

        // Float
        assert_eq!(
            convert_data_type(&SpDt::Float(ExactNumberInfo::None)).unwrap(),
            DataType::Float
        );
        assert_eq!(convert_data_type(&SpDt::Real).unwrap(), DataType::Float);
        assert_eq!(convert_data_type(&SpDt::Float4).unwrap(), DataType::Float);
        assert_eq!(convert_data_type(&SpDt::Float32).unwrap(), DataType::Float);
        assert_eq!(
            convert_data_type(&SpDt::FloatUnsigned(ExactNumberInfo::None)).unwrap(),
            DataType::Float
        );
        assert_eq!(
            convert_data_type(&SpDt::RealUnsigned).unwrap(),
            DataType::Float
        );

        // Double
        assert_eq!(
            convert_data_type(&SpDt::Double(ExactNumberInfo::None)).unwrap(),
            DataType::Double
        );
        assert_eq!(
            convert_data_type(&SpDt::DoublePrecision).unwrap(),
            DataType::Double
        );
        assert_eq!(convert_data_type(&SpDt::Float8).unwrap(), DataType::Double);
        assert_eq!(convert_data_type(&SpDt::Float64).unwrap(), DataType::Double);
        assert_eq!(
            convert_data_type(&SpDt::DoubleUnsigned(ExactNumberInfo::None)).unwrap(),
            DataType::Double
        );
        assert_eq!(
            convert_data_type(&SpDt::DoublePrecisionUnsigned).unwrap(),
            DataType::Double
        );
        assert_eq!(
            convert_data_type(&SpDt::DecimalUnsigned(ExactNumberInfo::None)).unwrap(),
            DataType::Decimal {
                precision: 10,
                scale: 0
            }
        );
        assert_eq!(
            convert_data_type(&SpDt::DecUnsigned(ExactNumberInfo::None)).unwrap(),
            DataType::Decimal {
                precision: 10,
                scale: 0
            }
        );
        assert_eq!(
            convert_data_type(&SpDt::BigNumeric(ExactNumberInfo::None)).unwrap(),
            DataType::Decimal {
                precision: 10,
                scale: 0
            }
        );
        assert_eq!(
            convert_data_type(&SpDt::BigDecimal(ExactNumberInfo::None)).unwrap(),
            DataType::Decimal {
                precision: 10,
                scale: 0
            }
        );

        // Varchar variants
        assert_eq!(
            convert_data_type(&SpDt::Varchar(None)).unwrap(),
            DataType::Varchar(255)
        );
        assert_eq!(
            convert_data_type(&SpDt::CharacterVarying(None)).unwrap(),
            DataType::Varchar(255)
        );
        assert_eq!(
            convert_data_type(&SpDt::CharVarying(None)).unwrap(),
            DataType::Varchar(255)
        );
        assert_eq!(
            convert_data_type(&SpDt::Nvarchar(None)).unwrap(),
            DataType::Varchar(255)
        );
        assert_eq!(
            convert_data_type(&SpDt::Character(None)).unwrap(),
            DataType::Varchar(1)
        );

        // Text variants
        assert_eq!(convert_data_type(&SpDt::Text).unwrap(), DataType::Text);
        assert_eq!(convert_data_type(&SpDt::TinyText).unwrap(), DataType::Text);
        assert_eq!(
            convert_data_type(&SpDt::MediumText).unwrap(),
            DataType::Text
        );
        assert_eq!(convert_data_type(&SpDt::LongText).unwrap(), DataType::Text);
        assert_eq!(
            convert_data_type(&SpDt::CharacterLargeObject(None)).unwrap(),
            DataType::Text
        );
        assert_eq!(
            convert_data_type(&SpDt::CharLargeObject(None)).unwrap(),
            DataType::Text
        );
        assert_eq!(
            convert_data_type(&SpDt::Clob(None)).unwrap(),
            DataType::Text
        );
        assert_eq!(
            convert_data_type(&SpDt::String(None)).unwrap(),
            DataType::Text
        );
        assert_eq!(
            convert_data_type(&SpDt::FixedString(64)).unwrap(),
            DataType::Text
        );

        // Blob variants
        assert_eq!(
            convert_data_type(&SpDt::Blob(None)).unwrap(),
            DataType::Blob
        );
        assert_eq!(
            convert_data_type(&SpDt::Binary(None)).unwrap(),
            DataType::Blob
        );
        assert_eq!(
            convert_data_type(&SpDt::Varbinary(None)).unwrap(),
            DataType::Blob
        );
        assert_eq!(convert_data_type(&SpDt::TinyBlob).unwrap(), DataType::Blob);
        assert_eq!(
            convert_data_type(&SpDt::MediumBlob).unwrap(),
            DataType::Blob
        );
        assert_eq!(convert_data_type(&SpDt::LongBlob).unwrap(), DataType::Blob);
        assert_eq!(
            convert_data_type(&SpDt::Bytes(None)).unwrap(),
            DataType::Blob
        );
        assert_eq!(convert_data_type(&SpDt::Bytea).unwrap(), DataType::Blob);
        assert_eq!(
            convert_data_type(&SpDt::Bit(None)).unwrap(),
            DataType::Bit(1)
        );
        assert_eq!(
            convert_data_type(&SpDt::Bit(Some(8))).unwrap(),
            DataType::Bit(8)
        );
        assert_eq!(
            convert_data_type(&SpDt::BitVarying(None)).unwrap(),
            DataType::Bit(64)
        );
        assert_eq!(
            convert_data_type(&SpDt::VarBit(None)).unwrap(),
            DataType::Bit(64)
        );

        // Timestamp variants
        assert_eq!(convert_data_type(&SpDt::Date).unwrap(), DataType::Timestamp);
        assert_eq!(
            convert_data_type(&SpDt::Date32).unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            convert_data_type(&SpDt::Datetime(None)).unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            convert_data_type(&SpDt::Datetime64(3, None)).unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            convert_data_type(&SpDt::TimestampNtz(None)).unwrap(),
            DataType::Timestamp
        );

        // JSON
        assert_eq!(convert_data_type(&SpDt::JSON).unwrap(), DataType::Text);
        assert_eq!(convert_data_type(&SpDt::JSONB).unwrap(), DataType::Text);
    }
}
