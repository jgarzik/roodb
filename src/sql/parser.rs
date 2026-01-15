//! SQL parser wrapper around sqlparser crate

use sqlparser::ast as sp;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::catalog::{ColumnDef, Constraint, DataType};
use crate::sql::ast::*;
use crate::sql::error::{SqlError, SqlResult};

/// SQL parser
pub struct Parser;

impl Parser {
    /// Parse a single SQL statement
    pub fn parse_one(sql: &str) -> SqlResult<Statement> {
        let dialect = MySqlDialect {};
        let ast = SqlParser::parse_sql(&dialect, sql)?;

        if ast.is_empty() {
            return Err(SqlError::Parse("Empty SQL statement".to_string()));
        }
        if ast.len() > 1 {
            return Err(SqlError::Parse(
                "Multiple statements not supported".to_string(),
            ));
        }

        convert_statement(&ast[0])
    }
}

/// Convert sqlparser statement to internal AST
fn convert_statement(stmt: &sp::Statement) -> SqlResult<Statement> {
    match stmt {
        sp::Statement::CreateTable(create) => convert_create_table(create),
        sp::Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => convert_drop(object_type, names, *if_exists),
        sp::Statement::CreateIndex(create_index) => convert_create_index(create_index),
        sp::Statement::Insert(insert) => convert_insert(insert),
        sp::Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => convert_update(table, assignments, selection),
        sp::Statement::Delete(delete) => convert_delete(delete),
        sp::Statement::Query(query) => convert_query(query),
        _ => Err(SqlError::Unsupported(format!(
            "Statement type: {:?}",
            stmt
        ))),
    }
}

/// Convert CREATE TABLE
fn convert_create_table(create: &sp::CreateTable) -> SqlResult<Statement> {
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

    Ok(Statement::CreateTable {
        name,
        columns,
        constraints,
        if_not_exists,
    })
}

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
fn convert_data_type(dt: &sp::DataType) -> SqlResult<DataType> {
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
            let ref_cols: Vec<String> =
                referred_columns.iter().map(|c| c.value.clone()).collect();
            Ok(Some(Constraint::ForeignKey {
                columns: cols,
                ref_table: foreign_table.to_string(),
                ref_columns: ref_cols,
            }))
        }
        sp::TableConstraint::Check { expr, .. } => {
            Ok(Some(Constraint::Check(expr.to_string())))
        }
        _ => Ok(None),
    }
}

/// Convert DROP statement
fn convert_drop(
    object_type: &sp::ObjectType,
    names: &[sp::ObjectName],
    if_exists: bool,
) -> SqlResult<Statement> {
    if names.is_empty() {
        return Err(SqlError::Parse("DROP requires a name".to_string()));
    }
    let name = names[0].to_string();

    match object_type {
        sp::ObjectType::Table => Ok(Statement::DropTable { name, if_exists }),
        sp::ObjectType::Index => Ok(Statement::DropIndex { name }),
        _ => Err(SqlError::Unsupported(format!("DROP {:?}", object_type))),
    }
}

/// Convert CREATE INDEX
fn convert_create_index(create: &sp::CreateIndex) -> SqlResult<Statement> {
    let name = create
        .name
        .as_ref()
        .map(|n| n.to_string())
        .ok_or_else(|| SqlError::Parse("CREATE INDEX requires a name".to_string()))?;

    let table = create.table_name.to_string();
    let unique = create.unique;

    let columns: Vec<String> = create.columns.iter().map(|c| c.expr.to_string()).collect();

    Ok(Statement::CreateIndex {
        name,
        table,
        columns,
        unique,
    })
}

/// Convert INSERT
fn convert_insert(insert: &sp::Insert) -> SqlResult<Statement> {
    let table = insert.table_name.to_string();

    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(|c| c.value.clone()).collect())
    };

    let values = match insert.source.as_ref().map(|s| s.body.as_ref()) {
        Some(sp::SetExpr::Values(sp::Values { rows, .. })) => {
            let mut result = Vec::new();
            for row in rows {
                let mut row_exprs = Vec::new();
                for expr in row {
                    row_exprs.push(convert_expr(expr)?);
                }
                result.push(row_exprs);
            }
            result
        }
        _ => return Err(SqlError::Unsupported("INSERT without VALUES".to_string())),
    };

    Ok(Statement::Insert {
        table,
        columns,
        values,
    })
}

/// Convert UPDATE
fn convert_update(
    table: &sp::TableWithJoins,
    assignments: &[sp::Assignment],
    selection: &Option<sp::Expr>,
) -> SqlResult<Statement> {
    let table_name = match &table.relation {
        sp::TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(SqlError::Unsupported("Complex UPDATE table".to_string())),
    };

    let mut assigns = Vec::new();
    for a in assignments {
        let column = extract_assignment_target(&a.target)?;
        let value = convert_expr(&a.value)?;
        assigns.push(Assignment { column, value });
    }

    let filter = selection.as_ref().map(convert_expr).transpose()?;

    Ok(Statement::Update {
        table: table_name,
        assignments: assigns,
        filter,
    })
}

/// Extract column name from assignment target
fn extract_assignment_target(target: &sp::AssignmentTarget) -> SqlResult<String> {
    match target {
        sp::AssignmentTarget::ColumnName(names) => {
            Ok(names.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        sp::AssignmentTarget::Tuple(_) => {
            Err(SqlError::Unsupported("Tuple assignment".to_string()))
        }
    }
}

/// Convert DELETE
fn convert_delete(delete: &sp::Delete) -> SqlResult<Statement> {
    let table = match &delete.from {
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

    let filter = delete.selection.as_ref().map(convert_expr).transpose()?;

    Ok(Statement::Delete { table, filter })
}

/// Convert SELECT query
fn convert_query(query: &sp::Query) -> SqlResult<Statement> {
    let select = convert_select_body(query.body.as_ref())?;

    let mut result = select;

    // Handle ORDER BY
    if let Some(order_by) = &query.order_by {
        for item in &order_by.exprs {
            result.order_by.push(OrderByItem {
                expr: convert_expr(&item.expr)?,
                ascending: item.asc.unwrap_or(true),
                nulls_first: item.nulls_first,
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

    Ok(Statement::Select(result))
}

/// Convert SELECT body
fn convert_select_body(body: &sp::SetExpr) -> SqlResult<SelectStatement> {
    match body {
        sp::SetExpr::Select(select) => {
            let mut result = SelectStatement {
                distinct: select.distinct.is_some(),
                ..Default::default()
            };

            // Convert projection (SELECT items)
            for item in &select.projection {
                result.columns.push(convert_select_item(item)?);
            }

            // Convert FROM clause
            for table in &select.from {
                result.from.push(convert_table_ref(&table.relation)?);
                for join in &table.joins {
                    result.joins.push(convert_join(join)?);
                }
            }

            // Convert WHERE
            result.filter = select.selection.as_ref().map(convert_expr).transpose()?;

            // Convert GROUP BY
            match &select.group_by {
                sp::GroupByExpr::Expressions(exprs, _) => {
                    for expr in exprs {
                        result.group_by.push(convert_expr(expr)?);
                    }
                }
                sp::GroupByExpr::All(_) => {
                    return Err(SqlError::Unsupported("GROUP BY ALL".to_string()));
                }
            }

            // Convert HAVING
            result.having = select.having.as_ref().map(convert_expr).transpose()?;

            Ok(result)
        }
        _ => Err(SqlError::Unsupported(
            "Complex query (UNION, etc.)".to_string(),
        )),
    }
}

/// Convert SELECT item
fn convert_select_item(item: &sp::SelectItem) -> SqlResult<SelectItem> {
    match item {
        sp::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        sp::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: Some(alias.value.clone()),
        }),
        sp::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
        sp::SelectItem::QualifiedWildcard(name, _) => {
            Ok(SelectItem::QualifiedWildcard(name.to_string()))
        }
    }
}

/// Convert table reference
fn convert_table_ref(table: &sp::TableFactor) -> SqlResult<TableRef> {
    match table {
        sp::TableFactor::Table { name, alias, .. } => Ok(TableRef {
            name: name.to_string(),
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        }),
        _ => Err(SqlError::Unsupported("Complex table reference".to_string())),
    }
}

/// Convert JOIN
fn convert_join(join: &sp::Join) -> SqlResult<Join> {
    let table = convert_table_ref(&join.relation)?;

    let join_type = match &join.join_operator {
        sp::JoinOperator::Inner(_) => JoinType::Inner,
        sp::JoinOperator::LeftOuter(_) => JoinType::Left,
        sp::JoinOperator::RightOuter(_) => JoinType::Right,
        sp::JoinOperator::FullOuter(_) => JoinType::Full,
        sp::JoinOperator::CrossJoin => JoinType::Cross,
        _ => return Err(SqlError::Unsupported("Join type".to_string())),
    };

    let condition = match &join.join_operator {
        sp::JoinOperator::Inner(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::LeftOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::RightOuter(sp::JoinConstraint::On(expr))
        | sp::JoinOperator::FullOuter(sp::JoinConstraint::On(expr)) => Some(convert_expr(expr)?),
        _ => None,
    };

    Ok(Join {
        table,
        join_type,
        condition,
    })
}

/// Convert expression
fn convert_expr(expr: &sp::Expr) -> SqlResult<Expr> {
    match expr {
        sp::Expr::Identifier(ident) => Ok(Expr::Column {
            table: None,
            name: ident.value.clone(),
        }),
        sp::Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expr::Column {
                    table: Some(idents[0].value.clone()),
                    name: idents[1].value.clone(),
                })
            } else {
                Err(SqlError::Unsupported("Compound identifier".to_string()))
            }
        }
        sp::Expr::Value(val) => Ok(Expr::Literal(convert_value(val)?)),
        sp::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),
        sp::Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: convert_unary_op(op)?,
            expr: Box::new(convert_expr(expr)?),
        }),
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
                                result.push(convert_expr(e)?);
                            }
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => {
                                result.push(Expr::Wildcard);
                            }
                            _ => {
                                return Err(SqlError::Unsupported("Function argument".to_string()))
                            }
                        }
                    }
                    result
                }
                sp::FunctionArguments::None => Vec::new(),
                _ => return Err(SqlError::Unsupported("Function arguments".to_string())),
            };
            Ok(Expr::Function {
                name,
                args,
                distinct,
            })
        }
        sp::Expr::IsNull(expr) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(expr)?),
            negated: false,
        }),
        sp::Expr::IsNotNull(expr) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(expr)?),
            negated: true,
        }),
        sp::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(convert_expr(expr)?),
            list: list
                .iter()
                .map(convert_expr)
                .collect::<SqlResult<Vec<_>>>()?,
            negated: *negated,
        }),
        sp::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(expr)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),
        sp::Expr::Nested(inner) => convert_expr(inner),
        sp::Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(expr)?),
            op: if *negated {
                BinaryOp::NotLike
            } else {
                BinaryOp::Like
            },
            right: Box::new(convert_expr(pattern)?),
        }),
        _ => Err(SqlError::Unsupported(format!("Expression: {:?}", expr))),
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

/// Hex decoding helper
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let stmt = Parser::parse_one("SELECT id, name FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert_eq!(select.from.len(), 1);
                assert_eq!(select.from[0].name, "users");
                assert!(select.filter.is_some());
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = Parser::parse_one("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        match stmt {
            Statement::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "users");
                assert_eq!(columns.unwrap(), vec!["id", "name"]);
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 2);
            }
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = Parser::parse_one(
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))",
        )
        .unwrap();
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
                assert!(!columns[0].nullable);
                assert!(columns[1].nullable);
                assert_eq!(constraints.len(), 1);
            }
            _ => panic!("Expected CREATE TABLE"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = Parser::parse_one("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update {
                table,
                assignments,
                filter,
            } => {
                assert_eq!(table, "users");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "name");
                assert!(filter.is_some());
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = Parser::parse_one("DELETE FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete { table, filter } => {
                assert_eq!(table, "users");
                assert!(filter.is_some());
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn test_parse_join() {
        let stmt = Parser::parse_one(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.from.len(), 1);
                assert_eq!(select.joins.len(), 1);
                assert_eq!(select.joins[0].join_type, JoinType::Inner);
            }
            _ => panic!("Expected SELECT"),
        }
    }
}
