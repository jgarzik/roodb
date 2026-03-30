//! Trigger execution engine
//!
//! Handles firing triggers during DML operations. Trigger bodies are stored as
//! parsed sqlparser AST statements. At fire time, NEW/OLD column references are
//! substituted with literal values, then the body is resolved and executed
//! through the normal SQL pipeline.

use std::collections::HashMap;

use sqlparser::ast as sp;

/// Substitute NEW.column references in a sqlparser Statement with literal values.
///
/// Walks the AST and replaces CompoundIdentifier(["NEW", col]) with the
/// corresponding literal value from the `new_values` map.
pub fn substitute_new_refs(
    stmt: &sp::Statement,
    new_values: &HashMap<String, sp::Value>,
) -> sp::Statement {
    let mut stmt = stmt.clone();
    substitute_stmt(&mut stmt, new_values);
    stmt
}

fn substitute_stmt(stmt: &mut sp::Statement, new_values: &HashMap<String, sp::Value>) {
    match stmt {
        sp::Statement::Insert(insert) => {
            if let Some(ref mut source) = insert.source {
                substitute_set_expr(&mut source.body, new_values);
            }
        }
        sp::Statement::Update(update) => {
            for assignment in &mut update.assignments {
                substitute_expr(&mut assignment.value, new_values);
            }
            if let Some(ref mut sel) = update.selection {
                substitute_expr(sel, new_values);
            }
        }
        sp::Statement::Delete(delete) => {
            if let Some(ref mut sel) = delete.selection {
                substitute_expr(sel, new_values);
            }
        }
        sp::Statement::Set(set) => match set {
            sp::Set::SingleAssignment { values, .. } => {
                for v in values {
                    substitute_expr(v, new_values);
                }
            }
            sp::Set::MultipleAssignments { assignments } => {
                for a in assignments {
                    substitute_expr(&mut a.value, new_values);
                }
            }
            _ => {}
        },
        _ => {}
    }
}

fn substitute_set_expr(body: &mut sp::SetExpr, new_values: &HashMap<String, sp::Value>) {
    match body {
        sp::SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    substitute_expr(expr, new_values);
                }
            }
        }
        sp::SetExpr::Select(select) => {
            for item in &mut select.projection {
                if let sp::SelectItem::UnnamedExpr(ref mut expr)
                | sp::SelectItem::ExprWithAlias { ref mut expr, .. } = item
                {
                    substitute_expr(expr, new_values);
                }
            }
            if let Some(ref mut sel) = select.selection {
                substitute_expr(sel, new_values);
            }
        }
        sp::SetExpr::SetOperation { left, right, .. } => {
            substitute_set_expr(left, new_values);
            substitute_set_expr(right, new_values);
        }
        sp::SetExpr::Query(q) => {
            substitute_set_expr(&mut q.body, new_values);
        }
        _ => {}
    }
}

fn substitute_expr(expr: &mut sp::Expr, new_values: &HashMap<String, sp::Value>) {
    match expr {
        sp::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("NEW") {
                let col_name = parts[1].value.clone();
                if let Some(val) = new_values.get(&col_name) {
                    *expr = sp::Expr::value(val.clone());
                }
            }
        }
        sp::Expr::Function(func) => {
            if let sp::FunctionArguments::List(ref mut list) = func.args {
                for arg in &mut list.args {
                    if let sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(ref mut e)) = arg {
                        substitute_expr(e, new_values);
                    }
                }
            }
        }
        sp::Expr::BinaryOp { left, right, .. } => {
            substitute_expr(left, new_values);
            substitute_expr(right, new_values);
        }
        sp::Expr::UnaryOp { expr: inner, .. } => {
            substitute_expr(inner, new_values);
        }
        sp::Expr::Nested(inner) => {
            substitute_expr(inner, new_values);
        }
        sp::Expr::Cast { expr: inner, .. } => {
            substitute_expr(inner, new_values);
        }
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(ref mut op) = operand {
                substitute_expr(op, new_values);
            }
            for cw in conditions {
                substitute_expr(&mut cw.condition, new_values);
                substitute_expr(&mut cw.result, new_values);
            }
            if let Some(ref mut e) = else_result {
                substitute_expr(e, new_values);
            }
        }
        sp::Expr::InList {
            expr: inner, list, ..
        } => {
            substitute_expr(inner, new_values);
            for item in list {
                substitute_expr(item, new_values);
            }
        }
        sp::Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            substitute_expr(inner, new_values);
            substitute_expr(low, new_values);
            substitute_expr(high, new_values);
        }
        sp::Expr::IsNull(inner) | sp::Expr::IsNotNull(inner) => {
            substitute_expr(inner, new_values);
        }
        _ => {}
    }
}

/// Convert a Datum to a sqlparser Value for AST substitution
pub fn datum_to_sp_value(datum: &crate::executor::datum::Datum) -> sp::Value {
    use crate::executor::datum::Datum;
    match datum {
        Datum::Null => sp::Value::Null,
        Datum::Bool(b) => {
            if *b {
                sp::Value::Boolean(true)
            } else {
                sp::Value::Boolean(false)
            }
        }
        Datum::Int(i) => sp::Value::Number(i.to_string(), false),
        Datum::UnsignedInt(u) => sp::Value::Number(u.to_string(), false),
        Datum::Float(f) => sp::Value::Number(format!("{}", f), false),
        Datum::String(s) => sp::Value::SingleQuotedString(s.clone()),
        Datum::Decimal { value, scale } => {
            if *scale == 0 {
                sp::Value::Number(value.to_string(), false)
            } else {
                let divisor = 10i128.pow(*scale as u32);
                let int_part = value / divisor;
                let frac_part = (value % divisor).unsigned_abs();
                sp::Value::Number(
                    format!(
                        "{}.{:0>width$}",
                        int_part,
                        frac_part,
                        width = *scale as usize
                    ),
                    false,
                )
            }
        }
        Datum::Bytes(b) | Datum::Geometry(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            sp::Value::HexStringLiteral(hex)
        }
        Datum::Bit { value, .. } => sp::Value::Number(value.to_string(), false),
        Datum::Timestamp(ts) => sp::Value::Number(ts.to_string(), false),
        Datum::Json(v) => {
            sp::Value::SingleQuotedString(serde_json::to_string(v).unwrap_or_default())
        }
    }
}
