//! Stored procedure interpreter
//!
//! Tree-walking interpreter over sqlparser AST for stored procedure bodies.
//! Each DML/DDL in the body re-enters the existing SQL pipeline.
//! Control flow (IF/WHILE/CASE) and variable ops (DECLARE/SET/FETCH) are
//! handled directly by the interpreter.

use std::collections::HashMap;

use crate::catalog::{ParamMode, ProcedureDef};
use crate::executor::datum::Datum;
use crate::executor::eval::{eval_binary_op, eval_function, eval_unary_op};
use crate::executor::row::Row;
use crate::planner::logical::{BinaryOp, UnaryOp};
use crate::server::session::UserVariables;

/// Control flow signal from procedure body execution
pub enum ProcControlFlow {
    Continue,
    Return,
}

/// State of a declared cursor
pub enum CursorState {
    /// Cursor declared but not yet opened
    Declared { query_sql: String },
    /// Cursor opened with buffered rows
    Open { rows: Vec<Row>, position: usize },
    /// Cursor has been closed
    Closed,
}

/// Per-execution context for a stored procedure
pub struct ProcedureContext {
    /// DECLARE'd variables and procedure parameters
    pub locals: HashMap<String, Datum>,
    /// Declared cursors
    pub cursors: HashMap<String, CursorState>,
    /// Whether last FETCH found a row (true = found, false = exhausted)
    pub found: bool,
    /// Total rows affected by DML within the procedure
    pub rows_affected: u64,
}

impl Default for ProcedureContext {
    fn default() -> Self {
        Self {
            locals: HashMap::new(),
            cursors: HashMap::new(),
            found: true,
            rows_affected: 0,
        }
    }
}

impl ProcedureContext {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Evaluate a sqlparser Expr in the context of procedure local variables and user variables.
/// This is a direct evaluator that doesn't go through the full resolve/plan pipeline.
pub fn eval_sp_expr(
    expr: &sqlparser::ast::Expr,
    ctx: &ProcedureContext,
    user_vars: &UserVariables,
) -> Result<Datum, String> {
    use sqlparser::ast::Expr;
    use sqlparser::ast::UnaryOperator;
    use sqlparser::ast::Value;

    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Datum::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Datum::Float(f))
                } else {
                    Ok(Datum::String(n.clone()))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(Datum::String(s.clone()))
            }
            Value::Boolean(b) => Ok(Datum::Bool(*b)),
            Value::Null => Ok(Datum::Null),
            _ => Ok(Datum::String(v.to_string())),
        },

        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            // User variables (@var): strip leading @ for lookup (session
            // store keys are stored without the prefix)
            if let Some(var_name) = name.strip_prefix('@') {
                if !var_name.starts_with('@') {
                    // Single @: user variable
                    let vars = user_vars.read();
                    return Ok(vars.get(var_name).cloned().unwrap_or(Datum::Null));
                }
            }
            // Check locals first, then user variables by bare name
            if let Some(val) = ctx.locals.get(&name) {
                return Ok(val.clone());
            }
            let vars = user_vars.read();
            if let Some(val) = vars.get(&name) {
                return Ok(val.clone());
            }
            // NULL for undefined variables in procedure context
            Ok(Datum::Null)
        }

        Expr::CompoundIdentifier(parts) => {
            // Handle @var as compound identifier
            let full_name = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            let name = full_name.to_lowercase();
            if let Some(val) = ctx.locals.get(&name) {
                return Ok(val.clone());
            }
            Ok(Datum::Null)
        }

        // Handle @user_variable
        Expr::AtTimeZone { .. } => Ok(Datum::Null), // not relevant

        // BinaryOp
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator;

            // Handle assignment (:=)
            if matches!(op, BinaryOperator::DuckIntegerDivide) {
                // This shouldn't happen for :=, but handle normally
            }

            let lval = eval_sp_expr(left, ctx, user_vars)?;
            let rval = eval_sp_expr(right, ctx, user_vars)?;

            let resolved_op = match op {
                BinaryOperator::Plus => BinaryOp::Add,
                BinaryOperator::Minus => BinaryOp::Sub,
                BinaryOperator::Multiply => BinaryOp::Mul,
                BinaryOperator::Divide => BinaryOp::Div,
                BinaryOperator::Modulo => BinaryOp::Mod,
                BinaryOperator::Eq => BinaryOp::Eq,
                BinaryOperator::NotEq => BinaryOp::NotEq,
                BinaryOperator::Lt => BinaryOp::Lt,
                BinaryOperator::LtEq => BinaryOp::LtEq,
                BinaryOperator::Gt => BinaryOp::Gt,
                BinaryOperator::GtEq => BinaryOp::GtEq,
                BinaryOperator::And => BinaryOp::And,
                BinaryOperator::Or => BinaryOp::Or,
                _ => return Err(format!("Unsupported binary operator in procedure: {}", op)),
            };
            eval_binary_op(&resolved_op, &lval, &rval).map_err(|e| e.to_string())
        }

        // UnaryOp
        Expr::UnaryOp { op, expr } => {
            let val = eval_sp_expr(expr, ctx, user_vars)?;
            let resolved_op = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not => UnaryOp::Not,
                UnaryOperator::Plus => return Ok(val),
                _ => return Err(format!("Unsupported unary operator in procedure: {}", op)),
            };
            eval_unary_op(&resolved_op, &val).map_err(|e| e.to_string())
        }

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let val = eval_sp_expr(inner, ctx, user_vars)?;
            Ok(Datum::Bool(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_sp_expr(inner, ctx, user_vars)?;
            Ok(Datum::Bool(!val.is_null()))
        }

        // Function call
        Expr::Function(func) => {
            let name = func.name.to_string();
            let args: Vec<Datum> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    let mut vals = Vec::new();
                    for arg in &arg_list.args {
                        match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => {
                                vals.push(eval_sp_expr(e, ctx, user_vars)?);
                            }
                            sqlparser::ast::FunctionArg::Named {
                                arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                                ..
                            } => {
                                vals.push(eval_sp_expr(e, ctx, user_vars)?);
                            }
                            _ => {}
                        }
                    }
                    vals
                }
                sqlparser::ast::FunctionArguments::None => vec![],
                sqlparser::ast::FunctionArguments::Subquery(_) => {
                    return Err("Subquery function args not supported in procedures".to_string());
                }
            };
            eval_function(&name, &args, None).map_err(|e| e.to_string())
        }

        // Nested expression
        Expr::Nested(inner) => eval_sp_expr(inner, ctx, user_vars),

        // BETWEEN
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let val = eval_sp_expr(expr, ctx, user_vars)?;
            let lo = eval_sp_expr(low, ctx, user_vars)?;
            let hi = eval_sp_expr(high, ctx, user_vars)?;
            let ge_lo = eval_binary_op(&BinaryOp::GtEq, &val, &lo).map_err(|e| e.to_string())?;
            let le_hi = eval_binary_op(&BinaryOp::LtEq, &val, &hi).map_err(|e| e.to_string())?;
            let result =
                eval_binary_op(&BinaryOp::And, &ge_lo, &le_hi).map_err(|e| e.to_string())?;
            if *negated {
                match result {
                    Datum::Bool(b) => Ok(Datum::Bool(!b)),
                    _ => Ok(result),
                }
            } else {
                Ok(result)
            }
        }

        // IN list
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_sp_expr(expr, ctx, user_vars)?;
            if val.is_null() {
                return Ok(Datum::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = eval_sp_expr(item, ctx, user_vars)?;
                if let Ok(Datum::Bool(true)) = eval_binary_op(&BinaryOp::Eq, &val, &item_val) {
                    found = true;
                    break;
                }
            }
            Ok(Datum::Bool(if *negated { !found } else { found }))
        }

        // CASE expression
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                let op_val = eval_sp_expr(operand, ctx, user_vars)?;
                for case_when in conditions {
                    let cond_val = eval_sp_expr(&case_when.condition, ctx, user_vars)?;
                    if let Ok(Datum::Bool(true)) = eval_binary_op(&BinaryOp::Eq, &op_val, &cond_val)
                    {
                        return eval_sp_expr(&case_when.result, ctx, user_vars);
                    }
                }
            } else {
                for case_when in conditions {
                    let cond_val = eval_sp_expr(&case_when.condition, ctx, user_vars)?;
                    if datum_is_true(&cond_val) {
                        return eval_sp_expr(&case_when.result, ctx, user_vars);
                    }
                }
            }
            if let Some(else_expr) = else_result {
                eval_sp_expr(else_expr, ctx, user_vars)
            } else {
                Ok(Datum::Null)
            }
        }

        // Handle other expression types we can stringify
        _ => {
            // Fallback: try to evaluate as a resolved expression through the normal path
            // For now, just produce a string representation
            Err(format!(
                "Unsupported expression in procedure context: {}",
                expr
            ))
        }
    }
}

/// Check if a Datum represents a truthy value
pub fn datum_is_true(d: &Datum) -> bool {
    match d {
        Datum::Bool(b) => *b,
        Datum::Int(i) => *i != 0,
        Datum::Float(f) => *f != 0.0,
        Datum::Null => false,
        Datum::String(s) => !s.is_empty() && s != "0",
        _ => false,
    }
}

/// Build a ProcedureContext from a ProcedureDef and call arguments.
/// Evaluates IN/INOUT argument expressions and binds them to parameters.
pub fn build_procedure_context(
    proc_def: &ProcedureDef,
    call_args: &[sqlparser::ast::Expr],
    user_vars: &UserVariables,
) -> Result<ProcedureContext, String> {
    if call_args.len() != proc_def.params.len() {
        return Err(format!(
            "Procedure '{}' expects {} arguments, got {}",
            proc_def.name,
            proc_def.params.len(),
            call_args.len()
        ));
    }

    let mut ctx = ProcedureContext::new();

    // Empty context for evaluating call args (no locals yet)
    let eval_ctx = ProcedureContext::new();

    for (param, arg_expr) in proc_def.params.iter().zip(call_args.iter()) {
        let param_name = param.name.to_lowercase();
        match param.mode {
            ParamMode::In | ParamMode::InOut => {
                let val = eval_sp_expr(arg_expr, &eval_ctx, user_vars)?;
                // Check BIGINT overflow: if param type is BIGINT and value is Float
                // that exceeds i64 range, the literal overflowed during parsing
                let param_type_upper = param.data_type.to_uppercase();
                if param_type_upper.contains("BIGINT") && !param_type_upper.contains("UNSIGNED") {
                    if let Datum::Float(f) = &val {
                        // 2^63 exactly — values >= this overflow BIGINT
                        const MAX_BIGINT_F64: f64 = 9_223_372_036_854_775_808.0;
                        if *f >= MAX_BIGINT_F64 || *f < i64::MIN as f64 {
                            return Err(format!(
                                "Out of range value for column '{}' at row 1",
                                param.name
                            ));
                        }
                    }
                    // Also check UnsignedInt values that exceed i64::MAX
                    if let Datum::UnsignedInt(u) = &val {
                        if *u > i64::MAX as u64 {
                            return Err(format!(
                                "Out of range value for column '{}' at row 1",
                                param.name
                            ));
                        }
                    }
                }
                ctx.locals.insert(param_name, val);
            }
            ParamMode::Out => {
                ctx.locals.insert(param_name, Datum::Null);
            }
        }
    }

    Ok(ctx)
}

/// Back-propagate OUT/INOUT parameters to user variables after procedure execution.
pub fn propagate_out_params(
    proc_def: &ProcedureDef,
    call_args: &[sqlparser::ast::Expr],
    ctx: &ProcedureContext,
    user_vars: &UserVariables,
) {
    for (param, arg_expr) in proc_def.params.iter().zip(call_args.iter()) {
        if param.mode == ParamMode::In {
            continue;
        }
        // Only propagate if the call arg was a @variable
        if let Some(var_name) = extract_user_var_name(arg_expr) {
            let param_name = param.name.to_lowercase();
            if let Some(val) = ctx.locals.get(&param_name) {
                user_vars.write().insert(var_name, val.clone());
            }
        }
    }
}

/// Evaluate a DECLARE variable's default assignment to a Datum.
/// Unwraps any DeclareAssignment variant to get the Expr, evaluates it via eval_sp_expr.
pub fn eval_declare_default(
    assignment: &sqlparser::ast::DeclareAssignment,
    ctx: &ProcedureContext,
    user_vars: &UserVariables,
) -> Datum {
    use sqlparser::ast::DeclareAssignment;
    let expr = match assignment {
        DeclareAssignment::Expr(e)
        | DeclareAssignment::Default(e)
        | DeclareAssignment::DuckAssignment(e)
        | DeclareAssignment::For(e)
        | DeclareAssignment::MsSqlAssignment(e) => e,
    };
    eval_sp_expr(expr, ctx, user_vars).unwrap_or(Datum::Null)
}

/// Extract @var_name from a sqlparser expression, if it is a user variable reference
fn extract_user_var_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    // In MySQL, @var is parsed by sqlparser as various forms
    // Check for Identifier starting with @ prefix - sqlparser often represents @x as
    // an identifier in SET context
    if let sqlparser::ast::Expr::Identifier(ident) = expr {
        if let Some(stripped) = ident.value.strip_prefix('@') {
            return Some(stripped.to_lowercase());
        }
    }
    None
}
