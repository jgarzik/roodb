//! Expression evaluation
//!
//! Evaluates ResolvedExpr against a Row to produce a Datum.

use std::cell::RefCell;

use crate::planner::logical::{BinaryOp, BooleanTestType, ResolvedExpr, UnaryOp};
use crate::server::session::UserVariables;

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

// MySQL-compatible RAND() state: (seed1, seed2)
thread_local! {
    static RAND_STATE: RefCell<(u64, u64)> = const { RefCell::new((0, 0)) };
}

/// MySQL's my_rnd() algorithm — advance state and return [0, 1) double
fn mysql_rand_next(seed1: &mut u64, seed2: &mut u64) -> f64 {
    const MAX_VALUE: u64 = 0x3FFFFFFF;
    *seed1 = (*seed1 * 3 + *seed2) % MAX_VALUE;
    *seed2 = (*seed1 + *seed2 + 33) % MAX_VALUE;
    *seed1 as f64 / MAX_VALUE as f64
}

/// Seed MySQL RAND from integer — matches MySQL's seed_random() in item_func.cc
/// MySQL uses `(uint32)` casts causing 32-bit wrapping, and only seed1 gets +55555555.
fn mysql_rand_seed(seed: u64) -> (u64, u64) {
    const MAX_VALUE: u64 = 0x3FFFFFFF;
    let tmp = seed as u32;
    let s1 = tmp.wrapping_mul(0x10001).wrapping_add(55555555);
    let s2 = tmp.wrapping_mul(0x10000001); // No +55555555 on seed2
    ((s1 as u64) % MAX_VALUE, (s2 as u64) % MAX_VALUE)
}

/// Internal variable name for NO_UNSIGNED_SUBTRACTION flag (stored in UserVariables)
const NO_UNSIGNED_SUB_VAR: &str = "__sys_no_unsigned_sub";

/// Internal variable name for ERROR_FOR_DIVISION_BY_ZERO flag (stored in UserVariables)
const ERROR_DIV_ZERO_VAR: &str = "__sys_error_div_zero";

/// Internal variable name for strict-DML context flag (set during INSERT/UPDATE evaluation)
const STRICT_DML_CONTEXT_VAR: &str = "__sys_strict_dml";

/// Internal variable name for STRICT_TRANS_TABLES sql_mode flag
const STRICT_TRANS_VAR: &str = "__sys_strict_trans_tables";

/// Set the STRICT_TRANS_TABLES flag for a session
pub fn set_strict_trans_tables(vars: &UserVariables, val: bool) {
    let mut w = vars.write();
    if val {
        w.insert(STRICT_TRANS_VAR.to_string(), Datum::Bool(true));
    } else {
        w.remove(STRICT_TRANS_VAR);
    }
}

/// Check if STRICT_TRANS_TABLES is active
pub fn is_strict_trans_tables(vars: &UserVariables) -> bool {
    let r = vars.read();
    matches!(r.get(STRICT_TRANS_VAR), Some(Datum::Bool(true)))
}

/// Set the NO_UNSIGNED_SUBTRACTION flag for a session (stored in UserVariables)
pub fn set_no_unsigned_subtraction(vars: &UserVariables, val: bool) {
    let mut w = vars.write();
    if val {
        w.insert(NO_UNSIGNED_SUB_VAR.to_string(), Datum::Bool(true));
    } else {
        w.remove(NO_UNSIGNED_SUB_VAR);
    }
}

/// Get the NO_UNSIGNED_SUBTRACTION flag from UserVariables
fn no_unsigned_subtraction(vars: &UserVariables) -> bool {
    let r = vars.read();
    matches!(r.get(NO_UNSIGNED_SUB_VAR), Some(Datum::Bool(true)))
}

/// Set the ERROR_FOR_DIVISION_BY_ZERO flag for a session (stored in UserVariables)
pub fn set_error_for_division_by_zero(vars: &UserVariables, val: bool) {
    let mut w = vars.write();
    if val {
        w.insert(ERROR_DIV_ZERO_VAR.to_string(), Datum::Bool(true));
    } else {
        w.remove(ERROR_DIV_ZERO_VAR);
    }
}

/// Get the ERROR_FOR_DIVISION_BY_ZERO flag from UserVariables
fn error_for_division_by_zero(vars: Option<&UserVariables>) -> bool {
    match vars {
        Some(v) => {
            let r = v.read();
            matches!(r.get(ERROR_DIV_ZERO_VAR), Some(Datum::Bool(true)))
        }
        None => false,
    }
}

/// Set/clear the strict DML context flag (called by Insert/Update executors)
pub fn set_strict_dml_context(vars: &UserVariables, val: bool) {
    let mut w = vars.write();
    if val {
        w.insert(STRICT_DML_CONTEXT_VAR.to_string(), Datum::Bool(true));
    } else {
        w.remove(STRICT_DML_CONTEXT_VAR);
    }
}

/// RAII guard that sets strict DML context on creation and clears on drop.
/// Ensures the flag is always cleared, even on early returns or errors.
pub struct StrictDmlGuard {
    vars: UserVariables,
}

impl StrictDmlGuard {
    pub fn new(vars: &UserVariables) -> Self {
        set_strict_dml_context(vars, true);
        StrictDmlGuard { vars: vars.clone() }
    }
}

impl Drop for StrictDmlGuard {
    fn drop(&mut self) {
        set_strict_dml_context(&self.vars, false);
    }
}

/// Check if we're in a strict DML context (INSERT/UPDATE with strict mode)
fn in_strict_dml_context(vars: Option<&UserVariables>) -> bool {
    match vars {
        Some(v) => {
            let r = v.read();
            matches!(r.get(STRICT_DML_CONTEXT_VAR), Some(Datum::Bool(true)))
        }
        None => false,
    }
}

/// Evaluate an expression against a row, with access to user variables
pub fn evaluate(expr: &ResolvedExpr, row: &Row, vars: &UserVariables) -> ExecutorResult<Datum> {
    match expr {
        ResolvedExpr::Column(col) => {
            let datum = row.get(col.index)?;
            Ok(datum.clone())
        }

        ResolvedExpr::Literal(lit) => Ok(Datum::from_literal(lit)),

        ResolvedExpr::BinaryOp {
            left,
            op: BinaryOp::Assign,
            right,
            ..
        } => {
            // @var := expr — evaluate right side, store in vars, return value
            let rval = evaluate(right, row, vars)?;
            if let ResolvedExpr::UserVariable { name } = left.as_ref() {
                vars.write().insert(name.clone(), rval.clone());
            }
            Ok(rval)
        }

        ResolvedExpr::BinaryOp {
            left, op, right, ..
        } => {
            let lval = evaluate(left, row, vars)?;
            let rval = evaluate(right, row, vars)?;
            eval_binary_op_ex(op, &lval, &rval, Some(vars))
        }

        ResolvedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate(expr, row, vars)?;
            let result = eval_unary_op(op, &val);
            // For unary Neg overflow in non-column contexts, promote to Decimal
            // instead of erroring. MySQL promotes literals to DECIMAL when
            // negation overflows BIGINT, but errors for column values.
            if result.is_err() && matches!(op, UnaryOp::Neg) {
                let is_column = matches!(expr.as_ref(), ResolvedExpr::Column(_));
                if !is_column {
                    match &val {
                        Datum::UnsignedInt(u) => {
                            return Ok(Datum::Decimal {
                                value: -(*u as i128),
                                scale: 0,
                            });
                        }
                        Datum::Int(i) => {
                            // -(i64::MIN) overflows BIGINT; promote to Decimal
                            return Ok(Datum::Decimal {
                                value: -(*i as i128),
                                scale: 0,
                            });
                        }
                        _ => {}
                    }
                }
            }
            result
        }

        ResolvedExpr::Function { name, args, .. } => {
            // IF() requires short-circuit evaluation — only compute the taken branch
            if name.eq_ignore_ascii_case("IF") {
                return eval_if_function(args, row, vars);
            }
            let arg_vals: Vec<Datum> = args
                .iter()
                .map(|a| evaluate(a, row, vars))
                .collect::<Result<_, _>>()?;
            eval_function(name, &arg_vals, Some(vars))
        }

        ResolvedExpr::IsNull { expr, negated } => {
            let val = evaluate(expr, row, vars)?;
            let is_null = val.is_null();
            Ok(Datum::Bool(if *negated { !is_null } else { is_null }))
        }

        ResolvedExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = evaluate(expr, row, vars)?;
            if val.is_null() {
                return Ok(Datum::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = evaluate(item, row, vars)?;
                if item_val.is_null() {
                    continue;
                }
                if val == item_val {
                    found = true;
                    break;
                }
            }
            Ok(Datum::Bool(if *negated { !found } else { found }))
        }

        ResolvedExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = evaluate(expr, row, vars)?;
            let low_val = evaluate(low, row, vars)?;
            let high_val = evaluate(high, row, vars)?;

            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Datum::Null);
            }

            let in_range = val >= low_val && val <= high_val;
            Ok(Datum::Bool(if *negated { !in_range } else { in_range }))
        }

        ResolvedExpr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                // Simple CASE: CASE op WHEN val1 THEN res1 ...
                let op_val = evaluate(op, row, vars)?;
                for (cond, result) in conditions.iter().zip(results.iter()) {
                    let cond_val = evaluate(cond, row, vars)?;
                    if !op_val.is_null() && !cond_val.is_null() && op_val == cond_val {
                        return evaluate(result, row, vars);
                    }
                }
            } else {
                // Searched CASE: CASE WHEN cond1 THEN res1 ...
                for (cond, result) in conditions.iter().zip(results.iter()) {
                    let cond_val = evaluate(cond, row, vars)?;
                    if cond_val.as_bool() == Some(true) {
                        return evaluate(result, row, vars);
                    }
                }
            }
            // No match — return ELSE or NULL
            match else_result {
                Some(e) => evaluate(e, row, vars),
                None => Ok(Datum::Null),
            }
        }

        ResolvedExpr::Cast {
            expr, target_type, ..
        } => {
            let val = evaluate(expr, row, vars)?;
            eval_cast(&val, target_type)
        }

        ResolvedExpr::UserVariable { name } => {
            let val = vars.read().get(name).cloned().unwrap_or(Datum::Null);
            Ok(val)
        }

        ResolvedExpr::BooleanTest { expr, test } => {
            let val = evaluate(expr, row, vars)?;
            let result = match test {
                BooleanTestType::IsTrue => val.as_bool() == Some(true),
                BooleanTestType::IsNotTrue => val.as_bool() != Some(true),
                BooleanTestType::IsFalse => val.as_bool() == Some(false),
                BooleanTestType::IsNotFalse => val.as_bool() != Some(false),
                BooleanTestType::IsUnknown => val.is_null(),
                BooleanTestType::IsNotUnknown => !val.is_null(),
            };
            Ok(Datum::Bool(result))
        }
    }
}

/// Evaluate a binary operation.
/// `vars` is optional — when provided, sql_mode flags (e.g. NO_UNSIGNED_SUBTRACTION) apply.
pub fn eval_binary_op(op: &BinaryOp, left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    eval_binary_op_ex(op, left, right, None)
}

fn eval_binary_op_ex(
    op: &BinaryOp,
    left: &Datum,
    right: &Datum,
    vars: Option<&UserVariables>,
) -> ExecutorResult<Datum> {
    // Handle NULL propagation for most operations
    // Geometry in arithmetic/DIV is always an error (MySQL: "Incorrect arguments to ...")
    if matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::IntDiv
    ) && (matches!(left, Datum::Geometry(_)) || matches!(right, Datum::Geometry(_)))
    {
        let op_name = match op {
            BinaryOp::IntDiv => "DIV",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "MOD",
            _ => "operator",
        };
        return Err(ExecutorError::InvalidOperation(format!(
            "Incorrect arguments to {}",
            op_name
        )));
    }

    if matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Like
            | BinaryOp::NotLike
            | BinaryOp::BitwiseOr
            | BinaryOp::BitwiseAnd
            | BinaryOp::BitwiseXor
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight
            | BinaryOp::IntDiv
    ) && (left.is_null() || right.is_null())
    {
        return Ok(Datum::Null);
    }

    // In strict DML context (INSERT/UPDATE), non-numeric strings in arithmetic
    // must raise ER_TRUNCATED_WRONG_VALUE (1292) instead of silently becoming 0
    if matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::IntDiv
    ) && in_strict_dml_context(vars)
        && vars.is_some_and(is_strict_trans_tables)
    {
        for operand in [left, right] {
            if let Datum::String(s) = operand {
                let trimmed = s.trim();
                if !trimmed.is_empty() && trimmed.parse::<f64>().is_err() {
                    // Leading digits might exist (e.g. "123abc" → 123) but MySQL
                    // still raises TRUNCATED_WRONG_VALUE in strict mode
                    return Err(ExecutorError::TruncatedWrongValue(format!(
                        "Truncated incorrect DOUBLE value: '{}'",
                        trimmed
                    )));
                }
            }
        }
    }

    match op {
        // Arithmetic
        BinaryOp::Add => eval_add(left, right),
        BinaryOp::Sub => eval_sub(left, right, vars),
        BinaryOp::Mul => eval_mul(left, right),
        BinaryOp::Div => eval_div(left, right),
        BinaryOp::Mod => eval_mod(left, right),

        // Comparison
        BinaryOp::Eq => Ok(Datum::Bool(left == right)),
        BinaryOp::NotEq => Ok(Datum::Bool(left != right)),
        BinaryOp::Lt => Ok(Datum::Bool(left < right)),
        BinaryOp::LtEq => Ok(Datum::Bool(left <= right)),
        BinaryOp::Gt => Ok(Datum::Bool(left > right)),
        BinaryOp::GtEq => Ok(Datum::Bool(left >= right)),

        // Logical - special NULL handling for AND/OR
        BinaryOp::And => eval_and(left, right),
        BinaryOp::Or => eval_or(left, right),

        // String
        BinaryOp::Like => left
            .like(right)
            .ok_or_else(|| ExecutorError::InvalidOperation("LIKE requires strings".to_string())),
        BinaryOp::NotLike => {
            let result = left.like(right).ok_or_else(|| {
                ExecutorError::InvalidOperation("NOT LIKE requires strings".to_string())
            })?;
            match result {
                Datum::Bool(b) => Ok(Datum::Bool(!b)),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutorError::InvalidOperation(
                    "LIKE produced non-bool".to_string(),
                )),
            }
        }

        // Bitwise
        BinaryOp::BitwiseOr => eval_bitwise_or(left, right),
        BinaryOp::BitwiseAnd => eval_bitwise_and(left, right),
        BinaryOp::BitwiseXor => eval_bitwise_xor(left, right),
        BinaryOp::ShiftLeft => eval_shift_left(left, right),
        BinaryOp::ShiftRight => eval_shift_right(left, right),

        // Integer division (DIV)
        BinaryOp::IntDiv => eval_int_div(left, right),

        // Logical XOR (with three-valued logic)
        BinaryOp::Xor => eval_xor(left, right),

        // NULL-safe equality (<=>)
        BinaryOp::Spaceship => Ok(Datum::Bool(eval_spaceship(left, right))),

        // Assignment (:=) — handled at the top level in evaluate()
        BinaryOp::Assign => Err(ExecutorError::InvalidOperation(
            "Assignment operator handled at expression level".to_string(),
        )),
    }
}

/// Check if a float result overflows in integer-promoted context.
/// For Int+Float, UnsignedInt+Float etc., values beyond BIGINT UNSIGNED
/// max (~1.8e19) are rejected (MySQL semantics).
fn check_float_overflow(v: f64) -> ExecutorResult<Datum> {
    if v.is_infinite() {
        Err(ExecutorError::DataOutOfRange(
            "DOUBLE value is out of range".to_string(),
        ))
    } else if v.abs() > 1.844_674_407_370_955e19 {
        Err(ExecutorError::DataOutOfRange(
            "BIGINT UNSIGNED value is out of range".to_string(),
        ))
    } else {
        Ok(Datum::Float(v))
    }
}

/// Convert float to i64, raising DataOutOfRange if it exceeds BIGINT range.
fn checked_float_to_int(v: f64) -> ExecutorResult<Datum> {
    if v > i64::MAX as f64 || v < i64::MIN as f64 || v.is_infinite() || v.is_nan() {
        Err(ExecutorError::DataOutOfRange(
            "BIGINT value is out of range".to_string(),
        ))
    } else {
        Ok(Datum::Int(v as i64))
    }
}

/// Promote Bit and String to numeric types for arithmetic operations (MySQL implicit coercion).
fn promote_to_numeric(d: &Datum) -> std::borrow::Cow<'_, Datum> {
    match d {
        Datum::Bit { value, .. } => std::borrow::Cow::Owned(Datum::Int(*value as i64)),
        Datum::Decimal { .. } => std::borrow::Cow::Borrowed(d),
        Datum::String(s) => {
            // MySQL coerces strings to numbers in arithmetic: "123.5" → 123.5, "abc" → 0
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0
                    && !trimmed.contains('.')
                    && !trimmed.contains('e')
                    && !trimmed.contains('E')
                {
                    if let Ok(i) = trimmed.parse::<i64>() {
                        return std::borrow::Cow::Owned(Datum::Int(i));
                    }
                }
                std::borrow::Cow::Owned(Datum::Float(f))
            } else {
                std::borrow::Cow::Owned(Datum::Int(parse_leading_int(trimmed)))
            }
        }
        other => std::borrow::Cow::Borrowed(other),
    }
}

fn eval_add(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => a.checked_add(*b).map(Datum::Int).ok_or_else(|| {
            ExecutorError::DataOutOfRange("BIGINT value is out of range".to_string())
        }),
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => {
            a.checked_add(*b).map(Datum::UnsignedInt).ok_or_else(|| {
                ExecutorError::DataOutOfRange("BIGINT UNSIGNED value is out of range".to_string())
            })
        }
        (Datum::Int(a), Datum::UnsignedInt(b)) | (Datum::UnsignedInt(b), Datum::Int(a)) => {
            let result = (*b as i128) + (*a as i128);
            if result < 0 || result > u64::MAX as i128 {
                Err(ExecutorError::DataOutOfRange(
                    "BIGINT UNSIGNED value is out of range".to_string(),
                ))
            } else {
                Ok(Datum::UnsignedInt(result as u64))
            }
        }
        (Datum::Float(a), Datum::Float(b)) => check_float_overflow(a + b),
        (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
            check_float_overflow(*a as f64 + b)
        }
        (Datum::UnsignedInt(a), Datum::Float(b)) | (Datum::Float(b), Datum::UnsignedInt(a)) => {
            check_float_overflow(*a as f64 + b)
        }
        // Decimal arithmetic
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => {
            let (a_norm, b_norm, result_scale) = normalize_decimal_scales(*a, *sa, *b, *sb)
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            match a_norm.checked_add(b_norm) {
                Some(v) => {
                    // For integer-valued decimals (scale 0), check BIGINT range
                    if result_scale == 0
                        && (v > i64::MAX as i128 || v < i64::MIN as i128)
                        && (*sa == 0 && *sb == 0)
                    {
                        return Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ));
                    }
                    Ok(Datum::Decimal {
                        value: v,
                        scale: result_scale,
                    })
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::Int(i))
        | (Datum::Int(i), Datum::Decimal { value: d, scale: s }) => {
            let i_scaled = (*i as i128).checked_mul(10i128.pow(*s as u32));
            match i_scaled.and_then(|is| d.checked_add(is)) {
                Some(v) => {
                    // For integer-valued decimals (scale 0), check BIGINT range
                    if *s == 0 && (v > i64::MAX as i128 || v < i64::MIN as i128) {
                        return Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ));
                    }
                    Ok(Datum::Decimal {
                        value: v,
                        scale: *s,
                    })
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::UnsignedInt(u))
        | (Datum::UnsignedInt(u), Datum::Decimal { value: d, scale: s }) => {
            let u_scaled = (*u as i128).checked_mul(10i128.pow(*s as u32));
            match u_scaled.and_then(|us| d.checked_add(us)) {
                Some(v) => Ok(Datum::Decimal {
                    value: v,
                    scale: *s,
                }),
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            check_float_overflow(a + b)
        }
        (Datum::String(a), Datum::String(b)) => Ok(Datum::String(format!("{}{}", a, b))),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot add {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_sub(left: &Datum, right: &Datum, vars: Option<&UserVariables>) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);

    // When NO_UNSIGNED_SUBTRACTION is active, mixed/unsigned subtraction produces signed result
    if vars.is_some_and(no_unsigned_subtraction) {
        match (left.as_ref(), right.as_ref()) {
            (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => {
                let result = (*a as i128) - (*b as i128);
                if result > i64::MAX as i128 || result < i64::MIN as i128 {
                    return Err(ExecutorError::DataOutOfRange(
                        "BIGINT value is out of range".to_string(),
                    ));
                }
                return Ok(Datum::Int(result as i64));
            }
            (Datum::UnsignedInt(a), Datum::Int(b)) => {
                let result = (*a as i128) - (*b as i128);
                if result > i64::MAX as i128 || result < i64::MIN as i128 {
                    return Err(ExecutorError::DataOutOfRange(
                        "BIGINT value is out of range".to_string(),
                    ));
                }
                return Ok(Datum::Int(result as i64));
            }
            (Datum::Int(a), Datum::UnsignedInt(b)) => {
                let result = (*a as i128) - (*b as i128);
                if result > i64::MAX as i128 || result < i64::MIN as i128 {
                    return Err(ExecutorError::DataOutOfRange(
                        "BIGINT value is out of range".to_string(),
                    ));
                }
                return Ok(Datum::Int(result as i64));
            }
            _ => {} // fall through to normal path
        }
    }

    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => a.checked_sub(*b).map(Datum::Int).ok_or_else(|| {
            ExecutorError::DataOutOfRange("BIGINT value is out of range".to_string())
        }),
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => {
            a.checked_sub(*b).map(Datum::UnsignedInt).ok_or_else(|| {
                ExecutorError::DataOutOfRange("BIGINT UNSIGNED value is out of range".to_string())
            })
        }
        (Datum::UnsignedInt(a), Datum::Int(b)) => {
            let result = (*a as i128) - (*b as i128);
            if result < 0 || result > u64::MAX as i128 {
                Err(ExecutorError::DataOutOfRange(
                    "BIGINT UNSIGNED value is out of range".to_string(),
                ))
            } else {
                Ok(Datum::UnsignedInt(result as u64))
            }
        }
        (Datum::Int(a), Datum::UnsignedInt(b)) => {
            let result = (*a as i128) - (*b as i128);
            if result < 0 || result > u64::MAX as i128 {
                Err(ExecutorError::DataOutOfRange(
                    "BIGINT UNSIGNED value is out of range".to_string(),
                ))
            } else {
                Ok(Datum::UnsignedInt(result as u64))
            }
        }
        (Datum::Float(a), Datum::Float(b)) => check_float_overflow(a - b),
        (Datum::Int(a), Datum::Float(b)) => check_float_overflow(*a as f64 - b),
        (Datum::Float(a), Datum::Int(b)) => check_float_overflow(a - *b as f64),
        (Datum::UnsignedInt(a), Datum::Float(b)) => check_float_overflow(*a as f64 - b),
        (Datum::Float(a), Datum::UnsignedInt(b)) => check_float_overflow(a - *b as f64),
        // Decimal subtraction
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => {
            let (a_norm, b_norm, result_scale) = normalize_decimal_scales(*a, *sa, *b, *sb)
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            match a_norm.checked_sub(b_norm) {
                Some(v) => {
                    if result_scale == 0
                        && (v > i64::MAX as i128 || v < i64::MIN as i128)
                        && (*sa == 0 && *sb == 0)
                    {
                        return Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ));
                    }
                    Ok(Datum::Decimal {
                        value: v,
                        scale: result_scale,
                    })
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::Int(i)) => {
            let i_scaled = (*i as i128).checked_mul(10i128.pow(*s as u32));
            match i_scaled.and_then(|is| d.checked_sub(is)) {
                Some(v) => {
                    if *s == 0 && (v > i64::MAX as i128 || v < i64::MIN as i128) {
                        return Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ));
                    }
                    Ok(Datum::Decimal {
                        value: v,
                        scale: *s,
                    })
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Int(i), Datum::Decimal { value: d, scale: s }) => {
            let i_scaled = (*i as i128).checked_mul(10i128.pow(*s as u32));
            match i_scaled.and_then(|is| is.checked_sub(*d)) {
                Some(v) => {
                    if *s == 0 && (v > i64::MAX as i128 || v < i64::MIN as i128) {
                        return Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ));
                    }
                    Ok(Datum::Decimal {
                        value: v,
                        scale: *s,
                    })
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::UnsignedInt(u)) => {
            let u_scaled = (*u as i128).checked_mul(10i128.pow(*s as u32));
            match u_scaled.and_then(|us| d.checked_sub(us)) {
                Some(v) => Ok(Datum::Decimal {
                    value: v,
                    scale: *s,
                }),
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::UnsignedInt(u), Datum::Decimal { value: d, scale: s }) => {
            let u_scaled = (*u as i128).checked_mul(10i128.pow(*s as u32));
            match u_scaled.and_then(|us| us.checked_sub(*d)) {
                Some(v) => Ok(Datum::Decimal {
                    value: v,
                    scale: *s,
                }),
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            check_float_overflow(a - b)
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot subtract {:?} from {:?}",
            right, left
        ))),
    }
}

fn eval_mul(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => a.checked_mul(*b).map(Datum::Int).ok_or_else(|| {
            ExecutorError::DataOutOfRange("BIGINT value is out of range".to_string())
        }),
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => {
            a.checked_mul(*b).map(Datum::UnsignedInt).ok_or_else(|| {
                ExecutorError::DataOutOfRange("BIGINT UNSIGNED value is out of range".to_string())
            })
        }
        (Datum::Int(a), Datum::UnsignedInt(b)) | (Datum::UnsignedInt(b), Datum::Int(a)) => {
            let result = (*a as i128) * (*b as i128);
            if result < 0 || result > u64::MAX as i128 {
                Err(ExecutorError::DataOutOfRange(
                    "BIGINT UNSIGNED value is out of range".to_string(),
                ))
            } else {
                Ok(Datum::UnsignedInt(result as u64))
            }
        }
        (Datum::Float(a), Datum::Float(b)) => check_float_overflow(a * b),
        (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
            check_float_overflow(*a as f64 * b)
        }
        (Datum::UnsignedInt(a), Datum::Float(b)) | (Datum::Float(b), Datum::UnsignedInt(a)) => {
            check_float_overflow(*a as f64 * b)
        }
        // Decimal multiplication
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => match a.checked_mul(*b) {
            Some(v) => Ok(Datum::Decimal {
                value: v,
                scale: sa + sb,
            }),
            None => Err(ExecutorError::DataOutOfRange(
                "DECIMAL value is out of range".to_string(),
            )),
        },
        (Datum::Decimal { value: d, scale: s }, Datum::Int(i))
        | (Datum::Int(i), Datum::Decimal { value: d, scale: s }) => {
            match d.checked_mul(*i as i128) {
                Some(v) => Ok(Datum::Decimal {
                    value: v,
                    scale: *s,
                }),
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::UnsignedInt(u))
        | (Datum::UnsignedInt(u), Datum::Decimal { value: d, scale: s }) => {
            match d.checked_mul(*u as i128) {
                Some(v) => Ok(Datum::Decimal {
                    value: v,
                    scale: *s,
                }),
                None => Err(ExecutorError::DataOutOfRange(
                    "DECIMAL value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            check_float_overflow(a * b)
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot multiply {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_div(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    // Division by zero returns NULL (MySQL semantics)
    match right.as_ref() {
        Datum::Int(0) => return Ok(Datum::Null),
        Datum::UnsignedInt(0) => return Ok(Datum::Null),
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        Datum::Decimal { value: v, .. } if *v == 0 => return Ok(Datum::Null),
        _ => {}
    }

    match (left.as_ref(), right.as_ref()) {
        // MySQL: integer / integer returns DECIMAL (float), not integer
        (Datum::Int(a), Datum::Int(b)) => check_float_overflow(*a as f64 / *b as f64),
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => {
            check_float_overflow(*a as f64 / *b as f64)
        }
        (Datum::Int(a), Datum::UnsignedInt(b)) => check_float_overflow(*a as f64 / *b as f64),
        (Datum::UnsignedInt(a), Datum::Int(b)) => check_float_overflow(*a as f64 / *b as f64),
        (Datum::Float(a), Datum::Float(b)) => check_float_overflow(a / b),
        (Datum::Int(a), Datum::Float(b)) => check_float_overflow(*a as f64 / b),
        (Datum::Float(a), Datum::Int(b)) => check_float_overflow(a / *b as f64),
        (Datum::UnsignedInt(a), Datum::Float(b)) => check_float_overflow(*a as f64 / b),
        (Datum::Float(a), Datum::UnsignedInt(b)) => check_float_overflow(a / *b as f64),
        // Decimal division — MySQL adds 4 extra scale digits for division
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => {
            let extra = 4u8;
            let result_scale = sa.saturating_add(extra);
            let scale_diff = result_scale.saturating_add(*sb).saturating_sub(*sa);
            let factor = 10i128.pow(scale_diff as u32);
            match a.checked_mul(factor) {
                Some(scaled_a) => Ok(Datum::Decimal {
                    value: scaled_a / b,
                    scale: result_scale,
                }),
                None => {
                    // Fall back to float
                    let fa = left.as_float().unwrap();
                    let fb = right.as_float().unwrap();
                    check_float_overflow(fa / fb)
                }
            }
        }
        (Datum::Decimal { .. }, _) | (_, Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            check_float_overflow(a / b)
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot divide {:?} by {:?}",
            left, right
        ))),
    }
}

fn eval_mod(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    // Modulo by zero returns NULL (MySQL semantics)
    match right.as_ref() {
        Datum::Int(0) => return Ok(Datum::Null),
        Datum::UnsignedInt(0) => return Ok(Datum::Null),
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        Datum::Decimal { value: v, .. } if *v == 0 => return Ok(Datum::Null),
        _ => {}
    }

    // Use checked_rem for Int/Int to avoid panic on i64::MIN % -1.
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a.checked_rem(*b).unwrap_or(0))),
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => Ok(Datum::UnsignedInt(a % b)),
        (Datum::Int(a), Datum::UnsignedInt(b)) => Ok(Datum::UnsignedInt((*a as u64) % b)),
        (Datum::UnsignedInt(a), Datum::Int(b)) => Ok(Datum::UnsignedInt(a % (*b as u64))),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a % b)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 % b)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Float(a % *b as f64)),
        (Datum::UnsignedInt(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 % b)),
        (Datum::Float(a), Datum::UnsignedInt(b)) => Ok(Datum::Float(a % *b as f64)),
        // Decimal modulo
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => {
            let (a_norm, b_norm, result_scale) = normalize_decimal_scales(*a, *sa, *b, *sb)
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            Ok(Datum::Decimal {
                value: a_norm.checked_rem(b_norm).unwrap_or(0),
                scale: result_scale,
            })
        }
        // Decimal % Int (exact)
        (Datum::Decimal { value: d, scale: s }, Datum::Int(i)) => {
            let i_scaled = (*i as i128)
                .checked_mul(10i128.pow(*s as u32))
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            Ok(Datum::Decimal {
                value: d.checked_rem(i_scaled).unwrap_or(0),
                scale: *s,
            })
        }
        (Datum::Int(i), Datum::Decimal { value: d, scale: s }) => {
            let i_scaled = (*i as i128)
                .checked_mul(10i128.pow(*s as u32))
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            Ok(Datum::Decimal {
                value: i_scaled.checked_rem(*d).unwrap_or(0),
                scale: *s,
            })
        }
        (Datum::Decimal { value: d, scale: s }, Datum::UnsignedInt(u)) => {
            let u_scaled = (*u as i128)
                .checked_mul(10i128.pow(*s as u32))
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            Ok(Datum::Decimal {
                value: d.checked_rem(u_scaled).unwrap_or(0),
                scale: *s,
            })
        }
        (Datum::UnsignedInt(u), Datum::Decimal { value: d, scale: s }) => {
            let u_scaled = (*u as i128)
                .checked_mul(10i128.pow(*s as u32))
                .ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            Ok(Datum::Decimal {
                value: u_scaled.checked_rem(*d).unwrap_or(0),
                scale: *s,
            })
        }
        (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            Ok(Datum::Float(a % b))
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot compute modulo of {:?} and {:?}",
            left, right
        ))),
    }
}

/// Normalize two decimal values to the same scale, returning (a_scaled, b_scaled, result_scale).
/// Returns None if the scaling multiplication overflows i128.
fn normalize_decimal_scales(a: i128, sa: u8, b: i128, sb: u8) -> Option<(i128, i128, u8)> {
    if sa == sb {
        Some((a, b, sa))
    } else if sa < sb {
        let factor = 10i128.pow((sb - sa) as u32);
        Some((a.checked_mul(factor)?, b, sb))
    } else {
        let factor = 10i128.pow((sa - sb) as u32);
        Some((a, b.checked_mul(factor)?, sa))
    }
}

/// Parse leading integer from a string (MySQL semantics: "123abc" → 123, "abc" → 0).
/// Caller should pass pre-trimmed input.
fn parse_leading_int(s: &str) -> i64 {
    if s.is_empty() {
        return 0;
    }
    // Try full parse first
    if let Ok(v) = s.parse::<i64>() {
        return v;
    }
    if let Ok(v) = s.parse::<f64>() {
        return v as i64;
    }
    // Extract leading numeric chars
    let mut end = 0;
    let bytes = s.as_bytes();
    if !bytes.is_empty() && (bytes[0] == b'-' || bytes[0] == b'+') {
        end = 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == 0 || (end == 1 && (bytes[0] == b'-' || bytes[0] == b'+')) {
        return 0;
    }
    s[..end].parse::<i64>().unwrap_or(0)
}

/// Coerce a Datum to i64 for bitwise operations (MySQL truncates floats, parses strings).
fn to_bitwise_int(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int(i) => Some(*i),
        Datum::UnsignedInt(u) => Some(*u as i64),
        Datum::Float(f) => Some(*f as i64),
        Datum::Bool(b) => Some(if *b { 1 } else { 0 }),
        Datum::Bit { value, .. } => Some(*value as i64),
        Datum::Decimal { value, scale } => {
            let divisor = 10i128.pow(*scale as u32);
            Some((value / divisor) as i64)
        }
        Datum::String(s) => Some(parse_leading_int(s)),
        _ => None,
    }
}

fn eval_bitwise_or(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (to_bitwise_int(left), to_bitwise_int(right)) {
        (Some(a), Some(b)) => Ok(Datum::Int(a | b)),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot bitwise OR {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_bitwise_and(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (to_bitwise_int(left), to_bitwise_int(right)) {
        (Some(a), Some(b)) => Ok(Datum::Int(a & b)),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot bitwise AND {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_bitwise_xor(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (to_bitwise_int(left), to_bitwise_int(right)) {
        (Some(a), Some(b)) => Ok(Datum::Int(a ^ b)),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot bitwise XOR {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_shift_left(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (to_bitwise_int(left), to_bitwise_int(right)) {
        (Some(a), Some(b)) => {
            // MySQL treats shift count as u64; any value >= 64 gives 0
            let shift = b as u64;
            let result = if shift >= 64 {
                0u64
            } else {
                (a as u64) << shift
            };
            Ok(Datum::UnsignedInt(result))
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot shift {:?} << {:?}",
            left, right
        ))),
    }
}

fn eval_shift_right(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (to_bitwise_int(left), to_bitwise_int(right)) {
        (Some(a), Some(b)) => {
            let shift = b as u64;
            let result = if shift >= 64 {
                0u64
            } else {
                (a as u64) >> shift
            };
            Ok(Datum::UnsignedInt(result))
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot shift {:?} >> {:?}",
            left, right
        ))),
    }
}

fn eval_int_div(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    // DIV by zero returns NULL (MySQL semantics)
    match right.as_ref() {
        Datum::Int(0) => return Ok(Datum::Null),
        Datum::UnsignedInt(0) => return Ok(Datum::Null),
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        Datum::Decimal { value: v, .. } if *v == 0 => return Ok(Datum::Null),
        _ => {}
    }
    // MySQL DIV: perform float division, then truncate toward zero.
    // Use checked_div for Int/Int to avoid panic on i64::MIN / -1.
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => match a.checked_div(*b) {
            Some(v) => Ok(Datum::Int(v)),
            None => Err(ExecutorError::DataOutOfRange(
                "BIGINT value is out of range".to_string(),
            )),
        },
        (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => Ok(Datum::UnsignedInt(a / b)),
        (Datum::Int(a), Datum::UnsignedInt(b)) => {
            if *a < 0 {
                // Negative / positive unsigned → negative result
                // Use i128 to avoid overflow
                let result = (*a as i128) / (*b as i128);
                if result < i64::MIN as i128 || result > i64::MAX as i128 {
                    Err(ExecutorError::DataOutOfRange(
                        "BIGINT value is out of range".to_string(),
                    ))
                } else {
                    Ok(Datum::Int(result as i64))
                }
            } else {
                // Both non-negative: safe to use unsigned
                Ok(Datum::UnsignedInt(*a as u64 / *b))
            }
        }
        (Datum::UnsignedInt(a), Datum::Int(b)) => {
            if *b < 0 {
                // Unsigned / negative → negative result
                let result = -(*a as i128 / (-*b as i128));
                if result < i64::MIN as i128 || result > i64::MAX as i128 {
                    Err(ExecutorError::DataOutOfRange(
                        "BIGINT value is out of range".to_string(),
                    ))
                } else {
                    Ok(Datum::Int(result as i64))
                }
            } else {
                // Both non-negative: safe to use unsigned
                Ok(Datum::UnsignedInt(*a / *b as u64))
            }
        }
        (Datum::Float(a), Datum::Float(b)) => checked_float_to_int((a / b).trunc()),
        (Datum::Int(a), Datum::Float(b)) => checked_float_to_int((*a as f64 / b).trunc()),
        (Datum::Float(a), Datum::Int(b)) => checked_float_to_int((a / *b as f64).trunc()),
        (Datum::UnsignedInt(a), Datum::Float(b)) => checked_float_to_int((*a as f64 / b).trunc()),
        (Datum::Float(a), Datum::UnsignedInt(b)) => checked_float_to_int((a / *b as f64).trunc()),
        // Decimal DIV: normalize scales, divide i128 values, return Int
        (
            Datum::Decimal {
                value: a,
                scale: sa,
            },
            Datum::Decimal {
                value: b,
                scale: sb,
            },
        ) => {
            let (a_norm, b_norm, _) =
                normalize_decimal_scales(*a, *sa, *b, *sb).ok_or_else(|| {
                    ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                })?;
            match a_norm.checked_div(b_norm) {
                Some(v) => {
                    // Result is an integer (truncated)
                    if v > i64::MAX as i128 || v < i64::MIN as i128 {
                        Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ))
                    } else {
                        Ok(Datum::Int(v as i64))
                    }
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "BIGINT value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { value: d, scale: s }, Datum::Int(i)) => {
            // Decimal DIV Int: divide the unscaled value by (i * 10^scale)
            let divisor = (*i as i128).checked_mul(10i128.pow(*s as u32));
            match divisor.and_then(|div| d.checked_div(div)) {
                Some(v) => {
                    if v > i64::MAX as i128 || v < i64::MIN as i128 {
                        Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ))
                    } else {
                        Ok(Datum::Int(v as i64))
                    }
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "BIGINT value is out of range".to_string(),
                )),
            }
        }
        (Datum::Int(i), Datum::Decimal { value: d, scale: s }) => {
            // Int DIV Decimal: (i * 10^scale) / d
            let numerator = (*i as i128).checked_mul(10i128.pow(*s as u32));
            match numerator.and_then(|n| n.checked_div(*d)) {
                Some(v) => {
                    if v > i64::MAX as i128 || v < i64::MIN as i128 {
                        Err(ExecutorError::DataOutOfRange(
                            "BIGINT value is out of range".to_string(),
                        ))
                    } else {
                        Ok(Datum::Int(v as i64))
                    }
                }
                None => Err(ExecutorError::DataOutOfRange(
                    "BIGINT value is out of range".to_string(),
                )),
            }
        }
        (Datum::Decimal { .. }, _) | (_, Datum::Decimal { .. }) => {
            let a = left.as_float().unwrap();
            let b = right.as_float().unwrap();
            checked_float_to_int((a / b).trunc())
        }
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot integer divide {:?} by {:?}",
            left, right
        ))),
    }
}

/// SQL XOR with three-valued logic
fn eval_xor(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (left.as_bool(), right.as_bool()) {
        (Some(a), Some(b)) => Ok(Datum::Bool(a ^ b)),
        _ => Ok(Datum::Null),
    }
}

/// NULL-safe equality: NULL <=> NULL is true, NULL <=> non-NULL is false
fn eval_spaceship(left: &Datum, right: &Datum) -> bool {
    match (left.is_null(), right.is_null()) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => left == right,
    }
}

/// Get the connection ID from user variables (stored as __sys_connection_id).
fn get_connection_id(vars: Option<&UserVariables>) -> u32 {
    if let Some(v) = vars {
        let r = v.read();
        if let Some(Datum::Int(id)) = r.get("__sys_connection_id") {
            return *id as u32;
        }
    }
    0
}

/// Clamp an overflow value to the nearest type boundary (MAX or MIN).
/// Used in non-strict INSERT mode when coercion overflows.
pub fn clamp_to_type_boundary(datum: &Datum, target: &crate::catalog::DataType) -> Datum {
    use crate::catalog::DataType;

    // Determine the numeric value to clamp
    let f_val = match datum {
        Datum::Float(f) => Some(*f),
        Datum::String(s) => s.trim().parse::<f64>().ok(),
        Datum::Int(i) => Some(*i as f64),
        Datum::UnsignedInt(u) => Some(*u as f64),
        _ => None,
    };

    match target {
        DataType::BigInt | DataType::Int | DataType::SmallInt | DataType::TinyInt => match f_val {
            Some(f) if f >= 0.0 => Datum::Int(i64::MAX),
            Some(_) => Datum::Int(i64::MIN),
            None => Datum::Int(0),
        },
        DataType::BigIntUnsigned => match f_val {
            Some(f) if f >= 0.0 => Datum::UnsignedInt(u64::MAX),
            Some(_) => Datum::UnsignedInt(0), // negative → 0
            None => Datum::UnsignedInt(0),
        },
        _ => Datum::default_for_type(target),
    }
}

/// Coerce a datum to match a target column type for implicit conversion (e.g. INSERT).
/// Only performs conversion when the source type doesn't match the target and a safe
/// conversion exists. Returns the datum unchanged for non-Bit types.
pub fn coerce_to_column_type(
    datum: Datum,
    target: &crate::catalog::DataType,
) -> ExecutorResult<Datum> {
    use crate::catalog::DataType;
    if datum.is_null() {
        return Ok(datum);
    }
    // Check if type already matches
    let already_matches = matches!(
        (&datum, target),
        (
            Datum::Int(_),
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
        ) | (Datum::UnsignedInt(_), DataType::BigIntUnsigned)
            | (Datum::Float(_), DataType::Float | DataType::Double)
            | (Datum::String(_), DataType::Varchar(_) | DataType::Text)
            | (Datum::Bool(_), DataType::Boolean)
            | (Datum::Bytes(_), DataType::Blob)
            | (Datum::Bit { .. }, DataType::Bit(_))
            | (Datum::Timestamp(_), DataType::Timestamp)
            | (Datum::Decimal { .. }, DataType::Decimal { .. })
    );
    if !already_matches {
        match eval_cast(&datum, target) {
            Ok(v) => Ok(v),
            Err(ExecutorError::DataOutOfRange(ref msg))
                if msg.starts_with("Out of range value for column") =>
            {
                // Propagate INSERT column overflow — real data integrity violation
                Err(ExecutorError::DataOutOfRange(msg.clone()))
            }
            Err(_) => {
                // Other cast failures (type mismatch, internal overflow, etc.) — fall back
                Ok(datum)
            }
        }
    } else {
        Ok(datum)
    }
}

/// CAST(expr AS type)
fn eval_cast(val: &Datum, target: &crate::catalog::DataType) -> ExecutorResult<Datum> {
    use crate::catalog::DataType;

    if val.is_null() {
        return Ok(Datum::Null);
    }

    match target {
        DataType::BigInt | DataType::Int | DataType::SmallInt | DataType::TinyInt => match val {
            Datum::Int(i) => Ok(Datum::Int(*i)),
            Datum::UnsignedInt(u) => {
                if *u > i64::MAX as u64 {
                    return Err(ExecutorError::DataOutOfRange(format!(
                        "BIGINT value is out of range, casting {} to SIGNED",
                        u
                    )));
                }
                Ok(Datum::Int(*u as i64))
            }
            Datum::Float(f) => {
                let rounded = f.round();
                // 2^63 = 9223372036854775808.0 — exact threshold
                const MAX_SIGNED: f64 = 9_223_372_036_854_775_808.0; // 2^63
                const MIN_SIGNED: f64 = -9_223_372_036_854_775_808.0; // -2^63
                if !(MIN_SIGNED..MAX_SIGNED).contains(&rounded) || f.is_nan() || f.is_infinite() {
                    return Err(ExecutorError::DataOutOfRange(format!(
                        "BIGINT value is out of range, casting {} to SIGNED",
                        f
                    )));
                }
                Ok(Datum::Int(rounded as i64))
            }
            Datum::Bool(b) => Ok(Datum::Int(if *b { 1 } else { 0 })),
            Datum::String(s) => {
                let trimmed = s.trim();
                // Try direct i64 parse
                if let Ok(v) = trimmed.parse::<i64>() {
                    return Ok(Datum::Int(v));
                }
                // Try f64 — check for overflow
                if let Ok(v) = trimmed.parse::<f64>() {
                    if v.is_finite() && v >= i64::MIN as f64 && v < 9_223_372_036_854_775_808.0 {
                        return Ok(Datum::Int(v.round() as i64));
                    }
                    return Err(ExecutorError::DataOutOfRange(format!(
                        "Out of range value for column, value '{}'",
                        &trimmed[..trimmed.len().min(64)]
                    )));
                }
                // Extract leading digits
                Ok(Datum::Int(parse_leading_int(trimmed)))
            }
            Datum::Bytes(b) => {
                // Interpret bytes as big-endian unsigned integer
                let mut val = 0i64;
                for &byte in b.iter().take(8) {
                    val = (val << 8) | byte as i64;
                }
                Ok(Datum::Int(val))
            }
            _ => Ok(Datum::Int(0)),
        },
        DataType::BigIntUnsigned => match val {
            Datum::UnsignedInt(u) => Ok(Datum::UnsignedInt(*u)),
            Datum::Int(i) => Ok(Datum::UnsignedInt(*i as u64)),
            Datum::Float(f) => Ok(Datum::UnsignedInt(f.round() as u64)),
            Datum::Bool(b) => Ok(Datum::UnsignedInt(if *b { 1 } else { 0 })),
            Datum::String(s) => {
                let trimmed = s.trim();
                // Try direct u64 parse
                if let Ok(v) = trimmed.parse::<u64>() {
                    return Ok(Datum::UnsignedInt(v));
                }
                // Try i64 for negative strings (wraps via two's complement)
                if let Ok(i) = trimmed.parse::<i64>() {
                    return Ok(Datum::UnsignedInt(i as u64));
                }
                // Try f64
                if let Ok(v) = trimmed.parse::<f64>() {
                    if v.is_finite() && v >= 0.0 && v <= u64::MAX as f64 {
                        return Ok(Datum::UnsignedInt(v.round() as u64));
                    }
                    if v.is_finite() && v < 0.0 {
                        return Ok(Datum::UnsignedInt(v.round() as i64 as u64));
                    }
                    // Overflow: clamp to u64::MAX (MySQL behavior for SELECT CAST)
                    return Ok(Datum::UnsignedInt(u64::MAX));
                }
                Ok(Datum::UnsignedInt(0))
            }
            Datum::Bit { value, .. } => Ok(Datum::UnsignedInt(*value)),
            Datum::Bytes(b) => {
                let mut val = 0u64;
                for &byte in b.iter().take(8) {
                    val = (val << 8) | byte as u64;
                }
                Ok(Datum::UnsignedInt(val))
            }
            _ => Ok(Datum::UnsignedInt(0)),
        },
        DataType::Double | DataType::Float => match val {
            Datum::Int(i) => Ok(Datum::Float(*i as f64)),
            Datum::Float(f) => Ok(Datum::Float(*f)),
            Datum::Bool(b) => Ok(Datum::Float(if *b { 1.0 } else { 0.0 })),
            Datum::String(s) => Ok(Datum::Float(s.parse::<f64>().unwrap_or(0.0))),
            _ => Ok(Datum::Float(0.0)),
        },
        DataType::Varchar(_) | DataType::Text => Ok(Datum::String(val.to_display_string())),
        DataType::Boolean => Ok(Datum::Bool(val.as_bool().unwrap_or(false))),
        DataType::Bit(w) => {
            let mask = if *w >= 64 { u64::MAX } else { (1u64 << w) - 1 };
            match val {
                Datum::Int(i) => Ok(Datum::Bit {
                    value: (*i as u64) & mask,
                    width: *w,
                }),
                Datum::Bit { value, .. } => Ok(Datum::Bit {
                    value: value & mask,
                    width: *w,
                }),
                Datum::Float(f) => Ok(Datum::Bit {
                    value: (*f as u64) & mask,
                    width: *w,
                }),
                Datum::Bool(b) => Ok(Datum::Bit {
                    value: if *b { 1 } else { 0 },
                    width: *w,
                }),
                Datum::String(s) => {
                    let v = s.parse::<u64>().unwrap_or(0);
                    Ok(Datum::Bit {
                        value: v & mask,
                        width: *w,
                    })
                }
                Datum::Bytes(b) => {
                    // Convert big-endian bytes to u64
                    let mut padded = [0u8; 8];
                    let start = 8 - b.len().min(8);
                    padded[start..].copy_from_slice(&b[..b.len().min(8)]);
                    let v = u64::from_be_bytes(padded);
                    Ok(Datum::Bit {
                        value: v & mask,
                        width: *w,
                    })
                }
                _ => Ok(Datum::Bit {
                    value: 0,
                    width: *w,
                }),
            }
        }
        DataType::Decimal { scale, .. } => {
            let s = *scale;
            let factor = 10i128.pow(s as u32);
            match val {
                Datum::Int(i) => match (*i as i128).checked_mul(factor) {
                    Some(v) => Ok(Datum::Decimal { value: v, scale: s }),
                    None => Err(ExecutorError::DataOutOfRange(
                        "DECIMAL value is out of range".to_string(),
                    )),
                },
                Datum::UnsignedInt(u) => match (*u as i128).checked_mul(factor) {
                    Some(v) => Ok(Datum::Decimal { value: v, scale: s }),
                    None => Err(ExecutorError::DataOutOfRange(
                        "DECIMAL value is out of range".to_string(),
                    )),
                },
                Datum::Float(f) => {
                    let scaled = f * factor as f64;
                    if scaled > i128::MAX as f64 || scaled < i128::MIN as f64 {
                        Err(ExecutorError::DataOutOfRange(
                            "DECIMAL value is out of range".to_string(),
                        ))
                    } else {
                        Ok(Datum::Decimal {
                            value: scaled.round() as i128,
                            scale: s,
                        })
                    }
                }
                Datum::Decimal {
                    value: v,
                    scale: vs,
                } => {
                    // Rescale
                    if *vs == s {
                        Ok(Datum::Decimal {
                            value: *v,
                            scale: s,
                        })
                    } else if s > *vs {
                        let up = 10i128.pow((s - vs) as u32);
                        match v.checked_mul(up) {
                            Some(scaled) => Ok(Datum::Decimal {
                                value: scaled,
                                scale: s,
                            }),
                            None => Err(ExecutorError::DataOutOfRange(
                                "DECIMAL value is out of range".to_string(),
                            )),
                        }
                    } else {
                        let down = 10i128.pow((vs - s) as u32);
                        Ok(Datum::Decimal {
                            value: v / down,
                            scale: s,
                        })
                    }
                }
                Datum::String(str_val) => parse_decimal_string(str_val, s),
                _ => Ok(Datum::Decimal { value: 0, scale: s }),
            }
        }
        DataType::Geometry => {
            // Geometry values pass through — Bytes/Geometry are both valid WKB
            match val {
                Datum::Geometry(_) | Datum::Bytes(_) => Ok(val.clone()),
                _ => Ok(Datum::String(val.to_display_string())),
            }
        }
        _ => Ok(Datum::String(val.to_display_string())),
    }
}

/// Parse a string into a Decimal datum with the given target scale.
fn parse_decimal_string(s: &str, target_scale: u8) -> ExecutorResult<Datum> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(Datum::Decimal {
            value: 0,
            scale: target_scale,
        });
    }

    let negative = trimmed.starts_with('-');
    let digits = if negative || trimmed.starts_with('+') {
        &trimmed[1..]
    } else {
        trimmed
    };

    let (integer_part, frac_part) = if let Some((ip, fp)) = digits.split_once('.') {
        (ip, fp)
    } else {
        (digits, "")
    };

    let src_scale = frac_part.len() as u8;

    // Build unscaled integer
    let mut combined = String::with_capacity(integer_part.len() + frac_part.len());
    combined.push_str(integer_part);
    combined.push_str(frac_part);

    let abs_val = combined.parse::<i128>().unwrap_or(0);
    let value = if negative { -abs_val } else { abs_val };

    // Rescale to target
    if src_scale == target_scale {
        Ok(Datum::Decimal {
            value,
            scale: target_scale,
        })
    } else if target_scale > src_scale {
        let factor = 10i128.pow((target_scale - src_scale) as u32);
        match value.checked_mul(factor) {
            Some(v) => Ok(Datum::Decimal {
                value: v,
                scale: target_scale,
            }),
            None => Err(ExecutorError::DataOutOfRange(
                "DECIMAL value is out of range".to_string(),
            )),
        }
    } else {
        let factor = 10i128.pow((src_scale - target_scale) as u32);
        Ok(Datum::Decimal {
            value: value / factor,
            scale: target_scale,
        })
    }
}

/// SQL AND with three-valued logic
fn eval_and(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (left.as_bool(), right.as_bool()) {
        (Some(false), _) | (_, Some(false)) => Ok(Datum::Bool(false)),
        (Some(true), Some(true)) => Ok(Datum::Bool(true)),
        _ => Ok(Datum::Null),
    }
}

/// SQL OR with three-valued logic
fn eval_or(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (left.as_bool(), right.as_bool()) {
        (Some(true), _) | (_, Some(true)) => Ok(Datum::Bool(true)),
        (Some(false), Some(false)) => Ok(Datum::Bool(false)),
        _ => Ok(Datum::Null),
    }
}

/// Evaluate a unary operation
pub fn eval_unary_op(op: &UnaryOp, val: &Datum) -> ExecutorResult<Datum> {
    match op {
        UnaryOp::Not => val
            .not()
            .ok_or_else(|| ExecutorError::InvalidOperation("NOT requires boolean".to_string())),
        UnaryOp::Neg => val.negate().ok_or_else(|| {
            // If the value is numeric but negate() failed, it's an overflow (e.g. i64::MIN)
            match val {
                Datum::Int(_) | Datum::UnsignedInt(_) | Datum::Float(_) | Datum::Decimal { .. } => {
                    ExecutorError::DataOutOfRange("BIGINT value is out of range".to_string())
                }
                _ => ExecutorError::InvalidOperation("negation requires number".to_string()),
            }
        }),
        UnaryOp::Plus => Ok(val.clone()),
        UnaryOp::BitwiseNot => {
            if val.is_null() {
                return Ok(Datum::Null);
            }
            match to_bitwise_int(val) {
                Some(v) => Ok(Datum::Int(!(v as u64) as i64)),
                None => Err(ExecutorError::InvalidOperation(
                    "bitwise NOT requires integer".to_string(),
                )),
            }
        }
    }
}

/// Evaluate a scalar function
pub fn eval_function(
    name: &str,
    args: &[Datum],
    vars: Option<&UserVariables>,
) -> ExecutorResult<Datum> {
    let name_upper = name.to_uppercase();
    match name_upper.as_str() {
        // String functions
        "UPPER" | "UCASE" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "UPPER requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::String(s) => Ok(Datum::String(s.to_uppercase())),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutorError::InvalidOperation(
                    "UPPER requires string".to_string(),
                )),
            }
        }

        "LOWER" | "LCASE" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LOWER requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::String(s) => Ok(Datum::String(s.to_lowercase())),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutorError::InvalidOperation(
                    "LOWER requires string".to_string(),
                )),
            }
        }

        "LENGTH" | "LEN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LENGTH requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::String(s) => Ok(Datum::Int(s.len() as i64)),
                Datum::Bytes(b) => Ok(Datum::Int(b.len() as i64)),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutorError::InvalidOperation(
                    "LENGTH requires string or bytes".to_string(),
                )),
            }
        }

        "SUBSTRING" | "SUBSTR" | "MID" => {
            if args.is_empty() || args.len() > 3 {
                return Err(ExecutorError::InvalidOperation(
                    "SUBSTRING requires 2-3 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let chars: Vec<char> = s.chars().collect();
            // MySQL SUBSTRING is 1-based; pos=0 returns empty
            let pos = if args.len() >= 2 {
                args[1].as_int().unwrap_or(1)
            } else {
                1
            };
            let len = if args.len() >= 3 {
                Some(args[2].as_int().unwrap_or(0) as usize)
            } else {
                None
            };
            // Convert 1-based position to 0-based index
            let start = if pos > 0 {
                (pos - 1) as usize
            } else if pos < 0 {
                // Negative position: count from end
                let from_end = (-pos) as usize;
                chars.len().saturating_sub(from_end)
            } else {
                return Ok(Datum::String(String::new()));
            };
            if start >= chars.len() {
                return Ok(Datum::String(String::new()));
            }
            let result: String = match len {
                Some(l) => chars[start..].iter().take(l).collect(),
                None => chars[start..].iter().collect(),
            };
            Ok(Datum::String(result))
        }

        "TRIM" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "TRIM requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            if args.len() >= 2 {
                // TRIM with specific character
                let what = args[1].to_display_string();
                let trimmed = s.trim_matches(|c: char| what.contains(c));
                Ok(Datum::String(trimmed.to_string()))
            } else {
                Ok(Datum::String(s.trim().to_string()))
            }
        }

        "LTRIM" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LTRIM requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            Ok(Datum::String(s.trim_start().to_string()))
        }

        "RTRIM" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "RTRIM requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            Ok(Datum::String(s.trim_end().to_string()))
        }

        "INSERT" | "_ROODB_INSERT" => {
            // INSERT(str, pos, len, newstr) — MySQL string INSERT function
            if args.len() != 4 {
                return Err(ExecutorError::InvalidOperation(
                    "INSERT requires 4 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[3].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let pos = args[1].as_int().unwrap_or(0);
            let len = args[2].as_int().unwrap_or(0).max(0) as usize;
            let newstr = args[3].to_display_string();
            if pos < 1 || pos as usize > s.len() {
                return Ok(Datum::String(s));
            }
            let start = (pos - 1) as usize;
            let end = (start + len).min(s.len());
            let mut result = String::with_capacity(s.len() + newstr.len());
            result.push_str(&s[..start]);
            result.push_str(&newstr);
            result.push_str(&s[end..]);
            Ok(Datum::String(result))
        }

        "CONCAT" => {
            let mut result = Vec::new();
            let mut has_binary = false;
            for arg in args {
                match arg {
                    Datum::String(s) => result.extend_from_slice(s.as_bytes()),
                    Datum::Int(i) => result.extend_from_slice(i.to_string().as_bytes()),
                    Datum::Float(f) => result.extend_from_slice(f.to_string().as_bytes()),
                    Datum::Bool(b) => result.extend_from_slice(if *b { b"true" } else { b"false" }),
                    Datum::Bytes(b) => {
                        result.extend_from_slice(b);
                        has_binary = true;
                    }
                    Datum::Bit { value, .. } => {
                        result.extend_from_slice(value.to_string().as_bytes())
                    }
                    Datum::Null => return Ok(Datum::Null),
                    _ => result.extend_from_slice(arg.to_display_string().as_bytes()),
                }
            }
            if has_binary {
                Ok(Datum::Bytes(result))
            } else {
                Ok(Datum::String(String::from_utf8(result).unwrap_or_default()))
            }
        }

        "CONCAT_WS" => {
            // CONCAT_WS(separator, str1, str2, ...)
            // NULL separator → NULL; NULL arguments are skipped
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "CONCAT_WS requires at least 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let sep = args[0].to_display_string();
            let parts: Vec<String> = args[1..]
                .iter()
                .filter(|a| !a.is_null())
                .map(|a| a.to_display_string())
                .collect();
            Ok(Datum::String(parts.join(&sep)))
        }

        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Datum::Null)
        }

        "NULLIF" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "NULLIF requires 2 arguments".to_string(),
                ));
            }
            if args[0] == args[1] {
                Ok(Datum::Null)
            } else {
                Ok(args[0].clone())
            }
        }

        "ABS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ABS requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Int(i) => i.checked_abs().map(Datum::Int).ok_or_else(|| {
                    ExecutorError::DataOutOfRange("BIGINT value is out of range".to_string())
                }),
                Datum::Float(f) => Ok(Datum::Float(f.abs())),
                Datum::Null => Ok(Datum::Null),
                Datum::Decimal { value, scale } => {
                    let abs_val = value.checked_abs().ok_or_else(|| {
                        ExecutorError::DataOutOfRange("DECIMAL value is out of range".to_string())
                    })?;
                    Ok(Datum::Decimal {
                        value: abs_val,
                        scale: *scale,
                    })
                }
                Datum::UnsignedInt(u) => Ok(Datum::UnsignedInt(*u)),
                Datum::String(s) => {
                    // MySQL: ABS coerces string to number
                    let trimmed = s.trim();
                    if let Ok(f) = trimmed.parse::<f64>() {
                        Ok(Datum::Float(f.abs()))
                    } else {
                        Ok(Datum::Int(parse_leading_int(trimmed).abs()))
                    }
                }
                _ => Ok(Datum::Int(0)),
            }
        }

        "ISNULL" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ISNULL requires 1 argument".to_string(),
                ));
            }
            Ok(Datum::Int(if args[0].is_null() { 1 } else { 0 }))
        }

        "IFNULL" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "IFNULL requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                Ok(args[1].clone())
            } else {
                Ok(args[0].clone())
            }
        }

        "WEIGHT_STRING" => {
            // WEIGHT_STRING(str) — returns binary collation sort key
            // Implements utf8mb4_general_ci weights: case-fold to uppercase
            // codepoint, pack as big-endian u16 per character.
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "WEIGHT_STRING requires at least 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let mut weights = Vec::with_capacity(s.len() * 2);
            for ch in s.chars() {
                // utf8mb4_general_ci: uppercase the character, use codepoint as weight
                let upper = ch.to_uppercase().next().unwrap_or(ch);
                let cp = upper as u32;
                // Pack as big-endian u16 (clamp BMP; supplementary chars use u32)
                if cp <= 0xFFFF {
                    weights.extend_from_slice(&(cp as u16).to_be_bytes());
                } else {
                    // Supplementary character: 4-byte weight
                    weights.extend_from_slice(&cp.to_be_bytes());
                }
            }
            Ok(Datum::Bytes(weights))
        }

        "HEX" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "HEX requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::Int(i) => Ok(Datum::String(format!("{:X}", i))),
                Datum::String(s) => {
                    let hex: String = s.bytes().map(|b| format!("{:02X}", b)).collect();
                    Ok(Datum::String(hex))
                }
                Datum::Bytes(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Datum::String(hex))
                }
                _ => Ok(Datum::String(format!(
                    "{:X}",
                    args[0].as_int().unwrap_or(0)
                ))),
            }
        }

        "UNHEX" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "UNHEX requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let hex_str = args[0].to_display_string();
            let trimmed = hex_str.trim();
            // Decode hex string to bytes; odd-length gets leading 0
            let padded = if trimmed.len() % 2 == 1 {
                format!("0{}", trimmed)
            } else {
                trimmed.to_string()
            };
            let bytes: Result<Vec<u8>, _> = (0..padded.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&padded[i..i + 2], 16))
                .collect();
            match bytes {
                Ok(b) => Ok(Datum::String(String::from_utf8_lossy(&b).to_string())),
                Err(_) => Ok(Datum::Null),
            }
        }

        "LPAD" => {
            if args.len() < 2 {
                return Err(ExecutorError::InvalidOperation(
                    "LPAD requires 2-3 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let len = args[1].as_int().unwrap_or(0) as usize;
            let pad = if args.len() >= 3 {
                args[2].to_display_string()
            } else {
                " ".to_string()
            };
            if pad.is_empty() || len <= s.len() {
                Ok(Datum::String(s.chars().take(len).collect()))
            } else {
                let need = len - s.len();
                let mut result = String::with_capacity(len);
                let pad_chars: Vec<char> = pad.chars().collect();
                for i in 0..need {
                    result.push(pad_chars[i % pad_chars.len()]);
                }
                result.push_str(&s);
                Ok(Datum::String(result))
            }
        }

        "RPAD" => {
            if args.len() < 2 {
                return Err(ExecutorError::InvalidOperation(
                    "RPAD requires 2-3 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let len = args[1].as_int().unwrap_or(0) as usize;
            let pad = if args.len() >= 3 {
                args[2].to_display_string()
            } else {
                " ".to_string()
            };
            if pad.is_empty() || len <= s.len() {
                Ok(Datum::String(s.chars().take(len).collect()))
            } else {
                let need = len - s.len();
                let mut result = s;
                let pad_chars: Vec<char> = pad.chars().collect();
                for i in 0..need {
                    result.push(pad_chars[i % pad_chars.len()]);
                }
                Ok(Datum::String(result))
            }
        }

        "LEFT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "LEFT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let n = args[1].as_int().unwrap_or(0).max(0) as usize;
            Ok(Datum::String(s.chars().take(n).collect()))
        }

        "RIGHT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "RIGHT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let n = args[1].as_int().unwrap_or(0).max(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Datum::String(chars[start..].iter().collect()))
        }

        "REVERSE" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "REVERSE requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::String(
                args[0].to_display_string().chars().rev().collect(),
            ))
        }

        "REPEAT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "REPEAT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let n = args[1].as_int().unwrap_or(0).max(0) as usize;
            Ok(Datum::String(s.repeat(n)))
        }

        "SPACE" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SPACE requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let n = args[0].as_int().unwrap_or(0).max(0) as usize;
            Ok(Datum::String(" ".repeat(n)))
        }

        "REPLACE" => {
            if args.len() != 3 {
                return Err(ExecutorError::InvalidOperation(
                    "REPLACE requires 3 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let from = args[1].to_display_string();
            let to = args[2].to_display_string();
            Ok(Datum::String(s.replace(&from, &to)))
        }

        "STRCMP" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "STRCMP requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let a = args[0].to_display_string();
            let b = args[1].to_display_string();
            Ok(Datum::Int(match a.cmp(&b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }))
        }

        "MOD" | "_ROODB_MOD" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "MOD requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            eval_mod(&args[0], &args[1])
        }

        "GREATEST" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "GREATEST requires arguments".to_string(),
                ));
            }
            let mut best = &args[0];
            for arg in &args[1..] {
                if arg.is_null() {
                    return Ok(Datum::Null);
                }
                if arg > best {
                    best = arg;
                }
            }
            Ok(best.clone())
        }

        "LEAST" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "LEAST requires arguments".to_string(),
                ));
            }
            let mut best = &args[0];
            for arg in &args[1..] {
                if arg.is_null() {
                    return Ok(Datum::Null);
                }
                if arg < best {
                    best = arg;
                }
            }
            Ok(best.clone())
        }

        "SIGN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SIGN requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::Int(i) => Ok(Datum::Int(i.signum())),
                Datum::Float(f) => Ok(Datum::Int(if *f > 0.0 {
                    1
                } else if *f < 0.0 {
                    -1
                } else {
                    0
                })),
                _ => Ok(Datum::Int(0)),
            }
        }

        "POW" | "POWER" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "POW requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let base = args[0].as_float().unwrap_or(0.0);
            let exp = args[1].as_float().unwrap_or(0.0);
            let result = base.powf(exp);
            if result.is_infinite() {
                return Err(ExecutorError::DataOutOfRange(
                    "DOUBLE value is out of range".to_string(),
                ));
            }
            Ok(float_or_null(result))
        }

        "SQRT" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SQRT requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).sqrt()))
        }

        "CEIL" | "CEILING" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "CEIL requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::Int(i) => Ok(Datum::Int(*i)),
                Datum::UnsignedInt(u) => Ok(Datum::UnsignedInt(*u)),
                Datum::Float(f) => {
                    let v = f.ceil();
                    if v >= 0.0 && v > i64::MAX as f64 {
                        Ok(Datum::UnsignedInt(v as u64))
                    } else {
                        Ok(Datum::Int(v as i64))
                    }
                }
                Datum::Decimal { value, scale } => {
                    if *scale == 0 {
                        // Already integer — fit into Int or UnsignedInt
                        decimal_int_result(*value)
                    } else {
                        let divisor = 10i128.pow(*scale as u32);
                        let truncated = *value / divisor;
                        let remainder = *value % divisor;
                        // Ceil: round toward +infinity
                        let result = if remainder > 0 {
                            truncated + 1
                        } else {
                            truncated
                        };
                        decimal_int_result(result)
                    }
                }
                _ => Ok(Datum::Int(0)),
            }
        }

        "FLOOR" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "FLOOR requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::Int(i) => Ok(Datum::Int(*i)),
                Datum::UnsignedInt(u) => Ok(Datum::UnsignedInt(*u)),
                Datum::Float(f) => {
                    let v = f.floor();
                    if v >= 0.0 && v > i64::MAX as f64 {
                        Ok(Datum::UnsignedInt(v as u64))
                    } else {
                        Ok(Datum::Int(v as i64))
                    }
                }
                Datum::Decimal { value, scale } => {
                    if *scale == 0 {
                        decimal_int_result(*value)
                    } else {
                        let divisor = 10i128.pow(*scale as u32);
                        let truncated = *value / divisor;
                        let remainder = *value % divisor;
                        // Floor: round toward -infinity
                        let result = if remainder < 0 {
                            truncated - 1
                        } else {
                            truncated
                        };
                        decimal_int_result(result)
                    }
                }
                _ => Ok(Datum::Int(0)),
            }
        }

        "ROUND" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "ROUND requires 1-2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let decimals = if args.len() >= 2 {
                args[1].as_int().unwrap_or(0)
            } else {
                0
            };
            // For integer types with negative decimals, use integer arithmetic
            // to avoid float precision loss on large values
            if decimals < 0 {
                match &args[0] {
                    Datum::UnsignedInt(u) => {
                        let d = (-decimals) as u32;
                        if d >= 20 {
                            return Ok(Datum::UnsignedInt(0));
                        }
                        let divisor = 10u64.pow(d);
                        let rounded = (u + divisor / 2) / divisor * divisor;
                        return Ok(Datum::UnsignedInt(rounded));
                    }
                    Datum::Int(i) => {
                        let d = (-decimals) as u32;
                        if d >= 19 {
                            return Ok(Datum::Int(0));
                        }
                        let divisor = 10i64.pow(d);
                        // Check for overflow: i64::MIN rounded can overflow
                        if *i == i64::MIN && divisor > 1 {
                            return Err(ExecutorError::DataOutOfRange(
                                "BIGINT value is out of range".to_string(),
                            ));
                        }
                        let rounded = if *i >= 0 {
                            (i + divisor / 2) / divisor * divisor
                        } else {
                            (i - divisor / 2) / divisor * divisor
                        };
                        return Ok(Datum::Int(rounded));
                    }
                    _ => {} // fall through to float path
                }
            }

            let val = args[0].as_float().unwrap_or(0.0);
            let factor = 10_f64.powi(decimals as i32);
            // Handle extreme decimals: when factor overflows, result rounds to 0
            let rounded = if factor.is_infinite() || factor == 0.0 {
                0.0
            } else {
                (val * factor).round() / factor
            };
            if decimals <= 0 {
                Ok(Datum::Int(rounded as i64))
            } else {
                Ok(Datum::Float(rounded))
            }
        }

        "TRUNCATE" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "TRUNCATE requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let decimals = args[1].as_int().unwrap_or(0);
            let val = args[0].as_float().unwrap_or(0.0);
            let factor = 10_f64.powi(decimals as i32);
            Ok(Datum::Float((val * factor).trunc() / factor))
        }

        "LOG" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::InvalidOperation(
                    "LOG requires 1-2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || (args.len() == 2 && args[1].is_null()) {
                return Ok(Datum::Null);
            }
            let result = if args.len() == 1 {
                let v = args[0].as_float().unwrap_or(0.0);
                check_log_arg(v, "log", vars)?;
                v.ln()
            } else {
                let base = args[0].as_float().unwrap_or(0.0);
                let val = args[1].as_float().unwrap_or(0.0);
                check_log_arg(val, "log", vars)?;
                val.log(base)
            };
            Ok(float_or_null(result))
        }

        "LOG2" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LOG2 requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let v = args[0].as_float().unwrap_or(0.0);
            check_log_arg(v, "log2", vars)?;
            Ok(float_or_null(v.log2()))
        }

        "LOG10" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LOG10 requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let v = args[0].as_float().unwrap_or(0.0);
            check_log_arg(v, "log10", vars)?;
            Ok(float_or_null(v.log10()))
        }

        "LN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LN requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let v = args[0].as_float().unwrap_or(0.0);
            check_log_arg(v, "ln", vars)?;
            Ok(float_or_null(v.ln()))
        }

        "EXP" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "EXP requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let result = args[0].as_float().unwrap_or(0.0).exp();
            if result.is_infinite() {
                return Err(ExecutorError::DataOutOfRange(
                    "DOUBLE value is out of range".to_string(),
                ));
            }
            Ok(Datum::Float(result))
        }

        "PI" => Ok(Datum::Float(std::f64::consts::PI)),

        "RAND" => {
            // MySQL-compatible RAND([seed])
            // RAND(NULL) seeds with 0 (same as MySQL behavior)
            let seed_arg = if !args.is_empty() {
                Some(args[0].as_int().unwrap_or(0) as u64)
            } else {
                None
            };
            RAND_STATE.with(|state| {
                let mut st = state.borrow_mut();
                if let Some(seed) = seed_arg {
                    let (s1, s2) = mysql_rand_seed(seed);
                    st.0 = s1;
                    st.1 = s2;
                }
                let (ref mut s1, ref mut s2) = *st;
                let result = mysql_rand_next(s1, s2);
                Ok(Datum::Float(result))
            })
        }

        "RADIANS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "RADIANS requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).to_radians()))
        }

        "DEGREES" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "DEGREES requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            check_float_overflow(args[0].as_float().unwrap_or(0.0).to_degrees())
        }

        "SIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SIN requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).sin()))
        }

        "COS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "COS requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).cos()))
        }

        "TAN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "TAN requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).tan()))
        }

        "ASIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ASIN requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).asin()))
        }

        "ACOS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ACOS requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).acos()))
        }

        "ATAN" | "ATAN2" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "ATAN requires 1-2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            if args.len() == 1 {
                Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).atan()))
            } else {
                let y = args[0].as_float().unwrap_or(0.0);
                let x = args[1].as_float().unwrap_or(0.0);
                Ok(Datum::Float(y.atan2(x)))
            }
        }

        "COT" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "COT requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let v = args[0].as_float().unwrap_or(0.0);
            let result = 1.0 / v.tan();
            if result.is_infinite() {
                return Err(ExecutorError::DataOutOfRange(
                    "DOUBLE value is out of range in 'cot'".to_string(),
                ));
            }
            Ok(float_or_null(result))
        }

        "CONV" => {
            if args.len() != 3 {
                return Err(ExecutorError::InvalidOperation(
                    "CONV requires 3 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let from_base = args[1].as_int().unwrap_or(10) as u32;
            let to_base = args[2].as_int().unwrap_or(10) as u32;
            if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
                return Ok(Datum::Null);
            }
            match i64::from_str_radix(&s, from_base) {
                Ok(val) => Ok(Datum::String(radix_string(val, to_base))),
                Err(_) => Ok(Datum::Null),
            }
        }

        "BIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "BIN requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::String(format!(
                "{:b}",
                args[0].as_int().unwrap_or(0)
            )))
        }

        "OCT" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "OCT requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::String(format!(
                "{:o}",
                args[0].as_int().unwrap_or(0)
            )))
        }

        "BIT_LENGTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "BIT_LENGTH requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::String(s) => Ok(Datum::Int(s.len() as i64 * 8)),
                Datum::Bytes(b) => Ok(Datum::Int(b.len() as i64 * 8)),
                other => Ok(Datum::Int(other.to_display_string().len() as i64 * 8)),
            }
        }

        "OCTET_LENGTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "OCTET_LENGTH requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::String(s) => Ok(Datum::Int(s.len() as i64)),
                Datum::Bytes(b) => Ok(Datum::Int(b.len() as i64)),
                other => Ok(Datum::Int(other.to_display_string().len() as i64)),
            }
        }

        "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "CHAR_LENGTH requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::String(s) => Ok(Datum::Int(s.chars().count() as i64)),
                other => Ok(Datum::Int(other.to_display_string().chars().count() as i64)),
            }
        }

        "LOCATE" | "POSITION" => {
            if args.len() < 2 {
                return Err(ExecutorError::InvalidOperation(
                    "LOCATE requires 2-3 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let substr = args[0].to_display_string();
            let s = args[1].to_display_string();
            let start = if args.len() >= 3 {
                (args[2].as_int().unwrap_or(1) - 1).max(0) as usize
            } else {
                0
            };
            if start >= s.len() {
                Ok(Datum::Int(0))
            } else {
                match s[start..].find(&substr) {
                    Some(pos) => Ok(Datum::Int((pos + start + 1) as i64)),
                    None => Ok(Datum::Int(0)),
                }
            }
        }

        "INSTR" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "INSTR requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let substr = args[1].to_display_string();
            match s.find(&substr) {
                Some(pos) => Ok(Datum::Int((pos + 1) as i64)),
                None => Ok(Datum::Int(0)),
            }
        }

        "ASCII" | "ORD" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ASCII requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            Ok(Datum::Int(s.bytes().next().unwrap_or(0) as i64))
        }

        "CHAR" => {
            let mut result = String::new();
            for arg in args {
                if let Some(code) = arg.as_int() {
                    if let Some(c) = char::from_u32(code as u32) {
                        result.push(c);
                    }
                }
            }
            Ok(Datum::String(result))
        }

        "FORMAT" => {
            if args.len() < 2 {
                return Err(ExecutorError::InvalidOperation(
                    "FORMAT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let val = args[0].as_float().unwrap_or(0.0);
            let decimals = args[1].as_int().unwrap_or(0).max(0) as usize;
            let formatted = format!("{:.prec$}", val, prec = decimals);
            // Add thousand separators
            let parts: Vec<&str> = formatted.splitn(2, '.').collect();
            let int_part = parts[0];
            let dec_part = if parts.len() > 1 {
                Some(parts[1])
            } else {
                None
            };
            let negative = int_part.starts_with('-');
            let digits: &str = if negative { &int_part[1..] } else { int_part };
            let mut with_commas = String::new();
            for (i, c) in digits.chars().rev().enumerate() {
                if i > 0 && i % 3 == 0 {
                    with_commas.push(',');
                }
                with_commas.push(c);
            }
            let with_commas: String = with_commas.chars().rev().collect();
            let mut result = if negative {
                format!("-{}", with_commas)
            } else {
                with_commas
            };
            if let Some(dec) = dec_part {
                result.push('.');
                result.push_str(dec);
            }
            Ok(Datum::String(result))
        }

        "SLEEP" => {
            // SLEEP(seconds) — for compatibility, return 0 immediately
            Ok(Datum::Int(0))
        }

        "CONNECTION_ID" => Ok(Datum::Int(0)),

        "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => {
            Ok(Datum::String("root@localhost".to_string()))
        }

        "VERSION" => Ok(Datum::String("8.0.0-RooDB".to_string())),

        "DATABASE" | "SCHEMA" => Ok(Datum::String("test".to_string())),

        "LAST_INSERT_ID" => Ok(Datum::Int(0)),

        "FOUND_ROWS" | "ROW_COUNT" => Ok(Datum::Int(0)),

        "TO_DAYS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "TO_DAYS requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            // Parse date string and convert to days since year 0
            let s = args[0].to_display_string();
            match parse_date_to_days(&s) {
                Some(days) => Ok(Datum::Int(days)),
                None => Ok(Datum::Null),
            }
        }

        "DATEDIFF" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "DATEDIFF requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let d1 = parse_date_to_days(&args[0].to_display_string());
            let d2 = parse_date_to_days(&args[1].to_display_string());
            match (d1, d2) {
                (Some(a), Some(b)) => Ok(Datum::Int(a - b)),
                _ => Ok(Datum::Null),
            }
        }

        "YEAR" | "MONTH" | "DAYOFMONTH" | "DAY" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(format!(
                    "{} requires 1 argument",
                    name_upper
                )));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            match parse_to_parts(&s) {
                Some(p) => {
                    let val = match name_upper.as_str() {
                        "YEAR" => p.year,
                        "MONTH" => p.month as i64,
                        "DAYOFMONTH" | "DAY" => p.day as i64,
                        _ => unreachable!(),
                    };
                    Ok(Datum::Int(val))
                }
                None => Ok(Datum::Null),
            }
        }

        "FIELD" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "FIELD requires at least 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Int(0));
            }
            let search = &args[0];
            for (i, arg) in args[1..].iter().enumerate() {
                if !arg.is_null() && search == arg {
                    return Ok(Datum::Int((i + 1) as i64));
                }
            }
            Ok(Datum::Int(0))
        }

        "ELT" => {
            if args.len() < 2 {
                return Err(ExecutorError::InvalidOperation(
                    "ELT requires at least 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let idx = args[0].as_int().unwrap_or(0);
            if idx < 1 || idx as usize > args.len() - 1 {
                return Ok(Datum::Null);
            }
            Ok(args[idx as usize].clone())
        }

        "FIND_IN_SET" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "FIND_IN_SET requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let needle = args[0].to_display_string();
            let haystack = args[1].to_display_string();
            for (i, item) in haystack.split(',').enumerate() {
                if item == needle {
                    return Ok(Datum::Int((i + 1) as i64));
                }
            }
            Ok(Datum::Int(0))
        }

        "TIMEDIFF" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "TIMEDIFF requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s1 = args[0].to_display_string();
            let s2 = args[1].to_display_string();
            let secs1 = parse_time_to_secs(&s1);
            let secs2 = parse_time_to_secs(&s2);
            let diff = secs1 - secs2;
            let sign = if diff < 0 { "-" } else { "" };
            let abs_diff = diff.unsigned_abs();
            let hours = abs_diff / 3600;
            let mins = (abs_diff % 3600) / 60;
            let secs = abs_diff % 60;
            Ok(Datum::String(format!(
                "{}{:02}:{:02}:{:02}",
                sign, hours, mins, secs
            )))
        }

        "GET_LOCK" => {
            if args.len() < 2 || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let lock_name = args[0].to_display_string();
            let conn_id = get_connection_id(vars);
            let mgr = crate::server::locks::global_lock_manager();
            Ok(Datum::Int(mgr.get_lock(&lock_name, conn_id)))
        }

        "RELEASE_LOCK" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let lock_name = args[0].to_display_string();
            let conn_id = get_connection_id(vars);
            let mgr = crate::server::locks::global_lock_manager();
            match mgr.release_lock(&lock_name, conn_id) {
                Some(v) => Ok(Datum::Int(v)),
                None => Ok(Datum::Null),
            }
        }

        "IS_FREE_LOCK" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let lock_name = args[0].to_display_string();
            let mgr = crate::server::locks::global_lock_manager();
            Ok(Datum::Int(mgr.is_free_lock(&lock_name)))
        }

        "INET_NTOA" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let n = args[0].as_int().unwrap_or(0) as u32;
            Ok(Datum::String(format!(
                "{}.{}.{}.{}",
                (n >> 24) & 0xFF,
                (n >> 16) & 0xFF,
                (n >> 8) & 0xFF,
                n & 0xFF
            )))
        }

        "INET_ATON" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let parts: Vec<u32> = s.split('.').filter_map(|p| p.parse().ok()).collect();
            if parts.len() != 4 {
                return Ok(Datum::Null);
            }
            let val = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
            Ok(Datum::Int(val as i64))
        }

        "INET6_ATON" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let trimmed = s.trim();
            // Try IPv6
            if let Ok(addr) = trimmed.parse::<std::net::Ipv6Addr>() {
                return Ok(Datum::Bytes(addr.octets().to_vec()));
            }
            // Try IPv4 (returns 4-byte binary)
            if let Ok(addr) = trimmed.parse::<std::net::Ipv4Addr>() {
                return Ok(Datum::Bytes(addr.octets().to_vec()));
            }
            Ok(Datum::Null)
        }

        "INET6_NTOA" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            match &args[0] {
                Datum::Bytes(b) if b.len() == 16 => {
                    let octets: [u8; 16] = b[..16].try_into().unwrap();
                    let addr = std::net::Ipv6Addr::from(octets);
                    Ok(Datum::String(addr.to_string()))
                }
                Datum::Bytes(b) if b.len() == 4 => {
                    let octets: [u8; 4] = b[..4].try_into().unwrap();
                    let addr = std::net::Ipv4Addr::from(octets);
                    Ok(Datum::String(addr.to_string()))
                }
                _ => Ok(Datum::Null),
            }
        }

        "CURDATE" | "CURRENT_DATE" => {
            let p = system_now_parts();
            Ok(Datum::String(p.format_date()))
        }
        "NOW" | "CURRENT_TIMESTAMP" | "SYSDATE" | "LOCALTIME" | "LOCALTIMESTAMP" => {
            let p = system_now_parts();
            Ok(Datum::String(p.format_datetime()))
        }
        "CURTIME" | "CURRENT_TIME" => {
            let p = system_now_parts();
            Ok(Datum::String(p.format_time()))
        }

        // ── Date/time format-string functions ──
        "DATE_FORMAT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "DATE_FORMAT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let fmt = args[1].to_display_string();
            match parse_to_parts(&s) {
                Some(p) => Ok(Datum::String(format_datetime_fmt(&p, &fmt))),
                None => Ok(Datum::Null),
            }
        }

        "TIME_FORMAT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "TIME_FORMAT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let fmt = args[1].to_display_string();
            // TIME_FORMAT: parse as time, date parts stay zero
            match parse_to_parts(&s) {
                Some(p) => {
                    let time_only = DateTimeParts {
                        year: 0,
                        month: 0,
                        day: 0,
                        hour: p.hour,
                        minute: p.minute,
                        second: p.second,
                        microsecond: p.microsecond,
                        negative: p.negative,
                    };
                    Ok(Datum::String(format_datetime_fmt(&time_only, &fmt)))
                }
                None => Ok(Datum::Null),
            }
        }

        "STR_TO_DATE" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "STR_TO_DATE requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let input = args[0].to_display_string();
            let fmt = args[1].to_display_string();
            match parse_datetime_fmt(&input, &fmt) {
                Some(p) => {
                    let has_date = p.year != 0 || p.month != 0 || p.day != 0;
                    let has_time =
                        p.hour != 0 || p.minute != 0 || p.second != 0 || p.microsecond != 0;
                    if has_date && has_time {
                        Ok(Datum::String(p.format_datetime()))
                    } else if has_date {
                        Ok(Datum::String(p.format_date()))
                    } else {
                        Ok(Datum::String(p.format_time()))
                    }
                }
                None => Ok(Datum::Null),
            }
        }

        "GET_FORMAT" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "GET_FORMAT requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let type_name = args[0].to_display_string().to_uppercase();
            let standard = args[1].to_display_string().to_uppercase();
            match get_format_string(&type_name, &standard) {
                Some(fmt) => Ok(Datum::String(fmt.to_string())),
                None => Ok(Datum::Null),
            }
        }

        "FROM_UNIXTIME" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::InvalidOperation(
                    "FROM_UNIXTIME requires 1 or 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let ts = args[0].as_int().unwrap_or(0);
            let p = DateTimeParts::from_unix_seconds(ts);
            if args.len() == 2 {
                if args[1].is_null() {
                    return Ok(Datum::Null);
                }
                let fmt = args[1].to_display_string();
                Ok(Datum::String(format_datetime_fmt(&p, &fmt)))
            } else {
                Ok(Datum::String(p.format_datetime()))
            }
        }

        // ── Date/time extraction functions ──
        "HOUR" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "HOUR requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.hour as i64)),
                None => Ok(Datum::Null),
            }
        }

        "MINUTE" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "MINUTE requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.minute as i64)),
                None => Ok(Datum::Null),
            }
        }

        "SECOND" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SECOND requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.second as i64)),
                None => Ok(Datum::Null),
            }
        }

        "MICROSECOND" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "MICROSECOND requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.microsecond as i64)),
                None => Ok(Datum::Null),
            }
        }

        "DAYOFWEEK" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "DAYOFWEEK requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.day_of_week() as i64)),
                None => Ok(Datum::Null),
            }
        }

        "DAYOFYEAR" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "DAYOFYEAR requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.day_of_year() as i64)),
                None => Ok(Datum::Null),
            }
        }

        "WEEKDAY" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "WEEKDAY requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.weekday() as i64)),
                None => Ok(Datum::Null),
            }
        }

        "DAYNAME" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "DAYNAME requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => {
                    if p.month == 0 || p.day == 0 {
                        return Ok(Datum::Null);
                    }
                    let dow = p.day_of_week_zero() as usize;
                    Ok(Datum::String(WEEKDAY_NAMES[dow % 7].to_string()))
                }
                None => Ok(Datum::Null),
            }
        }

        "MONTHNAME" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "MONTHNAME requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => {
                    if p.month == 0 || p.month > 12 {
                        return Ok(Datum::Null);
                    }
                    Ok(Datum::String(MONTH_NAMES[p.month as usize].to_string()))
                }
                None => Ok(Datum::Null),
            }
        }

        "WEEK" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::InvalidOperation(
                    "WEEK requires 1 or 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let mode = if args.len() == 2 {
                args[1].as_int().unwrap_or(0) as u32
            } else {
                0
            };
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.week_number(mode) as i64)),
                None => Ok(Datum::Null),
            }
        }

        "QUARTER" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "QUARTER requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => {
                    let q = p.quarter();
                    if q == 0 {
                        Ok(Datum::Null)
                    } else {
                        Ok(Datum::Int(q as i64))
                    }
                }
                None => Ok(Datum::Null),
            }
        }

        "YEARWEEK" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::InvalidOperation(
                    "YEARWEEK requires 1 or 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let mode = if args.len() == 2 {
                args[1].as_int().unwrap_or(0) as u32
            } else {
                0
            };
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => {
                    let wk = p.week_number(mode);
                    // If early January falls in the last week of the prior year,
                    // YEARWEEK must report the prior year
                    let display_year = if wk > 51 && p.month == 1 {
                        p.year - 1
                    } else {
                        p.year
                    };
                    Ok(Datum::Int(display_year * 100 + wk as i64))
                }
                None => Ok(Datum::Null),
            }
        }

        // ── Conversion/constructor functions ──
        "UNIX_TIMESTAMP" => {
            if args.is_empty() {
                // 0-arg: current Unix timestamp
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                return Ok(Datum::Int(now as i64));
            }
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "UNIX_TIMESTAMP requires 0 or 1 arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::Int(p.to_unix_seconds())),
                None => Ok(Datum::Null),
            }
        }

        "LAST_DAY" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "LAST_DAY requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => {
                    if p.month == 0 || p.month > 12 {
                        return Ok(Datum::Null);
                    }
                    let last = days_in_month(p.year, p.month);
                    Ok(Datum::String(format!(
                        "{:04}-{:02}-{:02}",
                        p.year, p.month, last
                    )))
                }
                None => Ok(Datum::Null),
            }
        }

        "MAKEDATE" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "MAKEDATE requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let year = args[0].as_int().unwrap_or(0);
            let doy = args[1].as_int().unwrap_or(0);
            if doy < 1 {
                return Ok(Datum::Null);
            }
            // Jan 1 of year + (doy - 1) days
            let epoch_days = ymd_to_days_from_epoch(year, 1, 1) + doy - 1;
            let (y, m, d) = days_from_epoch_to_ymd(epoch_days);
            Ok(Datum::String(format!("{:04}-{:02}-{:02}", y, m, d)))
        }

        "MAKETIME" => {
            if args.len() != 3 {
                return Err(ExecutorError::InvalidOperation(
                    "MAKETIME requires 3 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                return Ok(Datum::Null);
            }
            let h = args[0].as_int().unwrap_or(0);
            let m = args[1].as_int().unwrap_or(0);
            let s = args[2].as_int().unwrap_or(0);
            Ok(Datum::String(format!("{:02}:{:02}:{:02}", h, m, s)))
        }

        "SEC_TO_TIME" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "SEC_TO_TIME requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let total = args[0].as_int().unwrap_or(0);
            Ok(Datum::String(secs_to_time_string(total)))
        }

        "TIME_TO_SEC" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "TIME_TO_SEC requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            Ok(Datum::Int(parse_time_to_secs(&s)))
        }

        "PERIOD_ADD" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "PERIOD_ADD requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let period = args[0].as_int().unwrap_or(0);
            let n = args[1].as_int().unwrap_or(0);
            let total = period_to_months(period) + n;
            Ok(Datum::Int(months_to_period(total)))
        }

        "PERIOD_DIFF" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "PERIOD_DIFF requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let p1 = args[0].as_int().unwrap_or(0);
            let p2 = args[1].as_int().unwrap_or(0);
            let diff = period_to_months(p1) - period_to_months(p2);
            Ok(Datum::Int(diff))
        }

        "DATE" => {
            // DATE(expr) — extract date part from datetime
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "DATE requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::String(p.format_date())),
                None => Ok(Datum::Null),
            }
        }

        "TIME" => {
            // TIME(expr) — extract time part from datetime
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "TIME requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match parse_to_parts(&args[0].to_display_string()) {
                Some(p) => Ok(Datum::String(p.format_time())),
                None => Ok(Datum::Null),
            }
        }

        // STDDEV/VARIANCE family: handled as aggregate functions in aggregate.rs.
        // If called as scalar (e.g. SELECT STDDEV(1)), treat as single-value aggregate.
        "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "STD" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            // Single value: population stddev/variance is 0
            Ok(Datum::Float(0.0))
        }

        "REGEXP" => {
            if args.len() != 2 {
                return Err(ExecutorError::InvalidOperation(
                    "REGEXP requires 2 arguments".to_string(),
                ));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Datum::Null);
            }
            let s = args[0].to_display_string();
            let pattern = args[1].to_display_string();
            match regex::Regex::new(&pattern) {
                Ok(re) => Ok(Datum::Bool(re.is_match(&s))),
                Err(_) => Ok(Datum::Null),
            }
        }

        "BIT_COUNT" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "BIT_COUNT requires 1 argument".to_string(),
                ));
            }
            match to_bitwise_int(&args[0]) {
                Some(v) => Ok(Datum::Int((v as u64).count_ones() as i64)),
                None => Ok(Datum::Null),
            }
        }

        "CRC32" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "CRC32 requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            // Simple CRC32 implementation
            let s = args[0].to_display_string();
            let crc = crc32_compute(s.as_bytes());
            Ok(Datum::Int(crc as i64))
        }

        // ============ Geometry/Spatial Functions ============
        "ST_GEOMFROMTEXT" | "ST_GEOMETRYFROMTEXT" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(
                    "ST_GeomFromText requires at least 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkt = args[0].to_display_string();
            match crate::executor::geometry::wkt_to_wkb(&wkt) {
                Ok(wkb) => Ok(Datum::Geometry(wkb)),
                Err(e) => Err(ExecutorError::InvalidOperation(format!(
                    "Invalid WKT: {}",
                    e
                ))),
            }
        }

        // WKB (Well-Known Binary) constructors — accept binary input, return Geometry
        "ST_GEOMFROMWKB"
        | "ST_GEOMETRYFROMWKB"
        | "ST_LINESTRINGFROMWKB"
        | "ST_POINTFROMWKB"
        | "ST_POLYFROMWKB"
        | "ST_POLYGONFROMWKB" => {
            if args.is_empty() {
                return Err(ExecutorError::InvalidOperation(format!(
                    "{} requires at least 1 argument",
                    name_upper
                )));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            match &args[0] {
                Datum::Bytes(b) | Datum::Geometry(b) => Ok(Datum::Geometry(b.clone())),
                // MySQL accepts non-binary args and returns a geometry (which then
                // fails in arithmetic operators like DIV)
                _ => {
                    let bytes = args[0].to_display_string().into_bytes();
                    Ok(Datum::Geometry(bytes))
                }
            }
        }

        "ST_X" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ST_X requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkb = match &args[0] {
                Datum::Geometry(b) | Datum::Bytes(b) => b,
                _ => {
                    return Err(ExecutorError::InvalidOperation(
                        "ST_X requires a geometry argument".to_string(),
                    ))
                }
            };
            match crate::executor::geometry::st_x(wkb) {
                Ok(x) => Ok(Datum::Float(x)),
                Err(e) => Err(ExecutorError::InvalidOperation(e)),
            }
        }

        "ST_Y" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ST_Y requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkb = match &args[0] {
                Datum::Geometry(b) | Datum::Bytes(b) => b,
                _ => {
                    return Err(ExecutorError::InvalidOperation(
                        "ST_Y requires a geometry argument".to_string(),
                    ))
                }
            };
            match crate::executor::geometry::st_y(wkb) {
                Ok(y) => Ok(Datum::Float(y)),
                Err(e) => Err(ExecutorError::InvalidOperation(e)),
            }
        }

        "ST_NUMPOINTS" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ST_NumPoints requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkb = match &args[0] {
                Datum::Geometry(b) | Datum::Bytes(b) => b,
                _ => {
                    return Err(ExecutorError::InvalidOperation(
                        "ST_NumPoints requires a geometry argument".to_string(),
                    ))
                }
            };
            match crate::executor::geometry::st_numpoints(wkb) {
                Ok(n) => Ok(Datum::Int(n)),
                Err(e) => Err(ExecutorError::InvalidOperation(e)),
            }
        }

        "ST_LENGTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ST_Length requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkb = match &args[0] {
                Datum::Geometry(b) | Datum::Bytes(b) => b,
                _ => {
                    return Err(ExecutorError::InvalidOperation(
                        "ST_Length requires a geometry argument".to_string(),
                    ))
                }
            };
            match crate::executor::geometry::st_length(wkb) {
                Ok(l) => Ok(Datum::Float(l)),
                Err(e) => Err(ExecutorError::InvalidOperation(e)),
            }
        }

        "ST_AREA" => {
            if args.len() != 1 {
                return Err(ExecutorError::InvalidOperation(
                    "ST_Area requires 1 argument".to_string(),
                ));
            }
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            let wkb = match &args[0] {
                Datum::Geometry(b) | Datum::Bytes(b) => b,
                _ => {
                    return Err(ExecutorError::InvalidOperation(
                        "ST_Area requires a geometry argument".to_string(),
                    ))
                }
            };
            match crate::executor::geometry::st_area(wkb) {
                Ok(a) => Ok(Datum::Float(a)),
                Err(e) => Err(ExecutorError::InvalidOperation(e)),
            }
        }

        // Note: Aggregate functions (COUNT, SUM, AVG, MIN, MAX) are handled
        // by the Aggregate executor, not here

        // Check for user-defined functions via __udf_NAME in user variables
        _ => {
            if let Some(udf_result) = try_eval_udf(&name_upper, args, vars) {
                return udf_result;
            }
            Err(ExecutorError::InvalidOperation(format!(
                "unknown function: {}",
                name
            )))
        }
    }
}

// ── Date/Time infrastructure ──────────────────────────────────────────

const MONTH_NAMES: [&str; 13] = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const MONTH_ABBREVS: [&str; 13] = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const WEEKDAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const WEEKDAY_ABBREVS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// Hinnant civil_from_days: convert days since Unix epoch to (year, month, day)
fn days_from_epoch_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Inverse Hinnant: (year, month, day) to days since Unix epoch
fn ymd_to_days_from_epoch(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

fn is_leap_year(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

struct DateTimeParts {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
    negative: bool,
}

impl DateTimeParts {
    fn day_of_year(&self) -> u32 {
        let mut doy = self.day;
        for m in 1..self.month {
            doy += days_in_month(self.year, m);
        }
        doy
    }

    /// 0=Sunday..6=Saturday (internal), MySQL DAYOFWEEK = 1=Sun..7=Sat
    fn day_of_week_zero(&self) -> u32 {
        let d = ymd_to_days_from_epoch(self.year, self.month, self.day);
        // Unix epoch (1970-01-01) was Thursday (4)
        ((d % 7 + 4 + 7) % 7) as u32
    }

    /// MySQL DAYOFWEEK: 1=Sunday..7=Saturday
    fn day_of_week(&self) -> u32 {
        self.day_of_week_zero() + 1
    }

    /// MySQL WEEKDAY: 0=Monday..6=Sunday
    fn weekday(&self) -> u32 {
        let dow0 = self.day_of_week_zero(); // 0=Sun
        if dow0 == 0 {
            6
        } else {
            dow0 - 1
        }
    }

    fn quarter(&self) -> u32 {
        if self.month == 0 {
            return 0;
        }
        (self.month - 1) / 3 + 1
    }

    /// MySQL WEEK function with 8 modes (0-7)
    ///
    /// | Mode | 1st day | Range | Week 1 is the first week...      |
    /// |------|---------|-------|----------------------------------|
    /// |  0   | Sunday  | 0-53  | with a Sunday in this year       |
    /// |  1   | Monday  | 0-53  | with 4 or more days this year    |
    /// |  2   | Sunday  | 1-53  | with a Sunday in this year       |
    /// |  3   | Monday  | 1-53  | with 4 or more days this year    |
    /// |  4   | Sunday  | 0-53  | with 4 or more days this year    |
    /// |  5   | Monday  | 0-53  | with a Monday in this year       |
    /// |  6   | Sunday  | 1-53  | with 4 or more days this year    |
    /// |  7   | Monday  | 1-53  | with a Monday in this year       |
    fn week_number(&self, mode: u32) -> u32 {
        let mode = mode % 8;
        let first_day: u32 = match mode {
            0 | 2 | 4 | 6 => 0, // Sunday
            _ => 1,             // Monday
        };
        let range_1_53 = matches!(mode, 2 | 3 | 6 | 7);
        // "4+ days" rule: week 1 needs >=4 days in this year (threshold=3)
        // "first [day]" rule: week 1 starts on first occurrence of first_day (threshold=0)
        let four_day_rule = matches!(mode, 1 | 3 | 4 | 6);
        let threshold: i32 = if four_day_rule { 3 } else { 0 };

        let jan1_dow = {
            let d = ymd_to_days_from_epoch(self.year, 1, 1);
            ((d % 7 + 4 + 7) % 7) as u32 // 0=Sun
        };
        let doy = self.day_of_year() as i32;

        // days_offset: how many days Jan 1 is past the "first day of week"
        let days_offset = if jan1_dow >= first_day {
            (jan1_dow - first_day) as i32
        } else {
            (7 - (first_day - jan1_dow)) as i32
        };

        if days_offset <= threshold {
            // Jan 1 is in week 1 of this year
            let week = (doy + days_offset - 1) / 7 + 1;
            if !range_1_53 {
                return week as u32;
            }
            week as u32
        } else {
            // Jan 1 is in the last week of the previous year (or week 0)
            let adjusted = doy - (7 - days_offset);
            if adjusted <= 0 {
                if !range_1_53 {
                    return 0;
                }
                // Belongs to last year's last week — compute recursively
                let prev = DateTimeParts {
                    year: self.year - 1,
                    month: 12,
                    day: 31,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    microsecond: 0,
                    negative: false,
                };
                return prev.week_number(mode);
            }
            let week = (adjusted - 1) / 7 + 1;
            if !range_1_53 {
                return week as u32;
            }
            week as u32
        }
    }

    fn to_unix_seconds(&self) -> i64 {
        let days = ymd_to_days_from_epoch(self.year, self.month, self.day);
        days * 86400 + self.hour as i64 * 3600 + self.minute as i64 * 60 + self.second as i64
    }

    fn from_unix_seconds(secs: i64) -> Self {
        let neg = secs < 0;
        let abs = secs.unsigned_abs();
        let day_secs = if neg {
            let rem = abs % 86400;
            if rem == 0 {
                0
            } else {
                86400 - rem
            }
        } else {
            abs % 86400
        };
        let days = if neg {
            -(abs as i64 + 86399) / 86400
        } else {
            secs / 86400
        };
        let (y, m, d) = days_from_epoch_to_ymd(days);
        DateTimeParts {
            year: y,
            month: m,
            day: d,
            hour: (day_secs / 3600) as u32,
            minute: ((day_secs % 3600) / 60) as u32,
            second: (day_secs % 60) as u32,
            microsecond: 0,
            negative: false,
        }
    }

    fn format_date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    fn format_time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    fn format_datetime(&self) -> String {
        format!("{} {}", self.format_date(), self.format_time())
    }
}

/// Parse MySQL date/time/datetime input string into parts
fn parse_to_parts(input: &str) -> Option<DateTimeParts> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }

    // Try "YYYY-MM-DD HH:MM:SS[.ffffff]"
    if s.len() >= 19 && s.as_bytes()[4] == b'-' && s.as_bytes()[10] == b' ' {
        let year: i64 = s[0..4].parse().ok()?;
        let month: u32 = s[5..7].parse().ok()?;
        let day: u32 = s[8..10].parse().ok()?;
        let hour: u32 = s[11..13].parse().ok()?;
        let minute: u32 = s[14..16].parse().ok()?;
        let second: u32 = s[17..19].parse().ok()?;
        let microsecond = if s.len() > 20 && s.as_bytes()[19] == b'.' {
            let frac = &s[20..];
            let padded = format!("{:0<6}", &frac[..frac.len().min(6)]);
            padded.parse().unwrap_or(0)
        } else {
            0
        };
        return Some(DateTimeParts {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            negative: false,
        });
    }

    // Try "YYYY-MM-DD"
    if s.len() >= 10 && s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-' {
        let year: i64 = s[0..4].parse().ok()?;
        let month: u32 = s[5..7].parse().ok()?;
        let day: u32 = s[8..10].parse().ok()?;
        return Some(DateTimeParts {
            year,
            month,
            day,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
            negative: false,
        });
    }

    // Try negative time "-HH:MM:SS"
    if let Some(rest) = s.strip_prefix('-') {
        if let Some(p) = parse_time_parts(rest) {
            return Some(DateTimeParts {
                negative: true,
                ..p
            });
        }
    }

    // Try "HH:MM:SS[.ffffff]"
    if let Some(p) = parse_time_parts(s) {
        return Some(p);
    }

    // Try compact "YYYYMMDDHHMMSS" (14 digits) or "YYYYMMDD" (8 digits)
    if s.chars().all(|c| c.is_ascii_digit()) {
        if s.len() == 14 {
            let year: i64 = s[0..4].parse().ok()?;
            let month: u32 = s[4..6].parse().ok()?;
            let day: u32 = s[6..8].parse().ok()?;
            let hour: u32 = s[8..10].parse().ok()?;
            let minute: u32 = s[10..12].parse().ok()?;
            let second: u32 = s[12..14].parse().ok()?;
            return Some(DateTimeParts {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond: 0,
                negative: false,
            });
        } else if s.len() == 8 {
            let year: i64 = s[0..4].parse().ok()?;
            let month: u32 = s[4..6].parse().ok()?;
            let day: u32 = s[6..8].parse().ok()?;
            return Some(DateTimeParts {
                year,
                month,
                day,
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
                negative: false,
            });
        }
    }

    // Try integer coercion (e.g., Datum would have produced "0")
    if s == "0" {
        return Some(DateTimeParts {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
            negative: false,
        });
    }

    None
}

/// Parse a time-only string "HH:MM:SS[.ffffff]" or "H:MM:SS"
fn parse_time_parts(s: &str) -> Option<DateTimeParts> {
    let colon1 = s.find(':')?;
    let rest = &s[colon1 + 1..];
    let colon2 = rest.find(':')?;
    let hour: u32 = s[..colon1].parse().ok()?;
    let minute: u32 = rest[..colon2].parse().ok()?;
    let sec_part = &rest[colon2 + 1..];
    let (sec_str, micro) = if let Some(dot) = sec_part.find('.') {
        let frac = &sec_part[dot + 1..];
        let padded = format!("{:0<6}", &frac[..frac.len().min(6)]);
        (&sec_part[..dot], padded.parse::<u32>().unwrap_or(0))
    } else {
        (sec_part, 0)
    };
    let second: u32 = sec_str.parse().ok()?;
    Some(DateTimeParts {
        year: 0,
        month: 0,
        day: 0,
        hour,
        minute,
        second,
        microsecond: micro,
        negative: false,
    })
}

/// Get system time as DateTimeParts
fn system_now_parts() -> DateTimeParts {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() as i64;
    let micros = now.subsec_micros();
    let mut p = DateTimeParts::from_unix_seconds(secs);
    p.microsecond = micros;
    p
}

/// MySQL DATE_FORMAT / TIME_FORMAT format engine
fn format_datetime_fmt(parts: &DateTimeParts, fmt: &str) -> String {
    let mut out = String::with_capacity(fmt.len() * 2);
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            i += 1;
            match bytes[i] {
                b'Y' => out.push_str(&format!("{:04}", parts.year)),
                b'y' => out.push_str(&format!("{:02}", (parts.year % 100).unsigned_abs())),
                b'm' => out.push_str(&format!("{:02}", parts.month)),
                b'c' => out.push_str(&format!("{}", parts.month)),
                b'M' => {
                    let idx = (parts.month as usize).min(12);
                    out.push_str(MONTH_NAMES[idx]);
                }
                b'b' => {
                    let idx = (parts.month as usize).min(12);
                    out.push_str(MONTH_ABBREVS[idx]);
                }
                b'd' => out.push_str(&format!("{:02}", parts.day)),
                b'e' => out.push_str(&format!("{}", parts.day)),
                b'D' => {
                    let d = parts.day;
                    let suffix = match d % 10 {
                        1 if d != 11 => "st",
                        2 if d != 12 => "nd",
                        3 if d != 13 => "rd",
                        _ => "th",
                    };
                    out.push_str(&format!("{}{}", d, suffix));
                }
                b'j' => out.push_str(&format!("{:03}", parts.day_of_year())),
                b'H' => out.push_str(&format!("{:02}", parts.hour)),
                b'k' => out.push_str(&format!("{}", parts.hour)),
                b'h' | b'I' => {
                    let h12 = match parts.hour % 12 {
                        0 => 12,
                        h => h,
                    };
                    out.push_str(&format!("{:02}", h12));
                }
                b'l' => {
                    let h12 = match parts.hour % 12 {
                        0 => 12,
                        h => h,
                    };
                    out.push_str(&format!("{}", h12));
                }
                b'i' => out.push_str(&format!("{:02}", parts.minute)),
                b's' | b'S' => out.push_str(&format!("{:02}", parts.second)),
                b'p' => out.push_str(if parts.hour < 12 { "AM" } else { "PM" }),
                b'f' => out.push_str(&format!("{:06}", parts.microsecond)),
                b'W' => {
                    let dow = parts.day_of_week_zero() as usize;
                    out.push_str(WEEKDAY_NAMES[dow % 7]);
                }
                b'a' => {
                    let dow = parts.day_of_week_zero() as usize;
                    out.push_str(WEEKDAY_ABBREVS[dow % 7]);
                }
                b'w' => out.push_str(&format!("{}", parts.day_of_week_zero())),
                b'U' => out.push_str(&format!("{:02}", parts.week_number(0))),
                b'u' => out.push_str(&format!("{:02}", parts.week_number(1))),
                b'V' => out.push_str(&format!("{:02}", parts.week_number(2))),
                b'v' => out.push_str(&format!("{:02}", parts.week_number(3))),
                b'r' => {
                    let h12 = match parts.hour % 12 {
                        0 => 12,
                        h => h,
                    };
                    let ampm = if parts.hour < 12 { "AM" } else { "PM" };
                    out.push_str(&format!(
                        "{:02}:{:02}:{:02} {}",
                        h12, parts.minute, parts.second, ampm
                    ));
                }
                b'T' => out.push_str(&format!(
                    "{:02}:{:02}:{:02}",
                    parts.hour, parts.minute, parts.second
                )),
                b'%' => out.push('%'),
                _ => {
                    out.push('%');
                    out.push(bytes[i] as char);
                }
            }
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

/// STR_TO_DATE: parse input using a MySQL format string
fn parse_datetime_fmt(input: &str, fmt: &str) -> Option<DateTimeParts> {
    let mut parts = DateTimeParts {
        year: 0,
        month: 0,
        day: 0,
        hour: 0,
        minute: 0,
        second: 0,
        microsecond: 0,
        negative: false,
    };
    let ibytes = input.as_bytes();
    let fbytes = fmt.as_bytes();
    let mut ii = 0; // index into input
    let mut fi = 0; // index into fmt
    let mut ampm: Option<bool> = None; // true = PM

    while fi < fbytes.len() {
        if fbytes[fi] == b'%' && fi + 1 < fbytes.len() {
            fi += 1;
            match fbytes[fi] {
                b'Y' => {
                    let (v, n) = consume_digits(ibytes, ii, 4)?;
                    parts.year = v as i64;
                    ii += n;
                }
                b'y' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.year = if v <= 69 {
                        2000 + v as i64
                    } else {
                        1900 + v as i64
                    };
                    ii += n;
                }
                b'm' | b'c' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.month = v;
                    ii += n;
                }
                b'M' => {
                    let (v, n) = match_name(ibytes, ii, &MONTH_NAMES[1..])?;
                    parts.month = v as u32 + 1;
                    ii += n;
                }
                b'b' => {
                    let (v, n) = match_name(ibytes, ii, &MONTH_ABBREVS[1..])?;
                    parts.month = v as u32 + 1;
                    ii += n;
                }
                b'd' | b'e' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.day = v;
                    ii += n;
                }
                b'H' | b'k' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.hour = v;
                    ii += n;
                }
                b'h' | b'I' | b'l' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.hour = v;
                    ii += n;
                }
                b'i' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.minute = v;
                    ii += n;
                }
                b's' | b'S' => {
                    let (v, n) = consume_digits(ibytes, ii, 2)?;
                    parts.second = v;
                    ii += n;
                }
                b'f' => {
                    let (v, n) = consume_digits(ibytes, ii, 6)?;
                    // Pad right if fewer than 6 digits consumed
                    let scale = 10u32.pow(6u32.saturating_sub(n as u32));
                    parts.microsecond = v * scale;
                    ii += n;
                }
                b'p' => {
                    if ii + 2 > ibytes.len() {
                        return None;
                    }
                    let s = std::str::from_utf8(&ibytes[ii..ii + 2]).ok()?;
                    if s.eq_ignore_ascii_case("AM") {
                        ampm = Some(false);
                    } else if s.eq_ignore_ascii_case("PM") {
                        ampm = Some(true);
                    } else {
                        return None;
                    }
                    ii += 2;
                }
                b'%' => {
                    if ii >= ibytes.len() || ibytes[ii] != b'%' {
                        return None;
                    }
                    ii += 1;
                }
                b'j' => {
                    let (v, n) = consume_digits(ibytes, ii, 3)?;
                    // Day of year — set month/day later
                    parts.day = v; // temporarily store doy
                    parts.month = 0; // flag
                    ii += n;
                }
                _ => {
                    // Unknown specifier, try to skip
                }
            }
        } else {
            // Literal character — must match
            if ii >= ibytes.len() {
                return None;
            }
            if fbytes[fi] == ibytes[ii] {
                ii += 1;
            } else if fbytes[fi] == b' ' {
                // Allow optional space
            } else {
                return None;
            }
        }
        fi += 1;
    }

    // Convert day-of-year back to month/day if %j was parsed
    if parts.month == 0 && parts.day != 0 {
        let mut remaining = parts.day;
        let mut m = 1u32;
        while m <= 12 {
            let dim = days_in_month(parts.year, m);
            if remaining <= dim {
                break;
            }
            remaining -= dim;
            m += 1;
        }
        if m <= 12 {
            parts.month = m;
            parts.day = remaining;
        } else {
            return None;
        }
    }

    // Apply AM/PM
    if let Some(is_pm) = ampm {
        if is_pm {
            if parts.hour != 12 {
                parts.hour += 12;
            }
        } else if parts.hour == 12 {
            parts.hour = 0;
        }
    }

    Some(parts)
}

/// Consume up to `max` ASCII digits from input starting at pos, return (value, count)
fn consume_digits(input: &[u8], pos: usize, max: usize) -> Option<(u32, usize)> {
    let mut val: u32 = 0;
    let mut count = 0;
    while count < max && pos + count < input.len() && input[pos + count].is_ascii_digit() {
        val = val * 10 + (input[pos + count] - b'0') as u32;
        count += 1;
    }
    if count == 0 {
        None
    } else {
        Some((val, count))
    }
}

/// Match input at pos against a list of names (case-insensitive), return (index, consumed_len)
fn match_name(input: &[u8], pos: usize, names: &[&str]) -> Option<(usize, usize)> {
    let remaining = &input[pos..];
    let remaining_str = std::str::from_utf8(remaining).ok()?;
    // Try longest match first
    let mut best: Option<(usize, usize)> = None;
    for (i, name) in names.iter().enumerate() {
        if remaining_str.len() >= name.len()
            && remaining_str[..name.len()].eq_ignore_ascii_case(name)
            && (best.is_none() || name.len() > best.unwrap().1)
        {
            best = Some((i, name.len()));
        }
    }
    best
}

/// GET_FORMAT lookup table
fn get_format_string(type_name: &str, standard: &str) -> Option<&'static str> {
    match (type_name, standard) {
        ("DATE", "USA") => Some("%m.%d.%Y"),
        ("DATE", "JIS") | ("DATE", "ISO") => Some("%Y-%m-%d"),
        ("DATE", "EUR") => Some("%d.%m.%Y"),
        ("DATE", "INTERNAL") => Some("%Y%m%d"),
        ("DATETIME", "USA") => Some("%Y-%m-%d %H.%i.%s"),
        ("DATETIME", "JIS") | ("DATETIME", "ISO") => Some("%Y-%m-%d %H:%i:%s"),
        ("DATETIME", "EUR") => Some("%Y-%m-%d %H.%i.%s"),
        ("DATETIME", "INTERNAL") => Some("%Y%m%d%H%i%s"),
        ("TIME", "USA") => Some("%h:%i:%s %p"),
        ("TIME", "JIS") | ("TIME", "ISO") => Some("%H:%i:%s"),
        ("TIME", "EUR") => Some("%H.%i.%s"),
        ("TIME", "INTERNAL") => Some("%H%i%s"),
        ("TIMESTAMP", "USA") => Some("%Y-%m-%d %H.%i.%s"),
        ("TIMESTAMP", "JIS") | ("TIMESTAMP", "ISO") => Some("%Y-%m-%d %H:%i:%s"),
        ("TIMESTAMP", "EUR") => Some("%Y-%m-%d %H.%i.%s"),
        ("TIMESTAMP", "INTERNAL") => Some("%Y%m%d%H%i%s"),
        _ => None,
    }
}

/// Convert total seconds to a MySQL TIME string "[−]HH:MM:SS"
fn secs_to_time_string(total_secs: i64) -> String {
    let sign = if total_secs < 0 { "-" } else { "" };
    let abs = total_secs.unsigned_abs();
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    let s = abs % 60;
    format!("{}{:02}:{:02}:{:02}", sign, h, m, s)
}

/// PERIOD_ADD/PERIOD_DIFF helper: normalize YYMM/YYYYMM to total months
fn period_to_months(p: i64) -> i64 {
    if p == 0 {
        return 0;
    }
    let (y, m) = if p > 0 && p <= 9999 {
        // Could be YYMM or YYYYMM
        if p <= 1299 {
            // YYMM
            let yy = p / 100;
            let mm = p % 100;
            let yyyy = if yy <= 69 { 2000 + yy } else { 1900 + yy };
            (yyyy, mm)
        } else {
            (p / 100, p % 100)
        }
    } else {
        (p / 100, p % 100)
    };
    y * 12 + m
}

fn months_to_period(months: i64) -> i64 {
    let y = (months - 1) / 12;
    let m = (months - 1) % 12 + 1;
    y * 100 + m
}

// ── End date/time infrastructure ──────────────────────────────────────

/// Try to evaluate a user-defined function (UDF) stored in user variables.
/// UDFs are registered as `__udf_NAME` = body_sql and `__udf_NAME_params` = param_names
fn try_eval_udf(
    name: &str,
    args: &[Datum],
    vars: Option<&UserVariables>,
) -> Option<ExecutorResult<Datum>> {
    let vars = vars?;
    let udf_key = format!("__udf_{}", name.to_lowercase());

    // Look up UDF body and params from user variables
    let (body_sql, param_names) = {
        let r = vars.read();
        let body = match r.get(&udf_key) {
            Some(Datum::String(s)) => s.clone(),
            _ => return None,
        };
        let params_key = format!("{}_params", udf_key);
        let params = match r.get(&params_key) {
            Some(Datum::String(s)) => s.clone(),
            _ => String::new(),
        };
        (body, params)
    };

    // Build a mini procedure context with args bound to param names
    let param_list: Vec<&str> = if param_names.is_empty() {
        vec![]
    } else {
        param_names.split(',').collect()
    };

    let mut ctx = crate::executor::procedure::ProcedureContext {
        locals: std::collections::HashMap::new(),
        cursors: std::collections::HashMap::new(),
        found: false,
        rows_affected: 0,
    };

    for (i, pname) in param_list.iter().enumerate() {
        let val = args.get(i).cloned().unwrap_or(Datum::Null);
        ctx.locals.insert(pname.trim().to_string(), val);
    }

    // Parse and execute the body statements
    // Execute the body statements by splitting on ';' and processing each
    let trimmed = body_sql.trim();
    let upper = trimmed.to_uppercase();
    let inner_body = if upper.starts_with("BEGIN") && upper.ends_with("END") {
        trimmed[5..trimmed.len() - 3].trim()
    } else {
        trimmed
    };

    // Split body into statements and process each
    let into_re = regex::Regex::new(r"(?i)\bINTO\s+(\w+)").ok()?;
    for raw_stmt in inner_body.split(';') {
        let stmt_text = raw_stmt.trim();
        if stmt_text.is_empty() {
            continue;
        }
        let stmt_upper = stmt_text.to_uppercase();

        // DECLARE var TYPE → SET var = NULL
        if stmt_upper.starts_with("DECLARE ") {
            let parts: Vec<&str> = stmt_text.splitn(3, char::is_whitespace).collect();
            if let Some(var_name) = parts.get(1) {
                ctx.locals.insert(var_name.to_lowercase(), Datum::Null);
            }
            continue;
        }

        // SET var = expr
        if stmt_upper.starts_with("SET ") {
            let rest = stmt_text[4..].trim();
            if let Some(eq_pos) = rest.find('=') {
                let var_name = rest[..eq_pos].trim().to_lowercase();
                let expr_text = rest[eq_pos + 1..].trim();
                // Substitute locals in expression
                let subst = substitute_locals_in_sql(expr_text, &ctx);
                if let Some(val) = eval_sql_expr_in_sp_context(&subst, &ctx, vars) {
                    ctx.locals.insert(var_name, val);
                }
            }
            continue;
        }

        // RETURN expr
        if stmt_upper.starts_with("RETURN ") {
            let expr_text = stmt_text[7..].trim();
            let subst = substitute_locals_in_sql(expr_text, &ctx);
            if let Some(val) = eval_sql_expr_in_sp_context(&subst, &ctx, vars) {
                return Some(Ok(val));
            }
            continue;
        }

        // SELECT ... INTO var
        if stmt_upper.starts_with("SELECT ") {
            if let Some(caps) = into_re.captures(stmt_text) {
                let var_name = caps.get(1)?.as_str().to_lowercase();
                let into_match = caps.get(0)?;
                let select_sql = format!(
                    "{}{}",
                    &stmt_text[..into_match.start()],
                    &stmt_text[into_match.end()..]
                );
                let subst = substitute_locals_in_sql(&select_sql, &ctx);
                // The select_sql is already a full SELECT, no need to wrap
                if let Ok(sqlparser::ast::Statement::Query(q)) =
                    crate::sql::Parser::parse_one(&subst).as_ref()
                {
                    if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                        if let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) =
                            sel.projection.first()
                        {
                            if let Ok(val) =
                                crate::executor::procedure::eval_sp_expr(expr, &ctx, vars)
                            {
                                ctx.locals.insert(var_name, val);
                            }
                        }
                    }
                }
            }
            continue;
        }
    }

    // If no explicit RETURN, return NULL
    Some(Ok(Datum::Null))
}

/// Parse a SQL expression from "SELECT expr" and evaluate it in procedure context
fn eval_sql_expr_in_sp_context(
    expr_sql: &str,
    ctx: &crate::executor::procedure::ProcedureContext,
    vars: &UserVariables,
) -> Option<Datum> {
    if let Ok(sqlparser::ast::Statement::Query(q)) =
        crate::sql::Parser::parse_one(&format!("SELECT {}", expr_sql)).as_ref()
    {
        if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
            if let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) = sel.projection.first() {
                return crate::executor::procedure::eval_sp_expr(expr, ctx, vars).ok();
            }
        }
    }
    None
}

/// Substitute local variable names in SQL text with their values
fn substitute_locals_in_sql(
    sql: &str,
    ctx: &crate::executor::procedure::ProcedureContext,
) -> String {
    let mut result = sql.to_string();
    let mut vars: Vec<(&String, &Datum)> = ctx.locals.iter().collect();
    vars.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    for (name, val) in vars {
        result = result.replace(name.as_str(), &val.to_sql_literal());
    }
    result
}

/// Convert an i128 decimal integer result to the best-fitting Datum type.
/// Values in i64 range → Int, values in u64 range → UnsignedInt,
/// otherwise stays as Decimal with scale 0.
fn decimal_int_result(v: i128) -> ExecutorResult<Datum> {
    if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
        Ok(Datum::Int(v as i64))
    } else if v >= 0 && v <= u64::MAX as i128 {
        Ok(Datum::UnsignedInt(v as u64))
    } else {
        Ok(Datum::Decimal { value: v, scale: 0 })
    }
}

/// Return Datum::Float for finite values, Datum::Null for NaN/Infinity.
/// MySQL returns NULL for mathematically undefined results (log of negative, etc.)
fn float_or_null(v: f64) -> Datum {
    if v.is_finite() {
        Datum::Float(v)
    } else {
        Datum::Null
    }
}

/// Check if a logarithm argument is valid (> 0). In DML context with
/// ERROR_FOR_DIVISION_BY_ZERO sql_mode, returns ER_INVALID_ARGUMENT_FOR_LOGARITHM.
/// In SELECT context, the caller returns NULL via float_or_null.
fn check_log_arg(v: f64, func_name: &str, vars: Option<&UserVariables>) -> ExecutorResult<()> {
    if v <= 0.0 && error_for_division_by_zero(vars) && in_strict_dml_context(vars) {
        return Err(ExecutorError::InvalidArgumentForLogarithm(format!(
            "Invalid argument for logarithm in function {}",
            func_name
        )));
    }
    Ok(())
}

fn parse_time_to_secs(s: &str) -> i64 {
    let s = s.trim();
    // Integer 0 means "00:00:00"
    if s == "0" {
        return 0;
    }
    let parts: Vec<&str> = s.split([' ', ':', '-']).collect();
    if parts.len() >= 6 {
        // YYYY-MM-DD HH:MM:SS
        let year: i64 = parts[0].parse().unwrap_or(0);
        let month: i64 = parts[1].parse().unwrap_or(0);
        let day: i64 = parts[2].parse().unwrap_or(0);
        let hour: i64 = parts[3].parse().unwrap_or(0);
        let min: i64 = parts[4].parse().unwrap_or(0);
        let sec: i64 = parts[5].parse().unwrap_or(0);
        // Approximate: days since epoch * 86400 + time of day
        let days = year * 365 + year / 4 - year / 100 + year / 400 + (month * 30) + day;
        days * 86400 + hour * 3600 + min * 60 + sec
    } else if parts.len() >= 3 {
        // HH:MM:SS
        let hour: i64 = parts[0].parse().unwrap_or(0);
        let min: i64 = parts[1].parse().unwrap_or(0);
        let sec: i64 = parts[2].parse().unwrap_or(0);
        hour * 3600 + min * 60 + sec
    } else {
        0
    }
}

fn parse_date_to_days(s: &str) -> Option<i64> {
    let date_part = s.split(' ').next()?;
    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() < 3 {
        return None;
    }
    let year: i64 = parts[0].parse().ok()?;
    let month: i64 = parts[1].parse().ok()?;
    let day: i64 = parts[2].parse().ok()?;
    if year == 0 && month == 0 && day == 0 {
        return Some(0);
    }
    // MySQL TO_DAYS formula
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    Some(365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + day + 1721119)
}

fn radix_string(val: i64, radix: u32) -> String {
    if radix == 10 {
        return val.to_string();
    }
    let negative = val < 0;
    let mut n = if negative {
        (val as i128).unsigned_abs()
    } else {
        val as u128
    };
    if n == 0 {
        return "0".to_string();
    }
    let digits = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut result = Vec::new();
    while n > 0 {
        result.push(digits[(n % radix as u128) as usize]);
        n /= radix as u128;
    }
    if negative {
        result.push(b'-');
    }
    result.reverse();
    String::from_utf8(result).unwrap_or_default()
}

/// Simple CRC32 (IEEE) computation
fn crc32_compute(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Evaluate IF(cond, then_expr, else_expr) with short-circuit evaluation.
/// NULL condition takes the else branch (MySQL semantics).
fn eval_if_function(
    args: &[ResolvedExpr],
    row: &Row,
    vars: &UserVariables,
) -> ExecutorResult<Datum> {
    if args.len() != 3 {
        return Err(ExecutorError::InvalidOperation(
            "IF requires 3 arguments".to_string(),
        ));
    }
    let cond = evaluate(&args[0], row, vars)?;
    let is_true = cond.as_bool().unwrap_or_default();
    if is_true {
        evaluate(&args[1], row, vars)
    } else {
        evaluate(&args[2], row, vars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::planner::logical::{Literal, ResolvedColumn};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn empty_vars() -> UserVariables {
        Arc::new(RwLock::new(HashMap::new()))
    }

    /// Test helper: evaluate with empty user variables
    fn eval(expr: &ResolvedExpr, row: &Row) -> ExecutorResult<Datum> {
        evaluate(expr, row, &empty_vars())
    }

    fn make_row() -> Row {
        Row::new(vec![
            Datum::Int(42),
            Datum::String("hello".to_string()),
            Datum::Float(2.5),
            Datum::Null,
        ])
    }

    fn col_expr(index: usize) -> ResolvedExpr {
        ResolvedExpr::Column(ResolvedColumn {
            table: "t".to_string(),
            name: format!("c{}", index),
            index,
            data_type: DataType::Int,
            nullable: true,
        })
    }

    #[test]
    fn test_eval_column() {
        let row = make_row();
        let result = eval(&col_expr(0), &row).unwrap();
        assert!(matches!(result, Datum::Int(42)));
    }

    #[test]
    fn test_eval_literal() {
        let row = make_row();
        let expr = ResolvedExpr::Literal(Literal::Integer(100));
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Int(100)));
    }

    #[test]
    fn test_eval_add() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(col_expr(0)),
            op: BinaryOp::Add,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(8))),
            result_type: DataType::BigInt,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Int(50)));
    }

    #[test]
    fn test_eval_comparison() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(col_expr(0)),
            op: BinaryOp::Gt,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(40))),
            result_type: DataType::Boolean,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Bool(true)));
    }

    #[test]
    fn test_eval_null_propagation() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(col_expr(3)), // NULL column
            op: BinaryOp::Add,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            result_type: DataType::BigInt,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_and_three_valued() {
        let row = make_row();

        // false AND null = false
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Boolean(false))),
            op: BinaryOp::And,
            right: Box::new(col_expr(3)), // NULL
            result_type: DataType::Boolean,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Bool(false)));
    }

    #[test]
    fn test_eval_is_null() {
        let row = make_row();
        let expr = ResolvedExpr::IsNull {
            expr: Box::new(col_expr(3)),
            negated: false,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Bool(true)));
    }

    #[test]
    fn test_eval_between() {
        let row = make_row();
        let expr = ResolvedExpr::Between {
            expr: Box::new(col_expr(0)),
            low: Box::new(ResolvedExpr::Literal(Literal::Integer(40))),
            high: Box::new(ResolvedExpr::Literal(Literal::Integer(50))),
            negated: false,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(matches!(result, Datum::Bool(true)));
    }

    #[test]
    fn test_eval_function_upper() {
        let row = make_row();
        let expr = ResolvedExpr::Function {
            name: "UPPER".to_string(),
            args: vec![col_expr(1)],
            distinct: false,
            result_type: DataType::Text,
        };
        let result = eval(&expr, &row).unwrap();
        assert_eq!(result.as_str(), Some("HELLO"));
    }

    #[test]
    fn test_eval_mod_by_zero_returns_null() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(col_expr(0)),
            op: BinaryOp::Mod,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(0))),
            result_type: DataType::BigInt,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_if_true() {
        let row = make_row();
        let expr = ResolvedExpr::Function {
            name: "IF".to_string(),
            args: vec![
                ResolvedExpr::Literal(Literal::Integer(1)),
                ResolvedExpr::Literal(Literal::String("yes".to_string())),
                ResolvedExpr::Literal(Literal::String("no".to_string())),
            ],
            distinct: false,
            result_type: DataType::Text,
        };
        let result = eval(&expr, &row).unwrap();
        assert_eq!(result.as_str(), Some("yes"));
    }

    #[test]
    fn test_eval_if_false() {
        let row = make_row();
        let expr = ResolvedExpr::Function {
            name: "IF".to_string(),
            args: vec![
                ResolvedExpr::Literal(Literal::Integer(0)),
                ResolvedExpr::Literal(Literal::String("yes".to_string())),
                ResolvedExpr::Literal(Literal::String("no".to_string())),
            ],
            distinct: false,
            result_type: DataType::Text,
        };
        let result = eval(&expr, &row).unwrap();
        assert_eq!(result.as_str(), Some("no"));
    }

    #[test]
    fn test_eval_if_null_condition() {
        let row = make_row();
        let expr = ResolvedExpr::Function {
            name: "IF".to_string(),
            args: vec![
                ResolvedExpr::Literal(Literal::Null),
                ResolvedExpr::Literal(Literal::String("yes".to_string())),
                ResolvedExpr::Literal(Literal::String("no".to_string())),
            ],
            distinct: false,
            result_type: DataType::Text,
        };
        let result = eval(&expr, &row).unwrap();
        assert_eq!(result.as_str(), Some("no"));
    }

    #[test]
    fn test_eval_isnull() {
        let row = make_row();
        // ISNULL(NULL) = 1
        let expr = ResolvedExpr::Function {
            name: "ISNULL".to_string(),
            args: vec![ResolvedExpr::Literal(Literal::Null)],
            distinct: false,
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(1));

        // ISNULL(42) = 0
        let expr = ResolvedExpr::Function {
            name: "ISNULL".to_string(),
            args: vec![ResolvedExpr::Literal(Literal::Integer(42))],
            distinct: false,
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(0));
    }

    #[test]
    fn test_eval_ifnull() {
        let row = make_row();
        // IFNULL(NULL, 2) = 2
        let expr = ResolvedExpr::Function {
            name: "IFNULL".to_string(),
            args: vec![
                ResolvedExpr::Literal(Literal::Null),
                ResolvedExpr::Literal(Literal::Integer(2)),
            ],
            distinct: false,
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(2));

        // IFNULL(1, 2) = 1
        let expr = ResolvedExpr::Function {
            name: "IFNULL".to_string(),
            args: vec![
                ResolvedExpr::Literal(Literal::Integer(1)),
                ResolvedExpr::Literal(Literal::Integer(2)),
            ],
            distinct: false,
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(1));
    }

    #[test]
    fn test_eval_div_by_zero_returns_null() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(col_expr(0)),
            op: BinaryOp::Div,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(0))),
            result_type: DataType::BigInt,
        };
        let result = eval(&expr, &row).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_eval_boolean_test_is_true() {
        let row = make_row();
        // 1 IS TRUE = true
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            test: BooleanTestType::IsTrue,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(true));

        // 0 IS TRUE = false
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Integer(0))),
            test: BooleanTestType::IsTrue,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));

        // NULL IS TRUE = false
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Null)),
            test: BooleanTestType::IsTrue,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));
    }

    #[test]
    fn test_eval_boolean_test_is_false() {
        let row = make_row();
        // 0 IS FALSE = true
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Integer(0))),
            test: BooleanTestType::IsFalse,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(true));

        // NULL IS FALSE = false (NULL is unknown, not false)
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Null)),
            test: BooleanTestType::IsFalse,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));
    }

    #[test]
    fn test_eval_boolean_test_is_unknown() {
        let row = make_row();
        // NULL IS UNKNOWN = true
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Null)),
            test: BooleanTestType::IsUnknown,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(true));

        // 1 IS UNKNOWN = false
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            test: BooleanTestType::IsUnknown,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));
    }

    #[test]
    fn test_eval_boolean_test_negated() {
        let row = make_row();
        // 1 IS NOT TRUE = false
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            test: BooleanTestType::IsNotTrue,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));

        // NULL IS NOT TRUE = true
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Null)),
            test: BooleanTestType::IsNotTrue,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(true));

        // NULL IS NOT UNKNOWN = false
        let expr = ResolvedExpr::BooleanTest {
            expr: Box::new(ResolvedExpr::Literal(Literal::Null)),
            test: BooleanTestType::IsNotUnknown,
        };
        assert_eq!(eval(&expr, &row).unwrap(), Datum::Bool(false));
    }

    #[test]
    fn test_eval_bitwise_or() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Integer(5))),
            op: BinaryOp::BitwiseOr,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(3))),
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(7));
    }

    #[test]
    fn test_eval_bitwise_and() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Integer(5))),
            op: BinaryOp::BitwiseAnd,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(3))),
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(1));
    }

    #[test]
    fn test_eval_bitwise_xor() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Integer(5))),
            op: BinaryOp::BitwiseXor,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(3))),
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(6));
    }

    #[test]
    fn test_eval_bitwise_null_propagation() {
        let row = make_row();
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Integer(1))),
            op: BinaryOp::BitwiseOr,
            right: Box::new(ResolvedExpr::Literal(Literal::Null)),
            result_type: DataType::BigInt,
        };
        assert!(eval(&expr, &row).unwrap().is_null());
    }

    #[test]
    fn test_eval_bitwise_float_coercion() {
        let row = make_row();
        // 1.9 | 2 should truncate 1.9 to 1, giving 1 | 2 = 3
        let expr = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Literal(Literal::Float(1.9))),
            op: BinaryOp::BitwiseOr,
            right: Box::new(ResolvedExpr::Literal(Literal::Integer(2))),
            result_type: DataType::BigInt,
        };
        assert_eq!(eval(&expr, &row).unwrap().as_int(), Some(3));
    }

    // ── Date/time infrastructure tests ──

    #[test]
    fn test_hinnant_roundtrip() {
        // Verify days_from_epoch_to_ymd and ymd_to_days_from_epoch are inverses
        let cases: &[(i64, u32, u32)] = &[
            (1970, 1, 1),
            (2000, 1, 1),
            (2000, 2, 29),
            (1969, 12, 31),
            (2024, 3, 23),
            (1, 1, 1),
            (9999, 12, 31),
        ];
        for &(y, m, d) in cases {
            let days = ymd_to_days_from_epoch(y, m, d);
            let (y2, m2, d2) = days_from_epoch_to_ymd(days);
            assert_eq!(
                (y, m, d),
                (y2, m2, d2),
                "roundtrip failed for {}-{}-{}",
                y,
                m,
                d
            );
        }
        // Unix epoch should be day 0
        assert_eq!(ymd_to_days_from_epoch(1970, 1, 1), 0);
    }

    #[test]
    fn test_is_leap_year() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
        assert!(is_leap_year(2400));
    }

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2024, 1), 31);
        assert_eq!(days_in_month(2024, 4), 30);
    }

    #[test]
    fn test_parse_to_parts_datetime() {
        let p = parse_to_parts("2024-03-23 14:30:45").unwrap();
        assert_eq!(p.year, 2024);
        assert_eq!(p.month, 3);
        assert_eq!(p.day, 23);
        assert_eq!(p.hour, 14);
        assert_eq!(p.minute, 30);
        assert_eq!(p.second, 45);
        assert!(!p.negative);
    }

    #[test]
    fn test_parse_to_parts_date_only() {
        let p = parse_to_parts("2024-03-23").unwrap();
        assert_eq!(p.year, 2024);
        assert_eq!(p.month, 3);
        assert_eq!(p.day, 23);
        assert_eq!(p.hour, 0);
    }

    #[test]
    fn test_parse_to_parts_time_only() {
        let p = parse_to_parts("14:30:45").unwrap();
        assert_eq!(p.hour, 14);
        assert_eq!(p.minute, 30);
        assert_eq!(p.second, 45);
        assert_eq!(p.year, 0);
    }

    #[test]
    fn test_parse_to_parts_negative_time() {
        let p = parse_to_parts("-01:30:00").unwrap();
        assert!(p.negative);
        assert_eq!(p.hour, 1);
        assert_eq!(p.minute, 30);
    }

    #[test]
    fn test_parse_to_parts_compact() {
        let p = parse_to_parts("20240323").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 3, 23));
        let p = parse_to_parts("20240323143045").unwrap();
        assert_eq!(
            (p.year, p.month, p.day, p.hour, p.minute, p.second),
            (2024, 3, 23, 14, 30, 45)
        );
    }

    #[test]
    fn test_parse_to_parts_microseconds() {
        let p = parse_to_parts("2024-03-23 14:30:45.123456").unwrap();
        assert_eq!(p.microsecond, 123456);
        let p = parse_to_parts("14:30:45.5").unwrap();
        assert_eq!(p.microsecond, 500000);
    }

    #[test]
    fn test_parse_to_parts_zero() {
        let p = parse_to_parts("0").unwrap();
        assert_eq!((p.year, p.month, p.day), (0, 0, 0));
    }

    #[test]
    fn test_parse_to_parts_empty() {
        assert!(parse_to_parts("").is_none());
        assert!(parse_to_parts("garbage").is_none());
    }

    #[test]
    fn test_day_of_week() {
        // 2024-01-07 was Sunday
        let p = parse_to_parts("2024-01-07").unwrap();
        assert_eq!(p.day_of_week_zero(), 0); // 0=Sunday
        assert_eq!(p.day_of_week(), 1); // MySQL DAYOFWEEK 1=Sun
        assert_eq!(p.weekday(), 6); // MySQL WEEKDAY 6=Sun

        // 2024-01-08 was Monday
        let p = parse_to_parts("2024-01-08").unwrap();
        assert_eq!(p.day_of_week_zero(), 1); // 1=Monday
        assert_eq!(p.day_of_week(), 2); // MySQL DAYOFWEEK 2=Mon
        assert_eq!(p.weekday(), 0); // MySQL WEEKDAY 0=Mon

        // 1970-01-01 was Thursday
        let p = parse_to_parts("1970-01-01").unwrap();
        assert_eq!(p.day_of_week_zero(), 4); // 4=Thursday
    }

    #[test]
    fn test_day_of_year() {
        let p = parse_to_parts("2024-01-01").unwrap();
        assert_eq!(p.day_of_year(), 1);
        let p = parse_to_parts("2024-12-31").unwrap();
        assert_eq!(p.day_of_year(), 366); // 2024 is leap
        let p = parse_to_parts("2023-12-31").unwrap();
        assert_eq!(p.day_of_year(), 365);
    }

    #[test]
    fn test_quarter() {
        let p = parse_to_parts("2024-01-15").unwrap();
        assert_eq!(p.quarter(), 1);
        let p = parse_to_parts("2024-06-15").unwrap();
        assert_eq!(p.quarter(), 2);
        let p = parse_to_parts("2024-09-15").unwrap();
        assert_eq!(p.quarter(), 3);
        let p = parse_to_parts("2024-12-15").unwrap();
        assert_eq!(p.quarter(), 4);
        // month=0 should return 0 (caller maps to NULL)
        let p = parse_to_parts("0").unwrap();
        assert_eq!(p.quarter(), 0);
    }

    #[test]
    fn test_unix_seconds_roundtrip() {
        // 2024-01-01 00:00:00 UTC
        let p = parse_to_parts("2024-01-01 00:00:00").unwrap();
        let secs = p.to_unix_seconds();
        let p2 = DateTimeParts::from_unix_seconds(secs);
        assert_eq!((p2.year, p2.month, p2.day), (2024, 1, 1));
        assert_eq!((p2.hour, p2.minute, p2.second), (0, 0, 0));
        assert!(!p2.negative);
    }

    #[test]
    fn test_from_unix_seconds_negative() {
        // -1 should be 1969-12-31 23:59:59
        let p = DateTimeParts::from_unix_seconds(-1);
        assert_eq!((p.year, p.month, p.day), (1969, 12, 31));
        assert_eq!((p.hour, p.minute, p.second), (23, 59, 59));
        assert!(!p.negative); // datetimes should never be negative
    }

    #[test]
    fn test_from_unix_seconds_zero() {
        let p = DateTimeParts::from_unix_seconds(0);
        assert_eq!((p.year, p.month, p.day), (1970, 1, 1));
        assert_eq!((p.hour, p.minute, p.second), (0, 0, 0));
    }

    #[test]
    fn test_format_datetime_fmt_basic() {
        let p = parse_to_parts("2024-03-23 14:30:45").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%Y-%m-%d"), "2024-03-23");
        assert_eq!(format_datetime_fmt(&p, "%H:%i:%s"), "14:30:45");
        assert_eq!(
            format_datetime_fmt(&p, "%Y-%m-%d %H:%i:%s"),
            "2024-03-23 14:30:45"
        );
    }

    #[test]
    fn test_format_datetime_fmt_specifiers() {
        let p = parse_to_parts("2024-03-05 09:05:07").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%y"), "24");
        assert_eq!(format_datetime_fmt(&p, "%c"), "3");
        assert_eq!(format_datetime_fmt(&p, "%e"), "5");
        assert_eq!(format_datetime_fmt(&p, "%k"), "9");
        assert_eq!(format_datetime_fmt(&p, "%M"), "March");
        assert_eq!(format_datetime_fmt(&p, "%b"), "Mar");
    }

    #[test]
    fn test_format_datetime_12h() {
        let p = parse_to_parts("2024-01-01 00:30:00").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%h"), "12");
        assert_eq!(format_datetime_fmt(&p, "%p"), "AM");
        let p = parse_to_parts("2024-01-01 13:30:00").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%h"), "01");
        assert_eq!(format_datetime_fmt(&p, "%l"), "1");
        assert_eq!(format_datetime_fmt(&p, "%p"), "PM");
    }

    #[test]
    fn test_format_day_suffix() {
        let p = |d: u32| DateTimeParts {
            year: 2024,
            month: 1,
            day: d,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
            negative: false,
        };
        assert_eq!(format_datetime_fmt(&p(1), "%D"), "1st");
        assert_eq!(format_datetime_fmt(&p(2), "%D"), "2nd");
        assert_eq!(format_datetime_fmt(&p(3), "%D"), "3rd");
        assert_eq!(format_datetime_fmt(&p(4), "%D"), "4th");
        assert_eq!(format_datetime_fmt(&p(11), "%D"), "11th");
        assert_eq!(format_datetime_fmt(&p(12), "%D"), "12th");
        assert_eq!(format_datetime_fmt(&p(13), "%D"), "13th");
        assert_eq!(format_datetime_fmt(&p(21), "%D"), "21st");
        assert_eq!(format_datetime_fmt(&p(22), "%D"), "22nd");
        assert_eq!(format_datetime_fmt(&p(23), "%D"), "23rd");
    }

    #[test]
    fn test_format_r_and_t() {
        let p = parse_to_parts("2024-01-01 14:30:45").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%r"), "02:30:45 PM");
        assert_eq!(format_datetime_fmt(&p, "%T"), "14:30:45");
    }

    #[test]
    fn test_format_literal_percent() {
        let p = parse_to_parts("2024-01-01").unwrap();
        assert_eq!(format_datetime_fmt(&p, "%%"), "%");
        assert_eq!(format_datetime_fmt(&p, "100%%"), "100%");
    }

    #[test]
    fn test_parse_datetime_fmt_basic() {
        let p = parse_datetime_fmt("2024-03-23", "%Y-%m-%d").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 3, 23));
    }

    #[test]
    fn test_parse_datetime_fmt_month_name() {
        let p = parse_datetime_fmt("March 23, 2024", "%M %d, %Y").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 3, 23));
    }

    #[test]
    fn test_parse_datetime_fmt_abbrev_month() {
        let p = parse_datetime_fmt("Mar 23, 2024", "%b %d, %Y").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 3, 23));
    }

    #[test]
    fn test_parse_datetime_fmt_12h_ampm() {
        let p = parse_datetime_fmt("02:30:00 PM", "%h:%i:%s %p").unwrap();
        assert_eq!((p.hour, p.minute, p.second), (14, 30, 0));
        let p = parse_datetime_fmt("12:00:00 AM", "%h:%i:%s %p").unwrap();
        assert_eq!(p.hour, 0);
        let p = parse_datetime_fmt("12:00:00 PM", "%h:%i:%s %p").unwrap();
        assert_eq!(p.hour, 12);
    }

    #[test]
    fn test_parse_datetime_fmt_2digit_year() {
        let p = parse_datetime_fmt("69-03-23", "%y-%m-%d").unwrap();
        assert_eq!(p.year, 2069);
        let p = parse_datetime_fmt("70-03-23", "%y-%m-%d").unwrap();
        assert_eq!(p.year, 1970);
    }

    #[test]
    fn test_parse_datetime_fmt_day_of_year() {
        // Day 60 of 2024 (leap year): should be Feb 29
        let p = parse_datetime_fmt("2024 060", "%Y %j").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 2, 29));
        // Day 1 of 2024: should be Jan 1
        let p = parse_datetime_fmt("2024 001", "%Y %j").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 1, 1));
        // Day 366 of 2024: should be Dec 31
        let p = parse_datetime_fmt("2024 366", "%Y %j").unwrap();
        assert_eq!((p.year, p.month, p.day), (2024, 12, 31));
    }

    #[test]
    fn test_parse_datetime_fmt_microseconds() {
        let p = parse_datetime_fmt("123", "%f").unwrap();
        assert_eq!(p.microsecond, 123000);
        let p = parse_datetime_fmt("123456", "%f").unwrap();
        assert_eq!(p.microsecond, 123456);
    }

    #[test]
    fn test_week_number_mode0() {
        // Mode 0: Sunday-start, 0-53, first Sunday
        // 2024-01-01 is Monday → first Sunday is Jan 7 → Jan 1-6 are week 0
        let p = parse_to_parts("2024-01-01").unwrap();
        assert_eq!(p.week_number(0), 0);
        let p = parse_to_parts("2024-01-07").unwrap(); // first Sunday
        assert_eq!(p.week_number(0), 1);
    }

    #[test]
    fn test_week_number_mode1() {
        // Mode 1: Monday-start, 0-53, 4+ days
        // 2024-01-01 is Monday → Mon-Sun = 7 days in year → week 1
        let p = parse_to_parts("2024-01-01").unwrap();
        assert_eq!(p.week_number(1), 1);
    }

    #[test]
    fn test_week_number_mode3_iso() {
        // Mode 3: Monday-start, 1-53, 4+ days (ISO-like)
        // 2024-01-01 is Monday → ISO week 1
        let p = parse_to_parts("2024-01-01").unwrap();
        assert_eq!(p.week_number(3), 1);
        // 2023-01-01 is Sunday → ISO: belongs to week 52 of 2022
        let p = parse_to_parts("2023-01-01").unwrap();
        assert_eq!(p.week_number(3), 52);
    }

    #[test]
    fn test_get_format() {
        assert_eq!(get_format_string("DATE", "USA"), Some("%m.%d.%Y"));
        assert_eq!(
            get_format_string("DATETIME", "ISO"),
            Some("%Y-%m-%d %H:%i:%s")
        );
        assert_eq!(get_format_string("DATE", "UNKNOWN"), None);
    }

    #[test]
    fn test_secs_to_time_string() {
        assert_eq!(secs_to_time_string(3661), "01:01:01");
        assert_eq!(secs_to_time_string(0), "00:00:00");
        assert_eq!(secs_to_time_string(-3661), "-01:01:01");
    }

    #[test]
    fn test_period_to_months_and_back() {
        // YYYYMM format
        assert_eq!(period_to_months(202403), 2024 * 12 + 3);
        // Round-trip
        let m = period_to_months(202403);
        assert_eq!(months_to_period(m), 202403);
    }

    #[test]
    fn test_period_add_via_helpers() {
        // PERIOD_ADD(202401, 5) → 202406
        let total = period_to_months(202401) + 5;
        assert_eq!(months_to_period(total), 202406);
        // Cross year boundary: 202411 + 3 → 202502
        let total = period_to_months(202411) + 3;
        assert_eq!(months_to_period(total), 202502);
    }

    #[test]
    fn test_monthname_zero_date() {
        // MONTHNAME('0000-00-00') should return NULL
        let result = eval_function(
            "MONTHNAME",
            &[Datum::String("0000-00-00".to_string())],
            None,
        );
        assert_eq!(result.unwrap(), Datum::Null);
    }

    #[test]
    fn test_dayname_zero_date() {
        // DAYNAME('0000-00-00') should return NULL
        let result = eval_function("DAYNAME", &[Datum::String("0000-00-00".to_string())], None);
        assert_eq!(result.unwrap(), Datum::Null);
    }

    #[test]
    fn test_quarter_zero_date() {
        // QUARTER('0000-00-00') should return NULL
        let result = eval_function("QUARTER", &[Datum::String("0000-00-00".to_string())], None);
        assert_eq!(result.unwrap(), Datum::Null);
    }

    #[test]
    fn test_date_format_function() {
        let result = eval_function(
            "DATE_FORMAT",
            &[
                Datum::String("2024-03-23".to_string()),
                Datum::String("%W, %M %D, %Y".to_string()),
            ],
            None,
        );
        assert_eq!(
            result.unwrap(),
            Datum::String("Saturday, March 23rd, 2024".to_string())
        );
    }

    #[test]
    fn test_str_to_date_function() {
        let result = eval_function(
            "STR_TO_DATE",
            &[
                Datum::String("March 23, 2024".to_string()),
                Datum::String("%M %d, %Y".to_string()),
            ],
            None,
        );
        assert_eq!(result.unwrap(), Datum::String("2024-03-23".to_string()));
    }

    #[test]
    fn test_extraction_functions() {
        let dt = Datum::String("2024-03-23 14:30:45".to_string());
        assert_eq!(
            eval_function("HOUR", &[dt.clone()], None).unwrap(),
            Datum::Int(14)
        );
        assert_eq!(
            eval_function("MINUTE", &[dt.clone()], None).unwrap(),
            Datum::Int(30)
        );
        assert_eq!(
            eval_function("SECOND", &[dt.clone()], None).unwrap(),
            Datum::Int(45)
        );
        assert_eq!(
            eval_function("YEAR", &[dt.clone()], None).unwrap(),
            Datum::Int(2024)
        );
        assert_eq!(
            eval_function("MONTH", &[dt.clone()], None).unwrap(),
            Datum::Int(3)
        );
        assert_eq!(
            eval_function("DAY", &[dt.clone()], None).unwrap(),
            Datum::Int(23)
        );
        assert_eq!(
            eval_function("DAYOFMONTH", &[dt.clone()], None).unwrap(),
            Datum::Int(23)
        );
        // 2024-03-23 is Saturday
        assert_eq!(
            eval_function("DAYOFWEEK", &[dt.clone()], None).unwrap(),
            Datum::Int(7)
        ); // 7=Sat
        assert_eq!(
            eval_function("WEEKDAY", &[dt.clone()], None).unwrap(),
            Datum::Int(5)
        ); // 5=Sat
        assert_eq!(
            eval_function("DAYNAME", &[dt.clone()], None).unwrap(),
            Datum::String("Saturday".to_string())
        );
        assert_eq!(
            eval_function("MONTHNAME", &[dt.clone()], None).unwrap(),
            Datum::String("March".to_string())
        );
        // Day 83 of 2024 (leap year)
        assert_eq!(
            eval_function("DAYOFYEAR", &[dt.clone()], None).unwrap(),
            Datum::Int(83)
        );
        assert_eq!(
            eval_function("QUARTER", &[dt], None).unwrap(),
            Datum::Int(1)
        );
    }

    #[test]
    fn test_maketime() {
        let result = eval_function(
            "MAKETIME",
            &[Datum::Int(14), Datum::Int(30), Datum::Int(45)],
            None,
        );
        assert_eq!(result.unwrap(), Datum::String("14:30:45".to_string()));
    }

    #[test]
    fn test_makedate() {
        let result = eval_function("MAKEDATE", &[Datum::Int(2024), Datum::Int(1)], None);
        assert_eq!(result.unwrap(), Datum::String("2024-01-01".to_string()));
        let result = eval_function("MAKEDATE", &[Datum::Int(2024), Datum::Int(60)], None);
        assert_eq!(result.unwrap(), Datum::String("2024-02-29".to_string())); // leap year
    }

    #[test]
    fn test_last_day() {
        let result = eval_function("LAST_DAY", &[Datum::String("2024-02-15".to_string())], None);
        assert_eq!(result.unwrap(), Datum::String("2024-02-29".to_string()));
        let result = eval_function("LAST_DAY", &[Datum::String("2023-02-15".to_string())], None);
        assert_eq!(result.unwrap(), Datum::String("2023-02-28".to_string()));
    }

    #[test]
    fn test_sec_to_time() {
        assert_eq!(
            eval_function("SEC_TO_TIME", &[Datum::Int(3661)], None).unwrap(),
            Datum::String("01:01:01".to_string())
        );
        assert_eq!(
            eval_function("SEC_TO_TIME", &[Datum::Int(-3661)], None).unwrap(),
            Datum::String("-01:01:01".to_string())
        );
    }

    #[test]
    fn test_time_to_sec() {
        assert_eq!(
            eval_function(
                "TIME_TO_SEC",
                &[Datum::String("01:01:01".to_string())],
                None
            )
            .unwrap(),
            Datum::Int(3661)
        );
    }

    #[test]
    fn test_period_add() {
        assert_eq!(
            eval_function("PERIOD_ADD", &[Datum::Int(202401), Datum::Int(5)], None).unwrap(),
            Datum::Int(202406)
        );
        // Cross year boundary
        assert_eq!(
            eval_function("PERIOD_ADD", &[Datum::Int(202411), Datum::Int(3)], None).unwrap(),
            Datum::Int(202502)
        );
    }

    #[test]
    fn test_period_diff() {
        assert_eq!(
            eval_function(
                "PERIOD_DIFF",
                &[Datum::Int(202403), Datum::Int(202401)],
                None
            )
            .unwrap(),
            Datum::Int(2)
        );
    }

    #[test]
    fn test_date_and_time_extract() {
        let dt = Datum::String("2024-03-23 14:30:45".to_string());
        assert_eq!(
            eval_function("DATE", &[dt.clone()], None).unwrap(),
            Datum::String("2024-03-23".to_string())
        );
        assert_eq!(
            eval_function("TIME", &[dt], None).unwrap(),
            Datum::String("14:30:45".to_string())
        );
    }

    #[test]
    fn test_null_propagation_datetime_funcs() {
        assert_eq!(
            eval_function("HOUR", &[Datum::Null], None).unwrap(),
            Datum::Null
        );
        assert_eq!(
            eval_function("MINUTE", &[Datum::Null], None).unwrap(),
            Datum::Null
        );
        assert_eq!(
            eval_function("DAYNAME", &[Datum::Null], None).unwrap(),
            Datum::Null
        );
        assert_eq!(
            eval_function("MONTHNAME", &[Datum::Null], None).unwrap(),
            Datum::Null
        );
        assert_eq!(
            eval_function(
                "DATE_FORMAT",
                &[Datum::Null, Datum::String("%Y".to_string())],
                None,
            )
            .unwrap(),
            Datum::Null
        );
        assert_eq!(
            eval_function(
                "DATE_FORMAT",
                &[Datum::String("2024-01-01".to_string()), Datum::Null],
                None,
            )
            .unwrap(),
            Datum::Null
        );
    }

    #[test]
    fn test_no_unsigned_subtraction_flag() {
        use crate::server::session::UserVariables;
        use std::collections::HashMap;
        use std::sync::Arc;

        let vars: UserVariables = Arc::new(parking_lot::RwLock::new(HashMap::new()));

        // Without the flag: UnsignedInt(MAX) - Int(1) should succeed
        set_no_unsigned_subtraction(&vars, false);
        let result = eval_sub(&Datum::UnsignedInt(u64::MAX), &Datum::Int(1), Some(&vars));
        assert!(result.is_ok(), "Without flag, should succeed");
        assert_eq!(result.unwrap(), Datum::UnsignedInt(u64::MAX - 1));

        // With the flag: UnsignedInt(MAX) - Int(1) should overflow (> i64::MAX)
        set_no_unsigned_subtraction(&vars, true);
        let result = eval_sub(&Datum::UnsignedInt(u64::MAX), &Datum::Int(1), Some(&vars));
        assert!(
            result.is_err(),
            "With NO_UNSIGNED_SUBTRACTION, u64::MAX - 1 should overflow"
        );

        // With the flag: UnsignedInt(1) - Int(2) should return -1
        let result = eval_sub(&Datum::UnsignedInt(1), &Datum::Int(2), Some(&vars));
        assert!(result.is_ok(), "With flag, 1 - 2 should succeed");
        assert_eq!(result.unwrap(), Datum::Int(-1));
    }

    #[test]
    fn test_abs_int() {
        assert_eq!(
            eval_function("ABS", &[Datum::Int(-42)], None).unwrap(),
            Datum::Int(42)
        );
        assert_eq!(
            eval_function("ABS", &[Datum::Int(42)], None).unwrap(),
            Datum::Int(42)
        );
        assert_eq!(
            eval_function("ABS", &[Datum::Int(0)], None).unwrap(),
            Datum::Int(0)
        );
    }

    #[test]
    fn test_abs_int_min_overflow() {
        // ABS(i64::MIN) should error — |i64::MIN| > i64::MAX
        let result = eval_function("ABS", &[Datum::Int(i64::MIN)], None);
        assert!(result.is_err(), "ABS(i64::MIN) should overflow");
    }

    #[test]
    fn test_abs_decimal() {
        assert_eq!(
            eval_function(
                "ABS",
                &[Datum::Decimal {
                    value: -100,
                    scale: 2
                }],
                None,
            )
            .unwrap(),
            Datum::Decimal {
                value: 100,
                scale: 2
            }
        );
        assert_eq!(
            eval_function(
                "ABS",
                &[Datum::Decimal {
                    value: 100,
                    scale: 2
                }],
                None,
            )
            .unwrap(),
            Datum::Decimal {
                value: 100,
                scale: 2
            }
        );
    }

    #[test]
    fn test_abs_unsigned() {
        assert_eq!(
            eval_function("ABS", &[Datum::UnsignedInt(42)], None).unwrap(),
            Datum::UnsignedInt(42)
        );
    }

    #[test]
    fn test_abs_null() {
        assert_eq!(
            eval_function("ABS", &[Datum::Null], None).unwrap(),
            Datum::Null
        );
    }

    #[test]
    fn test_unary_neg_int_min_overflow() {
        use crate::planner::logical::UnaryOp;
        // Negating i64::MIN should produce DataOutOfRange, not InvalidOperation
        let result = eval_unary_op(&UnaryOp::Neg, &Datum::Int(i64::MIN));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ExecutorError::DataOutOfRange(_)),
            "Expected DataOutOfRange, got {:?}",
            err
        );
    }

    #[test]
    fn test_unary_neg_non_numeric() {
        use crate::planner::logical::UnaryOp;
        // Negating a string should produce InvalidOperation
        let result = eval_unary_op(&UnaryOp::Neg, &Datum::String("hello".to_string()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ExecutorError::InvalidOperation(_)),
            "Expected InvalidOperation, got {:?}",
            err
        );
    }
}
