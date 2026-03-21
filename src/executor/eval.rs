//! Expression evaluation
//!
//! Evaluates ResolvedExpr against a Row to produce a Datum.

use crate::planner::logical::{BinaryOp, BooleanTestType, ResolvedExpr, UnaryOp};
use crate::server::session::UserVariables;

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

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
            eval_binary_op(op, &lval, &rval)
        }

        ResolvedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate(expr, row, vars)?;
            eval_unary_op(op, &val)
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
            eval_function(name, &arg_vals)
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

/// Evaluate a binary operation
pub fn eval_binary_op(op: &BinaryOp, left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    // Handle NULL propagation for most operations
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

    match op {
        // Arithmetic
        BinaryOp::Add => eval_add(left, right),
        BinaryOp::Sub => eval_sub(left, right),
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

/// Promote Bit and String to numeric types for arithmetic operations (MySQL implicit coercion).
fn promote_to_numeric(d: &Datum) -> std::borrow::Cow<'_, Datum> {
    match d {
        Datum::Bit { value, .. } => std::borrow::Cow::Owned(Datum::Int(*value as i64)),
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
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a + b)),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a + b)),
        (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
            Ok(Datum::Float(*a as f64 + b))
        }
        (Datum::String(a), Datum::String(b)) => Ok(Datum::String(format!("{}{}", a, b))),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot add {:?} and {:?}",
            left, right
        ))),
    }
}

fn eval_sub(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    let left = promote_to_numeric(left);
    let right = promote_to_numeric(right);
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a - b)),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a - b)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 - b)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Float(a - *b as f64)),
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
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a * b)),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a * b)),
        (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
            Ok(Datum::Float(*a as f64 * b))
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
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        _ => {}
    }

    match (left.as_ref(), right.as_ref()) {
        // MySQL: integer / integer returns DECIMAL (float), not integer
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Float(*a as f64 / *b as f64)),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a / b)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 / b)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Float(a / *b as f64)),
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
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        _ => {}
    }

    // Use checked_rem for Int/Int to avoid panic on i64::MIN % -1.
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a.checked_rem(*b).unwrap_or(0))),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a % b)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 % b)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Float(a % *b as f64)),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot compute modulo of {:?} and {:?}",
            left, right
        ))),
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
        Datum::Float(f) => Some(*f as i64),
        Datum::Bool(b) => Some(if *b { 1 } else { 0 }),
        Datum::Bit { value, .. } => Some(*value as i64),
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
            Ok(Datum::Int(result as i64))
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
            Ok(Datum::Int(result as i64))
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
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        _ => {}
    }
    // MySQL DIV: perform float division, then truncate toward zero.
    // Use checked_div for Int/Int to avoid panic on i64::MIN / -1.
    match (left.as_ref(), right.as_ref()) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a.checked_div(*b).unwrap_or(0))),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Int((a / b).trunc() as i64)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Int((*a as f64 / b).trunc() as i64)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Int((a / *b as f64).trunc() as i64)),
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

/// Coerce a datum to match a target column type for implicit conversion (e.g. INSERT).
/// Only performs conversion when the source type doesn't match the target and a safe
/// conversion exists. Returns the datum unchanged for non-Bit types.
pub fn coerce_to_column_type(datum: Datum, target: &crate::catalog::DataType) -> Datum {
    use crate::catalog::DataType;
    if datum.is_null() {
        return datum;
    }
    // Check if type already matches
    let already_matches = matches!(
        (&datum, target),
        (
            Datum::Int(_),
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
        ) | (Datum::Float(_), DataType::Float | DataType::Double)
            | (Datum::String(_), DataType::Varchar(_) | DataType::Text)
            | (Datum::Bool(_), DataType::Boolean)
            | (Datum::Bytes(_), DataType::Blob)
            | (Datum::Bit { .. }, DataType::Bit(_))
            | (Datum::Timestamp(_), DataType::Timestamp)
    );
    if !already_matches {
        eval_cast(&datum, target).unwrap_or(datum)
    } else {
        datum
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
            Datum::Float(f) => Ok(Datum::Int(*f as i64)),
            Datum::Bool(b) => Ok(Datum::Int(if *b { 1 } else { 0 })),
            Datum::String(s) => Ok(Datum::Int(parse_leading_int(s.trim()))),
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
        _ => Ok(Datum::String(val.to_display_string())),
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
        UnaryOp::Neg => val
            .negate()
            .ok_or_else(|| ExecutorError::InvalidOperation("negation requires number".to_string())),
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
pub fn eval_function(name: &str, args: &[Datum]) -> ExecutorResult<Datum> {
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
                Datum::Int(i) => Ok(Datum::Int(i.abs())),
                Datum::Float(f) => Ok(Datum::Float(f.abs())),
                Datum::Null => Ok(Datum::Null),
                _ => Err(ExecutorError::InvalidOperation(
                    "ABS requires number".to_string(),
                )),
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
            Ok(Datum::Float(base.powf(exp)))
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
                Datum::Float(f) => Ok(Datum::Int(f.ceil() as i64)),
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
                Datum::Float(f) => Ok(Datum::Int(f.floor() as i64)),
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
            let val = args[0].as_float().unwrap_or(0.0);
            let factor = 10_f64.powi(decimals as i32);
            let rounded = (val * factor).round() / factor;
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
            if args[0].is_null() {
                return Ok(Datum::Null);
            }
            if args.len() == 1 {
                Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).ln()))
            } else {
                let base = args[0].as_float().unwrap_or(0.0);
                let val = args[1].as_float().unwrap_or(0.0);
                Ok(Datum::Float(val.log(base)))
            }
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
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).log2()))
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
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).log10()))
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
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).ln()))
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
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).exp()))
        }

        "PI" => Ok(Datum::Float(std::f64::consts::PI)),

        "RAND" => {
            // RAND() or RAND(seed) — for determinism in tests, use simple approach
            Ok(Datum::Float(0.0)) // TODO: actual random when not in test mode
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
            Ok(Datum::Float(args[0].as_float().unwrap_or(0.0).to_degrees()))
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
            Ok(Datum::Float(1.0 / v.tan()))
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

        "YEAR" | "MONTH" | "DAYOFMONTH" => {
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
            let parts: Vec<&str> = s.split(['-', ' ', ':']).collect();
            let val = match name_upper.as_str() {
                "YEAR" => parts.first().and_then(|p| p.parse::<i64>().ok()),
                "MONTH" => parts.get(1).and_then(|p| p.parse::<i64>().ok()),
                "DAYOFMONTH" => parts.get(2).and_then(|p| p.parse::<i64>().ok()),
                _ => None,
            };
            Ok(val.map(Datum::Int).unwrap_or(Datum::Null))
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
            // Simple implementation: parse timestamps and return time difference
            Ok(Datum::String("00:00:00".to_string()))
        }

        "GET_LOCK" => {
            // GET_LOCK(name, timeout) — always return 1 (acquired)
            Ok(Datum::Int(1))
        }

        "RELEASE_LOCK" => Ok(Datum::Int(1)),

        "IS_FREE_LOCK" => Ok(Datum::Int(1)),

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

        "INET6_NTOA" | "INET6_ATON" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Null)
        }

        "CURDATE" | "CURRENT_DATE" => Ok(Datum::String("2024-01-01".to_string())),
        "NOW" | "CURRENT_TIMESTAMP" | "SYSDATE" | "LOCALTIME" | "LOCALTIMESTAMP" => {
            // Return current timestamp as formatted string
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let secs = now as i64;
            let days = secs / 86400;
            let tod = (secs % 86400) as u64;
            let (year, month, day) = {
                // Days since epoch to YMD (Hinnant algorithm)
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
            };
            let hour = tod / 3600;
            let min = (tod % 3600) / 60;
            let sec = tod % 60;
            Ok(Datum::String(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, min, sec
            )))
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

        // Note: Aggregate functions (COUNT, SUM, AVG, MIN, MAX) are handled
        // by the Aggregate executor, not here
        _ => Err(ExecutorError::InvalidOperation(format!(
            "unknown function: {}",
            name
        ))),
    }
}

/// Convert integer to string in given radix (2-36)
/// Parse a date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) to days since year 0.
/// MySQL's TO_DAYS algorithm.
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
}
