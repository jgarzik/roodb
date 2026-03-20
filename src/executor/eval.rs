//! Expression evaluation
//!
//! Evaluates ResolvedExpr against a Row to produce a Datum.

use crate::planner::logical::{BinaryOp, BooleanTestType, ResolvedExpr, UnaryOp};

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

/// Evaluate an expression against a row
pub fn eval(expr: &ResolvedExpr, row: &Row) -> ExecutorResult<Datum> {
    match expr {
        ResolvedExpr::Column(col) => {
            let datum = row.get(col.index)?;
            Ok(datum.clone())
        }

        ResolvedExpr::Literal(lit) => Ok(Datum::from_literal(lit)),

        ResolvedExpr::BinaryOp {
            left, op, right, ..
        } => {
            let lval = eval(left, row)?;
            let rval = eval(right, row)?;
            eval_binary_op(op, &lval, &rval)
        }

        ResolvedExpr::UnaryOp { op, expr, .. } => {
            let val = eval(expr, row)?;
            eval_unary_op(op, &val)
        }

        ResolvedExpr::Function { name, args, .. } => {
            // IF() requires short-circuit evaluation — only compute the taken branch
            if name.eq_ignore_ascii_case("IF") {
                return eval_if_function(args, row);
            }
            let arg_vals: Vec<Datum> = args
                .iter()
                .map(|a| eval(a, row))
                .collect::<Result<_, _>>()?;
            eval_function(name, &arg_vals)
        }

        ResolvedExpr::IsNull { expr, negated } => {
            let val = eval(expr, row)?;
            let is_null = val.is_null();
            Ok(Datum::Bool(if *negated { !is_null } else { is_null }))
        }

        ResolvedExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval(expr, row)?;
            if val.is_null() {
                return Ok(Datum::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = eval(item, row)?;
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
            let val = eval(expr, row)?;
            let low_val = eval(low, row)?;
            let high_val = eval(high, row)?;

            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Datum::Null);
            }

            let in_range = val >= low_val && val <= high_val;
            Ok(Datum::Bool(if *negated { !in_range } else { in_range }))
        }

        ResolvedExpr::BooleanTest { expr, test } => {
            let val = eval(expr, row)?;
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
fn eval_binary_op(op: &BinaryOp, left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
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
    }
}

fn eval_add(left: &Datum, right: &Datum) -> ExecutorResult<Datum> {
    match (left, right) {
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
    match (left, right) {
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
    match (left, right) {
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
    // Division by zero returns NULL (MySQL semantics)
    match right {
        Datum::Int(0) => return Ok(Datum::Null),
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        _ => {}
    }

    match (left, right) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a / b)),
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
    // Modulo by zero returns NULL (MySQL semantics)
    match right {
        Datum::Int(0) => return Ok(Datum::Null),
        Datum::Float(f) if *f == 0.0 => return Ok(Datum::Null),
        _ => {}
    }

    match (left, right) {
        (Datum::Int(a), Datum::Int(b)) => Ok(Datum::Int(a % b)),
        (Datum::Float(a), Datum::Float(b)) => Ok(Datum::Float(a % b)),
        (Datum::Int(a), Datum::Float(b)) => Ok(Datum::Float(*a as f64 % b)),
        (Datum::Float(a), Datum::Int(b)) => Ok(Datum::Float(a % *b as f64)),
        _ => Err(ExecutorError::InvalidOperation(format!(
            "cannot compute modulo of {:?} and {:?}",
            left, right
        ))),
    }
}

/// Coerce a Datum to i64 for bitwise operations (MySQL truncates floats).
fn to_bitwise_int(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int(i) => Some(*i),
        Datum::Float(f) => Some(*f as i64),
        Datum::Bool(b) => Some(if *b { 1 } else { 0 }),
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
fn eval_unary_op(op: &UnaryOp, val: &Datum) -> ExecutorResult<Datum> {
    match op {
        UnaryOp::Not => val
            .not()
            .ok_or_else(|| ExecutorError::InvalidOperation("NOT requires boolean".to_string())),
        UnaryOp::Neg => val
            .negate()
            .ok_or_else(|| ExecutorError::InvalidOperation("negation requires number".to_string())),
    }
}

/// Evaluate a scalar function
fn eval_function(name: &str, args: &[Datum]) -> ExecutorResult<Datum> {
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

        "CONCAT" => {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Datum::String(s) => result.push_str(s),
                    Datum::Int(i) => result.push_str(&i.to_string()),
                    Datum::Float(f) => result.push_str(&f.to_string()),
                    Datum::Bool(b) => result.push_str(if *b { "true" } else { "false" }),
                    Datum::Null => return Ok(Datum::Null),
                    _ => {
                        return Err(ExecutorError::InvalidOperation(
                            "CONCAT: unsupported type".to_string(),
                        ))
                    }
                }
            }
            Ok(Datum::String(result))
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

        // Note: Aggregate functions (COUNT, SUM, AVG, MIN, MAX) are handled
        // by the Aggregate executor, not here
        _ => Err(ExecutorError::InvalidOperation(format!(
            "unknown function: {}",
            name
        ))),
    }
}

/// Evaluate IF(cond, then_expr, else_expr) with short-circuit evaluation.
/// NULL condition takes the else branch (MySQL semantics).
fn eval_if_function(args: &[ResolvedExpr], row: &Row) -> ExecutorResult<Datum> {
    if args.len() != 3 {
        return Err(ExecutorError::InvalidOperation(
            "IF requires 3 arguments".to_string(),
        ));
    }
    let cond = eval(&args[0], row)?;
    let is_true = cond.as_bool().unwrap_or_default();
    if is_true {
        eval(&args[1], row)
    } else {
        eval(&args[2], row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::planner::logical::{Literal, ResolvedColumn};

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
