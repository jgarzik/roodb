//! Datum type - runtime values in the executor

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::catalog::DataType;
use crate::planner::logical::Literal;

/// A single value in a row
#[derive(Debug, Clone, Default)]
pub enum Datum {
    /// NULL value
    #[default]
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (covers TinyInt, SmallInt, Int, BigInt)
    Int(i64),
    /// Unsigned 64-bit integer (BIGINT UNSIGNED, CAST AS UNSIGNED, shift results)
    UnsignedInt(u64),
    /// Floating point value (covers Float, Double)
    Float(f64),
    /// String value (covers Varchar, Text)
    String(String),
    /// Binary data (Blob)
    Bytes(Vec<u8>),
    /// Bit string value (width 1..64, value masked to width)
    Bit { value: u64, width: u8 },
    /// Timestamp as unix milliseconds
    Timestamp(i64),
    /// Fixed-point decimal (unscaled value, scale)
    Decimal { value: i128, scale: u8 },
}

impl Datum {
    /// Check if this datum is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Get the default/zero value for a given data type.
    /// Used when MySQL silently coerces NULL to default on NOT NULL columns.
    pub fn default_for_type(dt: &DataType) -> Datum {
        match dt {
            DataType::Boolean => Datum::Bool(false),
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                Datum::Int(0)
            }
            DataType::Float | DataType::Double => Datum::Float(0.0),
            DataType::Varchar(_) | DataType::Text => Datum::String(String::new()),
            DataType::Blob => Datum::Bytes(Vec::new()),
            DataType::Bit(w) => Datum::Bit {
                value: 0,
                width: *w,
            },
            DataType::BigIntUnsigned => Datum::UnsignedInt(0),
            DataType::Timestamp => Datum::Int(0),
            DataType::Decimal { scale, .. } => Datum::Decimal {
                value: 0,
                scale: *scale,
            },
        }
    }

    /// Get a numeric type tag for ordering different types
    fn type_tag(&self) -> u8 {
        match self {
            Datum::Null => 0,
            Datum::Bool(_) => 1,
            Datum::Int(_) => 2,
            Datum::UnsignedInt(_) => 2,
            Datum::Float(_) => 3,
            Datum::String(_) => 4,
            Datum::Bytes(_) => 5,
            Datum::Timestamp(_) => 6,
            Datum::Bit { .. } => 7,
            Datum::Decimal { .. } => 3, // same as Float for cross-type ordering
        }
    }

    /// Get the data type of this datum
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(DataType::Boolean),
            Datum::Int(_) => Some(DataType::BigInt),
            Datum::UnsignedInt(_) => Some(DataType::BigIntUnsigned),
            Datum::Float(_) => Some(DataType::Double),
            Datum::String(_) => Some(DataType::Text),
            Datum::Bytes(_) => Some(DataType::Blob),
            Datum::Bit { width, .. } => Some(DataType::Bit(*width)),
            Datum::Timestamp(_) => Some(DataType::Timestamp),
            Datum::Decimal { value, scale } => {
                let digits = if *value == 0 {
                    1
                } else {
                    value.unsigned_abs().ilog10() as u8 + 1
                };
                let precision = digits.max(*scale);
                Some(DataType::Decimal {
                    precision,
                    scale: *scale,
                })
            }
        }
    }

    /// Convert to boolean, returns None if NULL or not convertible
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Datum::Bool(b) => Some(*b),
            Datum::Int(i) => Some(*i != 0),
            Datum::UnsignedInt(u) => Some(*u != 0),
            Datum::Bit { value, .. } => Some(*value != 0),
            Datum::Decimal { value, .. } => Some(*value != 0),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to i64, returns None if NULL or not convertible
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Datum::Int(i) => Some(*i),
            Datum::UnsignedInt(u) => Some(*u as i64),
            Datum::Float(f) => Some(*f as i64),
            Datum::Bool(b) => Some(if *b { 1 } else { 0 }),
            Datum::Bit { value, .. } => Some(*value as i64),
            Datum::Decimal { value, scale } => {
                let divisor = 10i128.pow(*scale as u32);
                Some((value / divisor) as i64)
            }
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to f64, returns None if NULL or not convertible
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Datum::Float(f) => Some(*f),
            Datum::Int(i) => Some(*i as f64),
            Datum::UnsignedInt(u) => Some(*u as f64),
            Datum::Bit { value, .. } => Some(*value as f64),
            Datum::Decimal { value, scale } => Some(*value as f64 / 10f64.powi(*scale as i32)),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to string reference, returns None if NULL or not a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::String(s) => Some(s),
            _ => None,
        }
    }

    /// Convert to bytes reference, returns None if NULL or not bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Datum::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Convert to timestamp (unix millis), returns None if NULL or not timestamp
    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Datum::Timestamp(t) => Some(*t),
            Datum::Int(i) => Some(*i),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to a SQL literal string for substitution into SQL statements.
    /// Properly escapes and quotes values for safe embedding in SQL.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Datum::Null => "NULL".to_string(),
            Datum::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Datum::Int(i) => i.to_string(),
            Datum::UnsignedInt(u) => u.to_string(),
            Datum::Float(f) => f.to_string(),
            Datum::String(s) => {
                // Single-quote escape: replace ' with ''
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
            Datum::Bytes(b) => {
                let mut s = String::with_capacity(3 + b.len() * 2);
                s.push_str("X'");
                for byte in b {
                    s.push_str(&format!("{:02X}", byte));
                }
                s.push('\'');
                s
            }
            Datum::Bit { value, width } => {
                let w = *width as usize;
                let mut s = String::with_capacity(w + 2);
                s.push_str("b'");
                for i in (0..w).rev() {
                    s.push(if (value >> i) & 1 == 1 { '1' } else { '0' });
                }
                s.push('\'');
                s
            }
            Datum::Timestamp(t) => t.to_string(),
            Datum::Decimal { value, scale } => format_decimal(*value, *scale),
        }
    }

    /// Convert to display string for CAST and output formatting
    pub fn to_display_string(&self) -> String {
        match self {
            Datum::Null => "NULL".to_string(),
            Datum::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            Datum::Int(i) => i.to_string(),
            Datum::UnsignedInt(u) => u.to_string(),
            Datum::Float(f) => {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    format!("{:.1}", f) // Show at least one decimal
                } else {
                    f.to_string()
                }
            }
            Datum::String(s) => s.clone(),
            Datum::Bytes(b) => {
                let mut s = String::with_capacity(2 + b.len() * 2);
                s.push_str("0x");
                for byte in b {
                    s.push_str(&format!("{:02X}", byte));
                }
                s
            }
            Datum::Bit { value, .. } => value.to_string(),
            Datum::Timestamp(t) => t.to_string(),
            Datum::Decimal { value, scale } => format_decimal(*value, *scale),
        }
    }

    /// Create a Datum from a Literal
    pub fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Null => Datum::Null,
            Literal::Boolean(b) => Datum::Bool(*b),
            Literal::Integer(i) => Datum::Int(*i),
            Literal::UnsignedInteger(u) => Datum::UnsignedInt(*u),
            Literal::Float(f) => Datum::Float(*f),
            Literal::String(s) => Datum::String(s.clone()),
            Literal::Blob(b) => Datum::Bytes(b.clone()),
            Literal::Decimal(value, scale) => Datum::Decimal {
                value: *value,
                scale: *scale,
            },
            Literal::Placeholder(i) => {
                panic!("Unsubstituted placeholder ?{} in plan execution", i)
            }
        }
    }

    /// Negate this datum (for unary minus)
    pub fn negate(&self) -> Option<Datum> {
        match self {
            Datum::Bool(b) => Some(Datum::Int(if *b { -1 } else { 0 })),
            Datum::Int(i) => i.checked_neg().map(Datum::Int),
            Datum::Float(f) => Some(Datum::Float(-f)),
            Datum::UnsignedInt(u) => {
                // -UnsignedInt: convert to signed if result fits in BIGINT
                // e.g. -(2^63) == i64::MIN, which fits; larger values overflow
                let neg = -(*u as i128);
                if neg >= i64::MIN as i128 {
                    Some(Datum::Int(neg as i64))
                } else {
                    None // overflow — result doesn't fit in BIGINT
                }
            }
            Datum::Decimal { value, scale } => Some(Datum::Decimal {
                value: -value,
                scale: *scale,
            }),
            Datum::Null => Some(Datum::Null),
            Datum::Bit { .. } => None,
            _ => None,
        }
    }

    /// Logical NOT — MySQL treats NOT on integers as: NOT 0 = 1, NOT nonzero = 0
    pub fn not(&self) -> Option<Datum> {
        match self {
            Datum::Bool(b) => Some(Datum::Bool(!b)),
            Datum::Int(i) => Some(Datum::Bool(*i == 0)),
            Datum::UnsignedInt(u) => Some(Datum::Bool(*u == 0)),
            Datum::Float(f) => Some(Datum::Bool(*f == 0.0)),
            Datum::Bit { value, .. } => Some(Datum::Bool(*value == 0)),
            Datum::Decimal { value, .. } => Some(Datum::Bool(*value == 0)),
            Datum::Null => Some(Datum::Null),
            _ => None,
        }
    }

    /// Check if string matches pattern (SQL LIKE)
    /// MySQL coerces non-string operands to strings for LIKE comparison
    pub fn like(&self, pattern: &Datum) -> Option<Datum> {
        match (self, pattern) {
            (Datum::Null, _) | (_, Datum::Null) => Some(Datum::Null),
            (Datum::String(s), Datum::String(p)) => {
                let matched = like_match(s, p);
                Some(Datum::Bool(matched))
            }
            _ => {
                // Coerce to string for non-string LIKE comparisons
                let s = self.to_display_string();
                let p = pattern.to_display_string();
                Some(Datum::Bool(like_match(&s, &p)))
            }
        }
    }
}

/// Reduce a decimal to canonical form by stripping trailing fractional zeros.
/// e.g., (12300, 2) → (123, 0), (12345, 2) → (12345, 2)
fn canonical_decimal(value: i128, scale: u8) -> (i128, u8) {
    let mut v = value;
    let mut s = scale;
    while s > 0 && v % 10 == 0 {
        v /= 10;
        s -= 1;
    }
    (v, s)
}

/// Format a decimal value with the given scale as a string.
/// e.g., value=12345, scale=2 → "123.45"
pub fn format_decimal(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let negative = value < 0;
    let abs_val = value.unsigned_abs();
    let divisor = 10u128.pow(scale as u32);
    let integer_part = abs_val / divisor;
    let frac_part = abs_val % divisor;
    if negative {
        format!(
            "-{}.{:0>width$}",
            integer_part,
            frac_part,
            width = scale as usize
        )
    } else {
        format!(
            "{}.{:0>width$}",
            integer_part,
            frac_part,
            width = scale as usize
        )
    }
}

/// Simple SQL LIKE pattern matching without regex
/// % matches any sequence of characters
/// _ matches any single character
fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_impl(&s_chars, &p_chars)
}

fn like_match_impl(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }

    match p[0] {
        '%' => {
            // % matches zero or more characters
            // Try matching zero characters, then one, then two, etc.
            for i in 0..=s.len() {
                if like_match_impl(&s[i..], &p[1..]) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            !s.is_empty() && like_match_impl(&s[1..], &p[1..])
        }
        '\\' if p.len() > 1 => {
            // Escape sequence
            !s.is_empty() && s[0] == p[1] && like_match_impl(&s[1..], &p[2..])
        }
        c => {
            // Literal character match (case-sensitive)
            !s.is_empty() && s[0] == c && like_match_impl(&s[1..], &p[1..])
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Bool(a), Datum::Bool(b)) => a == b,
            (Datum::Int(a), Datum::Int(b)) => a == b,
            (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => a == b,
            // Cross-type: Int/UnsignedInt
            (Datum::Int(i), Datum::UnsignedInt(u)) | (Datum::UnsignedInt(u), Datum::Int(i)) => {
                if *i < 0 {
                    false
                } else {
                    *i as u64 == *u
                }
            }
            (Datum::Float(a), Datum::Float(b)) => a.to_bits() == b.to_bits(),
            (Datum::String(a), Datum::String(b)) => a == b,
            (Datum::Bytes(a), Datum::Bytes(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            (Datum::Bit { value: a, .. }, Datum::Bit { value: b, .. }) => a == b,
            // Cross-type: Bit/Int — BIT is unsigned, compare as u64
            (Datum::Bit { value, .. }, Datum::Int(i))
            | (Datum::Int(i), Datum::Bit { value, .. }) => *value == *i as u64,
            // Cross-type numeric comparisons
            (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
                (*a as f64).to_bits() == b.to_bits()
            }
            // Decimal comparisons
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
                if sa == sb {
                    a == b
                } else if sa < sb {
                    let factor = 10i128.pow((*sb - *sa) as u32);
                    a.checked_mul(factor) == Some(*b)
                } else {
                    let factor = 10i128.pow((*sa - *sb) as u32);
                    b.checked_mul(factor) == Some(*a)
                }
            }
            // Decimal vs Int/UnsignedInt: canonicalize decimal, then compare exactly
            (Datum::Decimal { value, scale }, Datum::Int(i))
            | (Datum::Int(i), Datum::Decimal { value, scale }) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    cv == *i as i128
                } else {
                    false // non-zero fractional part can't equal an integer
                }
            }
            (Datum::Decimal { value, scale }, Datum::UnsignedInt(u))
            | (Datum::UnsignedInt(u), Datum::Decimal { value, scale }) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    cv == *u as i128
                } else {
                    false
                }
            }
            // Decimal vs Float: compare via f64 (same as Int vs Float)
            (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
                match (self.as_float(), other.as_float()) {
                    (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

impl Eq for Datum {}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // NULLs sort first (smallest)
            (Datum::Null, Datum::Null) => Ordering::Equal,
            (Datum::Null, _) => Ordering::Less,
            (_, Datum::Null) => Ordering::Greater,

            (Datum::Bool(a), Datum::Bool(b)) => a.cmp(b),
            (Datum::Int(a), Datum::Int(b)) => a.cmp(b),
            (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => a.cmp(b),
            // Cross-type: Int/UnsignedInt
            (Datum::Int(i), Datum::UnsignedInt(u)) => {
                if *i < 0 {
                    Ordering::Less
                } else {
                    (*i as u64).cmp(u)
                }
            }
            (Datum::UnsignedInt(u), Datum::Int(i)) => {
                if *i < 0 {
                    Ordering::Greater
                } else {
                    u.cmp(&(*i as u64))
                }
            }
            (Datum::Float(a), Datum::Float(b)) => a.total_cmp(b),
            (Datum::String(a), Datum::String(b)) => a.cmp(b),
            (Datum::Bytes(a), Datum::Bytes(b)) => a.cmp(b),
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a.cmp(b),
            (Datum::Bit { value: a, .. }, Datum::Bit { value: b, .. }) => a.cmp(b),

            // Cross-type: Bit/Int — BIT is unsigned, compare as u64
            (Datum::Bit { value, .. }, Datum::Int(i)) => value.cmp(&(*i as u64)),
            (Datum::Int(i), Datum::Bit { value, .. }) => (*i as u64).cmp(value),

            // Cross-type numeric comparisons
            (Datum::Int(a), Datum::Float(b)) => (*a as f64).total_cmp(b),
            (Datum::Float(a), Datum::Int(b)) => a.total_cmp(&(*b as f64)),

            // Decimal ordering
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
                if sa == sb {
                    a.cmp(b)
                } else if sa < sb {
                    let factor = 10i128.pow((*sb - *sa) as u32);
                    match a.checked_mul(factor) {
                        Some(a_scaled) => a_scaled.cmp(b),
                        None => {
                            // Overflow means |a| is huge; use sign to determine order
                            if *a >= 0 {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                    }
                } else {
                    let factor = 10i128.pow((*sa - *sb) as u32);
                    match b.checked_mul(factor) {
                        Some(b_scaled) => a.cmp(&b_scaled),
                        None => {
                            if *b >= 0 {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    }
                }
            }
            // Decimal vs Int: canonicalize, compare via i128
            (Datum::Decimal { value, scale }, Datum::Int(i)) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    cv.cmp(&(*i as i128))
                } else {
                    // Has fractional part: compare with exact scaling
                    let i_scaled = (*i as i128).wrapping_mul(10i128.pow(cs as u32));
                    cv.cmp(&i_scaled)
                }
            }
            (Datum::Int(i), Datum::Decimal { value, scale }) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    (*i as i128).cmp(&cv)
                } else {
                    let i_scaled = (*i as i128).wrapping_mul(10i128.pow(cs as u32));
                    i_scaled.cmp(&cv)
                }
            }
            (Datum::Decimal { value, scale }, Datum::UnsignedInt(u)) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    cv.cmp(&(*u as i128))
                } else {
                    let u_scaled = (*u as i128).wrapping_mul(10i128.pow(cs as u32));
                    cv.cmp(&u_scaled)
                }
            }
            (Datum::UnsignedInt(u), Datum::Decimal { value, scale }) => {
                let (cv, cs) = canonical_decimal(*value, *scale);
                if cs == 0 {
                    (*u as i128).cmp(&cv)
                } else {
                    let u_scaled = (*u as i128).wrapping_mul(10i128.pow(cs as u32));
                    u_scaled.cmp(&cv)
                }
            }
            // Decimal vs Float: compare via f64
            (Datum::Decimal { .. }, Datum::Float(_)) | (Datum::Float(_), Datum::Decimal { .. }) => {
                match (self.as_float(), other.as_float()) {
                    (Some(a), Some(b)) => a.total_cmp(&b),
                    _ => self.type_tag().cmp(&other.type_tag()),
                }
            }

            // Different types: use type tag for stable ordering
            _ => self.type_tag().cmp(&other.type_tag()),
        }
    }
}

impl Hash for Datum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            // Int and Float share a discriminant tag so that cross-type
            // equal values (e.g. Int(1) == Float(1.0)) hash identically.
            Datum::Int(i) => {
                0u8.hash(state);
                (*i as f64).to_bits().hash(state);
            }
            Datum::Float(f) => {
                0u8.hash(state);
                f.to_bits().hash(state);
            }
            Datum::UnsignedInt(u) => {
                // Values <= i64::MAX are equal to Int(v), so must hash identically.
                // Values > i64::MAX cannot equal any Int, so use a distinct discriminant.
                if *u <= i64::MAX as u64 {
                    0u8.hash(state);
                    (*u as i64 as f64).to_bits().hash(state);
                } else {
                    10u8.hash(state);
                    u.hash(state);
                }
            }
            Datum::Bit { value, .. } => {
                // Use same discriminant tag as Int/Float so Bit(N) == Int(N) hashes match.
                // Cross-type PartialEq compares as u64 (*value == *i as u64), which is
                // equivalent to comparing as i64 for values that fit. Hash via the same
                // i64→f64 path as Datum::Int to stay consistent.
                0u8.hash(state);
                (*value as i64 as f64).to_bits().hash(state);
            }
            Datum::Decimal { value, scale } => {
                // Reduce to canonical form: strip trailing fractional zeros so that
                // Decimal(12300, 2) and Decimal(123, 0) hash identically (both equal 123).
                // This matches PartialEq which uses exact integer normalization.
                let mut v = *value;
                let mut s = *scale;
                while s > 0 && v % 10 == 0 {
                    v /= 10;
                    s -= 1;
                }
                if s == 0 {
                    // Integer-valued decimal: hash same as Int/Float for cross-type consistency
                    // (Int uses 0u8 + (i as f64).to_bits(), and PartialEq for Decimal==Int
                    // compares canonicalized value exactly)
                    0u8.hash(state);
                    (v as f64).to_bits().hash(state);
                } else {
                    // Fractional decimal: can't equal Int/UnsignedInt, use distinct tag
                    11u8.hash(state);
                    v.hash(state);
                    s.hash(state);
                }
            }
            other => {
                std::mem::discriminant(other).hash(state);
                match other {
                    Datum::Null => {}
                    Datum::Bool(b) => b.hash(state),
                    Datum::String(s) => s.hash(state),
                    Datum::Bytes(b) => b.hash(state),
                    Datum::Timestamp(t) => t.hash(state),
                    Datum::Int(_)
                    | Datum::UnsignedInt(_)
                    | Datum::Float(_)
                    | Datum::Bit { .. }
                    | Datum::Decimal { .. } => {
                        unreachable!()
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datum_null() {
        let d = Datum::Null;
        assert!(d.is_null());
        assert!(d.data_type().is_none());
    }

    #[test]
    fn test_datum_bool() {
        let d = Datum::Bool(true);
        assert!(!d.is_null());
        assert_eq!(d.as_bool(), Some(true));
        assert_eq!(d.as_int(), Some(1));
    }

    #[test]
    fn test_datum_int() {
        let d = Datum::Int(42);
        assert_eq!(d.as_int(), Some(42));
        assert_eq!(d.as_float(), Some(42.0));
    }

    #[test]
    fn test_datum_float() {
        let d = Datum::Float(2.5);
        assert_eq!(d.as_float(), Some(2.5));
        assert_eq!(d.as_int(), Some(2));
    }

    #[test]
    fn test_datum_string() {
        let d = Datum::String("hello".to_string());
        assert_eq!(d.as_str(), Some("hello"));
    }

    #[test]
    fn test_datum_comparison() {
        assert!(Datum::Int(1) < Datum::Int(2));
        assert!(Datum::Null < Datum::Int(0));
        assert_eq!(Datum::Float(1.0), Datum::Float(1.0));
    }

    #[test]
    fn test_datum_like() {
        let s = Datum::String("hello world".to_string());
        let p1 = Datum::String("hello%".to_string());
        let p2 = Datum::String("%world".to_string());
        let p3 = Datum::String("h_llo%".to_string());

        assert_eq!(s.like(&p1), Some(Datum::Bool(true)));
        assert_eq!(s.like(&p2), Some(Datum::Bool(true)));
        assert_eq!(s.like(&p3), Some(Datum::Bool(true)));
    }

    #[test]
    fn test_datum_from_literal() {
        assert!(matches!(Datum::from_literal(&Literal::Null), Datum::Null));
        assert!(matches!(
            Datum::from_literal(&Literal::Integer(42)),
            Datum::Int(42)
        ));
    }

    #[test]
    fn test_datum_negate() {
        assert_eq!(Datum::Int(5).negate(), Some(Datum::Int(-5)));
        assert_eq!(Datum::Int(-5).negate(), Some(Datum::Int(5)));
        assert_eq!(Datum::Int(0).negate(), Some(Datum::Int(0)));
        assert_eq!(Datum::Float(2.5).negate(), Some(Datum::Float(-2.5)));
        assert_eq!(Datum::Null.negate(), Some(Datum::Null));
    }

    #[test]
    fn test_datum_negate_int_min_overflow() {
        // i64::MIN cannot be negated (|i64::MIN| > i64::MAX)
        assert_eq!(Datum::Int(i64::MIN).negate(), None);
        // i64::MAX can be negated
        assert_eq!(Datum::Int(i64::MAX).negate(), Some(Datum::Int(-i64::MAX)));
    }

    #[test]
    fn test_datum_negate_unsigned_fits_bigint() {
        // -(2^63) == i64::MIN, which fits in BIGINT
        let val = i64::MAX as u64 + 1; // 2^63
        assert_eq!(Datum::UnsignedInt(val).negate(), Some(Datum::Int(i64::MIN)));
        // Small unsigned values negate fine
        assert_eq!(Datum::UnsignedInt(42).negate(), Some(Datum::Int(-42)));
        assert_eq!(Datum::UnsignedInt(0).negate(), Some(Datum::Int(0)));
    }

    #[test]
    fn test_datum_negate_unsigned_overflow() {
        // 2^63 + 1 doesn't fit in BIGINT when negated
        let val = i64::MAX as u64 + 2; // 2^63 + 1
        assert_eq!(Datum::UnsignedInt(val).negate(), None);
        // u64::MAX doesn't fit
        assert_eq!(Datum::UnsignedInt(u64::MAX).negate(), None);
    }

    #[test]
    fn test_datum_not() {
        assert_eq!(Datum::Bool(true).not(), Some(Datum::Bool(false)));
        assert_eq!(Datum::Bool(false).not(), Some(Datum::Bool(true)));
    }

    #[test]
    fn test_hash_eq_consistency_int_float() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn compute_hash(d: &Datum) -> u64 {
            let mut h = DefaultHasher::new();
            d.hash(&mut h);
            h.finish()
        }

        // Int(N) == Float(N.0) must imply identical hashes
        for n in [0, 1, -1, 42, i64::MIN, i64::MAX] {
            let int_val = Datum::Int(n);
            let float_val = Datum::Float(n as f64);
            assert_eq!(int_val, float_val, "PartialEq failed for {n}");
            assert_eq!(
                compute_hash(&int_val),
                compute_hash(&float_val),
                "Hash mismatch for Int({n}) vs Float({}.0)",
                n
            );
        }

        // Different values must not be equal
        assert_ne!(Datum::Int(1), Datum::Float(2.0));

        // Non-numeric types must still hash distinctly from numerics
        assert_ne!(
            compute_hash(&Datum::Bool(true)),
            compute_hash(&Datum::Int(1))
        );
    }

    #[test]
    fn test_int_float_hashmap_lookup() {
        use std::collections::HashMap;

        // Simulate the hash join scenario: build with Float, probe with Int
        let mut map: HashMap<Vec<Datum>, &str> = HashMap::new();
        map.insert(vec![Datum::Float(1.0)], "matched");

        // Probing with Int(1) must find the Float(1.0) entry
        assert_eq!(map.get(&vec![Datum::Int(1)]), Some(&"matched"));

        // And vice versa
        let mut map2: HashMap<Vec<Datum>, &str> = HashMap::new();
        map2.insert(vec![Datum::Int(42)], "found");
        assert_eq!(map2.get(&vec![Datum::Float(42.0)]), Some(&"found"));
    }

    #[test]
    fn test_datum_bit_basic() {
        let d = Datum::Bit { value: 5, width: 8 };
        assert!(!d.is_null());
        assert_eq!(d.as_bool(), Some(true));
        assert_eq!(d.as_int(), Some(5));
        assert_eq!(d.as_float(), Some(5.0));
        assert_eq!(d.data_type(), Some(DataType::Bit(8)));
        assert_eq!(d.to_display_string(), "5");
        assert_eq!(d.to_sql_literal(), "b'00000101'");
    }

    #[test]
    fn test_datum_bit_zero() {
        let d = Datum::Bit { value: 0, width: 1 };
        assert_eq!(d.as_bool(), Some(false));
        assert_eq!(d.as_int(), Some(0));
    }

    #[test]
    fn test_datum_bit_negate() {
        let d = Datum::Bit { value: 5, width: 8 };
        assert_eq!(d.negate(), None);
    }

    #[test]
    fn test_datum_bit_equality() {
        // Bit-Bit: same value, different width → equal
        assert_eq!(
            Datum::Bit { value: 5, width: 8 },
            Datum::Bit {
                value: 5,
                width: 16
            }
        );

        // Bit-Int cross-type
        assert_eq!(
            Datum::Bit {
                value: 42,
                width: 8
            },
            Datum::Int(42)
        );
        assert_eq!(
            Datum::Int(42),
            Datum::Bit {
                value: 42,
                width: 8
            }
        );

        // Not equal
        assert_ne!(Datum::Bit { value: 1, width: 1 }, Datum::Int(2));
    }

    #[test]
    fn test_datum_bit_ordering() {
        // Bit-Bit ordering: unsigned
        assert!(Datum::Bit { value: 1, width: 8 } < Datum::Bit { value: 2, width: 8 });
        assert!(
            Datum::Bit {
                value: 255,
                width: 8
            } > Datum::Bit { value: 0, width: 8 }
        );

        // Cross-type Bit/Int ordering
        assert!(
            Datum::Bit {
                value: 10,
                width: 8
            } > Datum::Int(5)
        );
        assert!(
            Datum::Int(5)
                < Datum::Bit {
                    value: 10,
                    width: 8
                }
        );
    }

    #[test]
    fn test_datum_bit_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn compute_hash(d: &Datum) -> u64 {
            let mut h = DefaultHasher::new();
            d.hash(&mut h);
            h.finish()
        }

        // Bit(N) == Int(N) must hash identically for small values
        for n in [0u64, 1, 42, 255] {
            let bit_val = Datum::Bit { value: n, width: 8 };
            let int_val = Datum::Int(n as i64);
            assert_eq!(bit_val, int_val, "PartialEq failed for {n}");
            assert_eq!(
                compute_hash(&bit_val),
                compute_hash(&int_val),
                "Hash mismatch for Bit({n}) vs Int({n})"
            );
        }
    }

    #[test]
    fn test_datum_bit_hashmap_lookup() {
        use std::collections::HashMap;

        // Build with Bit, probe with Int
        let mut map: HashMap<Vec<Datum>, &str> = HashMap::new();
        map.insert(
            vec![Datum::Bit {
                value: 42,
                width: 8,
            }],
            "found",
        );
        assert_eq!(map.get(&vec![Datum::Int(42)]), Some(&"found"));

        // Build with Int, probe with Bit
        let mut map2: HashMap<Vec<Datum>, &str> = HashMap::new();
        map2.insert(vec![Datum::Int(7)], "found");
        assert_eq!(
            map2.get(&vec![Datum::Bit { value: 7, width: 8 }]),
            Some(&"found")
        );
    }

    #[test]
    fn test_format_decimal() {
        assert_eq!(format_decimal(0, 0), "0");
        assert_eq!(format_decimal(123, 0), "123");
        assert_eq!(format_decimal(-5, 0), "-5");
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(-12345, 2), "-123.45");
        assert_eq!(format_decimal(0, 3), "0.000");
        assert_eq!(format_decimal(1, 3), "0.001");
        assert_eq!(format_decimal(-1, 3), "-0.001");
        assert_eq!(format_decimal(1, 10), "0.0000000001");
    }

    #[test]
    fn test_datum_decimal_equality() {
        // Same scale
        assert_eq!(
            Datum::Decimal {
                value: 100,
                scale: 2
            },
            Datum::Decimal {
                value: 100,
                scale: 2
            }
        );
        // Different scale, same value: 1.00 == 1.0
        assert_eq!(
            Datum::Decimal {
                value: 100,
                scale: 2
            },
            Datum::Decimal {
                value: 10,
                scale: 1
            }
        );
        // Decimal == Int: 5.0 == 5
        assert_eq!(
            Datum::Decimal {
                value: 50,
                scale: 1
            },
            Datum::Int(5)
        );
        // Decimal != Int when fractional
        assert_ne!(
            Datum::Decimal {
                value: 51,
                scale: 1
            },
            Datum::Int(5)
        );
    }

    #[test]
    fn test_datum_decimal_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn compute_hash(d: &Datum) -> u64 {
            let mut h = DefaultHasher::new();
            d.hash(&mut h);
            h.finish()
        }

        // Decimal(100, 2) == Decimal(10, 1) must hash identically
        let a = Datum::Decimal {
            value: 100,
            scale: 2,
        };
        let b = Datum::Decimal {
            value: 10,
            scale: 1,
        };
        assert_eq!(a, b);
        assert_eq!(compute_hash(&a), compute_hash(&b));

        // Decimal(50, 1) == Int(5): both hash consistently
        let dec = Datum::Decimal {
            value: 50,
            scale: 1,
        };
        let int = Datum::Int(5);
        assert_eq!(dec, int);
        assert_eq!(compute_hash(&dec), compute_hash(&int));
    }

    #[test]
    fn test_datum_decimal_hashmap_lookup() {
        use std::collections::HashMap;

        // Build with Decimal, probe with Int
        let mut map: HashMap<Vec<Datum>, &str> = HashMap::new();
        map.insert(
            vec![Datum::Decimal {
                value: 420,
                scale: 1,
            }],
            "found",
        );
        assert_eq!(map.get(&vec![Datum::Int(42)]), Some(&"found"));

        // Build with Decimal at one scale, probe with another
        let mut map2: HashMap<Vec<Datum>, &str> = HashMap::new();
        map2.insert(
            vec![Datum::Decimal {
                value: 1000,
                scale: 3,
            }],
            "found",
        );
        assert_eq!(
            map2.get(&vec![Datum::Decimal {
                value: 10,
                scale: 1
            }]),
            Some(&"found")
        );
    }
}
