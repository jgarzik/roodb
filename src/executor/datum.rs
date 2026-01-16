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
    /// Floating point value (covers Float, Double)
    Float(f64),
    /// String value (covers Varchar, Text)
    String(String),
    /// Binary data (Blob)
    Bytes(Vec<u8>),
    /// Timestamp as unix milliseconds
    Timestamp(i64),
}

impl Datum {
    /// Check if this datum is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Get a numeric type tag for ordering different types
    fn type_tag(&self) -> u8 {
        match self {
            Datum::Null => 0,
            Datum::Bool(_) => 1,
            Datum::Int(_) => 2,
            Datum::Float(_) => 3,
            Datum::String(_) => 4,
            Datum::Bytes(_) => 5,
            Datum::Timestamp(_) => 6,
        }
    }

    /// Get the data type of this datum
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(DataType::Boolean),
            Datum::Int(_) => Some(DataType::BigInt),
            Datum::Float(_) => Some(DataType::Double),
            Datum::String(_) => Some(DataType::Text),
            Datum::Bytes(_) => Some(DataType::Blob),
            Datum::Timestamp(_) => Some(DataType::Timestamp),
        }
    }

    /// Convert to boolean, returns None if NULL or not convertible
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Datum::Bool(b) => Some(*b),
            Datum::Int(i) => Some(*i != 0),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to i64, returns None if NULL or not convertible
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Datum::Int(i) => Some(*i),
            Datum::Float(f) => Some(*f as i64),
            Datum::Bool(b) => Some(if *b { 1 } else { 0 }),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to f64, returns None if NULL or not convertible
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Datum::Float(f) => Some(*f),
            Datum::Int(i) => Some(*i as f64),
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

    /// Create a Datum from a Literal
    pub fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Null => Datum::Null,
            Literal::Boolean(b) => Datum::Bool(*b),
            Literal::Integer(i) => Datum::Int(*i),
            Literal::Float(f) => Datum::Float(*f),
            Literal::String(s) => Datum::String(s.clone()),
            Literal::Blob(b) => Datum::Bytes(b.clone()),
        }
    }

    /// Negate this datum (for unary minus)
    pub fn negate(&self) -> Option<Datum> {
        match self {
            Datum::Int(i) => Some(Datum::Int(-i)),
            Datum::Float(f) => Some(Datum::Float(-f)),
            Datum::Null => Some(Datum::Null),
            _ => None,
        }
    }

    /// Logical NOT
    pub fn not(&self) -> Option<Datum> {
        match self {
            Datum::Bool(b) => Some(Datum::Bool(!b)),
            Datum::Null => Some(Datum::Null),
            _ => None,
        }
    }

    /// Check if string matches pattern (SQL LIKE)
    pub fn like(&self, pattern: &Datum) -> Option<Datum> {
        match (self, pattern) {
            (Datum::String(s), Datum::String(p)) => {
                let matched = like_match(s, p);
                Some(Datum::Bool(matched))
            }
            (Datum::Null, _) | (_, Datum::Null) => Some(Datum::Null),
            _ => None,
        }
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
            (Datum::Float(a), Datum::Float(b)) => a.to_bits() == b.to_bits(),
            (Datum::String(a), Datum::String(b)) => a == b,
            (Datum::Bytes(a), Datum::Bytes(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            // Cross-type numeric comparisons
            (Datum::Int(a), Datum::Float(b)) | (Datum::Float(b), Datum::Int(a)) => {
                (*a as f64).to_bits() == b.to_bits()
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
            (Datum::Float(a), Datum::Float(b)) => a.total_cmp(b),
            (Datum::String(a), Datum::String(b)) => a.cmp(b),
            (Datum::Bytes(a), Datum::Bytes(b)) => a.cmp(b),
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a.cmp(b),

            // Cross-type numeric comparisons
            (Datum::Int(a), Datum::Float(b)) => (*a as f64).total_cmp(b),
            (Datum::Float(a), Datum::Int(b)) => a.total_cmp(&(*b as f64)),

            // Different types: use type tag for stable ordering
            _ => self.type_tag().cmp(&other.type_tag()),
        }
    }
}

impl Hash for Datum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Datum::Null => {}
            Datum::Bool(b) => b.hash(state),
            Datum::Int(i) => i.hash(state),
            Datum::Float(f) => f.to_bits().hash(state),
            Datum::String(s) => s.hash(state),
            Datum::Bytes(b) => b.hash(state),
            Datum::Timestamp(t) => t.hash(state),
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
        assert_eq!(Datum::Float(2.5).negate(), Some(Datum::Float(-2.5)));
    }

    #[test]
    fn test_datum_not() {
        assert_eq!(Datum::Bool(true).not(), Some(Datum::Bool(false)));
        assert_eq!(Datum::Bool(false).not(), Some(Datum::Bool(true)));
    }
}
