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
    /// Bit string value (width 1..64, value masked to width)
    Bit { value: u64, width: u8 },
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
            Datum::Bit { .. } => 7,
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
            Datum::Bit { width, .. } => Some(DataType::Bit(*width)),
            Datum::Timestamp(_) => Some(DataType::Timestamp),
        }
    }

    /// Convert to boolean, returns None if NULL or not convertible
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Datum::Bool(b) => Some(*b),
            Datum::Int(i) => Some(*i != 0),
            Datum::Bit { value, .. } => Some(*value != 0),
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
            Datum::Bit { value, .. } => Some(*value as i64),
            Datum::Null => None,
            _ => None,
        }
    }

    /// Convert to f64, returns None if NULL or not convertible
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Datum::Float(f) => Some(*f),
            Datum::Int(i) => Some(*i as f64),
            Datum::Bit { value, .. } => Some(*value as f64),
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
        }
    }

    /// Convert to display string for CAST and output formatting
    pub fn to_display_string(&self) -> String {
        match self {
            Datum::Null => "NULL".to_string(),
            Datum::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            Datum::Int(i) => i.to_string(),
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
            Literal::Placeholder(i) => {
                panic!("Unsubstituted placeholder ?{} in plan execution", i)
            }
        }
    }

    /// Negate this datum (for unary minus)
    pub fn negate(&self) -> Option<Datum> {
        match self {
            Datum::Int(i) => Some(Datum::Int(-i)),
            Datum::Float(f) => Some(Datum::Float(-f)),
            Datum::Null => Some(Datum::Null),
            Datum::Bit { .. } => None,
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
            (Datum::Bit { value: a, .. }, Datum::Bit { value: b, .. }) => a == b,
            // Cross-type: Bit/Int — BIT is unsigned, compare as u64
            (Datum::Bit { value, .. }, Datum::Int(i))
            | (Datum::Int(i), Datum::Bit { value, .. }) => *value == *i as u64,
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
            (Datum::Bit { value: a, .. }, Datum::Bit { value: b, .. }) => a.cmp(b),

            // Cross-type: Bit/Int — BIT is unsigned, compare as u64
            (Datum::Bit { value, .. }, Datum::Int(i)) => value.cmp(&(*i as u64)),
            (Datum::Int(i), Datum::Bit { value, .. }) => (*i as u64).cmp(value),

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
            Datum::Bit { value, .. } => {
                // Use same discriminant tag as Int/Float so Bit(N) == Int(N) hashes match.
                // Cross-type PartialEq compares as u64 (*value == *i as u64), which is
                // equivalent to comparing as i64 for values that fit. Hash via the same
                // i64→f64 path as Datum::Int to stay consistent.
                0u8.hash(state);
                (*value as i64 as f64).to_bits().hash(state);
            }
            other => {
                std::mem::discriminant(other).hash(state);
                match other {
                    Datum::Null => {}
                    Datum::Bool(b) => b.hash(state),
                    Datum::String(s) => s.hash(state),
                    Datum::Bytes(b) => b.hash(state),
                    Datum::Timestamp(t) => t.hash(state),
                    Datum::Int(_) | Datum::Float(_) | Datum::Bit { .. } => unreachable!(),
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
        assert_eq!(Datum::Float(2.5).negate(), Some(Datum::Float(-2.5)));
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
}
