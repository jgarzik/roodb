//! Values as they appear in storage keys.
//!
//! A key is compared as bytes, so a value must be written such that its
//! bytes sort as the values do. Every type is: integers big-endian with the
//! sign bit flipped, floats by their bits arranged to sort, decimals as
//! sign, magnitude and digits (independent of scale), strings and bytes
//! escaped and terminated so that no value's bytes are the start of
//! another's.
//!
//! Each value is one *part* of a key: a presence byte (NULL sorts before
//! every value) and the value's bytes. A part may be descending (every byte
//! inverted), and a string part goes through its column's collation, which
//! turns a string into the bytes it sorts by.
//!
//! A value looked up by key must first be put in the key column's type
//! (`for_lookup`): the key of the integer 3 is not that of the float 3.0.

use std::cmp::Ordering;

use crate::catalog::DataType;

use super::datum::Datum;

/// Turns a string into the bytes it sorts by.
pub trait Collation: Send + Sync {
    /// The collation's name, as SQL spells it.
    fn name(&self) -> &'static str;
    /// Append `text`'s sort key to `out`. Two strings the collation holds
    /// equal give the same bytes; otherwise the bytes order as the strings.
    fn sort_key(&self, text: &str, out: &mut Vec<u8>);
}

/// Strings sort by their bytes.
pub struct Binary;

impl Collation for Binary {
    fn name(&self) -> &'static str {
        "utf8mb4_bin"
    }

    fn sort_key(&self, text: &str, out: &mut Vec<u8>) {
        out.extend_from_slice(text.as_bytes());
    }
}

/// How one value is written into a key.
#[derive(Clone, Copy)]
pub struct KeyPart<'a> {
    /// Larger values first.
    pub descending: bool,
    /// Only the first so many characters (strings) or bytes of the value
    /// take part (`KEY (name(10))`).
    pub prefix: Option<usize>,
    /// What strings sort by.
    pub collation: &'a dyn Collation,
}

impl KeyPart<'_> {
    /// Ascending, whole value, binary collation.
    pub const PLAIN: KeyPart<'static> = KeyPart {
        descending: false,
        prefix: None,
        collation: &Binary,
    };
}

const ABSENT: u8 = 0x00;
const PRESENT: u8 = 0x01;

/// Append `datum` to `key` as one part.
pub fn encode_part(key: &mut Vec<u8>, datum: &Datum, part: &KeyPart<'_>) {
    let start = key.len();
    match datum {
        Datum::Null => key.push(ABSENT),
        _ => {
            key.push(PRESENT);
            encode_value(key, datum, part);
        }
    }
    if part.descending {
        for byte in &mut key[start..] {
            *byte = !*byte;
        }
    }
}

fn encode_value(key: &mut Vec<u8>, datum: &Datum, part: &KeyPart<'_>) {
    match datum {
        Datum::Null => {}
        Datum::Bool(value) => key.push(u8::from(*value)),
        Datum::Int(value) => key.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes()),
        Datum::UnsignedInt(value) => key.extend_from_slice(&value.to_be_bytes()),
        Datum::Timestamp(value) => {
            key.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes())
        }
        Datum::Bit { value, .. } => key.extend_from_slice(&value.to_be_bytes()),
        Datum::Float(value) => key.extend_from_slice(&float_bits(*value).to_be_bytes()),
        Datum::Decimal { value, scale } => encode_decimal(key, *value, *scale),
        Datum::String(text) => {
            let text = match part.prefix {
                Some(chars) => match text.char_indices().nth(chars) {
                    Some((end, _)) => &text[..end],
                    None => text.as_str(),
                },
                None => text.as_str(),
            };
            let mut sort_key = Vec::with_capacity(text.len());
            part.collation.sort_key(text, &mut sort_key);
            encode_bytes(key, &sort_key);
        }
        Datum::Bytes(bytes) | Datum::Geometry(bytes) => {
            let bytes = match part.prefix {
                Some(len) if len < bytes.len() => &bytes[..len],
                _ => bytes.as_slice(),
            };
            encode_bytes(key, bytes);
        }
        // JSON has no order of its own; its text gives it one.
        Datum::Json(value) => encode_bytes(key, value.to_string().as_bytes()),
    }
}

/// A float's bits, arranged so that they sort as the floats do. Negative
/// zero is zero; every NaN is the same, below everything.
fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        return 0;
    }
    let value = if value == 0.0 { 0.0 } else { value };
    let bits = value.to_bits();
    if bits >> 63 == 0 {
        bits | (1 << 63)
    } else {
        !bits
    }
}

/// Bytes, such that no encoding is the start of another and the encodings
/// sort as the bytes do: a zero byte is written `00 FF`, and the end `00 01`.
fn encode_bytes(key: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        if byte == 0x00 {
            key.extend_from_slice(&[0x00, 0xFF]);
        } else {
            key.push(byte);
        }
    }
    key.extend_from_slice(&[0x00, 0x01]);
}

/// The string that a key part written with `KeyPart::PLAIN` holds, if the
/// part is a present string in the binary collation. The reverse of
/// `encode_part` for that case.
pub fn decode_string(part: &[u8]) -> Option<String> {
    let (&presence, mut rest) = part.split_first()?;
    if presence != PRESENT {
        return None;
    }
    let mut bytes = Vec::with_capacity(rest.len());
    loop {
        let (&byte, after) = rest.split_first()?;
        if byte != 0x00 {
            bytes.push(byte);
            rest = after;
            continue;
        }
        let (&next, after) = after.split_first()?;
        match next {
            0x01 => return String::from_utf8(bytes).ok(),
            0xFF => {
                bytes.push(0x00);
                rest = after;
            }
            _ => return None,
        }
    }
}

/// A decimal, whatever its scale: 1.5 and 1.50 give the same bytes.
///
/// A sign byte; then, for a non-zero value, the number of digits before the
/// decimal point (which may be negative: 0.001 has -2) and the digits
/// without leading or trailing zeros, each as digit + 1, ended by a zero
/// byte. For a negative value everything after the sign byte is inverted,
/// so that the larger magnitude sorts first.
fn encode_decimal(key: &mut Vec<u8>, value: i128, scale: u8) {
    const NEGATIVE: u8 = 0x00;
    const ZERO: u8 = 0x01;
    const POSITIVE: u8 = 0x02;
    if value == 0 {
        key.push(ZERO);
        return;
    }
    let digits = value.unsigned_abs().to_string();
    let before_point = digits.len() as i32 - i32::from(scale);
    let digits = digits.trim_end_matches('0');
    key.push(if value < 0 { NEGATIVE } else { POSITIVE });
    let start = key.len();
    key.extend_from_slice(&((before_point as u32) ^ (1 << 31)).to_be_bytes());
    key.extend(digits.bytes().map(|digit| digit - b'0' + 1));
    key.push(0x00);
    if value < 0 {
        for byte in &mut key[start..] {
            *byte = !*byte;
        }
    }
}

/// Which way a value that the key column cannot hold is moved to one it can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Toward {
    /// The nearest value of the column's type that is not below it.
    Above,
    /// The nearest value of the column's type that is not above it.
    Below,
}

/// A looked-up value in the key column's type.
#[derive(Debug, Clone, PartialEq)]
pub enum Lookup {
    /// The column can hold exactly this value.
    Exact(Datum),
    /// The column cannot hold the value; this is the nearest it can, on the
    /// side asked for. No row has the value itself.
    Nearest(Datum),
    /// The value lies beyond every value the column can hold, on this side.
    Beyond(Ordering),
}

/// Put `datum` in the type of a key column of type `column`, to look it up
/// or to bound a range. `toward` says which neighbour to take where the
/// column cannot hold the value (2.5 in an integer column: 3 above, 2
/// below).
pub fn for_lookup(datum: Datum, column: &DataType, toward: Toward) -> Lookup {
    let integer = matches!(
        column,
        DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
    );
    let unsigned = matches!(column, DataType::BigIntUnsigned);
    if integer || unsigned {
        return match whole_number(&datum, toward) {
            Some((whole, exact)) => {
                let in_type = if unsigned {
                    u64::try_from(whole)
                        .map(Datum::UnsignedInt)
                        .map_err(|_| whole < 0)
                } else {
                    i64::try_from(whole).map(Datum::Int).map_err(|_| whole < 0)
                };
                match (in_type, exact) {
                    (Ok(datum), true) => Lookup::Exact(datum),
                    (Ok(datum), false) => Lookup::Nearest(datum),
                    (Err(true), _) => Lookup::Beyond(Ordering::Less),
                    (Err(false), _) => Lookup::Beyond(Ordering::Greater),
                }
            }
            None => convert(datum, column),
        };
    }
    convert(datum, column)
}

/// The value to look a single row up by, in the key column's type; `None`
/// if the column cannot hold the value, so that no row has it. Without a
/// type (executors run without a catalog) the value is taken as it is.
pub fn for_point(datum: Datum, column: Option<&DataType>) -> Option<Datum> {
    match column {
        None => Some(datum),
        Some(column) => match for_lookup(datum, column, Toward::Above) {
            Lookup::Exact(datum) => Some(datum),
            Lookup::Nearest(_) | Lookup::Beyond(_) => None,
        },
    }
}

/// One end of a range of keys.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    /// From or to this value, itself included or not.
    At { value: Datum, inclusive: bool },
    /// No limit on this side.
    Open,
    /// Nothing lies within the range on this side: the range is empty.
    Nothing,
}

/// The lower (`lower`) or upper end of a range, given as `value` and
/// whether the range includes it, in the key column's type. A lower bound
/// of `> 2.5` on an integer column is `>= 3`.
pub fn for_bound(value: Datum, inclusive: bool, column: Option<&DataType>, lower: bool) -> Bound {
    let Some(column) = column else {
        return Bound::At { value, inclusive };
    };
    let toward = if lower { Toward::Above } else { Toward::Below };
    match for_lookup(value, column, toward) {
        Lookup::Exact(value) => Bound::At { value, inclusive },
        // The nearest value on the range's side is in the range.
        Lookup::Nearest(value) => Bound::At {
            value,
            inclusive: true,
        },
        Lookup::Beyond(side) => {
            // Beyond the column's values on the side the range extends
            // from: the range covers nothing. On the other: no limit.
            let outside = if lower {
                Ordering::Greater
            } else {
                Ordering::Less
            };
            if side == outside {
                Bound::Nothing
            } else {
                Bound::Open
            }
        }
    }
}

/// The column's own conversion, as an insert would apply it. A value it
/// cannot convert is left as it is: it then matches no stored key.
fn convert(datum: Datum, column: &DataType) -> Lookup {
    match super::eval::coerce_to_column_type(datum.clone(), column) {
        Ok(converted) => Lookup::Exact(converted),
        Err(_) => Lookup::Exact(datum),
    }
}

/// A number as a whole number, moved `toward` if it has a fraction, and
/// whether it was whole to begin with. `None` for what is not a number.
fn whole_number(datum: &Datum, toward: Toward) -> Option<(i128, bool)> {
    match datum {
        Datum::Int(value) => Some((i128::from(*value), true)),
        Datum::UnsignedInt(value) => Some((i128::from(*value), true)),
        Datum::Bool(value) => Some((i128::from(*value), true)),
        Datum::Float(value) => {
            if value.is_nan() {
                return None;
            }
            let moved = match toward {
                Toward::Above => value.ceil(),
                Toward::Below => value.floor(),
            };
            // Beyond what any integer column holds either way: the caller
            // finds it out of range from the sign and size.
            let whole = if moved >= 1.0e38 {
                i128::MAX
            } else if moved <= -1.0e38 {
                i128::MIN
            } else {
                moved as i128
            };
            Some((whole, moved == *value))
        }
        Datum::Decimal { value, scale } => {
            let unit = 10i128.checked_pow(u32::from(*scale))?;
            let below = value.div_euclid(unit);
            let exact = value.rem_euclid(unit) == 0;
            let whole = match toward {
                Toward::Above if !exact => below + 1,
                _ => below,
            };
            Some((whole, exact))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(datum: &Datum) -> Vec<u8> {
        let mut key = Vec::new();
        encode_part(&mut key, datum, &KeyPart::PLAIN);
        key
    }

    /// The keys of `values`, which are given in ascending order, ascend;
    /// equal values give equal keys.
    fn assert_ascending(values: &[Datum]) {
        for pair in values.windows(2) {
            let (low, high) = (key_of(&pair[0]), key_of(&pair[1]));
            assert!(low < high, "{:?} is not below {:?}", pair[0], pair[1]);
        }
        let descending = KeyPart {
            descending: true,
            ..KeyPart::PLAIN
        };
        for pair in values.windows(2) {
            let (mut low, mut high) = (Vec::new(), Vec::new());
            encode_part(&mut low, &pair[0], &descending);
            encode_part(&mut high, &pair[1], &descending);
            assert!(low > high, "descending: {:?} and {:?}", pair[0], pair[1]);
        }
    }

    fn decimal(text: &str) -> Datum {
        let negative = text.starts_with('-');
        let text = text.trim_start_matches('-');
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        let value: i128 = format!("{whole}{fraction}").parse().unwrap();
        Datum::Decimal {
            value: if negative { -value } else { value },
            scale: fraction.len() as u8,
        }
    }

    #[test]
    fn integers_sort() {
        assert_ascending(&[
            Datum::Null,
            Datum::Int(i64::MIN),
            Datum::Int(-65_536),
            Datum::Int(-1),
            Datum::Int(0),
            Datum::Int(1),
            Datum::Int(255),
            Datum::Int(256),
            Datum::Int(i64::MAX),
        ]);
        assert_ascending(&[
            Datum::Null,
            Datum::UnsignedInt(0),
            Datum::UnsignedInt(1 << 40),
            Datum::UnsignedInt(u64::MAX),
        ]);
        assert_ascending(&[
            Datum::Timestamp(-1),
            Datum::Timestamp(0),
            Datum::Timestamp(1_700_000_000_000),
        ]);
    }

    #[test]
    fn floats_sort() {
        assert_ascending(&[
            Datum::Null,
            Datum::Float(f64::NEG_INFINITY),
            Datum::Float(-1.0e300),
            Datum::Float(-250.5),
            Datum::Float(-2.5),
            Datum::Float(-1.0),
            Datum::Float(-1.0e-300),
            Datum::Float(0.0),
            Datum::Float(1.0e-300),
            Datum::Float(0.5),
            Datum::Float(3.0),
            Datum::Float(100.25),
            Datum::Float(f64::INFINITY),
        ]);
        assert_eq!(key_of(&Datum::Float(-0.0)), key_of(&Datum::Float(0.0)));
    }

    #[test]
    fn decimals_sort_whatever_their_scale() {
        assert_ascending(&[
            Datum::Null,
            decimal("-1000000.10"),
            decimal("-1000.50"),
            decimal("-999.999"),
            decimal("-10.25"),
            decimal("-10.2"),
            decimal("-1"),
            decimal("-0.5"),
            decimal("-0.01"),
            decimal("-0.001"),
            decimal("0"),
            decimal("0.001"),
            decimal("0.01"),
            decimal("0.099"),
            decimal("0.1"),
            decimal("0.99"),
            decimal("1"),
            decimal("1.5"),
            decimal("7"),
            decimal("9.99"),
            decimal("10"),
            decimal("10.01"),
            decimal("65536.00"),
            decimal("1000000.10"),
        ]);
        assert_eq!(key_of(&decimal("1.5")), key_of(&decimal("1.500")));
        assert_eq!(key_of(&decimal("0")), key_of(&decimal("0.00")));
        assert_eq!(key_of(&decimal("-20")), key_of(&decimal("-20.0")));
    }

    #[test]
    fn strings_and_bytes_sort_and_none_starts_another() {
        let texts = [
            "", "\0", "\0\0", "\0a", "a", "a\0", "a\0b", "aa", "ab", "b", "é",
        ];
        let values: Vec<Datum> = texts.iter().map(|t| Datum::String(t.to_string())).collect();
        assert_ascending(&values);
        for a in &values {
            for b in &values {
                if a != b {
                    assert!(
                        !key_of(b).starts_with(&key_of(a)),
                        "the key of {a:?} starts the key of {b:?}"
                    );
                }
            }
        }
        assert_ascending(&[
            Datum::Bytes(vec![]),
            Datum::Bytes(vec![0]),
            Datum::Bytes(vec![0, 0xFF]),
            Datum::Bytes(vec![1]),
            Datum::Bytes(vec![0xFF]),
            Datum::Bytes(vec![0xFF, 0]),
        ]);
    }

    #[test]
    fn a_prefix_takes_the_first_characters() {
        let part = KeyPart {
            prefix: Some(2),
            ..KeyPart::PLAIN
        };
        let mut long = Vec::new();
        encode_part(&mut long, &Datum::String("héllo".into()), &part);
        let mut short = Vec::new();
        encode_part(&mut short, &Datum::String("hé".into()), &part);
        assert_eq!(long, short);
    }

    #[test]
    fn several_parts_sort_part_by_part() {
        let key = |a: &str, b: i64| {
            let mut key = Vec::new();
            encode_part(&mut key, &Datum::String(a.into()), &KeyPart::PLAIN);
            encode_part(&mut key, &Datum::Int(b), &KeyPart::PLAIN);
            key
        };
        // "a" with a large number sorts before "ab" with a small one.
        assert!(key("a", 1_000_000) < key("ab", -5));
        assert!(key("a", -5) < key("a", 3));
    }

    #[test]
    fn a_looked_up_value_takes_the_columns_type() {
        use Toward::{Above, Below};
        let int = DataType::Int;
        assert_eq!(
            for_lookup(Datum::Float(3.0), &int, Above),
            Lookup::Exact(Datum::Int(3))
        );
        assert_eq!(
            for_lookup(Datum::Float(2.5), &int, Above),
            Lookup::Nearest(Datum::Int(3))
        );
        assert_eq!(
            for_lookup(Datum::Float(2.5), &int, Below),
            Lookup::Nearest(Datum::Int(2))
        );
        assert_eq!(
            for_lookup(decimal("-2.5"), &int, Above),
            Lookup::Nearest(Datum::Int(-2))
        );
        assert_eq!(
            for_lookup(decimal("-2.5"), &int, Below),
            Lookup::Nearest(Datum::Int(-3))
        );
        assert_eq!(
            for_lookup(decimal("7.00"), &int, Below),
            Lookup::Exact(Datum::Int(7))
        );
        assert_eq!(
            for_lookup(Datum::UnsignedInt(u64::MAX), &DataType::BigInt, Above),
            Lookup::Beyond(Ordering::Greater)
        );
        assert_eq!(
            for_lookup(Datum::Int(-1), &DataType::BigIntUnsigned, Above),
            Lookup::Beyond(Ordering::Less)
        );
        assert_eq!(
            for_lookup(Datum::Int(5), &DataType::BigIntUnsigned, Above),
            Lookup::Exact(Datum::UnsignedInt(5))
        );
        assert_eq!(
            for_lookup(Datum::Float(1.0e300), &int, Below),
            Lookup::Beyond(Ordering::Greater)
        );
        // An integer looked up in a float column is that float.
        assert_eq!(
            for_lookup(Datum::Int(-3), &DataType::Double, Above),
            Lookup::Exact(Datum::Float(-3.0))
        );
    }
}
