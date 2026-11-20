//! Mapping between RooDB values and TDS wire types.
//!
//! Each result column is sent with one TDS type chosen from its RooDB
//! type. Every type used is the nullable, variable-length form, so a row
//! is self-describing. Character data is always UTF-16 (NVARCHAR), which
//! every driver handles without relying on a code page.
//!
//! The executor's values do not always have exactly the type the planner
//! declared for the column, so values are converted to the column's wire
//! type where that is lossless and refused otherwise. A row is never sent
//! with bytes that disagree with the column metadata.

use crate::catalog::DataType;
use crate::executor::datum::{format_decimal, Datum};
use crate::planner::logical::OutputColumn;

use super::login::encode_ucs2;
use super::token::{put_b_varchar, DEFAULT_COLLATION, TOKEN_COLMETADATA, TOKEN_ROW};

// TDS type tokens
const TYPE_INTN: u8 = 0x26;
const TYPE_DATETIME2N: u8 = 0x2A;
const TYPE_BITN: u8 = 0x68;
const TYPE_DECIMALN: u8 = 0x6A;
const TYPE_FLTN: u8 = 0x6D;
const TYPE_BIGVARBINARY: u8 = 0xA5;
const TYPE_BIGBINARY: u8 = 0xAD;
const TYPE_NVARCHAR: u8 = 0xE7;

/// Length marker for the (max) types, which use partially length-prefixed
/// (PLP) values
const MAX_TYPE_LENGTH: u16 = 0xFFFF;
/// Largest value, in bytes, a non-max NVARCHAR can carry
const NVARCHAR_MAX_BYTES: usize = 8000;
/// Longest VARCHAR, in characters, sent as a non-max NVARCHAR
const NVARCHAR_MAX_CHARS: u32 = 4000;
/// PLP NULL
const PLP_NULL: u64 = u64::MAX;
/// Highest precision of a TDS decimal
const MAX_DECIMAL_PRECISION: u8 = 38;
/// Fractional-second digits sent for timestamps (milliseconds)
const TIMESTAMP_SCALE: u8 = 3;
/// Days from 0001-01-01 to 1970-01-01
const UNIX_EPOCH_DAY: i64 = 719_162;
/// Days from 0001-01-01 to 9999-12-31
const MAX_DAY: i64 = 3_652_058;
const MS_PER_DAY: i64 = 86_400_000;

/// The TDS type a column is sent as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireType {
    /// BITN
    Bit,
    /// INTN of 2, 4 or 8 bytes
    Int(u8),
    /// FLTN of 4 or 8 bytes
    Float(u8),
    /// NVARCHAR(4000)
    NVarchar,
    /// NVARCHAR(max)
    NVarcharMax,
    /// VARBINARY(max)
    VarBinaryMax,
    /// BINARY(n)
    Binary(u16),
    /// DECIMAL(precision, scale)
    Decimal { precision: u8, scale: u8 },
    /// DATETIME2(3)
    DateTime2,
}

/// A value that cannot be sent as its column's wire type.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValueError(String);

/// Choose the wire type for a RooDB column type.
pub fn wire_type(data_type: &DataType) -> WireType {
    match data_type {
        DataType::Boolean | DataType::Bit(1) => WireType::Bit,
        // TDS TINYINT is unsigned; RooDB's is signed, so it travels as a
        // 2-byte integer.
        DataType::TinyInt | DataType::SmallInt => WireType::Int(2),
        DataType::Int => WireType::Int(4),
        DataType::BigInt => WireType::Int(8),
        // The unsigned 64-bit range needs 20 digits.
        DataType::BigIntUnsigned => WireType::Decimal {
            precision: 20,
            scale: 0,
        },
        DataType::Float => WireType::Float(4),
        DataType::Double => WireType::Float(8),
        DataType::Varchar(n) if *n <= NVARCHAR_MAX_CHARS => WireType::NVarchar,
        DataType::Varchar(_) | DataType::Text | DataType::Json => WireType::NVarcharMax,
        DataType::Blob | DataType::Geometry => WireType::VarBinaryMax,
        DataType::Bit(_) => WireType::Binary(8),
        DataType::Timestamp => WireType::DateTime2,
        DataType::Decimal { precision, scale }
            if *precision <= MAX_DECIMAL_PRECISION && scale <= precision =>
        {
            WireType::Decimal {
                precision: (*precision).max(1),
                scale: *scale,
            }
        }
        // Wider than TDS can represent: send the digits as text.
        DataType::Decimal { .. } => WireType::NVarchar,
    }
}

/// Bytes a decimal of this precision occupies on the wire, sign included.
fn decimal_wire_len(precision: u8) -> u8 {
    match precision {
        0..=9 => 5,
        10..=19 => 9,
        20..=28 => 13,
        _ => 17,
    }
}

/// Append the TYPE_INFO for a wire type.
fn put_type_info(buf: &mut Vec<u8>, wire: WireType) {
    match wire {
        WireType::Bit => buf.extend_from_slice(&[TYPE_BITN, 1]),
        WireType::Int(size) => buf.extend_from_slice(&[TYPE_INTN, size]),
        WireType::Float(size) => buf.extend_from_slice(&[TYPE_FLTN, size]),
        WireType::NVarchar => {
            buf.push(TYPE_NVARCHAR);
            buf.extend_from_slice(&(NVARCHAR_MAX_BYTES as u16).to_le_bytes());
            buf.extend_from_slice(&DEFAULT_COLLATION);
        }
        WireType::NVarcharMax => {
            buf.push(TYPE_NVARCHAR);
            buf.extend_from_slice(&MAX_TYPE_LENGTH.to_le_bytes());
            buf.extend_from_slice(&DEFAULT_COLLATION);
        }
        WireType::VarBinaryMax => {
            buf.push(TYPE_BIGVARBINARY);
            buf.extend_from_slice(&MAX_TYPE_LENGTH.to_le_bytes());
        }
        WireType::Binary(len) => {
            buf.push(TYPE_BIGBINARY);
            buf.extend_from_slice(&len.to_le_bytes());
        }
        WireType::Decimal { precision, scale } => {
            buf.extend_from_slice(&[TYPE_DECIMALN, decimal_wire_len(precision), precision, scale]);
        }
        WireType::DateTime2 => buf.extend_from_slice(&[TYPE_DATETIME2N, TIMESTAMP_SCALE]),
    }
}

/// Append a COLMETADATA token describing `columns` and return the wire
/// type chosen for each.
pub fn put_colmetadata(buf: &mut Vec<u8>, columns: &[OutputColumn]) -> Vec<WireType> {
    // The column count is 16 bits; result sets are far narrower.
    let count = columns.len().min(u16::MAX as usize - 1);
    buf.push(TOKEN_COLMETADATA);
    buf.extend_from_slice(&(count as u16).to_le_bytes());

    let mut wire_types = Vec::with_capacity(count);
    for column in &columns[..count] {
        let wire = wire_type(&column.data_type);
        buf.extend_from_slice(&0u32.to_le_bytes()); // user type
        buf.extend_from_slice(&1u16.to_le_bytes()); // flags: nullable
        put_type_info(buf, wire);
        put_b_varchar(buf, &column.name);
        wire_types.push(wire);
    }
    wire_types
}

/// Append a ROW token for `values`, one per wire type.
///
/// On error nothing is appended, so the stream stays consistent with the
/// column metadata.
pub fn put_row(
    buf: &mut Vec<u8>,
    wire_types: &[WireType],
    values: &[Datum],
) -> Result<(), ValueError> {
    let start = buf.len();
    buf.push(TOKEN_ROW);
    for (index, wire) in wire_types.iter().enumerate() {
        let value = values.get(index).unwrap_or(&Datum::Null);
        if let Err(e) = put_value(buf, *wire, value) {
            buf.truncate(start);
            return Err(ValueError(format!("column {}: {}", index + 1, e.0)));
        }
    }
    Ok(())
}

fn mismatch(value: &Datum, wire: WireType) -> ValueError {
    let kind = match value {
        Datum::Null => "NULL",
        Datum::Bool(_) => "boolean",
        Datum::Int(_) | Datum::UnsignedInt(_) => "integer",
        Datum::Float(_) => "float",
        Datum::String(_) => "string",
        Datum::Bytes(_) => "binary",
        Datum::Bit { .. } => "bit",
        Datum::Timestamp(_) => "timestamp",
        Datum::Decimal { .. } => "decimal",
        Datum::Geometry(_) => "geometry",
        Datum::Json(_) => "json",
    };
    ValueError(format!("{} value cannot be sent as {:?}", kind, wire))
}

/// Append one value in the encoding of its wire type.
fn put_value(buf: &mut Vec<u8>, wire: WireType, value: &Datum) -> Result<(), ValueError> {
    if value.is_null() {
        match wire {
            WireType::NVarchar | WireType::Binary(_) => {
                buf.extend_from_slice(&u16::MAX.to_le_bytes())
            }
            WireType::NVarcharMax | WireType::VarBinaryMax => {
                buf.extend_from_slice(&PLP_NULL.to_le_bytes())
            }
            _ => buf.push(0),
        }
        return Ok(());
    }

    match wire {
        WireType::Bit => {
            let bit = match value {
                Datum::Bool(b) => *b,
                Datum::Bit { value: v, .. } => *v != 0,
                Datum::Int(i) => *i != 0,
                Datum::UnsignedInt(u) => *u != 0,
                _ => return Err(mismatch(value, wire)),
            };
            buf.extend_from_slice(&[1, bit as u8]);
        }
        WireType::Int(size) => {
            let n = integer_value(value).ok_or_else(|| mismatch(value, wire))?;
            let bits = u32::from(size) * 8;
            let (min, max) = (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1);
            if !(min..=max).contains(&n) {
                return Err(ValueError(format!(
                    "{} is out of range for a {}-byte integer column",
                    n, size
                )));
            }
            buf.push(size);
            buf.extend_from_slice(&n.to_le_bytes()[..size as usize]);
        }
        WireType::Float(size) => {
            let f = float_value(value).ok_or_else(|| mismatch(value, wire))?;
            buf.push(size);
            if size == 4 {
                buf.extend_from_slice(&(f as f32).to_le_bytes());
            } else {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        WireType::NVarchar => {
            let text = encode_ucs2(&text_value(value));
            if text.len() > NVARCHAR_MAX_BYTES {
                return Err(ValueError(format!(
                    "string of {} characters is too long for its column type",
                    text.len() / 2
                )));
            }
            buf.extend_from_slice(&(text.len() as u16).to_le_bytes());
            buf.extend_from_slice(&text);
        }
        WireType::NVarcharMax => put_plp(buf, &encode_ucs2(&text_value(value))),
        WireType::VarBinaryMax => match value {
            Datum::Bytes(b) | Datum::Geometry(b) => put_plp(buf, b),
            Datum::String(s) => put_plp(buf, s.as_bytes()),
            Datum::Bit { value: v, .. } => put_plp(buf, &v.to_be_bytes()),
            _ => return Err(mismatch(value, wire)),
        },
        WireType::Binary(len) => {
            let bytes: Vec<u8> = match value {
                Datum::Bit { value: v, .. } => v.to_be_bytes().to_vec(),
                Datum::Int(i) => i.to_be_bytes().to_vec(),
                Datum::UnsignedInt(u) => u.to_be_bytes().to_vec(),
                Datum::Bytes(b) if b.len() <= len as usize => b.clone(),
                _ => return Err(mismatch(value, wire)),
            };
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
        WireType::Decimal { precision, scale } => {
            let unscaled = decimal_value(value, scale).ok_or_else(|| mismatch(value, wire))?;
            let magnitude = unscaled.unsigned_abs();
            if magnitude >= 10u128.pow(u32::from(precision)) {
                return Err(ValueError(format!(
                    "value is out of range for DECIMAL({}, {})",
                    precision, scale
                )));
            }
            let len = decimal_wire_len(precision);
            buf.push(len);
            buf.push((unscaled >= 0) as u8); // 1 = non-negative
            buf.extend_from_slice(&magnitude.to_le_bytes()[..len as usize - 1]);
        }
        WireType::DateTime2 => {
            let Datum::Timestamp(ms) = value else {
                return Err(mismatch(value, wire));
            };
            let day = ms.div_euclid(MS_PER_DAY) + UNIX_EPOCH_DAY;
            let ms_of_day = ms.rem_euclid(MS_PER_DAY) as u32;
            if !(0..=MAX_DAY).contains(&day) {
                return Err(ValueError(
                    "timestamp is outside the range 0001-01-01 to 9999-12-31".to_string(),
                ));
            }
            // Scale 3: 4-byte time in milliseconds, then a 3-byte day number
            buf.push(7);
            buf.extend_from_slice(&ms_of_day.to_le_bytes());
            buf.extend_from_slice(&(day as u32).to_le_bytes()[..3]);
        }
    }
    Ok(())
}

/// Append a value in partially-length-prefixed form: total length, one
/// chunk, terminator.
fn put_plp(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
    // Chunk lengths are 32-bit.
    for chunk in data.chunks(u32::MAX as usize) {
        buf.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        buf.extend_from_slice(chunk);
    }
    buf.extend_from_slice(&0u32.to_le_bytes());
}

/// The value as an exact integer, if it is one.
fn integer_value(value: &Datum) -> Option<i128> {
    match value {
        Datum::Int(i) => Some(i128::from(*i)),
        Datum::UnsignedInt(u) => Some(i128::from(*u)),
        Datum::Bool(b) => Some(i128::from(*b)),
        Datum::Bit { value, .. } => Some(i128::from(*value)),
        Datum::Float(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e18 => Some(*f as i128),
        Datum::Decimal { value, scale } => {
            let divisor = 10i128.checked_pow(u32::from(*scale))?;
            (value % divisor == 0).then_some(value / divisor)
        }
        Datum::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// The value as a float, if it is numeric.
fn float_value(value: &Datum) -> Option<f64> {
    match value {
        Datum::Float(f) => Some(*f),
        Datum::Int(i) => Some(*i as f64),
        Datum::UnsignedInt(u) => Some(*u as f64),
        Datum::Bool(b) => Some(f64::from(u8::from(*b))),
        Datum::Decimal { value, scale } => format_decimal(*value, *scale).parse().ok(),
        Datum::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// The value as an unscaled decimal with `scale` fractional digits, if it
/// can be represented exactly.
fn decimal_value(value: &Datum, scale: u8) -> Option<i128> {
    let rescale = |unscaled: i128, from: u8| -> Option<i128> {
        if from <= scale {
            unscaled.checked_mul(10i128.checked_pow(u32::from(scale - from))?)
        } else {
            let divisor = 10i128.checked_pow(u32::from(from - scale))?;
            (unscaled % divisor == 0).then_some(unscaled / divisor)
        }
    };
    match value {
        Datum::Decimal { value, scale: from } => rescale(*value, *from),
        Datum::Int(i) => rescale(i128::from(*i), 0),
        Datum::UnsignedInt(u) => rescale(i128::from(*u), 0),
        Datum::Bool(b) => rescale(i128::from(*b), 0),
        Datum::String(s) => parse_decimal(s.trim()).and_then(|(v, from)| rescale(v, from)),
        Datum::Float(f) if f.is_finite() => {
            parse_decimal(&format!("{}", f)).and_then(|(v, from)| rescale(v, from))
        }
        _ => None,
    }
}

/// Parse plain decimal text ("-12.340") into (unscaled value, scale).
fn parse_decimal(text: &str) -> Option<(i128, u8)> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let scale = u8::try_from(fraction.len()).ok()?;
    let mut unscaled: i128 = 0;
    for c in whole.chars().chain(fraction.chars()) {
        let digit = c.to_digit(10)?;
        unscaled = unscaled.checked_mul(10)?.checked_add(i128::from(digit))?;
    }
    Some((if negative { -unscaled } else { unscaled }, scale))
}

/// The value as the text a client should see in a character column.
fn text_value(value: &Datum) -> String {
    match value {
        Datum::String(s) => s.clone(),
        Datum::Timestamp(ms) => format_timestamp(*ms),
        Datum::Bytes(b) | Datum::Geometry(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::Float(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 => {
            format!("{}", *f as i64)
        }
        other => other.to_display_string(),
    }
}

/// Format unix milliseconds as "YYYY-MM-DD HH:MM:SS[.mmm]".
fn format_timestamp(ms: i64) -> String {
    let days = ms.div_euclid(MS_PER_DAY);
    let ms_of_day = ms.rem_euclid(MS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let (h, m, s, frac) = (
        ms_of_day / 3_600_000,
        ms_of_day / 60_000 % 60,
        ms_of_day / 1000 % 60,
        ms_of_day % 1000,
    );
    if frac == 0 {
        format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02}.{frac:03}")
    }
}

/// Convert days since 1970-01-01 to (year, month, day) in the proleptic
/// Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(wire: WireType, value: Datum) -> Result<Vec<u8>, ValueError> {
        let mut buf = Vec::new();
        put_value(&mut buf, wire, &value).map(|()| buf)
    }

    #[test]
    fn every_data_type_has_a_wire_type() {
        use DataType::*;
        let cases = [
            (Boolean, WireType::Bit),
            (TinyInt, WireType::Int(2)),
            (SmallInt, WireType::Int(2)),
            (Int, WireType::Int(4)),
            (BigInt, WireType::Int(8)),
            (
                BigIntUnsigned,
                WireType::Decimal {
                    precision: 20,
                    scale: 0,
                },
            ),
            (Float, WireType::Float(4)),
            (Double, WireType::Float(8)),
            (Varchar(255), WireType::NVarchar),
            (Varchar(4000), WireType::NVarchar),
            (Varchar(4001), WireType::NVarcharMax),
            (Text, WireType::NVarcharMax),
            (Json, WireType::NVarcharMax),
            (Blob, WireType::VarBinaryMax),
            (Geometry, WireType::VarBinaryMax),
            (Bit(1), WireType::Bit),
            (Bit(64), WireType::Binary(8)),
            (Timestamp, WireType::DateTime2),
            (
                Decimal {
                    precision: 10,
                    scale: 2,
                },
                WireType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                Decimal {
                    precision: 65,
                    scale: 30,
                },
                WireType::NVarchar,
            ),
        ];
        for (data_type, expected) in cases {
            assert_eq!(wire_type(&data_type), expected, "{:?}", data_type);
        }
    }

    #[test]
    fn nulls_use_each_type_s_null_form() {
        assert_eq!(encode(WireType::Int(4), Datum::Null).unwrap(), [0]);
        assert_eq!(encode(WireType::Bit, Datum::Null).unwrap(), [0]);
        assert_eq!(encode(WireType::DateTime2, Datum::Null).unwrap(), [0]);
        assert_eq!(
            encode(WireType::NVarchar, Datum::Null).unwrap(),
            [0xFF, 0xFF]
        );
        assert_eq!(
            encode(WireType::Binary(8), Datum::Null).unwrap(),
            [0xFF, 0xFF]
        );
        assert_eq!(
            encode(WireType::NVarcharMax, Datum::Null).unwrap(),
            [0xFF; 8]
        );
        assert_eq!(
            encode(WireType::VarBinaryMax, Datum::Null).unwrap(),
            [0xFF; 8]
        );
    }

    #[test]
    fn integers() {
        assert_eq!(
            encode(WireType::Int(4), Datum::Int(-2)).unwrap(),
            [4, 0xFE, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            encode(WireType::Int(8), Datum::Int(i64::MIN)).unwrap()[1..],
            i64::MIN.to_le_bytes()
        );
        assert_eq!(
            encode(WireType::Int(2), Datum::Bool(true)).unwrap(),
            [2, 1, 0]
        );
        // Values that are integers in another representation are accepted
        assert_eq!(
            encode(WireType::Int(2), Datum::Float(7.0)).unwrap(),
            [2, 7, 0]
        );
        assert_eq!(
            encode(WireType::Int(2), Datum::String(" 12 ".into())).unwrap(),
            [2, 12, 0]
        );
        assert_eq!(
            encode(
                WireType::Int(2),
                Datum::Decimal {
                    value: 500,
                    scale: 2
                }
            )
            .unwrap(),
            [2, 5, 0]
        );
        // Out of range or not an integer: refused, never truncated
        assert!(encode(WireType::Int(2), Datum::Int(40_000)).is_err());
        assert!(encode(WireType::Int(8), Datum::UnsignedInt(u64::MAX)).is_err());
        assert!(encode(WireType::Int(4), Datum::Float(1.5)).is_err());
        assert!(encode(WireType::Int(4), Datum::String("abc".into())).is_err());
        assert!(encode(WireType::Int(4), Datum::Bytes(vec![1])).is_err());
    }

    #[test]
    fn floats_and_bits() {
        assert_eq!(
            encode(WireType::Float(8), Datum::Float(1.5)).unwrap()[1..],
            1.5f64.to_le_bytes()
        );
        assert_eq!(
            encode(WireType::Float(4), Datum::Int(2)).unwrap()[1..],
            2.0f32.to_le_bytes()
        );
        assert!(encode(WireType::Float(8), Datum::Bytes(vec![])).is_err());
        assert_eq!(encode(WireType::Bit, Datum::Bool(true)).unwrap(), [1, 1]);
        assert_eq!(encode(WireType::Bit, Datum::Int(0)).unwrap(), [1, 0]);
        assert_eq!(
            encode(
                WireType::Binary(8),
                Datum::Bit {
                    value: 0x0102,
                    width: 16
                }
            )
            .unwrap(),
            [8, 0, 0, 0, 0, 0, 0, 0, 1, 2]
        );
    }

    #[test]
    fn strings_are_utf16() {
        assert_eq!(
            encode(WireType::NVarchar, Datum::String("h\u{e9}".into())).unwrap(),
            [4, 0, b'h', 0, 0xE9, 0]
        );
        assert_eq!(
            encode(WireType::NVarchar, Datum::String(String::new())).unwrap(),
            [0, 0]
        );
        // Non-string values in a character column are sent as their text
        assert_eq!(
            encode(WireType::NVarchar, Datum::Int(42)).unwrap(),
            [4, 0, b'4', 0, b'2', 0]
        );
        // 4000 characters fit, 4001 do not
        assert!(encode(WireType::NVarchar, Datum::String("x".repeat(4000))).is_ok());
        assert!(encode(WireType::NVarchar, Datum::String("x".repeat(4001))).is_err());
    }

    #[test]
    fn max_types_use_plp_framing() {
        let got = encode(WireType::NVarcharMax, Datum::String("ab".into())).unwrap();
        let mut want = 4u64.to_le_bytes().to_vec();
        want.extend_from_slice(&4u32.to_le_bytes());
        want.extend_from_slice(&[b'a', 0, b'b', 0]);
        want.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(got, want);

        // Empty value: zero length, no chunks, terminator
        let got = encode(WireType::VarBinaryMax, Datum::Bytes(vec![])).unwrap();
        assert_eq!(got, [0u8; 12]);

        // Large value keeps every byte
        let big = vec![7u8; 100_000];
        let got = encode(WireType::VarBinaryMax, Datum::Bytes(big.clone())).unwrap();
        assert_eq!(got.len(), 8 + 4 + big.len() + 4);
        assert_eq!(&got[12..12 + big.len()], big.as_slice());
    }

    #[test]
    fn decimals() {
        let wire = WireType::Decimal {
            precision: 10,
            scale: 2,
        };
        // -12.34 -> sign 0, magnitude 1234, 9 bytes total after the length
        let got = encode(
            wire,
            Datum::Decimal {
                value: -1234,
                scale: 2,
            },
        )
        .unwrap();
        assert_eq!(got[..2], [9, 0]);
        assert_eq!(got[2..], 1234u64.to_le_bytes());

        // Rescaled up; integers and exact text accepted
        let got = encode(wire, Datum::Decimal { value: 5, scale: 1 }).unwrap();
        assert_eq!(got[2..], 50u64.to_le_bytes());
        assert_eq!(
            encode(wire, Datum::Int(3)).unwrap()[2..],
            300u64.to_le_bytes()
        );
        assert_eq!(
            encode(wire, Datum::String("1.5".into())).unwrap()[2..],
            150u64.to_le_bytes()
        );

        // Would lose digits or overflow the precision: refused
        assert!(encode(
            wire,
            Datum::Decimal {
                value: 12345,
                scale: 4
            }
        )
        .is_err());
        assert!(encode(wire, Datum::Int(1_000_000_000)).is_err());

        // u64::MAX fits DECIMAL(20, 0) in 13 bytes
        let wire = WireType::Decimal {
            precision: 20,
            scale: 0,
        };
        let got = encode(wire, Datum::UnsignedInt(u64::MAX)).unwrap();
        assert_eq!(got[..2], [13, 1]);
        assert_eq!(got[2..10], u64::MAX.to_le_bytes());
        assert_eq!(got[10..], [0, 0, 0, 0]);
    }

    #[test]
    fn decimal_wire_lengths_by_precision() {
        assert_eq!(decimal_wire_len(1), 5);
        assert_eq!(decimal_wire_len(9), 5);
        assert_eq!(decimal_wire_len(10), 9);
        assert_eq!(decimal_wire_len(19), 9);
        assert_eq!(decimal_wire_len(20), 13);
        assert_eq!(decimal_wire_len(28), 13);
        assert_eq!(decimal_wire_len(29), 17);
        assert_eq!(decimal_wire_len(38), 17);
    }

    #[test]
    fn timestamps() {
        // 1970-01-01 00:00:00.001
        let got = encode(WireType::DateTime2, Datum::Timestamp(1)).unwrap();
        assert_eq!(got[0], 7);
        assert_eq!(got[1..5], 1u32.to_le_bytes());
        assert_eq!(got[5..], (UNIX_EPOCH_DAY as u32).to_le_bytes()[..3]);

        // One millisecond before the epoch: previous day, last millisecond
        let got = encode(WireType::DateTime2, Datum::Timestamp(-1)).unwrap();
        assert_eq!(got[1..5], 86_399_999u32.to_le_bytes());
        assert_eq!(got[5..], ((UNIX_EPOCH_DAY - 1) as u32).to_le_bytes()[..3]);

        // Outside years 1..=9999
        assert!(encode(WireType::DateTime2, Datum::Timestamp(i64::MAX)).is_err());
        assert!(encode(WireType::DateTime2, Datum::Timestamp(i64::MIN)).is_err());
        assert!(encode(WireType::DateTime2, Datum::Int(5)).is_err());
    }

    #[test]
    fn timestamp_text_and_calendar() {
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00");
        assert_eq!(format_timestamp(951_782_400_123), "2000-02-29 00:00:00.123");
        assert_eq!(format_timestamp(-1), "1969-12-31 23:59:59.999");
        assert_eq!(civil_from_days(-UNIX_EPOCH_DAY), (1, 1, 1));
        assert_eq!(civil_from_days(MAX_DAY - UNIX_EPOCH_DAY), (9999, 12, 31));
    }

    #[test]
    fn colmetadata_layout() {
        let columns = [
            OutputColumn {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
            },
            OutputColumn {
                id: 1,
                name: "name".to_string(),
                data_type: DataType::Varchar(50),
                nullable: true,
            },
        ];
        let mut buf = Vec::new();
        let wire = put_colmetadata(&mut buf, &columns);
        assert_eq!(wire, vec![WireType::Int(4), WireType::NVarchar]);

        let mut want = vec![TOKEN_COLMETADATA, 2, 0];
        // id: usertype, flags, INTN(4), name
        want.extend_from_slice(&[0, 0, 0, 0, 1, 0, TYPE_INTN, 4, 2, b'i', 0, b'd', 0]);
        // name: usertype, flags, NVARCHAR(8000 bytes) + collation, name
        want.extend_from_slice(&[0, 0, 0, 0, 1, 0, TYPE_NVARCHAR, 0x40, 0x1F]);
        want.extend_from_slice(&DEFAULT_COLLATION);
        want.extend_from_slice(&[4, b'n', 0, b'a', 0, b'm', 0, b'e', 0]);
        assert_eq!(buf, want);
    }

    #[test]
    fn a_bad_value_leaves_no_partial_row() {
        let wire = [WireType::Int(4), WireType::Int(4)];
        let mut buf = vec![0xEE];
        let err = put_row(
            &mut buf,
            &wire,
            &[Datum::Int(1), Datum::String("oops".into())],
        )
        .unwrap_err();
        assert!(err.0.starts_with("column 2:"), "{}", err.0);
        assert_eq!(buf, [0xEE]);

        // A short row is padded with NULLs rather than misaligned
        put_row(&mut buf, &wire, &[Datum::Int(1)]).unwrap();
        assert_eq!(buf, [0xEE, TOKEN_ROW, 4, 1, 0, 0, 0, 0]);
    }
}
