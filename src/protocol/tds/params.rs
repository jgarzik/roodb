//! RPC request parameters: decoding TDS typed values into RooDB values.
//!
//! Every length and type byte here comes from the client. Decoding goes
//! through a bounds-checked reader and returns an error for anything
//! malformed or unsupported; it never indexes past the data.
//!
//! Parameter values become [`Datum`]s that are bound to the statement by
//! the query engine. They are never formatted into SQL text.

use crate::executor::datum::{format_decimal, Datum};

use super::login::decode_ucs2;

// TDS type tokens (MS-TDS 2.2.5.4)
const TYPE_NULL: u8 = 0x1F;
const TYPE_GUID: u8 = 0x24;
const TYPE_INTN: u8 = 0x26;
const TYPE_DATEN: u8 = 0x28;
const TYPE_TIMEN: u8 = 0x29;
const TYPE_DATETIME2N: u8 = 0x2A;
const TYPE_DATETIMEOFFSETN: u8 = 0x2B;
const TYPE_INT1: u8 = 0x30;
const TYPE_BIT: u8 = 0x32;
const TYPE_INT2: u8 = 0x34;
const TYPE_INT4: u8 = 0x38;
const TYPE_DATETIM4: u8 = 0x3A;
const TYPE_FLT4: u8 = 0x3B;
const TYPE_MONEY: u8 = 0x3C;
const TYPE_DATETIME: u8 = 0x3D;
const TYPE_FLT8: u8 = 0x3E;
const TYPE_BITN: u8 = 0x68;
const TYPE_DECIMALN: u8 = 0x6A;
const TYPE_NUMERICN: u8 = 0x6C;
const TYPE_FLTN: u8 = 0x6D;
const TYPE_MONEYN: u8 = 0x6E;
const TYPE_DATETIMN: u8 = 0x6F;
const TYPE_MONEY4: u8 = 0x7A;
const TYPE_INT8: u8 = 0x7F;
const TYPE_BIGVARBINARY: u8 = 0xA5;
const TYPE_BIGVARCHAR: u8 = 0xA7;
const TYPE_BIGBINARY: u8 = 0xAD;
const TYPE_BIGCHAR: u8 = 0xAF;
const TYPE_NVARCHAR: u8 = 0xE7;
const TYPE_NCHAR: u8 = 0xEF;

/// Length marker of the (max) types
const MAX_TYPE_LENGTH: u16 = 0xFFFF;
/// PLP NULL and "length unknown" markers
const PLP_NULL: u64 = u64::MAX;
const PLP_UNKNOWN_LENGTH: u64 = u64::MAX - 1;
/// Days from 0001-01-01 to 1900-01-01 (the legacy DATETIME epoch)
const DAYS_TO_1900: i64 = 693_595;
/// Days from 0001-01-01 to 1970-01-01
const DAYS_TO_1970: i64 = 719_162;
/// Last day number a date can hold (9999-12-31)
const MAX_DAY: i64 = 3_652_058;

/// A request the server cannot decode or does not support.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ParamError {
    #[error("malformed RPC request")]
    Malformed,
    #[error("parameter type 0x{0:02X} is not supported")]
    UnsupportedType(u8),
}

/// One parameter of an RPC request.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcParam {
    /// Parameter name including its `@`, or empty for a positional one
    pub name: String,
    /// The client wants the value returned (output parameter)
    pub output: bool,
    /// The value
    pub value: Datum,
}

/// Bounds-checked reader over request bytes.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Whether all data has been consumed.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// The next byte, without consuming it.
    pub fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8], ParamError> {
        let end = self.pos.checked_add(len).ok_or(ParamError::Malformed)?;
        let slice = self.data.get(self.pos..end).ok_or(ParamError::Malformed)?;
        self.pos = end;
        Ok(slice)
    }

    pub fn u8(&mut self) -> Result<u8, ParamError> {
        Ok(self.bytes(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, ParamError> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub fn u32(&mut self) -> Result<u32, ParamError> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> Result<u64, ParamError> {
        let b = self.bytes(8)?;
        Ok(u64::from_le_bytes(b.try_into().expect("8 bytes")))
    }

    /// A string with a 1-byte length in UTF-16 units.
    pub fn b_varchar(&mut self) -> Result<String, ParamError> {
        let chars = usize::from(self.u8()?);
        Ok(decode_ucs2(self.bytes(chars * 2)?))
    }

    /// A string with a 2-byte length in UTF-16 units.
    pub fn us_varchar(&mut self) -> Result<String, ParamError> {
        let chars = usize::from(self.u16()?);
        Ok(decode_ucs2(self.bytes(chars * 2)?))
    }
}

/// Little-endian unsigned integer of up to 16 bytes.
fn le_uint(bytes: &[u8]) -> u128 {
    bytes
        .iter()
        .rev()
        .fold(0u128, |acc, b| (acc << 8) | u128::from(*b))
}

/// Sign-extended little-endian integer of 1, 2, 4 or 8 bytes.
fn le_int(bytes: &[u8]) -> Result<i64, ParamError> {
    Ok(match bytes.len() {
        // TDS TINYINT is unsigned
        1 => i64::from(bytes[0]),
        2 => i64::from(i16::from_le_bytes([bytes[0], bytes[1]])),
        4 => i64::from(i32::from_le_bytes(bytes.try_into().expect("4 bytes"))),
        8 => i64::from_le_bytes(bytes.try_into().expect("8 bytes")),
        _ => return Err(ParamError::Malformed),
    })
}

fn float(bytes: &[u8]) -> Result<Datum, ParamError> {
    Ok(Datum::Float(match bytes.len() {
        4 => f64::from(f32::from_le_bytes(bytes.try_into().expect("4 bytes"))),
        8 => f64::from_le_bytes(bytes.try_into().expect("8 bytes")),
        _ => return Err(ParamError::Malformed),
    }))
}

/// Calendar date of a day number counted from 0001-01-01.
fn date_text(day: i64) -> Result<String, ParamError> {
    if !(0..=MAX_DAY).contains(&day) {
        return Err(ParamError::Malformed);
    }
    // Days since 1970-01-01 to civil date (proleptic Gregorian)
    let z = day - DAYS_TO_1970 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

/// Time of day from a count of 10^-scale second units since midnight.
fn time_text(units: u64, scale: u8) -> Result<String, ParamError> {
    if scale > 7 {
        return Err(ParamError::Malformed);
    }
    let per_second = 10u64.pow(u32::from(scale));
    let seconds = units / per_second;
    if seconds >= 86_400 {
        return Err(ParamError::Malformed);
    }
    let fraction = units % per_second;
    let (h, m, s) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    Ok(if scale == 0 || fraction == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!(
            "{h:02}:{m:02}:{s:02}.{fraction:0width$}",
            width = usize::from(scale)
        )
    })
}

/// Bytes a time of the given scale occupies.
fn time_len(scale: u8) -> Result<usize, ParamError> {
    match scale {
        0..=2 => Ok(3),
        3..=4 => Ok(4),
        5..=7 => Ok(5),
        _ => Err(ParamError::Malformed),
    }
}

/// Text of the single-byte code page characters clients send for VARCHAR
/// parameters: UTF-8 if the bytes are valid UTF-8, otherwise Latin-1.
fn code_page_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => bytes.iter().map(|b| char::from(*b)).collect(),
    }
}

/// Read a partially-length-prefixed value: `None` for NULL.
fn plp(r: &mut Reader<'_>, max_len: usize) -> Result<Option<Vec<u8>>, ParamError> {
    let total = r.u64()?;
    if total == PLP_NULL {
        return Ok(None);
    }
    if total != PLP_UNKNOWN_LENGTH && total > max_len as u64 {
        return Err(ParamError::Malformed);
    }
    let mut value = Vec::new();
    loop {
        let chunk = r.u32()? as usize;
        if chunk == 0 {
            return Ok(Some(value));
        }
        if value.len() + chunk > max_len {
            return Err(ParamError::Malformed);
        }
        value.extend_from_slice(r.bytes(chunk)?);
    }
}

/// Read a value with a 2-byte length, or a PLP value for a (max) type.
fn long_value(
    r: &mut Reader<'_>,
    declared_len: u16,
    max_len: usize,
) -> Result<Option<Vec<u8>>, ParamError> {
    if declared_len == MAX_TYPE_LENGTH {
        return plp(r, max_len);
    }
    let len = r.u16()?;
    if len == u16::MAX {
        return Ok(None);
    }
    Ok(Some(r.bytes(usize::from(len))?.to_vec()))
}

/// Legacy DATETIME (days since 1900 and 1/300 s ticks) or SMALLDATETIME
/// (days and minutes) as text.
fn legacy_datetime(bytes: &[u8]) -> Result<Datum, ParamError> {
    let (days, seconds, millis) = match bytes.len() {
        4 => {
            let days = i64::from(u16::from_le_bytes([bytes[0], bytes[1]]));
            let minutes = u64::from(u16::from_le_bytes([bytes[2], bytes[3]]));
            (days, minutes * 60, 0)
        }
        8 => {
            let days = i64::from(i32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")));
            let ticks = u64::from(u32::from_le_bytes(bytes[4..].try_into().expect("4 bytes")));
            // 300 ticks per second
            (days, ticks / 300, ticks % 300 * 1000 / 300)
        }
        _ => return Err(ParamError::Malformed),
    };
    let date = date_text(days + DAYS_TO_1900)?;
    let time = time_text(seconds * 1000 + millis, 3)?;
    Ok(Datum::String(format!("{date} {time}")))
}

/// MONEY: a scaled integer with 4 decimal places.
fn money(bytes: &[u8]) -> Result<Datum, ParamError> {
    let value = match bytes.len() {
        4 => i64::from(i32::from_le_bytes(bytes.try_into().expect("4 bytes"))),
        8 => {
            // High 4 bytes first, then low 4 bytes, each little-endian
            let high = i64::from(i32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")));
            let low = i64::from(u32::from_le_bytes(bytes[4..].try_into().expect("4 bytes")));
            (high << 32) | low
        }
        _ => return Err(ParamError::Malformed),
    };
    Ok(Datum::String(format_decimal(i128::from(value), 4)))
}

/// Decode one TYPE_INFO and its value.
///
/// `max_len` bounds any single value. Numbers become `Int`/`Float`; text
/// becomes `String`; binary becomes `Bytes`; decimals, dates and times
/// become their text form, which is how the engine takes them from the
/// RooDB protocol as well.
pub fn read_typed_value(r: &mut Reader<'_>, max_len: usize) -> Result<Datum, ParamError> {
    let type_id = r.u8()?;
    Ok(match type_id {
        TYPE_NULL => Datum::Null,

        // Fixed-length types: no length byte
        TYPE_INT1 => Datum::Int(le_int(r.bytes(1)?)?),
        TYPE_INT2 => Datum::Int(le_int(r.bytes(2)?)?),
        TYPE_INT4 => Datum::Int(le_int(r.bytes(4)?)?),
        TYPE_INT8 => Datum::Int(le_int(r.bytes(8)?)?),
        TYPE_BIT => Datum::Bool(r.u8()? != 0),
        TYPE_FLT4 => float(r.bytes(4)?)?,
        TYPE_FLT8 => float(r.bytes(8)?)?,
        TYPE_DATETIME => legacy_datetime(r.bytes(8)?)?,
        TYPE_DATETIM4 => legacy_datetime(r.bytes(4)?)?,
        TYPE_MONEY => money(r.bytes(8)?)?,
        TYPE_MONEY4 => money(r.bytes(4)?)?,

        // Nullable fixed-size types: declared size, then a length byte
        TYPE_INTN | TYPE_BITN | TYPE_FLTN | TYPE_MONEYN | TYPE_DATETIMN | TYPE_GUID => {
            let _declared = r.u8()?;
            let len = usize::from(r.u8()?);
            if len == 0 {
                return Ok(Datum::Null);
            }
            let value = r.bytes(len)?;
            match type_id {
                TYPE_INTN => Datum::Int(le_int(value)?),
                TYPE_BITN => match value {
                    [b] => Datum::Bool(*b != 0),
                    _ => return Err(ParamError::Malformed),
                },
                TYPE_FLTN => float(value)?,
                TYPE_MONEYN => money(value)?,
                TYPE_DATETIMN => legacy_datetime(value)?,
                _ => {
                    let g: [u8; 16] = value.try_into().map_err(|_| ParamError::Malformed)?;
                    // The first three groups are little-endian
                    Datum::String(format!(
                        "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        u32::from_le_bytes([g[0], g[1], g[2], g[3]]),
                        u16::from_le_bytes([g[4], g[5]]),
                        u16::from_le_bytes([g[6], g[7]]),
                        g[8],
                        g[9],
                        g[10],
                        g[11],
                        g[12],
                        g[13],
                        g[14],
                        g[15]
                    ))
                }
            }
        }

        TYPE_DECIMALN | TYPE_NUMERICN => {
            let _declared = r.u8()?;
            let _precision = r.u8()?;
            let scale = r.u8()?;
            let len = usize::from(r.u8()?);
            if len == 0 {
                return Ok(Datum::Null);
            }
            // Sign byte plus a 4, 8, 12 or 16 byte magnitude
            if !matches!(len, 5 | 9 | 13 | 17) || scale > 38 {
                return Err(ParamError::Malformed);
            }
            let value = r.bytes(len)?;
            let magnitude =
                i128::try_from(le_uint(&value[1..])).map_err(|_| ParamError::Malformed)?;
            let signed = if value[0] == 0 { -magnitude } else { magnitude };
            Datum::String(format_decimal(signed, scale))
        }

        TYPE_DATEN => {
            let len = usize::from(r.u8()?);
            match len {
                0 => Datum::Null,
                3 => Datum::String(date_text(le_uint(r.bytes(3)?) as i64)?),
                _ => return Err(ParamError::Malformed),
            }
        }

        TYPE_TIMEN | TYPE_DATETIME2N | TYPE_DATETIMEOFFSETN => {
            let scale = r.u8()?;
            let time_bytes = time_len(scale)?;
            let len = usize::from(r.u8()?);
            if len == 0 {
                return Ok(Datum::Null);
            }
            let expected = match type_id {
                TYPE_TIMEN => time_bytes,
                TYPE_DATETIME2N => time_bytes + 3,
                _ => time_bytes + 5,
            };
            if len != expected {
                return Err(ParamError::Malformed);
            }
            let value = r.bytes(len)?;
            let time = time_text(le_uint(&value[..time_bytes]) as u64, scale)?;
            if type_id == TYPE_TIMEN {
                Datum::String(time)
            } else {
                // DATETIMEOFFSET carries UTC time plus an offset; the
                // UTC value is what is stored.
                let date = date_text(le_uint(&value[time_bytes..time_bytes + 3]) as i64)?;
                Datum::String(format!("{date} {time}"))
            }
        }

        TYPE_NVARCHAR | TYPE_NCHAR => {
            let declared = r.u16()?;
            let _collation = r.bytes(5)?;
            match long_value(r, declared, max_len)? {
                None => Datum::Null,
                Some(bytes) if bytes.len() % 2 == 0 => Datum::String(decode_ucs2(&bytes)),
                Some(_) => return Err(ParamError::Malformed),
            }
        }

        TYPE_BIGVARCHAR | TYPE_BIGCHAR => {
            let declared = r.u16()?;
            let _collation = r.bytes(5)?;
            match long_value(r, declared, max_len)? {
                None => Datum::Null,
                Some(bytes) => Datum::String(code_page_text(&bytes)),
            }
        }

        TYPE_BIGVARBINARY | TYPE_BIGBINARY => {
            let declared = r.u16()?;
            match long_value(r, declared, max_len)? {
                None => Datum::Null,
                Some(bytes) => Datum::Bytes(bytes),
            }
        }

        other => return Err(ParamError::UnsupportedType(other)),
    })
}

/// Read one RPC parameter: name, status flags, type and value.
pub fn read_param(r: &mut Reader<'_>, max_len: usize) -> Result<RpcParam, ParamError> {
    let name = r.b_varchar()?;
    let status = r.u8()?;
    let value = read_typed_value(r, max_len)?;
    Ok(RpcParam {
        name,
        output: status & 0x01 != 0,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1 << 20;

    fn decode(bytes: &[u8]) -> Result<Datum, ParamError> {
        let mut r = Reader::new(bytes);
        let value = read_typed_value(&mut r, MAX)?;
        assert!(r.is_empty(), "trailing bytes after the value");
        Ok(value)
    }

    fn text(s: &str) -> Datum {
        Datum::String(s.to_string())
    }

    #[test]
    fn integers_bits_and_floats() {
        assert_eq!(
            decode(&[TYPE_INTN, 4, 4, 0xFE, 0xFF, 0xFF, 0xFF]),
            Ok(Datum::Int(-2))
        );
        assert_eq!(decode(&[TYPE_INTN, 8, 0]), Ok(Datum::Null));
        assert_eq!(decode(&[TYPE_INTN, 1, 1, 200]), Ok(Datum::Int(200)));
        assert_eq!(decode(&[TYPE_INT2, 0x00, 0x80]), Ok(Datum::Int(-32768)));
        let mut big = vec![TYPE_INT8];
        big.extend_from_slice(&i64::MIN.to_le_bytes());
        assert_eq!(decode(&big), Ok(Datum::Int(i64::MIN)));
        assert_eq!(decode(&[TYPE_BITN, 1, 1, 1]), Ok(Datum::Bool(true)));
        assert_eq!(decode(&[TYPE_BIT, 0]), Ok(Datum::Bool(false)));
        let mut f = vec![TYPE_FLTN, 8, 8];
        f.extend_from_slice(&1.5f64.to_le_bytes());
        assert_eq!(decode(&f), Ok(Datum::Float(1.5)));
        let mut f = vec![TYPE_FLTN, 4, 4];
        f.extend_from_slice(&2.5f32.to_le_bytes());
        assert_eq!(decode(&f), Ok(Datum::Float(2.5)));
        // A 3-byte integer does not exist
        assert_eq!(
            decode(&[TYPE_INTN, 4, 3, 1, 2, 3]),
            Err(ParamError::Malformed)
        );
        assert_eq!(decode(&[TYPE_BITN, 1, 2, 1, 1]), Err(ParamError::Malformed));
    }

    #[test]
    fn strings_and_binary() {
        let mut v = vec![TYPE_NVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 4, 0];
        v.extend_from_slice(&[b'h', 0, 0xE9, 0]);
        assert_eq!(decode(&v), Ok(text("h\u{e9}")));

        // NULL and empty are different
        let null = [TYPE_NVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 0xFF, 0xFF];
        assert_eq!(decode(&null), Ok(Datum::Null));
        let empty = [TYPE_NVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 0, 0];
        assert_eq!(decode(&empty), Ok(text("")));

        // Odd byte count is not UTF-16
        let odd = [TYPE_NVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 1, 0, b'x'];
        assert_eq!(decode(&odd), Err(ParamError::Malformed));

        // VARCHAR: UTF-8 if valid, otherwise Latin-1
        let v = [TYPE_BIGVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 2, 0, 0xC3, 0xA9];
        assert_eq!(decode(&v), Ok(text("\u{e9}")));
        let v = [TYPE_BIGVARCHAR, 8, 0, 9, 4, 0xD0, 0, 0x34, 1, 0, 0xE9];
        assert_eq!(decode(&v), Ok(text("\u{e9}")));

        let v = [TYPE_BIGVARBINARY, 8, 0, 3, 0, 0, 0xFF, 0x10];
        assert_eq!(decode(&v), Ok(Datum::Bytes(vec![0, 0xFF, 0x10])));
    }

    #[test]
    fn plp_values() {
        // NVARCHAR(max) "ab" in two chunks
        let mut v = vec![TYPE_NVARCHAR, 0xFF, 0xFF, 9, 4, 0xD0, 0, 0x34];
        v.extend_from_slice(&4u64.to_le_bytes());
        v.extend_from_slice(&2u32.to_le_bytes());
        v.extend_from_slice(&[b'a', 0]);
        v.extend_from_slice(&2u32.to_le_bytes());
        v.extend_from_slice(&[b'b', 0]);
        v.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&v), Ok(text("ab")));

        // NULL, and unknown total length
        let mut v = vec![TYPE_BIGVARBINARY, 0xFF, 0xFF];
        v.extend_from_slice(&PLP_NULL.to_le_bytes());
        assert_eq!(decode(&v), Ok(Datum::Null));
        let mut v = vec![TYPE_BIGVARBINARY, 0xFF, 0xFF];
        v.extend_from_slice(&PLP_UNKNOWN_LENGTH.to_le_bytes());
        v.extend_from_slice(&1u32.to_le_bytes());
        v.push(7);
        v.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&v), Ok(Datum::Bytes(vec![7])));

        // Declared or actual size above the cap is refused before reading
        let mut v = vec![TYPE_BIGVARBINARY, 0xFF, 0xFF];
        v.extend_from_slice(&(MAX as u64 + 1).to_le_bytes());
        assert_eq!(decode(&v), Err(ParamError::Malformed));
        let mut v = vec![TYPE_BIGVARBINARY, 0xFF, 0xFF];
        v.extend_from_slice(&PLP_UNKNOWN_LENGTH.to_le_bytes());
        v.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode(&v), Err(ParamError::Malformed));
    }

    #[test]
    fn decimals_and_money() {
        // DECIMAL(10,2) -12.34
        let mut v = vec![TYPE_DECIMALN, 9, 10, 2, 9, 0];
        v.extend_from_slice(&1234u64.to_le_bytes());
        assert_eq!(decode(&v), Ok(text("-12.34")));
        let mut v = vec![TYPE_NUMERICN, 5, 5, 0, 5, 1];
        v.extend_from_slice(&42u32.to_le_bytes());
        assert_eq!(decode(&v), Ok(text("42")));
        assert_eq!(decode(&[TYPE_DECIMALN, 9, 10, 2, 0]), Ok(Datum::Null));
        // Bad length and bad scale
        assert_eq!(
            decode(&[TYPE_DECIMALN, 9, 10, 2, 3, 1, 1, 0]),
            Err(ParamError::Malformed)
        );
        let mut v = vec![TYPE_DECIMALN, 5, 5, 255, 5, 1];
        v.extend_from_slice(&1u32.to_le_bytes());
        assert_eq!(decode(&v), Err(ParamError::Malformed));

        // MONEY 12.3456 = 123456 scaled by 10^4
        let mut v = vec![TYPE_MONEYN, 8, 8];
        v.extend_from_slice(&0i32.to_le_bytes());
        v.extend_from_slice(&123_456u32.to_le_bytes());
        assert_eq!(decode(&v), Ok(text("12.3456")));
    }

    #[test]
    fn dates_and_times() {
        // DATE: 2024-02-29 is day 738944
        let day = 738_944u32.to_le_bytes();
        assert_eq!(
            decode(&[TYPE_DATEN, 3, day[0], day[1], day[2]]),
            Ok(text("2024-02-29"))
        );
        assert_eq!(decode(&[TYPE_DATEN, 0]), Ok(Datum::Null));
        assert_eq!(decode(&[TYPE_DATEN, 2, 1, 2]), Err(ParamError::Malformed));

        // DATETIME2(3) 2024-02-29 13:14:15.250
        let ms = ((13 * 3600 + 14 * 60 + 15) * 1000 + 250u32).to_le_bytes();
        let v = [
            TYPE_DATETIME2N,
            3,
            7,
            ms[0],
            ms[1],
            ms[2],
            ms[3],
            day[0],
            day[1],
            day[2],
        ];
        assert_eq!(decode(&v), Ok(text("2024-02-29 13:14:15.250")));

        // TIME(0) 23:59:59
        let secs = 86_399u32.to_le_bytes();
        assert_eq!(
            decode(&[TYPE_TIMEN, 0, 3, secs[0], secs[1], secs[2]]),
            Ok(text("23:59:59"))
        );

        // Scale above 7, wrong length for the scale, time past midnight
        assert_eq!(
            decode(&[TYPE_TIMEN, 255, 3, 0, 0, 0]),
            Err(ParamError::Malformed)
        );
        assert_eq!(
            decode(&[TYPE_DATETIME2N, 3, 1, 0]),
            Err(ParamError::Malformed)
        );
        assert_eq!(
            decode(&[TYPE_TIMEN, 0, 3, 0xFF, 0xFF, 0xFF]),
            Err(ParamError::Malformed)
        );
        // Day number past 9999-12-31
        assert_eq!(
            decode(&[TYPE_DATEN, 3, 0xFF, 0xFF, 0xFF]),
            Err(ParamError::Malformed)
        );

        // Legacy DATETIME: 1900-01-02 00:00:01
        let mut v = vec![TYPE_DATETIMN, 8, 8];
        v.extend_from_slice(&1i32.to_le_bytes());
        v.extend_from_slice(&300u32.to_le_bytes());
        assert_eq!(decode(&v), Ok(text("1900-01-02 00:00:01")));
    }

    #[test]
    fn guid_and_unsupported_types() {
        let mut v = vec![TYPE_GUID, 16, 16];
        v.extend_from_slice(&[
            0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ]);
        assert_eq!(decode(&v), Ok(text("00112233-4455-6677-8899-aabbccddeeff")));

        // XML, UDT, table-valued and sql_variant parameters
        for type_id in [0xF1u8, 0xF0, 0xF3, 0x62] {
            let bytes = [type_id, 0, 0, 0, 0];
            let mut r = Reader::new(&bytes);
            assert_eq!(
                read_typed_value(&mut r, MAX),
                Err(ParamError::UnsupportedType(type_id))
            );
        }
    }

    #[test]
    fn parameter_header() {
        // "@P1", output flag, INTN 5
        let mut v = vec![3, b'@', 0, b'P', 0, b'1', 0, 0x01, TYPE_INTN, 4, 4];
        v.extend_from_slice(&5i32.to_le_bytes());
        let mut r = Reader::new(&v);
        let p = read_param(&mut r, MAX).unwrap();
        assert_eq!(p.name, "@P1");
        assert!(p.output);
        assert_eq!(p.value, Datum::Int(5));
        assert!(r.is_empty());

        // Unnamed, input
        let v = [0, 0x00, TYPE_BIT, 1];
        let p = read_param(&mut Reader::new(&v), MAX).unwrap();
        assert_eq!(
            (p.name.as_str(), p.output, p.value),
            ("", false, Datum::Bool(true))
        );
    }

    /// No input may panic: every truncation of valid values, and every
    /// single-byte corruption, must come back as Ok or Err.
    #[test]
    fn truncation_and_corruption_never_panic() {
        let mut samples: Vec<Vec<u8>> = Vec::new();
        samples.push(vec![TYPE_INTN, 4, 4, 1, 2, 3, 4]);
        let mut v = vec![TYPE_NVARCHAR, 0xFF, 0xFF, 9, 4, 0xD0, 0, 0x34];
        v.extend_from_slice(&4u64.to_le_bytes());
        v.extend_from_slice(&4u32.to_le_bytes());
        v.extend_from_slice(&[b'a', 0, b'b', 0]);
        v.extend_from_slice(&0u32.to_le_bytes());
        samples.push(v);
        let mut v = vec![TYPE_DECIMALN, 17, 38, 10, 17, 1];
        v.extend_from_slice(&[0xFF; 16]);
        samples.push(v);
        samples.push(vec![TYPE_DATETIME2N, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
        samples.push(vec![
            TYPE_DATETIMEOFFSETN,
            7,
            10,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
        ]);
        samples.push(vec![
            TYPE_GUID, 16, 16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ]);
        samples.push(vec![
            TYPE_DATETIMN,
            8,
            8,
            0xFF,
            0xFF,
            0xFF,
            0x7F,
            0xFF,
            0xFF,
            0xFF,
            0xFF,
        ]);

        for sample in &samples {
            for len in 0..sample.len() {
                let _ = read_typed_value(&mut Reader::new(&sample[..len]), MAX);
            }
            for pos in 0..sample.len() {
                for value in [0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF] {
                    let mut corrupt = sample.clone();
                    corrupt[pos] = value;
                    let _ = read_typed_value(&mut Reader::new(&corrupt), MAX);
                }
            }
        }
        // Every type byte with a short tail
        for type_id in 0..=255u8 {
            for tail in [&[][..], &[0xFF; 3], &[0x00; 12], &[0xFF; 24]] {
                let mut bytes = vec![type_id];
                bytes.extend_from_slice(tail);
                let _ = read_typed_value(&mut Reader::new(&bytes), MAX);
            }
        }
    }
}
