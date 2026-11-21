//! COM_STMT_PREPARE / COM_STMT_EXECUTE wire encoding.
//!
//! Statement storage and plan caching are protocol-neutral and live in
//! [`crate::server::query::prepared`]; this module holds the RooDB wire
//! format: the prepare response and binary parameter decoding.

use crate::executor::datum::Datum;

use super::error::{ProtocolError, ProtocolResult};
use super::packet::decode_length_encoded_bytes;
use super::types::ColumnType;

pub use crate::server::query::prepared::{
    count_placeholders, select_column_names, CachedPlan, PreparedStatement,
    PreparedStatementManager,
};

// ── COM_STMT_PREPARE_OK encoding ────────────────────────────────────

/// Encode the COM_STMT_PREPARE_OK packet (first packet of the response).
pub fn encode_prepare_ok(stmt_id: u32, num_cols: u16, num_params: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12);
    buf.push(0x00); // status = OK
    buf.extend_from_slice(&stmt_id.to_le_bytes()); // statement_id
    buf.extend_from_slice(&num_cols.to_le_bytes()); // num_columns
    buf.extend_from_slice(&num_params.to_le_bytes()); // num_params
    buf.push(0x00); // filler
    buf.extend_from_slice(&0u16.to_le_bytes()); // warning_count
    buf
}

// ── COM_STMT_EXECUTE parameter decoding ─────────────────────────────

/// Decode binary parameters from a COM_STMT_EXECUTE payload.
///
/// The payload layout (after stmt_id which is already consumed):
///   [1] flags
///   [4] iteration-count (always 1)
///   if num_params > 0:
///     [(num_params+7)/8]  null bitmap
///     [1]  new_params_bound_flag
///     if new_params_bound_flag == 1:
///       [num_params * 2]  type info (type_code:u8, unsigned_flag:u8)
///     [...]  parameter values
///
/// `prev_types` should be the types from the last execution of this statement.
/// When `new_params_bound_flag == 0`, the client reuses the previous types.
/// Returns (params, type_codes) so the caller can store the types for next time.
pub fn decode_execute_params(
    payload: &[u8],
    num_params: u16,
    prev_types: Option<&[ColumnType]>,
) -> ProtocolResult<(Vec<Datum>, Vec<ColumnType>)> {
    if num_params == 0 {
        return Ok((vec![], vec![]));
    }

    // payload starts right after statement_id (4 bytes already consumed by caller)
    // So payload[0] = flags, payload[1..5] = iteration_count
    if payload.len() < 5 {
        return Err(ProtocolError::InvalidPacket(
            "STMT_EXECUTE payload too short".to_string(),
        ));
    }

    let _flags = payload[0];
    // iteration_count at [1..5] is always 1, skip it
    let mut offset = 5;

    // Null bitmap
    let null_bitmap_len = (num_params as usize).div_ceil(8);
    if payload.len() < offset + null_bitmap_len {
        return Err(ProtocolError::InvalidPacket(
            "STMT_EXECUTE null bitmap truncated".to_string(),
        ));
    }
    let null_bitmap = &payload[offset..offset + null_bitmap_len];
    offset += null_bitmap_len;

    // new_params_bound_flag
    if offset >= payload.len() {
        return Err(ProtocolError::InvalidPacket(
            "STMT_EXECUTE missing new_params_bound_flag".to_string(),
        ));
    }
    let new_params_bound = payload[offset];
    offset += 1;

    // Type info (if new params bound, or reuse from previous execution)
    let type_codes = if new_params_bound == 1 {
        let mut codes = vec![ColumnType::VarString; num_params as usize];
        for tc in codes.iter_mut().take(num_params as usize) {
            if offset + 2 > payload.len() {
                return Err(ProtocolError::InvalidPacket(
                    "STMT_EXECUTE type info truncated".to_string(),
                ));
            }
            *tc = column_type_from_u8(payload[offset]);
            // payload[offset+1] is unsigned flag, we ignore for now
            offset += 2;
        }
        codes
    } else if let Some(prev) = prev_types {
        prev.to_vec()
    } else {
        // No previous types and client didn't send new ones — use VarString as fallback
        vec![ColumnType::VarString; num_params as usize]
    };

    // Decode parameter values
    let mut params = Vec::with_capacity(num_params as usize);
    for (i, &tc) in type_codes.iter().enumerate().take(num_params as usize) {
        // Check null bitmap (offset=0 for params)
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if null_bitmap[byte_idx] & (1 << bit_idx) != 0 {
            params.push(Datum::Null);
            continue;
        }

        let (datum, consumed) = decode_binary_value(&payload[offset..], tc)?;
        params.push(datum);
        offset += consumed;
    }

    Ok((params, type_codes))
}

/// Decode a single binary-encoded parameter value.
fn decode_binary_value(data: &[u8], type_code: ColumnType) -> ProtocolResult<(Datum, usize)> {
    if data.is_empty() {
        return Err(ProtocolError::InvalidPacket(
            "empty binary value".to_string(),
        ));
    }

    match type_code {
        ColumnType::Null => Ok((Datum::Null, 0)),

        ColumnType::Tiny => {
            if data.is_empty() {
                return Err(ProtocolError::InvalidPacket("truncated TINY".into()));
            }
            Ok((Datum::Int(data[0] as i8 as i64), 1))
        }

        ColumnType::Short | ColumnType::Year => {
            if data.len() < 2 {
                return Err(ProtocolError::InvalidPacket("truncated SHORT".into()));
            }
            let v = i16::from_le_bytes([data[0], data[1]]);
            Ok((Datum::Int(v as i64), 2))
        }

        ColumnType::Long | ColumnType::Int24 => {
            if data.len() < 4 {
                return Err(ProtocolError::InvalidPacket("truncated LONG".into()));
            }
            let v = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Ok((Datum::Int(v as i64), 4))
        }

        ColumnType::LongLong => {
            if data.len() < 8 {
                return Err(ProtocolError::InvalidPacket("truncated LONGLONG".into()));
            }
            let v = i64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok((Datum::Int(v), 8))
        }

        ColumnType::Float => {
            if data.len() < 4 {
                return Err(ProtocolError::InvalidPacket("truncated FLOAT".into()));
            }
            let v = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Ok((Datum::Float(v as f64), 4))
        }

        ColumnType::Double => {
            if data.len() < 8 {
                return Err(ProtocolError::InvalidPacket("truncated DOUBLE".into()));
            }
            let v = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok((Datum::Float(v), 8))
        }

        // String/Varchar/Blob types are length-encoded
        ColumnType::Varchar
        | ColumnType::VarString
        | ColumnType::String
        | ColumnType::Blob
        | ColumnType::TinyBlob
        | ColumnType::MediumBlob
        | ColumnType::LongBlob
        | ColumnType::Decimal
        | ColumnType::NewDecimal => {
            let (bytes, consumed) = decode_length_encoded_bytes(data)?;
            match String::from_utf8(bytes.clone()) {
                Ok(s) => Ok((Datum::String(s), consumed)),
                Err(_) => Ok((Datum::Bytes(bytes), consumed)),
            }
        }

        ColumnType::Bit => {
            let (bytes, consumed) = decode_length_encoded_bytes(data)?;
            let byte_count = bytes.len().min(8);
            if byte_count == 0 {
                return Ok((Datum::Null, consumed));
            }
            let mut padded = [0u8; 8];
            let start = 8 - byte_count;
            padded[start..].copy_from_slice(&bytes[..byte_count]);
            let value = u64::from_be_bytes(padded);
            let width = ((byte_count * 8) as u8).min(64);
            Ok((Datum::Bit { value, width }, consumed))
        }

        // Timestamp/Date/Datetime - length-prefixed, parse as string
        ColumnType::Timestamp | ColumnType::Datetime | ColumnType::Date | ColumnType::Time => {
            // MySQL sends datetime in binary as: length byte, then fields
            if data.is_empty() {
                return Err(ProtocolError::InvalidPacket("truncated datetime".into()));
            }
            let len = data[0] as usize;
            if data.len() < 1 + len {
                return Err(ProtocolError::InvalidPacket(
                    "truncated datetime value".into(),
                ));
            }
            if len == 0 {
                // Zero-length = "0000-00-00 00:00:00"
                Ok((Datum::Timestamp(0), 1))
            } else if len >= 4 {
                let year = u16::from_le_bytes([data[1], data[2]]) as i64;
                let month = data[3] as i64;
                let day = data[4] as i64;
                let (hour, minute, second) = if len >= 7 {
                    (data[5] as i64, data[6] as i64, data[7] as i64)
                } else {
                    (0, 0, 0)
                };
                // Simple conversion to unix timestamp (approximate)
                let ts = simple_datetime_to_unix(year, month, day, hour, minute, second);
                Ok((Datum::Timestamp(ts * 1000), 1 + len))
            } else {
                Ok((Datum::Timestamp(0), 1 + len))
            }
        }

        _ => {
            // Fallback: treat as length-encoded string
            let (bytes, consumed) = decode_length_encoded_bytes(data)?;
            match String::from_utf8(bytes.clone()) {
                Ok(s) => Ok((Datum::String(s), consumed)),
                Err(_) => Ok((Datum::Bytes(bytes), consumed)),
            }
        }
    }
}

/// Simple datetime to unix timestamp conversion
fn simple_datetime_to_unix(year: i64, month: i64, day: i64, hour: i64, min: i64, sec: i64) -> i64 {
    // Simplified - only handles dates from 1970+
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += month_days[m as usize] as i64;
        if m == 2 && is_leap_year(year) {
            days += 1;
        }
    }
    days += day - 1;
    days * 86400 + hour * 3600 + min * 60 + sec
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

/// Convert a u8 wire type code to our ColumnType enum.
fn column_type_from_u8(v: u8) -> ColumnType {
    match v {
        0x00 => ColumnType::Decimal,
        0x01 => ColumnType::Tiny,
        0x02 => ColumnType::Short,
        0x03 => ColumnType::Long,
        0x04 => ColumnType::Float,
        0x05 => ColumnType::Double,
        0x06 => ColumnType::Null,
        0x07 => ColumnType::Timestamp,
        0x08 => ColumnType::LongLong,
        0x09 => ColumnType::Int24,
        0x0a => ColumnType::Date,
        0x0b => ColumnType::Time,
        0x0c => ColumnType::Datetime,
        0x0d => ColumnType::Year,
        0x0f => ColumnType::Varchar,
        0x10 => ColumnType::Bit,
        0xf6 => ColumnType::NewDecimal,
        0xf7 => ColumnType::Enum,
        0xf8 => ColumnType::Set,
        0xf9 => ColumnType::TinyBlob,
        0xfa => ColumnType::MediumBlob,
        0xfb => ColumnType::LongBlob,
        0xfc => ColumnType::Blob,
        0xfd => ColumnType::VarString,
        0xfe => ColumnType::String,
        _ => ColumnType::VarString, // default fallback
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_prepare_ok() {
        let packet = encode_prepare_ok(1, 0, 2);
        assert_eq!(packet[0], 0x00); // OK
        assert_eq!(
            u32::from_le_bytes([packet[1], packet[2], packet[3], packet[4]]),
            1
        );
        assert_eq!(u16::from_le_bytes([packet[5], packet[6]]), 0); // columns
        assert_eq!(u16::from_le_bytes([packet[7], packet[8]]), 2); // params
    }

    #[test]
    fn test_decode_execute_params_no_params() {
        let (params, types) = decode_execute_params(&[], 0, None).unwrap();
        assert!(params.is_empty());
        assert!(types.is_empty());
    }

    #[test]
    fn test_column_type_from_u8_roundtrip() {
        assert_eq!(column_type_from_u8(0x03), ColumnType::Long);
        assert_eq!(column_type_from_u8(0x08), ColumnType::LongLong);
        assert_eq!(column_type_from_u8(0xfd), ColumnType::VarString);
    }
}
