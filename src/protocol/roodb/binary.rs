//! Binary result set encoding for prepared statement responses.
//!
//! MySQL protocol requires binary format for COM_STMT_EXECUTE results.
//! Each row uses a 0x00 header, a null bitmap (with offset=2), and
//! type-specific binary encoding for each non-null column value.

use crate::catalog::DataType;
use crate::executor::datum::Datum;
use crate::executor::row::Row;
use crate::planner::logical::OutputColumn;

use super::packet::encode_length_encoded_int;

/// Encode a single row in binary result set format.
///
/// Layout:
///   [0x00]  packet header
///   [null_bitmap]  (num_columns + 7 + 2) / 8 bytes, offset=2
///   [values]  binary-encoded column values (non-null only)
pub fn encode_binary_row(row: &Row, columns: &[OutputColumn]) -> Vec<u8> {
    let num_cols = columns.len();
    let null_bitmap_len = (num_cols + 7 + 2) / 8;

    let mut buf = Vec::with_capacity(4 + null_bitmap_len + num_cols * 8);

    // Header
    buf.push(0x00);

    // Null bitmap placeholder (filled in below)
    let bitmap_start = buf.len();
    buf.resize(bitmap_start + null_bitmap_len, 0);

    // Values
    for (i, datum) in row.iter().enumerate() {
        if datum.is_null() {
            // Set bit in null bitmap (offset=2)
            let bit_pos = i + 2;
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;
            buf[bitmap_start + byte_idx] |= 1 << bit_idx;
        } else {
            let dt = if i < columns.len() {
                &columns[i].data_type
            } else {
                // Fallback: infer from datum
                &DataType::Varchar(255)
            };
            buf.extend_from_slice(&datum_to_binary(datum, dt));
        }
    }

    buf
}

/// Encode a `Datum` in MySQL binary protocol format.
fn datum_to_binary(datum: &Datum, data_type: &DataType) -> Vec<u8> {
    match datum {
        Datum::Null => vec![], // handled by null bitmap

        Datum::Bool(b) => vec![if *b { 1 } else { 0 }],

        Datum::Int(i) => match data_type {
            DataType::TinyInt | DataType::Boolean => vec![*i as u8],
            DataType::SmallInt => (*i as i16).to_le_bytes().to_vec(),
            DataType::Int => (*i as i32).to_le_bytes().to_vec(),
            DataType::BigInt | DataType::Timestamp => i.to_le_bytes().to_vec(),
            // For other types, send as i64
            _ => i.to_le_bytes().to_vec(),
        },

        Datum::UnsignedInt(u) => u.to_le_bytes().to_vec(),

        Datum::Float(f) => match data_type {
            DataType::Float => (*f as f32).to_le_bytes().to_vec(),
            _ => f.to_le_bytes().to_vec(),
        },

        Datum::String(s) => {
            let bytes = s.as_bytes();
            let mut out = encode_length_encoded_int(bytes.len() as u64);
            out.extend_from_slice(bytes);
            out
        }

        Datum::Bytes(b) | Datum::Geometry(b) => {
            let mut out = encode_length_encoded_int(b.len() as u64);
            out.extend_from_slice(b);
            out
        }

        Datum::Bit { value, width } => {
            let byte_len = (*width as usize).div_ceil(8);
            let be_bytes = value.to_be_bytes();
            let bytes = &be_bytes[8 - byte_len..];
            let mut out = encode_length_encoded_int(bytes.len() as u64);
            out.extend_from_slice(bytes);
            out
        }

        Datum::Timestamp(ts) => {
            // Encode as MySQL binary datetime: length-prefixed fields
            encode_binary_timestamp(*ts)
        }

        Datum::Decimal { value, scale } => {
            // Encode as length-encoded string (MySQL sends DECIMAL as string in binary protocol)
            let s = crate::executor::datum::format_decimal(*value, *scale);
            let bytes = s.as_bytes();
            let mut out = encode_length_encoded_int(bytes.len() as u64);
            out.extend_from_slice(bytes);
            out
        }
    }
}

/// Encode a unix-millis timestamp as MySQL binary datetime.
fn encode_binary_timestamp(ts_millis: i64) -> Vec<u8> {
    if ts_millis == 0 {
        return vec![0]; // zero-length datetime
    }

    let secs = ts_millis / 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = (secs % 86400).unsigned_abs();

    let (year, month, day) = days_to_ymd(days_since_epoch);
    let hour = (time_of_day / 3600) as u8;
    let minute = ((time_of_day % 3600) / 60) as u8;
    let second = (time_of_day % 60) as u8;

    // 7-byte datetime: length, year(2), month, day, hour, minute, second
    vec![
        7, // length
        (year & 0xFF) as u8,
        ((year >> 8) & 0xFF) as u8,
        month as u8,
        day as u8,
        hour,
        minute,
        second,
    ]
}

/// Convert days since Unix epoch to (year, month, day)
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Reuse the algorithm from types.rs
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
    (y as i32, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_row_single_int() {
        let row = Row::new(vec![Datum::Int(42)]);
        let cols = vec![OutputColumn {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
        }];
        let encoded = encode_binary_row(&row, &cols);
        assert_eq!(encoded[0], 0x00); // header
                                      // null bitmap: (1 + 7 + 2) / 8 = 1 byte, all zeros (no nulls)
        assert_eq!(encoded[1], 0x00);
        // value: 42 as i32 LE
        let val = i32::from_le_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]);
        assert_eq!(val, 42);
    }

    #[test]
    fn test_binary_row_null() {
        let row = Row::new(vec![Datum::Null]);
        let cols = vec![OutputColumn {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: true,
        }];
        let encoded = encode_binary_row(&row, &cols);
        assert_eq!(encoded[0], 0x00); // header
                                      // null bitmap: column 0, bit position = 0+2 = 2, byte 0, bit 2
        assert_ne!(encoded[1] & (1 << 2), 0);
    }

    #[test]
    fn test_binary_row_string() {
        let row = Row::new(vec![Datum::String("hello".to_string())]);
        let cols = vec![OutputColumn {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::Varchar(255),
            nullable: false,
        }];
        let encoded = encode_binary_row(&row, &cols);
        assert_eq!(encoded[0], 0x00); // header
        assert_eq!(encoded[1], 0x00); // null bitmap
                                      // length-encoded string: 5, h, e, l, l, o
        assert_eq!(encoded[2], 5);
        assert_eq!(&encoded[3..8], b"hello");
    }

    #[test]
    fn test_binary_timestamp_zero() {
        let encoded = encode_binary_timestamp(0);
        assert_eq!(encoded, vec![0]); // zero-length
    }
}
