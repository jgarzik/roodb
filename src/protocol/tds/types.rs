//! TDS type system - mapping between RooDB data types and TDS wire types.

use crate::catalog::DataType;
use crate::executor::Datum;
use crate::planner::logical::OutputColumn;

use super::login::encode_ucs2;
use super::token::TOKEN_COLMETADATA;

// TDS type IDs (variable-length nullable types)
const INTNTYPE: u8 = 0x26;
const BITNTYPE: u8 = 0x68;
const FLTNTYPE: u8 = 0x6D;
const BIGVARCHRTYPE: u8 = 0xA7;
const BIGVARBINTYPE: u8 = 0xA5;
const DATETIME2NTYPE: u8 = 0x2A;
const NVARCHARTYPE: u8 = 0xE7;

// Column flags
const COL_NULLABLE: u16 = 0x0001;
const COL_READONLY: u16 = 0x0000; // usUpdateable = 0 (read-only)

// Default collation: SQL_Latin1_General_CP1_CI_AS
const DEFAULT_COLLATION: [u8; 5] = [0x09, 0x04, 0xD0, 0x00, 0x34];

/// Encode COLMETADATA token for a set of output columns.
pub fn encode_colmetadata(columns: &[OutputColumn]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.push(TOKEN_COLMETADATA);
    buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());

    for col in columns {
        // UserType: u32 LE = 0
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Flags: u16 LE
        let mut flags: u16 = COL_READONLY;
        if col.nullable {
            flags |= COL_NULLABLE;
        }
        // Set usUpdateable to 0 (bits 2-3 = 0b00 = ReadOnly) and
        // fComputed = 1 (bit 5 set = 0x20) for computed expressions
        flags |= 0x0020; // fComputed - marks as expression result
        buf.extend_from_slice(&flags.to_le_bytes());

        // TYPE_INFO
        encode_type_info(&mut buf, &col.data_type);

        // ColName (B_VARCHAR: 1-byte length in UCS-2 chars + UCS-2 data)
        let name_ucs2 = encode_ucs2(&col.name);
        let char_count = name_ucs2.len() / 2;
        buf.push(char_count as u8);
        buf.extend_from_slice(&name_ucs2);
    }

    buf
}

/// Encode TYPE_INFO for a RooDB DataType.
fn encode_type_info(buf: &mut Vec<u8>, data_type: &DataType) {
    match data_type {
        DataType::Boolean => {
            buf.push(BITNTYPE);
            buf.push(1); // max length
        }
        DataType::TinyInt => {
            buf.push(INTNTYPE);
            buf.push(1); // width
        }
        DataType::SmallInt => {
            buf.push(INTNTYPE);
            buf.push(2);
        }
        DataType::Int => {
            buf.push(INTNTYPE);
            buf.push(4);
        }
        DataType::BigInt => {
            buf.push(INTNTYPE);
            buf.push(8);
        }
        DataType::Float => {
            buf.push(FLTNTYPE);
            buf.push(4);
        }
        DataType::Double => {
            buf.push(FLTNTYPE);
            buf.push(8);
        }
        DataType::Varchar(max_len) => {
            buf.push(BIGVARCHRTYPE);
            // USHORT max length in bytes
            let byte_len = (*max_len as u16).min(8000);
            buf.extend_from_slice(&byte_len.to_le_bytes());
            // Collation
            buf.extend_from_slice(&DEFAULT_COLLATION);
        }
        DataType::Text => {
            // Use NVARCHAR(max) for Text
            buf.push(NVARCHARTYPE);
            // USHORT max length = 0xFFFF for MAX type
            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
            // Collation
            buf.extend_from_slice(&DEFAULT_COLLATION);
        }
        DataType::Blob => {
            buf.push(BIGVARBINTYPE);
            // USHORT max length = 0xFFFF for MAX
            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
        }
        DataType::Timestamp => {
            buf.push(DATETIME2NTYPE);
            buf.push(3); // scale (milliseconds)
        }
    }
}

/// Encode a ROW token for a set of column values.
pub fn encode_row(values: &[Datum], columns: &[OutputColumn]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.push(super::token::TOKEN_ROW);

    for (i, col) in columns.iter().enumerate() {
        let datum = values.get(i).unwrap_or(&Datum::Null);
        encode_value(&mut buf, datum, &col.data_type);
    }

    buf
}

/// Encode a single datum value as TYPE_VARBYTE.
fn encode_value(buf: &mut Vec<u8>, datum: &Datum, data_type: &DataType) {
    match datum {
        Datum::Null => {
            encode_null(buf, data_type);
        }
        Datum::Bool(v) => {
            buf.push(1); // length
            buf.push(u8::from(*v));
        }
        Datum::Int(v) => {
            match data_type {
                DataType::TinyInt => {
                    buf.push(1); // length
                    buf.push(*v as u8);
                }
                DataType::SmallInt => {
                    buf.push(2);
                    buf.extend_from_slice(&(*v as i16).to_le_bytes());
                }
                DataType::Int => {
                    buf.push(4);
                    buf.extend_from_slice(&(*v as i32).to_le_bytes());
                }
                _ => {
                    // Default to BigInt (8 bytes)
                    buf.push(8);
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
        Datum::Float(v) => match data_type {
            DataType::Float => {
                buf.push(4);
                buf.extend_from_slice(&(*v as f32).to_le_bytes());
            }
            _ => {
                buf.push(8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
        },
        Datum::String(s) => {
            match data_type {
                DataType::Text => {
                    // NVARCHAR: encode as UCS-2
                    let ucs2 = encode_ucs2(s);
                    if ucs2.len() > 0xFFFE {
                        // PLP (Partially Length-Prefixed) encoding for large values
                        // For simplicity, truncate to max USHORT
                        let truncated = &ucs2[..0xFFFE];
                        buf.extend_from_slice(&(truncated.len() as u16).to_le_bytes());
                        buf.extend_from_slice(truncated);
                    } else {
                        buf.extend_from_slice(&(ucs2.len() as u16).to_le_bytes());
                        buf.extend_from_slice(&ucs2);
                    }
                }
                _ => {
                    // VARCHAR: encode as raw bytes
                    let bytes = s.as_bytes();
                    if bytes.len() > 0xFFFE {
                        let truncated = &bytes[..0xFFFE];
                        buf.extend_from_slice(&(truncated.len() as u16).to_le_bytes());
                        buf.extend_from_slice(truncated);
                    } else {
                        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                        buf.extend_from_slice(bytes);
                    }
                }
            }
        }
        Datum::Bytes(b) => {
            if b.len() > 0xFFFE {
                let truncated = &b[..0xFFFE];
                buf.extend_from_slice(&(truncated.len() as u16).to_le_bytes());
                buf.extend_from_slice(truncated);
            } else {
                buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
        Datum::Timestamp(millis) => {
            // DATETIME2N with scale=3: encode as date (3 bytes) + time (4 bytes)
            // Total = 7 bytes for scale 3
            encode_datetime2(buf, *millis);
        }
    }
}

/// Encode a NULL value for the given type.
fn encode_null(buf: &mut Vec<u8>, data_type: &DataType) {
    match data_type {
        DataType::Boolean
        | DataType::TinyInt
        | DataType::SmallInt
        | DataType::Int
        | DataType::BigInt
        | DataType::Float
        | DataType::Double => {
            // Fixed-length nullable types: length byte = 0 means NULL
            buf.push(0);
        }
        DataType::Varchar(_) | DataType::Blob => {
            // USHORT length = 0xFFFF means NULL
            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
        }
        DataType::Text => {
            // NVARCHAR NULL: USHORT length = 0xFFFF
            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
        }
        DataType::Timestamp => {
            // DATETIME2N NULL: length byte = 0
            buf.push(0);
        }
    }
}

/// Encode a timestamp (unix millis) as DATETIME2N with scale=3.
fn encode_datetime2(buf: &mut Vec<u8>, millis: i64) {
    // DATETIME2 is stored as:
    // - time: number of 10^(-scale) units since midnight
    // - date: number of days since 0001-01-01
    //
    // For scale=3: time unit = millisecond (10^-3 seconds)
    // Time stored in variable bytes: 3 bytes for scale 0-2, 4 bytes for scale 3-4, 5 for 5-7

    // Convert unix millis to components
    const MILLIS_PER_DAY: i64 = 86_400_000;

    // Days between 0001-01-01 and 1970-01-01 (Unix epoch)
    const EPOCH_DAYS: i64 = 719_162;

    let total_days = millis.div_euclid(MILLIS_PER_DAY);
    let day_millis = millis.rem_euclid(MILLIS_PER_DAY);

    let date_days = (total_days + EPOCH_DAYS) as u32;
    let time_units = day_millis as u32; // already in millis, scale=3

    // Length: 4 bytes time + 3 bytes date = 7 for scale 3
    buf.push(7);

    // Time (4 bytes LE for scale 3)
    buf.extend_from_slice(&time_units.to_le_bytes());

    // Date (3 bytes LE)
    buf.push(date_days as u8);
    buf.push((date_days >> 8) as u8);
    buf.push((date_days >> 16) as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_colmetadata_int() {
        let cols = vec![OutputColumn {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
        }];
        let encoded = encode_colmetadata(&cols);
        assert_eq!(encoded[0], TOKEN_COLMETADATA);
        // Count = 1
        assert_eq!(encoded[1], 1);
        assert_eq!(encoded[2], 0);
    }

    #[test]
    fn test_encode_row_int() {
        let cols = vec![OutputColumn {
            id: 0,
            name: "x".to_string(),
            data_type: DataType::Int,
            nullable: false,
        }];
        let values = vec![Datum::Int(42)];
        let encoded = encode_row(&values, &cols);
        assert_eq!(encoded[0], super::super::token::TOKEN_ROW);
        // Length byte = 4 (int32)
        assert_eq!(encoded[1], 4);
        // Value = 42 LE
        assert_eq!(
            i32::from_le_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]),
            42
        );
    }
}
