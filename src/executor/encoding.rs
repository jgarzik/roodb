//! Row encoding/decoding for storage
//!
//! Key format: `t:{table_name}:r:{row_id}`
//! Value format: null_bitmap + packed columns

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

/// Encode a table row key
///
/// Format: `t:{table_name}:r:{row_id}` where row_id is big-endian u64
pub fn encode_row_key(table: &str, row_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(table.len() + 16);
    key.extend_from_slice(b"t:");
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(b":r:");
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

/// Decode a table row key
///
/// Returns (table_name, row_id)
pub fn decode_row_key(key: &[u8]) -> ExecutorResult<(String, u64)> {
    // Parse "t:{table}:r:{row_id}"
    if key.len() < 12 || !key.starts_with(b"t:") {
        return Err(ExecutorError::Encoding("invalid key prefix".to_string()));
    }

    // Find ":r:" separator
    let sep = b":r:";
    let sep_pos = key
        .windows(3)
        .position(|w| w == sep)
        .ok_or_else(|| ExecutorError::Encoding("missing :r: separator".to_string()))?;

    let table = String::from_utf8(key[2..sep_pos].to_vec())
        .map_err(|_| ExecutorError::Encoding("invalid table name encoding".to_string()))?;

    let row_id_start = sep_pos + 3;
    if key.len() < row_id_start + 8 {
        return Err(ExecutorError::Encoding("row_id too short".to_string()));
    }

    let row_id_bytes: [u8; 8] = key[row_id_start..row_id_start + 8]
        .try_into()
        .map_err(|_| ExecutorError::Encoding("invalid row_id".to_string()))?;
    let row_id = u64::from_be_bytes(row_id_bytes);

    Ok((table, row_id))
}

/// Get the key prefix for scanning all rows in a table
pub fn table_key_prefix(table: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(table.len() + 5);
    prefix.extend_from_slice(b"t:");
    prefix.extend_from_slice(table.as_bytes());
    prefix.extend_from_slice(b":r:");
    prefix
}

/// Get the end key for scanning all rows in a table (exclusive)
pub fn table_key_end(table: &str) -> Vec<u8> {
    let mut end = Vec::with_capacity(table.len() + 5);
    end.extend_from_slice(b"t:");
    end.extend_from_slice(table.as_bytes());
    end.extend_from_slice(b":s"); // 's' > 'r' for exclusive end
    end
}

// Datum type tags
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_TIMESTAMP: u8 = 6;

/// Encode a row value
///
/// Format: [num_cols:u16][datum...]
/// Each datum: [tag:u8][data...]
pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();

    // Number of columns
    let num_cols = row.len() as u16;
    buf.extend_from_slice(&num_cols.to_le_bytes());

    // Encode each datum
    for datum in row.iter() {
        encode_datum(&mut buf, datum);
    }

    buf
}

/// Encode a single datum
fn encode_datum(buf: &mut Vec<u8>, datum: &Datum) {
    match datum {
        Datum::Null => {
            buf.push(TAG_NULL);
        }
        Datum::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(if *b { 1 } else { 0 });
        }
        Datum::Int(i) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Datum::Float(f) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Datum::String(s) => {
            buf.push(TAG_STRING);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Datum::Bytes(b) => {
            buf.push(TAG_BYTES);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Datum::Timestamp(t) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
    }
}

/// Decode a row value
pub fn decode_row(data: &[u8]) -> ExecutorResult<Row> {
    if data.len() < 2 {
        return Err(ExecutorError::Encoding("row data too short".to_string()));
    }

    let num_cols = u16::from_le_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        let (datum, consumed) = decode_datum(&data[offset..])?;
        values.push(datum);
        offset += consumed;
    }

    Ok(Row::new(values))
}

/// Decode a single datum, returns (datum, bytes_consumed)
fn decode_datum(data: &[u8]) -> ExecutorResult<(Datum, usize)> {
    if data.is_empty() {
        return Err(ExecutorError::Encoding(
            "unexpected end of datum".to_string(),
        ));
    }

    let tag = data[0];
    match tag {
        TAG_NULL => Ok((Datum::Null, 1)),

        TAG_BOOL => {
            if data.len() < 2 {
                return Err(ExecutorError::Encoding("bool data too short".to_string()));
            }
            Ok((Datum::Bool(data[1] != 0), 2))
        }

        TAG_INT => {
            if data.len() < 9 {
                return Err(ExecutorError::Encoding("int data too short".to_string()));
            }
            // Length validated above; slice is exactly 8 bytes
            let i = i64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .expect("length checked: slice is 8 bytes"),
            );
            Ok((Datum::Int(i), 9))
        }

        TAG_FLOAT => {
            if data.len() < 9 {
                return Err(ExecutorError::Encoding("float data too short".to_string()));
            }
            // Length validated above; slice is exactly 8 bytes
            let f = f64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .expect("length checked: slice is 8 bytes"),
            );
            Ok((Datum::Float(f), 9))
        }

        TAG_STRING => {
            if data.len() < 5 {
                return Err(ExecutorError::Encoding(
                    "string header too short".to_string(),
                ));
            }
            // Length validated above; slice is exactly 4 bytes
            let len = u32::from_le_bytes(
                data[1..5]
                    .try_into()
                    .expect("length checked: slice is 4 bytes"),
            ) as usize;
            if data.len() < 5 + len {
                return Err(ExecutorError::Encoding("string data too short".to_string()));
            }
            let s = String::from_utf8(data[5..5 + len].to_vec())
                .map_err(|_| ExecutorError::Encoding("invalid utf8 in string".to_string()))?;
            Ok((Datum::String(s), 5 + len))
        }

        TAG_BYTES => {
            if data.len() < 5 {
                return Err(ExecutorError::Encoding(
                    "bytes header too short".to_string(),
                ));
            }
            // Length validated above; slice is exactly 4 bytes
            let len = u32::from_le_bytes(
                data[1..5]
                    .try_into()
                    .expect("length checked: slice is 4 bytes"),
            ) as usize;
            if data.len() < 5 + len {
                return Err(ExecutorError::Encoding("bytes data too short".to_string()));
            }
            Ok((Datum::Bytes(data[5..5 + len].to_vec()), 5 + len))
        }

        TAG_TIMESTAMP => {
            if data.len() < 9 {
                return Err(ExecutorError::Encoding(
                    "timestamp data too short".to_string(),
                ));
            }
            // Length validated above; slice is exactly 8 bytes
            let t = i64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .expect("length checked: slice is 8 bytes"),
            );
            Ok((Datum::Timestamp(t), 9))
        }

        _ => Err(ExecutorError::Encoding(format!(
            "unknown datum tag: {}",
            tag
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_row_key() {
        let table = "users";
        let row_id = 12345u64;
        let key = encode_row_key(table, row_id);
        let (decoded_table, decoded_row_id) = decode_row_key(&key).unwrap();
        assert_eq!(decoded_table, table);
        assert_eq!(decoded_row_id, row_id);
    }

    #[test]
    fn test_table_key_prefix() {
        let prefix = table_key_prefix("users");
        assert_eq!(prefix, b"t:users:r:");
    }

    #[test]
    fn test_encode_decode_row() {
        let row = Row::new(vec![
            Datum::Null,
            Datum::Bool(true),
            Datum::Int(42),
            Datum::Float(2.5),
            Datum::String("hello".to_string()),
            Datum::Bytes(vec![1, 2, 3]),
            Datum::Timestamp(1234567890),
        ]);

        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();

        assert_eq!(decoded.len(), 7);
        assert!(decoded.get(0).unwrap().is_null());
        assert_eq!(decoded.get(1).unwrap().as_bool(), Some(true));
        assert_eq!(decoded.get(2).unwrap().as_int(), Some(42));
        assert_eq!(decoded.get(3).unwrap().as_float(), Some(2.5));
        assert_eq!(decoded.get(4).unwrap().as_str(), Some("hello"));
        assert_eq!(decoded.get(5).unwrap().as_bytes(), Some(&[1u8, 2, 3][..]));
        assert_eq!(decoded.get(6).unwrap().as_timestamp(), Some(1234567890));
    }

    #[test]
    fn test_encode_decode_empty_row() {
        let row = Row::empty();
        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_encode_decode_empty_string() {
        let row = Row::new(vec![Datum::String(String::new())]);
        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();
        assert_eq!(decoded.get(0).unwrap().as_str(), Some(""));
    }
}
