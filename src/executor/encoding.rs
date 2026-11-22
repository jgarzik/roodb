//! Row encoding/decoding for storage
//!
//! Key format: `t`, table ID, index ID, then the key values (see `key`)
//! Value format: column count, then tagged values

use serde::{Deserialize, Serialize};

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

/// A table's number: what its rows are stored under. Given once, when the
/// table is created, and never to another table: a table that is renamed
/// keeps its rows, and a table created under a dropped table's name has
/// none of the old one's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableId(pub u64);

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An ID for a table that this process alone knows: a session's temporary
/// table, or a table of a catalog run without a log. From a range the
/// cluster's counter never reaches.
pub fn local_table_id() -> TableId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1 << 63);
    TableId(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Which of a table's key ranges: its rows (index 0), or one of its
/// secondary indexes.
pub type IndexId = u32;

/// The index every table has: the rows themselves, in key order.
pub const ROWS: IndexId = 0;

/// The first byte of every key that belongs to a table.
const TABLE_KEYS: u8 = b't';

/// Where the keys of one index of a table begin: `t`, the table's ID, the
/// index's ID (both big-endian, so that a table's keys are together and in
/// index order).
pub fn index_key_prefix(table: TableId, index: IndexId) -> Vec<u8> {
    let mut key = Vec::with_capacity(13 + 16);
    key.push(TABLE_KEYS);
    key.extend_from_slice(&table.0.to_be_bytes());
    key.extend_from_slice(&index.to_be_bytes());
    key
}

/// The first key after those of one index of a table.
pub fn index_key_end(table: TableId, index: IndexId) -> Vec<u8> {
    match index.checked_add(1) {
        Some(next) => index_key_prefix(table, next),
        None => table_range(table).1,
    }
}

/// Every key of a table, rows and indexes: `[start, end)`.
pub fn table_range(table: TableId) -> (Vec<u8>, Vec<u8>) {
    let mut start = vec![TABLE_KEYS];
    start.extend_from_slice(&table.0.to_be_bytes());
    let mut end = vec![TABLE_KEYS];
    match table.0.checked_add(1) {
        Some(next) => end.extend_from_slice(&next.to_be_bytes()),
        None => end[0] = TABLE_KEYS + 1,
    }
    (start, end)
}

/// The table a key belongs to, if it is a table's key.
pub fn table_of_row_key(key: &[u8]) -> Option<TableId> {
    match key {
        [TABLE_KEYS, id @ ..] if id.len() >= 8 => Some(TableId(u64::from_be_bytes(
            id[..8].try_into().expect("eight bytes"),
        ))),
        _ => None,
    }
}

/// The key of a row: the table's row prefix and the primary key values as
/// key parts (see `key`): bytes that sort as the values do. The values
/// must be in the key columns' types (a stored row's are; a looked-up value
/// goes through `key::for_lookup` first).
pub fn encode_pk_key(table: TableId, pk_values: &[Datum]) -> Vec<u8> {
    let mut key = index_key_prefix(table, ROWS);
    for val in pk_values {
        super::key::encode_part(&mut key, val, &super::key::KeyPart::PLAIN);
    }
    key
}

/// The key of a row of a table without a primary key: the table's row
/// prefix and the row's generated ID, big-endian.
pub fn encode_row_key(table: TableId, row_id: u64) -> Vec<u8> {
    let mut key = index_key_prefix(table, ROWS);
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

/// Where a table's rows begin.
pub fn table_key_prefix(table: TableId) -> Vec<u8> {
    index_key_prefix(table, ROWS)
}

/// The first key after a table's rows.
pub fn table_key_end(table: TableId) -> Vec<u8> {
    index_key_end(table, ROWS)
}

// Datum type tags
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_TIMESTAMP: u8 = 6;
const TAG_BIT: u8 = 7;
const TAG_UINT: u8 = 8;
const TAG_DECIMAL: u8 = 9;
const TAG_JSON: u8 = 10;
/// Written only by `encode_value`: the positional row format (`encode_row`)
/// writes a geometry as `TAG_BYTES`.
const TAG_GEOMETRY: u8 = 11;

/// Encode a row value
///
/// Format: [num_cols:u16][datum...]
/// Each datum: [tag:u8][data...]
pub fn encode_row(row: &Row) -> Vec<u8> {
    // Estimate capacity: 2 bytes header + ~10 bytes per datum average
    let mut buf = Vec::with_capacity(2 + row.len() * 10);

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
        Datum::Bytes(b) | Datum::Geometry(b) => {
            buf.push(TAG_BYTES);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Datum::UnsignedInt(u) => {
            buf.push(TAG_UINT);
            buf.extend_from_slice(&u.to_le_bytes());
        }
        Datum::Bit { value, width } => {
            buf.push(TAG_BIT);
            buf.push(*width);
            buf.extend_from_slice(&value.to_le_bytes());
        }
        Datum::Timestamp(t) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Datum::Decimal { value, scale } => {
            buf.push(TAG_DECIMAL);
            buf.push(*scale);
            buf.extend_from_slice(&value.to_le_bytes());
        }
        Datum::Json(v) => {
            buf.push(TAG_JSON);
            let s = serde_json::to_string(v).unwrap_or_default();
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

/// Encode a single datum such that `decode_datum` gives back the same
/// variant: as `encode_datum`, except that a geometry keeps its own tag
/// instead of being written as bytes.
pub(crate) fn encode_value(buf: &mut Vec<u8>, datum: &Datum) {
    match datum {
        Datum::Geometry(b) => {
            buf.push(TAG_GEOMETRY);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        other => encode_datum(buf, other),
    }
}

/// The number of bytes the datum at the start of `data` occupies, without
/// building it. The tag and the length are checked; what a string or a JSON
/// document holds is not.
pub(crate) fn skip_datum(data: &[u8]) -> ExecutorResult<usize> {
    let Some(&tag) = data.first() else {
        return Err(ExecutorError::Encoding(
            "unexpected end of datum".to_string(),
        ));
    };
    let size = match tag {
        TAG_NULL => 1,
        TAG_BOOL => 2,
        TAG_INT | TAG_FLOAT | TAG_UINT | TAG_TIMESTAMP => 9,
        TAG_BIT => 10,
        TAG_DECIMAL => 18,
        TAG_STRING | TAG_BYTES | TAG_JSON | TAG_GEOMETRY => {
            let Some(header) = data.get(1..5) else {
                return Err(ExecutorError::Encoding(
                    "length header too short".to_string(),
                ));
            };
            let len = u32::from_le_bytes(header.try_into().expect("slice is 4 bytes")) as usize;
            // Compared this way round so that the sum cannot overflow.
            if data.len() - 5 < len {
                return Err(ExecutorError::Encoding("datum data too short".to_string()));
            }
            return Ok(5 + len);
        }
        _ => {
            return Err(ExecutorError::Encoding(format!(
                "unknown datum tag: {}",
                tag
            )))
        }
    };
    if data.len() < size {
        return Err(ExecutorError::Encoding("datum data too short".to_string()));
    }
    Ok(size)
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
pub(crate) fn decode_datum(data: &[u8]) -> ExecutorResult<(Datum, usize)> {
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

        TAG_GEOMETRY => {
            let len = skip_datum(data)?;
            Ok((Datum::Geometry(data[5..len].to_vec()), len))
        }

        TAG_UINT => {
            if data.len() < 9 {
                return Err(ExecutorError::Encoding("uint data too short".to_string()));
            }
            let u = u64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .expect("length checked: slice is 8 bytes"),
            );
            Ok((Datum::UnsignedInt(u), 9))
        }

        TAG_BIT => {
            if data.len() < 10 {
                return Err(ExecutorError::Encoding("bit data too short".to_string()));
            }
            let width = data[1];
            let value = u64::from_le_bytes(
                data[2..10]
                    .try_into()
                    .expect("length checked: slice is 8 bytes"),
            );
            Ok((Datum::Bit { value, width }, 10))
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

        TAG_DECIMAL => {
            if data.len() < 18 {
                return Err(ExecutorError::Encoding(
                    "decimal data too short".to_string(),
                ));
            }
            let scale = data[1];
            let value = i128::from_le_bytes(
                data[2..18]
                    .try_into()
                    .expect("length checked: slice is 16 bytes"),
            );
            Ok((Datum::Decimal { value, scale }, 18))
        }

        TAG_JSON => {
            if data.len() < 5 {
                return Err(ExecutorError::Encoding("json header too short".to_string()));
            }
            let len = u32::from_le_bytes(
                data[1..5]
                    .try_into()
                    .expect("length checked: slice is 4 bytes"),
            ) as usize;
            if data.len() < 5 + len {
                return Err(ExecutorError::Encoding("json data too short".to_string()));
            }
            let s = std::str::from_utf8(&data[5..5 + len])
                .map_err(|_| ExecutorError::Encoding("invalid utf8 in json".to_string()))?;
            let v: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| ExecutorError::Encoding(format!("invalid json: {}", e)))?;
            Ok((Datum::Json(v), 5 + len))
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
    fn a_tables_keys_are_together_and_apart_from_other_tables() {
        let table = TableId(1024);
        let (start, end) = table_range(table);
        let rows = (table_key_prefix(table), table_key_end(table));
        let index = (index_key_prefix(table, 3), index_key_end(table, 3));
        // Rows, then indexes in order, all within the table's range.
        assert!(start <= rows.0 && rows.0 < rows.1);
        assert!(rows.1 <= index.0 && index.0 < index.1 && index.1 <= end);
        let row = encode_row_key(table, 12345);
        assert!(rows.0 < row && row < rows.1);
        let keyed = encode_pk_key(table, &[Datum::Int(-7)]);
        assert!(rows.0 < keyed && keyed < rows.1);
        // The next table's keys begin where this one's end, and a table
        // whose number is a prefix of another's in decimal is no neighbour.
        assert_eq!(end, table_range(TableId(1025)).0);
        assert!(table_range(TableId(10)).1 <= table_range(TableId(102)).0);
        // The last index and the last table still have an end.
        assert_eq!(index_key_end(table, u32::MAX), end);
        assert!(table_range(TableId(u64::MAX)).0 < table_range(TableId(u64::MAX)).1);
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

    #[test]
    fn test_encode_decode_bit() {
        // Test various BIT widths and values
        let test_cases = vec![
            Datum::Bit { value: 0, width: 1 },
            Datum::Bit { value: 1, width: 1 },
            Datum::Bit {
                value: 255,
                width: 8,
            },
            Datum::Bit {
                value: 42,
                width: 8,
            },
            Datum::Bit {
                value: 0,
                width: 64,
            },
            Datum::Bit {
                value: u64::MAX,
                width: 64,
            },
            Datum::Bit {
                value: 9999,
                width: 64,
            },
        ];

        for original in &test_cases {
            let row = Row::new(vec![original.clone()]);
            let encoded = encode_row(&row);
            let decoded = decode_row(&encoded).unwrap();
            let result = decoded.get(0).unwrap();
            match (original, result) {
                (
                    Datum::Bit {
                        value: ov,
                        width: ow,
                    },
                    Datum::Bit {
                        value: rv,
                        width: rw,
                    },
                ) => {
                    assert_eq!(ov, rv, "value mismatch for width={ow}");
                    assert_eq!(ow, rw, "width mismatch");
                }
                _ => panic!("expected Bit, got {:?}", result),
            }
        }
    }

    #[test]
    fn test_encode_decode_decimal() {
        let test_cases = vec![
            Datum::Decimal { value: 0, scale: 0 },
            Datum::Decimal {
                value: 12345,
                scale: 2,
            },
            Datum::Decimal {
                value: -99999,
                scale: 3,
            },
            Datum::Decimal {
                value: i128::MAX,
                scale: 0,
            },
            Datum::Decimal {
                value: i128::MIN,
                scale: 10,
            },
        ];

        for original in &test_cases {
            let row = Row::new(vec![original.clone()]);
            let encoded = encode_row(&row);
            let decoded = decode_row(&encoded).unwrap();
            let result = decoded.get(0).unwrap();
            match (original, result) {
                (
                    Datum::Decimal {
                        value: ov,
                        scale: os,
                    },
                    Datum::Decimal {
                        value: rv,
                        scale: rs,
                    },
                ) => {
                    assert_eq!(ov, rv, "value mismatch");
                    assert_eq!(os, rs, "scale mismatch");
                }
                _ => panic!("expected Decimal, got {:?}", result),
            }
        }
    }
}
