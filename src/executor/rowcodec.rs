//! Rows stored by column ID.
//!
//! A column has a numeric ID that it keeps for as long as it exists and that
//! no later column of the table is given. A stored row holds its values
//! beside the IDs of their columns, and is read against the table's columns
//! as they are now (`RowShape`). Changing the table's columns then changes
//! no stored row:
//!
//! - a value whose column ID the table no longer has is passed over (the
//!   column was dropped);
//! - a value goes to wherever its column now stands (columns were reordered);
//! - a column that the row holds nothing for reads as that column's
//!   `missing` value: `NULL`, or the default the column was added with;
//! - a column's name is not part of a row (a rename).
//!
//! # Bytes
//!
//! ```text
//! row    = FORMAT count value*        count values follow, and nothing after
//! FORMAT = 0x01
//! count  = varint
//! value  = id datum                   id: varint, above the previous value's
//! datum  = tag payload                as `encoding::encode_value` writes it
//! varint = a u32, seven bits a byte from the lowest, the top bit of a byte
//!          saying that another follows, in as few bytes as hold it
//! ```
//!
//! Values are in ascending order of column ID, each column at most once, so
//! a row has one encoding under a given shape. The count lets a row cut
//! short between two values be told from a whole one.
//!
//! # NULL, and a column added later
//!
//! A row can hold nothing for a column for two reasons: the value is `NULL`,
//! or the row was written before the column existed. Which of the two it
//! means is decided by the column, never by the row:
//!
//! - A column whose `missing` is `NULL` stores a `NULL` as nothing at all.
//!   Either reason reads as `NULL`, so they need not be told apart.
//! - A column whose `missing` is not `NULL` stores every value, a `NULL` as
//!   a datum with the NULL tag. For such a column, nothing stored always
//!   means that the row is older than the column, and reads as `missing`.
//!
//! This holds only while a column's `missing` stays what it was when the
//! first row was written under it: it is set when the column is added and
//! changed only by rewriting the table's rows.
//!
//! A reader accepts a stored `NULL` for any column.
//!
//! # Types
//!
//! A value is returned as the `Datum` it was stored as. Putting a column's
//! values in another type is done by rewriting the rows, not here.

use super::datum::Datum;
use super::encoding::{decode_datum, encode_value, skip_datum};
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;

/// The first byte of a row in this format.
const FORMAT: u8 = 0x01;

/// One of a table's current columns, as a row is read against it.
#[derive(Debug, Clone)]
pub struct ColumnSlot {
    /// The column's ID.
    pub id: u32,
    /// What a row that has no value stored for this column reads as: the
    /// default the column had when it was added to a table that already had
    /// rows, or `NULL`.
    pub missing: Datum,
}

impl ColumnSlot {
    /// A column that reads as `NULL` where a row holds nothing for it: any
    /// column the table was created with.
    pub fn new(id: u32) -> Self {
        ColumnSlot {
            id,
            missing: Datum::Null,
        }
    }

    /// A column added to a table that had rows, which read as `missing`.
    pub fn added(id: u32, missing: Datum) -> Self {
        ColumnSlot { id, missing }
    }
}

/// A table's current columns, in their current order: what rows are written
/// from and read into. Built once for a table's definition and used for
/// every row.
#[derive(Debug, Clone)]
pub struct RowShape {
    /// The columns, in the table's order.
    columns: Vec<ColumnSlot>,
    /// Each column's ID and position in `columns`, in ascending order of ID:
    /// the order a row's values are stored in.
    by_id: Vec<(u32, usize)>,
    /// The columns are in ascending order of ID already (no column was ever
    /// placed before an older one), so a row's values arrive in the order
    /// they are returned in.
    in_id_order: bool,
}

impl RowShape {
    /// The shape of a table with these columns, in this order. No two may
    /// have the same ID.
    pub fn new(columns: Vec<ColumnSlot>) -> ExecutorResult<Self> {
        let mut by_id: Vec<(u32, usize)> = columns
            .iter()
            .enumerate()
            .map(|(position, column)| (column.id, position))
            .collect();
        by_id.sort_unstable();
        if let Some(pair) = by_id.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(ExecutorError::Encoding(format!(
                "column ID {} is given to two columns",
                pair[0].0
            )));
        }
        let in_id_order = by_id
            .iter()
            .enumerate()
            .all(|(index, &(_, position))| index == position);
        Ok(RowShape {
            columns,
            by_id,
            in_id_order,
        })
    }

    /// The shape of a table with `count` columns numbered from 1 in their
    /// order, none added later: what a table has when it is created.
    pub fn numbered(count: usize) -> Self {
        let columns = (1..=count as u32).map(ColumnSlot::new).collect();
        RowShape::new(columns).expect("distinct column numbers")
    }

    /// The number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether there are no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// The columns, in the table's order.
    pub fn columns(&self) -> &[ColumnSlot] {
        &self.columns
    }
}

/// A table as it is stored: the ID its keys begin with, and the shape of
/// its rows. What an executor needs to read and write the table's rows.
#[derive(Debug, Clone)]
pub struct TableLayout {
    /// The ID the table's keys begin with.
    pub id: super::encoding::TableId,
    /// The shape of its rows.
    pub shape: std::sync::Arc<RowShape>,
}

impl TableLayout {
    /// The layout of the table `id`, whose rows have this shape.
    pub fn new(id: super::encoding::TableId, shape: RowShape) -> Self {
        TableLayout {
            id,
            shape: std::sync::Arc::new(shape),
        }
    }

    /// The layout of a table as it is created, with `columns` columns.
    pub fn numbered(id: super::encoding::TableId, columns: usize) -> Self {
        TableLayout::new(id, RowShape::numbered(columns))
    }

    /// The bytes one of the table's rows is stored as.
    pub fn encode(&self, row: &Row) -> ExecutorResult<Vec<u8>> {
        encode(&self.shape, row)
    }

    /// One of the table's rows, from the bytes it is stored as.
    pub fn decode(&self, bytes: &[u8]) -> ExecutorResult<Row> {
        decode(&self.shape, bytes)
    }
}

/// The bytes of `row`, whose values are in the shape's column order.
pub fn encode(shape: &RowShape, row: &Row) -> ExecutorResult<Vec<u8>> {
    let mut out = Vec::new();
    encode_into(&mut out, shape, row)?;
    Ok(out)
}

/// Append the bytes of `row`, whose values are in the shape's column order,
/// to `out`. An error (the row has not the shape's number of columns) leaves
/// `out` as it was.
pub fn encode_into(out: &mut Vec<u8>, shape: &RowShape, row: &Row) -> ExecutorResult<()> {
    let values = row.values();
    if values.len() != shape.len() {
        return Err(ExecutorError::Encoding(format!(
            "a row of {} values for a table of {} columns",
            values.len(),
            shape.len()
        )));
    }
    // A NULL is stored only where nothing stored would read as something
    // else (see the module's notes).
    let stored =
        |position: usize| !values[position].is_null() || !shape.columns[position].missing.is_null();
    let count = (0..values.len())
        .filter(|&position| stored(position))
        .count();
    out.reserve(2 + count * 10);
    out.push(FORMAT);
    write_varint(out, count as u32);
    for &(id, position) in &shape.by_id {
        if stored(position) {
            write_varint(out, id);
            encode_value(out, &values[position]);
        }
    }
    Ok(())
}

/// The row that `bytes` hold, as the shape's columns are now: a value for
/// each, in their order.
pub fn decode(shape: &RowShape, bytes: &[u8]) -> ExecutorResult<Row> {
    let columns = shape.len();
    // Values arrive in order of column ID. Where that is the columns' order
    // they are appended; otherwise each is put in its place.
    let mut values = if shape.in_id_order {
        Vec::with_capacity(columns)
    } else {
        vec![Datum::Null; columns]
    };
    let mut place = |position: usize, value: Datum| {
        if shape.in_id_order {
            values.push(value);
        } else {
            values[position] = value;
        }
    };
    let mut stored = Stored::open(bytes)?;
    // Both the stored values and `by_id` ascend by ID: one pass over each.
    let mut next = 0;
    while let Some(id) = stored.next_id()? {
        while next < columns && shape.by_id[next].0 < id {
            let position = shape.by_id[next].1;
            place(position, shape.columns[position].missing.clone());
            next += 1;
        }
        if next < columns && shape.by_id[next].0 == id {
            place(shape.by_id[next].1, stored.value()?);
            next += 1;
        } else {
            // A column the table no longer has.
            stored.skip()?;
        }
    }
    for &(_, position) in &shape.by_id[next..] {
        place(position, shape.columns[position].missing.clone());
    }
    Ok(Row::new(values))
}

/// The values of the columns at the positions `wanted` (positions in the
/// shape; in any order, and a position may be given twice), in the order of
/// `wanted`: what `decode` gives at those positions, without building the
/// other values.
///
/// The whole row is still walked, so bytes that are not a row are an error
/// here as in `decode`, with one exception: a value that is passed over is
/// checked for its length only, not for what it holds (a string's UTF-8, a
/// JSON document's text).
pub fn decode_columns(
    shape: &RowShape,
    bytes: &[u8],
    wanted: &[usize],
) -> ExecutorResult<Vec<Datum>> {
    let columns = shape.len();
    if let Some(&index) = wanted.iter().find(|&&position| position >= columns) {
        return Err(ExecutorError::ColumnIndexOutOfBounds {
            index,
            row_len: columns,
        });
    }
    let mut values = vec![Datum::Null; wanted.len()];
    // `wanted` is short where this is worth calling, so it is searched
    // rather than indexed: nothing is built per row but the result.
    let mut place = |position: usize, value: Datum| {
        let mut places = wanted
            .iter()
            .enumerate()
            .filter(|&(_, &wanted)| wanted == position)
            .map(|(index, _)| index);
        if let Some(mut index) = places.next() {
            for another in places {
                values[index] = value.clone();
                index = another;
            }
            values[index] = value;
        }
    };
    let missing = |position: usize, place: &mut dyn FnMut(usize, Datum)| {
        let missing = &shape.columns[position].missing;
        // The result starts out NULL.
        if !missing.is_null() && wanted.contains(&position) {
            place(position, missing.clone());
        }
    };
    let mut stored = Stored::open(bytes)?;
    let mut next = 0;
    while let Some(id) = stored.next_id()? {
        while next < columns && shape.by_id[next].0 < id {
            missing(shape.by_id[next].1, &mut place);
            next += 1;
        }
        if next < columns && shape.by_id[next].0 == id {
            let position = shape.by_id[next].1;
            if wanted.contains(&position) {
                place(position, stored.value()?);
            } else {
                stored.skip()?;
            }
            next += 1;
        } else {
            stored.skip()?;
        }
    }
    for &(_, position) in &shape.by_id[next..] {
        missing(position, &mut place);
    }
    Ok(values)
}

/// The values of a stored row, read one after another.
struct Stored<'a> {
    /// What has not been read.
    rest: &'a [u8],
    /// How many values are still to come.
    left: u32,
    /// The ID of the value read last.
    last_id: Option<u32>,
}

impl<'a> Stored<'a> {
    fn open(bytes: &'a [u8]) -> ExecutorResult<Self> {
        let Some((&format, rest)) = bytes.split_first() else {
            return Err(malformed("it is empty"));
        };
        if format != FORMAT {
            return Err(ExecutorError::Encoding(format!(
                "row: unknown format {format:#04x}"
            )));
        }
        let (left, size) = read_varint(rest)?;
        Ok(Stored {
            rest: &rest[size..],
            left,
            last_id: None,
        })
    }

    /// The column ID of the next value, which `value` or `skip` must then
    /// consume; `None` at the end of the row.
    fn next_id(&mut self) -> ExecutorResult<Option<u32>> {
        if self.left == 0 {
            if !self.rest.is_empty() {
                return Err(malformed("bytes follow its last value"));
            }
            return Ok(None);
        }
        if self.rest.is_empty() {
            return Err(malformed("it ends before its last value"));
        }
        let (id, size) = read_varint(self.rest)?;
        if self.last_id.is_some_and(|last| id <= last) {
            return Err(malformed("its column IDs do not ascend"));
        }
        self.rest = &self.rest[size..];
        self.left -= 1;
        self.last_id = Some(id);
        Ok(Some(id))
    }

    fn value(&mut self) -> ExecutorResult<Datum> {
        let (value, size) = decode_datum(self.rest)?;
        self.rest = &self.rest[size..];
        Ok(value)
    }

    fn skip(&mut self) -> ExecutorResult<()> {
        let size = skip_datum(self.rest)?;
        self.rest = &self.rest[size..];
        Ok(())
    }
}

fn malformed(what: &str) -> ExecutorError {
    ExecutorError::Encoding(format!("row: {what}"))
}

fn write_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// The varint at the start of `data` and the number of bytes it takes.
fn read_varint(data: &[u8]) -> ExecutorResult<(u32, usize)> {
    let mut value = 0u32;
    for (index, &byte) in data.iter().enumerate().take(5) {
        let bits = u32::from(byte & 0x7F);
        if index == 4 && bits > 0x0F {
            return Err(malformed("a number is too large"));
        }
        value |= bits << (7 * index);
        if byte & 0x80 == 0 {
            if index > 0 && byte == 0 {
                return Err(malformed("a number is not in its shortest form"));
            }
            return Ok((value, index + 1));
        }
    }
    Err(malformed(if data.len() < 5 {
        "it ends within a number"
    } else {
        "a number is too long"
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// A shape whose columns all read as NULL where nothing is stored.
    fn plain(ids: &[u32]) -> RowShape {
        RowShape::new(ids.iter().map(|&id| ColumnSlot::new(id)).collect()).unwrap()
    }

    fn text(value: &str) -> Datum {
        Datum::String(value.to_string())
    }

    /// The same variant holding the same value. (`Datum`'s own equality
    /// holds an integer equal to a float of its value, and so on.)
    fn identical(a: &Datum, b: &Datum) -> bool {
        match (a, b) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Bool(a), Datum::Bool(b)) => a == b,
            (Datum::Int(a), Datum::Int(b)) => a == b,
            (Datum::UnsignedInt(a), Datum::UnsignedInt(b)) => a == b,
            (Datum::Float(a), Datum::Float(b)) => a.to_bits() == b.to_bits(),
            (Datum::String(a), Datum::String(b)) => a == b,
            (Datum::Bytes(a), Datum::Bytes(b)) => a == b,
            (
                Datum::Bit {
                    value: a,
                    width: wa,
                },
                Datum::Bit {
                    value: b,
                    width: wb,
                },
            ) => a == b && wa == wb,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            (
                Datum::Decimal {
                    value: a,
                    scale: sa,
                },
                Datum::Decimal {
                    value: b,
                    scale: sb,
                },
            ) => a == b && sa == sb,
            (Datum::Geometry(a), Datum::Geometry(b)) => a == b,
            (Datum::Json(a), Datum::Json(b)) => a == b,
            _ => false,
        }
    }

    fn assert_identical(got: &[Datum], expected: &[Datum]) {
        assert_eq!(got.len(), expected.len(), "{got:?} for {expected:?}");
        for (got, expected) in got.iter().zip(expected) {
            assert!(identical(got, expected), "{got:?} for {expected:?}");
        }
    }

    /// Small deterministic generator (splitmix64).
    struct Prng(u64);

    impl Prng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, limit: usize) -> usize {
            (self.next() % limit as u64) as usize
        }

        fn bytes(&mut self, most: usize) -> Vec<u8> {
            let len = self.below(most + 1);
            (0..len).map(|_| self.next() as u8).collect()
        }

        fn string(&mut self, most: usize) -> String {
            const CHARS: [char; 8] = ['a', 'Z', '0', ' ', '\0', 'é', '語', '🦘'];
            let len = self.below(most + 1);
            (0..len).map(|_| CHARS[self.below(CHARS.len())]).collect()
        }

        fn json(&mut self, depth: usize) -> serde_json::Value {
            use serde_json::Value;
            match self.below(if depth == 0 { 4 } else { 6 }) {
                0 => Value::Null,
                1 => Value::Bool(self.next() & 1 == 1),
                2 => Value::from(self.next() as i64),
                3 => Value::String(self.string(6)),
                4 => Value::Array((0..self.below(3)).map(|_| self.json(depth - 1)).collect()),
                _ => Value::Object(
                    (0..self.below(3))
                        .map(|_| (self.string(4), self.json(depth - 1)))
                        .collect(),
                ),
            }
        }

        /// A value of any type but NULL.
        fn value(&mut self) -> Datum {
            match self.below(11) {
                0 => Datum::Bool(self.next() & 1 == 1),
                1 => Datum::Int(match self.below(4) {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    2 => self.below(200) as i64 - 100,
                    _ => self.next() as i64,
                }),
                2 => Datum::UnsignedInt(match self.below(3) {
                    0 => u64::MAX,
                    1 => 0,
                    _ => self.next(),
                }),
                // Any bits: NaNs of every kind, infinities, both zeros.
                3 => Datum::Float(f64::from_bits(self.next())),
                4 => Datum::String(self.string(24)),
                5 => Datum::Bytes(self.bytes(24)),
                6 => Datum::Bit {
                    value: self.next(),
                    width: 1 + self.below(64) as u8,
                },
                7 => Datum::Timestamp(self.next() as i64),
                8 => Datum::Decimal {
                    value: (i128::from(self.next() as i64) << 64) | i128::from(self.next()),
                    scale: self.below(39) as u8,
                },
                9 => Datum::Geometry(self.bytes(24)),
                _ => Datum::Json(self.json(2)),
            }
        }

        /// A value of any type, NULL one time in four.
        fn nullable(&mut self) -> Datum {
            if self.below(4) == 0 {
                Datum::Null
            } else {
                self.value()
            }
        }

        fn shuffle<T>(&mut self, items: &mut [T]) {
            for index in (1..items.len()).rev() {
                items.swap(index, self.below(index + 1));
            }
        }
    }

    #[test]
    fn every_type_comes_back_as_it_went_in() {
        let values = vec![
            Datum::Bool(true),
            Datum::Bool(false),
            Datum::Int(0),
            Datum::Int(-1),
            Datum::Int(i64::MIN),
            Datum::Int(i64::MAX),
            Datum::UnsignedInt(0),
            Datum::UnsignedInt(u64::MAX),
            Datum::Float(1.5),
            Datum::Float(0.0),
            Datum::Float(-0.0),
            Datum::Float(f64::NAN),
            Datum::Float(-f64::NAN),
            Datum::Float(f64::from_bits(0x7FF0_0000_0000_0001)),
            Datum::Float(f64::INFINITY),
            Datum::Float(f64::NEG_INFINITY),
            Datum::Float(f64::MIN_POSITIVE),
            text(""),
            text("hello"),
            text("nul \0 within, é, 語, 🦘"),
            Datum::Bytes(vec![]),
            Datum::Bytes(vec![0, 1, 0xFF, 0]),
            Datum::Bit { value: 1, width: 1 },
            Datum::Bit {
                value: u64::MAX,
                width: 64,
            },
            Datum::Bit { value: 5, width: 8 },
            Datum::Timestamp(0),
            Datum::Timestamp(-1),
            Datum::Timestamp(1_700_000_000_000),
            Datum::Decimal { value: 0, scale: 0 },
            Datum::Decimal {
                value: 12_345,
                scale: 2,
            },
            Datum::Decimal {
                value: -99_999,
                scale: 3,
            },
            Datum::Decimal {
                value: 100,
                scale: 2,
            },
            Datum::Decimal {
                value: i128::MIN,
                scale: 38,
            },
            Datum::Decimal {
                value: i128::MAX,
                scale: 0,
            },
            Datum::Geometry(vec![]),
            Datum::Geometry(vec![1, 1, 0, 0, 0, 9, 9]),
            Datum::Json(serde_json::Value::Null),
            Datum::Json(serde_json::json!({"a": [1, "two", null, {"b": false}], "c": ""})),
            Datum::Json(serde_json::json!("just a string")),
        ];
        // Each alone, then all in one row.
        for value in &values {
            let shape = plain(&[9]);
            let bytes = encode(&shape, &Row::new(vec![value.clone()])).unwrap();
            let row = decode(&shape, &bytes).unwrap();
            assert_identical(row.values(), std::slice::from_ref(value));
        }
        let ids: Vec<u32> = (1..=values.len() as u32).collect();
        let shape = plain(&ids);
        let bytes = encode(&shape, &Row::new(values.clone())).unwrap();
        assert_identical(decode(&shape, &bytes).unwrap().values(), &values);
    }

    #[test]
    fn the_bytes_are_as_documented() {
        let shape = plain(&[1, 2, 300]);
        let row = Row::new(vec![Datum::Int(7), Datum::Null, Datum::Bool(true)]);
        let bytes = encode(&shape, &row).unwrap();
        let mut expected = vec![0x01, 2];
        expected.extend([1, 2]); // column 1, an integer
        expected.extend(7i64.to_le_bytes());
        expected.extend([0xAC, 0x02, 1, 1]); // column 300, a boolean
        assert_eq!(bytes, expected);
        // A table with no columns, or a row of NULLs.
        assert_eq!(encode(&plain(&[]), &Row::empty()).unwrap(), [0x01, 0]);
        assert_eq!(
            encode(&plain(&[4, 5]), &Row::new(vec![Datum::Null; 2])).unwrap(),
            [0x01, 0]
        );
    }

    #[test]
    fn a_row_has_one_encoding() {
        // Whatever order the table's columns are in, the bytes are in order
        // of column ID.
        let values = [Datum::Int(1), text("two"), Datum::Float(3.0)];
        let first = encode(&plain(&[1, 2, 3]), &Row::new(values.to_vec())).unwrap();
        let reordered = Row::new(vec![
            values[2].clone(),
            values[0].clone(),
            values[1].clone(),
        ]);
        let second = encode(&plain(&[3, 1, 2]), &reordered).unwrap();
        assert_eq!(first, second);
        // Written again from what was read, the bytes are the same.
        let shape = plain(&[3, 1, 2]);
        let read = decode(&shape, &second).unwrap();
        assert_eq!(encode(&shape, &read).unwrap(), second);
    }

    #[test]
    fn a_null_takes_no_room() {
        let shape = plain(&[1, 2, 3]);
        let full = Row::new(vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
        let with_null = Row::new(vec![Datum::Int(1), Datum::Null, Datum::Int(3)]);
        let full_bytes = encode(&shape, &full).unwrap();
        let null_bytes = encode(&shape, &with_null).unwrap();
        // The ID, the tag and the integer are all gone.
        assert_eq!(null_bytes.len(), full_bytes.len() - 10);
        let read = decode(&shape, &null_bytes).unwrap();
        assert_identical(read.values(), with_null.values());
    }

    #[test]
    fn encode_into_appends() {
        let shape = plain(&[1]);
        let row = Row::new(vec![Datum::Int(5)]);
        let mut buffer = b"header".to_vec();
        encode_into(&mut buffer, &shape, &row).unwrap();
        assert_eq!(&buffer[..6], b"header");
        assert_eq!(buffer[6..], encode(&shape, &row).unwrap());
        // Reused for the next row.
        buffer.clear();
        encode_into(&mut buffer, &shape, &row).unwrap();
        assert_eq!(buffer, encode(&shape, &row).unwrap());
    }

    #[test]
    fn a_row_of_the_wrong_width_is_refused() {
        let shape = plain(&[1, 2]);
        let mut buffer = vec![0xEE];
        let short = Row::new(vec![Datum::Int(1)]);
        assert!(matches!(
            encode_into(&mut buffer, &shape, &short),
            Err(ExecutorError::Encoding(_))
        ));
        assert_eq!(buffer, [0xEE]);
        assert!(encode(&shape, &Row::new(vec![Datum::Null; 3])).is_err());
    }

    #[test]
    fn two_columns_cannot_share_an_id() {
        let columns = vec![ColumnSlot::new(1), ColumnSlot::new(2), ColumnSlot::new(1)];
        assert!(matches!(
            RowShape::new(columns),
            Err(ExecutorError::Encoding(_))
        ));
    }

    #[test]
    fn a_dropped_columns_value_is_passed_over() {
        let written = plain(&[1, 2, 3]);
        let row = Row::new(vec![Datum::Int(10), text("dropped"), Datum::Int(30)]);
        let bytes = encode(&written, &row).unwrap();
        // The positional format gives the dropped column's value to the
        // column after it; here each value stays with its column.
        let read = decode(&plain(&[1, 3]), &bytes).unwrap();
        assert_identical(read.values(), &[Datum::Int(10), Datum::Int(30)]);
        // The first, the last, and all of them.
        let read = decode(&plain(&[2, 3]), &bytes).unwrap();
        assert_identical(read.values(), &[text("dropped"), Datum::Int(30)]);
        let read = decode(&plain(&[1, 2]), &bytes).unwrap();
        assert_identical(read.values(), &[Datum::Int(10), text("dropped")]);
        assert!(decode(&plain(&[]), &bytes).unwrap().is_empty());
    }

    #[test]
    fn an_added_column_reads_as_the_default_it_was_added_with() {
        let before = plain(&[1, 2]);
        let old = encode(&before, &Row::new(vec![Datum::Int(1), text("a")])).unwrap();
        let after = RowShape::new(vec![
            ColumnSlot::new(1),
            ColumnSlot::new(2),
            ColumnSlot::added(7, Datum::Int(42)),
        ])
        .unwrap();
        let read = decode(&after, &old).unwrap();
        assert_identical(read.values(), &[Datum::Int(1), text("a"), Datum::Int(42)]);

        // A NULL written to the new column is a NULL, not the default.
        let with_null = Row::new(vec![Datum::Int(2), text("b"), Datum::Null]);
        let bytes = encode(&after, &with_null).unwrap();
        assert_identical(decode(&after, &bytes).unwrap().values(), with_null.values());
        // It is stored: the column's ID and the NULL tag.
        assert_eq!(bytes[bytes.len() - 2..], [7, 0]);
        // The old row updated to NULL likewise.
        let mut updated = read;
        updated.set(2, Datum::Null).unwrap();
        let bytes = encode(&after, &updated).unwrap();
        assert!(decode(&after, &bytes).unwrap().get(2).unwrap().is_null());

        // A value written to the new column is that value.
        let with_value = Row::new(vec![Datum::Int(3), Datum::Null, Datum::Int(-1)]);
        let bytes = encode(&after, &with_value).unwrap();
        assert_identical(
            decode(&after, &bytes).unwrap().values(),
            with_value.values(),
        );

        // A column added without a default reads as NULL in old rows, and
        // one added in front of the others as its default.
        let after = RowShape::new(vec![
            ColumnSlot::added(9, text("first")),
            ColumnSlot::new(1),
            ColumnSlot::new(8),
            ColumnSlot::new(2),
        ])
        .unwrap();
        let read = decode(&after, &old).unwrap();
        assert_identical(
            read.values(),
            &[text("first"), Datum::Int(1), Datum::Null, text("a")],
        );
    }

    #[test]
    fn a_stored_null_reads_as_null_in_any_column() {
        // Column 1, the NULL tag; column 2, the NULL tag.
        let bytes = [0x01, 2, 1, 0, 2, 0];
        let shape = RowShape::new(vec![
            ColumnSlot::new(1),
            ColumnSlot::added(2, Datum::Int(5)),
        ])
        .unwrap();
        let read = decode(&shape, &bytes).unwrap();
        assert_identical(read.values(), &[Datum::Null, Datum::Null]);
    }

    #[test]
    fn reordered_columns_take_their_values_with_them() {
        let row = Row::new(vec![Datum::Int(1), text("two"), Datum::Float(3.0)]);
        let bytes = encode(&plain(&[1, 2, 3]), &row).unwrap();
        let read = decode(&plain(&[3, 1, 2]), &bytes).unwrap();
        assert_identical(
            read.values(),
            &[Datum::Float(3.0), Datum::Int(1), text("two")],
        );
        // With a NULL among them.
        let row = Row::new(vec![Datum::Null, text("two"), Datum::Float(3.0)]);
        let bytes = encode(&plain(&[1, 2, 3]), &row).unwrap();
        let read = decode(&plain(&[2, 3, 1]), &bytes).unwrap();
        assert_identical(
            read.values(),
            &[text("two"), Datum::Float(3.0), Datum::Null],
        );
    }

    #[test]
    fn a_rename_changes_nothing() {
        // A shape is made of a table's column IDs, never of their names.
        struct Column {
            name: &'static str,
            id: u32,
        }
        let shape_of = |columns: &[Column]| {
            assert!(columns.iter().all(|column| !column.name.is_empty()));
            plain(&columns.iter().map(|column| column.id).collect::<Vec<_>>())
        };
        let before = [Column { name: "a", id: 1 }, Column { name: "b", id: 2 }];
        let after = [
            Column { name: "a", id: 1 },
            Column {
                name: "renamed",
                id: 2,
            },
        ];
        let row = Row::new(vec![Datum::Int(1), text("kept")]);
        let bytes = encode(&shape_of(&before), &row).unwrap();
        assert_eq!(encode(&shape_of(&after), &row).unwrap(), bytes);
        let read = decode(&shape_of(&after), &bytes).unwrap();
        assert_identical(read.values(), row.values());
    }

    #[test]
    fn a_value_keeps_the_type_it_was_stored_with() {
        // The column may since have become a string column; the row still
        // holds an integer.
        let bytes = encode(&plain(&[1]), &Row::new(vec![Datum::Int(5)])).unwrap();
        let read = decode(&plain(&[1]), &bytes).unwrap();
        assert!(matches!(read.get(0).unwrap(), Datum::Int(5)));
    }

    #[test]
    fn chosen_columns_are_what_decode_gives() {
        let written = plain(&[1, 2, 3, 4]);
        let row = Row::new(vec![
            Datum::Int(1),
            text("two"),
            Datum::Null,
            Datum::Bool(true),
        ]);
        let bytes = encode(&written, &row).unwrap();
        let shape = RowShape::new(vec![
            ColumnSlot::new(4),
            ColumnSlot::added(6, text("default")),
            ColumnSlot::new(1),
            ColumnSlot::new(3),
        ])
        .unwrap();
        let all = decode(&shape, &bytes).unwrap();
        assert_identical(
            all.values(),
            &[
                Datum::Bool(true),
                text("default"),
                Datum::Int(1),
                Datum::Null,
            ],
        );
        let some = decode_columns(&shape, &bytes, &[2, 1, 2, 3, 0]).unwrap();
        assert_identical(
            &some,
            &[
                Datum::Int(1),
                text("default"),
                Datum::Int(1),
                Datum::Null,
                Datum::Bool(true),
            ],
        );
        assert!(decode_columns(&shape, &bytes, &[]).unwrap().is_empty());
        assert!(matches!(
            decode_columns(&shape, &bytes, &[0, 4]),
            Err(ExecutorError::ColumnIndexOutOfBounds {
                index: 4,
                row_len: 4
            })
        ));
    }

    #[test]
    fn a_column_that_is_not_wanted_is_not_built() {
        // Column 2 holds bytes that are no string: reading it fails,
        // reading past it does not.
        let mut bytes = vec![0x01, 2, 1, 2];
        bytes.extend(7i64.to_le_bytes());
        bytes.extend([2, 4, 2, 0, 0, 0, 0xFF, 0xFE]);
        let shape = plain(&[1, 2]);
        assert!(decode(&shape, &bytes).is_err());
        assert!(decode_columns(&shape, &bytes, &[1]).is_err());
        let first = decode_columns(&shape, &bytes, &[0]).unwrap();
        assert_identical(&first, &[Datum::Int(7)]);
    }

    fn assert_malformed(bytes: &[u8]) {
        let shape = plain(&[1, 2, 3]);
        for result in [
            decode(&shape, bytes).map(Row::into_values),
            decode_columns(&shape, bytes, &[0, 1, 2]),
            decode_columns(&shape, bytes, &[]),
        ] {
            assert!(
                matches!(result, Err(ExecutorError::Encoding(_))),
                "{bytes:02x?} gave {result:?}"
            );
        }
    }

    #[test]
    fn bytes_that_are_no_row_are_an_error() {
        // Nothing at all; an unknown format; values behind a two-byte
        // count, as rows were once stored.
        assert_malformed(&[]);
        assert_malformed(&[0x00, 0]);
        assert_malformed(&[0x02, 0]);
        let mut by_position = vec![3, 0];
        for _ in 0..3 {
            encode_value(&mut by_position, &Datum::Int(1));
        }
        assert_malformed(&by_position);
        // No count; a count that does not end; one that is too long, too
        // large, or not in its shortest form.
        assert_malformed(&[0x01]);
        assert_malformed(&[0x01, 0x80]);
        assert_malformed(&[0x01, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
        assert_malformed(&[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F]);
        assert_malformed(&[0x01, 0x80, 0x00]);
        // Fewer values than the count; more bytes than the count.
        assert_malformed(&[0x01, 1]);
        assert_malformed(&[0x01, 2, 1, 1, 1]);
        assert_malformed(&[0x01, 0, 1, 1, 1]);
        assert_malformed(&[0x01, 1, 1, 1, 1, 0]);
        // A column ID that does not end, or is not in its shortest form.
        assert_malformed(&[0x01, 1, 0x81]);
        assert_malformed(&[0x01, 1, 0x81, 0x00, 1, 1]);
        // No value after the ID; an unknown tag; a value cut short.
        assert_malformed(&[0x01, 1, 1]);
        assert_malformed(&[0x01, 1, 1, 99]);
        assert_malformed(&[0x01, 1, 9, 99]);
        assert_malformed(&[0x01, 1, 1, 2, 0, 0, 0]);
        assert_malformed(&[0x01, 1, 1, 4, 5, 0, 0, 0, b'a']);
        assert_malformed(&[0x01, 1, 9, 4, 5, 0, 0, 0, b'a']);
        assert_malformed(&[0x01, 1, 9, 4, 0xFF, 0xFF, 0xFF, 0xFF, b'a']);
        // A column twice; columns out of order (in the table or not).
        assert_malformed(&[0x01, 2, 1, 1, 1, 1, 1, 0]);
        assert_malformed(&[0x01, 2, 2, 1, 1, 1, 1, 0]);
        assert_malformed(&[0x01, 2, 9, 1, 1, 8, 1, 0]);
        // The largest column ID is a column ID.
        let top = [0x01, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 1, 1];
        let read = decode(&plain(&[u32::MAX]), &top).unwrap();
        assert_identical(read.values(), &[Datum::Bool(true)]);
        assert_eq!(encode(&plain(&[u32::MAX]), &read).unwrap(), top);
    }

    /// A random table and a row of it.
    fn random_row(rng: &mut Prng) -> (RowShape, Row) {
        let mut ids: Vec<u32> = (0..rng.below(8))
            .map(|index| index as u32 * 50 + rng.below(50) as u32)
            .collect();
        rng.shuffle(&mut ids);
        let columns: Vec<ColumnSlot> = ids
            .into_iter()
            .map(|id| ColumnSlot::added(id, rng.nullable()))
            .collect();
        let row = Row::new(columns.iter().map(|_| rng.nullable()).collect());
        (RowShape::new(columns).unwrap(), row)
    }

    #[test]
    fn damaged_bytes_are_an_error_and_never_a_panic() {
        let mut rng = Prng(0x00C0_FFEE);
        for _ in 0..300 {
            let (shape, row) = random_row(&mut rng);
            let bytes = encode(&shape, &row).unwrap();
            let all: Vec<usize> = (0..shape.len()).collect();
            assert_identical(decode(&shape, &bytes).unwrap().values(), row.values());

            // Cut short anywhere, it is not a row.
            for len in 0..bytes.len() {
                assert!(decode(&shape, &bytes[..len]).is_err(), "cut to {len}");
                assert!(decode_columns(&shape, &bytes[..len], &all).is_err());
                assert!(decode_columns(&shape, &bytes[..len], &[]).is_err());
            }
            // With bytes after it, neither.
            let mut longer = bytes.clone();
            longer.push(rng.next() as u8);
            assert!(decode(&shape, &longer).is_err());

            // With bytes changed it is a row or it is not; whichever, both
            // readers say the same when all its columns are asked for.
            for _ in 0..40 {
                let mut damaged = bytes.clone();
                for _ in 0..1 + rng.below(3) {
                    let at = rng.below(damaged.len());
                    damaged[at] = match rng.below(3) {
                        0 => rng.next() as u8,
                        1 => damaged[at] ^ (1 << rng.below(8)),
                        _ => 0xFF,
                    };
                }
                let whole = decode(&shape, &damaged);
                let chosen = decode_columns(&shape, &damaged, &all);
                match (whole, chosen) {
                    (Ok(whole), Ok(chosen)) => assert_identical(whole.values(), &chosen),
                    (Err(ExecutorError::Encoding(_)), Err(ExecutorError::Encoding(_))) => {}
                    (whole, chosen) => panic!("{damaged:02x?}: {whole:?} but {chosen:?}"),
                }
                // Asked for nothing, the reader builds nothing; it must
                // still not panic.
                let _ = decode_columns(&shape, &damaged, &[]);
            }
        }
        // Bytes of no origin, with and without the format byte.
        let shape = plain(&[0, 1, 2, 3, 200, 70_000]);
        for _ in 0..20_000 {
            let mut noise = rng.bytes(40);
            if rng.below(4) != 0 {
                noise.insert(0, FORMAT);
            }
            let whole = decode(&shape, &noise);
            let chosen = decode_columns(&shape, &noise, &[0, 1, 2, 3, 4, 5]);
            assert_eq!(whole.is_ok(), chosen.is_ok(), "{noise:02x?}");
            let _ = decode_columns(&shape, &noise, &[5, 1]);
        }
    }

    #[test]
    fn rows_survive_any_sequence_of_schema_changes() {
        // The model: a row is what was written for each column ID. Read
        // against a table, a column has what was written for its ID, or
        // else what the column gives a row that has nothing.
        struct Written {
            bytes: Vec<u8>,
            by_id: HashMap<u32, Datum>,
        }
        let expected = |columns: &[ColumnSlot], written: &Written| -> Vec<Datum> {
            columns
                .iter()
                .map(|column| {
                    written
                        .by_id
                        .get(&column.id)
                        .unwrap_or(&column.missing)
                        .clone()
                })
                .collect()
        };

        for seed in 0..300u64 {
            let mut rng = Prng(seed);
            let mut next_id = 1u32;
            let mut new_id = |rng: &mut Prng| {
                // IDs of one byte and of several.
                next_id += 1 + rng.below(3) as u32 * 60;
                next_id
            };
            // The columns a table is created with read as NULL.
            let mut columns: Vec<ColumnSlot> = (0..rng.below(7))
                .map(|_| ColumnSlot::new(new_id(&mut rng)))
                .collect();
            let mut rows: Vec<Written> = Vec::new();

            for _ in 0..14 {
                let shape = RowShape::new(columns.clone()).unwrap();

                // New rows.
                for _ in 0..3 {
                    let values: Vec<Datum> = columns.iter().map(|_| rng.nullable()).collect();
                    let by_id = columns
                        .iter()
                        .zip(&values)
                        .map(|(column, value)| (column.id, value.clone()))
                        .collect();
                    let bytes = encode(&shape, &Row::new(values)).unwrap();
                    rows.push(Written { bytes, by_id });
                }

                // Every row, however old, reads as the model says.
                for written in &mut rows {
                    let expected = expected(&columns, written);
                    let read = decode(&shape, &written.bytes).unwrap();
                    assert_identical(read.values(), &expected);

                    let wanted: Vec<usize> = (0..rng.below(6))
                        .filter(|_| !columns.is_empty())
                        .map(|_| rng.below(columns.len()))
                        .collect();
                    let chosen = decode_columns(&shape, &written.bytes, &wanted).unwrap();
                    let of_whole: Vec<Datum> = wanted
                        .iter()
                        .map(|&position| expected[position].clone())
                        .collect();
                    assert_identical(&chosen, &of_whole);

                    // Some rows are written again as the table is now (an
                    // UPDATE that changes nothing): they read the same, and
                    // hold nothing of dropped columns any more.
                    if rng.below(3) == 0 {
                        let bytes = encode(&shape, &read).unwrap();
                        assert_eq!(bytes, encode(&shape, &read).unwrap());
                        let again = decode(&shape, &bytes).unwrap();
                        assert_identical(again.values(), &expected);
                        written.bytes = bytes;
                        written.by_id = columns
                            .iter()
                            .zip(expected)
                            .map(|(column, value)| (column.id, value))
                            .collect();
                    }
                }

                // Change the table.
                match rng.below(4) {
                    0 if !columns.is_empty() => {
                        columns.remove(rng.below(columns.len()));
                    }
                    1 => rng.shuffle(&mut columns),
                    _ => {
                        let column = ColumnSlot::added(new_id(&mut rng), rng.nullable());
                        columns.insert(rng.below(columns.len() + 1), column);
                    }
                }
            }
        }
    }

    #[test]
    fn the_size_of_a_row() {
        // sysbench's table: id INT, k INT, c CHAR(120), pad CHAR(60). The
        // format byte, the count, and for each value its column's number
        // before it.
        let row = Row::new(vec![
            Datum::Int(1),
            Datum::Int(50_000),
            Datum::String("c".repeat(120)),
            Datum::String("p".repeat(60)),
        ]);
        let values: usize = row
            .values()
            .iter()
            .map(|value| {
                let mut bytes = Vec::new();
                encode_value(&mut bytes, value);
                bytes.len()
            })
            .sum();
        let by_id = encode(&plain(&[1, 2, 3, 4]), &row).unwrap().len();
        assert_eq!(by_id, 2 + 4 + values);
        assert_eq!(by_id, 214);
    }
}
