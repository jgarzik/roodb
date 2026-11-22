//! Table definitions as they are stored.
//!
//! A table is one record in `system.tables`, keyed by the table's name:
//! its definition and its indexes, whole. A change to a table is a change
//! of that one record, so it is applied whole or not at all, and the
//! catalog in memory is brought up to date from the record itself, with
//! nothing else to read and nothing that can fail.

use serde::{Deserialize, Serialize};

use crate::executor::encoding::{decode_row, encode_pk_key, encode_row};
use crate::executor::{Datum, Row};
use crate::raft::RowChange;

use super::system_tables::SYSTEM_TABLES;
use super::{IndexDef, TableDef};

/// What is stored of a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRecord {
    pub def: TableDef,
    pub indexes: Vec<IndexDef>,
}

/// The key of a table's record.
pub fn table_key(name: &str) -> Vec<u8> {
    encode_pk_key(SYSTEM_TABLES, &[Datum::String(name.to_string())])
}

/// The row a table's record is stored as: its name and the encoded record.
fn table_row(record: &TableRecord) -> Vec<u8> {
    let encoded = bincode::serialize(record).expect("a table definition serialises");
    encode_row(&Row::new(vec![
        Datum::String(record.def.name.clone()),
        Datum::Bytes(encoded),
    ]))
}

/// The record in a row of `system.tables` (the row as changes carry it,
/// without a version header). `None` if it is not one.
pub fn decode_table(row: &[u8]) -> Option<TableRecord> {
    let row = decode_row(row).ok()?;
    match row.values().get(1)? {
        Datum::Bytes(encoded) => bincode::deserialize(encoded).ok(),
        _ => None,
    }
}

/// The change that stores a new table.
pub fn create_table(record: &TableRecord) -> RowChange {
    RowChange::insert(
        SYSTEM_TABLES,
        table_key(&record.def.name),
        table_row(record),
    )
}

/// The change that replaces a table's record with `record`.
pub fn replace_table(record: &TableRecord) -> RowChange {
    RowChange::update(
        SYSTEM_TABLES,
        table_key(&record.def.name),
        table_row(record),
    )
}

/// The change that removes a table's record.
pub fn drop_table(name: &str) -> RowChange {
    RowChange::delete(SYSTEM_TABLES, table_key(name))
}

/// A row of a system table, as stored.
#[derive(Debug, Clone)]
pub struct StoredRow {
    /// The row's key.
    pub key: Vec<u8>,
    /// The row's bytes, without the version header.
    pub data: Vec<u8>,
    /// The row.
    pub row: Row,
}

/// Which columns of each system table make up its key.
fn key_columns(table: &str) -> &'static [usize] {
    use std::collections::HashMap;
    use std::sync::LazyLock;

    static KEYS: LazyLock<HashMap<String, Vec<usize>>> = LazyLock::new(|| {
        super::system_tables::bootstrap_system_tables()
            .into_iter()
            .map(|def| {
                // The primary key, or for a table that has none, its unique
                // constraint.
                let columns = def.primary_key().map(<[String]>::to_vec).or_else(|| {
                    def.constraints
                        .iter()
                        .find_map(|constraint| match constraint {
                            super::Constraint::Unique(columns) => Some(columns.clone()),
                            _ => None,
                        })
                });
                let indexes = columns
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|column| def.get_column_index(column))
                    .collect();
                (def.name, indexes)
            })
            .collect()
    });
    KEYS.get(table)
        .map(Vec::as_slice)
        .expect("a system table with a key")
}

/// The key a row of a system table is stored under: the table's key
/// columns, as the row has them.
pub fn row_key(table: &str, row: &Row) -> Vec<u8> {
    let values: Vec<Datum> = key_columns(table)
        .iter()
        .map(|&column| row.values().get(column).cloned().unwrap_or(Datum::Null))
        .collect();
    encode_pk_key(table, &values)
}

/// The change that adds `row` to a system table. Refused when applied if
/// a row with its key is there already.
pub fn insert_row(table: &str, row: &Row) -> RowChange {
    RowChange::insert(table, row_key(table, row), encode_row(row))
}

/// The change that stores `row` in a system table, in place of the row
/// with its key if there is one.
pub fn put_row(table: &str, row: &Row) -> RowChange {
    RowChange::update(table, row_key(table, row), encode_row(row))
}

/// The change that removes a stored row from a system table.
pub fn delete_row(table: &str, stored: &StoredRow) -> RowChange {
    RowChange::delete_with_value(table, stored.key.clone(), stored.data.clone())
}

/// A row as initialisation stores it, before there is a log: under its
/// key, with the header every stored row has (written by no transaction).
pub fn initial_row(table: &str, row: &Row) -> (Vec<u8>, Vec<u8>) {
    (
        row_key(table, row),
        crate::raft::encode_mvcc_row(0, false, &encode_row(row)),
    )
}

/// The live rows of a system table.
pub async fn scan_rows(
    storage: &dyn crate::storage::StorageEngine,
    table: &str,
) -> crate::storage::StorageResult<Vec<StoredRow>> {
    use crate::executor::encoding::{table_key_end, table_key_prefix};
    use crate::raft::{is_mvcc_tombstone, MVCC_HEADER_SIZE};

    let stored = storage
        .scan(Some(&table_key_prefix(table)), Some(&table_key_end(table)))
        .await?;
    let mut rows = Vec::with_capacity(stored.len());
    for (key, value) in stored {
        if value.len() < MVCC_HEADER_SIZE || is_mvcc_tombstone(&value) {
            continue;
        }
        let data = value[MVCC_HEADER_SIZE..].to_vec();
        if let Ok(row) = decode_row(&data) {
            rows.push(StoredRow { key, data, row });
        }
    }
    Ok(rows)
}

/// The table a key of `system.tables` belongs to.
pub fn table_of_key(key: &[u8]) -> Option<String> {
    let prefix = crate::executor::encoding::table_key_prefix(SYSTEM_TABLES);
    let part = key.strip_prefix(prefix.as_slice())?;
    crate::executor::key::decode_string(part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, Constraint, DataType};

    #[test]
    fn a_record_survives_being_stored() {
        let def = TableDef::new("orders")
            .column(ColumnDef::new("id", DataType::Int).nullable(false))
            .column(ColumnDef::new("note", DataType::Varchar(40)))
            .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));
        let record = TableRecord {
            def,
            indexes: vec![IndexDef::new("by_note", "orders", vec!["note".to_string()])],
        };
        let change = create_table(&record);
        assert_eq!(table_of_key(&change.key).as_deref(), Some("orders"));
        let back = decode_table(change.value.as_deref().unwrap()).unwrap();
        assert_eq!(back.def.name, "orders");
        assert_eq!(back.def.columns.len(), 2);
        assert_eq!(back.def.primary_key(), Some(&["id".to_string()][..]));
        assert_eq!(back.indexes.len(), 1);
        assert_eq!(back.indexes[0].name, "by_note");
        // A name with bytes the key encoding escapes comes back whole.
        assert_eq!(table_of_key(&table_key("a\0b")).as_deref(), Some("a\0b"));
    }
}
