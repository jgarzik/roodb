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
