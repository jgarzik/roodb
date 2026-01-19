//! Result set encoding
//!
//! Encodes query results into RooDB wire protocol format.

use crate::executor::row::Row;
use crate::planner::logical::OutputColumn;

use super::handshake::status_flags;
use super::packet::{encode_length_encoded_int, encode_length_encoded_string};
use super::types::{
    charset, datatype_column_length, datatype_flags, datatype_to_protocol, datum_to_text_bytes,
    ColumnType,
};

/// Column definition packet (Protocol 4.1)
#[derive(Debug)]
pub struct ColumnDefinition41 {
    /// Catalog (always "def")
    pub catalog: String,
    /// Schema (database name)
    pub schema: String,
    /// Virtual table name (may be alias)
    pub table: String,
    /// Original table name
    pub org_table: String,
    /// Virtual column name (may be alias)
    pub name: String,
    /// Original column name
    pub org_name: String,
    /// Character set (2 bytes)
    pub character_set: u16,
    /// Maximum column length
    pub column_length: u32,
    /// Column type
    pub column_type: ColumnType,
    /// Column flags
    pub flags: u16,
    /// Decimals (for numeric types)
    pub decimals: u8,
}

impl ColumnDefinition41 {
    /// Create a column definition from an OutputColumn
    pub fn from_output_column(col: &OutputColumn, table: &str, schema: &str) -> Self {
        ColumnDefinition41 {
            catalog: "def".to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            org_table: table.to_string(),
            name: col.name.clone(),
            org_name: col.name.clone(),
            character_set: charset::UTF8MB4_GENERAL_CI as u16,
            column_length: datatype_column_length(&col.data_type),
            column_type: datatype_to_protocol(&col.data_type),
            flags: datatype_flags(&col.data_type, col.nullable),
            decimals: 0,
        }
    }

    /// Encode the column definition packet
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);

        // catalog
        buf.extend_from_slice(&encode_length_encoded_string(&self.catalog));
        // schema
        buf.extend_from_slice(&encode_length_encoded_string(&self.schema));
        // table
        buf.extend_from_slice(&encode_length_encoded_string(&self.table));
        // org_table
        buf.extend_from_slice(&encode_length_encoded_string(&self.org_table));
        // name
        buf.extend_from_slice(&encode_length_encoded_string(&self.name));
        // org_name
        buf.extend_from_slice(&encode_length_encoded_string(&self.org_name));

        // Length of fixed-length fields (0x0c = 12)
        buf.push(0x0c);

        // character_set (2 bytes)
        buf.extend_from_slice(&self.character_set.to_le_bytes());

        // column_length (4 bytes)
        buf.extend_from_slice(&self.column_length.to_le_bytes());

        // column_type (1 byte)
        buf.push(self.column_type as u8);

        // flags (2 bytes)
        buf.extend_from_slice(&self.flags.to_le_bytes());

        // decimals (1 byte)
        buf.push(self.decimals);

        // filler (2 bytes)
        buf.extend_from_slice(&[0x00, 0x00]);

        buf
    }
}

/// Encode an OK packet
pub fn encode_ok_packet(
    affected_rows: u64,
    last_insert_id: u64,
    status: u16,
    warnings: u16,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);

    // Header (0x00 = OK)
    buf.push(0x00);

    // affected_rows (length-encoded int)
    buf.extend_from_slice(&encode_length_encoded_int(affected_rows));

    // last_insert_id (length-encoded int)
    buf.extend_from_slice(&encode_length_encoded_int(last_insert_id));

    // status_flags (2 bytes)
    buf.extend_from_slice(&status.to_le_bytes());

    // warnings (2 bytes)
    buf.extend_from_slice(&warnings.to_le_bytes());

    buf
}

/// Encode an OK packet as EOF replacement (for DEPRECATE_EOF)
///
/// When CLIENT_DEPRECATE_EOF is set, the final OK packet in a result set
/// uses 0xFE header instead of 0x00 to distinguish it from regular OK.
pub fn encode_eof_ok_packet(status: u16, warnings: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);

    // Header (0xFE = EOF/OK replacement)
    buf.push(0xfe);

    // affected_rows = 0 (length-encoded int)
    buf.push(0x00);

    // last_insert_id = 0 (length-encoded int)
    buf.push(0x00);

    // status_flags (2 bytes)
    buf.extend_from_slice(&status.to_le_bytes());

    // warnings (2 bytes)
    buf.extend_from_slice(&warnings.to_le_bytes());

    buf
}

/// Encode an ERR packet
pub fn encode_err_packet(error_code: u16, sql_state: &str, message: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + message.len());

    // Header (0xff = ERR)
    buf.push(0xff);

    // error_code (2 bytes)
    buf.extend_from_slice(&error_code.to_le_bytes());

    // sql_state_marker (#)
    buf.push(b'#');

    // sql_state (5 bytes, ASCII)
    let state_bytes = sql_state.as_bytes();
    if state_bytes.len() >= 5 {
        buf.extend_from_slice(&state_bytes[..5]);
    } else {
        buf.extend_from_slice(state_bytes);
        // Pad with zeros if needed
        let padding = 5 - state_bytes.len();
        buf.extend(std::iter::repeat_n(b'0', padding));
    }

    // error_message (string<EOF>)
    buf.extend_from_slice(message.as_bytes());

    buf
}

/// Encode an EOF packet (deprecated, but some clients expect it)
pub fn encode_eof_packet(warnings: u16, status: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5);

    // Header (0xfe = EOF)
    buf.push(0xfe);

    // warnings (2 bytes)
    buf.extend_from_slice(&warnings.to_le_bytes());

    // status_flags (2 bytes)
    buf.extend_from_slice(&status.to_le_bytes());

    buf
}

/// Encode a result set row (text protocol)
pub fn encode_text_row(row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();

    for datum in row.iter() {
        buf.extend_from_slice(&datum_to_text_bytes(datum));
    }

    buf
}

/// Encode column count packet
pub fn encode_column_count(count: u64) -> Vec<u8> {
    encode_length_encoded_int(count)
}

/// Default server status (autocommit on)
pub fn default_status() -> u16 {
    status_flags::SERVER_STATUS_AUTOCOMMIT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::executor::datum::Datum;

    #[test]
    fn test_column_definition_encode() {
        let col = OutputColumn {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
        };

        let def = ColumnDefinition41::from_output_column(&col, "users", "test");
        let encoded = def.encode();

        // Should start with "def" catalog
        assert!(encoded.starts_with(&[3, b'd', b'e', b'f']));
    }

    #[test]
    fn test_ok_packet() {
        let packet = encode_ok_packet(5, 10, status_flags::SERVER_STATUS_AUTOCOMMIT, 0);

        assert_eq!(packet[0], 0x00); // OK header
        assert_eq!(packet[1], 5); // affected_rows
        assert_eq!(packet[2], 10); // last_insert_id
    }

    #[test]
    fn test_err_packet() {
        let packet = encode_err_packet(1064, "42000", "Syntax error");

        assert_eq!(packet[0], 0xff); // ERR header
        assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1064);
        assert_eq!(packet[3], b'#'); // sql_state_marker
        assert_eq!(&packet[4..9], b"42000");
    }

    #[test]
    fn test_eof_packet() {
        let packet = encode_eof_packet(0, status_flags::SERVER_STATUS_AUTOCOMMIT);

        assert_eq!(packet[0], 0xfe); // EOF header
        assert_eq!(packet.len(), 5);
    }

    #[test]
    fn test_text_row() {
        let row = Row::new(vec![
            Datum::Int(42),
            Datum::String("hello".to_string()),
            Datum::Null,
        ]);

        let encoded = encode_text_row(&row);

        // Should contain length-encoded "42", length-encoded "hello", and NULL marker
        assert!(encoded.contains(&0xfb)); // NULL marker
    }
}
