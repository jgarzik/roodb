//! Protocol type mapping
//!
//! Maps between RooDB DataType/Datum and wire protocol types.

use crate::catalog::DataType;
use crate::executor::datum::Datum;

/// Protocol column types (wire protocol codes)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Decimal = 0x00,
    Tiny = 0x01,
    Short = 0x02,
    Long = 0x03,
    Float = 0x04,
    Double = 0x05,
    Null = 0x06,
    Timestamp = 0x07,
    LongLong = 0x08,
    Int24 = 0x09,
    Date = 0x0a,
    Time = 0x0b,
    Datetime = 0x0c,
    Year = 0x0d,
    Varchar = 0x0f,
    Bit = 0x10,
    NewDecimal = 0xf6,
    Enum = 0xf7,
    Set = 0xf8,
    TinyBlob = 0xf9,
    MediumBlob = 0xfa,
    LongBlob = 0xfb,
    Blob = 0xfc,
    VarString = 0xfd,
    String = 0xfe,
    Geometry = 0xff,
}

impl From<ColumnType> for u8 {
    fn from(ct: ColumnType) -> u8 {
        ct as u8
    }
}

/// Column flags (bitmask)
pub mod column_flags {
    pub const NOT_NULL: u16 = 0x0001;
    pub const PRI_KEY: u16 = 0x0002;
    pub const UNIQUE_KEY: u16 = 0x0004;
    pub const MULTIPLE_KEY: u16 = 0x0008;
    pub const BLOB: u16 = 0x0010;
    pub const UNSIGNED: u16 = 0x0020;
    pub const ZEROFILL: u16 = 0x0040;
    pub const BINARY: u16 = 0x0080;
    pub const ENUM: u16 = 0x0100;
    pub const AUTO_INCREMENT: u16 = 0x0200;
    pub const TIMESTAMP: u16 = 0x0400;
    pub const SET: u16 = 0x0800;
    pub const NUM: u16 = 0x8000;
}

/// Convert RooDB DataType to protocol ColumnType
pub fn datatype_to_protocol(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::Tiny,
        DataType::TinyInt => ColumnType::Tiny,
        DataType::SmallInt => ColumnType::Short,
        DataType::Int => ColumnType::Long,
        DataType::BigInt => ColumnType::LongLong,
        DataType::Float => ColumnType::Float,
        DataType::Double => ColumnType::Double,
        DataType::Varchar(_) => ColumnType::Varchar,
        DataType::Text => ColumnType::Blob,
        DataType::Blob => ColumnType::Blob,
        DataType::Timestamp => ColumnType::Datetime,
    }
}

/// Get column length for a data type (for column definition packet)
pub fn datatype_column_length(dt: &DataType) -> u32 {
    match dt {
        DataType::Boolean => 1,
        DataType::TinyInt => 4,  // -128 to 127
        DataType::SmallInt => 6, // -32768 to 32767
        DataType::Int => 11,     // -2147483648 to 2147483647
        DataType::BigInt => 20,  // Full i64 range
        DataType::Float => 12,
        DataType::Double => 22,
        DataType::Varchar(n) => *n,
        DataType::Text => 65535,
        DataType::Blob => 65535,
        DataType::Timestamp => 19, // "YYYY-MM-DD HH:MM:SS"
    }
}

/// Get column flags for a data type
pub fn datatype_flags(dt: &DataType, nullable: bool) -> u16 {
    let mut flags = 0u16;

    if !nullable {
        flags |= column_flags::NOT_NULL;
    }

    // Add type-specific flags
    match dt {
        DataType::Boolean
        | DataType::TinyInt
        | DataType::SmallInt
        | DataType::Int
        | DataType::BigInt
        | DataType::Float
        | DataType::Double => {
            flags |= column_flags::NUM;
        }
        DataType::Blob | DataType::Text => {
            flags |= column_flags::BLOB;
        }
        _ => {}
    }

    flags
}

/// Encode a Datum as text protocol bytes (for result set rows)
pub fn datum_to_text_bytes(datum: &Datum) -> Vec<u8> {
    use super::packet::encode_length_encoded_string;

    match datum {
        Datum::Null => {
            // NULL is encoded as 0xfb
            vec![0xfb]
        }
        Datum::Bool(b) => encode_length_encoded_string(if *b { "1" } else { "0" }),
        Datum::Int(i) => encode_length_encoded_string(&i.to_string()),
        Datum::Float(f) => {
            // Use enough precision for round-trip
            encode_length_encoded_string(
                format!("{:.15}", f)
                    .trim_end_matches('0')
                    .trim_end_matches('.'),
            )
        }
        Datum::String(s) => encode_length_encoded_string(s),
        Datum::Bytes(b) => super::packet::encode_length_encoded_bytes(b),
        Datum::Timestamp(ts) => {
            // Convert unix millis to datetime format
            let datetime = format_timestamp(*ts);
            encode_length_encoded_string(&datetime)
        }
    }
}

/// Format a unix timestamp (milliseconds) as datetime string
fn format_timestamp(ts_millis: i64) -> String {
    // Simple formatting - in production would use chrono
    let secs = ts_millis / 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = (secs % 86400).unsigned_abs();

    // Very basic date calculation (doesn't handle negative correctly, just for MVP)
    let (year, month, day) = days_to_ymd(days_since_epoch);
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    )
}

/// Convert days since Unix epoch to (year, month, day)
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Simplified calculation - good enough for MVP
    // Based on the algorithm from Howard Hinnant
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

/// Character set constants
pub mod charset {
    pub const UTF8MB4_GENERAL_CI: u8 = 45;
    pub const BINARY: u8 = 63;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_protocol() {
        assert_eq!(datatype_to_protocol(&DataType::Boolean), ColumnType::Tiny);
        assert_eq!(datatype_to_protocol(&DataType::Int), ColumnType::Long);
        assert_eq!(
            datatype_to_protocol(&DataType::BigInt),
            ColumnType::LongLong
        );
        assert_eq!(datatype_to_protocol(&DataType::Double), ColumnType::Double);
        assert_eq!(
            datatype_to_protocol(&DataType::Varchar(255)),
            ColumnType::Varchar
        );
        assert_eq!(datatype_to_protocol(&DataType::Blob), ColumnType::Blob);
    }

    #[test]
    fn test_datum_to_text_bytes() {
        // NULL
        assert_eq!(datum_to_text_bytes(&Datum::Null), vec![0xfb]);

        // Boolean
        let true_bytes = datum_to_text_bytes(&Datum::Bool(true));
        assert_eq!(true_bytes, vec![1, b'1']);

        // Integer
        let int_bytes = datum_to_text_bytes(&Datum::Int(42));
        assert_eq!(int_bytes, vec![2, b'4', b'2']);

        // String
        let str_bytes = datum_to_text_bytes(&Datum::String("hello".to_string()));
        assert_eq!(str_bytes, vec![5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_format_timestamp() {
        // 2024-01-15 12:30:45 UTC
        let ts = 1705321845000i64;
        let formatted = format_timestamp(ts);
        assert!(formatted.contains("2024"));
    }

    #[test]
    fn test_column_flags() {
        let flags = datatype_flags(&DataType::Int, false);
        assert!(flags & column_flags::NOT_NULL != 0);
        assert!(flags & column_flags::NUM != 0);

        let nullable_flags = datatype_flags(&DataType::Int, true);
        assert!(nullable_flags & column_flags::NOT_NULL == 0);
    }
}
