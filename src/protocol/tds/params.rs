//! RPC parameter parsing and SQL parameter substitution for sp_executesql.
//!
//! When the Go rdb/ms client sends a parameterized query, it uses the RPC
//! packet type with sp_executesql (proc ID 10). The parameters are:
//!   1. SQL text (unnamed, NVARCHAR max/PLP)
//!   2. Parameter declarations string (unnamed, NVARCHAR max/PLP)
//!   3+ Named parameter values (various types)
//!
//! This module parses those parameters from the wire format and substitutes
//! @name references in the SQL with their literal values.

use super::connection::decode_ucs2_str;

/// A decoded named parameter with its SQL literal representation.
struct Param {
    /// Parameter name without '@' prefix.
    name: String,
    /// SQL literal representation (e.g. "42", "'hello'", "NULL").
    literal: String,
}

/// Parse sp_executesql RPC parameters and return the SQL with parameters substituted.
///
/// `data` starts at the first parameter (after ProcIDSwitch + ProcID + Options).
/// Returns the final SQL string with @param references replaced by literal values.
pub fn parse_and_substitute(data: &[u8]) -> Option<String> {
    let mut pos = 0;

    // Param 1: SQL text (unnamed, NVARCHAR max PLP)
    let sql = parse_nvarchar_param(data, &mut pos)?;

    // Param 2: declarations string (unnamed, NVARCHAR max PLP) — skip it
    let _declarations = parse_nvarchar_param(data, &mut pos)?;

    // Params 3+: named parameters
    let mut params = Vec::new();
    while pos < data.len() {
        // 0xFF = tokenDoneInProc, marks end of parameters
        if data[pos] == 0xFF {
            break;
        }
        if let Some(param) = parse_typed_param(data, &mut pos) {
            params.push(param);
        } else {
            break;
        }
    }

    if params.is_empty() {
        return Some(sql);
    }

    Some(substitute_params(&sql, &params))
}

/// Parse an unnamed NVARCHAR parameter (used for SQL text and declarations).
///
/// Wire format:
///   name_len(1, =0) + status(1) + type_id(1, =0xE7) + max_len(2) + collation(5)
///   + PLP value (if max_len == 0xFFFF) or u16 len + data
fn parse_nvarchar_param(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos >= data.len() {
        return None;
    }

    // Name length (0 for unnamed)
    let name_len = data[*pos] as usize;
    *pos += 1;
    *pos += name_len * 2; // skip UCS-2 name

    check_remaining(data, *pos, 2)?; // status + type_id
    *pos += 1; // status

    let type_id = data[*pos];
    *pos += 1;

    if type_id != 0xE7 && type_id != 0xA7 {
        // Not NVARCHAR or VARCHAR — try to skip this param as generic
        return None;
    }

    let is_nchar = type_id == 0xE7;

    check_remaining(data, *pos, 2)?;
    let max_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;

    // Collation (5 bytes) for text types
    check_remaining(data, *pos, 5)?;
    *pos += 5;

    if max_len == 0xFFFF {
        // PLP (Partially Length-Prefixed) format
        read_plp_string(data, pos, is_nchar)
    } else {
        // Regular: u16 actual_len + data
        check_remaining(data, *pos, 2)?;
        let val_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
        *pos += 2;
        if val_len == 0xFFFF {
            return Some(String::new()); // NULL
        }
        check_remaining(data, *pos, val_len)?;
        let text_data = &data[*pos..*pos + val_len];
        *pos += val_len;
        if is_nchar {
            Some(decode_ucs2_str(text_data))
        } else {
            Some(String::from_utf8_lossy(text_data).into_owned())
        }
    }
}

/// Read a PLP (Partially Length-Prefixed) value and decode as string.
///
/// Format: u64 total_size + chunks(u32 len + data)* + u32(0)
fn read_plp_string(data: &[u8], pos: &mut usize, is_nchar: bool) -> Option<String> {
    check_remaining(data, *pos, 8)?;
    let total_size = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;

    // NULL
    if total_size == 0xFFFFFFFFFFFFFFFF {
        return Some(String::new());
    }

    // Collect chunks
    let mut all_bytes = Vec::new();
    loop {
        check_remaining(data, *pos, 4)?;
        let chunk_len =
            u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                as usize;
        *pos += 4;
        if chunk_len == 0 {
            break; // End of PLP
        }
        check_remaining(data, *pos, chunk_len)?;
        all_bytes.extend_from_slice(&data[*pos..*pos + chunk_len]);
        *pos += chunk_len;
    }

    if is_nchar {
        Some(decode_ucs2_str(&all_bytes))
    } else {
        Some(String::from_utf8_lossy(&all_bytes).into_owned())
    }
}

/// Read raw PLP bytes (for binary data).
fn read_plp_bytes(data: &[u8], pos: &mut usize) -> Option<Option<Vec<u8>>> {
    check_remaining(data, *pos, 8)?;
    let total_size = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;

    if total_size == 0xFFFFFFFFFFFFFFFF {
        return Some(None); // NULL
    }

    let mut all_bytes = Vec::new();
    loop {
        check_remaining(data, *pos, 4)?;
        let chunk_len =
            u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                as usize;
        *pos += 4;
        if chunk_len == 0 {
            break;
        }
        check_remaining(data, *pos, chunk_len)?;
        all_bytes.extend_from_slice(&data[*pos..*pos + chunk_len]);
        *pos += chunk_len;
    }

    Some(Some(all_bytes))
}

/// Parse a single typed parameter (named, with value).
fn parse_typed_param(data: &[u8], pos: &mut usize) -> Option<Param> {
    check_remaining(data, *pos, 1)?;

    // Parameter name
    let name_len = data[*pos] as usize; // in UCS-2 chars
    *pos += 1;

    let name = if name_len > 0 {
        check_remaining(data, *pos, name_len * 2)?;
        let name_str = decode_ucs2_str(&data[*pos..*pos + name_len * 2]);
        *pos += name_len * 2;
        // Strip leading '@'
        name_str.strip_prefix('@').unwrap_or(&name_str).to_string()
    } else {
        String::new()
    };

    check_remaining(data, *pos, 2)?; // status + type_id
    *pos += 1; // status byte

    let type_id = data[*pos];
    *pos += 1;

    let literal = match type_id {
        // INTNTYPE
        0x26 => parse_int_value(data, pos)?,
        // BITNTYPE
        0x68 => parse_bit_value(data, pos)?,
        // FLTNTYPE
        0x6D => parse_float_value(data, pos)?,
        // DECIMALNTYPE / NUMERICNTYPE
        0x6A | 0x6C => parse_decimal_value(data, pos)?,
        // NVARCHARTYPE
        0xE7 => parse_nvarchar_value(data, pos, true)?,
        // BIGVARCHRTYPE (VARCHAR)
        0xA7 => parse_nvarchar_value(data, pos, false)?,
        // BIGVARBINTYPE (VARBINARY)
        0xA5 => parse_varbinary_value(data, pos)?,
        // DATETIME2NTYPE
        0x2A => parse_datetime2_value(data, pos)?,
        // DATENTYPE
        0x28 => parse_date_value(data, pos)?,
        // TIMENTYPE
        0x29 => parse_time_value(data, pos)?,
        // DATETIMEOFFSETNTYPE
        0x2B => parse_datetimeoffset_value(data, pos)?,
        // DATETIMNTYPE (old datetime)
        0x6F => parse_datetimen_value(data, pos)?,
        // GUIDTYPE
        0x24 => parse_guid_value(data, pos)?,
        // MONEYNTYPE
        0x6E => parse_money_value(data, pos)?,
        _ => return None, // Unknown type — stop parsing
    };

    Some(Param { name, literal })
}

// ---------------------------------------------------------------------------
// Individual type value parsers
// ---------------------------------------------------------------------------

/// INTNTYPE (0x26): type_width(1) then value_len(1) + LE integer bytes.
fn parse_int_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let _type_width = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let val = match val_len {
        1 => data[*pos] as i64,
        2 => i16::from_le_bytes([data[*pos], data[*pos + 1]]) as i64,
        4 => {
            i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as i64
        }
        8 => i64::from_le_bytes([
            data[*pos],
            data[*pos + 1],
            data[*pos + 2],
            data[*pos + 3],
            data[*pos + 4],
            data[*pos + 5],
            data[*pos + 6],
            data[*pos + 7],
        ]),
        _ => {
            *pos += val_len;
            return Some("NULL".to_string());
        }
    };
    *pos += val_len;
    Some(val.to_string())
}

/// BITNTYPE (0x68): type_width(1) then value_len(1) + 1 byte.
fn parse_bit_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    *pos += 1; // type_width (always 1)

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, 1)?;
    let val = data[*pos];
    *pos += 1;
    Some(if val != 0 { "1" } else { "0" }.to_string())
}

/// FLTNTYPE (0x6D): type_width(1) then value_len(1) + float bytes.
fn parse_float_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let _type_width = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let literal = match val_len {
        4 => {
            let bits =
                u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            format!("{}", f32::from_bits(bits))
        }
        8 => {
            let bits = u64::from_le_bytes([
                data[*pos],
                data[*pos + 1],
                data[*pos + 2],
                data[*pos + 3],
                data[*pos + 4],
                data[*pos + 5],
                data[*pos + 6],
                data[*pos + 7],
            ]);
            format!("{}", f64::from_bits(bits))
        }
        _ => "NULL".to_string(),
    };
    *pos += val_len;
    Some(literal)
}

/// DECIMALNTYPE (0x6A) / NUMERICNTYPE (0x6C):
/// type_width(1) + precision(1) + scale(1) then value_len(1) + sign(1) + magnitude(LE).
fn parse_decimal_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 3)?;
    let _max_width = data[*pos];
    let _precision = data[*pos + 1];
    let scale = data[*pos + 2] as u32;
    *pos += 3;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let sign = data[*pos]; // 0 = negative, 1 = positive
    let magnitude_bytes = &data[*pos + 1..*pos + val_len];

    // magnitude is LE unsigned integer
    let mut magnitude: u128 = 0;
    for (i, &b) in magnitude_bytes.iter().enumerate() {
        magnitude |= (b as u128) << (i * 8);
    }
    *pos += val_len;

    // Format as decimal string with proper scale
    if scale == 0 {
        let s = magnitude.to_string();
        return Some(if sign == 0 { format!("-{}", s) } else { s });
    }

    let s = magnitude.to_string();
    let literal = if s.len() as u32 <= scale {
        // Need leading zeros: e.g. magnitude=5, scale=3 → "0.005"
        let zeros = scale as usize - s.len();
        format!("0.{}{}", "0".repeat(zeros), s)
    } else {
        let split = s.len() - scale as usize;
        format!("{}.{}", &s[..split], &s[split..])
    };

    Some(if sign == 0 {
        format!("-{}", literal)
    } else {
        literal
    })
}

/// NVARCHAR (0xE7) or VARCHAR (0xA7) parameter value.
/// TYPE_INFO: max_len(2) + collation(5). Then PLP or u16+data.
fn parse_nvarchar_value(data: &[u8], pos: &mut usize, is_nchar: bool) -> Option<String> {
    check_remaining(data, *pos, 2)?;
    let max_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;

    // Collation (5 bytes)
    check_remaining(data, *pos, 5)?;
    *pos += 5;

    let raw_str = if max_len == 0xFFFF {
        // PLP format
        read_plp_string(data, pos, is_nchar)?
    } else {
        // Regular: u16 len + data
        check_remaining(data, *pos, 2)?;
        let val_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
        *pos += 2;
        if val_len == 0xFFFF {
            return Some("NULL".to_string());
        }
        check_remaining(data, *pos, val_len)?;
        let text_data = &data[*pos..*pos + val_len];
        *pos += val_len;
        if is_nchar {
            decode_ucs2_str(text_data)
        } else {
            String::from_utf8_lossy(text_data).into_owned()
        }
    };

    // Escape for SQL literal: replace ' with ''
    let escaped = raw_str.replace('\'', "''");
    Some(format!("'{}'", escaped))
}

/// VARBINARY (0xA5) parameter value.
/// TYPE_INFO: max_len(2). Then PLP or u16+data.
fn parse_varbinary_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 2)?;
    let max_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;

    // No collation for binary types

    if max_len == 0xFFFF {
        let bytes = read_plp_bytes(data, pos)?;
        match bytes {
            None => Some("NULL".to_string()),
            Some(b) => Some(format!("X'{}'", hex_encode(&b))),
        }
    } else {
        check_remaining(data, *pos, 2)?;
        let val_len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
        *pos += 2;
        if val_len == 0xFFFF {
            return Some("NULL".to_string());
        }
        check_remaining(data, *pos, val_len)?;
        let bytes = &data[*pos..*pos + val_len];
        *pos += val_len;
        Some(format!("X'{}'", hex_encode(bytes)))
    }
}

/// DATETIME2NTYPE (0x2A): scale(1) then value_len(1) + time(3-5 bytes) + date(3 bytes).
fn parse_datetime2_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let scale = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let val_data = &data[*pos..*pos + val_len];
    *pos += val_len;

    // time bytes = val_len - 3, date bytes = 3
    let time_len = val_len - 3;
    let (time_str, date_str) = decode_time_date(val_data, time_len, scale);
    Some(format!("'{} {}'", date_str, time_str))
}

/// DATENTYPE (0x28): no scale byte, value_len(1) + 3 bytes date.
fn parse_date_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let days = read_le_uint(&data[*pos..*pos + 3]) as i64;
    *pos += val_len;

    let date_str = days_to_date(days);
    Some(format!("'{}'", date_str))
}

/// TIMENTYPE (0x29): scale(1) then value_len(1) + time bytes.
fn parse_time_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let scale = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let time_str = decode_time_only(&data[*pos..*pos + val_len], scale);
    *pos += val_len;
    Some(format!("'{}'", time_str))
}

/// DATETIMEOFFSETNTYPE (0x2B): scale(1) then value_len(1) + time + date + offset(2).
fn parse_datetimeoffset_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let scale = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let val_data = &data[*pos..*pos + val_len];
    *pos += val_len;

    // time bytes = val_len - 5, date = 3 bytes, offset = 2 bytes
    let time_len = val_len - 5;
    let (time_str, date_str) = decode_time_date(val_data, time_len, scale);
    let offset_minutes = i16::from_le_bytes([val_data[val_len - 2], val_data[val_len - 1]]) as i32;
    let off_h = offset_minutes / 60;
    let off_m = (offset_minutes % 60).abs();
    Some(format!(
        "'{} {}{:+03}:{:02}'",
        date_str, time_str, off_h, off_m
    ))
}

/// DATETIMNTYPE (0x6F): width(1) then value_len(1) + value.
fn parse_datetimen_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    let _type_width = data[*pos];
    *pos += 1;

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let val_data = &data[*pos..*pos + val_len];
    *pos += val_len;

    match val_len {
        8 => {
            // datetime: 4 bytes days since 1900-01-01 + 4 bytes 1/300 seconds
            let days = i32::from_le_bytes([val_data[0], val_data[1], val_data[2], val_data[3]]);
            let ticks =
                u32::from_le_bytes([val_data[4], val_data[5], val_data[6], val_data[7]]) as u64;

            // days since 1900-01-01
            let total_days = 693595 + days as i64; // 1900-01-01 is day 693595 from 0001-01-01
            let date_str = days_to_date(total_days);

            let total_ms = ticks * 10 / 3; // 1/300 sec to milliseconds
            let secs = total_ms / 1000;
            let ms = total_ms % 1000;
            let h = secs / 3600;
            let m = (secs % 3600) / 60;
            let s = secs % 60;
            Some(format!(
                "'{} {:02}:{:02}:{:02}.{:03}'",
                date_str, h, m, s, ms
            ))
        }
        4 => {
            // smalldatetime: 2 bytes days since 1900-01-01 + 2 bytes minutes
            let days = u16::from_le_bytes([val_data[0], val_data[1]]) as i64;
            let mins = u16::from_le_bytes([val_data[2], val_data[3]]) as u64;
            let total_days = 693595 + days;
            let date_str = days_to_date(total_days);
            let h = mins / 60;
            let m = mins % 60;
            Some(format!("'{} {:02}:{:02}:00'", date_str, h, m))
        }
        _ => Some("NULL".to_string()),
    }
}

/// GUIDTYPE (0x24): width(1, =16) then value_len(1) + 16 bytes.
fn parse_guid_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    *pos += 1; // width (16)

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let b = &data[*pos..*pos + val_len];
    *pos += val_len;

    if val_len == 16 {
        // SQL Server GUID byte order: first 3 groups are LE, last 2 are BE
        Some(format!(
            "'{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}'",
            b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6],
            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        ))
    } else {
        Some("NULL".to_string())
    }
}

/// MONEYNTYPE (0x6E): width(1) then value_len(1) + value.
fn parse_money_value(data: &[u8], pos: &mut usize) -> Option<String> {
    check_remaining(data, *pos, 1)?;
    *pos += 1; // type width

    check_remaining(data, *pos, 1)?;
    let val_len = data[*pos] as usize;
    *pos += 1;

    if val_len == 0 {
        return Some("NULL".to_string());
    }

    check_remaining(data, *pos, val_len)?;
    let val_data = &data[*pos..*pos + val_len];
    *pos += val_len;

    match val_len {
        8 => {
            // money: high 4 bytes + low 4 bytes = i64 in units of 1/10000
            let hi = i32::from_le_bytes([val_data[0], val_data[1], val_data[2], val_data[3]]);
            let lo = u32::from_le_bytes([val_data[4], val_data[5], val_data[6], val_data[7]]);
            let raw = ((hi as i64) << 32) | lo as i64;
            let whole = raw / 10000;
            let frac = (raw % 10000).unsigned_abs();
            Some(format!("{}.{:04}", whole, frac))
        }
        4 => {
            // smallmoney: i32 in units of 1/10000
            let raw =
                i32::from_le_bytes([val_data[0], val_data[1], val_data[2], val_data[3]]) as i64;
            let whole = raw / 10000;
            let frac = (raw % 10000).unsigned_abs();
            Some(format!("{}.{:04}", whole, frac))
        }
        _ => Some("NULL".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Date/time helpers
// ---------------------------------------------------------------------------

/// Decode time bytes + date bytes into formatted strings.
fn decode_time_date(val_data: &[u8], time_len: usize, scale: u8) -> (String, String) {
    let time_val = read_le_uint(&val_data[..time_len]);
    let date_bytes = &val_data[time_len..time_len + 3];
    let days = read_le_uint(date_bytes) as i64;

    let time_str = format_time(time_val, scale);
    let date_str = days_to_date(days);
    (time_str, date_str)
}

/// Decode time-only bytes.
fn decode_time_only(val_data: &[u8], scale: u8) -> String {
    let time_val = read_le_uint(val_data);
    format_time(time_val, scale)
}

/// Format a time integer value with given scale into HH:MM:SS.fractional.
fn format_time(time_val: u64, scale: u8) -> String {
    let divisor = 10u64.pow(scale as u32);
    let total_seconds = time_val / divisor;
    let fractional = time_val % divisor;

    let h = total_seconds / 3600;
    let m = (total_seconds % 3600) / 60;
    let s = total_seconds % 60;

    if scale == 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!(
            "{:02}:{:02}:{:02}.{:0width$}",
            h,
            m,
            s,
            fractional,
            width = scale as usize
        )
    }
}

/// Convert days since 0001-01-01 to YYYY-MM-DD string.
fn days_to_date(days: i64) -> String {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    // TDS epoch is 0001-01-01; Hinnant internal epoch is 0000-03-01 (306 days earlier)
    let z = days + 306;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Read up to 8 bytes as a little-endian unsigned integer.
fn read_le_uint(data: &[u8]) -> u64 {
    let mut val: u64 = 0;
    for (i, &b) in data.iter().enumerate() {
        val |= (b as u64) << (i * 8);
    }
    val
}

/// Simple hex encoding.
fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for &b in data {
        s.push_str(&format!("{:02X}", b));
    }
    s
}

// ---------------------------------------------------------------------------
// Parameter substitution
// ---------------------------------------------------------------------------

/// Replace @name references in SQL with their literal values.
///
/// This preserves the line structure of the SQL so that error line numbers
/// remain correct. It handles:
/// - Not replacing inside string literals ('...')
/// - Longest-match-first for parameter names
/// - Word boundary checks (won't replace @param when @paramX exists)
fn substitute_params(sql: &str, params: &[Param]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }

    // Sort by name length descending for longest-match-first
    let mut sorted: Vec<(&str, &str)> = params
        .iter()
        .map(|p| (p.name.as_str(), p.literal.as_str()))
        .collect();
    sorted.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len() + sql.len() / 4);
    let mut i = 0;

    while i < chars.len() {
        // Skip string literals
        if chars[i] == '\'' {
            result.push('\'');
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    result.push('\'');
                    i += 1;
                    // Escaped quote ''
                    if i < chars.len() && chars[i] == '\'' {
                        result.push('\'');
                        i += 1;
                        continue;
                    }
                    break;
                }
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Check for @param reference
        if chars[i] == '@' {
            let mut matched = false;
            for &(name, literal) in &sorted {
                let name_chars: Vec<char> = name.chars().collect();
                let end = i + 1 + name_chars.len();
                if end <= chars.len() {
                    let matches = chars[i + 1..end]
                        .iter()
                        .zip(name_chars.iter())
                        .all(|(a, b)| a.eq_ignore_ascii_case(b));
                    if matches && (end >= chars.len() || !is_ident_char(chars[end])) {
                        result.push_str(literal);
                        i = end;
                        matched = true;
                        break;
                    }
                }
            }
            if !matched {
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        result.push(chars[i]);
        i += 1;
    }

    result
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn check_remaining(data: &[u8], pos: usize, need: usize) -> Option<()> {
    if pos + need <= data.len() {
        Some(())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_simple() {
        let params = vec![
            Param {
                name: "id".to_string(),
                literal: "42".to_string(),
            },
            Param {
                name: "name".to_string(),
                literal: "'hello'".to_string(),
            },
        ];
        let sql = "SELECT * FROM t WHERE id = @id AND name = @name";
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM t WHERE id = 42 AND name = 'hello'");
    }

    #[test]
    fn test_substitute_inside_string_literal() {
        let params = vec![Param {
            name: "x".to_string(),
            literal: "42".to_string(),
        }];
        let sql = "SELECT '@x is not replaced', @x";
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT '@x is not replaced', 42");
    }

    #[test]
    fn test_substitute_prefix_safety() {
        let params = vec![
            Param {
                name: "p".to_string(),
                literal: "1".to_string(),
            },
            Param {
                name: "prefix".to_string(),
                literal: "2".to_string(),
            },
        ];
        let sql = "SELECT @p, @prefix";
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 1, 2");
    }

    #[test]
    fn test_substitute_case_insensitive() {
        let params = vec![Param {
            name: "MyParam".to_string(),
            literal: "99".to_string(),
        }];
        let sql = "SELECT @myparam, @MYPARAM, @MyParam";
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 99, 99, 99");
    }

    #[test]
    fn test_substitute_preserves_lines() {
        let params = vec![
            Param {
                name: "a".to_string(),
                literal: "1".to_string(),
            },
            Param {
                name: "b".to_string(),
                literal: "2".to_string(),
            },
        ];
        let sql = "SELECT\n  @a,\n  @b\nFROM t";
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT\n  1,\n  2\nFROM t");
        // Same number of newlines
        assert_eq!(sql.matches('\n').count(), result.matches('\n').count());
    }

    #[test]
    fn test_days_to_date() {
        // 2000-01-01 is day 730119 from 0001-01-01
        assert_eq!(days_to_date(730119), "2000-01-01");
        // 1970-01-01 is day 719162
        assert_eq!(days_to_date(719162), "1970-01-01");
    }

    #[test]
    fn test_decimal_formatting() {
        // Simulate parsing a decimal: magnitude=12345, scale=2 → "123.45"
        let magnitude: u128 = 12345;
        let scale: u32 = 2;
        let s = magnitude.to_string();
        let split = s.len() - scale as usize;
        let formatted = format!("{}.{}", &s[..split], &s[split..]);
        assert_eq!(formatted, "123.45");
    }
}
