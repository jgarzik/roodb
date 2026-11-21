//! TDS token stream encoding for server responses.
//!
//! A server response (a TabularResult message) is a sequence of tokens.
//! Each encoder appends one token to a buffer.

// Token type constants
pub const TOKEN_RETURNSTATUS: u8 = 0x79;
pub const TOKEN_COLMETADATA: u8 = 0x81;
pub const TOKEN_ERROR: u8 = 0xAA;
pub const TOKEN_INFO: u8 = 0xAB;
pub const TOKEN_RETURNVALUE: u8 = 0xAC;
pub const TOKEN_LOGINACK: u8 = 0xAD;
pub const TOKEN_FEATUREEXTACK: u8 = 0xAE;
pub const TOKEN_ROW: u8 = 0xD1;
pub const TOKEN_ENVCHANGE: u8 = 0xE3;
pub const TOKEN_DONE: u8 = 0xFD;
pub const TOKEN_DONEPROC: u8 = 0xFE;
pub const TOKEN_DONEINPROC: u8 = 0xFF;

// DONE status flags
pub const DONE_FINAL: u16 = 0x0000;
pub const DONE_MORE: u16 = 0x0001;
pub const DONE_ERROR: u16 = 0x0002;
pub const DONE_COUNT: u16 = 0x0010;
pub const DONE_ATTN: u16 = 0x0020;

// ENVCHANGE types
pub const ENV_DATABASE: u8 = 1;
pub const ENV_LANGUAGE: u8 = 2;
pub const ENV_PACKET_SIZE: u8 = 4;
pub const ENV_COLLATION: u8 = 7;
pub const ENV_BEGIN_TRAN: u8 = 8;
pub const ENV_COMMIT_TRAN: u8 = 9;
pub const ENV_ROLLBACK_TRAN: u8 = 10;
pub const ENV_RESET_CONNECTION: u8 = 18;

/// TDS version reported in LOGINACK: 7.4. A TDS 8.0 connection differs
/// only in transport, and drivers expect 7.4 here on both.
pub const LOGINACK_TDS_VERSION: [u8; 4] = [0x74, 0x00, 0x00, 0x04];

/// Collation sent for character data: Latin1_General, case-insensitive,
/// accent-sensitive (LCID 0x0409, sort id 52). Character values are sent
/// as UTF-16, so the collation only informs client-side comparisons.
pub const DEFAULT_COLLATION: [u8; 5] = [0x09, 0x04, 0xD0, 0x00, 0x34];

/// Name the server reports in LOGINACK and in messages.
pub const SERVER_NAME: &str = "RooDB";

/// Truncate to at most `max_chars` UTF-16 units without splitting a
/// surrogate pair.
fn ucs2_prefix(s: &str, max_chars: usize) -> Vec<u8> {
    let mut units: Vec<u16> = s.encode_utf16().take(max_chars).collect();
    if let Some(last) = units.last() {
        if (0xD800..0xDC00).contains(last) {
            units.pop();
        }
    }
    units.iter().flat_map(|u| u.to_le_bytes()).collect()
}

/// Append a B_VARCHAR: 1-byte length in UTF-16 units, then the text
/// (truncated to 255 units).
pub fn put_b_varchar(buf: &mut Vec<u8>, s: &str) {
    let ucs2 = ucs2_prefix(s, u8::MAX as usize);
    buf.push((ucs2.len() / 2) as u8);
    buf.extend_from_slice(&ucs2);
}

/// Append a US_VARCHAR: 2-byte length in UTF-16 units, then the text
/// (truncated to 65535 units).
pub fn put_us_varchar(buf: &mut Vec<u8>, s: &str) {
    let ucs2 = ucs2_prefix(s, u16::MAX as usize);
    buf.extend_from_slice(&((ucs2.len() / 2) as u16).to_le_bytes());
    buf.extend_from_slice(&ucs2);
}

/// Append a token with a 2-byte length prefix.
fn put_token(buf: &mut Vec<u8>, token: u8, body: &[u8]) {
    buf.push(token);
    buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
    buf.extend_from_slice(body);
}

/// Append a LOGINACK token: SQL interface, TDS 7.4, server name and
/// product version (major, minor, build).
pub fn put_loginack(buf: &mut Vec<u8>, version: [u8; 4]) {
    let mut body = Vec::with_capacity(32);
    body.push(0x01); // interface: T-SQL
    body.extend_from_slice(&LOGINACK_TDS_VERSION);
    put_b_varchar(&mut body, SERVER_NAME);
    body.extend_from_slice(&version);
    put_token(buf, TOKEN_LOGINACK, &body);
}

/// Append a FEATUREEXTACK token acknowledging no optional features.
/// Clients that sent a feature extension treat an unacknowledged feature
/// as declined.
pub fn put_featureextack_none(buf: &mut Vec<u8>) {
    buf.push(TOKEN_FEATUREEXTACK);
    buf.push(0xFF); // terminator
}

/// Append an ENVCHANGE token with string new/old values.
pub fn put_envchange_string(buf: &mut Vec<u8>, env_type: u8, new_value: &str, old_value: &str) {
    let mut body = vec![env_type];
    put_b_varchar(&mut body, new_value);
    put_b_varchar(&mut body, old_value);
    put_token(buf, TOKEN_ENVCHANGE, &body);
}

/// Append an ENVCHANGE token with binary new/old values (collation,
/// transaction descriptor). Values are at most 255 bytes.
pub fn put_envchange_binary(buf: &mut Vec<u8>, env_type: u8, new_value: &[u8], old_value: &[u8]) {
    let new_value = &new_value[..new_value.len().min(255)];
    let old_value = &old_value[..old_value.len().min(255)];
    let mut body = vec![env_type, new_value.len() as u8];
    body.extend_from_slice(new_value);
    body.push(old_value.len() as u8);
    body.extend_from_slice(old_value);
    put_token(buf, TOKEN_ENVCHANGE, &body);
}

/// Append a DONE-family token (`TOKEN_DONE`, `TOKEN_DONEPROC` or
/// `TOKEN_DONEINPROC`).
pub fn put_done(buf: &mut Vec<u8>, token: u8, status: u16, row_count: u64) {
    buf.push(token);
    buf.extend_from_slice(&status.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // current command
    buf.extend_from_slice(&row_count.to_le_bytes());
}

/// Longest message text, in UTF-16 units, of an ERROR or INFO token. The
/// token's length field is 16 bits and counts bytes, so the text must
/// leave room for the other fields.
const MAX_MESSAGE_CHARS: usize = 8000;

/// Append an ERROR or INFO token.
fn put_message(buf: &mut Vec<u8>, token: u8, number: i32, state: u8, class: u8, message: &str) {
    let mut body = Vec::with_capacity(32 + message.len() * 2);
    body.extend_from_slice(&number.to_le_bytes());
    body.push(state);
    body.push(class);
    let text = ucs2_prefix(message, MAX_MESSAGE_CHARS);
    body.extend_from_slice(&((text.len() / 2) as u16).to_le_bytes());
    body.extend_from_slice(&text);
    put_b_varchar(&mut body, SERVER_NAME);
    put_b_varchar(&mut body, ""); // procedure name
    body.extend_from_slice(&1i32.to_le_bytes()); // line number
    put_token(buf, token, &body);
}

/// Append an ERROR token.
pub fn put_error(buf: &mut Vec<u8>, number: i32, state: u8, class: u8, message: &str) {
    put_message(buf, TOKEN_ERROR, number, state, class, message);
}

/// Append an INFO token.
pub fn put_info(buf: &mut Vec<u8>, number: i32, state: u8, class: u8, message: &str) {
    put_message(buf, TOKEN_INFO, number, state, class, message);
}

/// Append a RETURNVALUE token carrying an integer output parameter.
pub fn put_return_value_int(buf: &mut Vec<u8>, name: &str, value: i32) {
    buf.push(TOKEN_RETURNVALUE);
    buf.extend_from_slice(&0u16.to_le_bytes()); // parameter ordinal
    put_b_varchar(buf, name);
    buf.push(0x01); // status: output parameter
    buf.extend_from_slice(&0u32.to_le_bytes()); // user type
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&[0x26, 4]); // TYPE_INFO: INTN(4)
    buf.push(4);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Append a RETURNSTATUS token.
pub fn put_return_status(buf: &mut Vec<u8>, value: i32) {
    buf.push(TOKEN_RETURNSTATUS);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// SQL Server error number, severity class and state for a RooDB error.
///
/// RooDB's native error catalogue is MySQL's. Clients of this protocol
/// branch on SQL Server numbers, so the common cases are translated; the
/// rest are reported as user error 50000. All are severity 14-16, which
/// tells the client the session is still usable.
pub fn sql_server_error(code: u16) -> (i32, u8, u8) {
    match code {
        1064 => (102, 15, 1),          // syntax error
        1065 => (102, 15, 1),          // empty query
        1146 | 1051 => (208, 16, 1),   // invalid object name
        1050 => (2714, 16, 6),         // object already exists
        1054 => (207, 16, 1),          // invalid column name
        1062 => (2627, 14, 1),         // duplicate key
        1048 => (515, 16, 2),          // NULL into NOT NULL column
        1045 => (229, 14, 5),          // permission denied
        1049 => (911, 16, 1),          // database does not exist
        1136 => (213, 16, 1),          // column count mismatch
        1264 | 1690 => (8115, 16, 2),  // arithmetic overflow
        1292 => (245, 16, 1),          // conversion failed
        1210 => (8116, 16, 1),         // invalid argument
        1295 | 1047 => (40517, 16, 1), // unsupported
        1305 => (2812, 16, 62),        // stored procedure not found
        _ => (50000, 16, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn done_token_layout() {
        let mut buf = Vec::new();
        put_done(&mut buf, TOKEN_DONE, DONE_COUNT | DONE_MORE, 5);
        assert_eq!(buf.len(), 13);
        assert_eq!(buf[0], TOKEN_DONE);
        assert_eq!(u16::from_le_bytes([buf[1], buf[2]]), 0x0011);
        assert_eq!(u64::from_le_bytes(buf[5..13].try_into().unwrap()), 5);
    }

    #[test]
    fn loginack_reports_tds_7_4() {
        let mut buf = Vec::new();
        put_loginack(&mut buf, [16, 0, 0x03, 0xE8]);
        assert_eq!(buf[0], TOKEN_LOGINACK);
        let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
        assert_eq!(buf.len(), 3 + len);
        assert_eq!(buf[3], 1);
        assert_eq!(&buf[4..8], &[0x74, 0x00, 0x00, 0x04]);
        assert_eq!(buf[8] as usize, SERVER_NAME.len());
        assert_eq!(&buf[buf.len() - 4..], &[16, 0, 0x03, 0xE8]);
    }

    #[test]
    fn error_token_layout() {
        let mut buf = Vec::new();
        put_error(&mut buf, 208, 1, 16, "Invalid object name 't'");
        assert_eq!(buf[0], TOKEN_ERROR);
        let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
        assert_eq!(buf.len(), 3 + len);
        assert_eq!(i32::from_le_bytes(buf[3..7].try_into().unwrap()), 208);
        assert_eq!((buf[7], buf[8]), (1, 16));
        assert_eq!(u16::from_le_bytes([buf[9], buf[10]]), 23);
    }

    #[test]
    fn long_messages_cannot_overflow_the_token_length() {
        let mut buf = Vec::new();
        put_error(&mut buf, 102, 1, 15, &"e".repeat(100_000));
        let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
        assert_eq!(buf.len(), 3 + len, "length field covers the whole token");
        assert_eq!(
            u16::from_le_bytes([buf[9], buf[10]]) as usize,
            MAX_MESSAGE_CHARS
        );
    }

    #[test]
    fn return_value_layout() {
        let mut buf = Vec::new();
        put_return_value_int(&mut buf, "@handle", 7);
        assert_eq!(buf[0], TOKEN_RETURNVALUE);
        assert_eq!(buf[3], 7, "name length in characters");
        let tail = &buf[4 + 14..];
        assert_eq!(tail, [0x01, 0, 0, 0, 0, 0, 0, 0x26, 4, 4, 7, 0, 0, 0]);
    }

    #[test]
    fn envchange_layouts() {
        let mut buf = Vec::new();
        put_envchange_string(&mut buf, ENV_DATABASE, "test", "default");
        assert_eq!(buf[0], TOKEN_ENVCHANGE);
        assert_eq!(u16::from_le_bytes([buf[1], buf[2]]) as usize, buf.len() - 3);
        assert_eq!((buf[3], buf[4]), (ENV_DATABASE, 4));

        let mut buf = Vec::new();
        put_envchange_binary(&mut buf, ENV_BEGIN_TRAN, &[1, 2, 3, 4, 5, 6, 7, 8], &[]);
        assert_eq!(buf[3..], [ENV_BEGIN_TRAN, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0]);
    }

    #[test]
    fn long_strings_are_truncated_on_character_boundaries() {
        let mut buf = Vec::new();
        put_b_varchar(&mut buf, &"x".repeat(300));
        assert_eq!(buf[0], 255);
        assert_eq!(buf.len(), 1 + 255 * 2);

        // 254 ASCII then an astral character: its surrogate pair would be
        // split at 255 units, so it is dropped.
        let mut buf = Vec::new();
        put_b_varchar(&mut buf, &format!("{}\u{1F600}", "x".repeat(254)));
        assert_eq!(buf[0], 254);

        let mut buf = Vec::new();
        put_us_varchar(&mut buf, &"y".repeat(70_000));
        assert_eq!(u16::from_le_bytes([buf[0], buf[1]]), 65535);
    }

    #[test]
    fn error_numbers_stay_below_fatal_severity() {
        for code in [0u16, 1045, 1062, 1064, 1146, 1105, 9999] {
            let (_, class, _) = sql_server_error(code);
            assert!((11..=16).contains(&class));
        }
        assert_eq!(sql_server_error(1146).0, 208);
        assert_eq!(sql_server_error(1062).0, 2627);
        assert_eq!(sql_server_error(4242).0, 50000);
    }
}
