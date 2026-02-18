//! TDS token stream encoding for server responses.
//!
//! Tokens are the building blocks of TDS server responses (TabularResult messages).

use super::login::encode_ucs2;

// Token type constants
pub const TOKEN_ERROR: u8 = 0xAA;
pub const TOKEN_INFO: u8 = 0xAB;
pub const TOKEN_LOGINACK: u8 = 0xAD;
pub const TOKEN_ENVCHANGE: u8 = 0xE3;
pub const TOKEN_DONE: u8 = 0xFD;
pub const TOKEN_DONEPROC: u8 = 0xFE;
pub const TOKEN_DONEINPROC: u8 = 0xFF;
pub const TOKEN_COLMETADATA: u8 = 0x81;
pub const TOKEN_ROW: u8 = 0xD1;
pub const TOKEN_ORDER: u8 = 0xA9;
pub const TOKEN_RETURNSTATUS: u8 = 0x79;

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

/// Encode a B_VARCHAR: 1-byte length (in UCS-2 chars) + UCS-2 data.
fn encode_b_varchar(s: &str) -> Vec<u8> {
    let ucs2 = encode_ucs2(s);
    let char_count = ucs2.len() / 2;
    let mut buf = Vec::with_capacity(1 + ucs2.len());
    buf.push(char_count as u8);
    buf.extend_from_slice(&ucs2);
    buf
}

/// Encode a US_VARCHAR: 2-byte length (LE, in UCS-2 chars) + UCS-2 data.
fn encode_us_varchar(s: &str) -> Vec<u8> {
    let ucs2 = encode_ucs2(s);
    let char_count = (ucs2.len() / 2) as u16;
    let mut buf = Vec::with_capacity(2 + ucs2.len());
    buf.extend_from_slice(&char_count.to_le_bytes());
    buf.extend_from_slice(&ucs2);
    buf
}

/// Encode a LOGINACK token.
///
/// Interface=1 (T-SQL), TDS version bytes, program name, version.
pub fn encode_loginack(program_name: &str, major: u8, minor: u8, build: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);

    // Interface: 1 = T-SQL
    body.push(0x01);

    // TDS Version: 7.4 = 0x74 0x00 0x00 0x04 (as sent by server)
    // Client sends BE 0x04000074, server echoes the version bytes
    body.extend_from_slice(&[0x74, 0x00, 0x00, 0x04]);

    // ProgName (B_VARCHAR)
    body.extend_from_slice(&encode_b_varchar(program_name));

    // ProgVersion: Major, Minor, BuildNumber(BE u16)
    body.push(major);
    body.push(minor);
    body.extend_from_slice(&build.to_be_bytes());

    // Wrap with token type + length
    let mut buf = Vec::with_capacity(3 + body.len());
    buf.push(TOKEN_LOGINACK);
    buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Encode an ENVCHANGE token with string new/old values.
pub fn encode_envchange_string(env_type: u8, new_value: &str, old_value: &str) -> Vec<u8> {
    let new_bvar = encode_b_varchar(new_value);
    let old_bvar = encode_b_varchar(old_value);

    let body_len = 1 + new_bvar.len() + old_bvar.len(); // type + new + old

    let mut buf = Vec::with_capacity(3 + body_len);
    buf.push(TOKEN_ENVCHANGE);
    buf.extend_from_slice(&(body_len as u16).to_le_bytes());
    buf.push(env_type);
    buf.extend_from_slice(&new_bvar);
    buf.extend_from_slice(&old_bvar);
    buf
}

/// Encode an ENVCHANGE token with binary new value (e.g., collation, transaction descriptor).
pub fn encode_envchange_binary(env_type: u8, new_value: &[u8], old_value: &[u8]) -> Vec<u8> {
    // B_VARBYTE: 1-byte length + data
    let body_len = 1 + 1 + new_value.len() + 1 + old_value.len();

    let mut buf = Vec::with_capacity(3 + body_len);
    buf.push(TOKEN_ENVCHANGE);
    buf.extend_from_slice(&(body_len as u16).to_le_bytes());
    buf.push(env_type);
    buf.push(new_value.len() as u8);
    buf.extend_from_slice(new_value);
    buf.push(old_value.len() as u8);
    buf.extend_from_slice(old_value);
    buf
}

/// Encode a DONE token.
pub fn encode_done(status: u16, cur_cmd: u16, row_count: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(13);
    buf.push(TOKEN_DONE);
    buf.extend_from_slice(&status.to_le_bytes());
    buf.extend_from_slice(&cur_cmd.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf
}

/// Encode a DONEINPROC token.
pub fn encode_doneinproc(status: u16, cur_cmd: u16, row_count: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(13);
    buf.push(TOKEN_DONEINPROC);
    buf.extend_from_slice(&status.to_le_bytes());
    buf.extend_from_slice(&cur_cmd.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf
}

/// Encode a DONEPROC token.
pub fn encode_doneproc(status: u16, cur_cmd: u16, row_count: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(13);
    buf.push(TOKEN_DONEPROC);
    buf.extend_from_slice(&status.to_le_bytes());
    buf.extend_from_slice(&cur_cmd.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf
}

/// Encode an ERROR token.
pub fn encode_error(number: i32, state: u8, class: u8, message: &str, server: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);

    // Number (LE i32)
    body.extend_from_slice(&number.to_le_bytes());
    // State
    body.push(state);
    // Class (severity)
    body.push(class);
    // MsgText (US_VARCHAR)
    body.extend_from_slice(&encode_us_varchar(message));
    // ServerName (B_VARCHAR)
    body.extend_from_slice(&encode_b_varchar(server));
    // ProcName (B_VARCHAR) - empty
    body.extend_from_slice(&encode_b_varchar(""));
    // LineNumber (LE i32)
    body.extend_from_slice(&0i32.to_le_bytes());

    let mut buf = Vec::with_capacity(3 + body.len());
    buf.push(TOKEN_ERROR);
    buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Encode an INFO token (same format as ERROR but different token type).
pub fn encode_info(number: i32, state: u8, class: u8, message: &str, server: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);

    body.extend_from_slice(&number.to_le_bytes());
    body.push(state);
    body.push(class);
    body.extend_from_slice(&encode_us_varchar(message));
    body.extend_from_slice(&encode_b_varchar(server));
    body.extend_from_slice(&encode_b_varchar(""));
    body.extend_from_slice(&0i32.to_le_bytes());

    let mut buf = Vec::with_capacity(3 + body.len());
    buf.push(TOKEN_INFO);
    buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Encode a RETURNSTATUS token.
pub fn encode_return_status(value: i32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5);
    buf.push(TOKEN_RETURNSTATUS);
    buf.extend_from_slice(&value.to_le_bytes());
    buf
}

/// Build the login response token stream.
///
/// The rdb/ms Go client expects LOGINACK as the FIRST token in the response.
/// ENVCHANGE and other tokens follow, then DONE closes the message.
pub fn build_login_response(database: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);

    // LOGINACK must come first (Go rdb/ms client checks bb[0] == tokenLoginAck)
    buf.extend_from_slice(&encode_loginack("RooDB", 0, 7, 1));

    // ENVCHANGE: database
    buf.extend_from_slice(&encode_envchange_string(ENV_DATABASE, database, database));

    // ENVCHANGE: collation (binary data)
    let collation = [0x09, 0x04, 0xD0, 0x00, 0x34];
    buf.extend_from_slice(&encode_envchange_binary(ENV_COLLATION, &collation, &[]));

    // ENVCHANGE: language
    buf.extend_from_slice(&encode_envchange_string(ENV_LANGUAGE, "us_english", ""));

    // ENVCHANGE: packet size
    buf.extend_from_slice(&encode_envchange_string(ENV_PACKET_SIZE, "4096", "4096"));

    // DONE (final)
    buf.extend_from_slice(&encode_done(DONE_FINAL, 0, 0));

    buf
}

/// Build an error login response.
pub fn build_login_error_response(message: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(&encode_error(18456, 1, 14, message, "RooDB"));
    buf.extend_from_slice(&encode_done(DONE_FINAL, 0, 0));
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_done() {
        let done = encode_done(DONE_FINAL, 0, 0);
        assert_eq!(done[0], TOKEN_DONE);
        // Status = 0x0000
        assert_eq!(done[1], 0);
        assert_eq!(done[2], 0);
    }

    #[test]
    fn test_encode_loginack() {
        let ack = encode_loginack("RooDB", 0, 7, 1);
        assert_eq!(ack[0], TOKEN_LOGINACK);
        // Check that Interface = 1 (T-SQL)
        let len = u16::from_le_bytes([ack[1], ack[2]]) as usize;
        assert_eq!(ack[3], 0x01); // Interface
        assert!(len > 8);
    }

    #[test]
    fn test_build_login_response() {
        let response = build_login_response("master");
        // Should contain LOGINACK token somewhere
        assert!(response.contains(&TOKEN_LOGINACK));
        // Should end with DONE
        // Find last TOKEN_DONE
        let done_positions: Vec<_> = response
            .iter()
            .enumerate()
            .filter(|(_, &b)| b == TOKEN_DONE)
            .map(|(i, _)| i)
            .collect();
        assert!(!done_positions.is_empty());
    }
}
