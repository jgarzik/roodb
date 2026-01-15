//! MySQL command handling
//!
//! Parses and handles MySQL command packets.

use super::error::{ProtocolError, ProtocolResult};

/// MySQL command codes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// COM_SLEEP (internal, not used)
    Sleep = 0x00,
    /// COM_QUIT - close connection
    Quit = 0x01,
    /// COM_INIT_DB - change database
    InitDb = 0x02,
    /// COM_QUERY - execute SQL query
    Query = 0x03,
    /// COM_FIELD_LIST - get column definitions
    FieldList = 0x04,
    /// COM_CREATE_DB (deprecated)
    CreateDb = 0x05,
    /// COM_DROP_DB (deprecated)
    DropDb = 0x06,
    /// COM_REFRESH
    Refresh = 0x07,
    /// COM_SHUTDOWN (deprecated)
    Shutdown = 0x08,
    /// COM_STATISTICS
    Statistics = 0x09,
    /// COM_PROCESS_INFO (deprecated)
    ProcessInfo = 0x0a,
    /// COM_CONNECT (internal)
    Connect = 0x0b,
    /// COM_PROCESS_KILL
    ProcessKill = 0x0c,
    /// COM_DEBUG
    Debug = 0x0d,
    /// COM_PING - server ping
    Ping = 0x0e,
    /// COM_TIME (internal)
    Time = 0x0f,
    /// COM_DELAYED_INSERT (internal)
    DelayedInsert = 0x10,
    /// COM_CHANGE_USER
    ChangeUser = 0x11,
    /// COM_BINLOG_DUMP
    BinlogDump = 0x12,
    /// COM_TABLE_DUMP
    TableDump = 0x13,
    /// COM_CONNECT_OUT (internal)
    ConnectOut = 0x14,
    /// COM_REGISTER_SLAVE
    RegisterSlave = 0x15,
    /// COM_STMT_PREPARE - prepare statement
    StmtPrepare = 0x16,
    /// COM_STMT_EXECUTE - execute prepared statement
    StmtExecute = 0x17,
    /// COM_STMT_SEND_LONG_DATA
    StmtSendLongData = 0x18,
    /// COM_STMT_CLOSE - close prepared statement
    StmtClose = 0x19,
    /// COM_STMT_RESET - reset prepared statement
    StmtReset = 0x1a,
    /// COM_SET_OPTION
    SetOption = 0x1b,
    /// COM_STMT_FETCH
    StmtFetch = 0x1c,
    /// COM_DAEMON (internal)
    Daemon = 0x1d,
    /// COM_BINLOG_DUMP_GTID
    BinlogDumpGtid = 0x1e,
    /// COM_RESET_CONNECTION
    ResetConnection = 0x1f,
}

impl TryFrom<u8> for Command {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Command::Sleep),
            0x01 => Ok(Command::Quit),
            0x02 => Ok(Command::InitDb),
            0x03 => Ok(Command::Query),
            0x04 => Ok(Command::FieldList),
            0x05 => Ok(Command::CreateDb),
            0x06 => Ok(Command::DropDb),
            0x07 => Ok(Command::Refresh),
            0x08 => Ok(Command::Shutdown),
            0x09 => Ok(Command::Statistics),
            0x0a => Ok(Command::ProcessInfo),
            0x0b => Ok(Command::Connect),
            0x0c => Ok(Command::ProcessKill),
            0x0d => Ok(Command::Debug),
            0x0e => Ok(Command::Ping),
            0x0f => Ok(Command::Time),
            0x10 => Ok(Command::DelayedInsert),
            0x11 => Ok(Command::ChangeUser),
            0x12 => Ok(Command::BinlogDump),
            0x13 => Ok(Command::TableDump),
            0x14 => Ok(Command::ConnectOut),
            0x15 => Ok(Command::RegisterSlave),
            0x16 => Ok(Command::StmtPrepare),
            0x17 => Ok(Command::StmtExecute),
            0x18 => Ok(Command::StmtSendLongData),
            0x19 => Ok(Command::StmtClose),
            0x1a => Ok(Command::StmtReset),
            0x1b => Ok(Command::SetOption),
            0x1c => Ok(Command::StmtFetch),
            0x1d => Ok(Command::Daemon),
            0x1e => Ok(Command::BinlogDumpGtid),
            0x1f => Ok(Command::ResetConnection),
            _ => Err(ProtocolError::Unsupported(format!(
                "unknown command: 0x{:02x}",
                value
            ))),
        }
    }
}

/// Parsed command with payload
#[derive(Debug)]
pub enum ParsedCommand {
    /// COM_QUIT
    Quit,
    /// COM_INIT_DB with database name
    InitDb(String),
    /// COM_QUERY with SQL query string
    Query(String),
    /// COM_PING
    Ping,
    /// COM_STMT_PREPARE with SQL
    StmtPrepare(String),
    /// COM_STMT_EXECUTE with statement ID and parameters
    StmtExecute {
        statement_id: u32,
        // Parameters would go here for full implementation
    },
    /// COM_STMT_CLOSE with statement ID
    StmtClose(u32),
    /// COM_RESET_CONNECTION
    ResetConnection,
    /// Unsupported command
    Unsupported(Command),
}

/// Parse a command packet
pub fn parse_command(packet: &[u8]) -> ProtocolResult<ParsedCommand> {
    if packet.is_empty() {
        return Err(ProtocolError::InvalidPacket("empty command packet".to_string()));
    }

    let cmd = Command::try_from(packet[0])?;
    let payload = &packet[1..];

    match cmd {
        Command::Quit => Ok(ParsedCommand::Quit),

        Command::InitDb => {
            let db = String::from_utf8(payload.to_vec())
                .map_err(|_| ProtocolError::InvalidPacket("invalid UTF-8 in database name".to_string()))?;
            Ok(ParsedCommand::InitDb(db))
        }

        Command::Query => {
            let sql = String::from_utf8(payload.to_vec())
                .map_err(|_| ProtocolError::InvalidPacket("invalid UTF-8 in query".to_string()))?;
            Ok(ParsedCommand::Query(sql))
        }

        Command::Ping => Ok(ParsedCommand::Ping),

        Command::StmtPrepare => {
            let sql = String::from_utf8(payload.to_vec())
                .map_err(|_| ProtocolError::InvalidPacket("invalid UTF-8 in prepared statement".to_string()))?;
            Ok(ParsedCommand::StmtPrepare(sql))
        }

        Command::StmtExecute => {
            if payload.len() < 4 {
                return Err(ProtocolError::InvalidPacket("STMT_EXECUTE too short".to_string()));
            }
            let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(ParsedCommand::StmtExecute { statement_id })
        }

        Command::StmtClose => {
            if payload.len() < 4 {
                return Err(ProtocolError::InvalidPacket("STMT_CLOSE too short".to_string()));
            }
            let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(ParsedCommand::StmtClose(statement_id))
        }

        Command::ResetConnection => Ok(ParsedCommand::ResetConnection),

        _ => Ok(ParsedCommand::Unsupported(cmd)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        let packet = vec![0x01];
        let cmd = parse_command(&packet).unwrap();
        assert!(matches!(cmd, ParsedCommand::Quit));
    }

    #[test]
    fn test_parse_ping() {
        let packet = vec![0x0e];
        let cmd = parse_command(&packet).unwrap();
        assert!(matches!(cmd, ParsedCommand::Ping));
    }

    #[test]
    fn test_parse_query() {
        let mut packet = vec![0x03];
        packet.extend_from_slice(b"SELECT 1");
        let cmd = parse_command(&packet).unwrap();
        match cmd {
            ParsedCommand::Query(sql) => assert_eq!(sql, "SELECT 1"),
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_init_db() {
        let mut packet = vec![0x02];
        packet.extend_from_slice(b"testdb");
        let cmd = parse_command(&packet).unwrap();
        match cmd {
            ParsedCommand::InitDb(db) => assert_eq!(db, "testdb"),
            _ => panic!("expected InitDb"),
        }
    }

    #[test]
    fn test_command_try_from() {
        assert_eq!(Command::try_from(0x01).unwrap(), Command::Quit);
        assert_eq!(Command::try_from(0x03).unwrap(), Command::Query);
        assert!(Command::try_from(0xff).is_err());
    }
}
