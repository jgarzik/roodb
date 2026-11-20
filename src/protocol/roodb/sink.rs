//! RooDB-protocol encoder for query results.

use std::io;

use async_trait::async_trait;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::executor::row::Row;
use crate::planner::logical::OutputColumn;
use crate::server::query::{OkInfo, QueryError, ResponseSink, Status};

use super::binary::encode_binary_row;
use super::handshake::status_flags;
use super::packet::MAX_PACKET_SIZE;
use super::resultset::{
    encode_column_count, encode_eof_ok_packet, encode_eof_packet, encode_err_packet,
    encode_ok_packet, encode_text_row, ColumnDefinition41,
};

/// Row encoding for result sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFormat {
    /// Text protocol (COM_QUERY)
    Text,
    /// Binary protocol (COM_STMT_EXECUTE)
    Binary,
}

/// The packet that ends a result, held back until we know whether another
/// result follows it (which sets SERVER_MORE_RESULTS_EXISTS).
enum Terminator {
    /// End of a result set's rows
    EndOfRows(Status),
    /// OK for a statement without rows
    Ok(OkInfo, Status),
}

/// Encodes query results as RooDB protocol packets.
///
/// Packets are framed into an in-memory buffer and written out by `flush`.
pub struct RooDbSink<W> {
    writer: W,
    buf: Vec<u8>,
    sequence_id: u8,
    format: RowFormat,
    /// Client set CLIENT_DEPRECATE_EOF
    deprecate_eof: bool,
    /// Client set CLIENT_MULTI_RESULTS
    multi_results: bool,
    /// Columns of the result set being sent (binary rows need the types)
    columns: Vec<OutputColumn>,
    pending: Option<Terminator>,
}

impl<W: AsyncWrite + Unpin + Send> RooDbSink<W> {
    /// Create a sink writing to `writer`.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buf: Vec::with_capacity(8192),
            sequence_id: 0,
            format: RowFormat::Text,
            deprecate_eof: false,
            multi_results: false,
            columns: Vec::new(),
            pending: None,
        }
    }

    /// Record the capabilities the client negotiated.
    pub fn set_client_capabilities(&mut self, deprecate_eof: bool, multi_results: bool) {
        self.deprecate_eof = deprecate_eof;
        self.multi_results = multi_results;
    }

    /// Whether the client negotiated CLIENT_DEPRECATE_EOF.
    pub fn deprecate_eof(&self) -> bool {
        self.deprecate_eof
    }

    /// Begin the response to a command. The command packet used sequence 0,
    /// so the response starts at 1.
    pub fn start_command(&mut self, format: RowFormat) {
        self.sequence_id = 1;
        self.format = format;
        self.pending = None;
    }

    /// Set the sequence ID of the next packet (handshake exchanges).
    pub fn set_sequence(&mut self, sequence_id: u8) {
        self.sequence_id = sequence_id;
    }

    /// Frame a raw packet payload into the output buffer, splitting payloads
    /// of 2^24-1 bytes or more.
    pub fn packet(&mut self, payload: &[u8]) {
        let mut offset = 0;
        loop {
            let chunk = (payload.len() - offset).min(MAX_PACKET_SIZE);
            let len = (chunk as u32).to_le_bytes();
            self.buf
                .extend_from_slice(&[len[0], len[1], len[2], self.sequence_id]);
            self.sequence_id = self.sequence_id.wrapping_add(1);
            self.buf.extend_from_slice(&payload[offset..offset + chunk]);
            offset += chunk;
            // A full-size chunk must be followed by another (possibly empty) one.
            if chunk < MAX_PACKET_SIZE {
                break;
            }
        }
    }

    /// Complete the response: emit the held-back terminator and flush.
    pub async fn finish(&mut self) -> io::Result<()> {
        self.emit_pending(false);
        self.flush().await
    }

    fn status_bits(status: Status, more_results: bool) -> u16 {
        let mut bits = 0;
        if status.autocommit {
            bits |= status_flags::SERVER_STATUS_AUTOCOMMIT;
        }
        if status.in_transaction {
            bits |= status_flags::SERVER_STATUS_IN_TRANS;
        }
        if more_results {
            bits |= status_flags::SERVER_MORE_RESULTS_EXISTS;
        }
        bits
    }

    /// Emit the held-back terminator, if any. `more` says another result
    /// follows; it is only signalled to clients that accept multi-results.
    fn emit_pending(&mut self, more: bool) {
        let more = more && self.multi_results;
        match self.pending.take() {
            Some(Terminator::EndOfRows(status)) => {
                let bits = Self::status_bits(status, more);
                let packet = if self.deprecate_eof {
                    encode_eof_ok_packet(bits, 0)
                } else {
                    encode_eof_packet(0, bits)
                };
                self.packet(&packet);
            }
            Some(Terminator::Ok(info, status)) => {
                let bits = Self::status_bits(status, more);
                let packet = encode_ok_packet(info.affected_rows, info.last_insert_id, bits, 0);
                self.packet(&packet);
            }
            None => {}
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> ResponseSink for RooDbSink<W> {
    fn begin_result_set(&mut self, columns: &[OutputColumn], schema: &str) {
        self.emit_pending(true);

        self.packet(&encode_column_count(columns.len() as u64));
        for col in columns {
            let def = ColumnDefinition41::from_output_column(col, "", schema);
            self.packet(&def.encode());
        }
        if !self.deprecate_eof {
            let eof = encode_eof_packet(0, Self::status_bits(Status::AUTOCOMMIT, false));
            self.packet(&eof);
        }

        self.columns.clear();
        self.columns.extend_from_slice(columns);
    }

    fn row(&mut self, row: &Row) {
        let packet = match self.format {
            RowFormat::Text => encode_text_row(row),
            RowFormat::Binary => encode_binary_row(row, &self.columns),
        };
        self.packet(&packet);
    }

    fn end_result_set(&mut self, status: Status) {
        self.pending = Some(Terminator::EndOfRows(status));
    }

    fn ok(&mut self, info: OkInfo, status: Status) {
        self.emit_pending(true);
        self.pending = Some(Terminator::Ok(info, status));
    }

    fn error(&mut self, error: &QueryError) {
        self.emit_pending(true);
        self.packet(&encode_err_packet(
            error.code,
            &error.sqlstate,
            &error.message,
        ));
    }

    fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    async fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.writer.write_all(&self.buf).await?;
            self.buf.clear();
        }
        self.writer.flush().await
    }
}
