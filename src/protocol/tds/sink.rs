//! TDS encoder for query results.

use std::any::Any;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWrite;

use crate::executor::row::Row;
use crate::planner::logical::OutputColumn;
use crate::server::query::{OkInfo, QueryError, ResponseSink, Status};

use super::codec::{PacketError, PacketType, TdsWriter};
use super::token::{
    put_done, put_error, put_return_status, put_return_value_int, sql_server_error, DONE_ATTN,
    DONE_COUNT, DONE_ERROR, DONE_FINAL, DONE_MORE, TOKEN_DONE, TOKEN_DONEINPROC, TOKEN_DONEPROC,
};
use super::types::{declared_wire_types, put_colmetadata, put_row, reconcile_wire_types, WireType};

/// Error number for a value the server could not encode for the client
const ERROR_UNENCODABLE_VALUE: i32 = 50000;

/// A DONE token held back until it is known whether another result
/// follows (which sets DONE_MORE).
struct PendingDone {
    /// DONE, DONEINPROC or DONEPROC
    token: u8,
    status: u16,
    row_count: u64,
}

/// Encodes query results as a TDS token stream.
///
/// Tokens accumulate in a buffer; `flush` sends the whole packets
/// available and `finish` completes the response message.
pub struct TdsSink<W> {
    writer: TdsWriter<W>,
    buf: Vec<u8>,
    /// Columns of the result set being sent
    columns: Vec<OutputColumn>,
    /// Wire types of the result set being sent
    wire_types: Vec<WireType>,
    /// Column metadata is held back until the first row, so it can
    /// describe what the executor actually produces
    metadata_sent: bool,
    /// Rows sent in the current result set
    rows: u64,
    /// First value that could not be encoded in the current result set
    row_error: Option<String>,
    pending: Option<PendingDone>,
    /// Results are being produced by a procedure call (RPC): statements
    /// end with DONEINPROC and the call with DONEPROC
    in_procedure: bool,
    /// A statement of the current procedure call failed
    procedure_failed: bool,
    /// Set by the connection's reader when the client cancels the request
    cancel: Arc<AtomicBool>,
}

fn io_error(e: PacketError) -> io::Error {
    match e {
        PacketError::Io(e) => e,
        other => io::Error::other(other),
    }
}

impl<W: AsyncWrite + Unpin + Send + Sync + 'static> TdsSink<W> {
    /// Create a sink writing to `writer`. `cancel` is raised by the
    /// connection when the client sends an attention signal.
    pub fn new(writer: W, cancel: Arc<AtomicBool>) -> Self {
        Self {
            writer: TdsWriter::new(writer),
            buf: Vec::with_capacity(8192),
            columns: Vec::new(),
            wire_types: Vec::new(),
            metadata_sent: true,
            rows: 0,
            row_error: None,
            pending: None,
            in_procedure: false,
            procedure_failed: false,
            cancel,
        }
    }

    /// The packet writer, for messages sent outside a query response.
    pub fn writer(&mut self) -> &mut TdsWriter<W> {
        &mut self.writer
    }

    /// The token buffer, so the connection can add tokens that belong to
    /// the response in progress (environment changes). They land before
    /// the DONE that is still held back.
    pub fn tokens(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Start reporting the results of a procedure call (RPC).
    pub fn begin_procedure(&mut self) {
        self.emit_pending(true);
        self.in_procedure = true;
        self.procedure_failed = false;
    }

    /// Finish a procedure call: return status, integer output parameters
    /// (name, value), and the DONEPROC that closes it.
    pub fn end_procedure(&mut self, return_values: &[(&str, i32)]) {
        self.emit_pending(true);
        put_return_status(&mut self.buf, 0);
        for (name, value) in return_values {
            put_return_value_int(&mut self.buf, name, *value);
        }
        self.in_procedure = false;
        self.pending = Some(PendingDone {
            token: TOKEN_DONEPROC,
            status: if self.procedure_failed {
                DONE_ERROR
            } else {
                DONE_FINAL
            },
            row_count: 0,
        });
    }

    /// Whether the request produced any result yet.
    pub fn has_result(&self) -> bool {
        self.pending.is_some() || !self.buf.is_empty()
    }

    /// Complete the response message. If the client cancelled the request,
    /// the final DONE acknowledges the attention signal.
    pub async fn finish(&mut self, acknowledge_attention: bool) -> io::Result<()> {
        if acknowledge_attention {
            self.emit_pending(true);
            put_done(&mut self.buf, TOKEN_DONE, DONE_ATTN, 0);
        } else {
            if self.pending.is_none() {
                // Every response ends with a DONE, including one that
                // carries only informational tokens.
                self.pending = Some(PendingDone {
                    token: TOKEN_DONE,
                    status: DONE_FINAL,
                    row_count: 0,
                });
            }
            self.emit_pending(false);
        }
        let result = self
            .writer
            .finish_message(PacketType::TabularResult, &self.buf)
            .await
            .map_err(io_error);
        self.buf.clear();
        result
    }

    fn emit_pending(&mut self, more: bool) {
        if let Some(done) = self.pending.take() {
            let status = if more {
                done.status | DONE_MORE
            } else {
                done.status
            };
            put_done(&mut self.buf, done.token, status, done.row_count);
        }
    }

    /// The token that ends a statement's result.
    fn statement_done(&self) -> u8 {
        if self.in_procedure {
            TOKEN_DONEINPROC
        } else {
            TOKEN_DONE
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send + Sync + 'static> ResponseSink for TdsSink<W> {
    fn accepts_multiple_results(&self) -> bool {
        true
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    fn begin_result_set(&mut self, columns: &[OutputColumn], _schema: &str) {
        self.emit_pending(true);
        self.columns.clear();
        self.columns.extend_from_slice(columns);
        self.metadata_sent = false;
        self.rows = 0;
        self.row_error = None;
    }

    fn row(&mut self, row: &Row) {
        if self.row_error.is_some() {
            return;
        }
        if !self.metadata_sent {
            self.wire_types = reconcile_wire_types(&self.columns, row.values());
            put_colmetadata(&mut self.buf, &self.columns, &self.wire_types);
            self.metadata_sent = true;
        }
        match put_row(&mut self.buf, &self.wire_types, row.values()) {
            Ok(()) => self.rows += 1,
            Err(e) => self.row_error = Some(e.to_string()),
        }
    }

    fn end_result_set(&mut self, _status: Status) {
        if !self.metadata_sent {
            // No rows: describe the columns as declared.
            self.wire_types = declared_wire_types(&self.columns);
            put_colmetadata(&mut self.buf, &self.columns, &self.wire_types);
            self.metadata_sent = true;
        }
        let mut status = DONE_COUNT;
        if let Some(message) = self.row_error.take() {
            // The rows before the bad value were sent; report why the
            // result set stops here.
            put_error(&mut self.buf, ERROR_UNENCODABLE_VALUE, 1, 16, &message);
            status |= DONE_ERROR;
        }
        self.pending = Some(PendingDone {
            token: self.statement_done(),
            status,
            row_count: self.rows,
        });
    }

    fn ok(&mut self, info: OkInfo, _status: Status) {
        self.emit_pending(true);
        self.pending = Some(PendingDone {
            token: self.statement_done(),
            status: DONE_COUNT,
            row_count: info.affected_rows,
        });
    }

    fn error(&mut self, error: &QueryError) {
        self.emit_pending(true);
        // An error before the first row replaces the result set.
        self.metadata_sent = true;
        let (number, class, state) = sql_server_error(error.code);
        put_error(&mut self.buf, number, state, class, &error.message);
        self.procedure_failed = true;
        self.pending = Some(PendingDone {
            token: self.statement_done(),
            status: DONE_ERROR,
            row_count: 0,
        });
    }

    fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.writer
            .write_partial(PacketType::TabularResult, &mut self.buf)
            .await
            .map_err(io_error)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::executor::datum::Datum;
    use crate::protocol::tds::codec::{TdsReader, HEADER_SIZE};
    use crate::protocol::tds::token::{TOKEN_COLMETADATA, TOKEN_ERROR, TOKEN_ROW};

    type TestSink = TdsSink<tokio::io::DuplexStream>;

    fn sink() -> (TestSink, tokio::io::DuplexStream, Arc<AtomicBool>) {
        let (server, client) = tokio::io::duplex(1 << 20);
        let cancel = Arc::new(AtomicBool::new(false));
        (TdsSink::new(server, cancel.clone()), client, cancel)
    }

    async fn response(client: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let msg = TdsReader::new(client).read_message(1 << 24).await.unwrap();
        assert_eq!(msg.packet_type, PacketType::TabularResult);
        msg.payload
    }

    /// (status, row_count) of the DONE token at `pos`.
    fn done_at(tokens: &[u8], pos: usize) -> (u16, u64) {
        assert_eq!(tokens[pos], TOKEN_DONE);
        (
            u16::from_le_bytes([tokens[pos + 1], tokens[pos + 2]]),
            u64::from_le_bytes(tokens[pos + 5..pos + 13].try_into().unwrap()),
        )
    }

    fn int_column() -> Vec<OutputColumn> {
        vec![OutputColumn {
            id: 0,
            name: "n".to_string(),
            data_type: DataType::Int,
            nullable: true,
        }]
    }

    #[tokio::test]
    async fn ok_is_a_single_final_done_with_count() {
        let (mut sink, mut client, _) = sink();
        sink.ok(
            OkInfo {
                affected_rows: 3,
                last_insert_id: 0,
            },
            Status::AUTOCOMMIT,
        );
        sink.finish(false).await.unwrap();
        let tokens = response(&mut client).await;
        assert_eq!(tokens.len(), 13);
        assert_eq!(done_at(&tokens, 0), (DONE_COUNT, 3));
    }

    #[tokio::test]
    async fn result_set_then_ok_sets_more_on_the_first_done() {
        let (mut sink, mut client, _) = sink();
        sink.begin_result_set(&int_column(), "");
        sink.row(&Row::new(vec![Datum::Int(7)]));
        sink.row(&Row::new(vec![Datum::Null]));
        sink.end_result_set(Status::AUTOCOMMIT);
        sink.ok(OkInfo::default(), Status::AUTOCOMMIT);
        sink.finish(false).await.unwrap();

        let tokens = response(&mut client).await;
        assert_eq!(tokens[0], TOKEN_COLMETADATA);
        let rows = tokens.iter().filter(|b| **b == TOKEN_ROW).count();
        assert!(rows >= 2);
        let last = tokens.len() - 13;
        assert_eq!(done_at(&tokens, last - 13), (DONE_COUNT | DONE_MORE, 2));
        assert_eq!(done_at(&tokens, last), (DONE_COUNT, 0));
    }

    #[tokio::test]
    async fn error_is_followed_by_done_error() {
        let (mut sink, mut client, _) = sink();
        sink.error(&QueryError::new(1146, "42S02", "Table 't' doesn't exist"));
        sink.finish(false).await.unwrap();
        let tokens = response(&mut client).await;
        assert_eq!(tokens[0], TOKEN_ERROR);
        assert_eq!(i32::from_le_bytes(tokens[3..7].try_into().unwrap()), 208);
        assert_eq!(done_at(&tokens, tokens.len() - 13), (DONE_ERROR, 0));
    }

    #[tokio::test]
    async fn unencodable_value_ends_the_result_set_with_an_error() {
        let (mut sink, mut client, _) = sink();
        sink.begin_result_set(&int_column(), "");
        sink.row(&Row::new(vec![Datum::Int(1)]));
        sink.row(&Row::new(vec![Datum::String("not a number".into())]));
        sink.row(&Row::new(vec![Datum::Int(3)])); // dropped after the error
        sink.end_result_set(Status::AUTOCOMMIT);
        sink.finish(false).await.unwrap();

        let tokens = response(&mut client).await;
        let done = tokens.len() - 13;
        assert_eq!(done_at(&tokens, done), (DONE_COUNT | DONE_ERROR, 1));
        assert!(tokens[..done].contains(&TOKEN_ERROR));
    }

    #[tokio::test]
    async fn procedure_results_use_doneinproc_and_doneproc() {
        use crate::protocol::tds::token::{TOKEN_RETURNSTATUS, TOKEN_RETURNVALUE};

        let (mut sink, mut client, _) = sink();
        sink.begin_procedure();
        sink.ok(
            OkInfo {
                affected_rows: 2,
                last_insert_id: 0,
            },
            Status::AUTOCOMMIT,
        );
        sink.end_procedure(&[("@handle", 5)]);
        sink.finish(false).await.unwrap();

        let tokens = response(&mut client).await;
        // DONEINPROC(count, more) RETURNSTATUS RETURNVALUE DONEPROC(final)
        assert_eq!(tokens[0], TOKEN_DONEINPROC);
        assert_eq!(
            u16::from_le_bytes([tokens[1], tokens[2]]),
            DONE_COUNT | DONE_MORE
        );
        assert_eq!(tokens[13], TOKEN_RETURNSTATUS);
        assert_eq!(tokens[18], TOKEN_RETURNVALUE);
        let last = tokens.len() - 13;
        assert_eq!(tokens[last], TOKEN_DONEPROC);
        assert_eq!(u16::from_le_bytes([tokens[last + 1], tokens[last + 2]]), 0);

        // A failed statement marks the procedure's DONEPROC as an error.
        sink.begin_procedure();
        sink.error(&QueryError::new(1146, "42S02", "no such table"));
        sink.end_procedure(&[]);
        sink.finish(false).await.unwrap();
        let tokens = response(&mut client).await;
        let last = tokens.len() - 13;
        assert_eq!(tokens[last], TOKEN_DONEPROC);
        assert_eq!(
            u16::from_le_bytes([tokens[last + 1], tokens[last + 2]]),
            DONE_ERROR
        );
    }

    #[tokio::test]
    async fn empty_response_still_ends_with_done() {
        let (mut sink, mut client, _) = sink();
        assert!(!sink.has_result());
        sink.finish(false).await.unwrap();
        let tokens = response(&mut client).await;
        assert_eq!(done_at(&tokens, 0), (DONE_FINAL, 0));
    }

    #[tokio::test]
    async fn cancelled_request_is_acknowledged_with_done_attn() {
        let (mut sink, mut client, cancel) = sink();
        assert!(!sink.cancelled());
        cancel.store(true, Ordering::Relaxed);
        assert!(sink.cancelled());

        sink.begin_result_set(&int_column(), "");
        sink.row(&Row::new(vec![Datum::Int(1)]));
        sink.end_result_set(Status::AUTOCOMMIT);
        sink.finish(true).await.unwrap();

        let tokens = response(&mut client).await;
        let last = tokens.len() - 13;
        assert_eq!(done_at(&tokens, last - 13), (DONE_COUNT | DONE_MORE, 1));
        assert_eq!(done_at(&tokens, last), (DONE_ATTN, 0));
    }

    #[tokio::test]
    async fn flush_streams_whole_packets_of_one_message() {
        let (mut sink, mut client, _) = sink();
        sink.begin_result_set(&int_column(), "");
        for i in 0..2000 {
            sink.row(&Row::new(vec![Datum::Int(i)]));
        }
        let before = sink.buffered_bytes();
        sink.flush().await.unwrap();
        let body = sink.writer().packet_size() - HEADER_SIZE;
        assert!(
            sink.buffered_bytes() < body,
            "only a partial packet remains"
        );
        assert!(before > sink.buffered_bytes());
        sink.end_result_set(Status::AUTOCOMMIT);
        sink.finish(false).await.unwrap();

        // The reader reassembles one message from the partial writes.
        let tokens = response(&mut client).await;
        assert_eq!(done_at(&tokens, tokens.len() - 13), (DONE_COUNT, 2000));
    }
}
