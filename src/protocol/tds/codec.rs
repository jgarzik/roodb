//! TDS packet framing: 8-byte header, multi-packet message assembly.
//!
//! A TDS message is one or more packets of the same type; the last has the
//! end-of-message status bit. Every length here comes from the peer, so
//! each packet is checked against the largest legal packet and each
//! assembled message against a caller-supplied cap.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// TDS packet header size
pub const HEADER_SIZE: usize = 8;
/// Packet size both sides use until login negotiates another
pub const DEFAULT_PACKET_SIZE: usize = 4096;
/// Smallest packet size a client may negotiate
pub const MIN_PACKET_SIZE: usize = 512;
/// Largest packet size TDS allows
pub const MAX_PACKET_SIZE: usize = 32767;

/// TDS packet types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    SqlBatch = 1,
    Rpc = 3,
    TabularResult = 4,
    Attention = 6,
    BulkLoad = 7,
    TransactionManager = 0x0E,
    Tds7Login = 0x10,
    PreLogin = 0x12,
}

impl TryFrom<u8> for PacketType {
    type Error = PacketError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::SqlBatch),
            3 => Ok(Self::Rpc),
            4 => Ok(Self::TabularResult),
            6 => Ok(Self::Attention),
            7 => Ok(Self::BulkLoad),
            0x0E => Ok(Self::TransactionManager),
            0x10 => Ok(Self::Tds7Login),
            0x12 => Ok(Self::PreLogin),
            _ => Err(PacketError::UnknownType(v)),
        }
    }
}

/// Last packet of the message
pub const STATUS_EOM: u8 = 0x01;
/// Client abandons the message it was sending (set together with EOM)
pub const STATUS_IGNORE: u8 = 0x02;
/// Reset session state before running this request (connection pooling)
pub const STATUS_RESET_CONNECTION: u8 = 0x08;
/// As `STATUS_RESET_CONNECTION`, but keep the open transaction
pub const STATUS_RESET_CONNECTION_SKIP_TRAN: u8 = 0x10;

#[derive(Debug, thiserror::Error)]
pub enum PacketError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown packet type: 0x{0:02x}")]
    UnknownType(u8),
    #[error("invalid packet length: {0}")]
    BadLength(usize),
    #[error("message exceeds {0} bytes")]
    TooLarge(usize),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("unexpected packet type: expected {expected:?}, got {got:?}")]
    UnexpectedType {
        expected: PacketType,
        got: PacketType,
    },
}

/// A complete TDS message.
#[derive(Debug)]
pub struct Message {
    /// Packet type shared by every packet of the message
    pub packet_type: PacketType,
    /// Status bits of the first packet (reset-connection requests)
    pub status: u8,
    /// Concatenated packet bodies
    pub payload: Vec<u8>,
}

impl Message {
    /// Whether the client asked for the session to be reset first.
    pub fn resets_connection(&self) -> bool {
        self.status & (STATUS_RESET_CONNECTION | STATUS_RESET_CONNECTION_SKIP_TRAN) != 0
    }

    /// Whether a requested reset must leave the open transaction alone.
    pub fn reset_keeps_transaction(&self) -> bool {
        self.status & STATUS_RESET_CONNECTION_SKIP_TRAN != 0
    }
}

/// Reads complete TDS messages (potentially spanning multiple packets).
pub struct TdsReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> TdsReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read a complete message of at most `max_len` payload bytes,
    /// assembling packets until end-of-message.
    ///
    /// A message the client marks as ignored is discarded and the next one
    /// is returned instead.
    pub async fn read_message(&mut self, max_len: usize) -> Result<Message, PacketError> {
        loop {
            let (message, ignored) = self.read_one(max_len).await?;
            if !ignored {
                return Ok(message);
            }
        }
    }

    async fn read_one(&mut self, max_len: usize) -> Result<(Message, bool), PacketError> {
        let mut payload = Vec::new();
        let mut first: Option<(PacketType, u8)> = None;

        loop {
            let mut header = [0u8; HEADER_SIZE];
            match self.reader.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(PacketError::ConnectionClosed);
                }
                Err(e) => return Err(PacketError::Io(e)),
            }

            let packet_type = PacketType::try_from(header[0])?;
            let status = header[1];
            let length = u16::from_be_bytes([header[2], header[3]]) as usize;

            if !(HEADER_SIZE..=MAX_PACKET_SIZE).contains(&length) {
                return Err(PacketError::BadLength(length));
            }

            // Every packet of a message carries the same type
            let (message_type, first_status) = *first.get_or_insert((packet_type, status));
            if packet_type != message_type {
                return Err(PacketError::UnexpectedType {
                    expected: message_type,
                    got: packet_type,
                });
            }

            let body_len = length - HEADER_SIZE;
            if payload.len() + body_len > max_len {
                return Err(PacketError::TooLarge(max_len));
            }
            if body_len > 0 {
                let start = payload.len();
                payload.resize(start + body_len, 0);
                self.reader.read_exact(&mut payload[start..]).await?;
            }

            if status & STATUS_EOM != 0 {
                let ignored = status & STATUS_IGNORE != 0;
                let message = Message {
                    packet_type: message_type,
                    status: first_status,
                    payload,
                };
                return Ok((message, ignored));
            }
        }
    }
}

/// Writes TDS messages, splitting them into packets.
///
/// A long response can be sent in parts: `write_partial` sends whole
/// packets as data becomes available and `finish_message` sends the rest
/// with the end-of-message bit.
pub struct TdsWriter<W> {
    writer: W,
    packet_size: usize,
    packet_number: u8,
    frame: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> TdsWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            packet_size: DEFAULT_PACKET_SIZE,
            packet_number: 1,
            frame: Vec::with_capacity(DEFAULT_PACKET_SIZE),
        }
    }

    /// Set the negotiated packet size, clamped to what TDS allows.
    pub fn set_packet_size(&mut self, packet_size: usize) {
        self.packet_size = packet_size.clamp(MIN_PACKET_SIZE, MAX_PACKET_SIZE);
    }

    /// Packet size in use.
    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    /// Send the whole packets available in `data`, leaving any remainder
    /// (less than one packet body) in `data` for a later call.
    pub async fn write_partial(
        &mut self,
        packet_type: PacketType,
        data: &mut Vec<u8>,
    ) -> Result<(), PacketError> {
        let body_size = self.packet_size - HEADER_SIZE;
        let whole = data.len() / body_size * body_size;
        if whole == 0 {
            return Ok(());
        }
        self.frame.clear();
        for body in data[..whole].chunks(body_size) {
            self.push_packet(packet_type, body, false);
        }
        data.drain(..whole);
        self.writer.write_all(&self.frame).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Send `data` as the end of the current message and start a new one.
    pub async fn finish_message(
        &mut self,
        packet_type: PacketType,
        data: &[u8],
    ) -> Result<(), PacketError> {
        let body_size = self.packet_size - HEADER_SIZE;
        self.frame.clear();
        if data.is_empty() {
            self.push_packet(packet_type, &[], true);
        } else {
            let mut chunks = data.chunks(body_size).peekable();
            while let Some(body) = chunks.next() {
                self.push_packet(packet_type, body, chunks.peek().is_none());
            }
        }
        self.packet_number = 1;
        self.writer.write_all(&self.frame).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Send a complete message.
    pub async fn write_message(
        &mut self,
        packet_type: PacketType,
        data: &[u8],
    ) -> Result<(), PacketError> {
        self.finish_message(packet_type, data).await
    }

    fn push_packet(&mut self, packet_type: PacketType, body: &[u8], eom: bool) {
        let length = (HEADER_SIZE + body.len()) as u16;
        let length = length.to_be_bytes();
        self.frame.extend_from_slice(&[
            packet_type as u8,
            if eom { STATUS_EOM } else { 0 },
            length[0],
            length[1],
            0, // SPID (2 bytes), unused
            0,
            self.packet_number,
            0, // window, unused
        ]);
        self.frame.extend_from_slice(body);
        self.packet_number = self.packet_number.wrapping_add(1);
    }

    /// Get mutable reference to the underlying writer
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(packet_type: u8, status: u8, body: &[u8]) -> Vec<u8> {
        let len = ((HEADER_SIZE + body.len()) as u16).to_be_bytes();
        let mut p = vec![packet_type, status, len[0], len[1], 0, 0, 1, 0];
        p.extend_from_slice(body);
        p
    }

    async fn read(bytes: &[u8], max_len: usize) -> Result<Message, PacketError> {
        TdsReader::new(bytes).read_message(max_len).await
    }

    #[tokio::test]
    async fn round_trip_single_packet() {
        let mut buf = Vec::new();
        TdsWriter::new(&mut buf)
            .write_message(PacketType::SqlBatch, b"SELECT 1")
            .await
            .unwrap();
        assert_eq!(buf.len(), HEADER_SIZE + 8);

        let msg = read(&buf, 1024).await.unwrap();
        assert_eq!(msg.packet_type, PacketType::SqlBatch);
        assert_eq!(msg.payload, b"SELECT 1");
        assert!(!msg.resets_connection());
    }

    #[tokio::test]
    async fn round_trip_multi_packet_and_empty() {
        let data: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();
        let mut buf = Vec::new();
        let mut w = TdsWriter::new(&mut buf);
        w.set_packet_size(512);
        w.write_message(PacketType::TabularResult, &data)
            .await
            .unwrap();
        w.write_message(PacketType::TabularResult, &[])
            .await
            .unwrap();

        let mut r = TdsReader::new(buf.as_slice());
        let msg = r.read_message(1 << 20).await.unwrap();
        assert_eq!(msg.payload, data);
        let empty = r.read_message(1 << 20).await.unwrap();
        assert!(empty.payload.is_empty());
    }

    #[tokio::test]
    async fn partial_writes_form_one_message() {
        let mut buf = Vec::new();
        let mut w = TdsWriter::new(&mut buf);
        w.set_packet_size(512);
        let body = 512 - HEADER_SIZE;

        let mut pending = vec![7u8; body * 2 + 10];
        w.write_partial(PacketType::TabularResult, &mut pending)
            .await
            .unwrap();
        assert_eq!(pending.len(), 10, "less than a packet stays buffered");
        w.finish_message(PacketType::TabularResult, &pending)
            .await
            .unwrap();

        // Packet numbers run 1, 2, 3 and only the last packet has EOM.
        assert_eq!((buf[1], buf[6]), (0, 1));
        assert_eq!((buf[512 + 1], buf[512 + 6]), (0, 2));
        assert_eq!((buf[1024 + 1], buf[1024 + 6]), (STATUS_EOM, 3));

        let msg = read(&buf, 1 << 20).await.unwrap();
        assert_eq!(msg.payload, vec![7u8; body * 2 + 10]);
    }

    #[tokio::test]
    async fn message_cap_is_enforced_before_reading_the_body() {
        // Three packets without EOM, 100 bytes each; cap at 250.
        let mut bytes = Vec::new();
        for _ in 0..3 {
            bytes.extend(packet(0x12, 0, &[0u8; 100]));
        }
        assert!(matches!(
            read(&bytes, 250).await,
            Err(PacketError::TooLarge(250))
        ));
    }

    #[tokio::test]
    async fn bad_lengths_and_types_are_rejected() {
        // Length smaller than the header
        let short = [0x01, 0x01, 0x00, 0x04, 0, 0, 1, 0];
        assert!(matches!(
            read(&short, 1024).await,
            Err(PacketError::BadLength(4))
        ));
        // Length above the TDS maximum
        let long = [0x01, 0x01, 0xFF, 0xFF, 0, 0, 1, 0];
        assert!(matches!(
            read(&long, 1 << 20).await,
            Err(PacketError::BadLength(65535))
        ));
        // Unknown type
        let unknown = packet(0x55, STATUS_EOM, b"");
        assert!(matches!(
            read(&unknown, 1024).await,
            Err(PacketError::UnknownType(0x55))
        ));
        // Type changes mid-message
        let mut mixed = packet(0x01, 0, b"a");
        mixed.extend(packet(0x03, STATUS_EOM, b"b"));
        assert!(matches!(
            read(&mixed, 1024).await,
            Err(PacketError::UnexpectedType { .. })
        ));
    }

    #[tokio::test]
    async fn truncated_input_is_connection_closed_or_io() {
        assert!(matches!(
            read(&[0x01, 0x01, 0x00], 1024).await,
            Err(PacketError::ConnectionClosed)
        ));
        // Header promises 10 body bytes, only 2 arrive
        let mut bytes = packet(0x01, STATUS_EOM, &[0u8; 10]);
        bytes.truncate(HEADER_SIZE + 2);
        assert!(matches!(read(&bytes, 1024).await, Err(PacketError::Io(_))));
    }

    #[tokio::test]
    async fn ignored_message_is_skipped() {
        let mut bytes = packet(0x01, 0, b"abandoned");
        bytes.extend(packet(0x01, STATUS_EOM | STATUS_IGNORE, b""));
        bytes.extend(packet(0x01, STATUS_EOM, b"real"));
        let msg = read(&bytes, 1024).await.unwrap();
        assert_eq!(msg.payload, b"real");
    }

    #[tokio::test]
    async fn reset_connection_bits_come_from_first_packet() {
        let mut bytes = packet(0x01, STATUS_RESET_CONNECTION_SKIP_TRAN, b"a");
        bytes.extend(packet(0x01, STATUS_EOM, b"b"));
        let msg = read(&bytes, 1024).await.unwrap();
        assert!(msg.resets_connection());
        assert!(msg.reset_keeps_transaction());

        let plain = packet(0x01, STATUS_EOM | STATUS_RESET_CONNECTION, b"a");
        let msg = read(&plain, 1024).await.unwrap();
        assert!(msg.resets_connection());
        assert!(!msg.reset_keeps_transaction());
    }
}
