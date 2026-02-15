//! TDS packet framing: 8-byte header, multi-packet message assembly.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// TDS packet header size
const HEADER_SIZE: usize = 8;
/// Maximum TDS packet size (matches client default)
const MAX_PACKET_SIZE: usize = 4096;
/// Maximum body per packet
const MAX_BODY_SIZE: usize = MAX_PACKET_SIZE - HEADER_SIZE;

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

/// Packet status flags
const STATUS_EOM: u8 = 0x01;

#[derive(Debug, thiserror::Error)]
pub enum PacketError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown packet type: 0x{0:02x}")]
    UnknownType(u8),
    #[error("packet too large: {0}")]
    TooLarge(usize),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("unexpected packet type: expected {expected:?}, got {got:?}")]
    UnexpectedType {
        expected: PacketType,
        got: PacketType,
    },
}

/// Reads complete TDS messages (potentially spanning multiple packets).
pub struct TdsReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> TdsReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read a complete TDS message. Returns (packet_type, payload).
    /// Assembles multiple packets until EOM is set.
    pub async fn read_message(&mut self) -> Result<(PacketType, Vec<u8>), PacketError> {
        let mut payload = Vec::new();
        let mut msg_type = None;

        loop {
            let mut header = [0u8; HEADER_SIZE];
            match self.reader.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(PacketError::ConnectionClosed);
                }
                Err(e) => return Err(PacketError::Io(e)),
            }

            let pkt_type = PacketType::try_from(header[0])?;
            let status = header[1];
            let length = u16::from_be_bytes([header[2], header[3]]) as usize;

            if length < HEADER_SIZE || length > MAX_PACKET_SIZE {
                return Err(PacketError::TooLarge(length));
            }

            // Verify consistent packet type across multi-packet messages
            if let Some(expected) = msg_type {
                if pkt_type != expected {
                    return Err(PacketError::UnexpectedType {
                        expected,
                        got: pkt_type,
                    });
                }
            } else {
                msg_type = Some(pkt_type);
            }

            let body_len = length - HEADER_SIZE;
            if body_len > 0 {
                let start = payload.len();
                payload.resize(start + body_len, 0);
                self.reader.read_exact(&mut payload[start..]).await?;
            }

            if status & STATUS_EOM != 0 {
                break;
            }
        }

        Ok((msg_type.unwrap(), payload))
    }
}

/// Writes TDS messages, splitting into packets as needed.
pub struct TdsWriter<W> {
    writer: W,
    packet_number: u8,
}

impl<W: AsyncWrite + Unpin> TdsWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            packet_number: 0,
        }
    }

    /// Write a complete message, splitting into packets if needed.
    pub async fn write_message(
        &mut self,
        pkt_type: PacketType,
        data: &[u8],
    ) -> Result<(), PacketError> {
        self.packet_number = 0;

        if data.is_empty() {
            // Send a single empty EOM packet
            self.write_packet(pkt_type, &[], true).await?;
            return Ok(());
        }

        let mut offset = 0;
        while offset < data.len() {
            let remaining = data.len() - offset;
            let chunk_size = remaining.min(MAX_BODY_SIZE);
            let is_last = offset + chunk_size >= data.len();

            self.write_packet(pkt_type, &data[offset..offset + chunk_size], is_last)
                .await?;
            offset += chunk_size;
        }

        Ok(())
    }

    async fn write_packet(
        &mut self,
        pkt_type: PacketType,
        body: &[u8],
        eom: bool,
    ) -> Result<(), PacketError> {
        let length = (HEADER_SIZE + body.len()) as u16;
        let status = if eom { STATUS_EOM } else { 0 };

        let mut header = [0u8; HEADER_SIZE];
        header[0] = pkt_type as u8;
        header[1] = status;
        header[2..4].copy_from_slice(&length.to_be_bytes());
        // SPID = 0
        header[6] = self.packet_number;
        self.packet_number = self.packet_number.wrapping_add(1);
        // Window = 0

        self.writer.write_all(&header).await?;
        if !body.is_empty() {
            self.writer.write_all(body).await?;
        }
        self.writer.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_roundtrip() {
        let data = b"hello world";
        let mut buf = Vec::new();

        // Write
        {
            let mut writer = TdsWriter::new(&mut buf);
            writer
                .write_message(PacketType::TabularResult, data)
                .await
                .unwrap();
        }

        // Verify header
        assert_eq!(buf[0], PacketType::TabularResult as u8);
        assert_eq!(buf[1], STATUS_EOM);
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        assert_eq!(len, HEADER_SIZE + data.len());

        // Read back
        let mut reader = TdsReader::new(&buf[..]);
        let (pkt_type, payload) = reader.read_message().await.unwrap();
        assert_eq!(pkt_type, PacketType::TabularResult);
        assert_eq!(payload, data);
    }
}
