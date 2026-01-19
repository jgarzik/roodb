//! Protocol packet reading and writing
//!
//! Packets have a 4-byte header:
//! - 3 bytes: payload length (little-endian)
//! - 1 byte: sequence ID
//!
//! Maximum payload is 2^24 - 1 = 16,777,215 bytes.
//! Larger payloads are split into multiple packets.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::error::{ProtocolError, ProtocolResult};

/// Maximum payload size for a single packet (2^24 - 1)
pub const MAX_PACKET_SIZE: usize = 16_777_215;

/// Maximum total payload size for multi-packet messages (256 MB)
/// This prevents memory exhaustion attacks via very large multi-packet payloads.
pub const MAX_TOTAL_PAYLOAD_SIZE: usize = 256 * 1024 * 1024;

/// Reads protocol packets from an async stream
pub struct PacketReader<R> {
    reader: R,
    sequence_id: u8,
}

impl<R: AsyncRead + Unpin> PacketReader<R> {
    /// Create a new packet reader
    pub fn new(reader: R) -> Self {
        PacketReader {
            reader,
            sequence_id: 0,
        }
    }

    /// Reset sequence ID (for new command)
    pub fn reset_sequence(&mut self) {
        self.sequence_id = 0;
    }

    /// Get current sequence ID
    pub fn sequence_id(&self) -> u8 {
        self.sequence_id
    }

    /// Set sequence ID
    pub fn set_sequence(&mut self, seq: u8) {
        self.sequence_id = seq;
    }

    /// Read a complete packet, handling multi-packet payloads
    pub async fn read_packet(&mut self) -> ProtocolResult<Vec<u8>> {
        let mut payload = Vec::new();

        loop {
            // Read 4-byte header
            let mut header = [0u8; 4];
            match self.reader.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(ProtocolError::ConnectionClosed);
                }
                Err(e) => return Err(e.into()),
            }

            let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
            let seq = header[3];

            // Verify sequence ID
            if seq != self.sequence_id {
                return Err(ProtocolError::InvalidPacket(format!(
                    "sequence mismatch: expected {}, got {}",
                    self.sequence_id, seq
                )));
            }
            self.sequence_id = self.sequence_id.wrapping_add(1);

            // Read payload
            if length > 0 {
                let new_size = payload.len().saturating_add(length);
                if new_size > MAX_TOTAL_PAYLOAD_SIZE {
                    return Err(ProtocolError::InvalidPacket(format!(
                        "Payload exceeds maximum size of {} bytes",
                        MAX_TOTAL_PAYLOAD_SIZE
                    )));
                }
                let start = payload.len();
                payload.resize(start + length, 0);
                self.reader.read_exact(&mut payload[start..]).await?;
            }

            // If length < MAX_PACKET_SIZE, this is the last packet
            if length < MAX_PACKET_SIZE {
                break;
            }
        }

        Ok(payload)
    }

    /// Get mutable reference to inner reader
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

/// Writes protocol packets to an async stream
pub struct PacketWriter<W> {
    writer: W,
    sequence_id: u8,
}

impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    /// Create a new packet writer
    pub fn new(writer: W) -> Self {
        PacketWriter {
            writer,
            sequence_id: 0,
        }
    }

    /// Reset sequence ID (for new command)
    pub fn reset_sequence(&mut self) {
        self.sequence_id = 0;
    }

    /// Get current sequence ID
    pub fn sequence_id(&self) -> u8 {
        self.sequence_id
    }

    /// Set sequence ID
    pub fn set_sequence(&mut self, seq: u8) {
        self.sequence_id = seq;
    }

    /// Write a complete packet, splitting if necessary
    pub async fn write_packet(&mut self, payload: &[u8]) -> ProtocolResult<()> {
        let mut offset = 0;

        loop {
            let remaining = payload.len() - offset;
            let chunk_size = remaining.min(MAX_PACKET_SIZE);

            // Write header
            let length_bytes = (chunk_size as u32).to_le_bytes();
            let header = [
                length_bytes[0],
                length_bytes[1],
                length_bytes[2],
                self.sequence_id,
            ];

            self.writer.write_all(&header).await?;
            self.sequence_id = self.sequence_id.wrapping_add(1);

            // Write payload chunk
            if chunk_size > 0 {
                self.writer
                    .write_all(&payload[offset..offset + chunk_size])
                    .await?;
            }

            offset += chunk_size;

            // If we wrote less than MAX_PACKET_SIZE, we're done
            // (or if remaining was exactly MAX_PACKET_SIZE, send empty packet)
            if chunk_size < MAX_PACKET_SIZE {
                break;
            }
        }

        Ok(())
    }

    /// Flush the underlying writer
    pub async fn flush(&mut self) -> ProtocolResult<()> {
        self.writer.flush().await?;
        Ok(())
    }

    /// Get mutable reference to inner writer
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Encode a length-encoded integer
pub fn encode_length_encoded_int(value: u64) -> Vec<u8> {
    if value < 251 {
        vec![value as u8]
    } else if value < 65536 {
        let mut buf = vec![0xfc];
        buf.extend_from_slice(&(value as u16).to_le_bytes());
        buf
    } else if value < 16_777_216 {
        let bytes = (value as u32).to_le_bytes();
        vec![0xfd, bytes[0], bytes[1], bytes[2]]
    } else {
        let mut buf = vec![0xfe];
        buf.extend_from_slice(&value.to_le_bytes());
        buf
    }
}

/// Decode a length-encoded integer, returns (value, bytes_consumed)
pub fn decode_length_encoded_int(data: &[u8]) -> ProtocolResult<(u64, usize)> {
    if data.is_empty() {
        return Err(ProtocolError::InvalidPacket(
            "empty length-encoded int".to_string(),
        ));
    }

    match data[0] {
        0..=250 => Ok((data[0] as u64, 1)),
        0xfc => {
            if data.len() < 3 {
                return Err(ProtocolError::InvalidPacket(
                    "truncated 2-byte int".to_string(),
                ));
            }
            let value = u16::from_le_bytes([data[1], data[2]]);
            Ok((value as u64, 3))
        }
        0xfd => {
            if data.len() < 4 {
                return Err(ProtocolError::InvalidPacket(
                    "truncated 3-byte int".to_string(),
                ));
            }
            let value = u32::from_le_bytes([data[1], data[2], data[3], 0]);
            Ok((value as u64, 4))
        }
        0xfe => {
            if data.len() < 9 {
                return Err(ProtocolError::InvalidPacket(
                    "truncated 8-byte int".to_string(),
                ));
            }
            let value = u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Ok((value, 9))
        }
        0xfb => {
            // NULL indicator in row data
            Ok((0, 1))
        }
        0xff => Err(ProtocolError::InvalidPacket(
            "unexpected 0xff in length-encoded int".to_string(),
        )),
    }
}

/// Encode a length-encoded string
pub fn encode_length_encoded_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut result = encode_length_encoded_int(bytes.len() as u64);
    result.extend_from_slice(bytes);
    result
}

/// Encode a length-encoded bytes
pub fn encode_length_encoded_bytes(data: &[u8]) -> Vec<u8> {
    let mut result = encode_length_encoded_int(data.len() as u64);
    result.extend_from_slice(data);
    result
}

/// Decode a length-encoded string, returns (string, bytes_consumed)
pub fn decode_length_encoded_string(data: &[u8]) -> ProtocolResult<(String, usize)> {
    let (len, header_size) = decode_length_encoded_int(data)?;
    let len = len as usize;

    if data.len() < header_size + len {
        return Err(ProtocolError::InvalidPacket("truncated string".to_string()));
    }

    let s = String::from_utf8(data[header_size..header_size + len].to_vec())
        .map_err(|_| ProtocolError::InvalidPacket("invalid UTF-8 in string".to_string()))?;

    Ok((s, header_size + len))
}

/// Decode length-encoded binary data, returns (bytes, bytes_consumed)
pub fn decode_length_encoded_bytes(data: &[u8]) -> ProtocolResult<(Vec<u8>, usize)> {
    let (len, header_size) = decode_length_encoded_int(data)?;
    let len = len as usize;

    if data.len() < header_size + len {
        return Err(ProtocolError::InvalidPacket("truncated bytes".to_string()));
    }

    Ok((
        data[header_size..header_size + len].to_vec(),
        header_size + len,
    ))
}

/// Decode a null-terminated string
pub fn decode_null_terminated_string(data: &[u8]) -> ProtocolResult<(String, usize)> {
    let null_pos = data
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ProtocolError::InvalidPacket("missing null terminator".to_string()))?;

    let s = String::from_utf8(data[..null_pos].to_vec())
        .map_err(|_| ProtocolError::InvalidPacket("invalid UTF-8 in string".to_string()))?;

    Ok((s, null_pos + 1))
}

/// Encode a null-terminated string
pub fn encode_null_terminated_string(s: &str) -> Vec<u8> {
    let mut result = s.as_bytes().to_vec();
    result.push(0);
    result
}

/// Read a single packet directly from a stream (for STARTTLS)
///
/// Returns (payload, sequence_id)
pub async fn read_packet_raw<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> ProtocolResult<(Vec<u8>, u8)> {
    // Read 4-byte header
    let mut header = [0u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ProtocolError::ConnectionClosed);
        }
        Err(e) => return Err(e.into()),
    }

    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let seq = header[3];

    // Read payload
    let mut payload = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok((payload, seq))
}

/// Write a single packet directly to a stream (for STARTTLS)
pub async fn write_packet_raw<W: AsyncWrite + Unpin>(
    writer: &mut W,
    sequence_id: u8,
    payload: &[u8],
) -> ProtocolResult<()> {
    // Write header
    let length_bytes = (payload.len() as u32).to_le_bytes();
    let header = [
        length_bytes[0],
        length_bytes[1],
        length_bytes[2],
        sequence_id,
    ];
    writer.write_all(&header).await?;

    // Write payload
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length_encoded_int_small() {
        let encoded = encode_length_encoded_int(42);
        assert_eq!(encoded, vec![42]);

        let (decoded, consumed) = decode_length_encoded_int(&encoded).unwrap();
        assert_eq!(decoded, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_length_encoded_int_medium() {
        let encoded = encode_length_encoded_int(1000);
        assert_eq!(encoded, vec![0xfc, 0xe8, 0x03]);

        let (decoded, consumed) = decode_length_encoded_int(&encoded).unwrap();
        assert_eq!(decoded, 1000);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_length_encoded_int_large() {
        let encoded = encode_length_encoded_int(1_000_000);
        assert_eq!(encoded.len(), 4);

        let (decoded, _) = decode_length_encoded_int(&encoded).unwrap();
        assert_eq!(decoded, 1_000_000);
    }

    #[test]
    fn test_length_encoded_string() {
        let encoded = encode_length_encoded_string("hello");
        assert_eq!(encoded, vec![5, b'h', b'e', b'l', b'l', b'o']);

        let (decoded, consumed) = decode_length_encoded_string(&encoded).unwrap();
        assert_eq!(decoded, "hello");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_null_terminated_string() {
        let encoded = encode_null_terminated_string("test");
        assert_eq!(encoded, vec![b't', b'e', b's', b't', 0]);

        let (decoded, consumed) = decode_null_terminated_string(&encoded).unwrap();
        assert_eq!(decoded, "test");
        assert_eq!(consumed, 5);
    }
}
