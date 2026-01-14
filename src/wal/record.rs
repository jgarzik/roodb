//! WAL record format
//!
//! Record layout:
//! ```text
//! +--------+--------+--------+--------+--------+
//! | CRC32  |  Type  |  Len   |      LSN        |
//! | 4 bytes| 1 byte |2 bytes |    8 bytes      |
//! +--------+--------+--------+--------+--------+
//! |              Data (variable)               |
//! +--------------------------------------------+
//! ```
//!
//! - CRC32: Checksum of type + len + lsn + data
//! - Type: Record type (Data, Checkpoint, etc.)
//! - Len: Length of data (max 65535)
//! - LSN: Log sequence number
//! - Data: Actual payload

use crate::wal::error::{WalError, WalResult};

/// Record header size in bytes
pub const HEADER_SIZE: usize = 15; // 4 + 1 + 2 + 8

/// Maximum record data size (64KB - 1)
pub const MAX_RECORD_SIZE: usize = 65535;

/// Record types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Regular data record
    Data = 1,
    /// Checkpoint marker
    Checkpoint = 2,
    /// Segment end marker
    SegmentEnd = 3,
}

impl TryFrom<u8> for RecordType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Data),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::SegmentEnd),
            _ => Err(WalError::InvalidHeader),
        }
    }
}

/// A WAL record
#[derive(Debug, Clone)]
pub struct Record {
    /// Record type
    pub record_type: RecordType,
    /// Log sequence number
    pub lsn: u64,
    /// Record data
    pub data: Vec<u8>,
}

impl Record {
    /// Create a new data record
    pub fn new(lsn: u64, data: Vec<u8>) -> WalResult<Self> {
        if data.len() > MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge {
                size: data.len(),
                max: MAX_RECORD_SIZE,
            });
        }
        Ok(Self {
            record_type: RecordType::Data,
            lsn,
            data,
        })
    }

    /// Create a checkpoint record
    pub fn checkpoint(lsn: u64) -> Self {
        Self {
            record_type: RecordType::Checkpoint,
            lsn,
            data: Vec::new(),
        }
    }

    /// Create a segment end marker
    pub fn segment_end(lsn: u64) -> Self {
        Self {
            record_type: RecordType::SegmentEnd,
            lsn,
            data: Vec::new(),
        }
    }

    /// Encode the record to bytes
    pub fn encode(&self) -> Vec<u8> {
        let data_len = self.data.len() as u16;
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());

        // Reserve space for CRC (will be filled in at the end)
        buf.extend_from_slice(&[0u8; 4]);

        // Type
        buf.push(self.record_type as u8);

        // Length (big-endian)
        buf.extend_from_slice(&data_len.to_be_bytes());

        // LSN (big-endian)
        buf.extend_from_slice(&self.lsn.to_be_bytes());

        // Data
        buf.extend_from_slice(&self.data);

        // Calculate CRC over everything after the CRC field
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_be_bytes());

        buf
    }

    /// Decode a record from bytes
    pub fn decode(buf: &[u8]) -> WalResult<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(WalError::InvalidHeader);
        }

        // Read CRC
        let stored_crc = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        // Calculate CRC over the rest
        let computed_crc = crc32fast::hash(&buf[4..]);

        if stored_crc != computed_crc {
            return Err(WalError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Parse header
        let record_type = RecordType::try_from(buf[4])?;
        let data_len = u16::from_be_bytes([buf[5], buf[6]]) as usize;
        let lsn = u64::from_be_bytes([
            buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14],
        ]);

        // Validate data length
        if buf.len() < HEADER_SIZE + data_len {
            return Err(WalError::InvalidHeader);
        }

        // Extract data
        let data = buf[HEADER_SIZE..HEADER_SIZE + data_len].to_vec();

        Ok(Self {
            record_type,
            lsn,
            data,
        })
    }

    /// Get the total encoded size of this record
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let record = Record::new(42, b"hello world".to_vec()).unwrap();
        let encoded = record.encode();
        let decoded = Record::decode(&encoded).unwrap();

        assert_eq!(decoded.record_type, RecordType::Data);
        assert_eq!(decoded.lsn, 42);
        assert_eq!(decoded.data, b"hello world");
    }

    #[test]
    fn test_checkpoint_record() {
        let record = Record::checkpoint(100);
        let encoded = record.encode();
        let decoded = Record::decode(&encoded).unwrap();

        assert_eq!(decoded.record_type, RecordType::Checkpoint);
        assert_eq!(decoded.lsn, 100);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_crc_mismatch() {
        let record = Record::new(1, b"test".to_vec()).unwrap();
        let mut encoded = record.encode();
        // Corrupt the data
        encoded[HEADER_SIZE] ^= 0xFF;

        let result = Record::decode(&encoded);
        assert!(matches!(result, Err(WalError::CrcMismatch { .. })));
    }

    #[test]
    fn test_record_too_large() {
        let data = vec![0u8; MAX_RECORD_SIZE + 1];
        let result = Record::new(1, data);
        assert!(matches!(result, Err(WalError::RecordTooLarge { .. })));
    }
}
