//! SSTable block format
//!
//! Block layout (4KB aligned):
//! ```text
//! +------------------+
//! | Num Entries (4B) |
//! | Entry 1          |
//! | Entry 2          |
//! | ...              |
//! | Padding          |
//! | CRC32 (4B)       |
//! +------------------+
//! ```
//!
//! Entry format:
//! ```text
//! | Key Len (2B) | Value Len (2B) | Key | Value |
//! ```
//! Value Len = 0xFFFF indicates a tombstone (deleted key).

use crate::io::PAGE_SIZE;
use crate::storage::error::{StorageError, StorageResult};

/// Block size (4KB, page-aligned)
pub const BLOCK_SIZE: usize = PAGE_SIZE;

/// Header size: num_entries (4 bytes)
pub const BLOCK_HEADER_SIZE: usize = 4;

/// Footer size: CRC32 (4 bytes)
pub const BLOCK_FOOTER_SIZE: usize = 4;

/// Maximum usable space in a block
pub const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE - BLOCK_FOOTER_SIZE;

/// Entry header size: key_len (2) + value_len (2)
pub const ENTRY_HEADER_SIZE: usize = 4;

/// Maximum key size (64KB - 1)
pub const MAX_KEY_SIZE: usize = 65534;

/// Maximum value size (64KB - 2, 0xFFFF reserved for tombstone)
pub const MAX_VALUE_SIZE: usize = 65534;

/// Tombstone marker for deleted keys
pub const TOMBSTONE_MARKER: u16 = 0xFFFF;

/// A single key-value entry
#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None = tombstone
}

impl Entry {
    /// Create a new entry with a value
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }

    /// Create a tombstone entry
    pub fn tombstone(key: Vec<u8>) -> Self {
        Self { key, value: None }
    }

    /// Check if this is a tombstone
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Get encoded size of this entry
    pub fn encoded_size(&self) -> usize {
        ENTRY_HEADER_SIZE + self.key.len() + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
    }

    /// Encode entry to bytes
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u16;
        let value_len = match &self.value {
            Some(v) => v.len() as u16,
            None => TOMBSTONE_MARKER,
        };

        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.extend_from_slice(&key_len.to_be_bytes());
        buf.extend_from_slice(&value_len.to_be_bytes());
        buf.extend_from_slice(&self.key);
        if let Some(v) = &self.value {
            buf.extend_from_slice(v);
        }
        buf
    }

    /// Decode entry from bytes
    pub fn decode(buf: &[u8]) -> StorageResult<(Self, usize)> {
        if buf.len() < ENTRY_HEADER_SIZE {
            return Err(StorageError::InvalidSstable("entry too short".to_string()));
        }

        let key_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        let value_len_raw = u16::from_be_bytes([buf[2], buf[3]]);

        let is_tombstone = value_len_raw == TOMBSTONE_MARKER;
        let value_len = if is_tombstone {
            0
        } else {
            value_len_raw as usize
        };

        let total_len = ENTRY_HEADER_SIZE + key_len + value_len;
        if buf.len() < total_len {
            return Err(StorageError::InvalidSstable("entry truncated".to_string()));
        }

        let key = buf[ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + key_len].to_vec();
        let value = if is_tombstone {
            None
        } else {
            Some(buf[ENTRY_HEADER_SIZE + key_len..total_len].to_vec())
        };

        Ok((Self { key, value }, total_len))
    }
}

/// A block builder for writing entries
pub struct BlockBuilder {
    entries: Vec<Entry>,
    size: usize,
}

impl BlockBuilder {
    /// Create a new block builder
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: BLOCK_HEADER_SIZE + BLOCK_FOOTER_SIZE,
        }
    }

    /// Check if an entry can fit in this block
    pub fn can_fit(&self, entry: &Entry) -> bool {
        self.size + entry.encoded_size() <= BLOCK_SIZE
    }

    /// Add an entry to the block
    ///
    /// Returns false if the entry doesn't fit.
    pub fn add(&mut self, entry: Entry) -> bool {
        let entry_size = entry.encoded_size();
        if self.size + entry_size > BLOCK_SIZE {
            return false;
        }
        self.size += entry_size;
        self.entries.push(entry);
        true
    }

    /// Check if the block is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the first key in the block
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|e| e.key.as_slice())
    }

    /// Get the last key in the block
    pub fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|e| e.key.as_slice())
    }

    /// Build the block into a page-aligned buffer
    pub fn build(self) -> Vec<u8> {
        let mut buf = vec![0u8; BLOCK_SIZE];

        // Write header: num entries
        let num_entries = self.entries.len() as u32;
        buf[0..4].copy_from_slice(&num_entries.to_be_bytes());

        // Write entries
        let mut offset = BLOCK_HEADER_SIZE;
        for entry in &self.entries {
            let encoded = entry.encode();
            buf[offset..offset + encoded.len()].copy_from_slice(&encoded);
            offset += encoded.len();
        }

        // Calculate and write CRC32 (over header + entries, excluding footer)
        let crc = crc32fast::hash(&buf[..BLOCK_SIZE - BLOCK_FOOTER_SIZE]);
        buf[BLOCK_SIZE - BLOCK_FOOTER_SIZE..].copy_from_slice(&crc.to_be_bytes());

        buf
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A block reader for iterating entries
pub struct BlockReader {
    entries: Vec<Entry>,
}

impl BlockReader {
    /// Parse a block from bytes
    pub fn parse(buf: &[u8], offset: u64) -> StorageResult<Self> {
        if buf.len() < BLOCK_SIZE {
            return Err(StorageError::InvalidSstable("block too small".to_string()));
        }

        // Verify CRC32
        let stored_crc = u32::from_be_bytes([
            buf[BLOCK_SIZE - 4],
            buf[BLOCK_SIZE - 3],
            buf[BLOCK_SIZE - 2],
            buf[BLOCK_SIZE - 1],
        ]);
        let computed_crc = crc32fast::hash(&buf[..BLOCK_SIZE - BLOCK_FOOTER_SIZE]);

        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedBlock {
                offset,
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Read header
        let num_entries = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        // Read entries
        let mut entries = Vec::with_capacity(num_entries);
        let mut pos = BLOCK_HEADER_SIZE;

        for _ in 0..num_entries {
            let (entry, len) = Entry::decode(&buf[pos..])?;
            entries.push(entry);
            pos += len;
        }

        Ok(Self { entries })
    }

    /// Get all entries
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Find an entry by key using binary search
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.entries
            .binary_search_by(|e| e.key.as_slice().cmp(key))
            .ok()
            .map(|i| &self.entries[i])
    }

    /// Get the first key in the block
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|e| e.key.as_slice())
    }

    /// Get the last key in the block
    pub fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|e| e.key.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_encode_decode() {
        let entry = Entry::new(b"key".to_vec(), b"value".to_vec());
        let encoded = entry.encode();
        let (decoded, len) = Entry::decode(&encoded).unwrap();

        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.value, Some(b"value".to_vec()));
        assert_eq!(len, encoded.len());
    }

    #[test]
    fn test_tombstone_encode_decode() {
        let entry = Entry::tombstone(b"deleted".to_vec());
        let encoded = entry.encode();
        let (decoded, _) = Entry::decode(&encoded).unwrap();

        assert_eq!(decoded.key, b"deleted");
        assert!(decoded.is_tombstone());
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn test_block_builder() {
        let mut builder = BlockBuilder::new();

        assert!(builder.add(Entry::new(b"a".to_vec(), b"1".to_vec())));
        assert!(builder.add(Entry::new(b"b".to_vec(), b"2".to_vec())));
        assert!(builder.add(Entry::tombstone(b"c".to_vec())));

        assert_eq!(builder.first_key(), Some(b"a".as_slice()));
        assert_eq!(builder.last_key(), Some(b"c".as_slice()));

        let block = builder.build();
        assert_eq!(block.len(), BLOCK_SIZE);
    }

    #[test]
    fn test_block_reader() {
        let mut builder = BlockBuilder::new();
        builder.add(Entry::new(b"apple".to_vec(), b"red".to_vec()));
        builder.add(Entry::new(b"banana".to_vec(), b"yellow".to_vec()));
        builder.add(Entry::tombstone(b"cherry".to_vec()));

        let block = builder.build();
        let reader = BlockReader::parse(&block, 0).unwrap();

        assert_eq!(reader.entries().len(), 3);

        let found = reader.get(b"banana").unwrap();
        assert_eq!(found.value, Some(b"yellow".to_vec()));

        let tombstone = reader.get(b"cherry").unwrap();
        assert!(tombstone.is_tombstone());

        assert!(reader.get(b"notfound").is_none());
    }

    #[test]
    fn test_block_crc_validation() {
        let mut builder = BlockBuilder::new();
        builder.add(Entry::new(b"key".to_vec(), b"value".to_vec()));
        let mut block = builder.build();

        // Corrupt a byte
        block[BLOCK_HEADER_SIZE] ^= 0xFF;

        let result = BlockReader::parse(&block, 0);
        assert!(matches!(result, Err(StorageError::CorruptedBlock { .. })));
    }
}
