//! SSTable data blocks.
//!
//! A block is a whole number of 4 KiB pages. Most are exactly one page; a
//! block grows to as many pages as it needs when it holds an entry that does
//! not fit in one.
//!
//! ```text
//! +----------------------------+
//! | Block length (4B)          |  bytes, including padding and checksum
//! | Num entries (4B)           |
//! +----------------------------+
//! | Entry: key len (2B)        |
//! |        value len (4B)      |  0xFFFFFFFF = tombstone
//! |        key, value          |
//! | ...                        |
//! +----------------------------+
//! | Padding (zeros)            |
//! | CRC32 (4B)                 |  over everything before it
//! +----------------------------+
//! ```

use crate::io::PAGE_SIZE;
use crate::storage::error::{StorageError, StorageResult};

/// Target block size. Blocks are multiples of this.
pub const BLOCK_SIZE: usize = PAGE_SIZE;

/// Block header: length + entry count.
pub const BLOCK_HEADER_SIZE: usize = 8;

/// Block footer: CRC32.
pub const BLOCK_FOOTER_SIZE: usize = 4;

/// Entry header: key length (2) + value length (4).
pub const ENTRY_HEADER_SIZE: usize = 6;

/// Largest key the format can hold.
pub const MAX_KEY_SIZE: usize = u16::MAX as usize;

/// Largest value the format can hold.
pub const MAX_VALUE_SIZE: usize = (u32::MAX - 1) as usize;

/// Value length that marks a deleted key.
pub const TOMBSTONE_MARKER: u32 = u32::MAX;

/// Check that a key and value fit the on-disk format.
pub fn check_entry_size(key_len: usize, value_len: usize) -> StorageResult<()> {
    if key_len > MAX_KEY_SIZE {
        return Err(StorageError::KeyTooLarge {
            size: key_len,
            max: MAX_KEY_SIZE,
        });
    }
    if value_len > MAX_VALUE_SIZE {
        return Err(StorageError::ValueTooLarge {
            size: value_len,
            max: MAX_VALUE_SIZE,
        });
    }
    Ok(())
}

/// A key-value entry in a block
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

    /// Encoded size of this entry
    pub fn encoded_size(&self) -> usize {
        ENTRY_HEADER_SIZE + self.key.len() + self.value.as_ref().map_or(0, Vec::len)
    }

    /// Append the encoded entry to `out`. The sizes must have been checked
    /// with [`check_entry_size`].
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        debug_assert!(self.key.len() <= MAX_KEY_SIZE);
        let value_len = match &self.value {
            Some(v) => {
                debug_assert!(v.len() <= MAX_VALUE_SIZE);
                v.len() as u32
            }
            None => TOMBSTONE_MARKER,
        };
        out.extend_from_slice(&(self.key.len() as u16).to_be_bytes());
        out.extend_from_slice(&value_len.to_be_bytes());
        out.extend_from_slice(&self.key);
        if let Some(v) = &self.value {
            out.extend_from_slice(v);
        }
    }

    /// Encode entry to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        self.encode_into(&mut buf);
        buf
    }

    /// Decode entry from bytes, returns (entry, bytes_consumed)
    pub fn decode(buf: &[u8]) -> StorageResult<(Self, usize)> {
        if buf.len() < ENTRY_HEADER_SIZE {
            return Err(StorageError::InvalidSstable("entry too short".to_string()));
        }

        let key_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        let value_len_raw = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);

        let is_tombstone = value_len_raw == TOMBSTONE_MARKER;
        let value_len = if is_tombstone {
            0
        } else {
            value_len_raw as usize
        };

        let total_len = ENTRY_HEADER_SIZE
            .checked_add(key_len)
            .and_then(|n| n.checked_add(value_len))
            .ok_or_else(|| StorageError::InvalidSstable("entry length overflow".to_string()))?;
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

/// Block builder for creating data blocks
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

    /// Whether the entry should go in this block. An empty block takes any
    /// entry, growing past one page if it must; otherwise the block is kept to
    /// one page.
    pub fn can_fit(&self, entry: &Entry) -> bool {
        self.entries.is_empty() || self.size + entry.encoded_size() <= BLOCK_SIZE
    }

    /// Add an entry. Returns false, leaving the block unchanged, if
    /// [`Self::can_fit`] says it belongs in the next block.
    pub fn add(&mut self, entry: Entry) -> bool {
        if !self.can_fit(&entry) {
            return false;
        }
        self.size += entry.encoded_size();
        self.entries.push(entry);
        true
    }

    /// Check if block is empty
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

    /// Build the block: a whole number of pages.
    pub fn build(self) -> Vec<u8> {
        let len = self.size.next_multiple_of(BLOCK_SIZE);
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            entry.encode_into(&mut buf);
        }
        buf.resize(len - BLOCK_FOOTER_SIZE, 0);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Block reader for parsing data blocks
pub struct BlockReader {
    entries: Vec<Entry>,
}

impl BlockReader {
    /// Parse a block. `buf` must be exactly the block; `offset` is where it
    /// sits in the file, for error messages.
    pub fn parse(buf: &[u8], offset: u64) -> StorageResult<Self> {
        if buf.len() < BLOCK_SIZE || !buf.len().is_multiple_of(BLOCK_SIZE) {
            return Err(StorageError::InvalidSstable(format!(
                "block at offset {offset} has length {}",
                buf.len()
            )));
        }

        // Verify CRC before trusting anything else in the block.
        let body = buf.len() - BLOCK_FOOTER_SIZE;
        let stored_crc =
            u32::from_be_bytes([buf[body], buf[body + 1], buf[body + 2], buf[body + 3]]);
        let computed_crc = crc32fast::hash(&buf[..body]);
        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedBlock {
                offset,
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let declared_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if declared_len != buf.len() {
            return Err(StorageError::InvalidSstable(format!(
                "block at offset {offset} says it is {declared_len} bytes, index says {}",
                buf.len()
            )));
        }
        let num_entries = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;

        let mut entries = Vec::with_capacity(num_entries.min(body / ENTRY_HEADER_SIZE));
        let mut pos = BLOCK_HEADER_SIZE;
        for _ in 0..num_entries {
            let (entry, len) = Entry::decode(&buf[pos..body])?;
            entries.push(entry);
            pos += len;
        }

        Ok(Self { entries })
    }

    /// Get all entries
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Take ownership of all entries (avoids clones)
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }

    /// Search for a key (binary search)
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.entries
            .binary_search_by(|e| e.key.as_slice().cmp(key))
            .ok()
            .map(|i| &self.entries[i])
    }

    /// Get first key
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|e| e.key.as_slice())
    }

    /// Get last key
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
    }

    #[test]
    fn empty_value_is_not_a_tombstone() {
        let entry = Entry::new(b"k".to_vec(), Vec::new());
        let (decoded, _) = Entry::decode(&entry.encode()).unwrap();
        assert_eq!(decoded.value, Some(Vec::new()));
    }

    #[test]
    fn test_block_build_parse() {
        let mut builder = BlockBuilder::new();
        builder.add(Entry::new(b"a".to_vec(), b"1".to_vec()));
        builder.add(Entry::new(b"b".to_vec(), b"2".to_vec()));
        builder.add(Entry::tombstone(b"c".to_vec()));

        let block = builder.build();
        assert_eq!(block.len(), BLOCK_SIZE);

        let reader = BlockReader::parse(&block, 0).unwrap();
        assert_eq!(reader.entries().len(), 3);
        assert_eq!(reader.get(b"a").unwrap().value, Some(b"1".to_vec()));
        assert_eq!(reader.get(b"b").unwrap().value, Some(b"2".to_vec()));
        assert!(reader.get(b"c").unwrap().is_tombstone());
        assert!(reader.get(b"d").is_none());
    }

    #[test]
    fn small_entries_fill_exactly_one_page() {
        let mut builder = BlockBuilder::new();
        let mut added = 0;
        loop {
            let entry = Entry::new(format!("key{added:05}").into_bytes(), vec![1u8; 90]);
            if !builder.add(entry) {
                break;
            }
            added += 1;
        }
        assert!(added > 30);
        let block = builder.build();
        assert_eq!(block.len(), BLOCK_SIZE);
        assert_eq!(
            BlockReader::parse(&block, 0).unwrap().entries().len(),
            added
        );
    }

    #[test]
    fn an_entry_larger_than_a_page_gets_a_larger_block() {
        for value_len in [4085usize, 4096, 65_534, 65_535, 70_000, 1_000_000] {
            let value: Vec<u8> = (0..value_len).map(|i| (i % 251) as u8).collect();
            let mut builder = BlockBuilder::new();
            assert!(builder.add(Entry::new(b"big".to_vec(), value.clone())));
            // The block is full now: the next entry starts a new one.
            assert!(!builder.add(Entry::new(b"next".to_vec(), b"v".to_vec())));

            let block = builder.build();
            assert!(block.len().is_multiple_of(BLOCK_SIZE));
            assert!(block.len() >= value_len);
            let reader = BlockReader::parse(&block, 0).unwrap();
            assert_eq!(reader.entries().len(), 1);
            assert_eq!(
                reader.get(b"big").unwrap().value.as_deref(),
                Some(&value[..])
            );
        }
    }

    #[test]
    fn test_block_crc_validation() {
        let mut builder = BlockBuilder::new();
        builder.add(Entry::new(b"key".to_vec(), b"value".to_vec()));
        let mut block = builder.build();

        // Corrupt data
        block[10] ^= 0xFF;

        let result = BlockReader::parse(&block, 0);
        assert!(matches!(result, Err(StorageError::CorruptedBlock { .. })));
    }

    #[test]
    fn a_block_cut_short_is_rejected() {
        let mut builder = BlockBuilder::new();
        builder.add(Entry::new(b"big".to_vec(), vec![3u8; 10_000]));
        let block = builder.build();
        assert!(BlockReader::parse(&block[..BLOCK_SIZE], 0).is_err());
        assert!(BlockReader::parse(&block[..100], 0).is_err());
    }

    #[test]
    fn size_limits() {
        assert!(check_entry_size(MAX_KEY_SIZE, 0).is_ok());
        assert!(matches!(
            check_entry_size(MAX_KEY_SIZE + 1, 0),
            Err(StorageError::KeyTooLarge { .. })
        ));
        assert!(check_entry_size(1, 100_000_000).is_ok());
    }
}
