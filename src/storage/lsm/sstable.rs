//! SSTable (Sorted String Table) reader and writer
//!
//! SSTable file format:
//! ```text
//! +------------------+
//! | Data Block 0     |  (4KB)
//! | Data Block 1     |  (4KB)
//! | ...              |
//! | Index Block      |  (4KB)
//! | Footer           |  (4KB)
//! +------------------+
//! ```
//!
//! Footer format (last 4KB):
//! ```text
//! | Index Block Offset (8B) |
//! | Index Block Size (8B)   |
//! | Num Data Blocks (4B)    |
//! | Min Key Len (2B)        |
//! | Min Key                 |
//! | Max Key Len (2B)        |
//! | Max Key                 |
//! | Padding                 |
//! | Magic (4B): "LSMT"      |
//! | Version (4B): 1         |
//! +-------------------------+
//! ```

use std::path::{Path, PathBuf};

use crate::io::{AlignedBuffer, AsyncIO, AsyncIOFactory, PAGE_SIZE};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::block::{BlockBuilder, BlockReader, Entry, BLOCK_SIZE};

/// SSTable magic number
const SSTABLE_MAGIC: u32 = 0x4C534D54; // "LSMT"

/// SSTable version
const SSTABLE_VERSION: u32 = 1;

/// Footer size (one page)
const FOOTER_SIZE: usize = PAGE_SIZE;

/// Index entry: block offset + first key
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub block_offset: u64,
    pub first_key: Vec<u8>,
}

/// SSTable metadata from footer
#[derive(Debug, Clone)]
pub struct SstableMetadata {
    pub index_offset: u64,
    pub index_size: u64,
    pub num_blocks: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

/// SSTable writer
pub struct SstableWriter<IO: AsyncIO> {
    io: IO,
    path: PathBuf,
    offset: u64,
    block_builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl<IO: AsyncIO> SstableWriter<IO> {
    /// Create a new SSTable writer
    pub async fn create<F: AsyncIOFactory<IO = IO>>(
        factory: &F,
        path: &Path,
    ) -> StorageResult<Self> {
        let io = factory.open(path, true).await?;
        Ok(Self {
            io,
            path: path.to_path_buf(),
            offset: 0,
            block_builder: BlockBuilder::new(),
            index_entries: Vec::new(),
            min_key: None,
            max_key: None,
        })
    }

    /// Add an entry to the SSTable
    pub async fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> StorageResult<()> {
        let entry = match value {
            Some(v) => Entry::new(key.clone(), v),
            None => Entry::tombstone(key.clone()),
        };

        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key);

        // Try to add to current block
        if !self.block_builder.can_fit(&entry) {
            // Flush current block
            self.flush_block().await?;
        }

        self.block_builder.add(entry);
        Ok(())
    }

    /// Flush current block to disk
    async fn flush_block(&mut self) -> StorageResult<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }

        // Record index entry before building
        let first_key = self.block_builder.first_key().unwrap().to_vec();
        self.index_entries.push(IndexEntry {
            block_offset: self.offset,
            first_key,
        });

        // Build and write block
        let block = std::mem::take(&mut self.block_builder).build();
        let mut buf = AlignedBuffer::new(BLOCK_SIZE)?;
        buf.copy_from_slice(&block)?;

        self.io.write_at(&buf, self.offset).await?;
        self.offset += BLOCK_SIZE as u64;

        self.block_builder = BlockBuilder::new();
        Ok(())
    }

    /// Finish writing the SSTable
    pub async fn finish(mut self) -> StorageResult<PathBuf> {
        // Flush any remaining data
        self.flush_block().await?;

        if self.index_entries.is_empty() {
            // Empty SSTable - just close
            return Ok(self.path);
        }

        // Write index block
        let index_offset = self.offset;
        let index_block = self.build_index_block();
        let mut buf = AlignedBuffer::new(BLOCK_SIZE)?;
        buf.copy_from_slice(&index_block)?;
        self.io.write_at(&buf, self.offset).await?;
        self.offset += BLOCK_SIZE as u64;

        // Write footer
        let footer = self.build_footer(index_offset)?;
        let mut buf = AlignedBuffer::new(FOOTER_SIZE)?;
        buf.copy_from_slice(&footer)?;
        self.io.write_at(&buf, self.offset).await?;

        // Sync to disk
        self.io.sync().await?;

        Ok(self.path)
    }

    /// Build the index block
    fn build_index_block(&self) -> Vec<u8> {
        let mut buf = vec![0u8; BLOCK_SIZE];

        // Write num entries
        let num_entries = self.index_entries.len() as u32;
        buf[0..4].copy_from_slice(&num_entries.to_be_bytes());

        // Write index entries
        let mut offset = 4;
        for entry in &self.index_entries {
            // Block offset (8 bytes)
            buf[offset..offset + 8].copy_from_slice(&entry.block_offset.to_be_bytes());
            offset += 8;

            // Key length (2 bytes)
            let key_len = entry.first_key.len() as u16;
            buf[offset..offset + 2].copy_from_slice(&key_len.to_be_bytes());
            offset += 2;

            // Key
            buf[offset..offset + entry.first_key.len()].copy_from_slice(&entry.first_key);
            offset += entry.first_key.len();
        }

        // CRC32 at the end
        let crc = crc32fast::hash(&buf[..BLOCK_SIZE - 4]);
        buf[BLOCK_SIZE - 4..].copy_from_slice(&crc.to_be_bytes());

        buf
    }

    /// Build the footer block
    fn build_footer(&self, index_offset: u64) -> StorageResult<Vec<u8>> {
        let mut buf = vec![0u8; FOOTER_SIZE];

        let min_key = self.min_key.as_ref().ok_or_else(|| {
            StorageError::InvalidSstable(
                "Cannot build footer: no entries added (min_key missing)".to_string(),
            )
        })?;
        let max_key = self.max_key.as_ref().ok_or_else(|| {
            StorageError::InvalidSstable(
                "Cannot build footer: no entries added (max_key missing)".to_string(),
            )
        })?;

        let mut offset = 0;

        // Index block offset (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&index_offset.to_be_bytes());
        offset += 8;

        // Index block size (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&(BLOCK_SIZE as u64).to_be_bytes());
        offset += 8;

        // Num data blocks (4 bytes)
        let num_blocks = self.index_entries.len() as u32;
        buf[offset..offset + 4].copy_from_slice(&num_blocks.to_be_bytes());
        offset += 4;

        // Min key length (2 bytes)
        buf[offset..offset + 2].copy_from_slice(&(min_key.len() as u16).to_be_bytes());
        offset += 2;

        // Min key
        buf[offset..offset + min_key.len()].copy_from_slice(min_key);
        offset += min_key.len();

        // Max key length (2 bytes)
        buf[offset..offset + 2].copy_from_slice(&(max_key.len() as u16).to_be_bytes());
        offset += 2;

        // Max key
        buf[offset..offset + max_key.len()].copy_from_slice(max_key);

        // Magic and version at the very end
        buf[FOOTER_SIZE - 8..FOOTER_SIZE - 4].copy_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        buf[FOOTER_SIZE - 4..].copy_from_slice(&SSTABLE_VERSION.to_be_bytes());

        Ok(buf)
    }
}

/// SSTable reader
pub struct SstableReader<IO: AsyncIO> {
    io: IO,
    metadata: SstableMetadata,
    index: Vec<IndexEntry>,
}

impl<IO: AsyncIO> SstableReader<IO> {
    /// Open an existing SSTable
    pub async fn open<F: AsyncIOFactory<IO = IO>>(factory: &F, path: &Path) -> StorageResult<Self> {
        let io = factory.open(path, false).await?;

        // Read file size
        let file_size = io.file_size().await?;
        if file_size < FOOTER_SIZE as u64 {
            return Err(StorageError::InvalidSstable("file too small".to_string()));
        }

        // Read footer
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let mut footer_buf = AlignedBuffer::new(FOOTER_SIZE)?;
        io.read_at(&mut footer_buf, footer_offset).await?;

        // Parse footer
        let metadata = Self::parse_footer(&footer_buf)?;

        // Read index block
        let mut index_buf = AlignedBuffer::new(BLOCK_SIZE)?;
        io.read_at(&mut index_buf, metadata.index_offset).await?;

        // Parse index
        let index = Self::parse_index(&index_buf)?;

        Ok(Self {
            io,
            metadata,
            index,
        })
    }

    /// Parse footer from buffer
    fn parse_footer(buf: &[u8]) -> StorageResult<SstableMetadata> {
        // Verify magic
        let magic = u32::from_be_bytes([
            buf[FOOTER_SIZE - 8],
            buf[FOOTER_SIZE - 7],
            buf[FOOTER_SIZE - 6],
            buf[FOOTER_SIZE - 5],
        ]);
        if magic != SSTABLE_MAGIC {
            return Err(StorageError::InvalidSstable(format!(
                "invalid magic: expected {:#x}, got {:#x}",
                SSTABLE_MAGIC, magic
            )));
        }

        // Verify version
        let version = u32::from_be_bytes([
            buf[FOOTER_SIZE - 4],
            buf[FOOTER_SIZE - 3],
            buf[FOOTER_SIZE - 2],
            buf[FOOTER_SIZE - 1],
        ]);
        if version != SSTABLE_VERSION {
            return Err(StorageError::InvalidSstable(format!(
                "unsupported version: {}",
                version
            )));
        }

        let mut offset = 0;

        // Index offset
        let index_offset = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        offset += 8;

        // Index size
        let index_size = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        offset += 8;

        // Num blocks
        let num_blocks = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        // Min key
        let min_key_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;
        let min_key = buf[offset..offset + min_key_len].to_vec();
        offset += min_key_len;

        // Max key
        let max_key_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;
        let max_key = buf[offset..offset + max_key_len].to_vec();

        Ok(SstableMetadata {
            index_offset,
            index_size,
            num_blocks,
            min_key,
            max_key,
        })
    }

    /// Parse index block from buffer
    fn parse_index(buf: &[u8]) -> StorageResult<Vec<IndexEntry>> {
        // Verify CRC
        let stored_crc = u32::from_be_bytes([
            buf[BLOCK_SIZE - 4],
            buf[BLOCK_SIZE - 3],
            buf[BLOCK_SIZE - 2],
            buf[BLOCK_SIZE - 1],
        ]);
        let computed_crc = crc32fast::hash(&buf[..BLOCK_SIZE - 4]);
        if stored_crc != computed_crc {
            return Err(StorageError::InvalidSstable(
                "index CRC mismatch".to_string(),
            ));
        }

        let num_entries = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 4;

        for _ in 0..num_entries {
            // Block offset
            let block_offset = u64::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
            ]);
            offset += 8;

            // Key length
            let key_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
            offset += 2;

            // Key
            let first_key = buf[offset..offset + key_len].to_vec();
            offset += key_len;

            entries.push(IndexEntry {
                block_offset,
                first_key,
            });
        }

        Ok(entries)
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        // Check key range
        if key < self.metadata.min_key.as_slice() || key > self.metadata.max_key.as_slice() {
            return Ok(None);
        }

        // Binary search index to find the right block
        let block_idx = self.find_block(key);

        // Read and search the block
        let block_offset = self.index[block_idx].block_offset;
        let mut buf = AlignedBuffer::new(BLOCK_SIZE)?;
        self.io.read_at(&mut buf, block_offset).await?;

        let reader = BlockReader::parse(buf.as_slice(), block_offset)?;
        if let Some(entry) = reader.get(key) {
            Ok(Some(entry.value.clone()))
        } else {
            Ok(None)
        }
    }

    /// Find the block that might contain a key
    fn find_block(&self, key: &[u8]) -> usize {
        // Binary search: find last block where first_key <= key
        let mut lo = 0;
        let mut hi = self.index.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Return the block before (or first block if at start)
        if lo > 0 {
            lo - 1
        } else {
            0
        }
    }

    /// Scan all entries in the SSTable
    pub async fn scan(&self) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut results = Vec::new();

        for entry in &self.index {
            let mut buf = AlignedBuffer::new(BLOCK_SIZE)?;
            self.io.read_at(&mut buf, entry.block_offset).await?;

            let reader = BlockReader::parse(buf.as_slice(), entry.block_offset)?;
            for e in reader.entries() {
                results.push((e.key.clone(), e.value.clone()));
            }
        }

        Ok(results)
    }

    /// Scan entries in a key range
    pub async fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut results = Vec::new();

        // Find starting block
        let start_idx = match start {
            Some(k) => self.find_block(k),
            None => 0,
        };

        for i in start_idx..self.index.len() {
            let entry = &self.index[i];

            // Check if we've passed the end key
            if let Some(e) = end {
                if entry.first_key.as_slice() >= e {
                    break;
                }
            }

            let mut buf = AlignedBuffer::new(BLOCK_SIZE)?;
            self.io.read_at(&mut buf, entry.block_offset).await?;

            let reader = BlockReader::parse(buf.as_slice(), entry.block_offset)?;
            for e in reader.entries() {
                // Check range
                if let Some(s) = start {
                    if e.key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(en) = end {
                    if e.key.as_slice() >= en {
                        break;
                    }
                }
                results.push((e.key.clone(), e.value.clone()));
            }
        }

        Ok(results)
    }

    /// Get SSTable metadata
    pub fn metadata(&self) -> &SstableMetadata {
        &self.metadata
    }

    /// Check if key might be in this SSTable (based on key range)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= self.metadata.min_key.as_slice() && key <= self.metadata.max_key.as_slice()
    }
}
