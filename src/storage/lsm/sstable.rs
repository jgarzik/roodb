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
use std::sync::Arc;

use crate::io::{AlignedBuffer, File, IoEngine, OpenOptions, Urgency, PAGE_SIZE};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::block::{BlockBuilder, BlockReader, Entry, BLOCK_SIZE};
use crate::storage::lsm::block_cache::BlockCache;
use crate::storage::lsm::bloom::BloomFilter;

/// SSTable magic number
const SSTABLE_MAGIC: u32 = 0x4C534D54; // "LSMT"

/// SSTable version (v2 adds bloom filter)
const SSTABLE_VERSION: u32 = 2;

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
    /// Bloom filter offset (0 if no bloom filter)
    pub bloom_offset: u64,
    /// Bloom filter size (0 if no bloom filter)
    pub bloom_size: u64,
}

/// SSTable writer
pub struct SstableWriter {
    io: File,
    path: PathBuf,
    /// Logical end of the table: where the next block will live.
    offset: u64,
    /// Blocks not yet handed to the I/O engine. Always whole blocks.
    out: Vec<u8>,
    /// File offset where `out` starts.
    out_offset: u64,
    block_builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    /// How urgent this table's writes are.
    urgency: Urgency,
    /// Bloom filter built during add()
    bloom: BloomFilter,
    /// Number of keys added (for bloom sizing)
    num_keys: usize,
}

/// Blocks are gathered into writes of this size, so a flush costs one request
/// per megabyte rather than one per block.
const WRITE_CHUNK: usize = 256 * BLOCK_SIZE;

impl SstableWriter {
    /// Create a new SSTable writer
    pub async fn create(engine: &IoEngine, path: &Path, urgency: Urgency) -> StorageResult<Self> {
        let io = engine.open(path, OpenOptions::create()).await?;
        Ok(Self {
            io,
            path: path.to_path_buf(),
            offset: 0,
            out: Vec::with_capacity(WRITE_CHUNK),
            out_offset: 0,
            block_builder: BlockBuilder::new(),
            index_entries: Vec::new(),
            min_key: None,
            max_key: None,
            urgency,
            bloom: BloomFilter::new(1024), // Initial size, will be rebuilt in finish()
            num_keys: 0,
        })
    }

    /// Add an entry to the SSTable
    pub async fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> StorageResult<()> {
        let entry = match value {
            Some(v) => Entry::new(key.clone(), v),
            None => Entry::tombstone(key.clone()),
        };

        // Update min/max keys and bloom filter
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.bloom.add(&key);
        self.num_keys += 1;
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
        // is_empty() check above guarantees first_key() returns Some
        let first_key = self
            .block_builder
            .first_key()
            .expect("block not empty: first_key must exist")
            .to_vec();
        self.index_entries.push(IndexEntry {
            block_offset: self.offset,
            first_key,
        });

        // Build and write block
        let block = std::mem::take(&mut self.block_builder).build();
        self.append(&block).await?;

        self.block_builder = BlockBuilder::new();
        Ok(())
    }

    /// Append whole blocks to the table, writing out full chunks as they fill.
    async fn append(&mut self, blocks: &[u8]) -> StorageResult<()> {
        debug_assert!(blocks.len().is_multiple_of(BLOCK_SIZE));
        self.out.extend_from_slice(blocks);
        self.offset += blocks.len() as u64;
        if self.out.len() >= WRITE_CHUNK {
            self.write_out().await?;
        }
        Ok(())
    }

    /// Hand the gathered blocks to the I/O engine.
    async fn write_out(&mut self) -> StorageResult<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        let mut buf = AlignedBuffer::new(self.out.len())?;
        buf.copy_from_slice(&self.out)?;
        self.io.write_at(buf, self.out_offset, self.urgency).await?;
        self.out_offset += self.out.len() as u64;
        self.out.clear();
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

        // Write index block (may span multiple pages)
        let index_offset = self.offset;
        let index_block = self.build_index_block();
        let index_size = index_block.len();
        self.append(&index_block).await?;

        // Write bloom filter (take ownership via swap, pad to BLOCK_SIZE)
        let bloom_offset = self.offset;
        let mut bloom = BloomFilter::new(self.num_keys);
        std::mem::swap(&mut bloom, &mut self.bloom);
        let bloom_bytes = bloom.to_bytes();
        let bloom_size = bloom_bytes.len().next_multiple_of(BLOCK_SIZE);
        let mut bloom_padded = vec![0u8; bloom_size];
        bloom_padded[..bloom_bytes.len()].copy_from_slice(&bloom_bytes);
        self.append(&bloom_padded).await?;

        // Write footer
        let footer =
            self.build_footer(index_offset, index_size, bloom_offset, bloom_bytes.len())?;
        let mut footer_block = vec![0u8; FOOTER_SIZE];
        footer_block[..footer.len()].copy_from_slice(&footer);
        self.append(&footer_block).await?;
        self.write_out().await?;

        // The table is not referenced until it is on stable storage.
        self.io.sync_data(self.urgency).await?;

        Ok(self.path)
    }

    /// Build the index block (may span multiple pages).
    ///
    /// Returns a buffer whose length is a multiple of BLOCK_SIZE.
    /// Layout: [num_entries: 4B] [entries...] [padding] [CRC32: 4B]
    /// CRC covers all bytes from 0..total_size-4.
    fn build_index_block(&self) -> Vec<u8> {
        // Calculate required size: 4 (count) + entries + 4 (CRC)
        let entries_size: usize = self
            .index_entries
            .iter()
            .map(|e| 8 + 2 + e.first_key.len())
            .sum();
        let needed = 4 + entries_size + 4; // count + entries + CRC
        let total_size = needed.next_multiple_of(BLOCK_SIZE);

        let mut buf = vec![0u8; total_size];

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

        // CRC32 at the very end of the buffer
        let crc = crc32fast::hash(&buf[..total_size - 4]);
        buf[total_size - 4..].copy_from_slice(&crc.to_be_bytes());

        buf
    }

    /// Build the footer block
    fn build_footer(
        &self,
        index_offset: u64,
        index_size: usize,
        bloom_offset: u64,
        bloom_size: usize,
    ) -> StorageResult<Vec<u8>> {
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
        buf[offset..offset + 8].copy_from_slice(&(index_size as u64).to_be_bytes());
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
        offset += max_key.len();

        // Bloom filter offset (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&bloom_offset.to_be_bytes());
        offset += 8;

        // Bloom filter size (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&(bloom_size as u64).to_be_bytes());
        let _ = offset;

        // Magic and version at the very end
        buf[FOOTER_SIZE - 8..FOOTER_SIZE - 4].copy_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        buf[FOOTER_SIZE - 4..].copy_from_slice(&SSTABLE_VERSION.to_be_bytes());

        Ok(buf)
    }
}

/// SSTable reader
pub struct SstableReader {
    io: File,
    metadata: SstableMetadata,
    index: Vec<IndexEntry>,
    /// How urgent this reader's reads are.
    urgency: Urgency,
    /// Optional block cache for avoiding repeated disk reads
    block_cache: Option<Arc<BlockCache>>,
    /// SSTable name for cache key (shared to avoid per-lookup allocation)
    cache_name: Arc<str>,
    /// Bloom filter (if present in SSTable v2+)
    bloom: Option<BloomFilter>,
}

/// Read `len` bytes (a whole number of blocks) at `offset` with one request.
async fn read_region(
    io: &File,
    offset: u64,
    len: usize,
    urgency: Urgency,
) -> StorageResult<Vec<u8>> {
    let buf = io
        .read_at(AlignedBuffer::new(len)?, offset, urgency)
        .await?;
    if buf.len() != len {
        return Err(StorageError::InvalidSstable(format!(
            "short read at offset {offset}: wanted {len} bytes, file has {}",
            buf.len()
        )));
    }
    Ok(buf.as_slice().to_vec())
}

/// Longest run of adjacent blocks fetched with one request during a scan.
const MAX_READ_RUN: usize = 256;

impl SstableReader {
    /// Open an existing SSTable
    pub async fn open(engine: &IoEngine, path: &Path, urgency: Urgency) -> StorageResult<Self> {
        Self::open_with_cache(engine, path, urgency, None).await
    }

    /// Open an existing SSTable with an optional block cache
    pub async fn open_with_cache(
        engine: &IoEngine,
        path: &Path,
        urgency: Urgency,
        block_cache: Option<Arc<BlockCache>>,
    ) -> StorageResult<Self> {
        let io = engine.open(path, OpenOptions::read_only()).await?;

        // Derive cache name from file path
        let cache_name: Arc<str> = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .into();

        let file_size = io.size()?;
        if file_size < FOOTER_SIZE as u64 {
            return Err(StorageError::InvalidSstable("file too small".to_string()));
        }

        // Fetch a region through the block cache when there is one.
        let fetch = |offset: u64, len: usize| {
            let io = &io;
            let cache = block_cache.as_ref();
            let cache_name = &cache_name;
            async move {
                if let Some(cached) = cache.and_then(|c| c.get(cache_name, offset)) {
                    return Ok::<_, StorageError>(cached);
                }
                let data = read_region(io, offset, len, urgency).await?;
                if let Some(cache) = cache {
                    cache.insert(cache_name, offset, data.clone());
                }
                Ok(Arc::new(data))
            }
        };

        let footer_data = fetch(file_size - FOOTER_SIZE as u64, FOOTER_SIZE).await?;
        let metadata = Self::parse_footer(&footer_data)?;

        // The index may span several blocks; it is cached as one unit.
        let index_data = fetch(metadata.index_offset, metadata.index_size as usize).await?;
        let index = Self::parse_index(&index_data)?;

        // Bloom filter (v2+)
        let bloom = if metadata.bloom_size > 0 {
            let disk_size = (metadata.bloom_size as usize).next_multiple_of(BLOCK_SIZE);
            let bloom_data = fetch(metadata.bloom_offset, disk_size).await?;
            BloomFilter::from_bytes(&bloom_data[..metadata.bloom_size as usize])
        } else {
            None
        };

        Ok(Self {
            io,
            metadata,
            index,
            urgency,
            block_cache,
            cache_name,
            bloom,
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

        // Verify version (accept v1 and v2)
        let version = u32::from_be_bytes([
            buf[FOOTER_SIZE - 4],
            buf[FOOTER_SIZE - 3],
            buf[FOOTER_SIZE - 2],
            buf[FOOTER_SIZE - 1],
        ]);
        if version != 1 && version != SSTABLE_VERSION {
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
        offset += max_key_len;

        // Bloom filter fields (v2 only)
        let (bloom_offset, bloom_size) = if version >= 2 {
            let bo = u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let bs = u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());
            let _ = offset;
            (bo, bs)
        } else {
            (0, 0)
        };

        Ok(SstableMetadata {
            index_offset,
            index_size,
            num_blocks,
            min_key,
            max_key,
            bloom_offset,
            bloom_size,
        })
    }

    /// Parse index block from buffer (may be multiple pages)
    fn parse_index(buf: &[u8]) -> StorageResult<Vec<IndexEntry>> {
        let total_size = buf.len();
        // Verify CRC (last 4 bytes of the buffer)
        let stored_crc = u32::from_be_bytes([
            buf[total_size - 4],
            buf[total_size - 3],
            buf[total_size - 2],
            buf[total_size - 1],
        ]);
        let computed_crc = crc32fast::hash(&buf[..total_size - 4]);
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

    /// Read a data block, using cache if available
    pub async fn read_data_block(&self, block_offset: u64) -> StorageResult<Arc<Vec<u8>>> {
        if let Some(ref cache) = self.block_cache {
            if let Some(cached) = cache.get(&self.cache_name, block_offset) {
                return Ok(cached);
            }
        }

        let data = read_region(&self.io, block_offset, BLOCK_SIZE, self.urgency).await?;

        if let Some(ref cache) = self.block_cache {
            cache.insert(&self.cache_name, block_offset, data.clone());
        }

        Ok(Arc::new(data))
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        // Check key range
        if key < self.metadata.min_key.as_slice() || key > self.metadata.max_key.as_slice() {
            return Ok(None);
        }

        // Check bloom filter — skip block reads on definite miss
        if let Some(ref bloom) = self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        // Binary search index to find the right block
        let block_idx = self.find_block(key);

        // Read and search the block (with cache)
        let block_offset = self.index[block_idx].block_offset;
        let block_data = self.read_data_block(block_offset).await?;

        let reader = BlockReader::parse(&block_data, block_offset)?;
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
            let block_data = self.read_data_block(entry.block_offset).await?;
            let reader = BlockReader::parse(&block_data, entry.block_offset)?;
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

        for entry in self.index.iter().skip(start_idx) {
            // Check if we've passed the end key
            if let Some(e) = end {
                if entry.first_key.as_slice() >= e {
                    break;
                }
            }

            let block_data = self.read_data_block(entry.block_offset).await?;
            let reader = BlockReader::parse(&block_data, entry.block_offset)?;
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

    /// Scan entries in a key range using batched block reads.
    ///
    /// Instead of reading blocks one at a time, this determines which blocks
    /// are needed, submits all cache-miss reads in a single batch via read_batch(),
    /// then processes entries from the fetched blocks.
    pub async fn scan_range_batched(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        // Find starting and ending block indices
        let start_idx = match start {
            Some(k) => self.find_block(k),
            None => 0,
        };

        // Determine which blocks we need (and which are cache misses)
        let mut needed_blocks: Vec<(usize, u64)> = Vec::new(); // (block_index, offset)
        for (i, entry) in self.index.iter().enumerate().skip(start_idx) {
            if let Some(e) = end {
                if entry.first_key.as_slice() >= e {
                    break;
                }
            }
            needed_blocks.push((i, entry.block_offset));
        }

        if needed_blocks.is_empty() {
            return Ok(Vec::new());
        }

        // Separate cache hits from misses
        let mut miss_indices: Vec<usize> = Vec::new(); // indices into needed_blocks
        let mut cached_blocks: Vec<Option<Arc<Vec<u8>>>> = Vec::with_capacity(needed_blocks.len());

        for (idx, &(_, offset)) in needed_blocks.iter().enumerate() {
            if let Some(ref cache) = self.block_cache {
                if let Some(cached) = cache.get(&self.cache_name, offset) {
                    cached_blocks.push(Some(cached));
                    continue;
                }
            }
            cached_blocks.push(None);
            miss_indices.push(idx);
        }

        // Fetch the misses. Adjacent blocks are read with one request per run,
        // and all runs are in flight together.
        if !miss_indices.is_empty() {
            let mut runs: Vec<(usize, usize)> = Vec::new(); // (first miss, count)
            for (pos, &idx) in miss_indices.iter().enumerate() {
                match runs.last_mut() {
                    Some((first, count))
                        if *count < MAX_READ_RUN
                            && miss_indices[*first + *count - 1] + 1 == idx
                            && needed_blocks[miss_indices[*first]].1
                                + (*count * BLOCK_SIZE) as u64
                                == needed_blocks[idx].1 =>
                    {
                        *count += 1;
                    }
                    _ => runs.push((pos, 1)),
                }
            }

            let reads = runs.iter().map(|&(first, count)| {
                let offset = needed_blocks[miss_indices[first]].1;
                read_region(&self.io, offset, count * BLOCK_SIZE, self.urgency)
            });
            let fetched = futures_util::future::try_join_all(reads).await?;

            for (&(first, count), data) in runs.iter().zip(fetched) {
                for (i, block) in data
                    .as_chunks::<BLOCK_SIZE>()
                    .0
                    .iter()
                    .enumerate()
                    .take(count)
                {
                    let idx = miss_indices[first + i];
                    let offset = needed_blocks[idx].1;
                    let block = block.to_vec();
                    if let Some(ref cache) = self.block_cache {
                        cache.insert(&self.cache_name, offset, block.clone());
                    }
                    cached_blocks[idx] = Some(Arc::new(block));
                }
            }
        }

        // Process all blocks and collect results — take ownership to avoid clones
        let mut results = Vec::new();
        for (i, &(_, offset)) in needed_blocks.iter().enumerate() {
            let block_data = cached_blocks[i]
                .as_ref()
                .expect("all blocks should be loaded after batch read");

            let reader = BlockReader::parse(block_data, offset)?;
            for e in reader.into_entries() {
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
                results.push((e.key, e.value));
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

    /// Eagerly load all data blocks into the block cache.
    ///
    /// Called after flush/compaction to pre-populate the cache so the first
    /// scan has zero cold misses.
    pub async fn eager_load_blocks(&self) -> StorageResult<()> {
        for entry in &self.index {
            self.read_data_block(entry.block_offset).await?;
        }
        Ok(())
    }
}
