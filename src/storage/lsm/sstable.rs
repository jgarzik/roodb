//! SSTable (Sorted String Table) reader and writer
//!
//! File layout. Every region is a whole number of 4 KiB pages:
//! ```text
//! +------------------+
//! | Data block 0     |  one page, or more for an oversized entry
//! | Data block 1     |
//! | ...              |
//! | Index            |  where each block is, its first key, the last key
//! | Bloom filter     |
//! | Footer           |  one page
//! +------------------+
//! ```
//!
//! Index:
//! ```text
//! | Num blocks (4B)                                             |
//! | per block: offset (8B), length (4B), key len (2B), first key |
//! | Max key len (2B), max key                                    |
//! | Padding, CRC32 (4B) over everything before it                |
//! ```
//!
//! Footer (last page of the file):
//! ```text
//! | Index offset (8B) | Index size (8B)                |
//! | Bloom offset (8B) | Bloom size (8B)                |
//! | Num blocks (4B)   | Num entries (8B)               |
//! | Bloom CRC32 (4B)                                   |
//! | Padding                                            |
//! | CRC32 (4B) over everything before it               |
//! | Magic (4B): "LSMT" | Version (4B): 3               |
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::{AlignedBuffer, File, IoEngine, OpenOptions, Urgency, PAGE_SIZE};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::lsm::block::{
    check_entry_size, find_in_block, verify_block, BlockBuilder, BlockReader, Entry, BLOCK_SIZE,
};
use crate::storage::lsm::block_cache::BlockCache;
use crate::storage::lsm::bloom::BloomFilter;

/// SSTable magic number
const SSTABLE_MAGIC: u32 = 0x4C534D54; // "LSMT"

/// SSTable format version. Version 3 has variable-size blocks, 32-bit value
/// lengths and checksums on the index and footer.
const SSTABLE_VERSION: u32 = 3;

/// Footer size (one page)
const FOOTER_SIZE: usize = PAGE_SIZE;

/// Offset of the footer checksum; magic and version follow it.
const FOOTER_CRC_AT: usize = FOOTER_SIZE - 12;

/// Blocks are gathered into writes of about this size, so a flush costs one
/// request per megabyte rather than one per block.
const WRITE_CHUNK: usize = 256 * BLOCK_SIZE;

/// Adjacent blocks are fetched with one request per run of about this size.
const MAX_READ_RUN: usize = 256 * BLOCK_SIZE;

/// Blocks loaded per step when warming the cache, which bounds how much of a
/// table is held in memory at once.
const WARM_BATCH: usize = 256;

/// Where one data block is and the first key in it.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub block_offset: u64,
    pub block_len: u32,
    pub first_key: Vec<u8>,
}

/// SSTable metadata from the footer and index
#[derive(Debug, Clone)]
pub struct SstableMetadata {
    pub index_offset: u64,
    pub index_size: u64,
    pub num_blocks: u32,
    pub num_entries: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    /// Bloom filter offset
    pub bloom_offset: u64,
    /// Bloom filter size in bytes (its region is padded to a page)
    pub bloom_size: u64,
    /// Checksum of the bloom filter bytes
    pub bloom_crc: u32,
}

fn invalid(what: impl Into<String>) -> StorageError {
    StorageError::InvalidSstable(what.into())
}

/// SSTable writer
pub struct SstableWriter {
    io: File,
    path: PathBuf,
    /// Logical end of the table: where the next region will live.
    offset: u64,
    /// Regions not yet handed to the I/O engine. Always whole pages.
    out: Vec<u8>,
    /// File offset where `out` starts.
    out_offset: u64,
    block_builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    max_key: Vec<u8>,
    /// How urgent this table's writes are.
    urgency: Urgency,
    /// Hashes of every key, for a bloom filter sized once the count is known.
    key_hashes: Vec<(u64, u64)>,
}

impl SstableWriter {
    /// Create a new SSTable writer
    pub async fn create(engine: &IoEngine, path: &Path, urgency: Urgency) -> StorageResult<Self> {
        // Table names are never reused within a run and leftovers from a
        // crashed run are deleted at startup, so the file must not exist.
        let io = engine.open(path, OpenOptions::create_new()).await?;
        Ok(Self {
            io,
            path: path.to_path_buf(),
            offset: 0,
            out: Vec::with_capacity(WRITE_CHUNK),
            out_offset: 0,
            block_builder: BlockBuilder::new(),
            index_entries: Vec::new(),
            max_key: Vec::new(),
            urgency,
            key_hashes: Vec::new(),
        })
    }

    /// Add an entry to the SSTable. Entries must arrive in key order.
    pub async fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> StorageResult<()> {
        check_entry_size(key.len(), value.as_ref().map_or(0, Vec::len))?;
        self.key_hashes.push(BloomFilter::hash_key(&key));
        self.max_key.clone_from(&key);

        let entry = Entry { key, value };
        if !self.block_builder.can_fit(&entry) {
            self.flush_block().await?;
        }
        let added = self.block_builder.add(entry);
        debug_assert!(added, "an empty block takes any entry");
        Ok(())
    }

    /// Finish the current block and queue it for writing.
    async fn flush_block(&mut self) -> StorageResult<()> {
        let Some(first_key) = self.block_builder.first_key().map(<[u8]>::to_vec) else {
            return Ok(());
        };
        let block = std::mem::take(&mut self.block_builder).build();
        let block_len =
            u32::try_from(block.len()).map_err(|_| invalid("block larger than 4 GiB"))?;
        self.index_entries.push(IndexEntry {
            block_offset: self.offset,
            block_len,
            first_key,
        });
        self.append(&block).await
    }

    /// Append whole pages to the table, writing out full chunks as they fill.
    async fn append(&mut self, pages: &[u8]) -> StorageResult<()> {
        debug_assert!(pages.len().is_multiple_of(PAGE_SIZE));
        self.out.extend_from_slice(pages);
        self.offset += pages.len() as u64;
        if self.out.len() >= WRITE_CHUNK {
            self.write_out().await?;
        }
        Ok(())
    }

    /// Hand the gathered pages to the I/O engine.
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

    /// Finish writing the SSTable and make it durable.
    pub async fn finish(mut self) -> StorageResult<PathBuf> {
        self.flush_block().await?;

        let index_offset = self.offset;
        let index = self.build_index();
        let index_size = index.len();
        self.append(&index).await?;

        // The bloom filter is sized for the keys actually written.
        let bloom_offset = self.offset;
        let mut bloom = BloomFilter::new(self.key_hashes.len());
        for hash in &self.key_hashes {
            bloom.add_hash(*hash);
        }
        let mut bloom_bytes = bloom.to_bytes();
        let bloom_size = bloom_bytes.len();
        bloom_bytes.resize(bloom_size.next_multiple_of(PAGE_SIZE), 0);
        self.append(&bloom_bytes).await?;

        let bloom_crc = crc32fast::hash(&bloom_bytes[..bloom_size]);
        let footer = self.build_footer(
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            bloom_crc,
        );
        self.append(&footer).await?;
        self.write_out().await?;

        // The table is not referenced until it is on stable storage.
        self.io.sync_data(self.urgency).await?;

        Ok(self.path)
    }

    fn build_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.index_entries.len() as u32).to_be_bytes());
        for entry in &self.index_entries {
            buf.extend_from_slice(&entry.block_offset.to_be_bytes());
            buf.extend_from_slice(&entry.block_len.to_be_bytes());
            buf.extend_from_slice(&(entry.first_key.len() as u16).to_be_bytes());
            buf.extend_from_slice(&entry.first_key);
        }
        buf.extend_from_slice(&(self.max_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.max_key);

        let len = (buf.len() + 4).next_multiple_of(PAGE_SIZE);
        buf.resize(len - 4, 0);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }

    fn build_footer(
        &self,
        index_offset: u64,
        index_size: usize,
        bloom_offset: u64,
        bloom_size: usize,
        bloom_crc: u32,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&index_offset.to_be_bytes());
        buf[8..16].copy_from_slice(&(index_size as u64).to_be_bytes());
        buf[16..24].copy_from_slice(&bloom_offset.to_be_bytes());
        buf[24..32].copy_from_slice(&(bloom_size as u64).to_be_bytes());
        buf[32..36].copy_from_slice(&(self.index_entries.len() as u32).to_be_bytes());
        buf[36..44].copy_from_slice(&(self.key_hashes.len() as u64).to_be_bytes());
        buf[44..48].copy_from_slice(&bloom_crc.to_be_bytes());
        let crc = crc32fast::hash(&buf[..FOOTER_CRC_AT]);
        buf[FOOTER_CRC_AT..FOOTER_CRC_AT + 4].copy_from_slice(&crc.to_be_bytes());
        buf[FOOTER_SIZE - 8..FOOTER_SIZE - 4].copy_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        buf[FOOTER_SIZE - 4..].copy_from_slice(&SSTABLE_VERSION.to_be_bytes());
        buf
    }
}

/// Read `len` bytes (a whole number of pages) at `offset` with one request.
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
        return Err(invalid(format!(
            "short read at offset {offset}: wanted {len} bytes, file has {}",
            buf.len()
        )));
    }
    Ok(buf.as_slice().to_vec())
}

fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(buf[at..at + 8].try_into().expect("8 bytes"))
}

fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(buf[at..at + 4].try_into().expect("4 bytes"))
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
    bloom: Option<BloomFilter>,
}

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
        if file_size < FOOTER_SIZE as u64 || !file_size.is_multiple_of(PAGE_SIZE as u64) {
            return Err(invalid(format!("file size {file_size} is not a table")));
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

        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer = fetch(footer_offset, FOOTER_SIZE).await?;
        let mut metadata = Self::parse_footer(&footer, footer_offset)?;

        let index_data = fetch(metadata.index_offset, metadata.index_size as usize).await?;
        let (index, max_key) = Self::parse_index(&index_data, &metadata)?;
        metadata.min_key = index
            .first()
            .map(|e| e.first_key.clone())
            .unwrap_or_default();
        metadata.max_key = max_key;

        let bloom_region = (metadata.bloom_size as usize).next_multiple_of(PAGE_SIZE);
        let bloom = if bloom_region > 0 {
            let data = fetch(metadata.bloom_offset, bloom_region).await?;
            let bytes = &data[..metadata.bloom_size as usize];
            // A damaged filter would answer "not here" for keys that are.
            let actual = crc32fast::hash(bytes);
            if actual != metadata.bloom_crc {
                return Err(StorageError::CorruptedBlock {
                    offset: metadata.bloom_offset,
                    expected: metadata.bloom_crc,
                    actual,
                });
            }
            Some(
                BloomFilter::from_bytes(bytes)
                    .ok_or_else(|| invalid("bloom filter header is not valid"))?,
            )
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

    /// Parse and validate the footer. `footer_offset` is where it sits, which
    /// bounds every region it points at.
    fn parse_footer(buf: &[u8], footer_offset: u64) -> StorageResult<SstableMetadata> {
        if buf.len() != FOOTER_SIZE {
            return Err(invalid("footer has the wrong size"));
        }
        let magic = u32_at(buf, FOOTER_SIZE - 8);
        if magic != SSTABLE_MAGIC {
            return Err(invalid(format!("bad magic {magic:#x}")));
        }
        let version = u32_at(buf, FOOTER_SIZE - 4);
        if version != SSTABLE_VERSION {
            return Err(invalid(format!(
                "format version {version} is not supported (this build reads version {SSTABLE_VERSION})"
            )));
        }
        let stored = u32_at(buf, FOOTER_CRC_AT);
        let actual = crc32fast::hash(&buf[..FOOTER_CRC_AT]);
        if stored != actual {
            return Err(StorageError::CorruptedBlock {
                offset: footer_offset,
                expected: stored,
                actual,
            });
        }

        let metadata = SstableMetadata {
            index_offset: u64_at(buf, 0),
            index_size: u64_at(buf, 8),
            bloom_offset: u64_at(buf, 16),
            bloom_size: u64_at(buf, 24),
            num_blocks: u32_at(buf, 32),
            num_entries: u64_at(buf, 36),
            bloom_crc: u32_at(buf, 44),
            min_key: Vec::new(),
            max_key: Vec::new(),
        };

        let page = PAGE_SIZE as u64;
        let bloom_region = metadata.bloom_size.next_multiple_of(page);
        let layout_ok = metadata.index_offset.is_multiple_of(page)
            && metadata.index_size >= page
            && metadata.index_size.is_multiple_of(page)
            && metadata.index_offset.checked_add(metadata.index_size)
                == Some(metadata.bloom_offset)
            && metadata.bloom_offset.checked_add(bloom_region) == Some(footer_offset);
        if !layout_ok {
            return Err(invalid("footer describes an impossible layout"));
        }
        Ok(metadata)
    }

    /// Parse the index: the block list and the table's last key.
    fn parse_index(
        buf: &[u8],
        metadata: &SstableMetadata,
    ) -> StorageResult<(Vec<IndexEntry>, Vec<u8>)> {
        if buf.len() < PAGE_SIZE {
            return Err(invalid("index too small"));
        }
        let body = buf.len() - 4;
        let stored = u32_at(buf, body);
        let actual = crc32fast::hash(&buf[..body]);
        if stored != actual {
            return Err(StorageError::CorruptedBlock {
                offset: metadata.index_offset,
                expected: stored,
                actual,
            });
        }

        let take = |pos: &mut usize, len: usize| -> StorageResult<&[u8]> {
            let end = pos
                .checked_add(len)
                .filter(|end| *end <= body)
                .ok_or_else(|| invalid("index truncated"))?;
            let slice = &buf[*pos..end];
            *pos = end;
            Ok(slice)
        };

        let mut pos = 0;
        let num_blocks = u32_at(take(&mut pos, 4)?, 0);
        if num_blocks != metadata.num_blocks {
            return Err(invalid("index and footer disagree on the block count"));
        }
        let mut entries = Vec::new();
        let mut expected_offset = 0u64;
        for _ in 0..num_blocks {
            let block_offset = u64_at(take(&mut pos, 8)?, 0);
            let block_len = u32_at(take(&mut pos, 4)?, 0);
            let key_len = u16::from_be_bytes(take(&mut pos, 2)?.try_into().expect("2 bytes"));
            let first_key = take(&mut pos, key_len as usize)?.to_vec();
            // Blocks are laid out back to back from the start of the file.
            if block_offset != expected_offset
                || block_len == 0
                || !(block_len as usize).is_multiple_of(BLOCK_SIZE)
            {
                return Err(invalid("index describes an impossible block"));
            }
            expected_offset += block_len as u64;
            entries.push(IndexEntry {
                block_offset,
                block_len,
                first_key,
            });
        }
        if expected_offset != metadata.index_offset {
            return Err(invalid("blocks do not end where the index begins"));
        }
        let max_key_len = u16::from_be_bytes(take(&mut pos, 2)?.try_into().expect("2 bytes"));
        let max_key = take(&mut pos, max_key_len as usize)?.to_vec();
        Ok((entries, max_key))
    }

    /// Fetch the given blocks (indices into the index, ascending), from the
    /// cache where possible. Blocks that have to be read are fetched as runs of
    /// adjacent blocks, one request per run, all runs in flight together.
    async fn fetch_blocks(&self, wanted: &[usize]) -> StorageResult<Vec<Arc<Vec<u8>>>> {
        let mut blocks: Vec<Option<Arc<Vec<u8>>>> = Vec::with_capacity(wanted.len());
        let mut misses: Vec<usize> = Vec::new(); // positions in `wanted`
        for (pos, &idx) in wanted.iter().enumerate() {
            let cached = self
                .block_cache
                .as_ref()
                .and_then(|cache| cache.get(&self.cache_name, self.index[idx].block_offset));
            if cached.is_none() {
                misses.push(pos);
            }
            blocks.push(cached);
        }

        if !misses.is_empty() {
            // (first position in `misses`, block count, bytes)
            let mut runs: Vec<(usize, usize, usize)> = Vec::new();
            for (m, &pos) in misses.iter().enumerate() {
                let entry = &self.index[wanted[pos]];
                let len = entry.block_len as usize;
                match runs.last_mut() {
                    Some((first, count, bytes))
                        if *bytes + len <= MAX_READ_RUN
                            && self.index[wanted[misses[*first]]].block_offset + *bytes as u64
                                == entry.block_offset =>
                    {
                        *count += 1;
                        *bytes += len;
                    }
                    _ => runs.push((m, 1, len)),
                }
            }

            let reads = runs.iter().map(|&(first, _, bytes)| {
                let offset = self.index[wanted[misses[first]]].block_offset;
                read_region(&self.io, offset, bytes, self.urgency)
            });
            let fetched = futures_util::future::try_join_all(reads).await?;

            for (&(first, count, _), data) in runs.iter().zip(fetched) {
                let mut at = 0;
                for &pos in &misses[first..first + count] {
                    let entry = &self.index[wanted[pos]];
                    let len = entry.block_len as usize;
                    let block = data[at..at + len].to_vec();
                    at += len;
                    // Checked once here; cached blocks are trusted.
                    verify_block(&block, entry.block_offset)?;
                    if let Some(ref cache) = self.block_cache {
                        cache.insert(&self.cache_name, entry.block_offset, block.clone());
                    }
                    blocks[pos] = Some(Arc::new(block));
                }
            }
        }

        Ok(blocks
            .into_iter()
            .map(|block| block.expect("every block is cached or was just read"))
            .collect())
    }

    /// Get a value by key. `Some(None)` is a tombstone.
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Option<Vec<u8>>>> {
        if self.index.is_empty() || !self.may_contain(key) {
            return Ok(None);
        }

        // Check bloom filter — skip block reads on definite miss
        if let Some(ref bloom) = self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        let block_idx = self.find_block(key);
        let block = self.fetch_blocks(&[block_idx]).await?.remove(0);
        find_in_block(&block, key)
    }

    /// Find the block that might contain a key: the last one whose first key
    /// is not after it.
    fn find_block(&self, key: &[u8]) -> usize {
        self.index
            .partition_point(|entry| entry.first_key.as_slice() <= key)
            .saturating_sub(1)
    }

    /// The first entry after `key`: its key, and whether it is a tombstone.
    /// Reads one block, or two when `key` is the last of its block.
    pub async fn next_after(&self, key: &[u8]) -> StorageResult<Option<(Vec<u8>, bool)>> {
        self.first_beyond(key, false).await
    }

    /// The first entry at or after `key`: its key, and whether it is a
    /// tombstone. Reads one block, or two when the block `key` falls in has
    /// nothing that far on.
    pub async fn first_from(&self, key: &[u8]) -> StorageResult<Option<(Vec<u8>, bool)>> {
        self.first_beyond(key, true).await
    }

    /// The first entry after `key`, or at it if `inclusive`.
    async fn first_beyond(
        &self,
        key: &[u8],
        inclusive: bool,
    ) -> StorageResult<Option<(Vec<u8>, bool)>> {
        let max_key = self.metadata.max_key.as_slice();
        if self.index.is_empty() || key > max_key || (key == max_key && !inclusive) {
            return Ok(None);
        }
        let mut block_idx = self.find_block(key);
        while block_idx < self.index.len() {
            let block = self.fetch_blocks(&[block_idx]).await?.remove(0);
            let reader = BlockReader::parse_verified(&block)?;
            let entries = reader.entries();
            let at = entries.partition_point(|entry| {
                entry.key.as_slice() < key || (!inclusive && entry.key.as_slice() == key)
            });
            if let Some(entry) = entries.get(at) {
                return Ok(Some((entry.key.clone(), entry.is_tombstone())));
            }
            block_idx += 1;
        }
        Ok(None)
    }

    /// The last entry before `key`: its key, and whether it is a tombstone.
    /// Reads one block.
    pub async fn prev_before(&self, key: &[u8]) -> StorageResult<Option<(Vec<u8>, bool)>> {
        if self.index.is_empty() || key <= self.metadata.min_key.as_slice() {
            return Ok(None);
        }
        // The last block whose first key is before `key` holds the answer:
        // that first key at least.
        let block_idx = self
            .index
            .partition_point(|entry| entry.first_key.as_slice() < key)
            .saturating_sub(1);
        let block = self.fetch_blocks(&[block_idx]).await?.remove(0);
        let reader = BlockReader::parse_verified(&block)?;
        let entries = reader.entries();
        let at = entries.partition_point(|entry| entry.key.as_slice() < key);
        Ok(at
            .checked_sub(1)
            .map(|i| (entries[i].key.clone(), entries[i].is_tombstone())))
    }

    /// Entries after `after` (from the start if `None`), in order, about
    /// `max_bytes` of them, tombstones included. The flag says whether the
    /// end of the table was reached. Reads only the blocks it returns
    /// entries from.
    pub async fn scan_after(
        &self,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> StorageResult<(Vec<(Vec<u8>, Option<Vec<u8>>)>, bool)> {
        let mut entries = Vec::new();
        let mut bytes = 0;
        let mut block_idx = after.map_or(0, |key| self.find_block(key));
        while block_idx < self.index.len() {
            if bytes >= max_bytes && !entries.is_empty() {
                return Ok((entries, false));
            }
            let block = self.fetch_blocks(&[block_idx]).await?.remove(0);
            for entry in BlockReader::parse_verified(&block)?.into_entries() {
                if after.is_some_and(|after| entry.key.as_slice() <= after) {
                    continue;
                }
                bytes += entry.key.len() + entry.value.as_ref().map_or(0, Vec::len);
                entries.push((entry.key, entry.value));
            }
            block_idx += 1;
        }
        Ok((entries, true))
    }

    /// Scan all entries in the SSTable
    pub async fn scan(&self) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.scan_range_batched(None, None).await
    }

    /// Scan entries in a key range
    pub async fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.scan_range_batched(start, end).await
    }

    /// Scan entries in `[start, end)`.
    ///
    /// Works out which blocks are needed, reads the ones not in the cache -
    /// adjacent blocks as one request per run, all runs in flight together -
    /// then collects the entries in range.
    pub async fn scan_range_batched(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> StorageResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let start_idx = start.map_or(0, |key| self.find_block(key));
        let wanted: Vec<usize> = (start_idx..self.index.len())
            .take_while(|&i| end.is_none_or(|e| self.index[i].first_key.as_slice() < e))
            .collect();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }

        let blocks = self.fetch_blocks(&wanted).await?;
        let mut results = Vec::new();
        for block in &blocks {
            let reader = BlockReader::parse_verified(block)?;
            for entry in reader.into_entries() {
                if start.is_some_and(|s| entry.key.as_slice() < s) {
                    continue;
                }
                if end.is_some_and(|e| entry.key.as_slice() >= e) {
                    break;
                }
                results.push((entry.key, entry.value));
            }
        }
        Ok(results)
    }

    /// The name of the table's file.
    pub fn file_name(&self) -> &str {
        &self.cache_name
    }

    /// Get SSTable metadata
    pub fn metadata(&self) -> &SstableMetadata {
        &self.metadata
    }

    /// Check if key might be in this SSTable (based on key range)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= self.metadata.min_key.as_slice() && key <= self.metadata.max_key.as_slice()
    }

    /// Load every data block into the block cache.
    ///
    /// Called after flush/compaction to pre-populate the cache so the first
    /// scan has zero cold misses.
    pub async fn eager_load_blocks(&self) -> StorageResult<()> {
        let all: Vec<usize> = (0..self.index.len()).collect();
        for batch in all.chunks(WARM_BATCH) {
            self.fetch_blocks(batch).await?;
        }
        Ok(())
    }
}
