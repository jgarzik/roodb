//! WAL manager for append, sync, and recovery operations

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::io::{AsyncIO, AsyncIOFactory};
use crate::wal::error::{WalError, WalResult};
use crate::wal::record::Record;
use crate::wal::segment::{Segment, DEFAULT_SEGMENT_SIZE, SEGMENT_HEADER_SIZE};

/// WAL manager configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL files
    pub dir: PathBuf,
    /// Maximum segment size
    pub segment_size: u64,
    /// Sync after every write
    pub sync_on_write: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("wal"),
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_on_write: true,
        }
    }
}

/// WAL manager
pub struct WalManager<IO: AsyncIO> {
    config: WalConfig,
    /// Current active segment for writes (async mutex for awaits)
    active_segment: Mutex<Option<Segment<IO>>>,
    /// All segments indexed by starting LSN
    segments: RwLock<BTreeMap<u64, PathBuf>>,
    /// Current LSN
    current_lsn: AtomicU64,
    /// Closed flag
    closed: AtomicBool,
}

impl<IO: AsyncIO> WalManager<IO> {
    /// Create a new WAL manager
    pub async fn new<F: AsyncIOFactory<IO = IO>>(
        factory: Arc<F>,
        config: WalConfig,
    ) -> WalResult<Self> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&config.dir)?;

        let manager = Self {
            config: config.clone(),
            active_segment: Mutex::new(None),
            segments: RwLock::new(BTreeMap::new()),
            current_lsn: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        };

        // Scan for existing segments
        manager.scan_segments()?;

        // Recover from existing segments
        let max_lsn = manager.recover(factory.as_ref()).await?;
        manager.current_lsn.store(max_lsn + 1, Ordering::SeqCst);

        Ok(manager)
    }

    /// Scan the WAL directory for existing segments
    fn scan_segments(&self) -> WalResult<()> {
        let mut segments = self.segments.write();

        for entry in std::fs::read_dir(&self.config.dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal_") && name.ends_with(".log") {
                    // Parse segment ID from filename
                    if let Some(id_str) = name
                        .strip_prefix("wal_")
                        .and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(id) = u64::from_str_radix(id_str, 16) {
                            segments.insert(id, path);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Recover and find the maximum LSN
    async fn recover<F: AsyncIOFactory<IO = IO>>(&self, factory: &F) -> WalResult<u64> {
        // Copy segment IDs under lock, then release before async operations
        let segment_ids: Vec<u64> = {
            let segments = self.segments.read();
            segments.keys().copied().collect()
        };

        let mut max_lsn = 0u64;

        for segment_id in segment_ids {
            let segment = Segment::open(
                factory,
                &self.config.dir,
                segment_id,
                self.config.segment_size,
            )
            .await?;

            // Scan records to find max LSN
            let mut offset = SEGMENT_HEADER_SIZE;
            while offset < segment.write_offset() {
                match segment.read_at(offset).await {
                    Ok(record) => {
                        max_lsn = max_lsn.max(record.lsn);
                        offset += record.encoded_size() as u64;
                    }
                    Err(_) => break,
                }
            }
        }

        Ok(max_lsn)
    }

    /// Append a record to the WAL
    pub async fn append<F: AsyncIOFactory<IO = IO>>(
        &self,
        factory: &F,
        data: Vec<u8>,
    ) -> WalResult<u64> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(WalError::Closed);
        }

        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        let record = Record::new(lsn, data)?;

        let mut active = self.active_segment.lock().await;

        // Try to append to existing segment
        if let Some(ref mut segment) = *active {
            if let Some(_offset) = segment.append(&record).await? {
                if self.config.sync_on_write {
                    segment.sync().await?;
                }
                return Ok(lsn);
            }
            // Segment is full, need new one
        }

        // Create new segment
        let mut new_segment =
            Segment::create(factory, &self.config.dir, lsn, self.config.segment_size).await?;

        // Register segment
        self.segments
            .write()
            .insert(lsn, new_segment.path().to_path_buf());

        // Append to new segment
        new_segment.append(&record).await?;
        if self.config.sync_on_write {
            new_segment.sync().await?;
        }

        *active = Some(new_segment);

        Ok(lsn)
    }

    /// Sync the WAL to disk
    pub async fn sync(&self) -> WalResult<()> {
        let active = self.active_segment.lock().await;
        if let Some(ref segment) = *active {
            segment.sync().await?;
        }
        Ok(())
    }

    /// Read all records from a specific LSN
    pub async fn read_from<F: AsyncIOFactory<IO = IO>>(
        &self,
        factory: &F,
        from_lsn: u64,
    ) -> WalResult<Vec<Record>> {
        // Copy segment IDs under lock, then release before async operations
        let segment_ids: Vec<u64> = {
            let segments = self.segments.read();
            segments.keys().copied().collect()
        };

        let mut records = Vec::new();

        for segment_id in segment_ids {
            let segment = Segment::open(
                factory,
                &self.config.dir,
                segment_id,
                self.config.segment_size,
            )
            .await?;

            let mut offset = SEGMENT_HEADER_SIZE;
            while offset < segment.write_offset() {
                match segment.read_at(offset).await {
                    Ok(record) => {
                        if record.lsn >= from_lsn {
                            records.push(record.clone());
                        }
                        offset += record.encoded_size() as u64;
                    }
                    Err(_) => break,
                }
            }
        }

        // Sort by LSN
        records.sort_by_key(|r| r.lsn);
        Ok(records)
    }

    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }

    /// Close the WAL manager
    pub async fn close(&self) -> WalResult<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.sync().await?;
        *self.active_segment.lock().await = None;
        Ok(())
    }

    /// Get the WAL directory path
    pub fn dir(&self) -> &Path {
        &self.config.dir
    }

    /// Truncate WAL up to (and including) the given LSN
    /// Used after checkpointing to reclaim space
    pub fn truncate_to(&self, _up_to_lsn: u64) -> WalResult<()> {
        // For now, just a stub - full implementation would delete old segments
        Ok(())
    }
}
