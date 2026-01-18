//! Leveled compaction for LSM-Tree
//!
//! Compaction merges SSTables to:
//! 1. Reduce read amplification (fewer files to search)
//! 2. Remove tombstones and superseded values
//! 3. Maintain level size invariants
//!
//! Level sizing: L0 has 4 files max, L1+ have 10x size ratio

use crate::io::scheduler::IoPriority;
use crate::io::{AsyncIO, AsyncIOFactory};
use crate::storage::error::StorageResult;
use crate::storage::lsm::manifest::{Manifest, SstableInfo};
use crate::storage::lsm::sstable::{SstableReader, SstableWriter};

/// Maximum files in L0 before compaction triggers
pub const L0_COMPACTION_TRIGGER: usize = 4;

/// Size ratio between levels (10x)
pub const LEVEL_SIZE_RATIO: u64 = 10;

/// Base size for L1 (10MB)
pub const L1_BASE_SIZE: u64 = 10 * 1024 * 1024;

/// Check if compaction is needed for L0
pub fn needs_l0_compaction(manifest: &Manifest) -> bool {
    manifest.level_count(0) >= L0_COMPACTION_TRIGGER
}

/// Check if compaction is needed for a level
pub fn needs_level_compaction(manifest: &Manifest, level: u32) -> bool {
    if level == 0 {
        return needs_l0_compaction(manifest);
    }

    let max_size = L1_BASE_SIZE * LEVEL_SIZE_RATIO.pow(level - 1);
    manifest.level_size(level) > max_size
}

/// Find which level needs compaction most urgently
pub fn pick_compaction_level(manifest: &Manifest) -> Option<u32> {
    // L0 has priority
    if needs_l0_compaction(manifest) {
        return Some(0);
    }

    // Check other levels
    (1..4).find(|&level| needs_level_compaction(manifest, level))
}

/// Information gathered for a compaction job (gathered with lock held)
pub struct L0CompactionJob {
    /// L0 files to compact
    pub l0_files: Vec<SstableInfo>,
    /// L1 files that overlap with L0
    pub l1_files: Vec<SstableInfo>,
    /// Reserved name for output SSTable
    pub output_name: String,
    /// Path for output SSTable
    pub output_path: std::path::PathBuf,
    /// Paths to input files (L0 reversed + L1)
    pub input_paths: Vec<std::path::PathBuf>,
}

/// Phase 1: Gather compaction job info with manifest lock held
///
/// Returns None if no compaction is needed.
pub fn prepare_l0_compaction(manifest: &mut Manifest) -> Option<L0CompactionJob> {
    if !needs_l0_compaction(manifest) {
        return None;
    }

    let l0_files = manifest.get_level(0).to_vec();
    if l0_files.is_empty() {
        return None;
    }

    // Find L1 files that overlap with L0 key range
    let (min_key, max_key) = find_key_range(&l0_files);
    let l1_files: Vec<_> = manifest
        .get_level(1)
        .iter()
        .filter(|f| ranges_overlap(&f.min_key, &f.max_key, &min_key, &max_key))
        .cloned()
        .collect();

    // Reserve output SSTable name
    let output_name = manifest.next_sstable_name();
    let output_path = manifest.sstable_path(&output_name);

    // Collect paths for reading (L0 files reversed so newest come first)
    let input_paths: Vec<_> = l0_files
        .iter()
        .rev()
        .chain(l1_files.iter())
        .map(|info| manifest.sstable_path(&info.name))
        .collect();

    Some(L0CompactionJob {
        l0_files,
        l1_files,
        output_name,
        output_path,
        input_paths,
    })
}

/// Phase 2: Execute compaction I/O (without manifest lock)
///
/// Returns the new SSTable info on success, or None if all entries were tombstones.
pub async fn execute_l0_compaction<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    job: &L0CompactionJob,
) -> StorageResult<Option<SstableInfo>> {
    // Load all entries from all files
    let mut all_entries: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = Vec::new();

    for (idx, path) in job.input_paths.iter().enumerate() {
        let reader = SstableReader::open(factory, path, IoPriority::Compaction).await?;
        let entries = reader.scan().await?;
        for (key, value) in entries {
            all_entries.push((key, value, idx));
        }
    }

    // Sort by key, then by source index (lower = newer)
    all_entries.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.2.cmp(&b.2),
        other => other,
    });

    // Deduplicate: keep only the first (newest) entry for each key
    let mut deduped: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;

    for (key, value, _idx) in all_entries {
        if last_key.as_ref() != Some(&key) {
            // Skip tombstones during compaction (they're cleaned up)
            if value.is_some() {
                deduped.push((key.clone(), value));
            }
            last_key = Some(key);
        }
    }

    // If all entries were tombstones, don't create an empty SSTable
    if deduped.is_empty() {
        return Ok(None);
    }

    // Write to new SSTable
    let mut writer =
        SstableWriter::create(factory, &job.output_path, IoPriority::Compaction).await?;

    for (key, value) in &deduped {
        writer.add(key.clone(), value.clone()).await?;
    }

    writer.finish().await?;

    // Get file metadata - safe to unwrap since deduped is non-empty
    let size = std::fs::metadata(&job.output_path)?.len();
    let min_key = deduped.first().unwrap().0.clone();
    let max_key = deduped.last().unwrap().0.clone();

    Ok(Some(SstableInfo {
        name: job.output_name.clone(),
        min_key,
        max_key,
        size,
    }))
}

/// Phase 3: Finalize compaction (with manifest lock)
///
/// Updates manifest and deletes old files. If new_file is None (all entries
/// were tombstones), only cleans up old files without adding a new one.
pub fn finalize_l0_compaction(
    manifest: &mut Manifest,
    job: &L0CompactionJob,
    new_file: Option<SstableInfo>,
) -> StorageResult<()> {
    // Remove old files from manifest and delete them
    for f in &job.l0_files {
        manifest.remove_sstable(0, &f.name);
        let path = manifest.sstable_path(&f.name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(path = ?path, error = %e, "Failed to delete old SSTable during L0 compaction");
        }
    }
    for f in &job.l1_files {
        manifest.remove_sstable(1, &f.name);
        let path = manifest.sstable_path(&f.name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(path = ?path, error = %e, "Failed to delete old SSTable during L0 compaction");
        }
    }

    // Only add new file if there were non-tombstone entries
    if let Some(file) = new_file {
        manifest.add_sstable(1, file);
    }
    manifest.save()?;
    Ok(())
}

/// Find overall key range from a set of files
fn find_key_range(files: &[SstableInfo]) -> (Vec<u8>, Vec<u8>) {
    let min = files
        .iter()
        .map(|f| &f.min_key)
        .min()
        .cloned()
        .unwrap_or_default();
    let max = files
        .iter()
        .map(|f| &f.max_key)
        .max()
        .cloned()
        .unwrap_or_default();
    (min, max)
}

/// Check if two key ranges overlap
fn ranges_overlap(min1: &[u8], max1: &[u8], min2: &[u8], max2: &[u8]) -> bool {
    min1 <= max2 && min2 <= max1
}
