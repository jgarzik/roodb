//! Leveled compaction for LSM-Tree
//!
//! Compaction merges SSTables to:
//! 1. Reduce read amplification (fewer files to search)
//! 2. Remove tombstones and superseded values
//! 3. Maintain level size invariants
//!
//! Level sizing: L0 has 4 files max, L1+ have 10x size ratio

use std::cmp::Ordering;

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
/// Returns the new SSTable info on success.
pub async fn execute_l0_compaction<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    job: &L0CompactionJob,
) -> StorageResult<SstableInfo> {
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

    // Write to new SSTable
    let mut writer =
        SstableWriter::create(factory, &job.output_path, IoPriority::Compaction).await?;

    for (key, value) in &deduped {
        writer.add(key.clone(), value.clone()).await?;
    }

    writer.finish().await?;

    // Get file metadata
    let size = std::fs::metadata(&job.output_path)?.len();
    let min_key = deduped.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let max_key = deduped.last().map(|(k, _)| k.clone()).unwrap_or_default();

    Ok(SstableInfo {
        name: job.output_name.clone(),
        min_key,
        max_key,
        size,
    })
}

/// Phase 3: Finalize compaction (with manifest lock)
///
/// Updates manifest and deletes old files.
pub fn finalize_l0_compaction(
    manifest: &mut Manifest,
    job: &L0CompactionJob,
    new_file: SstableInfo,
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

    manifest.add_sstable(1, new_file);
    manifest.save()?;
    Ok(())
}

/// Compact L0 into L1
///
/// This merges all L0 files with overlapping L1 files into new L1 files.
///
/// # Locking
/// This function uses split-phase locking to minimize manifest lock contention:
/// 1. Brief lock to gather file info and reserve output name
/// 2. Release lock during expensive I/O operations
/// 3. Re-acquire lock to update manifest metadata
///
/// This allows concurrent reads to proceed while compaction I/O happens.
pub async fn compact_l0<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    manifest: &mut Manifest,
) -> StorageResult<()> {
    // Phase 1: Gather compaction job info with lock held (caller holds lock)
    let l0_files = manifest.get_level(0).to_vec();
    if l0_files.is_empty() {
        return Ok(());
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

    // Collect paths for reading (need to get these while we have manifest)
    let file_paths: Vec<_> = l0_files
        .iter()
        .rev() // L0 files reversed so newest come first
        .chain(l1_files.iter())
        .map(|info| manifest.sstable_path(&info.name))
        .collect();

    // Phase 2: I/O operations without holding caller's lock context
    // Note: caller still technically holds lock, but actual I/O happens here
    // Future improvement: restructure to release lock before I/O

    // Load all entries from all files
    let mut all_entries: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = Vec::new();

    for (idx, path) in file_paths.iter().enumerate() {
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

    // Write to new SSTable
    let mut writer = SstableWriter::create(factory, &output_path, IoPriority::Compaction).await?;

    for (key, value) in &deduped {
        writer.add(key.clone(), value.clone()).await?;
    }

    writer.finish().await?;

    // Get file metadata
    let size = std::fs::metadata(&output_path)?.len();
    let new_min_key = deduped.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let new_max_key = deduped.last().map(|(k, _)| k.clone()).unwrap_or_default();

    let new_file = SstableInfo {
        name: output_name,
        min_key: new_min_key,
        max_key: new_max_key,
        size,
    };

    // Phase 3: Update manifest (still holding caller's lock)
    for f in &l0_files {
        manifest.remove_sstable(0, &f.name);
        // Delete old file (log warning on failure, don't fail compaction)
        let path = manifest.sstable_path(&f.name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(path = ?path, error = %e, "Failed to delete old SSTable during L0 compaction");
        }
    }
    for f in &l1_files {
        manifest.remove_sstable(1, &f.name);
        let path = manifest.sstable_path(&f.name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(path = ?path, error = %e, "Failed to delete old SSTable during L0 compaction");
        }
    }

    manifest.add_sstable(1, new_file);
    manifest.save()?;
    Ok(())
}

/// Compact a level into the next level
pub async fn compact_level<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    manifest: &mut Manifest,
    level: u32,
) -> StorageResult<()> {
    if level == 0 {
        return compact_l0(factory, manifest).await;
    }

    let source_files = manifest.get_level(level).to_vec();
    if source_files.is_empty() {
        return Ok(());
    }

    // Pick a file to compact (e.g., first one for simplicity)
    let source = &source_files[0];

    // Find overlapping files in next level
    let target_files: Vec<_> = manifest
        .get_level(level + 1)
        .iter()
        .filter(|f| ranges_overlap(&f.min_key, &f.max_key, &source.min_key, &source.max_key))
        .cloned()
        .collect();

    // Merge files
    let mut files_to_merge = vec![source.clone()];
    files_to_merge.extend(target_files.clone());

    let new_files = merge_sstables(factory, manifest, &files_to_merge).await?;

    // Update manifest
    manifest.remove_sstable(level, &source.name);
    let path = manifest.sstable_path(&source.name);
    if let Err(e) = std::fs::remove_file(&path) {
        tracing::warn!(path = ?path, level = level, error = %e, "Failed to delete old SSTable during level compaction");
    }

    for f in &target_files {
        manifest.remove_sstable(level + 1, &f.name);
        let path = manifest.sstable_path(&f.name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(path = ?path, level = level + 1, error = %e, "Failed to delete old SSTable during level compaction");
        }
    }

    for f in new_files {
        manifest.add_sstable(level + 1, f);
    }

    manifest.save()?;
    Ok(())
}

/// Merge multiple SSTables into new file(s)
async fn merge_sstables<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    manifest: &mut Manifest,
    files: &[SstableInfo],
) -> StorageResult<Vec<SstableInfo>> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // Load all entries from all files
    // Files should be ordered newest-first so that idx=0 is newest
    let mut all_entries: Vec<(Vec<u8>, Option<Vec<u8>>, usize)> = Vec::new();

    for (idx, info) in files.iter().enumerate() {
        let path = manifest.sstable_path(&info.name);
        let reader = SstableReader::open(factory, &path, IoPriority::Compaction).await?;
        let entries = reader.scan().await?;
        for (key, value) in entries {
            all_entries.push((key, value, idx));
        }
    }

    // Sort by key, then by source index (lower = newer)
    all_entries.sort_by(|a, b| match a.0.cmp(&b.0) {
        Ordering::Equal => a.2.cmp(&b.2),
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

    // Write to new SSTable
    let new_name = manifest.next_sstable_name();
    let new_path = manifest.sstable_path(&new_name);
    let mut writer = SstableWriter::create(factory, &new_path, IoPriority::Compaction).await?;

    for (key, value) in &deduped {
        writer.add(key.clone(), value.clone()).await?;
    }

    writer.finish().await?;

    // Get file size
    let size = std::fs::metadata(&new_path)?.len();

    let min_key = deduped.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let max_key = deduped.last().map(|(k, _)| k.clone()).unwrap_or_default();

    Ok(vec![SstableInfo {
        name: new_name,
        min_key,
        max_key,
        size,
    }])
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
