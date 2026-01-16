//! Leveled compaction for LSM-Tree
//!
//! Compaction merges SSTables to:
//! 1. Reduce read amplification (fewer files to search)
//! 2. Remove tombstones and superseded values
//! 3. Maintain level size invariants
//!
//! Level sizing: L0 has 4 files max, L1+ have 10x size ratio

use std::cmp::Ordering;

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

/// Compact L0 into L1
///
/// This merges all L0 files with overlapping L1 files into new L1 files.
pub async fn compact_l0<IO: AsyncIO, F: AsyncIOFactory<IO = IO>>(
    factory: &F,
    manifest: &mut Manifest,
) -> StorageResult<()> {
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

    // Collect all files to merge
    // L0 files are reversed so newest (highest numbered) come first
    // This ensures newer versions win during merge deduplication
    let mut all_files: Vec<_> = l0_files.iter().rev().cloned().collect();
    all_files.extend(l1_files.clone());

    // Merge into new L1 file(s)
    let new_files = merge_sstables(factory, manifest, &all_files).await?;

    // Update manifest: remove old files, add new ones
    for f in &l0_files {
        manifest.remove_sstable(0, &f.name);
        // Delete old file
        let _ = std::fs::remove_file(manifest.sstable_path(&f.name));
    }
    for f in &l1_files {
        manifest.remove_sstable(1, &f.name);
        let _ = std::fs::remove_file(manifest.sstable_path(&f.name));
    }
    for f in new_files {
        manifest.add_sstable(1, f);
    }

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
    let _ = std::fs::remove_file(manifest.sstable_path(&source.name));

    for f in &target_files {
        manifest.remove_sstable(level + 1, &f.name);
        let _ = std::fs::remove_file(manifest.sstable_path(&f.name));
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
        let reader = SstableReader::open(factory, &path).await?;
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
    let mut writer = SstableWriter::create(factory, &new_path).await?;

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
