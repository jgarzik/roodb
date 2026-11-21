//! Leveled compaction for the LSM tree.
//!
//! Compaction merges tables to keep reads cheap (fewer files to search), to
//! discard superseded values, and to keep each level within its size.
//!
//! Level 0 holds whole flushed memtables, newest last; its files overlap.
//! From level 1 down, the files of a level cover disjoint key ranges and each
//! level may be `LEVEL_SIZE_RATIO` times the size of the one above.
//!
//! A job merges some files of one level with the files they overlap in the
//! next, and replaces all of them with one output file in the lower level.
//!
//! Deletion markers (tombstones) are carried down with the data. One is
//! dropped only when nothing below the output could still hold an older value
//! of that key; dropping it earlier would bring the deleted value back.

use std::path::PathBuf;

use crate::io::{IoEngine, Urgency};
use crate::storage::error::StorageResult;
use crate::storage::lsm::manifest::{Manifest, SstableInfo};
use crate::storage::lsm::merge_iter::MergeIterator;
use crate::storage::lsm::sstable::{SstableReader, SstableWriter};

/// Level 0 is compacted once it holds this many files.
pub const L0_COMPACTION_TRIGGER: usize = 4;

/// Size ratio between adjacent levels.
pub const LEVEL_SIZE_RATIO: u64 = 10;

/// Size at which level 1 is compacted.
pub const L1_BASE_SIZE: u64 = 10 * 1024 * 1024;

/// The deepest level. Its files are never compacted further.
pub const LAST_LEVEL: u32 = 6;

/// When compaction runs.
#[derive(Debug, Clone, Copy)]
pub struct CompactionLimits {
    /// Number of level-0 files that triggers a compaction into level 1.
    pub l0_trigger: usize,
    /// Size of level 1 that triggers a compaction into level 2. Each deeper
    /// level is allowed `LEVEL_SIZE_RATIO` times more.
    pub level_base_bytes: u64,
}

impl Default for CompactionLimits {
    fn default() -> Self {
        Self {
            l0_trigger: L0_COMPACTION_TRIGGER,
            level_base_bytes: L1_BASE_SIZE,
        }
    }
}

impl CompactionLimits {
    fn max_level_bytes(&self, level: u32) -> u64 {
        self.level_base_bytes
            .saturating_mul(LEVEL_SIZE_RATIO.saturating_pow(level.saturating_sub(1)))
    }

    /// Whether `level` is due for compaction.
    pub fn needs_compaction(&self, manifest: &Manifest, level: u32) -> bool {
        if level >= LAST_LEVEL {
            return false;
        }
        if level == 0 {
            return manifest.level_count(0) >= self.l0_trigger.max(1);
        }
        manifest.level_size(level) > self.max_level_bytes(level)
    }
}

/// One compaction: inputs from two adjacent levels, one output.
#[derive(Debug)]
pub struct CompactionJob {
    pub source_level: u32,
    pub target_level: u32,
    /// Files taken from the source level.
    pub source_files: Vec<SstableInfo>,
    /// Files of the target level that overlap them.
    pub target_files: Vec<SstableInfo>,
    pub output_name: String,
    pub output_path: PathBuf,
    /// Every input, newest data first: where two inputs hold the same key the
    /// earlier one wins.
    pub input_paths: Vec<PathBuf>,
    /// Nothing below the target level overlaps this job's key range, so
    /// tombstones have nothing left to hide and can be dropped.
    pub drop_tombstones: bool,
}

impl CompactionJob {
    /// Names of every input file.
    pub fn input_names(&self) -> impl Iterator<Item = &str> {
        self.source_files
            .iter()
            .chain(&self.target_files)
            .map(|f| f.name.as_str())
    }
}

/// Choose the next compaction, if any level is due. Level 0 goes first: its
/// files are the ones every read has to search.
pub fn pick_compaction(
    manifest: &mut Manifest,
    limits: &CompactionLimits,
) -> Option<CompactionJob> {
    let source_level = (0..LAST_LEVEL).find(|&level| limits.needs_compaction(manifest, level))?;
    let target_level = source_level + 1;

    let source_files: Vec<SstableInfo> = if source_level == 0 {
        // All of level 0, newest first (files are appended as they are flushed).
        manifest.get_level(0).iter().rev().cloned().collect()
    } else {
        // The largest file: the most relief for the level.
        vec![manifest
            .get_level(source_level)
            .iter()
            .max_by_key(|f| f.size)?
            .clone()]
    };
    if source_files.is_empty() {
        return None;
    }

    let (min_key, max_key) = find_key_range(&source_files);
    let overlaps = |f: &SstableInfo| ranges_overlap(&f.min_key, &f.max_key, &min_key, &max_key);
    let target_files: Vec<SstableInfo> = manifest
        .get_level(target_level)
        .iter()
        .filter(|f| overlaps(f))
        .cloned()
        .collect();

    // The output covers the union of the inputs' ranges. If any deeper file
    // overlaps that, it may hold an older value that a tombstone must go on
    // hiding.
    let (min_key, max_key) = find_key_range(
        &source_files
            .iter()
            .chain(&target_files)
            .cloned()
            .collect::<Vec<_>>(),
    );
    let deeper_overlap = manifest
        .levels()
        .iter()
        .filter(|level| level.level > target_level)
        .flat_map(|level| &level.files)
        .any(|f| ranges_overlap(&f.min_key, &f.max_key, &min_key, &max_key));

    let output_name = manifest.next_sstable_name();
    let output_path = manifest.sstable_path(&output_name);
    let input_paths = source_files
        .iter()
        .chain(&target_files)
        .map(|info| manifest.sstable_path(&info.name))
        .collect();

    Some(CompactionJob {
        source_level,
        target_level,
        source_files,
        target_files,
        output_name,
        output_path,
        input_paths,
        drop_tombstones: !deeper_overlap,
    })
}

/// Merge the inputs and write the output table. Returns `None` if nothing is
/// left to write (every entry was a tombstone with nothing to hide).
pub async fn execute_compaction(
    engine: &IoEngine,
    job: &CompactionJob,
) -> StorageResult<Option<SstableInfo>> {
    let mut merger = MergeIterator::new();
    for (priority, path) in job.input_paths.iter().enumerate() {
        let reader = SstableReader::open(engine, path, Urgency::Background).await?;
        merger.add(reader.scan().await?, priority);
    }

    let mut merged = merger.merge();
    if job.drop_tombstones {
        merged.retain(|(_, value)| value.is_some());
    }
    let (Some(first), Some(last)) = (merged.first(), merged.last()) else {
        return Ok(None);
    };
    let min_key = first.0.clone();
    let max_key = last.0.clone();

    let mut writer = SstableWriter::create(engine, &job.output_path, Urgency::Background).await?;
    for (key, value) in merged {
        writer.add(key, value).await?;
    }
    writer.finish().await?;

    let size = std::fs::metadata(&job.output_path)?.len();
    Ok(Some(SstableInfo {
        name: job.output_name.clone(),
        min_key,
        max_key,
        size,
    }))
}

/// Record a finished job in the manifest: the inputs go, the output arrives.
/// Memory only; the caller persists the manifest and deletes the input files
/// afterwards, in that order.
pub fn apply_compaction(manifest: &mut Manifest, job: &CompactionJob, output: Option<SstableInfo>) {
    for f in &job.source_files {
        manifest.remove_sstable(job.source_level, &f.name);
    }
    for f in &job.target_files {
        manifest.remove_sstable(job.target_level, &f.name);
    }
    if let Some(file) = output {
        manifest.add_sstable(job.target_level, file);
    }
}

/// Find min and max keys across a set of files
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

#[cfg(test)]
mod tests {
    use super::*;

    fn info(name: &str, min: &str, max: &str, size: u64) -> SstableInfo {
        SstableInfo {
            name: name.to_string(),
            min_key: min.as_bytes().to_vec(),
            max_key: max.as_bytes().to_vec(),
            size,
        }
    }

    fn manifest() -> (tempfile::TempDir, Manifest) {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::open(dir.path()).unwrap();
        (dir, manifest)
    }

    #[test]
    fn nothing_to_do_below_the_trigger() {
        let (_dir, mut m) = manifest();
        let limits = CompactionLimits::default();
        for i in 0..L0_COMPACTION_TRIGGER - 1 {
            m.add_sstable(0, info(&format!("{i}.sst"), "a", "z", 100));
        }
        assert!(pick_compaction(&mut m, &limits).is_none());
        m.add_sstable(0, info("last.sst", "a", "z", 100));
        assert!(pick_compaction(&mut m, &limits).is_some());
    }

    #[test]
    fn level_zero_inputs_are_newest_first() {
        let (_dir, mut m) = manifest();
        let limits = CompactionLimits {
            l0_trigger: 3,
            ..Default::default()
        };
        m.add_sstable(0, info("old.sst", "a", "m", 100));
        m.add_sstable(0, info("mid.sst", "c", "p", 100));
        m.add_sstable(0, info("new.sst", "b", "z", 100));
        m.add_sstable(1, info("l1-in.sst", "k", "q", 100));
        m.add_sstable(1, info("l1-out.sst", "zz", "zzz", 100));

        let job = pick_compaction(&mut m, &limits).unwrap();
        assert_eq!(job.source_level, 0);
        assert_eq!(job.target_level, 1);
        let names: Vec<&str> = job.input_names().collect();
        assert_eq!(names, ["new.sst", "mid.sst", "old.sst", "l1-in.sst"]);
        let paths: Vec<String> = job
            .input_paths
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(paths, ["new.sst", "mid.sst", "old.sst", "l1-in.sst"]);
    }

    #[test]
    fn tombstones_are_kept_while_deeper_data_overlaps() {
        let limits = CompactionLimits {
            l0_trigger: 1,
            ..Default::default()
        };

        // Nothing deeper: tombstones can go.
        let (_dir, mut m) = manifest();
        m.add_sstable(0, info("a.sst", "c", "f", 100));
        assert!(pick_compaction(&mut m, &limits).unwrap().drop_tombstones);

        // Deeper file overlapping the range: keep them.
        let (_dir, mut m) = manifest();
        m.add_sstable(0, info("a.sst", "c", "f", 100));
        m.add_sstable(3, info("deep.sst", "e", "k", 100));
        assert!(!pick_compaction(&mut m, &limits).unwrap().drop_tombstones);

        // Deeper file elsewhere in the key space: nothing to hide.
        let (_dir, mut m) = manifest();
        m.add_sstable(0, info("a.sst", "c", "f", 100));
        m.add_sstable(3, info("deep.sst", "x", "z", 100));
        assert!(pick_compaction(&mut m, &limits).unwrap().drop_tombstones);

        // The output range includes the target files pulled in, which may
        // reach further than the source files did.
        let (_dir, mut m) = manifest();
        m.add_sstable(0, info("a.sst", "c", "f", 100));
        m.add_sstable(1, info("wide.sst", "e", "p", 100));
        m.add_sstable(2, info("deep.sst", "m", "n", 100));
        assert!(!pick_compaction(&mut m, &limits).unwrap().drop_tombstones);
    }

    #[test]
    fn deeper_levels_compact_their_largest_file() {
        let (_dir, mut m) = manifest();
        let limits = CompactionLimits {
            l0_trigger: 100,
            level_base_bytes: 1000,
        };
        m.add_sstable(1, info("small.sst", "a", "c", 300));
        m.add_sstable(1, info("large.sst", "d", "h", 900));
        m.add_sstable(2, info("under.sst", "e", "f", 500));
        m.add_sstable(2, info("aside.sst", "x", "z", 500));

        let job = pick_compaction(&mut m, &limits).unwrap();
        assert_eq!((job.source_level, job.target_level), (1, 2));
        let names: Vec<&str> = job.input_names().collect();
        assert_eq!(names, ["large.sst", "under.sst"]);

        apply_compaction(&mut m, &job, Some(info("out.sst", "d", "h", 1200)));
        let l1: Vec<&str> = m.get_level(1).iter().map(|f| f.name.as_str()).collect();
        let l2: Vec<&str> = m.get_level(2).iter().map(|f| f.name.as_str()).collect();
        assert_eq!(l1, ["small.sst"]);
        assert_eq!(l2, ["aside.sst", "out.sst"]);
    }

    #[test]
    fn the_last_level_is_never_a_source() {
        let (_dir, mut m) = manifest();
        let limits = CompactionLimits {
            l0_trigger: 100,
            level_base_bytes: 1,
        };
        m.add_sstable(LAST_LEVEL, info("bottom.sst", "a", "z", u64::MAX / 2));
        assert!(pick_compaction(&mut m, &limits).is_none());
    }
}
