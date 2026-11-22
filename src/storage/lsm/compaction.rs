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
//!
//! Compaction is also what removes the keys of forgotten ranges (see
//! `Manifest::forget`) from tables that hold other keys as well: every merge
//! leaves them out, and a table that may hold some is compacted for that
//! reason alone, whatever its level's size.

use std::path::PathBuf;

use crate::io::{IoEngine, Urgency};
use crate::storage::error::StorageResult;
use crate::storage::lsm::manifest::{is_forgotten, ForgottenRange, Manifest, SstableInfo};
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

    /// Whether the compaction task has anything to do: a level is over its
    /// limit, or forgotten ranges are listed (their tables are to be removed
    /// or rewritten, and the ranges retired once that is done).
    pub fn work_due(&self, manifest: &Manifest) -> bool {
        !manifest.forgotten().is_empty()
            || (0..LAST_LEVEL).any(|level| self.needs_compaction(manifest, level))
    }
}

/// One compaction: inputs from two adjacent levels, one output. A job that
/// only rewrites a table to rid it of forgotten keys has one input, and its
/// output stays in the same level.
#[derive(Debug)]
pub struct CompactionJob {
    pub source_level: u32,
    /// The level below the source; or the source level itself when a table
    /// is rewritten in place.
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
    /// The forgotten ranges listed when the job was picked. Their keys are
    /// left out of the output.
    pub forgotten: Vec<ForgottenRange>,
    /// The number of the newest forgotten range at that moment: what the
    /// output records as [`SstableInfo::filtered_seq`].
    pub filtered_seq: u64,
}

impl CompactionJob {
    /// Names of every input file.
    pub fn input_names(&self) -> impl Iterator<Item = &str> + Clone {
        self.source_files
            .iter()
            .chain(&self.target_files)
            .map(|f| f.name.as_str())
    }
}

/// Choose the next compaction, if any level is due. Level 0 goes first: its
/// files are the ones every read has to search.
///
/// When no level is due, a table that may hold keys of a forgotten range is
/// chosen instead, so that those keys do not stay on disk until the table
/// happens to be compacted for its size. (Tables that hold nothing else are
/// not compacted but simply removed; the engine does that before it asks.)
pub fn pick_compaction(
    manifest: &mut Manifest,
    limits: &CompactionLimits,
) -> Option<CompactionJob> {
    if let Some(source_level) =
        (0..LAST_LEVEL).find(|&level| limits.needs_compaction(manifest, level))
    {
        let source_files: Vec<SstableInfo> = if source_level == 0 {
            level_zero_newest_first(manifest)
        } else {
            // The largest file: the most relief for the level.
            vec![manifest
                .get_level(source_level)
                .iter()
                .max_by_key(|f| f.size)?
                .clone()]
        };
        return build_job(manifest, source_level, source_level + 1, source_files);
    }

    let (level, table) = manifest.levels().iter().find_map(|level| {
        let table = level.files.iter().find(|f| manifest.holds_forgotten(f))?;
        Some((level.level, table.clone()))
    })?;
    if level == 0 {
        // A level-0 table cannot be taken out on its own: the others may
        // hold older values of its keys, and what leaves level 0 counts as
        // older than what stays. All of level 0 goes down together.
        let source_files = level_zero_newest_first(manifest);
        build_job(manifest, 0, 1, source_files)
    } else {
        // Deeper tables do not overlap their level's other tables, and what
        // comes out covers no more than what went in: rewrite it where it is.
        build_job(manifest, level, level, vec![table])
    }
}

/// All of level 0, newest first (files are appended as they are flushed).
fn level_zero_newest_first(manifest: &Manifest) -> Vec<SstableInfo> {
    manifest.get_level(0).iter().rev().cloned().collect()
}

/// A job that merges `source_files` of `source_level` with the files they
/// overlap in `target_level`; or, when the two levels are the same, rewrites
/// them on their own.
fn build_job(
    manifest: &mut Manifest,
    source_level: u32,
    target_level: u32,
    source_files: Vec<SstableInfo>,
) -> Option<CompactionJob> {
    if source_files.is_empty() {
        return None;
    }

    let (min_key, max_key) = find_key_range(&source_files);
    let overlaps = |f: &SstableInfo| ranges_overlap(&f.min_key, &f.max_key, &min_key, &max_key);
    let target_files: Vec<SstableInfo> = if target_level == source_level {
        Vec::new()
    } else {
        manifest
            .get_level(target_level)
            .iter()
            .filter(|f| overlaps(f))
            .cloned()
            .collect()
    };

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
        forgotten: manifest.forgotten().to_vec(),
        filtered_seq: manifest.forgotten_seq(),
    })
}

/// About how much of the inputs a compaction holds in memory at a time.
const COMPACTION_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The next stretch of the merge of `tables` (newest data first): entries
/// with keys greater than `after` (from the start if `None`), in key order,
/// about `max_bytes` of them, deleted keys included as `None`. Where several
/// tables hold a key, the first of them decides. The flag says whether
/// these were the last.
///
/// Keys in one of the `forgotten` ranges are left out, as if no table held
/// them.
pub(crate) async fn merged_chunk(
    tables: &[std::sync::Arc<SstableReader>],
    after: Option<&[u8]>,
    max_bytes: usize,
    forgotten: &[ForgottenRange],
) -> StorageResult<(Vec<(Vec<u8>, Option<Vec<u8>>)>, bool)> {
    if forgotten.is_empty() {
        return merged_stretch(tables, after, max_bytes).await;
    }
    let mut after = after.map(<[u8]>::to_vec);
    loop {
        let (mut entries, done) = merged_stretch(tables, after.as_deref(), max_bytes).await?;
        let last = entries.last().map(|(key, _)| key.clone());
        entries.retain(|(key, _)| !is_forgotten(forgotten, key));
        if !entries.is_empty() || done {
            return Ok((entries, done));
        }
        // Everything in this stretch was forgotten: go on from its end.
        match last {
            Some(last) => after = Some(last),
            None => return Ok((entries, true)),
        }
    }
}

/// [`merged_chunk`] before forgotten keys are left out.
///
/// Only the tables the stretch can come from are read. Tables are mostly
/// non-overlapping and in key order; reading every table for every stretch
/// would read the whole of them over and over.
async fn merged_stretch(
    tables: &[std::sync::Arc<SstableReader>],
    after: Option<&[u8]>,
    max_bytes: usize,
) -> StorageResult<(Vec<(Vec<u8>, Option<Vec<u8>>)>, bool)> {
    // Tables are read only once the position has reached where they start;
    // `reach` extends that to a table that starts further on when nothing
    // nearer is left.
    let mut reach: Option<Vec<u8>> = None;
    loop {
        // Tables with anything beyond the position.
        let remaining: Vec<(usize, &std::sync::Arc<SstableReader>)> = tables
            .iter()
            .enumerate()
            .filter(|(_, table)| {
                after.is_none_or(|after| table.metadata().max_key.as_slice() > after)
            })
            .collect();
        if remaining.is_empty() {
            return Ok((Vec::new(), true));
        }
        let started = |table: &SstableReader| {
            let min_key = table.metadata().min_key.as_slice();
            after.is_some_and(|after| min_key <= after)
                || reach.as_deref().is_some_and(|reach| min_key <= reach)
        };
        let next_start: Option<Vec<u8>> = remaining
            .iter()
            .filter(|(_, table)| !started(table))
            .map(|(_, table)| table.metadata().min_key.clone())
            .min();
        let chosen: Vec<(usize, &std::sync::Arc<SstableReader>)> = remaining
            .iter()
            .filter(|(_, table)| started(table))
            .copied()
            .collect();
        if chosen.is_empty() {
            reach = next_start;
            continue;
        }

        // Each chosen table gives its next stretch. A table that stopped
        // short of its end may hold keys beyond its last one that the
        // others have already passed, so the merge is only good up to the
        // earliest such stopping point; and only short of where the nearest
        // unread table starts.
        let mut merger = MergeIterator::new();
        let mut bound: Option<Vec<u8>> = None;
        for (priority, table) in &chosen {
            let (entries, ended) = table.scan_after(after, max_bytes).await?;
            if !ended {
                if let Some((last, _)) = entries.last() {
                    if bound.as_ref().is_none_or(|bound| last < bound) {
                        bound = Some(last.clone());
                    }
                }
            }
            merger.add(entries, *priority);
        }
        let mut merged = merger.merge();
        if let Some(bound) = &bound {
            merged.retain(|(key, _)| key <= bound);
        }
        if let Some(next_start) = &next_start {
            merged.retain(|(key, _)| key < next_start);
        }
        if !merged.is_empty() {
            let done = bound.is_none() && next_start.is_none();
            return Ok((merged, done));
        }
        // Nothing before the nearest unread table: read that one too.
        match next_start {
            Some(next_start) => reach = Some(next_start),
            None => return Ok((Vec::new(), true)),
        }
    }
}

/// Merge the inputs and write the output table, leaving out the keys of the
/// job's forgotten ranges. Returns `None` if nothing is left to write (every
/// entry was forgotten, or a tombstone with nothing to hide).
///
/// The inputs are read and the output written a stretch at a time: what a
/// compaction holds in memory does not grow with the size of its tables.
pub async fn execute_compaction(
    engine: &IoEngine,
    job: &CompactionJob,
) -> StorageResult<Option<SstableInfo>> {
    let mut inputs = Vec::with_capacity(job.input_paths.len());
    for path in &job.input_paths {
        let reader = SstableReader::open(engine, path, Urgency::Background).await?;
        inputs.push(std::sync::Arc::new(reader));
    }

    let mut writer: Option<SstableWriter> = None;
    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Vec<u8> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (entries, done) = merged_chunk(
            &inputs,
            after.as_deref(),
            COMPACTION_CHUNK_BYTES,
            &job.forgotten,
        )
        .await?;
        if let Some((last, _)) = entries.last() {
            after = Some(last.clone());
        }
        for (key, value) in entries {
            if job.drop_tombstones && value.is_none() {
                continue;
            }
            if writer.is_none() {
                writer = Some(
                    SstableWriter::create(engine, &job.output_path, Urgency::Background).await?,
                );
                min_key = Some(key.clone());
            }
            max_key.clone_from(&key);
            writer
                .as_mut()
                .expect("created above")
                .add(key, value)
                .await?;
        }
        if done {
            break;
        }
    }
    let (Some(writer), Some(min_key)) = (writer, min_key) else {
        return Ok(None);
    };
    writer.finish().await?;

    let size = std::fs::metadata(&job.output_path)?.len();
    Ok(Some(SstableInfo {
        name: job.output_name.clone(),
        min_key,
        max_key,
        size,
        filtered_seq: job.filtered_seq,
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
            filtered_seq: 0,
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
    fn a_table_that_may_hold_forgotten_keys_is_picked_without_a_size_trigger() {
        let limits = CompactionLimits::default();

        // In a deeper level - the last one included - the table is
        // rewritten where it is, alone.
        for level in [1, 3, LAST_LEVEL] {
            let (_dir, mut m) = manifest();
            m.add_sstable(level, info("aside.sst", "a", "c", 100));
            m.add_sstable(level, info("part.sst", "k", "n", 100));
            m.add_sstable(level, info("other.sst", "q", "z", 100));
            assert!(pick_compaction(&mut m, &limits).is_none());
            assert!(!limits.work_due(&m));

            m.forget(b"m", b"p");
            assert!(limits.work_due(&m));
            let job = pick_compaction(&mut m, &limits).unwrap();
            assert_eq!((job.source_level, job.target_level), (level, level));
            assert_eq!(job.input_names().collect::<Vec<_>>(), ["part.sst"]);
            assert_eq!(job.forgotten, m.forgotten());
            assert_eq!(job.filtered_seq, 1);
            assert!(job.drop_tombstones);

            // The output is known to be rid of the range: nothing more to
            // pick, though it still spans the range.
            let mut out = info("out.sst", "k", "q", 90);
            out.filtered_seq = job.filtered_seq;
            apply_compaction(&mut m, &job, Some(out));
            assert_eq!(m.level_count(level), 3);
            assert!(pick_compaction(&mut m, &limits).is_none());
        }

        // Tombstones in a rewritten table go on hiding what lies deeper.
        let (_dir, mut m) = manifest();
        m.add_sstable(2, info("part.sst", "k", "n", 100));
        m.add_sstable(4, info("deep.sst", "j", "l", 100));
        m.forget(b"m", b"p");
        assert!(!pick_compaction(&mut m, &limits).unwrap().drop_tombstones);

        // In level 0 the whole level goes down, newest first.
        let (_dir, mut m) = manifest();
        m.add_sstable(0, info("old.sst", "a", "n", 100));
        m.add_sstable(0, info("new.sst", "a", "c", 100));
        m.add_sstable(1, info("l1.sst", "b", "d", 100));
        m.forget(b"m", b"p");
        let job = pick_compaction(&mut m, &limits).unwrap();
        assert_eq!((job.source_level, job.target_level), (0, 1));
        let names: Vec<&str> = job.input_names().collect();
        assert_eq!(names, ["new.sst", "old.sst", "l1.sst"]);
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
