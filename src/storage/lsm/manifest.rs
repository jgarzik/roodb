//! Manifest file for tracking SSTable metadata
//!
//! The manifest tracks which SSTables exist at each level and their key ranges,
//! and which key ranges the engine has been told to forget and has not yet
//! finished removing. It is stored as JSON for simplicity and human
//! readability.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::io::{AlignedBuffer, IoEngine, OpenOptions, Urgency};
use crate::storage::error::{StorageError, StorageResult};

/// Manifest file name
pub const MANIFEST_FILE: &str = "MANIFEST";

/// Version of the on-disk layout this build reads and writes: the manifest
/// itself and the table format it points at. A directory written by a
/// different version is refused rather than misread.
pub const MANIFEST_VERSION: u32 = 3;

/// SSTable file info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstableInfo {
    /// File name (e.g., "000001.sst")
    pub name: String,
    /// Minimum key in the SSTable
    pub min_key: Vec<u8>,
    /// Maximum key in the SSTable
    pub max_key: Vec<u8>,
    /// File size in bytes
    pub size: u64,
    /// The table holds no key of any forgotten range numbered up to this:
    /// those known when it was written were left out of it. A range with a
    /// higher number that overlaps the table may still have keys in it.
    #[serde(default)]
    pub filtered_seq: u64,
}

/// A key range `[start, end)` the engine has been told to forget: its keys
/// are hidden from every read and left out of every table written from now
/// on. It stays listed until no memtable and no table can still hold one of
/// its keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgottenRange {
    /// First key of the range.
    pub start: Vec<u8>,
    /// The key just past the range.
    pub end: Vec<u8>,
    /// Ranges are numbered in the order they arrive; see
    /// [`SstableInfo::filtered_seq`].
    pub seq: u64,
}

impl ForgottenRange {
    /// Whether `key` is in the range.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    /// Whether a table whose keys run from `min_key` to `max_key` (both
    /// included) could hold a key of the range.
    pub fn overlaps(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        min_key < self.end.as_slice() && max_key >= self.start.as_slice()
    }

    /// Whether every key from `min_key` to `max_key` is in the range.
    pub fn covers(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        min_key >= self.start.as_slice() && max_key < self.end.as_slice()
    }

    /// Whether `table` may still hold keys of the range.
    pub fn in_table(&self, table: &SstableInfo) -> bool {
        table.filtered_seq < self.seq && self.overlaps(&table.min_key, &table.max_key)
    }
}

/// The range of `ranges` (sorted and disjoint, as the manifest keeps them)
/// that holds `key`, if any.
pub fn forgotten_range<'a>(ranges: &'a [ForgottenRange], key: &[u8]) -> Option<&'a ForgottenRange> {
    let after = ranges.partition_point(|range| range.start.as_slice() <= key);
    ranges[..after].last().filter(|range| range.contains(key))
}

/// Whether `key` is in one of `ranges`.
pub fn is_forgotten(ranges: &[ForgottenRange], key: &[u8]) -> bool {
    forgotten_range(ranges, key).is_some()
}

/// Level info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelInfo {
    /// Level number (0, 1, 2, ...)
    pub level: u32,
    /// SSTables in this level
    pub files: Vec<SstableInfo>,
}

/// Manifest data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestData {
    /// On-disk layout version; see [`MANIFEST_VERSION`].
    #[serde(default)]
    pub version: u32,
    /// Sequence number for generating SSTable names
    pub sequence: u64,
    /// Levels
    pub levels: Vec<LevelInfo>,
    /// Key ranges being forgotten, sorted by start, no two overlapping.
    /// (Like the fields below it, absent from older layouts: defaulted so
    /// that such a manifest is refused for its version, not as unreadable.)
    #[serde(default)]
    pub forgotten: Vec<ForgottenRange>,
    /// Number given to the newest forgotten range. Never goes back, also
    /// when ranges are retired.
    #[serde(default)]
    pub forgotten_seq: u64,
}

impl Default for ManifestData {
    fn default() -> Self {
        Self {
            version: MANIFEST_VERSION,
            sequence: 0,
            levels: vec![
                LevelInfo {
                    level: 0,
                    files: vec![],
                },
                LevelInfo {
                    level: 1,
                    files: vec![],
                },
                LevelInfo {
                    level: 2,
                    files: vec![],
                },
                LevelInfo {
                    level: 3,
                    files: vec![],
                },
            ],
            forgotten: vec![],
            forgotten_seq: 0,
        }
    }
}

/// Manifest manager
pub struct Manifest {
    dir: PathBuf,
    data: ManifestData,
}

impl Manifest {
    /// Create or load manifest
    pub fn open(dir: &Path) -> StorageResult<Self> {
        let manifest_path = dir.join(MANIFEST_FILE);

        let data: ManifestData = if manifest_path.try_exists()? {
            let content = std::fs::read_to_string(&manifest_path)?;
            serde_json::from_str(&content)
                .map_err(|e| StorageError::Manifest(format!("failed to parse manifest: {}", e)))?
        } else {
            // No manifest means a new directory. If tables are lying here
            // anyway, the manifest was lost: treating the directory as new
            // would delete them as leftovers.
            let has_tables = match std::fs::read_dir(dir) {
                Ok(entries) => entries
                    .filter_map(Result::ok)
                    .any(|e| e.file_name().to_string_lossy().ends_with(".sst")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => return Err(e.into()),
            };
            if has_tables {
                return Err(StorageError::Manifest(format!(
                    "{} holds table files but no {MANIFEST_FILE}; refusing to treat it as empty",
                    dir.display()
                )));
            }
            ManifestData::default()
        };
        if data.version != MANIFEST_VERSION {
            return Err(StorageError::Manifest(format!(
                "data directory {} uses on-disk format {}, this build uses format {}; \
                 remove the directory and run roodb_init again",
                dir.display(),
                data.version,
                MANIFEST_VERSION
            )));
        }

        let manifest = Self {
            dir: dir.to_path_buf(),
            data,
        };
        manifest.remove_leftovers()?;
        Ok(manifest)
    }

    /// Delete what an interrupted run left behind: a half-written manifest,
    /// and table files the manifest does not list (a flush or compaction that
    /// wrote its output but did not get as far as recording it). Without this
    /// a later table given the same name would land on top of the old file.
    fn remove_leftovers(&self) -> StorageResult<()> {
        let listed: std::collections::HashSet<&str> = self
            .data
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|f| f.name.as_str()))
            .collect();
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let manifest_tmp = format!("{MANIFEST_FILE}.tmp");
        for entry in entries {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let leftover =
                name == manifest_tmp || (name.ends_with(".sst") && !listed.contains(name.as_str()));
            if leftover {
                tracing::warn!(file = %name, "removing file left by an interrupted run");
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    /// The table lists and the forgotten ranges, to put back with
    /// [`Self::restore_state`] if a change cannot be made durable.
    pub fn state_snapshot(&self) -> (Vec<LevelInfo>, Vec<ForgottenRange>) {
        (self.data.levels.clone(), self.data.forgotten.clone())
    }

    /// Undo changes to the table lists and the forgotten ranges made since
    /// `state` was taken. Counters are left alone: table names and range
    /// numbers handed out meanwhile stay used.
    pub fn restore_state(&mut self, state: (Vec<LevelInfo>, Vec<ForgottenRange>)) {
        (self.data.levels, self.data.forgotten) = state;
    }

    /// The key ranges being forgotten, sorted by start, no two overlapping.
    pub fn forgotten(&self) -> &[ForgottenRange] {
        &self.data.forgotten
    }

    /// The number of the newest forgotten range ever listed.
    pub fn forgotten_seq(&self) -> u64 {
        self.data.forgotten_seq
    }

    /// Whether `key` is in a forgotten range.
    pub fn is_forgotten(&self, key: &[u8]) -> bool {
        is_forgotten(&self.data.forgotten, key)
    }

    /// Whether one listed range already takes in all of `[start, end)`.
    pub fn forgets_all_of(&self, start: &[u8], end: &[u8]) -> bool {
        self.data
            .forgotten
            .iter()
            .any(|range| range.start.as_slice() <= start && end <= range.end.as_slice())
    }

    /// List `[start, end)` as forgotten. Nothing changes if a listed range
    /// already takes it all in, so repeating a call is harmless. Listed
    /// ranges it overlaps are merged into it; the result gets a new number,
    /// which makes every table it overlaps count as possibly holding its
    /// keys again.
    pub fn forget(&mut self, start: &[u8], end: &[u8]) {
        if start >= end || self.forgets_all_of(start, end) {
            return;
        }
        self.data.forgotten_seq += 1;
        let mut new = ForgottenRange {
            start: start.to_vec(),
            end: end.to_vec(),
            seq: self.data.forgotten_seq,
        };
        // The listed ranges are disjoint, so taking in the ones that overlap
        // cannot make the new range reach any of the others.
        self.data.forgotten.retain(|old| {
            let overlap = old.start < new.end && new.start < old.end;
            if overlap {
                if old.start < new.start {
                    new.start.clone_from(&old.start);
                }
                if old.end > new.end {
                    new.end.clone_from(&old.end);
                }
            }
            !overlap
        });
        let at = self
            .data
            .forgotten
            .partition_point(|old| old.start < new.start);
        self.data.forgotten.insert(at, new);
    }

    /// Whether a forgotten range may still have keys in `table`.
    pub fn holds_forgotten(&self, table: &SstableInfo) -> bool {
        self.data
            .forgotten
            .iter()
            .any(|range| range.in_table(table))
    }

    /// Whether `range` can come off the list: no table may still hold one of
    /// its keys, and `in_memory` says no memtable does.
    pub fn can_retire(
        &self,
        range: &ForgottenRange,
        in_memory: impl Fn(&ForgottenRange) -> bool,
    ) -> bool {
        let in_tables = self
            .data
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .any(|table| range.in_table(table));
        !in_tables && !in_memory(range)
    }

    /// Take every range that [`Self::can_retire`] off the list.
    pub fn retire_forgotten(&mut self, in_memory: impl Fn(&ForgottenRange) -> bool) {
        let keep: Vec<bool> = self
            .data
            .forgotten
            .iter()
            .map(|range| !self.can_retire(range, &in_memory))
            .collect();
        let mut keep = keep.into_iter();
        self.data
            .forgotten
            .retain(|_| keep.next().expect("one per range"));
    }

    /// Take every range off the list: the contents have been replaced by
    /// ones that never held their keys.
    pub fn clear_forgotten(&mut self) {
        self.data.forgotten.clear();
    }

    /// A copy of the current contents, for [`persist`].
    pub fn snapshot(&self) -> ManifestData {
        self.data.clone()
    }

    /// Generate next SSTable file name
    pub fn next_sstable_name(&mut self) -> String {
        self.data.sequence += 1;
        format!("{:06}.sst", self.data.sequence)
    }

    /// Get the next sequence number without incrementing
    pub fn peek_sequence(&self) -> u64 {
        self.data.sequence + 1
    }

    /// Add an SSTable to a level
    pub fn add_sstable(&mut self, level: u32, info: SstableInfo) {
        // Ensure level exists
        while self.data.levels.len() <= level as usize {
            self.data.levels.push(LevelInfo {
                level: self.data.levels.len() as u32,
                files: vec![],
            });
        }

        self.data.levels[level as usize].files.push(info);
    }

    /// Remove an SSTable from a level
    pub fn remove_sstable(&mut self, level: u32, name: &str) {
        if let Some(level_info) = self.data.levels.get_mut(level as usize) {
            level_info.files.retain(|f| f.name != name);
        }
    }

    /// Get all SSTables at a level
    pub fn get_level(&self, level: u32) -> &[SstableInfo] {
        self.data
            .levels
            .get(level as usize)
            .map(|l| l.files.as_slice())
            .unwrap_or(&[])
    }

    /// Get total size of a level in bytes
    pub fn level_size(&self, level: u32) -> u64 {
        self.get_level(level).iter().map(|f| f.size).sum()
    }

    /// Get number of SSTables at a level
    pub fn level_count(&self, level: u32) -> usize {
        self.get_level(level).len()
    }

    /// Get all levels
    pub fn levels(&self) -> &[LevelInfo] {
        &self.data.levels
    }

    /// Get directory path
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Get SSTable path
    pub fn sstable_path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

/// Write a manifest to disk, replacing the previous one atomically:
/// write a temporary file, make it durable, rename it over the manifest, then
/// make the rename durable. A crash leaves either the old manifest or the new
/// one, never a mixture.
///
/// An error before the rename means the old manifest is still the one on
/// disk. An error after it is reported as [`StorageError::ManifestNotSynced`]:
/// the new manifest is what a restart will most likely find, so the caller
/// must not undo the change or delete anything the new manifest lists.
pub async fn persist(io: &IoEngine, dir: &Path, data: &ManifestData) -> StorageResult<()> {
    let manifest_path = dir.join(MANIFEST_FILE);
    let temp_path = dir.join(format!("{MANIFEST_FILE}.tmp"));

    let content = serde_json::to_string_pretty(data)
        .map_err(|e| StorageError::Manifest(format!("failed to serialize manifest: {}", e)))?;

    // The manifest is a small text file of arbitrary length, so it is written
    // through the page cache: a whole number of pages first, then cut to size.
    let options = OpenOptions {
        direct: false,
        ..OpenOptions::create_truncate()
    };
    let file = io.open(&temp_path, options).await?;
    let mut buf = AlignedBuffer::new(content.len().max(1))?;
    buf.copy_from_slice(content.as_bytes())?;
    file.write_at(buf, 0, Urgency::Critical).await?;
    file.set_len(content.len() as u64)?;
    file.sync_all(Urgency::Critical).await?;
    drop(file);

    std::fs::rename(&temp_path, &manifest_path)?;

    // The rename lives in the directory; sync that too. Not every platform
    // can open a directory, and where it cannot there is nothing to sync.
    let dir_options = OpenOptions {
        direct: false,
        ..OpenOptions::read_only()
    };
    if let Ok(dir_file) = io.open(dir, dir_options).await {
        dir_file
            .sync_all(Urgency::Critical)
            .await
            .map_err(|e| StorageError::ManifestNotSynced(e.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "roodb_manifest_test_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn test_manifest_create_and_save() {
        let io = IoEngine::new(crate::io::IoBackend::from_env().unwrap()).unwrap();
        let dir = temp_dir("create");

        let mut manifest = Manifest::open(&dir).unwrap();
        assert_eq!(manifest.data.sequence, 0);

        let name = manifest.next_sstable_name();
        assert_eq!(name, "000001.sst");

        manifest.add_sstable(
            0,
            SstableInfo {
                name: name.clone(),
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                size: 4096,
                filtered_seq: 0,
            },
        );

        persist(&io, manifest.dir(), &manifest.snapshot())
            .await
            .unwrap();
        assert!(!dir.join("MANIFEST.tmp").exists());

        // Reload and verify
        let manifest2 = Manifest::open(&dir).unwrap();
        assert_eq!(manifest2.data.sequence, 1);
        assert_eq!(manifest2.get_level(0).len(), 1);
        assert_eq!(manifest2.get_level(0)[0].name, name);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_manifest_level_operations() {
        let dir = temp_dir("levels");

        let mut manifest = Manifest::open(&dir).unwrap();

        // Add files to different levels
        for level in 0..3 {
            for i in 0..3 {
                let name = manifest.next_sstable_name();
                manifest.add_sstable(
                    level,
                    SstableInfo {
                        name,
                        min_key: vec![b'a' + i],
                        max_key: vec![b'a' + i + 1],
                        size: 1024 * (level as u64 + 1),
                        filtered_seq: 0,
                    },
                );
            }
        }

        assert_eq!(manifest.level_count(0), 3);
        assert_eq!(manifest.level_count(1), 3);
        assert_eq!(manifest.level_count(2), 3);

        assert_eq!(manifest.level_size(0), 1024 * 3);
        assert_eq!(manifest.level_size(1), 2048 * 3);

        // Remove a file
        manifest.remove_sstable(0, "000001.sst");
        assert_eq!(manifest.level_count(0), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    fn table(min: &str, max: &str, filtered_seq: u64) -> SstableInfo {
        SstableInfo {
            name: format!("{min}-{max}.sst"),
            min_key: min.as_bytes().to_vec(),
            max_key: max.as_bytes().to_vec(),
            size: 1,
            filtered_seq,
        }
    }

    fn listed(manifest: &Manifest) -> Vec<(&str, &str, u64)> {
        manifest
            .forgotten()
            .iter()
            .map(|r| {
                (
                    std::str::from_utf8(&r.start).unwrap(),
                    std::str::from_utf8(&r.end).unwrap(),
                    r.seq,
                )
            })
            .collect()
    }

    #[test]
    fn a_manifest_of_the_previous_layout_is_refused_by_its_version() {
        let dir = tempfile::tempdir().unwrap();
        let old = r#"{"version": 2, "sequence": 1, "levels": [{"level": 0, "files": [
            {"name": "000001.sst", "min_key": [97], "max_key": [122], "size": 4096}]}]}"#;
        fs::write(dir.path().join(MANIFEST_FILE), old).unwrap();
        let Err(error) = Manifest::open(dir.path()) else {
            panic!("a version 2 manifest was accepted");
        };
        assert!(error.to_string().contains("on-disk format 2"), "{error}");
    }

    #[test]
    fn forgotten_ranges_are_kept_sorted_and_disjoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::open(dir.path()).unwrap();
        m.forget(b"m", b"p");
        m.forget(b"c", b"e");
        // Empty and inverted ranges hold nothing.
        m.forget(b"x", b"x");
        m.forget(b"z", b"y");
        assert_eq!(listed(&m), [("c", "e", 2), ("m", "p", 1)]);

        // Repeats, and ranges inside a listed one, change nothing.
        m.forget(b"m", b"p");
        m.forget(b"n", b"o");
        assert_eq!(listed(&m), [("c", "e", 2), ("m", "p", 1)]);

        // A neighbour that only touches stays a range of its own.
        m.forget(b"e", b"g");
        assert_eq!(listed(&m), [("c", "e", 2), ("e", "g", 3), ("m", "p", 1)]);

        // One that overlaps takes the others in, under a new number.
        m.forget(b"f", b"n");
        assert_eq!(listed(&m), [("c", "e", 2), ("e", "p", 4)]);

        for (key, forgotten) in [
            ("b", false),
            ("c", true),
            ("d", true),
            ("e", true),
            ("ozzz", true),
            ("p", false),
            ("q", false),
        ] {
            assert_eq!(m.is_forgotten(key.as_bytes()), forgotten, "{key}");
        }
    }

    #[test]
    fn a_range_is_retired_when_nothing_can_hold_its_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::open(dir.path()).unwrap();
        m.forget(b"m", b"p"); // number 1
        m.add_sstable(1, table("a", "c", 0)); // elsewhere
        m.add_sstable(1, table("k", "z", 1)); // spans it, written since
        m.add_sstable(0, table("o", "q", 0)); // may hold its keys
        assert!(!m.holds_forgotten(&m.get_level(1)[0]));
        assert!(!m.holds_forgotten(&m.get_level(1)[1]));
        assert!(m.holds_forgotten(&m.get_level(0)[0]));

        // Kept while a table may hold its keys.
        m.retire_forgotten(|_| false);
        assert_eq!(listed(&m).len(), 1);
        // Kept while a memtable does.
        m.remove_sstable(0, "o-q.sst");
        m.retire_forgotten(|_| true);
        assert_eq!(listed(&m).len(), 1);
        m.retire_forgotten(|_| false);
        assert!(listed(&m).is_empty());

        // The numbering goes on from where it was.
        m.forget(b"m", b"p");
        assert_eq!(listed(&m), [("m", "p", 2)]);
        assert!(m.holds_forgotten(&m.get_level(1)[1]));
    }

    #[tokio::test]
    async fn forgotten_ranges_are_stored_in_the_manifest() {
        let io = IoEngine::new(crate::io::IoBackend::from_env().unwrap()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::open(dir.path()).unwrap();
        m.forget(b"m", b"p");
        m.forget(b"c", b"e");
        m.retire_forgotten(|range| range.start == b"m");
        persist(&io, m.dir(), &m.snapshot()).await.unwrap();

        let m = Manifest::open(dir.path()).unwrap();
        assert_eq!(listed(&m), [("m", "p", 1)]);
        assert_eq!(m.forgotten_seq(), 2);
    }
}
