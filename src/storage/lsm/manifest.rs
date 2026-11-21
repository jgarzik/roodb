//! Manifest file for tracking SSTable metadata
//!
//! The manifest tracks which SSTables exist at each level and their key ranges.
//! It is stored as JSON for simplicity and human readability.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage::error::{StorageError, StorageResult};

/// Manifest file name
pub const MANIFEST_FILE: &str = "MANIFEST";

/// Version of the on-disk layout this build reads and writes: the manifest
/// itself and the table format it points at. A directory written by a
/// different version is refused rather than misread.
pub const MANIFEST_VERSION: u32 = 2;

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

        let data: ManifestData = if manifest_path.exists() {
            let content = std::fs::read_to_string(&manifest_path)?;
            serde_json::from_str(&content)
                .map_err(|e| StorageError::Manifest(format!("failed to parse manifest: {}", e)))?
        } else {
            ManifestData::default()
        };
        if data.version != MANIFEST_VERSION {
            return Err(StorageError::Manifest(format!(
                "data directory {} uses on-disk format {}, this build uses format {}; \
                 re-initialize it with roodb_init",
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

    /// Save manifest to disk using atomic rename
    ///
    /// This ensures the manifest is never partially written:
    /// 1. Write to a temporary file
    /// 2. fsync the temporary file
    /// 3. Rename to final path (atomic on POSIX)
    pub fn save(&self) -> StorageResult<()> {
        let manifest_path = self.dir.join(MANIFEST_FILE);
        let temp_path = self.dir.join(format!("{}.tmp", MANIFEST_FILE));

        // Serialize content
        let content = serde_json::to_string_pretty(&self.data)
            .map_err(|e| StorageError::Manifest(format!("failed to serialize manifest: {}", e)))?;

        // Write to temporary file
        {
            let mut file = File::create(&temp_path)?;
            file.write_all(content.as_bytes())?;
            // fsync to ensure data is on disk before rename
            file.sync_all()?;
        }

        // Atomic rename
        std::fs::rename(&temp_path, &manifest_path)?;

        // Optionally sync the directory to ensure rename is durable
        // (Some filesystems require this for full durability)
        #[cfg(unix)]
        {
            if let Ok(dir) = File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
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

    #[test]
    fn test_manifest_create_and_save() {
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
            },
        );

        manifest.save().unwrap();

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
}
