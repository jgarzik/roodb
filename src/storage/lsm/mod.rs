//! LSM-Tree storage engine implementation
//!
//! Architecture:
//! - Memtable: In-memory sorted buffer (BTreeMap)
//! - SSTables: Immutable sorted files on disk
//! - Levels: L0 (unsorted), L1+ (sorted, non-overlapping)
//! - Compaction: Background merge of SSTables

pub mod block;
pub mod compaction;
pub mod engine;
pub mod manifest;
pub mod memtable;
pub mod sstable;

pub use engine::{LsmConfig, LsmEngine};
