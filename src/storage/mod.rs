//! Storage engine subsystem
//!
//! Provides key-value storage with ordered iteration using an LSM-Tree architecture.

pub mod error;
pub mod lsm;
pub mod row_id;
pub mod traits;

pub use error::{StorageError, StorageResult};
pub use lsm::{LsmConfig, LsmEngine};
pub use row_id::next_row_id;
pub use traits::StorageEngine;
