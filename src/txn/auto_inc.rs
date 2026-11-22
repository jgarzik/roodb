//! AUTO_INCREMENT counters: one per table, as in MySQL.
//!
//! The next value of each table is held in memory on the primary, where
//! inserts take values from it. A value once handed out is not handed out
//! again, whether or not its insert commits. What is stored (and so
//! replicated, and there after a restart) is the counter as committed rows
//! left it: every inserted row of such a table carries the counter's value
//! after it (`RowChange::auto_inc`), and applying the row raises the stored
//! counter to that. The memory copy is loaded from the stored one the first
//! time a table is inserted into, and dropped when the table is dropped or
//! the node becomes primary.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::storage::{StorageEngine, StorageResult};

/// Where a table's counter is stored: the next value, eight bytes,
/// little-endian. A key of its own, outside every table's rows.
pub fn storage_key(table: &str) -> Vec<u8> {
    let mut key = b"autoinc:".to_vec();
    key.extend_from_slice(table.as_bytes());
    key
}

/// A stored counter's value. Anything malformed counts as no counter.
pub fn decode(stored: &[u8]) -> Option<u64> {
    stored.try_into().ok().map(u64::from_le_bytes)
}

/// The next AUTO_INCREMENT value of each table inserted into so far.
#[derive(Debug, Default)]
pub struct AutoIncrement {
    next: Mutex<HashMap<String, u64>>,
}

impl AutoIncrement {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `with` on the table's counter, loading it from `storage` first
    /// if this is the table's first use.
    async fn with_counter<T>(
        &self,
        storage: &dyn StorageEngine,
        table: &str,
        with: impl Fn(&mut u64) -> T,
    ) -> StorageResult<T> {
        loop {
            if let Some(counter) = self.next.lock().get_mut(table) {
                return Ok(with(counter));
            }
            let stored = storage
                .get(&storage_key(table))
                .await?
                .and_then(|stored| decode(&stored))
                .unwrap_or(1);
            self.next
                .lock()
                .entry(table.to_string())
                .or_insert(stored.max(1));
        }
    }

    /// Take the table's next value.
    pub async fn take(&self, storage: &dyn StorageEngine, table: &str) -> StorageResult<u64> {
        self.with_counter(storage, table, |counter| {
            let value = *counter;
            *counter += 1;
            value
        })
        .await
    }

    /// A row was given `value` explicitly: the counter moves past it.
    pub async fn observe(
        &self,
        storage: &dyn StorageEngine,
        table: &str,
        value: u64,
    ) -> StorageResult<()> {
        self.with_counter(storage, table, |counter| {
            *counter = (*counter).max(value.saturating_add(1));
        })
        .await
    }

    /// A committed row put the stored counter at `next`: a counter held in
    /// memory is not left behind it. (It is behind only on a node that has
    /// just become primary and loaded the counter before it had applied
    /// everything the previous primary committed.)
    pub fn raise(&self, table: &str, next: u64) {
        if let Some(counter) = self.next.lock().get_mut(table) {
            *counter = (*counter).max(next);
        }
    }

    /// Forget the table's counter: the table is gone, and one created under
    /// its name starts from what is stored for it (nothing: 1).
    pub fn forget(&self, table: &str) {
        self.next.lock().remove(table);
    }

    /// Forget every counter. For a node that has just become primary: the
    /// stored counters are what the previous primary's commits left.
    pub fn forget_all(&self) {
        self.next.lock().clear();
    }
}
