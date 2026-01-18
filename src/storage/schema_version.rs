//! Schema version tracking for database initialization
//!
//! The schema version marker indicates whether the database has been properly
//! initialized and what schema version it's running.

use std::sync::Arc;

use crate::storage::{StorageEngine, StorageResult};

/// Current schema version
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Key for schema version marker in storage
const SCHEMA_VERSION_KEY: &[u8] = b"m:schema_version";

/// Check if the database has been initialized
pub async fn is_initialized(storage: &Arc<dyn StorageEngine>) -> StorageResult<bool> {
    let value = storage.get(SCHEMA_VERSION_KEY).await?;
    Ok(value.is_some())
}

/// Get the current schema version from storage
pub async fn get_schema_version(storage: &Arc<dyn StorageEngine>) -> StorageResult<Option<u32>> {
    let value = storage.get(SCHEMA_VERSION_KEY).await?;
    match value {
        Some(bytes) if bytes.len() == 4 => {
            let arr: [u8; 4] = bytes.try_into().unwrap();
            Ok(Some(u32::from_le_bytes(arr)))
        }
        Some(_) => Ok(None), // Invalid format
        None => Ok(None),
    }
}

/// Write the schema version marker to storage
pub async fn write_schema_version(
    storage: &Arc<dyn StorageEngine>,
    version: u32,
) -> StorageResult<()> {
    storage
        .put(SCHEMA_VERSION_KEY, &version.to_le_bytes())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version_key() {
        // Ensure key is stable
        assert_eq!(SCHEMA_VERSION_KEY, b"m:schema_version");
    }
}
