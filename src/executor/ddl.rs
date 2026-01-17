//! DDL executors
//!
//! Implements CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX.
//!
//! DDL operations are replicated via Raft by persisting schema changes to system tables
//! (system.tables, system.columns, system.indexes). When raft_node is provided, changes
//! are proposed to Raft and the catalog is updated when apply() processes the changes.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::catalog::system_tables::{
    index_def_to_row, is_system_table, table_def_to_columns_rows, table_def_to_constraints_rows,
    table_def_to_tables_row, SYSTEM_COLUMNS, SYSTEM_CONSTRAINTS, SYSTEM_INDEXES, SYSTEM_TABLES,
};
use crate::catalog::{Catalog, ColumnDef, Constraint, IndexDef, TableDef};
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::storage::row_id::{allocate_row_id_batch, encode_row_id};

use super::datum::Datum;
use super::encoding::{encode_row, encode_row_key};
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;
use super::Executor;

/// CREATE TABLE executor
pub struct CreateTable {
    /// Table name
    name: String,
    /// Column definitions
    columns: Vec<ColumnDef>,
    /// Constraints
    constraints: Vec<Constraint>,
    /// IF NOT EXISTS flag
    if_not_exists: bool,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Optional Raft node for replication
    raft_node: Option<Arc<RaftNode>>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl CreateTable {
    /// Create a new CREATE TABLE executor
    pub fn new(
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
            catalog,
            raft_node: None,
            done: false,
            changes: Vec::new(),
        }
    }

    /// Create a new CREATE TABLE executor with Raft replication
    pub fn with_raft(
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
            catalog,
            raft_node: Some(raft_node),
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for CreateTable {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Prevent creating tables with system. prefix
        if is_system_table(&self.name) {
            return Err(ExecutorError::Internal(format!(
                "cannot create system table '{}'",
                self.name
            )));
        }

        // Check if table exists (catalog read)
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.name).is_some() {
                if self.if_not_exists {
                    self.done = true;
                    return Ok(Some(Row::new(vec![Datum::Int(0)])));
                }
                return Err(ExecutorError::Internal(format!(
                    "table '{}' already exists",
                    self.name
                )));
            }
        }

        // Build table definition
        let mut table_def = TableDef::new(&self.name);
        for col in &self.columns {
            table_def = table_def.column(col.clone());
        }
        for constraint in &self.constraints {
            table_def = table_def.constraint(constraint.clone());
        }

        if let Some(ref raft_node) = self.raft_node {
            // Generate RowChanges for system tables and propose via Raft
            let mut changeset = ChangeSet::new(0);

            // Pre-allocate row IDs: 1 for system.tables + N for system.columns + M for system.constraints
            let column_rows = table_def_to_columns_rows(&table_def);
            let constraint_rows = table_def_to_constraints_rows(&table_def);
            let total_ids = 1 + column_rows.len() as u64 + constraint_rows.len() as u64;
            let (mut next_local, node_id) = allocate_row_id_batch(total_ids);

            // Insert into system.tables
            let table_row = table_def_to_tables_row(&table_def);
            let row_id = encode_row_id(next_local, node_id);
            next_local += 1;
            let key = encode_row_key(SYSTEM_TABLES, row_id);
            let value = encode_row(&table_row);
            changeset.push(RowChange::insert(SYSTEM_TABLES, key.clone(), value.clone()));
            self.changes
                .push(RowChange::insert(SYSTEM_TABLES, key, value));

            // Insert into system.columns (one row per column)
            for col_row in column_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_COLUMNS, row_id);
                let value = encode_row(&col_row);
                changeset.push(RowChange::insert(
                    SYSTEM_COLUMNS,
                    key.clone(),
                    value.clone(),
                ));
                self.changes
                    .push(RowChange::insert(SYSTEM_COLUMNS, key, value));
            }

            // Insert into system.constraints (one row per constraint)
            for constraint_row in constraint_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_CONSTRAINTS, row_id);
                let value = encode_row(&constraint_row);
                changeset.push(RowChange::insert(
                    SYSTEM_CONSTRAINTS,
                    key.clone(),
                    value.clone(),
                ));
                self.changes
                    .push(RowChange::insert(SYSTEM_CONSTRAINTS, key, value));
            }

            // Propose to Raft - catalog is updated synchronously in apply()
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            // No need to update catalog here - apply() already did it before propose returns
        } else {
            // No Raft - update catalog directly (for tests without Raft)
            let mut catalog = self.catalog.write();
            catalog
                .create_table(table_def)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// DROP TABLE executor
pub struct DropTable {
    /// Table name
    name: String,
    /// IF EXISTS flag
    if_exists: bool,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Optional Raft node for replication
    raft_node: Option<Arc<RaftNode>>,
    /// MVCC storage for scanning system tables
    mvcc: Option<Arc<crate::txn::MvccStorage>>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl DropTable {
    /// Create a new DROP TABLE executor
    pub fn new(name: String, if_exists: bool, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropTable {
            name,
            if_exists,
            catalog,
            raft_node: None,
            mvcc: None,
            done: false,
            changes: Vec::new(),
        }
    }

    /// Create a new DROP TABLE executor with Raft replication
    pub fn with_raft(
        name: String,
        if_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
        mvcc: Arc<crate::txn::MvccStorage>,
    ) -> Self {
        DropTable {
            name,
            if_exists,
            catalog,
            raft_node: Some(raft_node),
            mvcc: Some(mvcc),
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for DropTable {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Prevent dropping system tables
        if is_system_table(&self.name) {
            return Err(ExecutorError::Internal(format!(
                "cannot drop system table '{}'",
                self.name
            )));
        }

        // Check if table exists (catalog read)
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.name).is_none() {
                if self.if_exists {
                    self.done = true;
                    return Ok(Some(Row::new(vec![Datum::Int(0)])));
                }
                return Err(ExecutorError::TableNotFound(self.name.clone()));
            }
        }

        if let (Some(ref raft_node), Some(ref mvcc)) = (&self.raft_node, &self.mvcc) {
            // Scan system tables to find rows to delete
            let mut changeset = ChangeSet::new(0);

            // Find and delete from system.tables
            let prefix = format!("t:{SYSTEM_TABLES}:");
            let end = format!("t:{SYSTEM_TABLES};\x00");
            let rows = mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            for (key, value) in rows {
                // Skip MVCC header (17 bytes) if present
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = super::encoding::decode_row(row_data) {
                    if let Some(Datum::String(table_name)) = row.values().first() {
                        if table_name == &self.name {
                            // Emit delete with value for tracking
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_TABLES,
                                key.clone(),
                                row_data.to_vec(),
                            ));
                            self.changes.push(RowChange::delete_with_value(
                                SYSTEM_TABLES,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            // Find and delete from system.columns
            let prefix = format!("t:{SYSTEM_COLUMNS}:");
            let end = format!("t:{SYSTEM_COLUMNS};\x00");
            let rows = mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            for (key, value) in rows {
                // Skip MVCC header (17 bytes) if present
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = super::encoding::decode_row(row_data) {
                    if let Some(Datum::String(table_name)) = row.values().first() {
                        if table_name == &self.name {
                            // Emit delete with value for tracking
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_COLUMNS,
                                key.clone(),
                                row_data.to_vec(),
                            ));
                            self.changes.push(RowChange::delete_with_value(
                                SYSTEM_COLUMNS,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            // Find and delete from system.constraints
            let prefix = format!("t:{SYSTEM_CONSTRAINTS}:");
            let end = format!("t:{SYSTEM_CONSTRAINTS};\x00");
            let rows = mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            for (key, value) in rows {
                // Skip MVCC header (17 bytes) if present
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = super::encoding::decode_row(row_data) {
                    if let Some(Datum::String(table_name)) = row.values().first() {
                        if table_name == &self.name {
                            // Emit delete with value for tracking
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_CONSTRAINTS,
                                key.clone(),
                                row_data.to_vec(),
                            ));
                            self.changes.push(RowChange::delete_with_value(
                                SYSTEM_CONSTRAINTS,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            // Propose to Raft - catalog is updated synchronously in apply()
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            // No need to update catalog here - apply() already did it before propose returns
        } else {
            // No Raft - update catalog directly (for tests without Raft)
            let mut catalog = self.catalog.write();
            catalog
                .drop_table(&self.name)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// CREATE INDEX executor
pub struct CreateIndex {
    /// Index name
    name: String,
    /// Table name
    table: String,
    /// Column names and indices
    columns: Vec<(String, usize)>,
    /// Whether index is unique
    unique: bool,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Optional Raft node for replication
    raft_node: Option<Arc<RaftNode>>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl CreateIndex {
    /// Create a new CREATE INDEX executor
    pub fn new(
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        CreateIndex {
            name,
            table,
            columns,
            unique,
            catalog,
            raft_node: None,
            done: false,
            changes: Vec::new(),
        }
    }

    /// Create a new CREATE INDEX executor with Raft replication
    pub fn with_raft(
        name: String,
        table: String,
        columns: Vec<(String, usize)>,
        unique: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        CreateIndex {
            name,
            table,
            columns,
            unique,
            catalog,
            raft_node: Some(raft_node),
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for CreateIndex {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Verify table exists (catalog read)
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.table).is_none() {
                return Err(ExecutorError::TableNotFound(self.table.clone()));
            }
        }

        let column_names: Vec<String> = self.columns.iter().map(|(n, _)| n.clone()).collect();

        let mut index_def = IndexDef::new(&self.name, &self.table, column_names);
        if self.unique {
            index_def = index_def.unique();
        }

        if let Some(ref raft_node) = self.raft_node {
            // Generate RowChange for system.indexes and propose via Raft
            let mut changeset = ChangeSet::new(0);

            // Allocate single row ID for system.indexes
            let (local_id, node_id) = allocate_row_id_batch(1);
            let row_id = encode_row_id(local_id, node_id);

            // Insert into system.indexes
            let index_row = index_def_to_row(&index_def);
            let key = encode_row_key(SYSTEM_INDEXES, row_id);
            let value = encode_row(&index_row);
            changeset.push(RowChange::insert(
                SYSTEM_INDEXES,
                key.clone(),
                value.clone(),
            ));
            self.changes
                .push(RowChange::insert(SYSTEM_INDEXES, key, value));

            // Propose to Raft - catalog is updated synchronously in apply()
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            // No need to update catalog here - apply() already did it before propose returns
        } else {
            // No Raft - update catalog directly (for tests without Raft)
            let mut catalog = self.catalog.write();
            catalog
                .create_index(index_def)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

/// DROP INDEX executor
pub struct DropIndex {
    /// Index name
    name: String,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Optional Raft node for replication
    raft_node: Option<Arc<RaftNode>>,
    /// MVCC storage for scanning system tables
    mvcc: Option<Arc<crate::txn::MvccStorage>>,
    /// Whether execution is complete
    done: bool,
    /// Collected row changes
    changes: Vec<RowChange>,
}

impl DropIndex {
    /// Create a new DROP INDEX executor
    pub fn new(name: String, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropIndex {
            name,
            catalog,
            raft_node: None,
            mvcc: None,
            done: false,
            changes: Vec::new(),
        }
    }

    /// Create a new DROP INDEX executor with Raft replication
    pub fn with_raft(
        name: String,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
        mvcc: Arc<crate::txn::MvccStorage>,
    ) -> Self {
        DropIndex {
            name,
            catalog,
            raft_node: Some(raft_node),
            mvcc: Some(mvcc),
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for DropIndex {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        if let (Some(ref raft_node), Some(ref mvcc)) = (&self.raft_node, &self.mvcc) {
            // Scan system.indexes to find rows to delete
            let mut changeset = ChangeSet::new(0);

            let prefix = format!("t:{SYSTEM_INDEXES}:");
            let end = format!("t:{SYSTEM_INDEXES};\x00");
            let rows = mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            for (key, value) in rows {
                // Skip MVCC header (17 bytes) if present
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = super::encoding::decode_row(row_data) {
                    if let Some(Datum::String(index_name)) = row.values().first() {
                        if index_name == &self.name {
                            // Emit delete with value for tracking
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_INDEXES,
                                key.clone(),
                                row_data.to_vec(),
                            ));
                            self.changes.push(RowChange::delete_with_value(
                                SYSTEM_INDEXES,
                                key,
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            // Propose to Raft - catalog is updated synchronously in apply()
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            // No need to update catalog here - apply() already did it before propose returns
        } else {
            // No Raft - update catalog directly (for tests without Raft)
            let mut catalog = self.catalog.write();
            catalog
                .drop_index(&self.name)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;

    #[tokio::test]
    async fn test_create_table() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        let columns = vec![
            ColumnDef::new("id", DataType::Int).nullable(false),
            ColumnDef::new("name", DataType::Varchar(100)),
        ];
        let constraints = vec![Constraint::PrimaryKey(vec!["id".to_string()])];

        let mut create = CreateTable::new(
            "users".to_string(),
            columns,
            constraints,
            false,
            catalog.clone(),
        );
        create.open().await.unwrap();

        let result = create.next().await.unwrap();
        assert!(result.is_some());

        create.close().await.unwrap();

        // Verify table exists
        let cat = catalog.read();
        assert!(cat.get_table("users").is_some());
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Create table first time
        let columns = vec![ColumnDef::new("id", DataType::Int)];
        let mut create1 = CreateTable::new(
            "users".to_string(),
            columns.clone(),
            vec![],
            false,
            catalog.clone(),
        );
        create1.open().await.unwrap();
        create1.next().await.unwrap();

        // Try to create again with IF NOT EXISTS
        let mut create2 = CreateTable::new(
            "users".to_string(),
            columns,
            vec![],
            true, // if_not_exists
            catalog.clone(),
        );
        create2.open().await.unwrap();
        let result = create2.next().await.unwrap();
        assert!(result.is_some()); // Should succeed silently
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Create table first
        {
            let mut cat = catalog.write();
            cat.create_table(TableDef::new("users").column(ColumnDef::new("id", DataType::Int)))
                .unwrap();
        }

        // Drop it
        let mut drop = DropTable::new("users".to_string(), false, catalog.clone());
        drop.open().await.unwrap();
        drop.next().await.unwrap();

        // Verify table is gone
        let cat = catalog.read();
        assert!(cat.get_table("users").is_none());
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Try to drop non-existent table with IF EXISTS
        let mut drop = DropTable::new("nonexistent".to_string(), true, catalog.clone());
        drop.open().await.unwrap();
        let result = drop.next().await.unwrap();
        assert!(result.is_some()); // Should succeed silently
    }
}
