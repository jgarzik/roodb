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

            // Delete all user data rows for this table
            let prefix = super::encoding::table_key_prefix(&self.name);
            let end = super::encoding::table_key_end(&self.name);
            let data_rows = mvcc
                .scan_raw(&prefix, &end)
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            for (key, _value) in data_rows {
                changeset.push(RowChange::delete(&self.name, key.clone()));
                self.changes.push(RowChange::delete(&self.name, key));
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

/// CREATE DATABASE executor
pub struct CreateDatabase {
    name: String,
    if_not_exists: bool,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Option<Arc<RaftNode>>,
    done: bool,
    changes: Vec<RowChange>,
}

impl CreateDatabase {
    pub fn new(name: String, if_not_exists: bool, catalog: Arc<RwLock<Catalog>>) -> Self {
        CreateDatabase {
            name,
            if_not_exists,
            catalog,
            raft_node: None,
            done: false,
            changes: Vec::new(),
        }
    }

    pub fn with_raft(
        name: String,
        if_not_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        CreateDatabase {
            name,
            if_not_exists,
            catalog,
            raft_node: Some(raft_node),
            done: false,
            changes: Vec::new(),
        }
    }
}

#[async_trait]
impl Executor for CreateDatabase {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Check if database already exists
        {
            let catalog = self.catalog.read();
            if catalog.database_exists(&self.name) {
                if self.if_not_exists {
                    self.done = true;
                    return Ok(Some(Row::new(vec![Datum::Int(0)])));
                }
                return Err(ExecutorError::Internal(format!(
                    "Can't create database '{}'; database exists",
                    self.name
                )));
            }
        }

        if let Some(ref raft_node) = self.raft_node {
            use crate::catalog::system_tables::SYSTEM_DATABASES;

            // Write to system.databases via Raft
            let (start, node_id) = allocate_row_id_batch(1);
            let row_id = encode_row_id(start, node_id);

            let row = Row::new(vec![Datum::String(self.name.clone())]);
            let key = encode_row_key(SYSTEM_DATABASES, row_id);
            let value = encode_row(&row);

            let change = RowChange::insert(SYSTEM_DATABASES.to_string(), key, value);
            let changeset = ChangeSet::new_with_changes(0, vec![change]);

            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        } else {
            // Direct catalog update (no Raft)
            let mut catalog = self.catalog.write();
            catalog
                .create_database(&self.name)
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

/// DROP DATABASE executor
pub struct DropDatabase {
    name: String,
    if_exists: bool,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Option<Arc<RaftNode>>,
    mvcc: Option<Arc<crate::txn::MvccStorage>>,
    done: bool,
    changes: Vec<RowChange>,
}

impl DropDatabase {
    pub fn new(name: String, if_exists: bool, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropDatabase {
            name,
            if_exists,
            catalog,
            raft_node: None,
            mvcc: None,
            done: false,
            changes: Vec::new(),
        }
    }

    pub fn with_raft(
        name: String,
        if_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
        mvcc: Arc<crate::txn::MvccStorage>,
    ) -> Self {
        DropDatabase {
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
impl Executor for DropDatabase {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.changes.clear();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Prevent dropping the "default" database
        if self.name == "default" {
            return Err(ExecutorError::Internal(
                "Cannot drop the 'default' database".to_string(),
            ));
        }

        // Check if database exists
        {
            let catalog = self.catalog.read();
            if !catalog.database_exists(&self.name) {
                if self.if_exists {
                    self.done = true;
                    return Ok(Some(Row::new(vec![Datum::Int(0)])));
                }
                return Err(ExecutorError::Internal(format!(
                    "Can't drop database '{}'; database doesn't exist",
                    self.name
                )));
            }
        }

        if let (Some(ref raft_node), Some(ref mvcc)) = (&self.raft_node, &self.mvcc) {
            use crate::catalog::system_tables::SYSTEM_DATABASES;

            // Scan system.databases to find the row to delete
            let prefix = format!("t:{SYSTEM_DATABASES}:");
            let end = format!("t:{SYSTEM_DATABASES};\x00");

            let rows = mvcc
                .scan_raw(prefix.as_bytes(), end.as_bytes())
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;

            let mut changeset = ChangeSet::new(0);
            for (key, value) in rows {
                let row_data = if value.len() > 17 {
                    &value[17..]
                } else {
                    &value
                };
                if let Ok(row) = super::encoding::decode_row(row_data) {
                    if let Some(Datum::String(db_name)) = row.get_opt(0) {
                        if db_name == &self.name {
                            changeset.push(RowChange::delete_with_value(
                                SYSTEM_DATABASES,
                                key.clone(),
                                row_data.to_vec(),
                            ));
                        }
                    }
                }
            }

            if !changeset.changes.is_empty() {
                raft_node
                    .propose_changes(changeset)
                    .await
                    .map_err(|e| ExecutorError::Internal(e.to_string()))?;
            }
        } else {
            // Direct catalog update (no Raft)
            let mut catalog = self.catalog.write();
            catalog
                .drop_database(&self.name)
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

// ============ CREATE TABLE ... SELECT ============

use super::context::TransactionContext;
use super::encoding::encode_pk_key;

/// CREATE TABLE ... SELECT executor
///
/// Combines DDL (table creation) and DML (row insertion from a SELECT source).
/// The table is created via Raft first, then rows from the source SELECT are
/// streamed into storage.
pub struct CreateTableAs {
    name: String,
    columns: Vec<ColumnDef>,
    constraints: Vec<Constraint>,
    if_not_exists: bool,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Option<Arc<RaftNode>>,
    source: Box<dyn Executor>,
    txn_context: Option<TransactionContext>,
    done: bool,
    rows_inserted: u64,
}

impl CreateTableAs {
    pub fn new(
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        source: Box<dyn Executor>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        CreateTableAs {
            name,
            columns,
            constraints,
            if_not_exists,
            catalog,
            raft_node: None,
            source,
            txn_context,
            done: false,
            rows_inserted: 0,
        }
    }

    pub fn with_raft(
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
        if_not_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
        source: Box<dyn Executor>,
        txn_context: Option<TransactionContext>,
    ) -> Self {
        CreateTableAs {
            name,
            columns,
            constraints,
            if_not_exists,
            catalog,
            raft_node: Some(raft_node),
            source,
            txn_context,
            done: false,
            rows_inserted: 0,
        }
    }

    /// Create the table definition and register it (DDL phase).
    /// Returns the PK column indices for row storage.
    async fn create_table_ddl(&mut self) -> ExecutorResult<Vec<usize>> {
        if is_system_table(&self.name) {
            return Err(ExecutorError::Internal(format!(
                "cannot create system table '{}'",
                self.name
            )));
        }

        // Check if table exists
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.name).is_some() {
                if self.if_not_exists {
                    return Ok(vec![]);
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

        // Determine PK column indices for row storage
        let pk_indices: Vec<usize> = if let Some(pk_cols) = table_def.primary_key() {
            pk_cols
                .iter()
                .filter_map(|pk_name| table_def.get_column_index(pk_name))
                .collect()
        } else {
            vec![]
        };

        if let Some(ref raft_node) = self.raft_node {
            // Raft path: propose DDL changes to system tables
            let mut changeset = ChangeSet::new(0);
            let column_rows = table_def_to_columns_rows(&table_def);
            let constraint_rows = table_def_to_constraints_rows(&table_def);
            let total_ids = 1 + column_rows.len() as u64 + constraint_rows.len() as u64;
            let (mut next_local, node_id) = allocate_row_id_batch(total_ids);

            let table_row = table_def_to_tables_row(&table_def);
            let row_id = encode_row_id(next_local, node_id);
            next_local += 1;
            let key = encode_row_key(SYSTEM_TABLES, row_id);
            let value = encode_row(&table_row);
            changeset.push(RowChange::insert(SYSTEM_TABLES, key, value));

            for col_row in column_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_COLUMNS, row_id);
                let value = encode_row(&col_row);
                changeset.push(RowChange::insert(SYSTEM_COLUMNS, key, value));
            }

            for constraint_row in constraint_rows {
                let row_id = encode_row_id(next_local, node_id);
                next_local += 1;
                let key = encode_row_key(SYSTEM_CONSTRAINTS, row_id);
                let value = encode_row(&constraint_row);
                changeset.push(RowChange::insert(SYSTEM_CONSTRAINTS, key, value));
            }

            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
        } else {
            // No Raft: update catalog directly
            let mut catalog = self.catalog.write();
            catalog
                .create_table(table_def)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        Ok(pk_indices)
    }
}

#[async_trait]
impl Executor for CreateTableAs {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        self.rows_inserted = 0;
        self.source.open().await
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // Phase 1: Create the table (DDL)
        let pk_indices = self.create_table_ddl().await?;

        // If table already existed (IF NOT EXISTS), return 0 rows affected
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&self.name).is_none() && self.if_not_exists {
                self.done = true;
                return Ok(Some(Row::new(vec![Datum::Int(0)])));
            }
        }

        // Phase 2: Stream rows from source SELECT into the new table
        // Allocate row IDs in batches for performance
        let batch_size = 1000u64;
        let (mut next_local, node_id) = allocate_row_id_batch(batch_size);
        let mut batch_remaining = batch_size;

        while let Some(row) = self.source.next().await? {
            if batch_remaining == 0 {
                let (new_local, new_node) = allocate_row_id_batch(batch_size);
                next_local = new_local;
                let _ = new_node; // Same node
                batch_remaining = batch_size;
            }

            let row_id = encode_row_id(next_local, node_id);
            next_local += 1;
            batch_remaining -= 1;

            // Encode storage key (PK-based or row-ID-based)
            let key = if !pk_indices.is_empty() {
                let pk_values: Vec<_> = pk_indices
                    .iter()
                    .filter_map(|&idx| row.get(idx).ok().cloned())
                    .collect();
                encode_pk_key(&self.name, &pk_values)
            } else {
                encode_row_key(&self.name, row_id)
            };
            let value = encode_row(&row);

            // Buffer the row change for Raft replication
            if let Some(ref mut ctx) = self.txn_context {
                ctx.add_change(RowChange::insert(&self.name, key.clone(), value.clone()));
                ctx.buffer_write(key, value);
            }
            self.rows_inserted += 1;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(self.rows_inserted as i64)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.source.close().await
    }

    fn take_changes(&mut self) -> Vec<RowChange> {
        self.txn_context
            .as_mut()
            .map(|ctx| ctx.take_changes())
            .unwrap_or_default()
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
