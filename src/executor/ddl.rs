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

use crate::catalog::system_tables::{is_system_table, SYSTEM_TRIGGERS};
use crate::catalog::{Catalog, ColumnDef, Constraint, IndexDef, TableDef};
use crate::raft::{ChangeSet, RaftNode, RowChange};
use crate::storage::row_id::allocate_row_id_batch;

use super::datum::Datum;
use super::encoding::{encode_row, encode_row_key};
use super::error::{ExecutorError, ExecutorResult};
use super::row::Row;
use super::Executor;

/// CREATE TABLE executor
pub struct CreateTable {
    /// Table name
    name: crate::catalog::TableName,
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
}

impl CreateTable {
    /// Create a new CREATE TABLE executor
    pub fn new(
        name: crate::catalog::TableName,
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
        }
    }

    /// Create a new CREATE TABLE executor with Raft replication
    pub fn with_raft(
        name: crate::catalog::TableName,
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
        }
    }
}

#[async_trait]
impl Executor for CreateTable {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
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
            // The table's ID, from the cluster's counter: every key of the
            // table begins with it.
            let id = raft_node
                .alloc_ids(1)
                .await
                .map(super::encoding::TableId)
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            let table_def = table_def.with_id(id);
            // Checked here, before it is proposed: what is in the log is
            // applied as it is.
            self.catalog
                .read()
                .validate_table(&table_def)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
            // The table is one record: its definition, and no indexes yet.
            let record = crate::catalog::store::TableRecord {
                def: table_def,
                indexes: Vec::new(),
            };
            let mut changeset = ChangeSet::new(0);
            changeset.push(crate::catalog::store::create_table(&record));
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            // The catalog was updated when the change was applied, before
            // the proposal returned.
        } else {
            // No Raft - update catalog directly (for tests without Raft)
            let table_def = table_def.with_id(super::encoding::local_table_id());
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
}

/// The changes that drop a table: its record (and with it its definition
/// and its indexes), its triggers, and its rows. `triggers` is what
/// `system.triggers` holds.
async fn drop_table_changes(
    mvcc: &crate::txn::MvccStorage,
    name: &crate::catalog::TableName,
    id: super::encoding::TableId,
    triggers: &[crate::catalog::store::StoredRow],
    changeset: &mut ChangeSet,
) -> ExecutorResult<()> {
    changeset.push(crate::catalog::store::drop_table(name, id));

    // Left behind, its triggers would come back with the next table of
    // this name. Their rows name the table after the trigger's own name.
    for trigger in triggers {
        let on_this_table = matches!(
            trigger.row.values(),
            [Datum::String(database), _, Datum::String(table), ..]
                if *database == name.database && *table == name.table
        );
        if on_this_table {
            changeset.push(crate::catalog::store::delete_row(trigger));
        }
    }

    // Everything stored under the table's ID.
    let prefix = super::encoding::table_key_prefix(id);
    let end = super::encoding::table_key_end(id);
    let data_rows = mvcc
        .scan_raw(&prefix, &end)
        .await
        .map_err(|e| ExecutorError::Internal(e.to_string()))?;
    for (key, _value) in data_rows {
        changeset.push(RowChange::delete(key));
    }
    Ok(())
}

/// DROP TABLE executor
pub struct DropTable {
    /// Table name
    name: crate::catalog::TableName,
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
}

impl DropTable {
    /// Create a new DROP TABLE executor
    pub fn new(
        name: crate::catalog::TableName,
        if_exists: bool,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        DropTable {
            name,
            if_exists,
            catalog,
            raft_node: None,
            mvcc: None,
            done: false,
        }
    }

    /// Create a new DROP TABLE executor with Raft replication
    pub fn with_raft(
        name: crate::catalog::TableName,
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
        }
    }
}

#[async_trait]
impl Executor for DropTable {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
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

        // The table, if it exists (catalog read)
        let table_id = {
            let catalog = self.catalog.read();
            match catalog.get_table(&self.name) {
                Some(def) => def.id,
                None => {
                    if self.if_exists {
                        self.done = true;
                        return Ok(Some(Row::new(vec![Datum::Int(0)])));
                    }
                    return Err(ExecutorError::TableNotFound(self.name.to_string()));
                }
            }
        };

        if let (Some(ref raft_node), Some(ref mvcc)) = (&self.raft_node, &self.mvcc) {
            let mut changeset = ChangeSet::new(0);
            let triggers = crate::catalog::store::scan_rows(mvcc.inner().as_ref(), SYSTEM_TRIGGERS)
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
            drop_table_changes(mvcc, &self.name, table_id, &triggers, &mut changeset).await?;

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
}

/// CREATE INDEX executor
pub struct CreateIndex {
    /// Index name
    name: String,
    /// Table name
    table: crate::catalog::TableName,
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
}

impl CreateIndex {
    /// Create a new CREATE INDEX executor
    pub fn new(
        name: String,
        table: crate::catalog::TableName,
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
        }
    }

    /// Create a new CREATE INDEX executor with Raft replication
    pub fn with_raft(
        name: String,
        table: crate::catalog::TableName,
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
        }
    }
}

#[async_trait]
impl Executor for CreateIndex {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // The table as it is now, with its indexes. Everything is checked
        // here, before anything is proposed.
        let (def, mut indexes) = {
            let catalog = self.catalog.read();
            let Some(def) = catalog.get_table(&self.table) else {
                return Err(ExecutorError::TableNotFound(self.table.to_string()));
            };
            if catalog.get_index(&self.name).is_some() {
                return Err(ExecutorError::DuplicateKeyName(self.name.clone()));
            }
            (def.clone(), catalog.table_indexes(&self.table))
        };

        let column_names: Vec<String> = self.columns.iter().map(|(n, _)| n.clone()).collect();

        let mut index_def = IndexDef::new(&self.name, &self.table, column_names);
        if self.unique {
            index_def = index_def.unique();
        }

        if def.is_temporary {
            // A temporary table is known to its session and nowhere else:
            // its index goes to the catalog, never to the log.
            indexes.push(index_def);
            self.catalog.write().set_table(def, indexes);
        } else if let Some(ref raft_node) = self.raft_node {
            // The index is part of the table's record.
            indexes.push(index_def);
            let record = crate::catalog::store::TableRecord { def, indexes };
            let mut changeset = ChangeSet::new(0);
            changeset.push(crate::catalog::store::replace_table(&record));
            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
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
}

/// DROP INDEX executor
pub struct DropIndex {
    /// Index name
    name: String,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Optional Raft node for replication
    raft_node: Option<Arc<RaftNode>>,
    /// Whether execution is complete
    done: bool,
}

impl DropIndex {
    /// Create a new DROP INDEX executor
    pub fn new(name: String, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropIndex {
            name,
            catalog,
            raft_node: None,
            done: false,
        }
    }

    /// Create a new DROP INDEX executor with Raft replication
    pub fn with_raft(
        name: String,
        catalog: Arc<RwLock<Catalog>>,
        raft_node: Arc<RaftNode>,
    ) -> Self {
        DropIndex {
            name,
            catalog,
            raft_node: Some(raft_node),
            done: false,
        }
    }
}

#[async_trait]
impl Executor for DropIndex {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        if let Some(ref raft_node) = self.raft_node {
            // The index is part of its table's record.
            let record = {
                let catalog = self.catalog.read();
                let Some(index) = catalog.get_index(&self.name) else {
                    return Err(ExecutorError::CantDropKey(self.name.clone()));
                };
                let table = index.table.clone();
                let Some(def) = catalog.get_table(&table) else {
                    return Err(ExecutorError::TableNotFound(table.to_string()));
                };
                let mut indexes = catalog.table_indexes(&table);
                indexes.retain(|index| index.name != self.name);
                crate::catalog::store::TableRecord {
                    def: def.clone(),
                    indexes,
                }
            };
            if record.def.is_temporary {
                // A temporary table's indexes live in the catalog only.
                self.catalog.write().set_table(record.def, record.indexes);
            } else {
                let mut changeset = ChangeSet::new(0);
                changeset.push(crate::catalog::store::replace_table(&record));
                raft_node
                    .propose_changes(changeset)
                    .await
                    .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            }
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
}

/// CREATE DATABASE executor
pub struct CreateDatabase {
    name: String,
    if_not_exists: bool,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Option<Arc<RaftNode>>,
    done: bool,
}

impl CreateDatabase {
    pub fn new(name: String, if_not_exists: bool, catalog: Arc<RwLock<Catalog>>) -> Self {
        CreateDatabase {
            name,
            if_not_exists,
            catalog,
            raft_node: None,
            done: false,
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
        }
    }
}

#[async_trait]
impl Executor for CreateDatabase {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
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
                return Err(ExecutorError::DatabaseExists(self.name.clone()));
            }
        }

        if let Some(ref raft_node) = self.raft_node {
            use crate::catalog::system_tables::SYSTEM_DATABASES;

            // The database's row, under its name
            let row = Row::new(vec![Datum::String(self.name.clone())]);
            let change = crate::catalog::store::insert_row(SYSTEM_DATABASES, &row);
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
}

/// DROP DATABASE executor
pub struct DropDatabase {
    name: String,
    if_exists: bool,
    catalog: Arc<RwLock<Catalog>>,
    raft_node: Option<Arc<RaftNode>>,
    mvcc: Option<Arc<crate::txn::MvccStorage>>,
    done: bool,
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
        }
    }
}

#[async_trait]
impl Executor for DropDatabase {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.done = false;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        // The system's own databases stay.
        if crate::catalog::system_tables::is_system_database(&self.name) {
            return Err(ExecutorError::Internal(format!(
                "cannot drop the system database '{}'",
                self.name
            )));
        }

        // The database, and the tables it holds. (A temporary table belongs
        // to its session, not to the database it is named in.)
        let tables: Vec<(crate::catalog::TableName, super::encoding::TableId)> = {
            let catalog = self.catalog.read();
            if !catalog.database_exists(&self.name) {
                if self.if_exists {
                    self.done = true;
                    return Ok(Some(Row::new(vec![Datum::Int(0)])));
                }
                return Err(ExecutorError::DatabaseNotFound(self.name.clone()));
            }
            catalog
                .list_tables()
                .into_iter()
                .filter(|name| name.is_in(&self.name))
                .filter_map(|name| catalog.get_table(name))
                .filter(|def| !def.is_temporary)
                .map(|def| (def.name.clone(), def.id))
                .collect()
        };

        if let (Some(raft_node), Some(mvcc)) = (&self.raft_node, &self.mvcc) {
            use crate::catalog::store::{delete_keyed, delete_row, scan_rows};
            use crate::catalog::system_tables::{
                SYSTEM_DATABASES, SYSTEM_PROCEDURES, SYSTEM_VIEWS,
            };
            let internal = |e: crate::storage::StorageError| ExecutorError::Internal(e.to_string());

            // One change set: the database goes with everything in it, or
            // nothing goes.
            let mut changeset = ChangeSet::new(0);
            let triggers = scan_rows(mvcc.inner().as_ref(), SYSTEM_TRIGGERS)
                .await
                .map_err(internal)?;
            for (name, id) in &tables {
                drop_table_changes(mvcc, name, *id, &triggers, &mut changeset).await?;
            }
            // Its views: their rows begin with the database's name.
            for view in scan_rows(mvcc.inner().as_ref(), SYSTEM_VIEWS)
                .await
                .map_err(internal)?
            {
                if matches!(view.row.values().first(), Some(Datum::String(database)) if *database == self.name)
                {
                    changeset.push(delete_row(&view));
                }
            }
            // Its routines likewise.
            for routine in scan_rows(mvcc.inner().as_ref(), SYSTEM_PROCEDURES)
                .await
                .map_err(internal)?
            {
                if matches!(routine.row.values().first(), Some(Datum::String(database)) if *database == self.name)
                {
                    changeset.push(delete_row(&routine));
                }
            }
            // The database's own row is keyed by its name.
            let row = Row::new(vec![Datum::String(self.name.clone())]);
            changeset.push(delete_keyed(SYSTEM_DATABASES, &row));

            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        } else {
            // Direct catalog update (no Raft)
            let mut catalog = self.catalog.write();
            for (name, _) in &tables {
                let _ = catalog.drop_table(name);
            }
            for view in catalog.get_views_in_database(&self.name) {
                let _ = catalog.drop_view(
                    &crate::catalog::TableName::new(self.name.clone(), view),
                    true,
                );
            }
            catalog
                .drop_database(&self.name)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
        }

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(tables.len() as i64)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
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
    name: crate::catalog::TableName,
    /// The new table's ID, once it has been created.
    table_id: super::encoding::TableId,
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

/// Parameters for creating a CreateTableAs executor.
pub struct CreateTableAsParams {
    pub name: crate::catalog::TableName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<Constraint>,
    pub if_not_exists: bool,
    pub catalog: Arc<RwLock<Catalog>>,
    pub raft_node: Option<Arc<RaftNode>>,
    pub source: Box<dyn Executor>,
    pub txn_context: Option<TransactionContext>,
}

impl CreateTableAs {
    pub fn new(params: CreateTableAsParams) -> Self {
        CreateTableAs {
            name: params.name,
            table_id: super::encoding::TableId(0),
            columns: params.columns,
            constraints: params.constraints,
            if_not_exists: params.if_not_exists,
            catalog: params.catalog,
            raft_node: params.raft_node,
            source: params.source,
            txn_context: params.txn_context,
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
            // Raft path: the new table's ID, then its record
            let id = raft_node
                .alloc_ids(1)
                .await
                .map(super::encoding::TableId)
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
            self.table_id = id;
            let table_def = table_def.with_id(id);
            self.catalog
                .read()
                .validate_table(&table_def)
                .map_err(|e| ExecutorError::Internal(e.to_string()))?;
            let record = crate::catalog::store::TableRecord {
                def: table_def,
                indexes: Vec::new(),
            };
            let mut changeset = ChangeSet::new(0);
            changeset.push(crate::catalog::store::create_table(&record));

            raft_node
                .propose_changes(changeset)
                .await
                .map_err(|e| ExecutorError::Internal(format!("Raft error: {}", e)))?;
        } else {
            // No Raft: update catalog directly
            self.table_id = super::encoding::local_table_id();
            let table_def = table_def.with_id(self.table_id);
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
        let mut next_local = allocate_row_id_batch(batch_size);
        let mut batch_remaining = batch_size;

        while let Some(row) = self.source.next().await? {
            if batch_remaining == 0 {
                next_local = allocate_row_id_batch(batch_size);
                batch_remaining = batch_size;
            }

            let row_id = next_local;
            next_local += 1;
            batch_remaining -= 1;

            // Encode storage key (PK-based or row-ID-based)
            let key = if !pk_indices.is_empty() {
                let pk_values: Vec<_> = pk_indices
                    .iter()
                    .filter_map(|&idx| row.get(idx).ok().cloned())
                    .collect();
                encode_pk_key(self.table_id, &pk_values)
            } else {
                encode_row_key(self.table_id, row_id)
            };
            let value = encode_row(&row);

            // Buffer the row change for Raft replication
            if let Some(ref mut ctx) = self.txn_context {
                ctx.add_change(RowChange::insert(key.clone(), value.clone()));
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

// ============ Materialize (derived table) ============

/// Materialize executor — eagerly materializes all rows from input on open(),
/// then streams them from memory on subsequent next() calls.
/// Used for derived tables (subquery in FROM clause).
pub struct Materialize {
    input: Box<dyn Executor>,
    rows: Vec<Row>,
    position: usize,
}

impl Materialize {
    pub fn new(input: Box<dyn Executor>) -> Self {
        Materialize {
            input,
            rows: Vec::new(),
            position: 0,
        }
    }
}

#[async_trait]
impl Executor for Materialize {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows.clear();
        self.position = 0;
        self.input.open().await?;
        while let Some(row) = self.input.next().await? {
            self.rows.push(row);
        }
        self.input.close().await?;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.position].clone();
        self.position += 1;
        Ok(Some(row))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.rows.clear();
        self.position = 0;
        Ok(())
    }
}

// ============ CREATE VIEW / DROP VIEW ============

/// CREATE VIEW executor
// CreateView and DropView executors removed — views are now handled
// at the protocol level with Raft persistence (handle_create_view_raft/handle_drop_view_raft).

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

        let mut create =
            CreateTable::new("users".into(), columns, constraints, false, catalog.clone());
        create.open().await.unwrap();

        let result = create.next().await.unwrap();
        assert!(result.is_some());

        create.close().await.unwrap();

        // Verify table exists
        let cat = catalog.read();
        assert!(cat.get_table(&"users".into()).is_some());
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Create table first time
        let columns = vec![ColumnDef::new("id", DataType::Int)];
        let mut create1 = CreateTable::new(
            "users".into(),
            columns.clone(),
            vec![],
            false,
            catalog.clone(),
        );
        create1.open().await.unwrap();
        create1.next().await.unwrap();

        // Try to create again with IF NOT EXISTS
        let mut create2 = CreateTable::new(
            "users".into(),
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
        let mut drop = DropTable::new("users".into(), false, catalog.clone());
        drop.open().await.unwrap();
        drop.next().await.unwrap();

        // Verify table is gone
        let cat = catalog.read();
        assert!(cat.get_table(&"users".into()).is_none());
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));

        // Try to drop non-existent table with IF EXISTS
        let mut drop = DropTable::new("nonexistent".into(), true, catalog.clone());
        drop.open().await.unwrap();
        let result = drop.next().await.unwrap();
        assert!(result.is_some()); // Should succeed silently
    }
}
