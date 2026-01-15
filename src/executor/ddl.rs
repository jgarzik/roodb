//! DDL executors
//!
//! Implements CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::catalog::{Catalog, ColumnDef, Constraint, IndexDef, TableDef};

use super::datum::Datum;
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
    /// Whether execution is complete
    done: bool,
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

        let mut catalog = self.catalog.write();

        // Check if table exists
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

        // Build table definition
        let mut table_def = TableDef::new(&self.name);
        for col in &self.columns {
            table_def = table_def.column(col.clone());
        }
        for constraint in &self.constraints {
            table_def = table_def.constraint(constraint.clone());
        }

        catalog
            .create_table(table_def)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
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
    /// Whether execution is complete
    done: bool,
}

impl DropTable {
    /// Create a new DROP TABLE executor
    pub fn new(name: String, if_exists: bool, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropTable {
            name,
            if_exists,
            catalog,
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

        let mut catalog = self.catalog.write();

        // Check if table exists
        if catalog.get_table(&self.name).is_none() {
            if self.if_exists {
                self.done = true;
                return Ok(Some(Row::new(vec![Datum::Int(0)])));
            }
            return Err(ExecutorError::TableNotFound(self.name.clone()));
        }

        catalog
            .drop_table(&self.name)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

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
    table: String,
    /// Column names and indices
    columns: Vec<(String, usize)>,
    /// Whether index is unique
    unique: bool,
    /// Catalog reference
    catalog: Arc<RwLock<Catalog>>,
    /// Whether execution is complete
    done: bool,
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

        let mut catalog = self.catalog.write();

        // Verify table exists
        if catalog.get_table(&self.table).is_none() {
            return Err(ExecutorError::TableNotFound(self.table.clone()));
        }

        let column_names: Vec<String> = self.columns.iter().map(|(n, _)| n.clone()).collect();

        let mut index_def = IndexDef::new(&self.name, &self.table, column_names);
        if self.unique {
            index_def = index_def.unique();
        }

        catalog
            .create_index(index_def)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

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
    /// Whether execution is complete
    done: bool,
}

impl DropIndex {
    /// Create a new DROP INDEX executor
    pub fn new(name: String, catalog: Arc<RwLock<Catalog>>) -> Self {
        DropIndex {
            name,
            catalog,
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

        let mut catalog = self.catalog.write();

        catalog
            .drop_index(&self.name)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

        self.done = true;
        Ok(Some(Row::new(vec![Datum::Int(0)])))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
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
