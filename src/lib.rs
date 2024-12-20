pub mod error;
pub mod types;
pub mod storage;
pub mod query;
pub mod index;
pub mod buffer;

use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Database {
    storage: Arc<storage::Storage>,
    type_system: Arc<types::TypeSystem>,
    query_engine: Arc<query::QueryEngine>,
}

impl Database {
    pub async fn new(path: &str) -> Result<Self, error::Error> {
        let storage = Arc::new(storage::Storage::new(path)?);
        let type_system = Arc::new(types::TypeSystem::new());
        let query_engine = Arc::new(query::QueryEngine::new(
            Arc::clone(&storage),
            Arc::clone(&type_system),
        ));
        
        Ok(Self {
            storage,
            type_system,
            query_engine,
        })
    }

    /// Execute a query string and return results
    pub async fn execute(&self, query: &str) -> Result<query::QueryResult, error::Error> {
        let parsed = self.query_engine.parse(query)?;
        let plan = self.query_engine.plan(parsed)?;
        self.query_engine.execute(plan).await
    }

    /// Create a new table with the given schema
    pub async fn create_table(&self, name: &str, schema: storage::TableSchema) -> Result<(), error::Error> {
        self.storage.create_table(name, schema).await
    }

    /// Drop an existing table
    pub async fn drop_table(&self, name: &str) -> Result<(), error::Error> {
        self.storage.drop_table(name).await
    }

    /// Create an index on the specified table and columns
    pub async fn create_index(
        &self,
        table: &str,
        name: &str,
        columns: Vec<String>,
    ) -> Result<(), error::Error> {
        self.storage.create_index(table, name, columns).await
    }

    /// Begin a new transaction
    pub async fn begin_transaction(&self) -> Result<Transaction, error::Error> {
        Ok(Transaction::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.query_engine),
        ))
    }
}

pub struct Transaction {
    storage: Arc<storage::Storage>,
    query_engine: Arc<query::QueryEngine>,
    changes: Vec<TransactionChange>,
}

#[derive(Debug)]
enum TransactionChange {
    Insert { table: String, row: storage::Row },
    Update { table: String, row: storage::Row, old_row: storage::Row },
    Delete { table: String, row: storage::Row },
}

impl Transaction {
    fn new(storage: Arc<storage::Storage>, query_engine: Arc<query::QueryEngine>) -> Self {
        Self {
            storage,
            query_engine,
            changes: Vec::new(),
        }
    }

    pub async fn execute(&mut self, query: &str) -> Result<query::QueryResult, error::Error> {
        let parsed = self.query_engine.parse(query)?;
        let plan = self.query_engine.plan(parsed)?;
        let result = self.query_engine.execute_in_transaction(plan, self).await?;
        Ok(result)
    }

    pub async fn commit(self) -> Result<(), error::Error> {
        // Apply all changes in order
        for change in self.changes {
            match change {
                TransactionChange::Insert { table, row } => {
                    self.storage.insert_row(&table, row).await?;
                }
                TransactionChange::Update { table, row, old_row } => {
                    self.storage.update_row(&table, old_row, row).await?;
                }
                TransactionChange::Delete { table, row } => {
                    self.storage.delete_row(&table, row).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), error::Error> {
        // Nothing to do as changes haven't been applied yet
        Ok(())
    }
}