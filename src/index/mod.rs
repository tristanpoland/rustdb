use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::error::Error;
use crate::storage::{Page, PageId, Storage};
use crate::types::{Type, Value, TypeSystem};
use serde::{Serialize, Deserialize};

mod btree;
use btree::{BTree, BTreeConfig};

/// Index types supported by RustDB
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    // Future: LSM, Skip List, etc.
}

/// Index configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
    pub nullable: bool,
}

/// Main index structure that manages different index types
pub struct Index {
    config: IndexConfig,
    btree: Option<Arc<RwLock<BTree>>>,
    type_system: Arc<TypeSystem>,
}

impl Index {
    /// Create a new index with the given configuration
    pub async fn create(
        config: IndexConfig,
        storage: Arc<Storage>,
        type_system: Arc<TypeSystem>,
    ) -> Result<Self, Error> {
        let mut index = Self {
            config: config.clone(),
            btree: None,
            type_system,
        };

        // Initialize the appropriate index structure
        match config.index_type {
            IndexType::BTree => {
                let btree_config = BTreeConfig {
                    name: config.name.clone(),
                    unique: config.unique,
                    nullable: config.nullable,
                };
                let btree = BTree::create(btree_config, Arc::clone(&storage)).await?;
                index.btree = Some(Arc::new(RwLock::new(btree)));
            }
            IndexType::Hash => {
                // TODO: Implement hash index
                return Err(Error::Storage("Hash index not implemented yet".into()));
            }
        }

        Ok(index)
    }

    /// Open an existing index
    pub async fn open(
        config: IndexConfig,
        storage: Arc<Storage>,
        type_system: Arc<TypeSystem>,
    ) -> Result<Self, Error> {
        let mut index = Self {
            config: config.clone(),
            btree: None,
            type_system,
        };

        match config.index_type {
            IndexType::BTree => {
                let btree = BTree::open(&config.name, Arc::clone(&storage)).await?;
                index.btree = Some(Arc::new(RwLock::new(btree)));
            }
            IndexType::Hash => {
                return Err(Error::Storage("Hash index not implemented yet".into()));
            }
        }

        Ok(index)
    }

    /// Insert a key-value pair into the index
    pub async fn insert(&self, key: IndexKey, row_id: u64) -> Result<(), Error> {
        match self.config.index_type {
            IndexType::BTree => {
                if let Some(btree) = &self.btree {
                    let mut btree = btree.write().await;
                    btree.insert(key, row_id).await?;
                }
            }
            IndexType::Hash => {
                return Err(Error::Storage("Hash index not implemented yet".into()));
            }
        }
        Ok(())
    }

    /// Look up a key in the index
    pub async fn lookup(&self, key: &IndexKey) -> Result<Option<u64>, Error> {
        match self.config.index_type {
            IndexType::BTree => {
                if let Some(btree) = &self.btree {
                    let btree = btree.read().await;
                    btree.find(key).await
                } else {
                    Ok(None)
                }
            }
            IndexType::Hash => {
                Err(Error::Storage("Hash index not implemented yet".into()))
            }
        }
    }

    /// Range scan the index
    pub async fn range_scan(
        &self,
        start: &IndexKey,
        end: &IndexKey,
    ) -> Result<Vec<(IndexKey, u64)>, Error> {
        match self.config.index_type {
            IndexType::BTree => {
                if let Some(btree) = &self.btree {
                    let btree = btree.read().await;
                    btree.range(start, end).await
                } else {
                    Ok(vec![])
                }
            }
            IndexType::Hash => {
                Err(Error::Storage("Hash index does not support range scans".into()))
            }
        }
    }

    /// Delete a key from the index
    pub async fn delete(&self, key: &IndexKey) -> Result<(), Error> {
        match self.config.index_type {
            IndexType::BTree => {
                if let Some(btree) = &self.btree {
                    let mut btree = btree.write().await;
                    btree.delete(key).await?;
                }
            }
            IndexType::Hash => {
                return Err(Error::Storage("Hash index not implemented yet".into()));
            }
        }
        Ok(())
    }

    /// Check if a key exists in the index
    pub async fn exists(&self, key: &IndexKey) -> Result<bool, Error> {
        Ok(self.lookup(key).await?.is_some())
    }

    /// Get index statistics
    pub async fn stats(&self) -> Result<IndexStats, Error> {
        match self.config.index_type {
            IndexType::BTree => {
                if let Some(btree) = &self.btree {
                    let btree = btree.read().await;
                    btree.stats().await
                } else {
                    Ok(IndexStats::default())
                }
            }
            IndexType::Hash => {
                Err(Error::Storage("Hash index not implemented yet".into()))
            }
        }
    }

    /// Build the index from scratch using the table data
    pub async fn build(&self, storage: Arc<Storage>) -> Result<(), Error> {
        let table = storage.get_table(&self.config.table_name).await?;
        let mut scanner = table.scan().await?;

        while let Some((row_id, row)) = scanner.next().await? {
            let key = self.create_key_from_row(&row)?;
            self.insert(key, row_id).await?;
        }

        Ok(())
    }

    /// Create an index key from a row's values
    fn create_key_from_row(&self, row: &[u8]) -> Result<IndexKey, Error> {
        let mut key_values = Vec::with_capacity(self.config.columns.len());
        
        for column in &self.config.columns {
            let value = self.extract_column_value(row, column)?;
            key_values.push(value);
        }

        Ok(IndexKey::new(key_values))
    }

    // Helper method to extract a column value from a row
    fn extract_column_value(&self, row: &[u8], column: &str) -> Result<Value, Error> {
        // Implement value extraction based on schema and serialization format
        unimplemented!()
    }
}

/// Index statistics
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub num_entries: u64,
    pub height: u32,
    pub num_nodes: u64,
    pub num_pages: u64,
    pub bytes_used: u64,
}

/// Composite index key that supports multiple columns
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexKey {
    values: Vec<Value>,
}

impl IndexKey {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        bincode::serialize(&self.values)
            .map_err(|e| Error::Storage(format!("Failed to serialize index key: {}", e)))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let values = bincode::deserialize(bytes)
            .map_err(|e| Error::Storage(format!("Failed to deserialize index key: {}", e)))?;
        Ok(Self { values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_btree_index() -> Result<(), Error> {
        let dir = tempdir()?;
        let storage = Arc::new(Storage::new(dir.path())?);
        let type_system = Arc::new(TypeSystem::new());

        // Create index configuration
        let config = IndexConfig {
            name: "test_index".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: true,
            nullable: false,
        };

        // Create index
        let index = Index::create(config, Arc::clone(&storage), Arc::clone(&type_system)).await?;

        // Insert some data
        let key1 = IndexKey::new(vec![Value::Int32(1)]);
        let key2 = IndexKey::new(vec![Value::Int32(2)]);
        
        index.insert(key1.clone(), 1).await?;
        index.insert(key2.clone(), 2).await?;

        // Test lookup
        assert_eq!(index.lookup(&key1).await?, Some(1));
        assert_eq!(index.lookup(&key2).await?, Some(2));

        // Test range scan
        let results = index.range_scan(&key1, &key2).await?;
        assert_eq!(results.len(), 2);

        // Test delete
        index.delete(&key1).await?;
        assert_eq!(index.lookup(&key1).await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_constraint() -> Result<(), Error> {
        let dir = tempdir()?;
        let storage = Arc::new(Storage::new(dir.path())?);
        let type_system = Arc::new(TypeSystem::new());

        let config = IndexConfig {
            name: "unique_index".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: true,
            nullable: false,
        };

        let index = Index::create(config, Arc::clone(&storage), Arc::clone(&type_system)).await?;

        // Insert first value
        let key = IndexKey::new(vec![Value::Int32(1)]);
        index.insert(key.clone(), 1).await?;

        // Try to insert duplicate
        let result = index.insert(key.clone(), 2).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_composite_key() -> Result<(), Error> {
        let dir = tempdir()?;
        let storage = Arc::new(Storage::new(dir.path())?);
        let type_system = Arc::new(TypeSystem::new());

        let config = IndexConfig {
            name: "composite_index".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["first".to_string(), "last".to_string()],
            index_type: IndexType::BTree,
            unique: true,
            nullable: false,
        };

        let index = Index::create(config, Arc::clone(&storage), Arc::clone(&type_system)).await?;

        // Insert composite keys
        let key1 = IndexKey::new(vec![
            Value::String("John".into()),
            Value::String("Doe".into()),
        ]);
        let key2 = IndexKey::new(vec![
            Value::String("Jane".into()),
            Value::String("Doe".into()),
        ]);

        index.insert(key1.clone(), 1).await?;
        index.insert(key2.clone(), 2).await?;

        // Test range scan on partial key
        let start = IndexKey::new(vec![Value::String("J".into())]);
        let end = IndexKey::new(vec![Value::String("K".into())]);
        
        let results = index.range_scan(&start, &end).await?;
        assert_eq!(results.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> Result<(), Error> {
        use tokio::task;

        let dir = tempdir()?;
        let storage = Arc::new(Storage::new(dir.path())?);
        let type_system = Arc::new(TypeSystem::new());

        let config = IndexConfig {
            name: "concurrent_index".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: true,
            nullable: false,
        };

        let index = Arc::new(Index::create(config, Arc::clone(&storage), Arc::clone(&type_system)).await?);

        let mut handles = vec![];

        // Spawn multiple tasks accessing the index
        for i in 0..10 {
            let index = Arc::clone(&index);
            handles.push(task::spawn(async move {
                let key = IndexKey::new(vec![Value::Int32(i)]);
                index.insert(key.clone(), i as u64).await?;
                index.lookup(&key).await
            }));
        }

        // Wait for all tasks and check results
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await??;
            assert_eq!(result, Some(i as u64));
        }

        Ok(())
    }
}