use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::error::Error;
use crate::types::{Type, Value, TypeSystem};
use crate::storage::{Page, PageId};
use crate::buffer_pool::BufferPool;
use crate::index::{Index, IndexConfig};

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexConfig>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<Value>,
}

/// Table structure for managing data pages and metadata
pub struct Table {
    schema: TableSchema,
    root_page_id: PageId,
    file: tokio::fs::File,
    buffer_pool: Arc<BufferPool>,
    indexes: RwLock<HashMap<String, Arc<Index>>>,
    type_system: Arc<TypeSystem>,
    stats: RwLock<TableStats>,
}

#[derive(Debug, Clone, Default)]
pub struct TableStats {
    row_count: u64,
    page_count: u64,
    avg_row_size: u32,
    free_space: u64,
}

impl Table {
    /// Create a new table with the given schema
    pub async fn create(
        path: impl AsRef<Path>,
        schema: TableSchema,
        buffer_pool: Arc<BufferPool>,
        type_system: Arc<TypeSystem>,
    ) -> Result<Self, Error> {
        // Validate schema
        Self::validate_schema(&schema, &type_system)?;

        // Create table file
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

        // Initialize root page
        let root_page_id = PageId::new(0, 0);
        let root_page = Page::new(root_page_id);
        buffer_pool.put_page(root_page_id, root_page).await?;

        let mut table = Self {
            schema,
            root_page_id,
            file,
            buffer_pool,
            indexes: RwLock::new(HashMap::new()),
            type_system,
            stats: RwLock::new(TableStats::default()),
        };

        // Create initial indexes
        table.create_indexes().await?;

        Ok(table)
    }

    /// Open an existing table
    pub async fn open(
        path: impl AsRef<Path>,
        schema: TableSchema,
        root_page_id: PageId,
        buffer_pool: Arc<BufferPool>,
        type_system: Arc<TypeSystem>,
    ) -> Result<Self, Error> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await?;

        let mut table = Self {
            schema,
            root_page_id,
            file,
            buffer_pool,
            indexes: RwLock::new(HashMap::new()),
            type_system,
            stats: RwLock::new(TableStats::default()),
        };

        // Load existing indexes
        table.load_indexes().await?;
        
        Ok(table)
    }

    /// Insert a row into the table
    pub async fn insert(&self, values: HashMap<String, Value>) -> Result<u64, Error> {
        // Validate values against schema
        self.validate_row_values(&values)?;

        // Serialize row data
        let row_data = self.serialize_row(&values)?;

        // Find a page with enough space
        let page_id = self.find_page_for_row(row_data.len()).await?;
        let page = self.buffer_pool.get_page(page_id).await?;
        
        // Insert row and get row ID
        let mut page = page.write().await;
        let slot_id = page.insert_record(&row_data)?;
        let row_id = self.make_row_id(page_id, slot_id);

        // Update indexes
        let indexes = self.indexes.read().await;
        for index in indexes.values() {
            let key = self.create_index_key_for_row(&values, &index.config().columns)?;
            index.insert(key, row_id).await?;
        }

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.row_count += 1;
        
        Ok(row_id)
    }

    /// Find a row by its primary key
    pub async fn find_by_pk(&self, pk_values: &[Value]) -> Result<Option<HashMap<String, Value>>, Error> {
        // Get primary key index
        let indexes = self.indexes.read().await;
        let pk_index = indexes.get("PRIMARY")
            .ok_or_else(|| Error::Storage("Primary key index not found".into()))?;

        // Create index key from PK values
        let key = self.create_index_key(pk_values, &self.schema.primary_key)?;

        // Look up row ID in index
        if let Some(row_id) = pk_index.lookup(&key).await? {
            self.read_row(row_id).await
        } else {
            Ok(None)
        }
    }

    /// Update a row by its primary key
    pub async fn update(
        &self,
        pk_values: &[Value],
        new_values: HashMap<String, Value>,
    ) -> Result<bool, Error> {
        // Find the row first
        let old_values = match self.find_by_pk(pk_values).await? {
            Some(values) => values,
            None => return Ok(false),
        };

        // Validate new values
        self.validate_row_values(&new_values)?;

        // Merge old and new values
        let mut updated_values = old_values.clone();
        for (key, value) in new_values {
            updated_values.insert(key, value);
        }

        // Get row location
        let row_id = self.find_row_id(&old_values).await?;
        let (page_id, slot_id) = self.split_row_id(row_id);

        // Update row data
        let row_data = self.serialize_row(&updated_values)?;
        let page = self.buffer_pool.get_page(page_id).await?;
        let mut page = page.write().await;
        page.update_record(slot_id, &row_data)?;

        // Update indexes
        let indexes = self.indexes.read().await;
        for index in indexes.values() {
            let old_key = self.create_index_key_for_row(&old_values, &index.config().columns)?;
            let new_key = self.create_index_key_for_row(&updated_values, &index.config().columns)?;
            
            index.delete(&old_key).await?;
            index.insert(new_key, row_id).await?;
        }

        Ok(true)
    }

    /// Delete a row by its primary key
    pub async fn delete(&self, pk_values: &[Value]) -> Result<bool, Error> {
        // Find the row first
        let values = match self.find_by_pk(pk_values).await? {
            Some(values) => values,
            None => return Ok(false),
        };

        // Get row location
        let row_id = self.find_row_id(&values).await?;
        let (page_id, slot_id) = self.split_row_id(row_id);

        // Delete from indexes first
        let indexes = self.indexes.read().await;
        for index in indexes.values() {
            let key = self.create_index_key_for_row(&values, &index.config().columns)?;
            index.delete(&key).await?;
        }

        // Delete row data
        let page = self.buffer_pool.get_page(page_id).await?;
        let mut page = page.write().await;
        page.delete_record(slot_id)?;

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.row_count -= 1;

        Ok(true)
    }

    /// Scan the table with an optional predicate
    pub async fn scan<F>(&self, predicate: Option<F>) -> Result<TableScanner, Error>
    where
        F: Fn(&HashMap<String, Value>) -> Result<bool, Error> + Send + 'static,
    {
        Ok(TableScanner {
            table: self,
            current_page: self.root_page_id,
            current_slot: 0,
            predicate: predicate.map(Box::new),
        })
    }

    // Helper methods

    fn validate_schema(schema: &TableSchema, type_system: &TypeSystem) -> Result<(), Error> {
        // Validate column types
        for column in &schema.columns {
            if !type_system.type_exists(&column.type_name) {
                return Err(Error::Type(format!("Unknown type: {}", column.type_name)));
            }
        }

        // Validate primary key
        for pk_col in &schema.primary_key {
            if !schema.columns.iter().any(|c| &c.name == pk_col) {
                return Err(Error::Storage(format!("Primary key column not found: {}", pk_col)));
            }
        }

        // Validate index columns
        for index in &schema.indexes {
            for col in &index.columns {
                if !schema.columns.iter().any(|c| &c.name == col) {
                    return Err(Error::Storage(format!("Index column not found: {}", col)));
                }
            }
        }

        Ok(())
    }

    fn validate_row_values(&self, values: &HashMap<String, Value>) -> Result<(), Error> {
        // Check all required columns are present
        for column in &self.schema.columns {
            match values.get(&column.name) {
                Some(value) => {
                    // Validate value type
                    let type_def = self.type_system.get_type(&column.type_name)
                        .ok_or_else(|| Error::Type(format!("Unknown type: {}", column.type_name)))?;
                    self.type_system.validate_value(value, &type_def)?;
                }
                None if !column.nullable => {
                    return Err(Error::Storage(format!("Missing required column: {}", column.name)));
                }
                None => continue,
            }
        }
        Ok(())
    }

    async fn find_page_for_row(&self, row_size: usize) -> Result<PageId, Error> {
        // First try last page
        let last_page_id = {
            let stats = self.stats.read().await;
            PageId::new(0, stats.page_count - 1)
        };

        let page = self.buffer_pool.get_page(last_page_id).await?;
        let page = page.read().await;
        if page.free_space() >= row_size {
            return Ok(last_page_id);
        }

        // Create new page
        let new_page_id = {
            let mut stats = self.stats.write().await;
            let page_id = PageId::new(0, stats.page_count);
            stats.page_count += 1;
            page_id
        };

        let new_page = Page::new(new_page_id);
        self.buffer_pool.put_page(new_page_id, new_page).await?;

        Ok(new_page_id)
    }

    fn serialize_row(&self, values: &HashMap<String, Value>) -> Result<Vec<u8>, Error> {
        bincode::serialize(values)
            .map_err(|e| Error::Storage(format!("Failed to serialize row: {}", e)))
    }

    fn deserialize_row(&self, data: &[u8]) -> Result<HashMap<String, Value>, Error> {
        bincode::deserialize(data)
            .map_err(|e| Error::Storage(format!("Failed to deserialize row: {}", e)))
    }

    fn make_row_id(&self, page_id: PageId, slot_id: u16) -> u64 {
        ((page_id.file_id as u64) << 48) | ((page_id.page_num as u64) << 16) | (slot_id as u64)
    }

    fn split_row_id(&self, row_id: u64) -> (PageId, u16) {
        let file_id = (row_id >> 48) as u32;
        let page_num = ((row_id >> 16) & 0xFFFFFFFF) as u32;
        let slot_id = (row_id & 0xFFFF) as u16;
        (PageId::new(file_id, page_num), slot_id)
    }

    async fn create_index_key_for_row(
        &self,
        values: &HashMap<String, Value>,
        columns: &[String],
    ) -> Result<Vec<Value>, Error> {
        let mut key_values = Vec::with_capacity(columns.len());
        for column in columns {
            if let Some(value) = values.get(column) {
                key_values.push(value.clone());
            } else {
                return Err(Error::Storage(format!("Missing index column: {}", column)));
            }
        }
        Ok(key_values)
    }
}

pub struct TableScanner<'a> {
    table: &'a Table,
    current_page: PageId,
    current_slot: u16,
    predicate: Option<Box<dyn Fn(&HashMap<String, Value>) -> Result<bool, Error> + Send>>,
}

impl<'a> TableScanner<'a> {
    pub async fn next(&mut self) -> Result<Option<(u64, HashMap<String, Value>)>, Error> {
        loop {
            let page = self.table.buffer_pool.get_page(self.current_page).await?;
            let page = page.read().await;

            while self.current_slot < page.slot_count() {
                let row_id = self.table.make_row_id(self.current_page, self.current_slot);
                if let Some(data) = page.read_record(self.current_slot)? {
                    let values = self.table.deserialize_row(&data)?;
                    self.current_slot += 1;

                    // Apply predicate if any
                    if let Some(ref predicate) = self.predicate {
                        if predicate(&values)? {
                            return Ok(Some((row_id, values)));
                        }
                        continue;
                    }
                    return Ok(Some((row_id, values)));
                }
                self.current_slot += 1;
            }

            // Move to next page
            if let Some(next_page) = page.next_page() {
                self.current_page = next_page;
                self.current_slot = 0;
                continue;
            }
            return Ok(None);
        }
    }
}

impl Table {
    /// Create all defined indexes for the table
    async fn create_indexes(&self) -> Result<(), Error> {
        let mut indexes = self.indexes.write().await;
        
        // Create primary key index
        let pk_config = IndexConfig {
            name: "PRIMARY".to_string(),
            columns: self.schema.primary_key.clone(),
            unique: true,
            nullable: false,
        };
        let pk_index = Index::create(pk_config, Arc::clone(&self.type_system)).await?;
        indexes.insert("PRIMARY".to_string(), Arc::new(pk_index));

        // Create secondary indexes
        for index_config in &self.schema.indexes {
            let index = Index::create(index_config.clone(), Arc::clone(&self.type_system)).await?;
            indexes.insert(index_config.name.clone(), Arc::new(index));
        }

        Ok(())
    }

    /// Load existing indexes for the table
    async fn load_indexes(&self) -> Result<(), Error> {
        let mut indexes = self.indexes.write().await;
        
        // Load primary key index
        let pk_config = IndexConfig {
            name: "PRIMARY".to_string(),
            columns: self.schema.primary_key.clone(),
            unique: true,
            nullable: false,
        };
        let pk_index = Index::open(pk_config, Arc::clone(&self.type_system)).await?;
        indexes.insert("PRIMARY".to_string(), Arc::new(pk_index));

        // Load secondary indexes
        for index_config in &self.schema.indexes {
            let index = Index::open(index_config.clone(), Arc::clone(&self.type_system)).await?;
            indexes.insert(index_config.name.clone(), Arc::new(index));
        }

        Ok(())
    }

    /// Rebuild all indexes from table data
    pub async fn rebuild_indexes(&self) -> Result<(), Error> {
        // Clear existing indexes
        {
            let mut indexes = self.indexes.write().await;
            indexes.clear();
        }

        // Recreate indexes
        self.create_indexes().await?;

        // Scan table and rebuild indexes
        let mut scanner = self.scan(None).await?;
        while let Some((row_id, values)) = scanner.next().await? {
            let indexes = self.indexes.read().await;
            for index in indexes.values() {
                let key = self.create_index_key_for_row(&values, &index.config().columns)?;
                index.insert(key, row_id).await?;
            }
        }

        Ok(())
    }

    /// Get table statistics
    pub async fn stats(&self) -> TableStats {
        self.stats.read().await.clone()
    }

    /// Update table statistics
    pub async fn update_stats(&self) -> Result<(), Error> {
        let mut stats = self.stats.write().await;
        let mut row_count = 0;
        let mut total_size = 0;

        let mut scanner = self.scan(None).await?;
        while let Some((_, values)) = scanner.next().await? {
            row_count += 1;
            total_size += self.serialize_row(&values)?.len();
        }

        stats.row_count = row_count;
        stats.avg_row_size = if row_count > 0 {
            (total_size / row_count as usize) as u32
        } else {
            0
        };

        Ok(())
    }

    /// Compact the table by reclaiming space from deleted rows
    pub async fn compact(&self) -> Result<(), Error> {
        let mut current_page = self.root_page_id;
        
        while let Some(page_id) = {
            let page = self.buffer_pool.get_page(current_page).await?;
            let page = page.read().await;
            page.next_page()
        } {
            let page = self.buffer_pool.get_page(page_id).await?;
            let mut page = page.write().await;
            page.compact()?;
            current_page = page_id;
        }

        // Update statistics
        self.update_stats().await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_table() -> Result<(Table, Arc<TypeSystem>), Error> {
        let dir = tempdir()?;
        let buffer_pool = Arc::new(BufferPool::new(1000));
        let type_system = Arc::new(TypeSystem::new());

        // Create schema
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_name: "Int32".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "name".to_string(),
                    type_name: "String".to_string(),
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
        };

        let table = Table::create(
            dir.path().join("test.db"),
            schema,
            buffer_pool,
            Arc::clone(&type_system),
        ).await?;

        Ok((table, type_system))
    }

    #[tokio::test]
    async fn test_basic_operations() -> Result<(), Error> {
        let (table, _) = create_test_table().await?;

        // Insert a row
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Int32(1));
        values.insert("name".to_string(), Value::String("test".to_string()));
        
        let row_id = table.insert(values).await?;

        // Read the row back
        let pk_values = vec![Value::Int32(1)];
        let row = table.find_by_pk(&pk_values).await?.unwrap();
        assert_eq!(row.get("id"), Some(&Value::Int32(1)));
        assert_eq!(row.get("name"), Some(&Value::String("test".to_string())));

        // Update the row
        let mut new_values = HashMap::new();
        new_values.insert("name".to_string(), Value::String("updated".to_string()));
        assert!(table.update(&pk_values, new_values).await?);

        // Verify update
        let row = table.find_by_pk(&pk_values).await?.unwrap();
        assert_eq!(row.get("name"), Some(&Value::String("updated".to_string())));

        // Delete the row
        assert!(table.delete(&pk_values).await?);
        assert!(table.find_by_pk(&pk_values).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_table_scan() -> Result<(), Error> {
        let (table, _) = create_test_table().await?;

        // Insert multiple rows
        for i in 0..10 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Int32(i));
            values.insert("name".to_string(), Value::String(format!("test{}", i)));
            table.insert(values).await?;
        }

        // Scan all rows
        let mut scanner = table.scan(None).await?;
        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }
        assert_eq!(count, 10);

        // Scan with predicate
        let mut scanner = table.scan(Some(|row| {
            Ok(match row.get("id") {
                Some(Value::Int32(id)) => *id < 5,
                _ => false,
            })
        })).await?;

        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }
        assert_eq!(count, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> Result<(), Error> {
        use tokio::task;
        
        let (table, _) = create_test_table().await?;
        let table = Arc::new(table);
        let mut handles = vec![];

        // Spawn multiple insert tasks
        for i in 0..10 {
            let table = Arc::clone(&table);
            handles.push(task::spawn(async move {
                let mut values = HashMap::new();
                values.insert("id".to_string(), Value::Int32(i));
                values.insert("name".to_string(), Value::String(format!("test{}", i)));
                table.insert(values).await
            }));
        }

        // Wait for all inserts
        for handle in handles {
            handle.await??;
        }

        // Verify all rows
        let mut scanner = table.scan(None).await?;
        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }
        assert_eq!(count, 10);

        Ok(())
    }
}