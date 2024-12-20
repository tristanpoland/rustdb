use std::sync::Arc;
use tokio::sync::RwLock;
use crate::error::Error;
use crate::storage::{Page, PageId, Table};
use crate::buffer_pool::BufferPool;
use crate::types::Value;
use std::collections::HashMap;

/// Table scanner for efficient sequential access
pub struct TableScanner {
    table: Arc<Table>,
    buffer_pool: Arc<BufferPool>,
    current_page: PageId,
    current_slot: u16,
    prefetch_distance: usize,
    predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
}

/// Configuration for scanner behavior
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    pub prefetch_distance: usize,
    pub buffer_hint: bool,
    pub sequential_hint: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            prefetch_distance: 2,  // Number of pages to prefetch
            buffer_hint: true,     // Hint to buffer pool about usage pattern
            sequential_hint: true,  // Hint about sequential access
        }
    }
}

impl TableScanner {
    /// Create a new table scanner
    pub async fn new(
        table: Arc<Table>,
        buffer_pool: Arc<BufferPool>,
        config: ScannerConfig,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
    ) -> Result<Self, Error> {
        let start_page = table.get_first_page_id().await?;

        let scanner = Self {
            table,
            buffer_pool,
            current_page: start_page,
            current_slot: 0,
            prefetch_distance: config.prefetch_distance,
            predicate,
        };

        // Start prefetching
        scanner.prefetch_pages().await?;

        Ok(scanner)
    }

    /// Get the next row
    pub async fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>, Error> {
        loop {
            // Get current page
            let page = self.buffer_pool.get_page(self.current_page).await?;
            let page = page.read().await;

            // Check if we've reached the end of the current page
            if self.current_slot >= page.get_slot_count() {
                // Move to next page
                if let Some(next_page) = page.get_next_page() {
                    self.current_page = next_page;
                    self.current_slot = 0;
                    drop(page);
                    
                    // Start prefetching next set of pages
                    self.prefetch_pages().await?;
                    continue;
                } else {
                    return Ok(None); // End of table
                }
            }

            // Read row from current slot
            if let Some(row_data) = page.read_slot(self.current_slot)? {
                let row_id = self.make_row_id(self.current_page, self.current_slot);
                self.current_slot += 1;

                // Apply predicate if any
                if let Some(ref predicate) = self.predicate {
                    if !predicate(&row_data)? {
                        continue;
                    }
                }

                return Ok(Some((row_id, row_data)));
            }

            // Skip deleted rows
            self.current_slot += 1;
        }
    }

    /// Get the next row as a HashMap of column values
    pub async fn next_row(&mut self) -> Result<Option<(u64, HashMap<String, Value>)>, Error> {
        if let Some((row_id, row_data)) = self.next().await? {
            let row: HashMap<String, Value> = bincode::deserialize(&row_data)?;
            Ok(Some((row_id, row)))
        } else {
            Ok(None)
        }
    }

    /// Skip a number of rows
    pub async fn skip(&mut self, count: usize) -> Result<(), Error> {
        for _ in 0..count {
            if self.next().await?.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Reset scanner to beginning of table
    pub async fn reset(&mut self) -> Result<(), Error> {
        self.current_page = self.table.get_first_page_id().await?;
        self.current_slot = 0;
        self.prefetch_pages().await?;
        Ok(())
    }

    /// Get estimated number of remaining rows
    pub async fn estimate_remaining(&self) -> Result<u64, Error> {
        let stats = self.table.get_stats().await?;
        let total_rows = stats.row_count;
        let current_pos = self.get_current_position().await?;
        
        Ok(total_rows.saturating_sub(current_pos))
    }

    /// Get current position in the table
    pub async fn get_current_position(&self) -> Result<u64, Error> {
        let page_size = self.table.get_page_size();
        let page_num = self.current_page.to_u64();
        Ok(page_num * page_size as u64 + self.current_slot as u64)
    }

    // Helper methods

    async fn prefetch_pages(&self) -> Result<(), Error> {
        let mut current = self.current_page;
        
        for _ in 0..self.prefetch_distance {
            let page = self.buffer_pool.get_page(current).await?;
            let page = page.read().await;
            
            if let Some(next_page) = page.get_next_page() {
                // Prefetch next page
                self.buffer_pool.prefetch_page(next_page).await?;
                current = next_page;
            } else {
                break;
            }
        }
        
        Ok(())
    }

    fn make_row_id(&self, page_id: PageId, slot: u16) -> u64 {
        ((page_id.to_u64() as u64) << 16) | (slot as u64)
    }
}

/// Iterator implementation for easier usage
impl TableScanner {
    pub async fn iter<'a>(
        &'a mut self
    ) -> impl Iterator<Item = Result<(u64, Vec<u8>), Error>> + 'a {
        std::iter::from_fn(move || {
            match futures::executor::block_on(self.next()) {
                Ok(Some(row)) => Some(Ok(row)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    pub async fn iter_rows<'a>(
        &'a mut self
    ) -> impl Iterator<Item = Result<(u64, HashMap<String, Value>), Error>> + 'a {
        std::iter::from_fn(move || {
            match futures::executor::block_on(self.next_row()) {
                Ok(Some(row)) => Some(Ok(row)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_table() -> Result<(Arc<Table>, Arc<BufferPool>), Error> {
        let dir = tempdir()?;
        let buffer_pool = Arc::new(BufferPool::new(1000));
        
        // Create table with test data
        let table = Arc::new(Table::create(
            dir.path().join("test.db"),
            "test_table".to_string(),
            vec![
                ("id".to_string(), Type::Integer),
                ("name".to_string(), Type::String),
            ],
            Arc::clone(&buffer_pool),
        ).await?);

        // Insert test rows
        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), Value::Integer(i));
            row.insert("name".to_string(), Value::String(format!("name{}", i)));
            
            let row_data = bincode::serialize(&row)?;
            table.insert_row(row_data).await?;
        }

        Ok((table, buffer_pool))
    }

    #[tokio::test]
    async fn test_basic_scan() -> Result<(), Error> {
        let (table, buffer_pool) = create_test_table().await?;
        let config = ScannerConfig::default();
        
        let mut scanner = TableScanner::new(
            Arc::clone(&table),
            Arc::clone(&buffer_pool),
            config,
            None,
        ).await?;

        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }

        assert_eq!(count, 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_scan() -> Result<(), Error> {
        let (table, buffer_pool) = create_test_table().await?;
        let config = ScannerConfig::default();
        
        // Scan only even IDs
        let predicate = Box::new(|row_data: &[u8]| {
            let row: HashMap<String, Value> = bincode::deserialize(row_data)?;
            if let Value::Integer(id) = row.get("id").unwrap() {
                Ok(id % 2 == 0)
            } else {
                Ok(false)
            }
        });

        let mut scanner = TableScanner::new(
            Arc::clone(&table),
            Arc::clone(&buffer_pool),
            config,
            Some(predicate),
        ).await?;

        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }

        assert_eq!(count, 50); // Only even IDs
        Ok(())
    }

    #[tokio::test]
    async fn test_iterator_usage() -> Result<(), Error> {
        let (table, buffer_pool) = create_test_table().await?;
        let config = ScannerConfig::default();
        
        let mut scanner = TableScanner::new(
            Arc::clone(&table),
            Arc::clone(&buffer_pool),
            config,
            None,
        ).await?;

        let count = scanner.iter().await
            .filter_map(Result::ok)
            .count();

        assert_eq!(count, 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_skip_and_reset() -> Result<(), Error> {
        let (table, buffer_pool) = create_test_table().await?;
        let config = ScannerConfig::default();
        
        let mut scanner = TableScanner::new(
            Arc::clone(&table),
            Arc::clone(&buffer_pool),
            config,
            None,
        ).await?;

        // Skip first 50 rows
        scanner.skip(50).await?;
        
        let mut count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }
        assert_eq!(count, 50);

        // Reset and read all
        scanner.reset().await?;
        count = 0;
        while let Some(_) = scanner.next().await? {
            count += 1;
        }
        assert_eq!(count, 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_scans() -> Result<(), Error> {
        use tokio::task;
        
        let (table, buffer_pool) = create_test_table().await?;
        let config = ScannerConfig::default();
        let mut handles = vec![];

        // Start multiple concurrent scans
        for _ in 0..5 {
            let table = Arc::clone(&table);
            let buffer_pool = Arc::clone(&buffer_pool);
            let config = config.clone();

            handles.push(task::spawn(async move {
                let mut scanner = TableScanner::new(
                    table,
                    buffer_pool,
                    config,
                    None,
                ).await?;

                let mut count = 0;
                while let Some(_) = scanner.next().await? {
                    count += 1;
                }
                
                Result::<_, Error>::Ok(count)
            }));
        }

        for handle in handles {
            assert_eq!(handle.await?, 100);
        }

        Ok(())
    }
}