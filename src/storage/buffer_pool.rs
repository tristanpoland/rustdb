use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::io::{self, SeekFrom};
use tokio::sync::{RwLock, Mutex};
use lru::LruCache;
use crate::error::Error;
use super::page::{Page, PAGE_SIZE};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Buffer pool entry containing a page and its metadata
#[derive(Debug)]
struct BufferEntry {
    page: Arc<RwLock<Page>>,
    dirty: bool,
    pin_count: u32,
    last_accessed: std::time::Instant,
}

/// Buffer pool for caching database pages in memory
pub struct BufferPool {
    /// Maximum number of pages the buffer pool can hold
    max_pages: usize,
    
    /// Current cached pages and their metadata
    pages: RwLock<HashMap<PageId, Arc<RwLock<BufferEntry>>>>,
    
    /// LRU cache for page eviction
    lru: Mutex<LruCache<PageId, ()>>,
    
    /// Statistics for buffer pool performance
    stats: RwLock<BufferPoolStats>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PageId {
    pub file_id: u64,  // Unique identifier for each database file
    pub page_num: u64, // Page number within the file
}

impl BufferPool {
    pub fn new(max_pages: usize) -> Self {
        Self {
            max_pages,
            pages: RwLock::new(HashMap::with_capacity(max_pages)),
            lru: Mutex::new(LruCache::new(max_pages)),
            stats: RwLock::new(BufferPoolStats::default()),
        }
    }

    /// Get a page from the buffer pool, reading it from disk if necessary
    pub async fn get_page(&self, file: &File, page_id: PageId) -> Result<Arc<RwLock<Page>>, Error> {
        // Try to get page from cache first
        {
            let pages = self.pages.read().await;
            if let Some(entry) = pages.get(&page_id) {
                let mut entry = entry.write().await;
                entry.pin_count += 1;
                entry.last_accessed = std::time::Instant::now();
                
                // Update stats
                let mut stats = self.stats.write().await;
                stats.hit_count += 1;
                
                return Ok(Arc::clone(&entry.page));
            }
        }

        // Page not in cache, need to load it
        let mut stats = self.stats.write().await;
        stats.miss_count += 1;
        drop(stats);  // Release the lock

        // Load the page from disk
        let page = self.load_page(file, page_id).await?;
        
        // Try to add to cache, potentially evicting other pages
        self.add_to_cache(page_id, page).await
    }

    /// Pin a page in memory, preventing it from being evicted
    pub async fn pin_page(&self, page_id: PageId) -> Result<(), Error> {
        let pages = self.pages.read().await;
        if let Some(entry) = pages.get(&page_id) {
            let mut entry = entry.write().await;
            entry.pin_count += 1;
            entry.last_accessed = std::time::Instant::now();
            Ok(())
        } else {
            Err(Error::Storage(format!("Page not in buffer pool: {:?}", page_id)))
        }
    }

    /// Unpin a previously pinned page
    pub async fn unpin_page(&self, page_id: PageId) -> Result<(), Error> {
        let pages = self.pages.read().await;
        if let Some(entry) = pages.get(&page_id) {
            let mut entry = entry.write().await;
            if entry.pin_count > 0 {
                entry.pin_count -= 1;
                Ok(())
            } else {
                Err(Error::Storage("Page is not pinned".to_string()))
            }
        } else {
            Err(Error::Storage(format!("Page not in buffer pool: {:?}", page_id)))
        }
    }

    /// Mark a page as dirty, requiring it to be written back to disk
    pub async fn mark_dirty(&self, page_id: PageId) -> Result<(), Error> {
        let pages = self.pages.read().await;
        if let Some(entry) = pages.get(&page_id) {
            let mut entry = entry.write().await;
            entry.dirty = true;
            Ok(())
        } else {
            Err(Error::Storage(format!("Page not in buffer pool: {:?}", page_id)))
        }
    }

    /// Flush a specific page to disk if it's dirty
    pub async fn flush_page(&self, page_id: PageId) -> Result<(), Error> {
        let pages = self.pages.read().await;
        if let Some(entry) = pages.get(&page_id) {
            let mut entry = entry.write().await;
            if entry.dirty {
                entry.page.write().await.flush().await?;
                entry.dirty = false;
            }
            Ok(())
        } else {
            Err(Error::Storage(format!("Page not in buffer pool: {:?}", page_id)))
        }
    }

    /// Flush all dirty pages to disk
    pub async fn flush_all(&self) -> Result<(), Error> {
        let pages = self.pages.read().await;
        for (page_id, entry) in pages.iter() {
            let mut entry = entry.write().await;
            if entry.dirty {
                entry.page.write().await.flush().await?;
                entry.dirty = false;
            }
        }
        Ok(())
    }

    /// Get buffer pool statistics
    pub async fn stats(&self) -> BufferPoolStats {
        self.stats.read().await.clone()
    }

    // Private helper methods

    async fn load_page(&self, file: &File, page_id: PageId) -> Result<Page, Error> {
        let mut buffer = vec![0; PAGE_SIZE];
        let offset = page_id.page_num as u64 * PAGE_SIZE as u64;
        
        let mut file = tokio::fs::File::from_std(file.try_clone()?);
        file.seek(SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buffer).await?;
        
        Ok(Page::new(page_id, buffer))
    }

    async fn add_to_cache(&self, page_id: PageId, page: Page) -> Result<Arc<RwLock<Page>>, Error> {
        let mut pages = self.pages.write().await;
        let mut lru = self.lru.lock().await;

        // Evict if necessary
        while pages.len() >= self.max_pages {
            if let Some((evict_id, _)) = lru.pop_lru() {
                if let Some(entry) = pages.get(&evict_id) {
                    let entry = entry.read().await;
                    if entry.pin_count == 0 {
                        if entry.dirty {
                            entry.page.write().await.flush().await?;
                        }
                        pages.remove(&evict_id);
                        
                        // Update stats
                        let mut stats = self.stats.write().await;
                        stats.eviction_count += 1;
                    }
                }
            } else {
                return Err(Error::Storage("No pages available for eviction".to_string()));
            }
        }

        // Create new entry
        let page = Arc::new(RwLock::new(page));
        let entry = Arc::new(RwLock::new(BufferEntry {
            page: Arc::clone(&page),
            dirty: false,
            pin_count: 1,
            last_accessed: std::time::Instant::now(),
        }));

        // Add to cache and LRU
        pages.insert(page_id, Arc::clone(&entry));
        lru.put(page_id, ());

        Ok(page)
    }
}

#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    pub total_pages: usize,
    pub dirty_pages: usize,
    pub pinned_pages: usize,
    pub hit_count: usize,
    pub miss_count: usize,
    pub eviction_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[tokio::test]
    async fn test_basic_operations() -> Result<(), Error> {
        let pool = BufferPool::new(5);
        let file = tempfile()?;

        // Get a page
        let page_id = PageId { file_id: 1, page_num: 0 };
        let page = pool.get_page(&file, page_id).await?;

        // Write some data
        {
            let mut page = page.write().await;
            page.write_at(0, &[1, 2, 3, 4])?;
        }

        // Mark dirty and unpin
        pool.mark_dirty(page_id).await?;
        pool.unpin_page(page_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_page_eviction() -> Result<(), Error> {
        let pool = BufferPool::new(2);
        let file = tempfile()?;

        // Fill buffer pool
        let page1 = PageId { file_id: 1, page_num: 0 };
        let page2 = PageId { file_id: 1, page_num: 1 };
        let page3 = PageId { file_id: 1, page_num: 2 };

        let _ = pool.get_page(&file, page1).await?;
        let _ = pool.get_page(&file, page2).await?;

        // Unpin first page
        pool.unpin_page(page1).await?;

        // Get third page - should evict first page
        let _ = pool.get_page(&file, page3).await?;

        // Verify first page was evicted
        assert!(pool.pin_page(page1).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> Result<(), Error> {
        use tokio::task;
        
        let pool = Arc::new(BufferPool::new(10));
        let file = Arc::new(tempfile()?);
        let mut handles = vec![];

        for i in 0..5 {
            let pool = Arc::clone(&pool);
            let file = Arc::clone(&file);
            
            handles.push(task::spawn(async move {
                let page_id = PageId { file_id: 1, page_num: i };
                let page = pool.get_page(&file, page_id).await?;
                
                {
                    let mut page = page.write().await;
                    page.write_at(0, &[i as u8])?;
                }
                
                pool.mark_dirty(page_id).await?;
                pool.unpin_page(page_id).await?;
                Result::<_, Error>::Ok(())
            }));
        }

        for handle in handles {
            handle.await??;
        }

        Ok(())
    }
}