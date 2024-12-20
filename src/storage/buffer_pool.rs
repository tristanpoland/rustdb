use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BufferPool {
    pages: RwLock<HashMap<u64, Arc<RwLock<Page>>>>,
    max_pages: usize,
}

impl BufferPool {
    pub fn new(max_pages: usize) -> Self {
        Self {
            pages: RwLock::new(HashMap::new()),
            max_pages,
        }
    }

    pub async fn get_page(&self, file: &File, page_id: u64) -> std::io::Result<Arc<RwLock<Page>>> {
        let mut pages = self.pages.write();
        if let Some(page) = pages.get(&page_id) {
            return Ok(Arc::clone(page));
        }

        if pages.len() >= self.max_pages {
            self.evict_page().await?;
        }

        let page = Arc::new(RwLock::new(Page::new(file, page_id)?));
        pages.insert(page_id, Arc::clone(&page));
        Ok(page)
    }

    async fn evict_page(&self) -> std::io::Result<()> {
        // Implement LRU or other eviction strategy
        unimplemented!()
    }
}