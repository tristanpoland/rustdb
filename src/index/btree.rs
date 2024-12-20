use std::fs::File;
use std::sync::Arc;
use crate::error::Error;
use crate::storage::buffer_pool::{BufferPool, PageId};
use crate::storage::page::Page;

const B: usize = 6;  // B-tree order
const MIN_KEYS: usize = B - 1;
const MAX_KEYS: usize = 2 * B - 1;

/// B-tree node stored on disk
#[derive(Debug)]
struct Node {
    page_id: PageId,
    keys: Vec<Vec<u8>>,      // Serialized key values
    values: Vec<u64>,        // Row IDs
    children: Vec<PageId>,   // Child page IDs
    is_leaf: bool,
}

/// B-tree index implementation
pub struct BTree {
    root_page_id: PageId,
    buffer_pool: Arc<BufferPool>,
    config: BTreeConfig,
}

#[derive(Clone)]
pub struct BTreeConfig {
    pub name: String,
    pub unique: bool,
    pub nullable: bool,
}

impl Node {
    /// Create a new node
    fn new(page_id: PageId, is_leaf: bool) -> Self {
        Self {
            page_id,
            keys: Vec::with_capacity(MAX_KEYS),
            values: Vec::with_capacity(MAX_KEYS),
            children: if is_leaf {
                Vec::new()
            } else {
                Vec::with_capacity(MAX_KEYS + 1)
            },
            is_leaf,
        }
    }

    /// Load a node from a page
    fn from_page(page: &Page) -> Result<Self, Error> {
        let data = page.get_data();
        let mut pos = 0;

        // Read header
        let page_id = PageId::from_bytes(&data[pos..pos + 8])?;
        pos += 8;
        let is_leaf = data[pos] != 0;
        pos += 1;
        let key_count = u16::from_le_bytes(data[pos..pos + 2].try_into()?);
        pos += 2;

        let mut node = Self::new(page_id, is_leaf);

        // Read keys and values
        for _ in 0..key_count {
            let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into()?) as usize;
            pos += 2;
            node.keys.push(data[pos..pos + key_len].to_vec());
            pos += key_len;
            node.values.push(u64::from_le_bytes(data[pos..pos + 8].try_into()?));
            pos += 8;
        }

        // Read child pointers if not leaf
        if !is_leaf {
            for _ in 0..key_count + 1 {
                node.children.push(PageId::from_bytes(&data[pos..pos + 8])?);
                pos += 8;
            }
        }

        Ok(node)
    }

    /// Save node to a page
    fn to_page(&self, page: &mut Page) -> Result<(), Error> {
        let mut data = Vec::new();

        // Write header
        data.extend_from_slice(&self.page_id.to_bytes());
        data.push(if self.is_leaf { 1 } else { 0 });
        data.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Write keys and values
        for i in 0..self.keys.len() {
            data.extend_from_slice(&(self.keys[i].len() as u16).to_le_bytes());
            data.extend_from_slice(&self.keys[i]);
            data.extend_from_slice(&self.values[i].to_le_bytes());
        }

        // Write child pointers if not leaf
        if !self.is_leaf {
            for child in &self.children {
                data.extend_from_slice(&child.to_bytes());
            }
        }

        page.write_data(&data)?;
        Ok(())
    }
}

impl BTree {
    /// Create a new B-tree
    pub async fn create(config: BTreeConfig, buffer_pool: Arc<BufferPool>) -> Result<Self, Error> {
        let root_page_id = buffer_pool.allocate_page().await?;
        let mut root_page = buffer_pool.get_page(file, root_page_id).await?;
        root_node.to_page(&mut root_page)?;

        Ok(Self {
            root_page_id,
            buffer_pool,
            config,
        })
    }

    /// Open an existing B-tree
    pub async fn open(
        config: BTreeConfig,
        buffer_pool: Arc<BufferPool>,
        root_page_id: PageId,
    ) -> Result<Self, Error> {
        Ok(Self {
            root_page_id,
            buffer_pool,
            config,
        })
    }

    /// Insert a key-value pair
    pub async fn insert(&mut self, key: &[u8], value: u64) -> Result<(), Error> {
        let mut root = self.load_node(self.root_page_id).await?;
        
        // Split root if full
        if root.keys.len() == MAX_KEYS {
            let new_root_id = self.buffer_pool.allocate_page().await?;
            let mut new_root = Node::new(new_root_id, false);
            new_root.children.push(self.root_page_id);
            
            self.split_child(&mut new_root, 0, root).await?;
            self.root_page_id = new_root_id;
            
            self.insert_non_full(&mut new_root, key, value).await?;
        } else {
            self.insert_non_full(&mut root, key, value).await?;
        }
        
        Ok(())
    }

    /// Search for a key
    pub async fn search(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        self.search_node(self.root_page_id, key).await
    }

    /// Range scan from start (inclusive) to end (exclusive)
    pub async fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, u64)>, Error> {
        let mut results = Vec::new();
        self.range_scan_node(self.root_page_id, start, end, &mut results).await?;
        Ok(results)
    }

    /// Delete a key
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.delete_key(self.root_page_id, key).await?;
        Ok(())
    }

    /// Get B-tree height
    pub async fn height(&self) -> Result<usize, Error> {
        let mut height = 1;
        let mut node = self.load_node(self.root_page_id).await?;
        
        while !node.is_leaf {
            height += 1;
            node = self.load_node(node.children[0]).await?;
        }
        
        Ok(height)
    }

    // Helper methods

    async fn load_node(&self, page_id: PageId) -> Result<Node, Error> {
        let page = self.buffer_pool.get_page(page_id).await?;
        let page_guard = page.read().await;
        Node::from_page(&page_guard)
    }
    async fn save_node(&self, node: &Node) -> Result<(), Error> {
        let page = self.buffer_pool.get_page(file, node.page_id).await?;
        let mut page_guard = page.write().await;
        node.to_page(&mut page_guard)?;
    }

    async fn insert_non_full(
        &mut self,
        node: &mut Node,
        key: &[u8],
        value: u64,
    ) -> Result<(), Error> {
        let mut i = node.keys.len();
        
        if node.is_leaf {
            // Insert into leaf node
            while i > 0 && key < &node.keys[i - 1] {
                i -= 1;
            }
            
            // Check for duplicates if unique index
            if self.config.unique && i > 0 && key == &node.keys[i - 1] {
                return Err(Error::Storage("Duplicate key in unique index".into()));
            }
            
            node.keys.insert(i, key.to_vec());
            node.values.insert(i, value);
            self.save_node(node).await?;
        } else {
            // Insert into internal node
            while i > 0 && key < &node.keys[i - 1] {
                i -= 1;
            }
            
            let mut child = self.load_node(node.children[i]).await?;
            
            if child.keys.len() == MAX_KEYS {
                // Split child if full
                self.split_child(node, i, child).await?;
                if key > &node.keys[i] {
                    i += 1;
                }
                child = self.load_node(node.children[i]).await?;
            }
            
            self.insert_non_full(&mut child, key, value).await?;
        }
        
        Ok(())
    }

    async fn split_child(
        &mut self,
        parent: &mut Node,
        index: usize,
        mut child: Node,
    ) -> Result<(), Error> {
        let new_page_id = self.buffer_pool.allocate_page().await?;
        let mut new_node = Node::new(new_page_id, child.is_leaf);
        
        // Move half the keys to new node
        let mid = MIN_KEYS;
        new_node.keys = child.keys.split_off(mid + 1);
        new_node.values = child.values.split_off(mid + 1);
        
        if !child.is_leaf {
            new_node.children = child.children.split_off(mid + 1);
        }
        
        // Move median key to parent
        parent.keys.insert(index, child.keys.remove(mid));
        parent.values.insert(index, child.values.remove(mid));
        parent.children.insert(index + 1, new_page_id);
        
        // Save all modified nodes
        self.save_node(parent).await?;
        self.save_node(&child).await?;
        self.save_node(&new_node).await?;
        
        Ok(())
    }

    async fn search_node(&self, page_id: PageId, key: &[u8]) -> Result<Option<u64>, Error> {
        let node = self.load_node(page_id).await?;
        let mut i = 0;
        
        while i < node.keys.len() && key > &node.keys[i] {
            i += 1;
        }
        
        if i < node.keys.len() && key == &node.keys[i] {
            Ok(Some(node.values[i]))
        } else if node.is_leaf {
            Ok(None)
        } else {
            self.search_node(node.children[i], key).await
        }
    }

    async fn range_scan_node(
        &self,
        page_id: PageId,
        start: &[u8],
        end: &[u8],
        results: &mut Vec<(Vec<u8>, u64)>,
    ) -> Result<(), Error> {
        let node = self.load_node(page_id).await?;
        let mut i = 0;
        
        while i < node.keys.len() && &node.keys[i] < start {
            i += 1;
        }
        
        if !node.is_leaf {
            self.range_scan_node(node.children[i], start, end, results).await?;
        }
        
        while i < node.keys.len() && &node.keys[i] < end {
            results.push((node.keys[i].clone(), node.values[i]));
            i += 1;
            
            if !node.is_leaf && i < node.children.len() {
                self.range_scan_node(node.children[i], start, end, results).await?;
            }
        }
        
        Ok(())
    }

    async fn delete_key(&mut self, page_id: PageId, key: &[u8]) -> Result<(), Error> {
        let mut node = self.load_node(page_id).await?;
        let mut i = 0;
        
        while i < node.keys.len() && key > &node.keys[i] {
            i += 1;
        }
        
        if node.is_leaf {
            if i < node.keys.len() && key == &node.keys[i] {
                node.keys.remove(i);
                node.values.remove(i);
                self.save_node(&node).await?;
            }
        } else {
            if i < node.keys.len() && key == &node.keys[i] {
                // Key found in internal node, replace with predecessor
                let predecessor = self.get_predecessor(&node, i).await?;
                node.keys[i] = predecessor.0;
                node.values[i] = predecessor.1;
                self.save_node(&node).await?;
                self.delete_key(node.children[i], &predecessor.0).await?;
            } else {
                // Key not found, recurse into appropriate child
                self.delete_key(node.children[i], key).await?;
                
                // Rebalance if necessary
                let child = self.load_node(node.children[i]).await?;
                if child.keys.len() < MIN_KEYS {
                    self.rebalance(&mut node, i).await?;
                }
            }
        }
        
        Ok(())
    }

    async fn get_predecessor(&self, node: &Node, index: usize) -> Result<(Vec<u8>, u64), Error> {
        let mut current = self.load_node(node.children[index]).await?;
        
        while !current.is_leaf {
            current = self.load_node(current.children[current.children.len() - 1]).await?;
        }
        
        Ok((
            current.keys[current.keys.len() - 1].clone(),
            current.values[current.values.len() - 1],
        ))
    }

    async fn rebalance(&mut self, parent: &mut Node, child_index: usize) -> Result<(), Error> {
        let child = self.load_node(parent.children[child_index]).await?;
        
        if child_index > 0 {
            // Try borrowing from left sibling
            let left_sibling = self.load_node(parent.children[child_index - 1]).await?;
            if left_sibling.keys.len() > MIN_KEYS {
                self.rotate_right(parent, child_index - 1).await?;
                return Ok(());
            }
        }
        
        if child_index < parent.children.len() - 1 {
            // Try borrowing from right sibling
            let right_sibling = self.load_node(parent.children[child_index + 1]).await?;
            if right_sibling.keys.len() > MIN_KEYS {
                self.rotate_left(parent, child_index).await?;
                return Ok(());
            }
        }
        
        // Merge with a sibling
        if child_index > 0 {
            self.merge_nodes(parent, child_index - 1).await?;
        } else {
            self.merge_nodes(parent, child_index).await?;
        }
        
        Ok(())
    }

    async fn rotate_left(&mut self, parent: &mut Node, index: usize) -> Result<(), Error> {
        let mut left = self.load_node(parent.children[index]).await?;
        let mut right = self.load_node(parent.children[index + 1]).await?;
        
        // Move parent's key down to left child
        left.keys.push(parent.keys[index].clone());
        left.values.push(parent.values[index]);
        
        // Move right's first key up to parent
        parent.keys[index] = right.keys.remove(0);
        parent.values[index] = right.values.remove(0);
        
        if !left.is_leaf {
            left.children.push(right.children.remove(0));
        }
        
        // Save modified nodes
        self.save_node(parent).await?;
        self.save_node(&left).await?;
        self.save_node(&right).await?;
        
        Ok(())
    }

    async fn rotate_right(&mut self, parent: &mut Node, index: usize) -> Result<(), Error> {
        let mut left = self.load_node(parent.children[index]).await?;
        let mut right = self.load_node(parent.children[index + 1]).await?;
        
        // Move parent's key down to right child
        right.keys.insert(0, parent.keys[index].clone());
        right.values.insert(0, parent.values[index]);
        
        // Move left's last key up to parent
        parent.keys[index] = left.keys.pop().unwrap();
        parent.values[index] = left.values.pop().unwrap();
        
        if !right.is_leaf {
            right.children.insert(0, left.children.pop().unwrap());
        }
        
        // Save modified nodes
        self.save_node(parent).await?;
        self.save_node(&left).await?;
        self.save_node(&right).await?;
        
        Ok(())
    }

    async fn merge_nodes(&mut self, parent: &mut Node, index: usize) -> Result<(), Error> {
        let mut left = self.load_node(parent.children[index]).await?;
        let right = self.load_node(parent.children[index + 1]).await?;
        
        // Move parent's key down to left child
        left.keys.push(parent.keys.remove(index));
        left.values.push(parent.values.remove(index));
        
        // Move all keys from right to left
        left.keys.extend(right.keys.iter().cloned());
        left.values.extend(right.values.iter().cloned());
        
        if !left.is_leaf {
            left.children.extend(right.children.iter().cloned());
        }
        
        // Remove right child from parent
        parent.children.remove(index + 1);
        
        // Save modified nodes
        self.save_node(parent).await?;
        self.save_node(&left).await?;
        
        // Free the right node's page
        self.buffer_pool.free_page(right.page_id).await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_btree() -> Result<BTree, Error> {
        let dir = tempdir()?;
        let buffer_pool = Arc::new(BufferPool::new(1000));
        
        let config = BTreeConfig {
            name: "test_index".to_string(),
            unique: true,
            nullable: false,
        };
        
        BTree::create(config, buffer_pool).await
    }

    #[tokio::test]
    async fn test_basic_operations() -> Result<(), Error> {
        let mut btree = create_test_btree().await?;

        // Test insertion
        btree.insert(b"key1", 1).await?;
        btree.insert(b"key2", 2).await?;
        btree.insert(b"key3", 3).await?;

        // Test search
        assert_eq!(btree.search(b"key1").await?, Some(1));
        assert_eq!(btree.search(b"key2").await?, Some(2));
        assert_eq!(btree.search(b"key3").await?, Some(3));
        assert_eq!(btree.search(b"key4").await?, None);

        // Test range scan
        let results = btree.range_scan(b"key1", b"key3").await?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, 1);
        assert_eq!(results[1].1, 2);

        // Test deletion
        btree.delete(b"key2").await?;
        assert_eq!(btree.search(b"key2").await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_constraint() -> Result<(), Error> {
        let mut btree = create_test_btree().await?;

        btree.insert(b"key1", 1).await?;
        assert!(btree.insert(b"key1", 2).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_large_dataset() -> Result<(), Error> {
        let mut btree = create_test_btree().await?;
        
        // Insert 1000 items
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            btree.insert(key.as_bytes(), i).await?;
        }

        // Verify height is reasonable (should be around 3-4 for 1000 items)
        let height = btree.height().await?;
        assert!(height >= 3 && height <= 4);

        // Test range scan
        let results = btree.range_scan(b"key0100", b"key0200").await?;
        assert_eq!(results.len(), 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> Result<(), Error> {
        use tokio::task;
        
        let btree = Arc::new(RwLock::new(create_test_btree().await?));
        let mut handles = vec![];

        // Spawn multiple tasks inserting and reading
        for i in 0..10 {
            let btree = Arc::clone(&btree);
            
            handles.push(task::spawn(async move {
                let key = format!("key{}", i);
                
                // Insert
                {
                    let mut btree = btree.write().await;
                    btree.insert(key.as_bytes(), i as u64).await?;
                }
                
                // Read back
                let btree = btree.read().await;
                let value = btree.search(key.as_bytes()).await?;
                assert_eq!(value, Some(i as u64));
                
                Ok::<_, Error>(())
            }));
        }

        for handle in handles {
            handle.await??;
        }

        Ok(())
    }
}