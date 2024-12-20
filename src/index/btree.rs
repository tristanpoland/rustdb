use std::cmp::Ordering;
use std::mem;

const B: usize = 6; // Order of B-tree
const MIN_KEYS: usize = B - 1;
const MAX_KEYS: usize = 2 * B - 1;

#[derive(Debug)]
pub struct BTree<K: Ord + Clone, V: Clone> {
    root: Option<Box<Node<K, V>>>,
    height: usize,
}

#[derive(Debug)]
struct Node<K: Ord + Clone, V: Clone> {
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<Option<Box<Node<K, V>>>>,
    is_leaf: bool,
}

impl<K: Ord + Clone, V: Clone> Node<K, V> {
    fn new(is_leaf: bool) -> Self {
        Self {
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
}

impl<K: Ord + Clone, V: Clone> BTree<K, V> {
    pub fn new() -> Self {
        Self {
            root: None,
            height: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if let Some(root) = &mut self.root {
            if root.keys.len() == MAX_KEYS {
                // Split the root
                let mut new_root = Box::new(Node::new(false));
                let old_root = mem::replace(&mut self.root, Some(new_root));
                let mut new_root = self.root.as_mut().unwrap();
                new_root.children.push(old_root);
                self.split_child(new_root, 0);
                self.height += 1;
                self.insert_non_full(self.root.as_mut().unwrap(), key, value);
            } else {
                self.insert_non_full(root, key, value);
            }
        } else {
            let mut root = Box::new(Node::new(true));
            root.keys.push(key);
            root.values.push(value);
            self.root = Some(root);
            self.height = 1;
        }
    }

    fn insert_non_full(&mut self, node: &mut Box<Node<K, V>>, key: K, value: V) {
        let mut i = node.keys.len();
        if node.is_leaf {
            // Find position and insert
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }
            node.keys.insert(i, key);
            node.values.insert(i, value);
        } else {
            // Find child to recurse into
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }
            
            let child = &mut node.children[i];
            if child.as_ref().unwrap().keys.len() == MAX_KEYS {
                self.split_child(node, i);
                if key > node.keys[i] {
                    i += 1;
                }
            }
            self.insert_non_full(node.children[i].as_mut().unwrap(), key, value);
        }
    }

    fn split_child(&mut self, parent: &mut Box<Node<K, V>>, child_index: usize) {
        let child = parent.children[child_index].as_mut().unwrap();
        let mut new_node = Box::new(Node::new(child.is_leaf));
        
        // Move right half of keys and values to new node
        new_node.keys = child.keys.split_off(MIN_KEYS);
        new_node.values = child.values.split_off(MIN_KEYS);
        
        // If not leaf, move children too
        if !child.is_leaf {
            new_node.children = child.children.split_off(MIN_KEYS + 1);
        }
        
        // Move median key and value to parent
        let median_key = child.keys.pop().unwrap();
        let median_value = child.values.pop().unwrap();
        
        // Insert new node into parent
        parent.children.insert(child_index + 1, Some(new_node));
        parent.keys.insert(child_index, median_key);
        parent.values.insert(child_index, median_value);
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        let root = self.root.as_ref()?;
        self.search_node(root, key)
    }

    fn search_node(&self, node: &Box<Node<K, V>>, key: &K) -> Option<&V> {
        let mut i = 0;
        while i < node.keys.len() && key > &node.keys[i] {
            i += 1;
        }

        if i < node.keys.len() && key == &node.keys[i] {
            Some(&node.values[i])
        } else if node.is_leaf {
            None
        } else {
            self.search_node(node.children[i].as_ref().unwrap(), key)
        }
    }

    pub fn range(&self, start: &K, end: &K) -> Vec<(&K, &V)> {
        let mut result = Vec::new();
        if let Some(root) = &self.root {
            self.range_search(root, start, end, &mut result);
        }
        result
    }

    fn range_search<'a>(
        &'a self,
        node: &'a Box<Node<K, V>>,
        start: &K,
        end: &K,
        result: &mut Vec<(&'a K, &'a V)>
    ) {
        let mut i = 0;
        while i < node.keys.len() && start > &node.keys[i] {
            i += 1;
        }

        if !node.is_leaf {
            self.range_search(node.children[i].as_ref().unwrap(), start, end, result);
        }

        while i < node.keys.len() && &node.keys[i] <= end {
            result.push((&node.keys[i], &node.values[i]));
            i += 1;
            if !node.is_leaf && i < node.children.len() {
                self.range_search(node.children[i].as_ref().unwrap(), start, end, result);
            }
        }
    }
}