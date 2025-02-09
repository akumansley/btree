use crate::node_ptr::marker;
use crate::node_ptr::NodeRef;
use crate::reference::{ExclusiveRef, Ref};
use crate::search::get_leaf_shared_using_optimistic_search;
use crate::search::get_leaf_shared_using_shared_search;
use crate::search::{
    get_first_leaf_exclusively_using_optimistic_search,
    get_first_leaf_exclusively_using_shared_search, get_first_leaf_shared_using_optimistic_search,
    get_first_leaf_shared_using_shared_search, get_last_leaf_exclusively_using_optimistic_search,
    get_last_leaf_exclusively_using_shared_search, get_last_leaf_shared_using_optimistic_search,
    get_last_leaf_shared_using_shared_search, get_leaf_exclusively_using_optimistic_search,
    get_leaf_exclusively_using_shared_search,
};
use crate::tree::{BTree, BTreeKey, BTreeValue};
use crate::util::UnwrapEither;

pub struct Cursor<'a, K: BTreeKey, V: BTreeValue> {
    pub tree: &'a BTree<K, V>,
    pub current_leaf: Option<NodeRef<K, V, marker::Shared, marker::Leaf>>,
    pub current_index: usize,
}

impl<'a, K: BTreeKey, V: BTreeValue> Cursor<'a, K, V> {
    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_shared();
        }
        let leaf = match get_first_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref())
        {
            Ok(leaf) => leaf,
            Err(_) => get_first_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.current_leaf = Some(leaf);
        self.current_index = 0;
    }

    pub fn seek_to_end(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_shared();
        }
        let leaf = match get_last_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref())
        {
            Ok(leaf) => leaf,
            Err(_) => get_last_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.current_leaf = Some(leaf);
        self.current_index = leaf.num_keys() - 1;
    }

    pub fn seek(&mut self, key: &K) {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if *leaf_key == *key {
                    self.current_index = index;
                    return;
                }
            }
            self.current_leaf.take().unwrap().unlock_shared();
        }

        let leaf = match get_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref(), key)
        {
            Ok(leaf) => leaf,
            Err(_) => get_leaf_shared_using_shared_search(self.tree.root.as_node_ref(), key),
        };
        let index = leaf.binary_search_key(key).unwrap_either();
        self.current_leaf = Some(leaf);
        self.current_index = index;
    }

    pub fn current(&self) -> Option<Ref<K, V>> {
        if let Some(leaf) = self.current_leaf {
            if self.current_index < leaf.num_keys() {
                return Some(Ref::new(leaf, leaf.storage.get_value(self.current_index)));
            }
        }
        None
    }

    pub fn move_next(&mut self) -> bool {
        loop {
            if self.current_leaf.is_none() {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < leaf.num_keys() - 1 {
                self.current_index += 1;
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                self.current_leaf.take().unwrap().unlock_shared();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_shared() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index - 1);
                    self.current_leaf.take().unwrap().unlock_shared();
                    self.seek(&key);
                    continue;
                }
            };
            self.current_leaf.take().unwrap().unlock_shared();
            self.current_leaf = Some(next_leaf);
            self.current_index = 0;
            return true;
        }
    }

    pub fn move_prev(&mut self) -> bool {
        loop {
            if self.current_leaf.is_none() {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index > 0 {
                self.current_index -= 1;
                return true;
            }

            // Move to previous leaf
            let maybe_prev_leaf = leaf.prev_leaf();
            if maybe_prev_leaf.is_none() {
                self.current_leaf.take().unwrap().unlock_shared();
                return false;
            }

            let prev_leaf = match maybe_prev_leaf.unwrap().try_lock_shared() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index + 1);
                    self.current_leaf.take().unwrap().unlock_shared();
                    self.seek(&key);
                    continue;
                }
            };
            self.current_leaf.take().unwrap().unlock_shared();
            self.current_leaf = Some(prev_leaf);
            self.current_index = prev_leaf.num_keys();
        }
    }
}

impl<'a, K: BTreeKey, V: BTreeValue> Drop for Cursor<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_shared();
        }
    }
}

pub struct CursorMut<'a, K: BTreeKey, V: BTreeValue> {
    pub tree: &'a BTree<K, V>,
    pub current_leaf: Option<NodeRef<K, V, marker::Exclusive, marker::Leaf>>,
    pub current_index: usize,
}

impl<'a, K: BTreeKey, V: BTreeValue> CursorMut<'a, K, V> {
    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_exclusive();
        }
        let leaf = match get_first_leaf_exclusively_using_optimistic_search(
            self.tree.root.as_node_ref(),
        ) {
            Ok(leaf) => leaf,
            Err(_) => get_first_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.current_leaf = Some(leaf);
        self.current_index = 0;
    }

    pub fn seek_to_end(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_exclusive();
        }
        let leaf =
            match get_last_leaf_exclusively_using_optimistic_search(self.tree.root.as_node_ref()) {
                Ok(leaf) => leaf,
                Err(_) => {
                    get_last_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref())
                }
            };
        self.current_leaf = Some(leaf);
        self.current_index = leaf.num_keys() - 1;
    }

    pub fn seek(&mut self, key: &K) {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if *leaf_key == *key {
                    self.current_index = index;
                    return;
                }
            }
            self.current_leaf.take().unwrap().unlock_exclusive();
        }

        let leaf =
            match get_leaf_exclusively_using_optimistic_search(self.tree.root.as_node_ref(), key) {
                Ok(leaf) => leaf,
                Err(_) => {
                    get_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref(), key)
                }
            };
        let index = leaf.binary_search_key(key).unwrap_either();
        self.current_leaf = Some(leaf);
        self.current_index = index;
    }

    pub fn current(&self) -> Option<ExclusiveRef<K, V>> {
        if let Some(leaf) = self.current_leaf {
            if self.current_index < leaf.num_keys() {
                let value = ExclusiveRef::new(leaf, leaf.storage.get_value(self.current_index));
                return Some(value);
            }
        }
        None
    }

    pub fn move_next(&mut self) -> bool {
        loop {
            if let None = self.current_leaf {
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < leaf.num_keys() - 1 {
                self.current_index += 1;
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                self.current_leaf.take().unwrap().unlock_exclusive();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_exclusive() {
                Ok(leaf) => leaf,
                Err(_) => {
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index - 1);
                    self.current_leaf.take().unwrap().unlock_exclusive();
                    self.seek(&key);
                    continue;
                }
            };
            self.current_leaf.take().unwrap().unlock_exclusive();
            self.current_leaf = Some(next_leaf);
            self.current_index = 0;
            return true;
        }
    }

    pub fn move_prev(&mut self) -> bool {
        loop {
            if let Some(leaf) = self.current_leaf {
                if self.current_index > 0 {
                    self.current_index -= 1;
                    return true;
                }

                // Move to previous leaf
                let maybe_prev_leaf = leaf.prev_leaf();
                if maybe_prev_leaf.is_none() {
                    self.current_leaf.take().unwrap().unlock_exclusive();
                    return false;
                }

                let prev_leaf = match maybe_prev_leaf.unwrap().try_lock_exclusive() {
                    Ok(leaf) => leaf,
                    Err(_) => {
                        // we didn't attain the lock, so restart from the top
                        // to the current key
                        let key = leaf.storage.get_key(self.current_index + 1);
                        self.current_leaf.take().unwrap().unlock_exclusive();
                        self.seek(&key);
                        continue;
                    }
                };
                self.current_leaf.take().unwrap().unlock_exclusive();
                self.current_leaf = Some(prev_leaf);
                self.current_index = prev_leaf.num_keys() - 1;
                return true;
            }
        }
    }
}

impl<'a, K: BTreeKey, V: BTreeValue> Drop for CursorMut<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_exclusive();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array_types::ORDER;
    use crate::qsbr::qsbr_reclaimer;

    use super::*;

    #[test]
    fn test_cursor() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
        }

        // Test forward traversal
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        for i in 0..10 {
            let value = cursor.current().unwrap();
            cursor.move_next();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let value = cursor.current().unwrap();
            cursor.move_prev();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap(), "value7");

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_cursor_leaf_boundaries() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
            tree.check_invariants();
        }

        // Test forward traversal across leaves
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        for i in 0..n {
            let value = cursor.current();
            cursor.move_next();
            assert_eq!(*value.unwrap(), format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let value = cursor.current().unwrap();
            cursor.move_prev();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER - 1));
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER)); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER - 1)); // Should cross back to first leaf

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_cursor_mut() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
        }

        // Test forward traversal
        let mut cursor = tree.cursor_mut();
        cursor.seek_to_start();
        for i in 0..10 {
            let value = cursor.current().unwrap();
            cursor.move_next();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let value = cursor.current().unwrap();
            cursor.move_prev();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap(), "value7");

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_cursor_mut_leaf_boundaries() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
            tree.check_invariants();
        }

        // Test forward traversal across leaves
        let mut cursor = tree.cursor_mut();
        cursor.seek_to_start();
        for i in 0..n {
            let value = cursor.current();
            cursor.move_next();
            assert_eq!(*value.unwrap(), format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let value = cursor.current();
            cursor.move_prev();
            assert_eq!(*value.unwrap(), format!("value{}", i));
        }
        assert_eq!(cursor.current(), None);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER - 1));
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER)); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap(), format!("value{}", ORDER - 1)); // Should cross back to first leaf

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
