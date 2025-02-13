use crate::node_ptr::marker;
use crate::node_ptr::NodeRef;
use crate::reference::Entry;
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
        self.current_index = leaf.num_keys().saturating_sub(1);
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
        self.seek_from_top(key);
    }

    fn seek_from_top(&mut self, key: &K) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_shared();
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

    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = self.current_leaf {
            if self.current_index < leaf.num_keys() {
                return Some(Entry::new(
                    leaf.storage.get_key(self.current_index),
                    leaf.storage.get_value(self.current_index),
                ));
            }
        }
        None
    }

    pub fn move_next(&mut self) -> bool {
        println!("moving next");
        loop {
            if self.current_leaf.is_none() {
                println!("current_leaf is None");
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < leaf.num_keys() - 1 {
                self.current_index += 1;
                println!(
                    "moving to next key in current leaf -- index is {}",
                    self.current_index
                );
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                println!("no next leaf");
                self.current_leaf.take().unwrap().unlock_shared();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_shared() {
                Ok(leaf) => {
                    println!("got lock on next leaf");
                    leaf
                }
                Err(_) => {
                    println!("next_leaf lock failed - reseeking");
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    return self.move_next();
                }
            };
            println!("locked next leaf -- advancing to it");
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
                    let key = leaf.storage.get_key(self.current_index);
                    self.current_leaf.take().unwrap().unlock_shared();
                    self.seek(&key);
                    return self.move_prev();
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
        self.seek_from_top(key);
    }

    fn seek_from_top(&mut self, key: &K) {
        if let Some(leaf) = self.current_leaf.as_ref() {
            leaf.unlock_exclusive();
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

    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = self.current_leaf {
            if self.current_index < leaf.num_keys() {
                return Some(Entry::new(
                    leaf.storage.get_key(self.current_index),
                    leaf.storage.get_value(self.current_index),
                ));
            }
        }
        None
    }

    pub fn move_next(&mut self) -> bool {
        loop {
            if let None = self.current_leaf {
                println!("current_leaf is None");
                return false;
            }
            let leaf = self.current_leaf.unwrap();
            if self.current_index < leaf.num_keys() - 1 {
                println!("moving to next key in current leaf");
                self.current_index += 1;
                return true;
            }

            // Move to next leaf
            let maybe_next_leaf = leaf.next_leaf();
            if maybe_next_leaf.is_none() {
                println!("no next leaf");
                self.current_leaf.take().unwrap().unlock_exclusive();
                return false;
            }

            let next_leaf = match maybe_next_leaf.unwrap().try_lock_exclusive() {
                Ok(leaf) => {
                    println!("got lock on next leaf");
                    leaf
                }
                Err(_) => {
                    println!("failed to get lock on next leaf");
                    // we didn't attain the lock, so restart from the top
                    // to the current key
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    return self.move_next();
                }
            };
            println!("locked next leaf -- advancing to it");
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
                        println!("prev_leaf lock failed - reseeking");
                        // we didn't attain the lock, so restart from the top
                        // to the current key
                        let key = leaf.storage.get_key(self.current_index);
                        self.seek_from_top(&key);
                        return self.move_prev();
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
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap().value(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap().value(), "value7");

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
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
        cursor.move_next();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER)
        ); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        ); // Should cross back to first leaf

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
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal
        cursor.seek_to_end();
        for i in (0..10).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test mixed traversal
        cursor.seek_to_start();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value2");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value1");
        cursor.move_prev();
        assert_eq!(*cursor.current().unwrap().value(), "value0");
        cursor.move_next();
        assert_eq!(*cursor.current().unwrap().value(), "value1");

        // Test seeking
        cursor.seek(&5);
        assert_eq!(*cursor.current().unwrap().value(), "value5");
        cursor.seek(&7);
        assert_eq!(*cursor.current().unwrap().value(), "value7");

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
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_next();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test backward traversal across leaves
        cursor.seek_to_end();
        for i in (0..n).rev() {
            let entry = cursor.current().unwrap();
            assert_eq!(*entry.value(), format!("value{}", i));
            cursor.move_prev();
        }
        assert_eq!(cursor.current().is_none(), true);

        // Test seeking across leaves
        cursor.seek(&(ORDER - 1)); // Should be near end of first leaf
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        );
        cursor.move_next();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER)
        ); // Should cross to next leaf

        cursor.seek(&ORDER); // Should be at start of second leaf
        cursor.move_prev();
        assert_eq!(
            *cursor.current().unwrap().value(),
            format!("value{}", ORDER - 1)
        ); // Should cross back to first leaf

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_interaction_between_mut_cursor_and_shared_cursor() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to ensure multiple leaves
        let n = ORDER * 3; // Use 3 times ORDER to ensure multiple leaves
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
            tree.check_invariants();
        }

        let barrier = std::sync::Barrier::new(2);

        std::thread::scope(|s| {
            // First thread starts at end with mut cursor and moves backwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            let handle = s.spawn(move || {
                qsbr_reclaimer().register_thread();
                let mut cursor_mut = tree_ref.cursor_mut();
                cursor_mut.seek_to_end();

                // Wait for both cursors to be ready
                barrier_ref.wait();
                println!("writer past the barrier!");

                // Move backwards and verify values
                let mut expected = n - 1;
                loop {
                    assert_eq!(
                        *cursor_mut.current().unwrap().value(),
                        format!("value{}", expected)
                    );
                    if !cursor_mut.move_prev() {
                        break;
                    }
                    expected -= 1;
                    std::thread::yield_now();
                }
                assert_eq!(expected, 0);
                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
            });

            // Second thread starts at beginning with shared cursor and moves forwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            let handle2 = s.spawn(move || {
                qsbr_reclaimer().register_thread();
                let mut cursor_shared = tree_ref.cursor();
                cursor_shared.seek_to_start();

                // Wait for both cursors to be ready
                barrier_ref.wait();
                println!("reader past the barrier!");

                // Move forward and verify values
                let mut expected = 0;
                loop {
                    assert_eq!(
                        *cursor_shared.current().unwrap().value(),
                        format!("value{}", expected)
                    );
                    if !cursor_shared.move_next() {
                        break;
                    }
                    expected += 1;
                    std::thread::yield_now();
                }
                assert_eq!(expected, n - 1);
                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
            });

            handle.join().unwrap();
            handle2.join().unwrap();
        });

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
