use crate::graceful_pointers::GracefulArc;
use crate::node_ptr::marker;
use crate::node_ptr::NodeRef;
use crate::reference::Entry;
use crate::search::get_leaf_exclusively_using_optimistic_search_with_fallback;
use crate::search::get_leaf_shared_using_optimistic_search_with_fallback;
use crate::search::{
    get_first_leaf_exclusively_using_optimistic_search,
    get_first_leaf_exclusively_using_shared_search, get_first_leaf_shared_using_optimistic_search,
    get_first_leaf_shared_using_shared_search, get_last_leaf_exclusively_using_optimistic_search,
    get_last_leaf_exclusively_using_shared_search, get_last_leaf_shared_using_optimistic_search,
    get_last_leaf_shared_using_shared_search,
};
use crate::splitting::EntryLocation;
use crate::tree::{BTree, BTreeKey, BTreeValue, ModificationType};
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
        if self.current_leaf.is_some() {
            self.current_leaf.take().unwrap().unlock_shared();
        }
        let leaf = get_leaf_shared_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            key,
        );
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
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    return self.move_next();
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
    tree: &'a BTree<K, V>,
    current_leaf: Option<NodeRef<K, V, marker::Exclusive, marker::Leaf>>,
    current_index: usize,
}

impl<'a, K: BTreeKey, V: BTreeValue> CursorMut<'a, K, V> {
    pub(crate) fn new(tree: &'a BTree<K, V>) -> Self {
        Self {
            tree,
            current_leaf: None,
            current_index: 0,
        }
    }
    pub(crate) fn new_from_location(
        tree: &'a BTree<K, V>,
        entry_location: EntryLocation<K, V>,
    ) -> Self {
        Self::new_from_leaf_and_index(tree, entry_location.leaf, entry_location.index)
    }
    pub(crate) fn new_from_leaf_and_index(
        tree: &'a BTree<K, V>,
        leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        index: usize,
    ) -> Self {
        CursorMut {
            tree,
            current_leaf: Some(leaf),
            current_index: index,
        }
    }
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

    pub fn update_value(&mut self, value: Box<V>) {
        let mut leaf = self.current_leaf.unwrap();
        leaf.update(self.current_index, Box::into_raw(value));
    }

    pub fn insert_or_update<F>(&mut self, key: GracefulArc<K>, value: *mut V, update_fn: F) -> bool
    where
        F: Fn(*mut V) -> *mut V + Send + Sync,
    {
        self.seek(&key);

        let mut leaf = self.current_leaf.unwrap();
        let search_result = leaf.binary_search_key(&key);
        if let Ok(index) = search_result {
            let old_value = leaf.storage.get_value(index);
            let new_value = update_fn(old_value);
            leaf.update(index, new_value);
            return false; // Key already existed, no insertion
        } else if leaf.has_capacity_for_modification(ModificationType::Insertion) {
            let index = search_result.unwrap_err();
            leaf.insert_new_value_at_index(key, value, index);
            return true; // New key inserted
        } else {
            // we need to split the leaf, so give up the lock
            leaf.unlock_exclusive();

            let (entry_location, was_inserted) = self.tree.get_or_insert_pessimistic(key, value);

            // someone may have inserted while we were searching
            if was_inserted {
                self.current_leaf = Some(entry_location.leaf);
                self.current_index = entry_location.index;
            } else {
                // get_or_insert_pessimistic just did a get, so we need to update the value
                let mut leaf = entry_location.leaf;
                let new_value = update_fn(leaf.storage.get_value(entry_location.index));
                leaf.update(entry_location.index, new_value);
                self.current_leaf = Some(leaf);
                self.current_index = entry_location.index;
            }
            return was_inserted; // Return whether a new key was inserted
        }
    }

    fn seek_from_top(&mut self, key: &K) {
        if self.current_leaf.is_some() {
            self.current_leaf.take().unwrap().unlock_exclusive();
        }
        let leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            key,
        );
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
                    let key = leaf.storage.get_key(self.current_index);
                    self.seek_from_top(&key);
                    return self.move_next();
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
    use std::sync::Barrier;

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

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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

        let barrier = Barrier::new(2);

        std::thread::scope(|s| {
            // First thread starts at end with mut cursor and moves backwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                qsbr_reclaimer().register_thread();
                let mut cursor_mut = tree_ref.cursor_mut();
                cursor_mut.seek_to_end();

                // Wait for both cursors to be ready
                barrier_ref.wait();

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
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
            });

            // Second thread starts at beginning with shared cursor and moves forwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                qsbr_reclaimer().register_thread();
                let mut cursor_shared = tree_ref.cursor();
                cursor_shared.seek_to_start();

                // Wait for both cursors to be ready
                barrier_ref.wait();

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
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
            });
        });

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }
}
