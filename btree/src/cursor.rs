use std::borrow::Borrow;
use std::ops::Deref;

use crate::pointers::node_ref::{marker, SharedNodeRef};
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
use crate::tree::{
    BTree, BTreeKey, BTreeValue, GetOrInsertResult, InsertOrModifyIfResult, ModificationType,
};
use crate::util::UnwrapEither;
use thin::{QsArc, QsOwned, QsShared};

pub struct Cursor<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub tree: &'a BTree<K, V>,
    pub current_leaf: Option<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>>,
    pub current_index: usize,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Cursor<'a, K, V> {
    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref().cloned() {
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
        if let Some(leaf) = self.current_leaf.as_ref().cloned() {
            leaf.unlock_shared();
        }
        let leaf = match get_last_leaf_shared_using_optimistic_search(self.tree.root.as_node_ref())
        {
            Ok(leaf) => leaf,
            Err(_) => get_last_leaf_shared_using_shared_search(self.tree.root.as_node_ref()),
        };
        self.current_index = leaf.num_keys().saturating_sub(1);
        self.current_leaf = Some(leaf);
    }

    pub fn seek<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if leaf_key.deref().borrow() == key {
                    self.current_index = index;
                    return true;
                }
            }
            self.current_leaf.take().unwrap().unlock_shared();
        }
        self.seek_from_top(key)
    }

    fn seek_from_top<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if self.current_leaf.is_some() {
            self.current_leaf.take().unwrap().unlock_shared();
        }
        let leaf = get_leaf_shared_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            key,
        );
        let result = leaf.binary_search_key(key);
        self.current_leaf = Some(leaf);
        self.current_index = result.unwrap_either();
        result.is_ok()
    }

    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = &self.current_leaf {
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
                    let key = leaf.storage.get_key(self.current_index);
                    self.current_leaf.take().unwrap().unlock_shared();
                    self.seek(&key);
                    continue;
                }
            };
            self.current_leaf.take().unwrap().unlock_shared();
            self.current_index = prev_leaf.num_keys();
            self.current_leaf = Some(prev_leaf);
        }
    }
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for Cursor<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_shared();
        }
    }
}

pub struct CursorMut<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    tree: &'a BTree<K, V>,
    current_leaf: Option<SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>>,
    current_index: usize,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> CursorMut<'a, K, V> {
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
        leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        index: usize,
    ) -> Self {
        CursorMut {
            tree,
            current_leaf: Some(leaf),
            current_index: index,
        }
    }
    pub fn seek_to_start(&mut self) {
        if let Some(leaf) = self.current_leaf.as_ref().cloned() {
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
        if let Some(leaf) = self.current_leaf.as_ref().cloned() {
            leaf.unlock_exclusive();
        }
        let leaf =
            match get_last_leaf_exclusively_using_optimistic_search(self.tree.root.as_node_ref()) {
                Ok(leaf) => leaf,
                Err(_) => {
                    get_last_leaf_exclusively_using_shared_search(self.tree.root.as_node_ref())
                }
            };
        self.current_index = leaf.num_keys() - 1;
        self.current_leaf = Some(leaf);
    }

    pub fn seek<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // optimistically try the current leaf, but quickly give up and search from the top
        if let Some(leaf) = self.current_leaf.as_ref() {
            let index = leaf.binary_search_key(key).unwrap_either();
            if index < leaf.num_keys() {
                let leaf_key = leaf.storage.get_key(index);
                if leaf_key.deref().borrow() == key {
                    self.current_index = index;
                    return true;
                }
            }
            self.current_leaf.take().unwrap().unlock_exclusive();
        }
        self.seek_from_top(key)
    }

    pub fn update_value(&mut self, value: QsOwned<V>) {
        let leaf = self.current_leaf.as_mut().unwrap();
        leaf.update(self.current_index, value);
    }

    pub fn modify_value(&mut self, modify_fn: impl FnOnce(QsOwned<V>) -> QsOwned<V>) {
        let leaf = self.current_leaf.as_mut().unwrap();
        let index = self.current_index;
        leaf.modify_value(index, modify_fn);
    }

    /// update_fn is called with the new value and the existing old value
    pub fn insert_or_modify_if<F>(
        &mut self,
        key: QsArc<K>,
        new_value: QsOwned<V>,
        predicate: impl Fn(QsShared<V>) -> bool,
        modify_fn: F,
    ) -> InsertOrModifyIfResult
    where
        F: Fn(QsOwned<V>, QsOwned<V>) -> QsOwned<V> + Send + Sync,
    {
        self.seek(&key);

        let mut leaf = self.current_leaf.unwrap();
        let search_result = leaf.binary_search_key(&key);
        if let Ok(index) = search_result {
            let existing_value = leaf.storage.get_value(index);
            if predicate(existing_value) {
                leaf.modify_value(index, |old_value| modify_fn(old_value, new_value));
                return InsertOrModifyIfResult::Modified;
            }
            return InsertOrModifyIfResult::DidNothing;
        } else if leaf.has_capacity_for_modification(ModificationType::Insertion) {
            let index = search_result.unwrap_err();
            leaf.insert_new_value_at_index(key, new_value, index);
            return InsertOrModifyIfResult::Inserted; // New key inserted
        } else {
            // we need to split the leaf, so give up the lock
            leaf.unlock_exclusive();

            let (entry_location, result) =
                // if this returned new value in the non-insert case, it'd work
                self.tree.get_or_insert_pessimistic(key, new_value);

            // someone may have inserted while we were searching
            match result {
                GetOrInsertResult::Inserted => {
                    self.current_leaf = Some(entry_location.leaf);
                    self.current_index = entry_location.index;
                    InsertOrModifyIfResult::Inserted
                }
                GetOrInsertResult::GotReturningNewValue(returned_value) => {
                    let mut leaf = entry_location.leaf;
                    leaf.modify_value(entry_location.index, |old_value| {
                        modify_fn(old_value, returned_value)
                    });
                    self.current_leaf = Some(leaf);
                    self.current_index = entry_location.index;
                    InsertOrModifyIfResult::Modified
                }
            }
        }
    }

    fn seek_from_top<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if self.current_leaf.is_some() {
            self.current_leaf.take().unwrap().unlock_exclusive();
        }
        let leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.tree.root.as_node_ref(),
            key,
        );
        let result = leaf.binary_search_key(key);
        self.current_leaf = Some(leaf);
        self.current_index = result.unwrap_either();
        result.is_ok()
    }

    pub fn current(&self) -> Option<Entry<K, V>> {
        if let Some(leaf) = &self.current_leaf {
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
                        let key = leaf.storage.get_key(self.current_index);
                        self.seek_from_top(&key);
                        continue;
                    }
                };
                self.current_leaf.take().unwrap().unlock_exclusive();
                self.current_index = prev_leaf.num_keys() - 1;
                self.current_leaf = Some(prev_leaf);
                return true;
            }
        }
    }
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for CursorMut<'a, K, V> {
    fn drop(&mut self) {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.unlock_exclusive();
        }
    }
}

#[cfg(test)]
mod tests {
    use btree_macros::qsbr_test;
    use thin::QsArc;

    use crate::array_types::ORDER;
    use crate::qsbr_reclaimer;
    use std::sync::Barrier;

    use super::*;

    #[qsbr_test]
    fn test_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
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
    }

    #[qsbr_test]
    fn test_cursor_leaf_boundaries() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
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
    }

    #[qsbr_test]
    fn test_cursor_mut() {
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
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

        drop(cursor);
        drop(tree);
    }

    #[qsbr_test]
    fn test_cursor_mut_leaf_boundaries() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to force leaf splits
        // Using ORDER * 2 ensures we have at least 2 leaves
        let n = ORDER * 2;
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
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

        drop(cursor);
        drop(tree);
    }

    #[qsbr_test]
    fn test_interaction_between_mut_cursor_and_shared_cursor() {
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to ensure multiple leaves
        let n = ORDER * 3; // Use 3 times ORDER to ensure multiple leaves
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            tree.check_invariants();
        }

        let barrier = Barrier::new(2);

        std::thread::scope(|s| {
            // First thread starts at end with mut cursor and moves backwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
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
                }
            });

            // Second thread starts at beginning with shared cursor and moves forwards
            let tree_ref = &tree;
            let barrier_ref = &barrier;
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
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
                }
            });
        });
    }
}
