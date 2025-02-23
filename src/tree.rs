use crate::array_types::MIN_KEYS_PER_NODE;
use crate::bulk_load::{bulk_load_from_sorted_kv_pairs, bulk_load_from_sorted_kv_pairs_parallel};
use crate::bulk_update::{
    bulk_insert_or_update_from_sorted_kv_pairs_parallel, bulk_update_from_sorted_kv_pairs_parallel,
};
use crate::coalescing::coalesce_or_redistribute_leaf_node;
use crate::cursor::Cursor;
use crate::cursor::CursorMut;
use crate::debug_println;
use crate::graceful_pointers::GracefulArc;
use crate::iter::{BTreeIterator, BackwardBTreeIterator, ForwardBTreeIterator};
use crate::node::debug_assert_no_locks_held;
use crate::node_ptr::{marker, DiscriminatedNode, NodeRef};
use crate::reference::Ref;
use crate::root_node::RootNode;
use crate::search::{
    get_leaf_exclusively_using_exclusive_search, get_leaf_exclusively_using_optimistic_search,
    get_leaf_exclusively_using_shared_search, get_leaf_shared_using_optimistic_search,
    get_leaf_shared_using_shared_search,
};
use crate::splitting::{
    insert_into_leaf_after_splitting,
    insert_into_leaf_after_splitting_returning_leaf_with_new_entry, EntryLocation,
};
use std::fmt::{Debug, Display};
use std::sync::atomic::Ordering;

pub trait BTreeKey: PartialOrd + Ord + Clone + Debug + Display + Send + 'static {}
pub trait BTreeValue: Debug + Display + Send + 'static {}

impl<K: PartialOrd + Ord + Clone + Debug + Display + Send + 'static> BTreeKey for K {}
impl<V: Debug + Display + Send + 'static> BTreeValue for V {}

/// B+Tree
/// Todo
/// - conditional removal
///   tree.cas("key1", old_val, None) -> remove_if
/// Perf ideas:
/// - try inlined key descriminator with node-level key prefixes
/// - try the "no coalescing" or "relaxed" btree idea
/// - try unordered leaf storage, or lazily sorted leaf storage
/// To test with Miri:
///   MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test

pub struct BTree<K: BTreeKey, V: BTreeValue> {
    pub root: RootNode<K, V>,
}

impl<K: BTreeKey, V: BTreeValue> BTree<K, V> {
    pub fn new() -> Self {
        BTree {
            root: RootNode::new(),
        }
    }

    pub fn bulk_load(sorted_kv_pairs: Vec<(K, V)>) -> Self {
        bulk_load_from_sorted_kv_pairs(sorted_kv_pairs)
    }
    pub fn bulk_load_parallel(sorted_kv_pairs: Vec<(K, V)>) -> Self {
        bulk_load_from_sorted_kv_pairs_parallel(sorted_kv_pairs)
    }
    pub fn bulk_update_parallel(&self, updates: Vec<(K, V)>) {
        bulk_update_from_sorted_kv_pairs_parallel(updates, self)
    }
    pub fn bulk_insert_or_update_parallel<F>(&self, entries: Vec<(K, V)>, update_fn: &F)
    where
        F: Fn(*mut V) -> *mut V + Send + Sync,
    {
        bulk_insert_or_update_from_sorted_kv_pairs_parallel(entries, update_fn, self)
    }

    pub fn get(&self, search_key: &K) -> Option<Ref<V>> {
        debug_println!("top-level get {:?}", search_key);

        // try optimistic search first
        if let Ok(leaf_node_shared) =
            get_leaf_shared_using_optimistic_search(self.root.as_node_ref(), search_key)
        {
            let result = match leaf_node_shared.get(search_key) {
                Some((_, v_ptr)) => Some(Ref::new(v_ptr)),
                None => None,
            };
            leaf_node_shared.unlock_shared();
            return result;
        }

        // fall back to shared search
        let leaf_node_shared =
            get_leaf_shared_using_shared_search(self.root.as_node_ref(), search_key);
        debug_println!("top-level get {:?} done", search_key);
        let result = match leaf_node_shared.get(search_key) {
            Some((_, v_ptr)) => Some(Ref::new(v_ptr)),
            None => None,
        };
        leaf_node_shared.unlock_shared();
        debug_assert_no_locks_held::<'g'>();
        result
    }

    pub fn len(&self) -> usize {
        self.root.len.load(Ordering::Relaxed)
    }

    pub fn insert(&self, key: Box<K>, value: Box<V>) {
        debug_println!("top-level insert {:?}", key);
        let graceful_key: GracefulArc<K> = GracefulArc::new(*key);

        // first try fully optimistic search
        let mut optimistic_leaf = match get_leaf_exclusively_using_optimistic_search(
            self.root.as_node_ref(),
            &graceful_key,
        ) {
            Ok(leaf) => leaf,
            // if that doesn't work, use shared search, which is still optimistic
            // in the sense that we're assuming we don't need any structural modifications
            Err(_) => {
                get_leaf_exclusively_using_shared_search(self.root.as_node_ref(), &graceful_key)
            }
        };
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion)
            || optimistic_leaf.get(&graceful_key).is_some()
        {
            optimistic_leaf.insert(graceful_key, Box::into_raw(value));
            optimistic_leaf.unlock_exclusive();
            self.root.len.fetch_add(1, Ordering::Relaxed);
            debug_assert_no_locks_held::<'i'>();
            return;
        }
        optimistic_leaf.unlock_exclusive();

        // we need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.root.as_node_ref().lock_exclusive(),
            &graceful_key,
            ModificationType::Insertion,
        );
        let mut leaf_node = search_stack.peek_lowest().assert_leaf().assert_exclusive();

        if leaf_node.has_capacity_for_modification(ModificationType::Insertion) {
            leaf_node.insert(graceful_key, Box::into_raw(value));
            search_stack.drain().for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
        } else {
            // if the key already exists, we don't need to split
            // so check for that case and exit early, but only bother checking
            // in the case where we otherwise would split
            // this is necessary for correctness, because split doesn't (and shouldn't)
            // handle the case where the key already exists
            // you might think we handled this above, but someone could've come along and
            // inserted the key between the optimistic search and the exclusive search
            if leaf_node.get(&graceful_key).is_some() {
                leaf_node.insert(graceful_key, Box::into_raw(value));
                search_stack.drain().for_each(|n| {
                    n.assert_exclusive().unlock_exclusive();
                });
                debug_assert_no_locks_held::<'i'>();
                return;
            } else {
                insert_into_leaf_after_splitting(search_stack, graceful_key, Box::into_raw(value));
            }
        }
        self.root.len.fetch_add(1, Ordering::Relaxed);
        debug_println!("top-level insert done");
        debug_assert_no_locks_held::<'i'>();
    }

    pub fn get_or_insert(&self, key: Box<K>, value: Box<V>) -> CursorMut<K, V> {
        let mut optimistic_leaf =
            match get_leaf_exclusively_using_optimistic_search(self.root.as_node_ref(), &key) {
                Ok(leaf) => leaf,
                Err(_) => get_leaf_exclusively_using_shared_search(self.root.as_node_ref(), &key),
            };
        let search_result = optimistic_leaf.binary_search_key(&key);

        // case 1: key already exists
        if let Ok(index) = search_result {
            return CursorMut::new_from_leaf_and_index(self, optimistic_leaf, index);
        }

        // case 2: key doesn't exist, but we have capacity to insert
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion) {
            let index = search_result.unwrap_err();
            optimistic_leaf.insert_new_value_at_index(
                GracefulArc::new(*key),
                Box::into_raw(value),
                index,
            );
            return CursorMut::new_from_leaf_and_index(self, optimistic_leaf, index);
        }
        // unlock the leaf so we can restart our search
        optimistic_leaf.unlock_exclusive();

        // case 3: key doesn't exist, and we need to split
        let (entry_location, _) =
            self.get_or_insert_pessimistic(GracefulArc::new(*key), Box::into_raw(value));
        CursorMut::new_from_location(self, entry_location)
    }

    pub(crate) fn get_or_insert_pessimistic(
        &self,
        key: GracefulArc<K>,
        value: *mut V,
    ) -> (EntryLocation<K, V>, bool) {
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.root.as_node_ref().lock_exclusive(),
            &key,
            ModificationType::Insertion,
        );
        // since we restarted our search, we need to see if the key exists again before we split
        let leaf_node = search_stack.peek_lowest().assert_leaf().assert_exclusive();
        let index = leaf_node.binary_search_key(&key);
        if index.is_ok() {
            // someone has inserted the key while we were searching
            // pop off the leaf, unlock the rest, and then we're done
            search_stack.pop_lowest();
            search_stack.drain().for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });

            return (EntryLocation::new(leaf_node, index.unwrap()), false);
        }

        let entry_location = insert_into_leaf_after_splitting_returning_leaf_with_new_entry(
            search_stack,
            key,
            value,
        );
        (entry_location, true)
    }

    pub fn remove(&self, key: &K) {
        debug_println!("top-level remove {:?}", key);

        let mut optimistic_leaf =
                // first try fully optimistic search
                match get_leaf_exclusively_using_optimistic_search(self.root.as_node_ref(), key) {
                    Ok(leaf) => leaf,
                    // if that doesn't work, use shared search, which is still optimistic
                    // in the sense that we're assuming we don't need any structural modifications
                    Err(_) => get_leaf_exclusively_using_shared_search(self.root.as_node_ref(), key),
                };
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Removal)
            || optimistic_leaf.get(&key).is_none()
        {
            let removed = optimistic_leaf.remove(key);
            if removed {
                self.root.len.fetch_sub(1, Ordering::Relaxed);
            }
            debug_println!("top-level remove {:?} done - removed? {:?}", key, removed);
            optimistic_leaf.unlock_exclusive();
            debug_assert_no_locks_held::<'r'>();
            return;
        }
        optimistic_leaf.unlock_exclusive();

        // we need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.root.as_node_ref().lock_exclusive(),
            key,
            ModificationType::Removal,
        );

        let mut leaf_node_exclusive = search_stack.peek_lowest().assert_leaf().assert_exclusive();
        let removed = leaf_node_exclusive.remove(key);
        if removed {
            self.root.len.fetch_sub(1, Ordering::Relaxed);
        }
        if leaf_node_exclusive.num_keys() < MIN_KEYS_PER_NODE {
            coalesce_or_redistribute_leaf_node(search_stack);
        } else {
            // the remove might not actually remove the key (it might be not found)
            // so the stack may still contain nodes -- unlock them
            search_stack.drain().for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
        }
        debug_println!("top-level remove {:?} done - removed? {:?}", key, removed);
        debug_assert_no_locks_held::<'r'>();
    }

    pub fn print_tree(&self) {
        let root = self.root.as_node_ref().lock_shared();
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree length: {}      |", self.len());
        println!("+----------------------+");
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree(),
        )
        .force()
        {
            DiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.print_node();
                internal.unlock_shared();
            }
            DiscriminatedNode::Leaf(leaf) => {
                let leaf = leaf.lock_shared();
                leaf.print_node();
                leaf.unlock_shared();
            }
            _ => unreachable!(),
        }
        root.unlock_shared();
    }

    pub fn check_invariants(&self) {
        debug_println!("checking invariants");
        let root = self.root.as_node_ref().lock_shared();
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree(),
        )
        .force()
        {
            DiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.check_invariants();
                internal.unlock_shared();
            }
            DiscriminatedNode::Leaf(leaf) => {
                let leaf = leaf.lock_shared();
                leaf.check_invariants();
                leaf.unlock_shared();
            }
            _ => unreachable!(),
        }
        root.unlock_shared();
    }

    pub fn iter(&self) -> ForwardBTreeIterator<K, V> {
        BTreeIterator::new(self)
    }

    pub fn iter_rev(&self) -> BackwardBTreeIterator<K, V> {
        BTreeIterator::new(self)
    }

    pub fn cursor(&self) -> Cursor<K, V> {
        Cursor {
            tree: self,
            current_leaf: None,
            current_index: 0,
        }
    }

    pub fn cursor_mut(&self) -> CursorMut<K, V> {
        CursorMut {
            tree: self,
            current_leaf: None,
            current_index: 0,
        }
    }
}

unsafe impl<K: BTreeKey, V: BTreeValue> Send for BTree<K, V> {}
unsafe impl<K: BTreeKey, V: BTreeValue> Sync for BTree<K, V> {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ModificationType {
    Insertion,
    Removal,
    NonModifying,
}

#[cfg(test)]
mod tests {
    use crate::array_types::ORDER;
    use crate::qsbr::qsbr_reclaimer;

    use super::*;

    #[test]
    fn test_insert_and_get() {
        #[cfg(not(miri))]
        const NUM_OPERATIONS: usize = ORDER.pow(2);
        #[cfg(miri)]
        const NUM_OPERATIONS: usize = ORDER;

        qsbr_reclaimer().register_thread();

        let tree = BTree::<usize, String>::new();
        let n = NUM_OPERATIONS;
        for i in 1..=n {
            let value = format!("value{}", i);
            tree.insert(Box::new(i), Box::new(value.clone()));
            tree.check_invariants();
            let result = tree.get(&i);
            assert_eq!(result.as_deref(), Some(&value));
        }

        println!("tree should be full:");

        assert_eq!(tree.get(&1).unwrap(), &"value1".to_string());
        assert_eq!(tree.get(&2).unwrap(), &"value2".to_string());
        assert_eq!(tree.get(&3).unwrap(), &"value3".to_string());

        // Remove all elements in sequence
        // this will force the tree to coalesce and redistribute
        for i in 1..=n {
            tree.remove(&i);
            tree.check_invariants();
            assert_eq!(tree.get(&i), None);
        }

        // Check that elements are removed
        assert_eq!(tree.get(&1), None);
        assert_eq!(tree.get(&2), None);
        assert_eq!(tree.get(&3), None);
        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    use rand::rngs::StdRng;
    use rand::seq::IteratorRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[cfg(not(miri))]
    const NUM_OPERATIONS: usize = 100000;

    #[cfg(miri)]
    const NUM_OPERATIONS: usize = 100;

    const INTERESTING_SEEDS: [u64; 1] = [13142251578868436595];
    #[test]
    fn test_random_inserts_gets_and_removes_with_seed_single_threaded() {
        // Test with predefined interesting seeds
        for &seed in &INTERESTING_SEEDS {
            run_random_operations_with_seed_single_threaded(seed);
        }

        // Test with 10 random seeds
        for _ in 0..10 {
            let random_seed: u64 = rand::thread_rng().gen();
            println!("Using random seed: {}", random_seed);
            run_random_operations_with_seed_single_threaded(random_seed);
        }
    }

    fn run_random_operations_with_seed_single_threaded(seed: u64) {
        qsbr_reclaimer().register_thread();
        let mut rng = StdRng::seed_from_u64(seed);
        let tree = BTree::<usize, String>::new();
        let mut reference_map = HashMap::new();
        println!("Using seed: {}", seed);

        // Perform random operations for a while
        for _ in 0..NUM_OPERATIONS {
            let operation = rng.gen_range(0..3);
            match operation {
                0 => {
                    // Random insert
                    let key = rng.gen_range(0..1000);
                    let value = format!("value{}", key);
                    tree.insert(Box::new(key), Box::new(value.clone()));
                    reference_map.insert(key, value);
                    tree.check_invariants();
                }
                1 => {
                    // Random get
                    let key = rng.gen_range(0..1000);
                    let btree_result = tree.get(&key);
                    let hashmap_result = reference_map.get(&key);
                    if btree_result.as_deref() != hashmap_result {
                        println!("Mismatch for key {}", key);
                        println!("btree_result: {:?}", btree_result);
                        println!("hashmap_result: {:?}", hashmap_result);
                        tree.check_invariants();
                    }
                    assert_eq!(
                        btree_result.as_deref(),
                        hashmap_result,
                        "Mismatch for key {}",
                        key
                    );
                }
                2 => {
                    // Random remove
                    if !reference_map.is_empty() {
                        let key = *reference_map.keys().choose(&mut rng).unwrap();
                        tree.remove(&key);
                        reference_map.remove(&key);
                        tree.check_invariants();
                    }
                }
                _ => unreachable!(),
            }
        }
        qsbr_reclaimer().mark_current_thread_quiescent();

        // Verify all keys at the end
        for key in reference_map.keys() {
            assert_eq!(
                tree.get(key).as_deref(),
                reference_map.get(key),
                "Final verification failed for key {}",
                key
            );
        }
        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_random_inserts_gets_and_removes_with_seed_multi_threaded() {
        for &seed in &INTERESTING_SEEDS {
            run_random_operations_with_seed_multi_threaded(seed);
        }

        // also run with a random seed
        let random_seed: u64 = rand::thread_rng().gen();
        println!("Using random seed: {}", random_seed);
        run_random_operations_with_seed_multi_threaded(random_seed);
    }

    fn run_random_operations_with_seed_multi_threaded(seed: u64) {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();
        let num_threads = 8;
        #[cfg(not(miri))]
        let operations_per_thread = 25000;
        #[cfg(miri)]
        let operations_per_thread = 100;

        // Use an AtomicUsize to count completed threads
        let completed_threads = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|s| {
            // Spawn operation threads
            for _ in 0..num_threads {
                let tree_ref = &tree;
                let completed_threads = completed_threads.clone();
                s.spawn(move || {
                    qsbr_reclaimer().register_thread();
                    let mut rng = StdRng::seed_from_u64(seed);
                    for _ in 0..operations_per_thread {
                        let operation = rng.gen_range(0..3);
                        match operation {
                            0 => {
                                let key = rng.gen_range(0..1000);
                                let value = format!("value{}", key);
                                tree_ref.insert(Box::new(key), Box::new(value.clone()));
                            }
                            1 => {
                                let key = rng.gen_range(0..1000);
                                let _ = tree_ref.get(&key);
                            }
                            2 => {
                                let key = rng.gen_range(0..1000);
                                tree_ref.remove(&key);
                            }
                            _ => unreachable!(),
                        }
                    }
                    // Increment the counter when this thread completes
                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                    completed_threads.fetch_add(1, Ordering::Release);
                });
            }

            // Spawn invariant checking thread
            let completed_threads = completed_threads.clone();
            let tree_ref = &tree;
            s.spawn(move || {
                qsbr_reclaimer().register_thread();
                while completed_threads.load(Ordering::Acquire) < num_threads {
                    thread::sleep(Duration::from_secs(1));
                    tree_ref.check_invariants();
                }
                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
            });
        });

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_get_or_insert() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        {
            println!("Case 1: Insert into empty tree");
            // Case 1: Insert into empty tree
            let mut cursor = tree.get_or_insert(Box::new(5), Box::new("value5".to_string()));
            assert_eq!(cursor.current().unwrap().key(), &5);
            assert_eq!(cursor.current().unwrap().value(), &"value5".to_string());

            // Move cursor and verify we can find the entry again
            cursor.move_next();
            cursor.seek(&5);
            assert_eq!(cursor.current().unwrap().key(), &5);
        }
        {
            println!("Case 2: Get existing entry");
            // Case 2: Get existing entry
            let cursor = tree.get_or_insert(Box::new(5), Box::new("new_value5".to_string()));
            assert_eq!(cursor.current().unwrap().key(), &5);
            assert_eq!(cursor.current().unwrap().value(), &"value5".to_string());
            // Should keep old value
        }
        {
            println!("Case 3: Insert when there's capacity (no split needed)");
            // Case 3: Insert when there's capacity (no split needed)
            let mut cursor = tree.get_or_insert(Box::new(3), Box::new("value3".to_string()));
            assert_eq!(cursor.current().unwrap().key(), &3);
            assert_eq!(cursor.current().unwrap().value(), &"value3".to_string());

            // Verify we can navigate to both entries
            cursor.seek(&5);
            assert_eq!(cursor.current().unwrap().key(), &5);
            cursor.seek(&3);
            assert_eq!(cursor.current().unwrap().key(), &3);
        }
        {
            // Case 4: Insert enough entries to cause a split
            // Fill up the first leaf node
            for i in 0..ORDER {
                let cursor =
                    tree.get_or_insert(Box::new(i * 2), Box::new(format!("value{}", i * 2)));
                assert_eq!(cursor.current().unwrap().key(), &(i * 2));
                assert_eq!(
                    cursor.current().unwrap().value(),
                    &format!("value{}", i * 2)
                );
            }

            // Insert one more entry that should cause a split
            let split_key = ORDER * 2;
            let mut cursor =
                tree.get_or_insert(Box::new(split_key), Box::new(format!("value{}", split_key)));

            // Verify cursor points to the correct entry after split
            assert_eq!(cursor.current().unwrap().key(), &split_key);
            assert_eq!(
                cursor.current().unwrap().value(),
                &format!("value{}", split_key)
            );

            // Verify we can still access entries before and after the split point
            cursor.seek(&0);
            assert_eq!(cursor.current().unwrap().key(), &0);
            cursor.seek(&((ORDER - 1) * 2));
            assert_eq!(cursor.current().unwrap().key(), &((ORDER - 1) * 2));
        }

        debug_assert_no_locks_held::<'t'>();
        // Verify tree invariants after all operations
        tree.check_invariants();

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
