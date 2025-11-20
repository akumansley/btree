use thin::{Arcable, Pointable, QsArc, QsWeak};

use crate::array_types::MIN_KEYS_PER_NODE;
use crate::bulk_operations::{
    bulk_insert_or_update_from_sorted_kv_pairs_parallel, bulk_load_from_sorted_kv_pairs,
    bulk_load_from_sorted_kv_pairs_parallel, bulk_update_from_sorted_kv_pairs_parallel,
    scan_parallel,
};
use crate::coalescing::coalesce_or_redistribute_leaf_node;
use crate::cursor::Cursor;
use crate::cursor::CursorMut;
use crate::iter::{BTreeIterator, BackwardBTreeIterator, ForwardBTreeIterator};
use crate::pointers::node_ref::{marker, SharedDiscriminatedNode};
use crate::pointers::SharedNodeRef;
use crate::reference::Ref;
use crate::root_node::RootNode;
use crate::search::{
    get_leaf_exclusively_using_exclusive_search,
    get_leaf_exclusively_using_optimistic_search_with_fallback,
    get_leaf_shared_using_optimistic_search_with_fallback,
};
use crate::splitting::{
    insert_into_leaf_after_splitting,
    insert_into_leaf_after_splitting_returning_leaf_with_new_entry, EntryLocation,
};
use crate::sync::Ordering;
use std::borrow::Borrow;
use std::fmt::Debug;
use thin::{QsOwned, QsShared};

pub trait BTreeKey: PartialOrd + Ord + Debug + Send + Sync + Arcable + 'static {}
pub trait BTreeValue: Debug + Send + 'static + Pointable + 'static {}

impl<K: PartialOrd + Ord + Debug + Send + Sync + ?Sized + Arcable + 'static> BTreeKey for K {}
impl<V: Debug + Send + Pointable + ?Sized + 'static> BTreeValue for V {}

/// Concurrent B+Tree
// Todo
// - exp w interval guard for borrowed gets
// Perf ideas:
// - try inlined key descriminator with node-level key prefixes
// - try the "no coalescing" or "relaxed" btree idea
// - try unordered leaf storage, or lazily sorted leaf storage
// - experiment with chili instead of rayon
// To test with Miri:
//   MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test
// Run one experimental shuttle test:
//   cargo test --features=shuttle -- test_concurrent_operations_under_shuttle
// Run benchmarks:
//   cargo bench

pub struct BTree<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub root: RootNode<K, V>,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> BTree<K, V> {
    pub fn new() -> Self {
        BTree {
            root: RootNode::new(),
        }
    }

    pub fn bulk_load(sorted_kv_pairs: Vec<(QsArc<K>, QsOwned<V>)>) -> Self {
        bulk_load_from_sorted_kv_pairs(sorted_kv_pairs)
    }
    pub fn bulk_load_parallel(sorted_kv_pairs: Vec<(QsArc<K>, QsOwned<V>)>) -> Self {
        bulk_load_from_sorted_kv_pairs_parallel(sorted_kv_pairs)
    }
    pub fn bulk_update_parallel(&self, updates: Vec<(QsArc<K>, QsOwned<V>)>) {
        bulk_update_from_sorted_kv_pairs_parallel(updates, self)
    }
    pub fn bulk_insert_or_update_parallel<F>(
        &self,
        entries: Vec<(QsArc<K>, QsOwned<V>)>,
        update_fn: &F,
    ) where
        F: Fn(QsOwned<V>, QsOwned<V>) -> QsOwned<V> + Send + Sync,
    {
        bulk_insert_or_update_from_sorted_kv_pairs_parallel(entries, update_fn, self)
    }

    pub fn get<Q>(&self, search_key: &Q) -> Option<Ref<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        debug_println!("top-level get {:?}", search_key);
        let leaf_node_shared = get_leaf_shared_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            search_key,
        );
        let result = match leaf_node_shared.get(search_key) {
            Some((_, v_ptr)) => Some(Ref::new(v_ptr)),
            None => None,
        };
        leaf_node_shared.unlock_shared();
        result
    }

    pub fn get_with<Q, T>(&self, search_key: &Q, closure: impl Fn(QsShared<V>) -> T) -> Option<T>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        let leaf_node_shared = get_leaf_shared_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            search_key,
        );
        let value = match leaf_node_shared.get(search_key) {
            Some((_, value)) => value,
            None => return None,
        };
        let result = closure(value);
        leaf_node_shared.unlock_shared();
        Some(result)
    }

    pub fn get_exclusively_and<Q>(&self, search_key: &Q, closure: impl Fn(QsShared<V>))
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        let leaf_node_exclusive = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            search_key,
        );
        let value = match leaf_node_exclusive.get(search_key) {
            Some((_, value)) => value,
            None => return,
        };
        closure(value);
        leaf_node_exclusive.unlock_exclusive();
    }

    pub fn scan_parallel(
        &self,
        start_key: QsWeak<K>,
        end_key: QsWeak<K>,
        predicate: impl Fn(&V) -> bool + Sync,
    ) -> Vec<QsShared<V>> {
        scan_parallel(Some(start_key), Some(end_key), predicate, self)
    }

    pub fn len(&self) -> usize {
        self.root.len.load(Ordering::Relaxed)
    }

    pub fn insert(&self, key: QsArc<K>, value: impl Into<QsOwned<V>>) {
        debug_println!("top-level insert {:?}", key);

        let value = value.into();
        let mut optimistic_leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            &key,
        );
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion)
            || optimistic_leaf.get(&key).is_some()
        {
            let inserted = optimistic_leaf.insert(key, value);
            optimistic_leaf.unlock_exclusive();
            if inserted {
                self.root.len.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        optimistic_leaf.unlock_exclusive();

        // we need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.root.as_node_ref().lock_exclusive(),
            &key,
            ModificationType::Insertion,
        );
        let mut leaf_node = search_stack.peek_lowest().assert_leaf().assert_exclusive();

        if leaf_node.has_capacity_for_modification(ModificationType::Insertion) {
            leaf_node.insert(key, value);
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
            if leaf_node.get(&key).is_some() {
                leaf_node.insert(key, value);
                search_stack.drain().for_each(|n| {
                    n.assert_exclusive().unlock_exclusive();
                });
                return;
            } else {
                insert_into_leaf_after_splitting(search_stack, key, value);
            }
        }
        self.root.len.fetch_add(1, Ordering::Relaxed);
        debug_println!("top-level insert done");
    }

    pub fn get_or_insert(&self, key: QsArc<K>, value: impl Into<QsOwned<V>>) -> CursorMut<K, V> {
        let value: QsOwned<V> = value.into();

        let mut optimistic_leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            &key,
        );
        let search_result = optimistic_leaf.binary_search_key(&key);

        // case 1: key already exists
        if let Ok(index) = search_result {
            return CursorMut::new_from_leaf_and_index(self, optimistic_leaf, index);
        }

        // case 2: key doesn't exist, but we have capacity to insert
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion) {
            let index = search_result.unwrap_err();
            optimistic_leaf.insert_new_value_at_index(key, value, index);
            // Increment the tree length since we inserted a new key
            self.root.len.fetch_add(1, Ordering::Relaxed);
            return CursorMut::new_from_leaf_and_index(self, optimistic_leaf, index);
        }
        // unlock the leaf so we can restart our search
        optimistic_leaf.unlock_exclusive();

        // case 3: key doesn't exist, and we need to split
        let (entry_location, result) = self.get_or_insert_pessimistic(key, value);

        // Increment the tree length if a new key was inserted
        if matches!(result, GetOrInsertResult::Inserted) {
            self.root.len.fetch_add(1, Ordering::Relaxed);
        }

        CursorMut::new_from_location(self, entry_location)
    }

    // returns the passed in value if it wasn't inserted
    pub(crate) fn get_or_insert_pessimistic(
        &self,
        key: QsArc<K>,
        value: QsOwned<V>,
    ) -> (EntryLocation<K, V>, GetOrInsertResult<V>) {
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

            return (
                EntryLocation::new(leaf_node, index.unwrap()),
                GetOrInsertResult::GotReturningNewValue(value),
            );
        }

        let entry_location = insert_into_leaf_after_splitting_returning_leaf_with_new_entry(
            search_stack,
            key,
            value,
        );
        (entry_location, GetOrInsertResult::Inserted)
    }
    pub fn remove(&self, key: &K) {
        debug_println!("top-level remove {:?}", key);
        self.remove_if(key, |_| true);
    }

    pub fn remove_if(&self, key: &K, predicate: impl Fn(QsShared<V>) -> bool) -> bool {
        debug_println!("top-level remove {:?}", key);

        let mut optimistic_leaf = get_leaf_exclusively_using_optimistic_search_with_fallback(
            self.root.as_node_ref(),
            key,
        );
        let index = match optimistic_leaf.binary_search_key(&key) {
            Ok(index) => index,
            Err(_) => {
                optimistic_leaf.unlock_exclusive();
                return false;
            } // nothing to remove
        };
        let value = optimistic_leaf.storage.get_value(index);
        if !predicate(value) {
            optimistic_leaf.unlock_exclusive();
            return false;
        }
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Removal) {
            optimistic_leaf.remove_at_index(index);
            self.root.len.fetch_sub(1, Ordering::Relaxed);
            debug_println!("top-level remove {:?} done - removed? {:?}", key, removed);
            optimistic_leaf.unlock_exclusive();
            return true;
        }
        optimistic_leaf.unlock_exclusive();

        // we may need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.root.as_node_ref().lock_exclusive(),
            key,
            ModificationType::Removal,
        );

        let mut leaf_node_exclusive = search_stack.peek_lowest().assert_leaf().assert_exclusive();

        // need to re-check the predicate since we released our locks to restart the search
        let search_result = leaf_node_exclusive.binary_search_key(&key);
        let mut removed = false;
        match search_result {
            Ok(index) => {
                let value = leaf_node_exclusive.storage.get_value(index);
                if predicate(value) {
                    leaf_node_exclusive.remove_at_index(search_result.unwrap());
                    self.root.len.fetch_sub(1, Ordering::Relaxed);
                    removed = true;

                    if leaf_node_exclusive.num_keys() < MIN_KEYS_PER_NODE {
                        coalesce_or_redistribute_leaf_node(search_stack);
                        return true;
                    }
                }
            }
            Err(_) => {
                // nothing to remove
            }
        }
        search_stack.drain().for_each(|n| {
            n.assert_exclusive().unlock_exclusive();
        });
        removed
    }

    pub fn modify_if(
        &self,
        key: &K,
        predicate: impl Fn(&V) -> bool,
        modify_fn: impl FnOnce(QsOwned<V>) -> QsOwned<V>,
    ) -> bool {
        debug_println!("top-level modify_if {:?}", key);

        let mut cursor = self.cursor_mut();
        let found = cursor.seek(key);
        if !found {
            return false;
        }
        let entry = cursor.current().unwrap();
        let value = entry.value();
        if !predicate(value) {
            return false;
        }
        cursor.modify_value(modify_fn);
        true
    }

    pub fn print_tree(&self) {
        let root = self.root.as_node_ref().lock_shared();
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree length: {}      |", self.len());
        println!("+----------------------+");
        match SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree(),
        )
        .assume_unlocked()
        .force()
        {
            SharedDiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.print_node();
                internal.unlock_shared();
            }
            SharedDiscriminatedNode::Leaf(leaf) => {
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
        match SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree(),
        )
        .assume_unlocked()
        .force()
        {
            SharedDiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.check_invariants();
                internal.unlock_shared();
            }
            SharedDiscriminatedNode::Leaf(leaf) => {
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
        CursorMut::new(self)
    }
}

unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Send for BTree<K, V> {}
unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Sync for BTree<K, V> {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ModificationType {
    Insertion,
    Removal,
    NonModifying,
}

pub(crate) enum GetOrInsertResult<V: BTreeValue + ?Sized> {
    Inserted,
    GotReturningNewValue(QsOwned<V>),
}

pub enum InsertOrModifyIfResult {
    Inserted,
    Modified,
    DidNothing,
}

#[cfg(test)]
mod tests {
    use crate::array_types::ORDER;
    use qsbr::qsbr_reclaimer;
    use thin::Owned;

    use super::*;
    use std::ops::Deref;

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
            tree.insert(QsArc::new(i), QsOwned::new(value.clone()));
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
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    use crate::sync::AtomicUsize;
    use rand::rngs::StdRng;
    use rand::seq::IteratorRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;

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
            let random_seed: u64 = rand::rng().random();
            println!("Using random seed: {}", random_seed);
            run_random_operations_with_seed_single_threaded(random_seed);
        }
    }

    fn run_random_operations_with_seed_single_threaded(seed: u64) {
        qsbr_reclaimer().register_thread();
        {
            let mut rng = StdRng::seed_from_u64(seed);
            println!("about to call btree new");
            let tree = BTree::<usize, String>::new();
            println!("back from btree new");
            let mut reference_map = HashMap::new();
            println!("Using seed: {}", seed);

            // Perform random operations for a while
            for _ in 0..NUM_OPERATIONS {
                let operation = rng.random_range(0..3);
                match operation {
                    0 => {
                        // Random insert
                        let key = rng.random_range(0..1000);
                        let value = format!("value{}", key);
                        tree.insert(QsArc::new(key), QsOwned::new(value.clone()));
                        reference_map.insert(key, value);
                        tree.check_invariants();
                    }
                    1 => {
                        // Random get
                        let key = rng.random_range(0..1000);
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
            unsafe { qsbr_reclaimer().mark_current_thread_quiescent() };

            // Verify all keys at the end
            for key in reference_map.keys() {
                assert_eq!(
                    tree.get(key).as_deref(),
                    reference_map.get(key),
                    "Final verification failed for key {}",
                    key
                );
            }
        }
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_random_inserts_gets_and_removes_with_seed_multi_threaded() {
        for &seed in &INTERESTING_SEEDS {
            run_random_operations_with_seed_multi_threaded(seed);
        }

        // also run with a random seed
        let random_seed: u64 = rand::rng().random();
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
                        let operation = rng.random_range(0..3);
                        match operation {
                            0 => {
                                let key = rng.random_range(0..1000);
                                let value = format!("value{}", key);
                                tree_ref.insert(QsArc::new(key), QsOwned::new(value));
                            }
                            1 => {
                                let key = rng.random_range(0..1000);
                                let _ = tree_ref.get(&key);
                            }
                            2 => {
                                let key = rng.random_range(0..1000);
                                tree_ref.remove(&key);
                            }
                            _ => unreachable!(),
                        }
                    }
                    // Increment the counter when this thread completes
                    unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                    completed_threads.fetch_add(1, Ordering::Release);
                });
            }

            // Spawn invariant checking thread
            let completed_threads = completed_threads.clone();
            let tree_ref = &tree;
            s.spawn(move || {
                qsbr_reclaimer().register_thread();
                let start_time = std::time::Instant::now();
                while completed_threads.load(Ordering::Acquire) < num_threads {
                    if start_time.elapsed() >= Duration::from_secs(10) {
                        break;
                    }
                    thread::sleep(Duration::from_secs(1));
                    tree_ref.check_invariants();
                }
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
            });
        });

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn insert_owned() {
        let _guard = qsbr_reclaimer().guard();
        let tree = BTree::<usize, str>::new();
        let key = QsArc::new(5);
        let value = Owned::new_from_str("value5");
        tree.insert(key, value);
    }

    #[test]
    fn test_get_or_insert() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, str>::new();

        {
            println!("Case 1: Insert into empty tree");
            // Case 1: Insert into empty tree
            let mut cursor = tree.get_or_insert(QsArc::new(5), QsOwned::new_from_str("value5"));
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
            let cursor = tree.get_or_insert(QsArc::new(5), QsOwned::new_from_str("new_value5"));
            assert_eq!(cursor.current().unwrap().key(), &5);
            assert_eq!(cursor.current().unwrap().value(), &"value5".to_string());
            // Should keep old value
        }
        {
            println!("Case 3: Insert when there's capacity (no split needed)");
            // Case 3: Insert when there's capacity (no split needed)
            let mut cursor = tree.get_or_insert(QsArc::new(3), QsOwned::new_from_str("value3"));
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
                let cursor = tree.get_or_insert(
                    QsArc::new(i * 2),
                    QsOwned::new_from_str(&format!("value{}", i * 2)),
                );
                assert_eq!(cursor.current().unwrap().key(), &(i * 2));
                assert_eq!(
                    cursor.current().unwrap().value(),
                    &format!("value{}", i * 2)
                );
            }

            // Insert one more entry that should cause a split
            let split_key = ORDER * 2;
            let mut cursor = tree.get_or_insert(
                QsArc::new(split_key),
                QsOwned::new_from_str(&format!("value{}", split_key)),
            );

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

        // Verify tree invariants after all operations
        tree.check_invariants();

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_remove_if() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert enough elements to ensure multiple leaves
        let n = ORDER * 3; // Use 3 times ORDER to ensure multiple leaves
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(format!("{}", i)));
        }

        // Test 1: Remove only even numbers
        for i in 0..n {
            tree.remove_if(&i, |v| v.parse::<usize>().unwrap() % 2 == 0);
        }

        // Verify: Even numbers should be removed, odd numbers should remain
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        let expected_remaining = (0..n).filter(|i| i % 2 != 0).collect::<Vec<_>>();
        let mut actual_remaining = Vec::new();
        while let Some(entry) = cursor.current() {
            actual_remaining.push(*entry.key());
            cursor.move_next();
        }
        assert_eq!(actual_remaining, expected_remaining);
        assert_eq!(tree.len(), n / 2);

        // Test 2: Try to remove with a predicate that always returns false
        for i in 0..n {
            tree.remove_if(&i, |_| false);
        }
        // Verify: No elements should be removed
        cursor.seek_to_start();
        let mut count = 0;
        while cursor.current().is_some() {
            count += 1;
            cursor.move_next();
        }
        assert_eq!(count, n / 2); // Same count as before

        // Test 3: Remove elements that would cause redistribution/coalescing
        // First, remove enough elements to force structural modifications
        for i in (0..n).step_by(2) {
            if i % 4 == 1 {
                // Remove every fourth element
                tree.remove_if(&i, |v| v.contains('1')); // Only remove values containing '1'
            }
        }
        tree.check_invariants();

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    #[cfg(feature = "shuttle")]
    fn test_concurrent_operations_under_shuttle() {
        shuttle::check_random(
            || {
                // Register the main thread with QSBR
                qsbr_reclaimer().register_thread();
                // Create a shared B-tree
                let tree = Arc::new(BTree::<usize, str>::new());
                let insert_count = Arc::new(AtomicUsize::new(0));
                let remove_count = Arc::new(AtomicUsize::new(0));
                let get_count = Arc::new(AtomicUsize::new(0));

                // Pre-populate the tree with some values that will be targets for removal and gets
                for i in 0..50 {
                    tree.insert(
                        QsArc::new(i),
                        QsOwned::new_from_str(&format!("initial-value-{}", i)),
                    );
                }

                // Create threads for different operations
                let mut handles = Vec::new();

                // Create insert threads
                for thread_id in 0..3 {
                    let tree_clone = Arc::clone(&tree);
                    let insert_count_clone = Arc::clone(&insert_count);

                    let handle = shuttle::thread::spawn(move || {
                        // Register this thread with QSBR
                        qsbr_reclaimer().register_thread();

                        for i in 0..10 {
                            // Use a unique key for each thread and insertion
                            let key = thread_id * 100 + i + 100; // Start at 100 to avoid overlap with pre-populated values
                            let value = format!("insert-{}-{}", thread_id, i);

                            // Insert the key-value pair
                            tree_clone.insert(QsArc::new(key), QsOwned::new_from_str(&value));

                            // Verify the insertion worked
                            let result = tree_clone.get(&key);
                            if result.as_deref() == Some(&value) {
                                insert_count_clone.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        // Deregister this thread from QSBR and mark it quiescent
                        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                    });

                    handles.push(handle);
                }

                // Create remove threads
                for thread_id in 0..3 {
                    let tree_clone = Arc::clone(&tree);
                    let remove_count_clone = Arc::clone(&remove_count);

                    let handle = shuttle::thread::spawn(move || {
                        // Register this thread with QSBR
                        qsbr_reclaimer().register_thread();

                        for i in 0..10 {
                            // Remove from the pre-populated values
                            let key = thread_id * 10 + i;

                            // Remove the key-value pair
                            tree_clone.remove(&key);

                            // Verify the removal worked
                            let result = tree_clone.get(&key);
                            if result.is_none() {
                                remove_count_clone.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        // Deregister this thread from QSBR and mark it quiescent
                        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                    });

                    handles.push(handle);
                }

                // Create get threads
                for thread_id in 0..3 {
                    let tree_clone = Arc::clone(&tree);
                    let get_count_clone = Arc::clone(&get_count);

                    let handle = shuttle::thread::spawn(move || {
                        // Register this thread with QSBR
                        qsbr_reclaimer().register_thread();

                        for i in 0..10 {
                            // Get from the pre-populated values that shouldn't be removed
                            let key = 30 + thread_id * 5 + i % 5; // This ensures we're accessing values that won't be removed

                            // Get the key-value pair
                            let result = tree_clone.get(&key);
                            if result.is_some() {
                                get_count_clone.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        // Deregister this thread from QSBR and mark it quiescent
                        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                    });

                    handles.push(handle);
                }

                // Join all threads
                for handle in handles {
                    handle.join().unwrap();
                }

                // // Verify some specific inserted values
                assert_eq!(tree.get(&100).as_deref(), Some("insert-0-0"));

                // // Verify some specific removed values
                assert_eq!(tree.get(&5), None);

                // // Verify some specific values that should still exist
                assert_eq!(tree.get(&45).as_deref(), Some("initial-value-45"));

                tree.check_invariants();

                // Deregister the main thread from QSBR and mark it quiescent
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
            },
            1000, // Number of iterations
        );
    }

    #[test]
    fn test_len_updates() {
        qsbr_reclaimer().register_thread();

        // Test 1: Insert and remove
        {
            let tree = BTree::<usize, String>::new();
            assert_eq!(tree.len(), 0, "Initial tree length should be 0");

            // Insert 10 elements
            for i in 0..10 {
                tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
                assert_eq!(
                    tree.len(),
                    i + 1,
                    "Tree length should be {} after {} inserts",
                    i + 1,
                    i + 1
                );
            }

            // Remove 5 elements
            for i in 0..5 {
                tree.remove(&i);
                assert_eq!(
                    tree.len(),
                    10 - (i + 1),
                    "Tree length should be {} after removing {} elements",
                    10 - (i + 1),
                    i + 1
                );
            }
        }

        // Test 2: bulk_load
        {
            let mut pairs = Vec::new();
            for i in 0..100 {
                pairs.push((QsArc::new(i), QsOwned::new(format!("value{}", i))));
            }
            let tree = BTree::bulk_load(pairs);
            assert_eq!(tree.len(), 100, "Tree length should be 100 after bulk_load");
            drop(tree);
        }

        // Test 3: bulk_load_parallel
        #[cfg(not(miri))]
        {
            let mut pairs = Vec::new();
            for i in 0..100 {
                pairs.push((QsArc::new(i), QsOwned::new(format!("value{}", i))));
            }
            let tree = BTree::bulk_load_parallel(pairs);
            assert_eq!(
                tree.len(),
                100,
                "Tree length should be 100 after bulk_load_parallel"
            );
        }

        // Test 4: get_or_insert
        {
            let tree = BTree::<usize, str>::new();
            assert_eq!(tree.len(), 0, "Initial tree length should be 0");

            // Insert 10 elements using get_or_insert
            for i in 0..10 {
                let cursor = tree
                    .get_or_insert(QsArc::new(i), QsOwned::new_from_str(&format!("value{}", i)));
                assert_eq!(
                    tree.len(),
                    i + 1,
                    "Tree length should be {} after {} get_or_inserts",
                    i + 1,
                    i + 1
                );
                assert_eq!(cursor.current().unwrap().key(), &i);
            }

            // Call get_or_insert on existing keys (should not change length)
            for i in 0..10 {
                let cursor = tree.get_or_insert(
                    QsArc::new(i),
                    QsOwned::new_from_str(&format!("new_value{}", i)),
                );
                assert_eq!(
                    tree.len(),
                    10,
                    "Tree length should still be 10 after get_or_insert on existing keys"
                );
                assert_eq!(cursor.current().unwrap().key(), &i);
                // Value should not change
                assert_eq!(cursor.current().unwrap().value(), &format!("value{}", i));
            }
        }

        #[cfg(not(miri))]
        // Test 5: bulk_update_parallel (should not change length)
        {
            let tree = BTree::<usize, str>::new();

            // Insert initial elements
            for i in 0..20 {
                tree.insert(QsArc::new(i), QsOwned::new_from_str(&format!("value{}", i)));
            }
            assert_eq!(tree.len(), 20, "Tree length should be 20 after inserts");

            // Update existing elements
            let updates: Vec<(QsArc<usize>, QsOwned<str>)> = (0..20)
                .map(|i| {
                    (
                        QsArc::new(i),
                        QsOwned::new_from_str(&format!("updated_value{}", i)),
                    )
                })
                .collect();

            tree.bulk_update_parallel(updates);
            assert_eq!(
                tree.len(),
                20,
                "Tree length should still be 20 after bulk_update_parallel"
            );
        }

        #[cfg(not(miri))]
        // Test 6: bulk_insert_or_update_parallel
        {
            let tree = BTree::<usize, str>::new();

            // Insert initial elements (even numbers)
            for i in 0..20 {
                if i % 2 == 0 {
                    tree.insert(QsArc::new(i), QsOwned::new_from_str(&format!("value{}", i)));
                }
            }
            assert_eq!(
                tree.len(),
                10,
                "Tree length should be 10 after inserting even numbers"
            );

            // Mix of updates (even numbers) and inserts (odd numbers)
            let entries: Vec<(QsArc<usize>, QsOwned<str>)> = (0..20)
                .map(|i| {
                    if i % 2 == 0 {
                        (
                            QsArc::new(i),
                            QsOwned::new_from_str(&format!("updated_value{}", i)),
                        )
                    } else {
                        (QsArc::new(i), QsOwned::new_from_str(&format!("value{}", i)))
                    }
                })
                .collect();

            // Define update function
            let update_fn = |old_value: QsOwned<str>, _| {
                QsOwned::new_from_str(&format!("updated_{}", old_value.deref()))
            };

            tree.bulk_insert_or_update_parallel(entries, &update_fn);
            assert_eq!(
                tree.len(),
                20,
                "Tree length should be 20 after bulk_insert_or_update_parallel"
            );
        }

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_modify_if() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert some test data
        for i in 0..10 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test 1: Modify with predicate that returns true
        tree.modify_if(
            &5,
            |v| v.contains("value"),
            |_| QsOwned::new("modified5".to_string()),
        );
        assert_eq!(tree.get(&5).as_deref(), Some(&"modified5".to_string()));

        // Test 2: Try to modify with predicate that returns false (should not modify)
        tree.modify_if(
            &5,
            |v| v.contains("nonexistent"),
            |_| QsOwned::new("should_not_change".to_string()),
        );
        assert_eq!(
            tree.get(&5).as_deref(),
            Some(&"modified5".to_string()),
            "Value should not change when predicate returns false"
        );

        // Test 3: Modify based on existing value
        tree.modify_if(
            &3,
            |_| true,
            |old_val| QsOwned::new(format!("updated_{}", old_val.deref())),
        );
        assert_eq!(tree.get(&3).as_deref(), Some(&"updated_value3".to_string()));

        // Test 4: Try to modify non-existent key (should do nothing)
        tree.modify_if(
            &999,
            |_| true,
            |_| QsOwned::new("should_not_insert".to_string()),
        );
        assert_eq!(tree.get(&999), None, "Non-existent key should remain None");

        // Test 5: Modify with selective predicate
        for i in 0..10 {
            tree.modify_if(
                &i,
                |v| v.starts_with("value"),
                |old_val| QsOwned::new(format!("prefix_{}", old_val.deref())),
            );
        }
        // Check that keys 0, 1, 2, 4, 6, 7, 8, 9 were modified (they still had "value" prefix)
        assert_eq!(tree.get(&0).as_deref(), Some(&"prefix_value0".to_string()));
        assert_eq!(tree.get(&1).as_deref(), Some(&"prefix_value1".to_string()));
        // Key 3 should still be "updated_value3" since it doesn't start with "value"
        assert_eq!(
            tree.get(&3).as_deref(),
            Some(&"updated_value3".to_string()),
            "Key 3 should not be modified since 'updated_value3' doesn't start with 'value'"
        );
        // Key 5 was already "modified5", which doesn't start with "value", so it shouldn't change
        assert_eq!(
            tree.get(&5).as_deref(),
            Some(&"modified5".to_string()),
            "Key 5 should not be modified since it doesn't match predicate"
        );

        tree.check_invariants();

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }
}
