use crate::array_types::{MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE};
use crate::coalescing::coalesce_or_redistribute_leaf_node;
use crate::debug_println;
use crate::graceful_pointers::GracefulArc;
use crate::leaf_node::LeafNode;
use crate::node::{
    debug_assert_no_locks_held, debug_assert_one_shared_lock_held, Height, NodeHeader,
};
use crate::node_ptr::{marker, DiscriminatedNode, NodePtr, NodeRef};
use crate::reference::Ref;
use crate::search::{
    get_leaf_exclusively_using_exclusive_search, get_leaf_exclusively_using_optimistic_search,
    get_leaf_exclusively_using_shared_search, get_leaf_shared_using_optimistic_search,
    get_leaf_shared_using_shared_search,
};
use crate::splitting::insert_into_leaf_after_splitting;
use std::cell::UnsafeCell;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub trait BTreeKey: PartialOrd + Ord + Clone + Debug + Display + Send + 'static {}
pub trait BTreeValue: Debug + Display + Send + 'static {}

impl<K: PartialOrd + Ord + Clone + Debug + Display + Send + 'static> BTreeKey for K {}
impl<V: Debug + Display + Send + 'static> BTreeValue for V {}

/// B+Tree
/// Todo
/// - implement iterators
/// - bulk loading
/// Perf ideas:
/// - try inlined key descriminator with node-level key prefixes
/// - experiment with hybrid latch strategies:
///   - how many tries should we give optimistic locks?
///   - try being more pessimistic once we're close to the leaves
///   - try switching between shared and optimstic as we descend
/// - try the "no coalescing" or "relaxed" btree idea
/// - try unordered leaf storage, or lazily sorted leaf storage

pub struct BTree<K: BTreeKey, V: BTreeValue> {
    root: RootNode<K, V>,
}

impl<K: BTreeKey, V: BTreeValue> BTree<K, V> {
    pub fn new() -> Self {
        BTree {
            root: RootNode::new(),
        }
    }
}

unsafe impl<K: BTreeKey, V: BTreeValue> Send for BTree<K, V> {}
unsafe impl<K: BTreeKey, V: BTreeValue> Sync for BTree<K, V> {}

impl<K: BTreeKey, V: BTreeValue> Deref for BTree<K, V> {
    type Target = RootNode<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

#[repr(C)]
pub struct RootNode<K: BTreeKey, V: BTreeValue> {
    pub header: NodeHeader,
    pub inner: UnsafeCell<RootNodeInner<K, V>>,
    // this is updated atomically after insert/remove, so it's not perfectly consistent
    // but that lets us avoid some extra locking -- otherwise we'd need to hold a lock during any
    // insert/remove for the duration of the operation
    pub len: AtomicUsize,
}

pub struct RootNodeInner<K: BTreeKey, V: BTreeValue> {
    pub top_of_tree: NodePtr,
    phantom: PhantomData<(K, V)>,
}

impl<K: BTreeKey, V: BTreeValue> Drop for RootNodeInner<K, V> {
    fn drop(&mut self) {
        let top_of_tree_ref =
            NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
                self.top_of_tree,
            );
        match top_of_tree_ref.force() {
            DiscriminatedNode::Leaf(leaf) => unsafe {
                ptr::drop_in_place(leaf.to_raw_leaf_ptr());
            },
            DiscriminatedNode::Internal(internal) => {
                let internal_node_ptr = internal.to_raw_internal_ptr();
                unsafe {
                    internal_node_ptr.as_ref().unwrap().drop_node_recursively();
                }
            }
            _ => panic!("RootNodeInner::drop: top_of_tree is not a leaf or internal node"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ModificationType {
    Insertion,
    Removal,
    NonModifying,
}

impl<K: BTreeKey, V: BTreeValue> RootNode<K, V> {
    pub fn new() -> Self {
        let top_of_tree = LeafNode::<K, V>::new();
        RootNode {
            header: NodeHeader::new(Height::Root),
            inner: UnsafeCell::new(RootNodeInner {
                top_of_tree: NodePtr(NonNull::new(top_of_tree as *mut NodeHeader).unwrap()),
                phantom: PhantomData,
            }),
            len: AtomicUsize::new(0),
        }
    }

    pub fn as_node_ref(&self) -> NodeRef<K, V, marker::Unlocked, marker::Root> {
        NodeRef::from_root_unlocked(self as *const _ as *mut _)
    }

    /// Locks the leaf node (shared) and returns a reference to the value
    /// The leaf will be unlocked when the reference is dropped
    pub fn get(&self, search_key: &K) -> Option<Ref<K, V>> {
        debug_println!("top-level get {:?}", search_key);

        // try optimistic search first
        if let Ok(locked_root) = self.as_node_ref().lock_optimistic() {
            if let Ok(leaf_node_shared) =
                get_leaf_shared_using_optimistic_search(locked_root, search_key)
            {
                match leaf_node_shared.get(search_key) {
                    Some(v_ptr) => return Some(Ref::new(leaf_node_shared, v_ptr)),
                    None => {
                        leaf_node_shared.unlock_shared();
                        return None;
                    }
                }
            }
        }

        // fall back to shared search
        let leaf_node_shared =
            get_leaf_shared_using_shared_search(self.as_node_ref().lock_shared(), search_key);
        debug_println!("top-level get {:?} done", search_key);
        debug_assert_one_shared_lock_held();
        match leaf_node_shared.get(search_key) {
            Some(v_ptr) => Some(Ref::new(leaf_node_shared, v_ptr)),
            None => {
                leaf_node_shared.unlock_shared();
                None
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /**
     * Removal methods:
     * - remove - the top-level remove method
     * - coalesce_or_redistribute_leaf_node - called when a leaf node has too few keys
     * - coalesce_or_redistribute_internal_node - called when an internal node has too few keys
     */

    pub fn remove(&self, key: &K) {
        debug_println!("top-level remove {:?}", key);

        if let Ok(locked_root) = self.as_node_ref().lock_optimistic() {
            let mut optimistic_leaf =
                // first try fully optimistic search
                match get_leaf_exclusively_using_optimistic_search(locked_root, key) {
                    Ok(leaf) => leaf,
                    // if that doesn't work, use shared search, which is still optimistic
                    // in the sense that we're assuming we don't need any structural modifications
                    Err(_) => get_leaf_exclusively_using_shared_search(
                        self.as_node_ref().lock_shared(),
                        key,
                    ),
                };
            if optimistic_leaf.has_capacity_for_modification(ModificationType::Removal)
                || optimistic_leaf.get(&key).is_none()
            {
                let removed = optimistic_leaf.remove(key);
                if removed {
                    self.len.fetch_sub(1, Ordering::Relaxed);
                }
                debug_println!("top-level remove {:?} done - removed? {:?}", key, removed);
                optimistic_leaf.unlock_exclusive();
                debug_assert_no_locks_held::<'r'>();
                return;
            }
            optimistic_leaf.unlock_exclusive();
        }

        // we need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.as_node_ref().lock_exclusive(),
            key,
            ModificationType::Removal,
        );

        let mut leaf_node_exclusive = search_stack.peek_lowest().assert_leaf().assert_exclusive();
        let removed = leaf_node_exclusive.remove(key);
        if removed {
            self.len.fetch_sub(1, Ordering::Relaxed);
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

    pub fn insert(&self, key: Box<K>, value: Box<V>) {
        debug_println!("top-level insert {:?}", key);
        let graceful_key = GracefulArc::new(*key);

        // TODO(ak): should we try a few times optimistically?
        // first try fully optimistic search
        if let Ok(locked_root) = self.as_node_ref().lock_optimistic() {
            let mut optimistic_leaf =
                match get_leaf_exclusively_using_optimistic_search(locked_root, &graceful_key) {
                    Ok(leaf) => leaf,
                    // if that doesn't work, use shared search, which is still optimistic
                    // in the sense that we're assuming we don't need any structural modifications
                    Err(_) => get_leaf_exclusively_using_shared_search(
                        self.as_node_ref().lock_shared(),
                        &graceful_key,
                    ),
                };
            if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion)
                || optimistic_leaf.get(&graceful_key).is_some()
            {
                optimistic_leaf.insert(graceful_key, Box::into_raw(value));
                optimistic_leaf.unlock_exclusive();
                self.len.fetch_add(1, Ordering::Relaxed);
                debug_assert_no_locks_held::<'i'>();
                return;
            }
            optimistic_leaf.unlock_exclusive();
        }

        // we need structural modifications, so fall back to exclusive search
        let mut search_stack = get_leaf_exclusively_using_exclusive_search(
            self.as_node_ref().lock_exclusive(),
            &graceful_key,
            ModificationType::Insertion,
        );
        let mut leaf_node = search_stack.peek_lowest().assert_leaf().assert_exclusive();

        // this should get us to 3 keys in the leaf
        if leaf_node.num_keys() < MAX_KEYS_PER_NODE {
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
        self.len.fetch_add(1, Ordering::Relaxed);
        debug_println!("top-level insert done");
        debug_assert_no_locks_held::<'i'>();
    }

    pub fn print_tree(&self) {
        let root = self.as_node_ref().lock_shared();
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree length: {}      |", self.len.load(Ordering::Relaxed));
        println!("+----------------------+");
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree,
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
        let root = self.as_node_ref().lock_shared();
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree,
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
}

#[cfg(test)]
mod tests {
    use crate::array_types::ORDER;
    use crate::qsbr::qsbr_reclaimer;

    use super::*;

    #[test]
    fn test_insert_and_get() {
        qsbr_reclaimer().register_thread();

        let tree = BTree::<usize, String>::new();
        let n = ORDER.pow(2);
        for i in 1..=n {
            let value = format!("value{}", i);
            tree.insert(Box::new(i), Box::new(value.clone()));
            tree.check_invariants();
            let result = tree.get(&i);
            assert_eq!(result.as_deref(), Some(&value));
        }

        println!("tree should be full:");
        tree.print_tree();

        assert_eq!(tree.get(&1).unwrap(), &"value1".to_string());
        assert_eq!(tree.get(&2).unwrap(), &"value2".to_string());
        assert_eq!(tree.get(&3).unwrap(), &"value3".to_string());

        // Remove all elements in sequence
        // this will force the tree to coalesce and redistribute
        for i in 1..=n {
            println!("removing {}", i);
            tree.remove(&i);
            tree.check_invariants();
            assert_eq!(tree.get(&i), None);
        }

        // Check that elements are removed
        assert_eq!(tree.get(&1), None);
        assert_eq!(tree.get(&2), None);
        assert_eq!(tree.get(&3), None);
    }

    use rand::rngs::StdRng;
    use rand::seq::IteratorRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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
        for _ in 0..100000 {
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
                        tree.print_tree();
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
        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();

        println!("tree.print_tree()");
        tree.check_invariants();
        tree.print_tree();
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

    const INTERESTING_SEEDS: [u64; 2] = [13142251578868436595, 15830960132082815423];

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
        let operations_per_thread = 25000;

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
}
