use crate::array_types::{
    InternalChildTempArray, InternalKeyTempArray, LeafTempArray, KV_IDX_CENTER, MAX_KEYS_PER_NODE,
    MIN_KEYS_PER_NODE,
};
use crate::debug_println;
use crate::internal_node::{InternalNode, InternalNodeInner};
use crate::leaf_node::{LeafNode, LeafNodeInner};
use crate::node::{
    debug_assert_no_locks_held, debug_assert_one_shared_lock_held, Height, NodeHeader,
};
use crate::node_ptr::marker::NodeType;
use crate::node_ptr::{marker, DiscriminatedNode, NodePtr, NodeRef};
use crate::reference::Ref;
use crate::search_dequeue::SearchDequeue;
use crate::util::UnwrapEither;
use smallvec::SmallVec;
use std::cell::UnsafeCell;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub trait BTreeKey: PartialOrd + Ord + Clone + Debug + Display {}
pub trait BTreeValue: Debug + Display {}

impl<K: PartialOrd + Ord + Clone + Debug + Display> BTreeKey for K {}
impl<V: Debug + Display> BTreeValue for V {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UnderfullNodePosition {
    Leftmost,
    Other,
}

/// B+Tree
/// Todo
/// - implement EBR for keys, values and nodes
/// - try inlined key descriminator with node-level key prefixes
/// - implement iterators
/// - bulk loading
/// Perf ideas:
/// - experiment with hybrid latch strategies:
///   - how many tries should we give optimistic locks?
///   - try being more pessimistic once we're close to the leaves
///   - try switching between shared and optimstic as we descend
/// - try the "no coalescing" or "relaxed" btree idea
/// - experiment with blockId/offset-based key storage and use that to avoid atomics since we're bounds-checking the result
///
///   Problems and answers:

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
    top_of_tree: NodePtr,
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

    fn as_node_ptr(&self) -> NodeRef<K, V, marker::Unlocked, marker::Root> {
        NodeRef::from_root_unlocked(self as *const _ as *mut _)
    }

    fn get_leaf_shared_using_optimistic_search(
        &self,
        search_key: &K,
    ) -> Result<NodeRef<K, V, marker::Shared, marker::Leaf>, ()> {
        let locked_root = self.as_node_ptr().lock_optimistic()?;
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_optimistic()?.assert_internal();
            prev_node.unlock_optimistic()?;
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }
        let leaf = current_node.lock_shared();
        Ok(leaf.assert_leaf())
    }

    fn get_leaf_shared_using_shared_serach(
        &self,
        search_key: &K,
    ) -> NodeRef<K, V, marker::Shared, marker::Leaf> {
        let locked_root = self.as_node_ptr().lock_shared();
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_shared().assert_internal();
            prev_node.unlock_shared();
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }

        let leaf = current_node.lock_shared().assert_leaf();
        prev_node.unlock_shared();
        leaf
    }

    fn get_leaf_exclusively_using_optimistic_search(
        &self,
        search_key: &K,
    ) -> Result<NodeRef<K, V, marker::Exclusive, marker::Leaf>, ()> {
        let locked_root = self.as_node_ptr().lock_optimistic()?;
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_optimistic()?.assert_internal();
            prev_node.unlock_optimistic()?;
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }

        let leaf = current_node.lock_exclusive().assert_leaf();
        match prev_node.unlock_optimistic() {
            Ok(_) => Ok(leaf),
            Err(_) => {
                leaf.unlock_exclusive();
                Err(())
            }
        }
    }

    fn get_leaf_exclusively_using_shared_search(
        &self,
        search_key: &K,
    ) -> NodeRef<K, V, marker::Exclusive, marker::Leaf> {
        let locked_root = self.as_node_ptr().lock_shared();
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_shared().assert_internal();
            prev_node.unlock_shared();
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }

        let leaf = current_node.lock_exclusive().assert_leaf();
        prev_node.unlock_shared();
        leaf
    }

    fn get_leaf_exclusively_using_exclusive_search(
        &self,
        search_key: &K,
        modification_type: ModificationType,
    ) -> SearchDequeue<K, V> {
        let mut search = SearchDequeue::new();
        let locked_root = self.as_node_ptr().lock_exclusive();

        search.push_node_on_bottom(locked_root);
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);
        let top_of_tree = top_of_tree.lock_exclusive();
        search.push_node_on_bottom(top_of_tree);
        match top_of_tree.force() {
            DiscriminatedNode::Leaf(leaf) => {
                if leaf.has_capacity_for_modification_as_top_of_tree(modification_type) {
                    search
                        .pop_highest()
                        .assert_root()
                        .assert_exclusive()
                        .unlock_exclusive();
                }
            }
            DiscriminatedNode::Internal(internal) => {
                if internal.has_capacity_for_modification_as_top_of_tree(modification_type) {
                    search
                        .pop_highest()
                        .assert_root()
                        .assert_exclusive()
                        .unlock_exclusive();
                }
            }
            _ => unreachable!(),
        }

        let mut current_node = search.peek_lowest();

        while current_node.is_internal() {
            let current_exclusive = current_node.assert_internal().assert_exclusive();
            let found_child =
                NodeRef::from_unknown_node_ptr(current_exclusive.find_child(search_key));
            let found_child = found_child.lock_exclusive();
            search.push_node_on_bottom(found_child);
            match found_child.force() {
                DiscriminatedNode::Leaf(leaf) => {
                    if leaf.has_capacity_for_modification(modification_type) {
                        search.pop_highest_until(found_child).for_each(|n| {
                            n.assert_exclusive().unlock_exclusive();
                        });
                    }
                }
                DiscriminatedNode::Internal(internal) => {
                    if internal.has_capacity_for_modification(modification_type) {
                        search.pop_highest_until(found_child).for_each(|n| {
                            n.assert_exclusive().unlock_exclusive();
                        });
                    }
                }
                _ => unreachable!(),
            }
            current_node = search.peek_lowest();
        }
        debug_println!(
            "find_leaf {:?} found {:?}",
            search_key,
            search.peek_lowest()
        );
        search
    }

    /// Locks the leaf node (shared) and returns a reference to the value
    /// The leaf will be unlocked when the reference is dropped
    pub fn get(&self, search_key: &K) -> Option<Ref<K, V>> {
        debug_println!("top-level get {:?}", search_key);
        if let Ok(leaf_node_shared) = self.get_leaf_shared_using_optimistic_search(search_key) {
            match leaf_node_shared.get(search_key) {
                Some(v_ptr) => return Some(Ref::new(leaf_node_shared, v_ptr)),
                None => {
                    leaf_node_shared.unlock_shared();
                    return None;
                }
            }
        }

        let leaf_node_shared = self.get_leaf_shared_using_shared_serach(search_key);
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
        let mut optimistic_leaf = match self.get_leaf_exclusively_using_optimistic_search(&key) {
            Ok(leaf) => leaf,
            Err(_) => self.get_leaf_exclusively_using_shared_search(&key),
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
        let mut search_stack =
            self.get_leaf_exclusively_using_exclusive_search(key, ModificationType::Removal);
        let mut leaf_node_exclusive = search_stack.peek_lowest().assert_leaf().assert_exclusive();
        let removed = leaf_node_exclusive.remove(key);
        if removed {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        if leaf_node_exclusive.num_keys() < MIN_KEYS_PER_NODE {
            self.coalesce_or_redistribute_leaf_node(search_stack);
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

    fn coalesce_or_redistribute_leaf_node(&self, mut search_stack: SearchDequeue<K, V>) {
        debug_println!("coalesce_or_redistribute_leaf_node");
        let locked_underfull_leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();

        // if the leaf is the top_of_tree, we don't need to do anything -- we let it get under-full
        if search_stack.peek_lowest().is_root() {
            debug_println!("not coalescing or redistributing leaf at top of tree");
            // unlock the leaf (which is top of tree)
            locked_underfull_leaf.unlock_exclusive();
            debug_assert!(search_stack.is_empty());
            return;
        }

        let locked_parent = search_stack
            .peek_lowest()
            .assert_internal()
            .assert_exclusive();

        // a neighbor is the node to the left, except in the case of the leftmost child,
        // where it's one to the right
        let (full_leaf_node, node_position) =
            locked_parent.get_neighbor_of_underfull_leaf(locked_underfull_leaf);

        // we always need to lock the neighbor exclusive -- we're either coalescing or redistributing
        // both of which modify the neighbor
        // this is safe from deadlocks because we're holding the parent exclusive and we only block
        // on "downwards" locks
        let locked_full_leaf = full_leaf_node.lock_exclusive();

        if locked_underfull_leaf.num_keys() + locked_full_leaf.num_keys() < MAX_KEYS_PER_NODE {
            match node_position {
                UnderfullNodePosition::Other => {
                    // if the neighbor is to the left (the common case),
                    // we absorb the underfull leaf into its neighbor
                    self.coalesce_into_left_leaf_from_right_neighbor(
                        locked_full_leaf,      // the left-hand leaf
                        locked_underfull_leaf, // its right neighbor, to be absorbed
                        locked_parent,
                        search_stack,
                    );
                }
                UnderfullNodePosition::Leftmost => {
                    // in the case of the leftmost child, we don't want to hop to a different parent
                    // so we absorb the full leaf into the underfull leaf
                    // in both calls, the leftmost leaf is the first argument
                    self.coalesce_into_left_leaf_from_right_neighbor(
                        locked_underfull_leaf, // the left-hand leaf
                        locked_full_leaf,      // it's right-hand neighbor, to be absorbed
                        locked_parent,
                        search_stack,
                    );
                }
            }
        } else {
            search_stack.pop_highest_until(locked_parent).for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
            // no need for the full stack here -- if we're redistributing, the changes are local to the two leaves
            self.redistribute_into_underfull_leaf_from_neighbor(
                locked_underfull_leaf,
                locked_full_leaf,
                node_position,
                locked_parent,
            );
        }
    }

    fn coalesce_into_left_leaf_from_right_neighbor(
        &self,
        left_leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        right_leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        search_stack: SearchDequeue<K, V>, // TODO(ak): this should be a reference, right?
    ) {
        debug_println!("coalesce_into_left_leaf_from_right_neighbor");
        debug_assert!(search_stack.peek_lowest().node_ptr() == parent.node_ptr());

        // this drops the right_leaf
        LeafNodeInner::move_from_right_neighbor_into_left_node(parent, right_leaf, left_leaf);
        // we're done with the left leaf now
        left_leaf.unlock_exclusive();
        if parent.num_keys() < MIN_KEYS_PER_NODE {
            self.coalesce_or_redistribute_internal_node(search_stack);
        } else {
            parent.unlock_exclusive();
        }
    }

    fn coalesce_or_redistribute_internal_node(&self, mut search_stack: SearchDequeue<K, V>) {
        debug_println!("coalesce_or_redistribute_internal_node");
        let underfull_internal = search_stack
            .pop_lowest()
            .assert_exclusive()
            .assert_internal(); // pop -- we're handling this internal node right here

        // if the underfull node is the top_of_tree, we need to adjust the root
        if search_stack.peek_lowest().is_root() {
            self.adjust_top_of_tree(search_stack, underfull_internal.erase_node_type());
            return;
        }
        let parent = search_stack
            .peek_lowest()
            .assert_internal()
            .assert_exclusive();

        let (full_internal, node_position) =
            parent.get_neighboring_internal_node(underfull_internal);

        let full_internal = full_internal.lock_exclusive();
        if full_internal.num_keys() + underfull_internal.num_keys() < MAX_KEYS_PER_NODE {
            match node_position {
                // the common case -- we absorb the underfull node into its full neighbor
                UnderfullNodePosition::Other => {
                    self.coalesce_into_left_internal_from_right_neighbor(
                        full_internal,
                        underfull_internal,
                        parent,
                        search_stack,
                    );
                }
                // the leftmost node -- we absorb the full neighbor into the underfull node
                // so we can keep the operation under the same parent
                UnderfullNodePosition::Leftmost => {
                    self.coalesce_into_left_internal_from_right_neighbor(
                        underfull_internal,
                        full_internal,
                        parent,
                        search_stack,
                    );
                }
            }
        } else {
            search_stack.pop_highest_until(parent).for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
            self.redistribute_into_underfull_internal_from_neighbor(
                underfull_internal,
                full_internal,
                node_position,
                parent,
            );
        }
    }

    fn coalesce_into_left_internal_from_right_neighbor(
        &self,
        left_internal: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        right_internal: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        search_stack: SearchDequeue<K, V>,
    ) {
        debug_println!("coalesce_into_left_internal_from_right_neighbor");
        // this consumes the right_internal
        InternalNodeInner::move_from_right_neighbor_into_left_node(
            parent,
            right_internal,
            left_internal,
        );
        // we're done with the left internal now
        left_internal.unlock_exclusive();
        if parent.num_keys() < MIN_KEYS_PER_NODE {
            self.coalesce_or_redistribute_internal_node(search_stack);
        } else {
            parent.unlock_exclusive();
        }
    }

    fn redistribute_into_underfull_internal_from_neighbor(
        &self,
        underfull_internal: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        full_internal: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        node_position: UnderfullNodePosition,
        parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("redistribute_into_underfull_internal_from_neighbor");
        match node_position {
            UnderfullNodePosition::Other => {
                // this is the common case
                // we have the full left neighbor, and shift a key forwards to the right
                InternalNodeInner::move_last_to_front_of(full_internal, underfull_internal, parent);
            }
            UnderfullNodePosition::Leftmost => {
                // this is the uncommon case
                // we have the full right neighbor, and shift a key back to the left
                InternalNodeInner::move_first_to_end_of(full_internal, underfull_internal, parent);
            }
        }
        parent.unlock_exclusive();
        full_internal.unlock_exclusive();
        underfull_internal.unlock_exclusive();
    }

    fn redistribute_into_underfull_leaf_from_neighbor(
        &self,
        underfull_leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        full_leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        node_position: UnderfullNodePosition,
        parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("redistribute_into_underfull_leaf_from_neighbor");
        match node_position {
            UnderfullNodePosition::Other => {
                // this is the common case
                // we have the full left neighbor, and shift a key to the right
                LeafNodeInner::move_last_to_front_of(full_leaf, underfull_leaf, parent);
            }
            UnderfullNodePosition::Leftmost => {
                // this is the uncommon case
                // we have the full right neighbor, and shift a key to the left
                LeafNodeInner::move_first_to_end_of(full_leaf, underfull_leaf, parent);
            }
        }
        parent.unlock_exclusive();
        full_leaf.unlock_exclusive();
        underfull_leaf.unlock_exclusive();
    }

    fn adjust_top_of_tree(
        &self,
        mut search_stack: SearchDequeue<K, V>,
        top_of_tree: NodeRef<K, V, marker::Exclusive, marker::Unknown>,
    ) {
        debug_println!("adjust_top_of_tree");
        if top_of_tree.is_internal() {
            debug_println!("top_of_tree is an internal node");
            // root is an internal node
            let top_of_tree = top_of_tree.assert_internal();
            // if the (internal node) root has only one child, it's the new root
            if top_of_tree.num_keys() == 0 {
                let mut root = search_stack.pop_lowest().assert_root().assert_exclusive();
                debug_println!("top_of_tree is an internal node with one child");
                let new_top_of_tree = top_of_tree.storage.get_child(0);
                root.top_of_tree = new_top_of_tree;
                // not necessary, but it lets us track the lock count correctly
                top_of_tree.unlock_exclusive();
                unsafe {
                    ptr::drop_in_place(top_of_tree.to_raw_internal_ptr());
                }
                root.unlock_exclusive();
            } else {
                debug_println!(
                    "top_of_tree is an internal node with multiple children -- we're done"
                );
                top_of_tree.unlock_exclusive();
            }
        } else {
            top_of_tree.unlock_exclusive();
        }
    }
    /**
     * Insertion methods:
     * - insert - the top-level insert method
     * - insert_into_leaf_after_splitting - split a leaf node
     * - insert_into_internal_node_after_splitting - split an internal node
     * - insert_into_parent - insert a new node (leaf or internal) into its parent
     * - insert_into_new_root - create a new root
     */

    pub fn insert(&self, key: Box<K>, value: Box<V>) {
        debug_println!("top-level insert {:?}", key);
        // TODO(ak): should we try a few times optimistically?
        let mut optimistic_leaf = match self.get_leaf_exclusively_using_optimistic_search(&key) {
            Ok(leaf) => leaf,
            Err(_) => self.get_leaf_exclusively_using_shared_search(&key),
        };
        if optimistic_leaf.has_capacity_for_modification(ModificationType::Insertion)
            || optimistic_leaf.get(&key).is_some()
        {
            optimistic_leaf.insert(Box::into_raw(key), Box::into_raw(value));
            optimistic_leaf.unlock_exclusive();
            self.len.fetch_add(1, Ordering::Relaxed);
            debug_assert_no_locks_held::<'i'>();
            return;
        }
        optimistic_leaf.unlock_exclusive();

        let mut search_stack =
            self.get_leaf_exclusively_using_exclusive_search(&key, ModificationType::Insertion);
        let mut leaf_node = search_stack.peek_lowest().assert_leaf().assert_exclusive();

        // this should get us to 3 keys in the leaf
        if leaf_node.num_keys() < MAX_KEYS_PER_NODE {
            leaf_node.insert(Box::into_raw(key), Box::into_raw(value));
            search_stack.drain().for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
        } else {
            // if the key already exists, we don't need to split
            // so check for that case and exit early, but only bother checking
            // in the case where we otherwise would split
            // this is necessary for correctness, because split doesn't (and shouldn't)
            // handle the case where the key already exists
            if leaf_node.get(&key).is_some() {
                leaf_node.insert(Box::into_raw(key), Box::into_raw(value));
                search_stack.drain().for_each(|n| {
                    n.assert_exclusive().unlock_exclusive();
                });
                debug_assert_no_locks_held::<'i'>();
                return;
            } else {
                self.insert_into_leaf_after_splitting(
                    search_stack,
                    Box::into_raw(key),
                    Box::into_raw(value),
                );
            }
        }
        self.len.fetch_add(1, Ordering::Relaxed);
        debug_println!("top-level insert done");
        debug_assert_no_locks_held::<'i'>();
    }

    fn insert_into_leaf_after_splitting(
        &self,
        mut search_stack: SearchDequeue<K, V>,
        key_to_insert: *mut K,
        value_to_insert: *mut V,
    ) {
        debug_println!("insert_into_leaf_after_splitting");
        let leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();
        let new_leaf = NodeRef::from_leaf_unlocked(LeafNode::<K, V>::new()).lock_exclusive();

        let mut temp_leaf_vec: SmallVec<LeafTempArray<(*mut K, *mut V)>> = SmallVec::new();

        let mut key_to_insert = Some(key_to_insert);
        let mut value_to_insert = Some(value_to_insert);

        leaf.storage.drain().for_each(|(k, v)| {
            if let Some(key) = key_to_insert {
                if unsafe { &*key < &*k } {
                    temp_leaf_vec.push((
                        key_to_insert.take().unwrap(),
                        value_to_insert.take().unwrap(),
                    ));
                }
            }
            temp_leaf_vec.push((k, v));
        });

        if let Some(_) = &key_to_insert {
            temp_leaf_vec.push((
                key_to_insert.take().unwrap(),
                value_to_insert.take().unwrap(),
            ));
        }

        new_leaf
            .storage
            .extend(temp_leaf_vec.drain(KV_IDX_CENTER + 1..));

        leaf.storage.extend(temp_leaf_vec.drain(..));

        // this clone is necessary because the key is moved into the parent
        let split_key = new_leaf.storage.get_key(0);

        self.insert_into_parent(search_stack, leaf, split_key, new_leaf);
    }

    fn insert_into_parent<N: NodeType>(
        &self,
        mut search_stack: SearchDequeue<K, V>,
        left: NodeRef<K, V, marker::Exclusive, N>,
        split_key: *mut K,
        right: NodeRef<K, V, marker::Exclusive, N>,
    ) {
        debug_println!("insert_into_parent");
        // if the parent is the root, we need to create a top of tree
        if search_stack.peek_lowest().is_root() {
            self.insert_into_new_top_of_tree(
                search_stack.pop_lowest().assert_root().assert_exclusive(),
                left,
                split_key,
                right,
            );
        } else {
            // need to unlock left?
            left.unlock_exclusive();
            let mut parent = search_stack
                .pop_lowest()
                .assert_internal()
                .assert_exclusive();

            if parent.num_keys() < MAX_KEYS_PER_NODE {
                debug_println!(
                    "{:?} inserting split key {:?} for {:?} into existing parent",
                    parent.node_ptr(),
                    unsafe { &*split_key },
                    right.node_ptr()
                );
                parent.insert(split_key, right.node_ptr());
                parent.unlock_exclusive();
                right.unlock_exclusive();
                // we may still have a root / top of tree node on the stack
                // TODO(ak): complicate the logic of has_capacity_for_modification to handle this
                search_stack.drain().for_each(|n| {
                    n.assert_exclusive().unlock_exclusive();
                });
            } else {
                self.insert_into_internal_node_after_splitting(
                    search_stack,
                    parent,
                    split_key,
                    right,
                );
            }
        }
    }

    fn insert_into_internal_node_after_splitting<ChildType: NodeType>(
        &self,
        parent_stack: SearchDequeue<K, V>,
        old_internal_node: NodeRef<K, V, marker::Exclusive, marker::Internal>, // this is the node we're splitting
        split_key: *mut K, // this is the key for the new child
        new_child: NodeRef<K, V, marker::Exclusive, ChildType>, // this is the new child we're inserting
    ) {
        debug_println!("insert_into_internal_node_after_splitting");
        let new_internal_node =
            NodeRef::from_internal_unlocked(InternalNode::<K, V>::new(old_internal_node.height()))
                .lock_exclusive();

        let mut temp_keys_vec: SmallVec<InternalKeyTempArray<*mut K>> = SmallVec::new();
        let mut temp_children_vec: SmallVec<InternalChildTempArray<NodePtr>> = SmallVec::new();

        let new_key_index = old_internal_node
            .storage
            .binary_search_keys(unsafe { &*split_key })
            .unwrap_either();

        let is_last_element = new_key_index == old_internal_node.storage.num_keys();

        for (i, key) in old_internal_node.storage.iter_keys().enumerate() {
            if i == new_key_index {
                temp_keys_vec.push(split_key);
            }
            temp_keys_vec.push(key);
        }

        for (i, child) in old_internal_node.storage.iter_children().enumerate() {
            if i == new_key_index + 1 {
                temp_children_vec.push(new_child.node_ptr());
            }
            temp_children_vec.push(child);
        }

        if is_last_element {
            temp_keys_vec.push(split_key);
            temp_children_vec.push(new_child.node_ptr());
        }

        old_internal_node.storage.truncate();

        // we have 4 keys and 5 children (1, "A", 2, "B", 3, "C", 4, "D", 5)
        // or [1, 2, 3, 4, 5] and ["A", "B", "C", "D"]
        // the keys are the minimum values for each child to their right
        // we need to split that into two nodes, one with (1, "A", 2) and one with (3, "C", 4, "D", 5)
        // or equivalently [1, 2] and [3, 4, 5] and ["A"] and ["C", "D"]
        // and "B" gets hoisted up to the parent (which was the minimum value for 3)
        new_internal_node
            .storage
            .extend_children(temp_children_vec.drain(KV_IDX_CENTER + 1..));
        new_internal_node
            .storage
            .extend_keys(temp_keys_vec.drain(KV_IDX_CENTER + 1..));

        let new_split_key = temp_keys_vec.pop().unwrap();

        old_internal_node
            .storage
            .extend_children(temp_children_vec.drain(..));
        old_internal_node
            .storage
            .extend_keys(temp_keys_vec.drain(..));

        new_child.unlock_exclusive();

        self.insert_into_parent(
            parent_stack,
            old_internal_node,
            new_split_key,
            new_internal_node,
        );
    }

    fn insert_into_new_top_of_tree<N: NodeType>(
        &self,
        mut root: NodeRef<K, V, marker::Exclusive, marker::Root>,
        left: NodeRef<K, V, marker::Exclusive, N>,
        split_key: *mut K,
        right: NodeRef<K, V, marker::Exclusive, N>,
    ) {
        debug_println!("insert_into_new_top_of_tree");
        let new_top_of_tree = NodeRef::from_internal_unlocked(InternalNode::<K, V>::new(
            left.height().one_level_higher(),
        ))
        .lock_exclusive();

        new_top_of_tree.storage.push_extra_child(left.node_ptr());
        new_top_of_tree.storage.push(split_key, right.node_ptr());

        root.top_of_tree = new_top_of_tree.node_ptr();
        root.unlock_exclusive();
        new_top_of_tree.unlock_exclusive();
        left.unlock_exclusive();
        right.unlock_exclusive();
    }

    pub fn print_tree(&self) {
        let root = self.as_node_ptr().lock_shared();
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
        let root = self.as_node_ptr().lock_shared();
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

    use super::*;

    #[test]
    fn test_insert_and_get() {
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

    const INTERESTING_SEEDS: [u64; 1] = [13142251578868436595];

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
                    completed_threads.fetch_add(1, Ordering::Release);
                });
            }

            // Spawn invariant checking thread
            let completed_threads = completed_threads.clone();
            let tree_ref = &tree;
            s.spawn(move || {
                while completed_threads.load(Ordering::Acquire) < num_threads {
                    thread::sleep(Duration::from_secs(1));
                    tree_ref.check_invariants();
                }
            });
        });
    }
}
