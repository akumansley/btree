use std::fmt::{Debug, Display};

use crate::array_types::{
    ChildTempArray, KeyTempArray, ValueTempArray, KV_IDX_CENTER, MAX_KEYS_PER_NODE,
    MIN_KEYS_PER_NODE,
};
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::pointer_types::{NodePtr, NodeRef};
use crate::search_dequeue::SearchDequeue;
use smallvec::SmallVec;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UnderfullNodePosition {
    Leftmost,
    Other,
}

/// B+Tree
/// Todo
/// - replace parent pointers with a search stack
/// - bulk loading
/// - scans
/// - concurrency
pub struct BTree<K: PartialOrd + Clone + Debug + Display, V: Debug + Display> {
    height: usize,
    len: usize,
    root: NodePtr<K, V>,
}

impl<K: PartialOrd + Clone + Debug + Display, V: Debug + Display> Drop for BTree<K, V> {
    fn drop(&mut self) {
        let mut node_ref = NodeRef::new(self.root, self.height);
        node_ref.drop_node();
    }
}

impl<K: PartialOrd + Clone + Debug + Display, V: Debug + Display> BTree<K, V> {
    pub fn new() -> Self {
        let root = LeafNode::new();
        BTree {
            len: 0,
            root: NodePtr::from_leaf(root),
            height: 0,
        }
    }

    fn find_leaf(&self, search_key: &K) -> SearchDequeue<K, V> {
        let mut search_stack: SearchDequeue<K, V> = SearchDequeue::new(self.height);
        search_stack.push_node_on_bottom(self.root);
        let mut node_ref = search_stack.peek_lowest();

        while node_ref.is_internal() {
            unsafe {
                let current = node_ref.as_internal_node();
                let found_child = (*current).find_child(search_key);
                search_stack.push_node_on_bottom(found_child);
                node_ref = search_stack.peek_lowest();
            }
        }
        let leaf_node = search_stack.must_get_leaf();

        println!("find_leaf {:?} found {:?}", search_key, leaf_node);
        search_stack
    }

    pub fn get(&self, search_key: &K) -> Option<&V> {
        let search_stack = self.find_leaf(search_key);
        let leaf_node = search_stack.must_get_leaf();
        unsafe { (*leaf_node).get(search_key) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /**
     * Removal methods:
     * - remove - the top-level remove method
     * - coalesce_or_redistribute_leaf_node - called when a leaf node has too few keys
     * - coalesce_or_redistribute_internal_node - called when an internal node has too few keys
     */

    pub fn remove(&mut self, key: &K) {
        println!("top-level remove {}", key);
        let search_stack = self.find_leaf(key);
        let leaf_node = search_stack.must_get_leaf();
        unsafe {
            (*leaf_node).remove(key);
            if (*leaf_node).num_keys() < MIN_KEYS_PER_NODE {
                self.coalesce_or_redistribute_leaf_node(search_stack);
            }
        }
        println!("top-level remove done -- {}", key);
    }

    fn coalesce_or_redistribute_leaf_node(&mut self, mut search_stack: SearchDequeue<K, V>) {
        println!("coalesce_or_redistribute_leaf_node");
        // if the leaf is the root, we don't need to do anything -- we let it get under-full
        if self.height == 0 {
            return;
        }

        unsafe {
            let underfull_leaf = search_stack.pop_lowest().as_leaf_node(); // pop -- we're done with this leaf after this
            let parent = search_stack.peek_lowest().as_internal_node();

            // a neighbor is the node to the left, except in the case of the leftmost child,
            // where it's one to the right
            let (full_leaf_node, node_position) =
                (*parent).get_neighbor_of_underfull_leaf(underfull_leaf);
            if (*underfull_leaf).num_keys() + (*full_leaf_node).num_keys() < MAX_KEYS_PER_NODE {
                match node_position {
                    UnderfullNodePosition::Other => {
                        // if the neighbor is to the left (the common case),
                        // we absorb the underfull leaf into its neighbor
                        self.coalesce_into_left_leaf_from_right_neighbor(
                            full_leaf_node, // the left-hand leaf
                            underfull_leaf, // its right neighbor, to be absorbed
                            search_stack,
                        );
                    }
                    UnderfullNodePosition::Leftmost => {
                        // in the case of the leftmost child, we don't want to hop to a different parent
                        // so we absorb the full leaf into the underfull leaf
                        // in both calls, the leftmost leaf is the first argument
                        self.coalesce_into_left_leaf_from_right_neighbor(
                            underfull_leaf, // the left-hand leaf
                            full_leaf_node, // it's right-hand neighbor, to be absorbed
                            search_stack,
                        );
                    }
                }
            } else {
                // no need for the full stack here -- if we're redistributing, the changes are local to the two leaves
                // TODO(ak): assert that we've unlocked the parents by now
                self.redistribute_into_underfull_leaf_from_neighbor(
                    underfull_leaf,
                    full_leaf_node,
                    node_position,
                    parent,
                );
            }
        }
    }

    fn coalesce_into_left_leaf_from_right_neighbor(
        &mut self,
        left_leaf: *mut LeafNode<K, V>,
        right_leaf: *mut LeafNode<K, V>,
        search_stack: SearchDequeue<K, V>, // TODO(ak): this should be a reference, right?
    ) {
        println!("coalesce_into_left_leaf_from_right_neighbor");
        unsafe {
            let parent = search_stack.peek_lowest().as_internal_node();
            let right_leaf_box = Box::from_raw(right_leaf);
            LeafNode::move_from_right_neighbor_into_left_node(parent, right_leaf_box, left_leaf);
            if (*parent).num_keys() < MIN_KEYS_PER_NODE {
                self.coalesce_or_redistribute_internal_node(search_stack);
            }
        }
    }

    fn coalesce_or_redistribute_internal_node(&mut self, mut search_stack: SearchDequeue<K, V>) {
        println!("coalesce_or_redistribute_internal_node");
        let underfull_internal = search_stack.pop_lowest().as_internal_node(); // pop -- we're handling this internal node right here
        if NodePtr::from_internal(underfull_internal) == self.root {
            self.adjust_root();
            return;
        }
        unsafe {
            let parent = search_stack.peek_lowest().as_internal_node();
            let (full_internal, node_position) =
                (*parent).get_neighboring_internal_node(underfull_internal);
            if (*full_internal).num_keys() + (*underfull_internal).num_keys() < MAX_KEYS_PER_NODE {
                match node_position {
                    // the common case -- we absorb the underfull node into its full neighbor
                    UnderfullNodePosition::Other => {
                        self.coalesce_into_left_internal_from_right_neighbor(
                            full_internal,
                            underfull_internal,
                            search_stack,
                        );
                    }
                    // the leftmost node -- we absorb the full neighbor into the underfull node
                    // so we can keep the operation under the same parent
                    UnderfullNodePosition::Leftmost => {
                        self.coalesce_into_left_internal_from_right_neighbor(
                            underfull_internal,
                            full_internal,
                            search_stack,
                        );
                    }
                }
            } else {
                self.redistribute_into_underfull_internal_from_neighbor(
                    underfull_internal,
                    full_internal,
                    node_position,
                    parent,
                );
            }
        }
    }

    fn coalesce_into_left_internal_from_right_neighbor(
        &mut self,
        left_internal: *mut InternalNode<K, V>,
        right_internal: *mut InternalNode<K, V>,
        search_stack: SearchDequeue<K, V>,
    ) {
        println!("coalesce_into_left_internal_from_right_neighbor");
        unsafe {
            let parent = search_stack.peek_lowest().as_internal_node();
            let right_internal_box = Box::from_raw(right_internal);
            InternalNode::move_from_right_neighbor_into_left_node(
                parent,
                right_internal_box,
                left_internal,
            );
            if (*parent).num_keys() < MIN_KEYS_PER_NODE {
                self.coalesce_or_redistribute_internal_node(search_stack);
            }
        }
    }

    fn redistribute_into_underfull_internal_from_neighbor(
        &mut self,
        underfull_internal: *mut InternalNode<K, V>,
        full_internal: *mut InternalNode<K, V>,
        node_position: UnderfullNodePosition,
        parent: *mut InternalNode<K, V>,
    ) {
        println!("redistribute_into_underfull_internal_from_neighbor");
        match node_position {
            UnderfullNodePosition::Other => {
                // this is the common case
                // we have the full left neighbor, and shift a key to the right
                unsafe {
                    (*full_internal).move_last_to_front_of(underfull_internal, parent);
                }
            }
            UnderfullNodePosition::Leftmost => {
                // this is the uncommon case
                // we have the full right neighbor, and shift a key to the left
                unsafe {
                    (*full_internal).move_first_to_end_of(underfull_internal, parent);
                }
            }
        }
    }

    fn redistribute_into_underfull_leaf_from_neighbor(
        &mut self,
        underfull_leaf: *mut LeafNode<K, V>,
        full_leaf: *mut LeafNode<K, V>,
        node_position: UnderfullNodePosition,
        parent: *mut InternalNode<K, V>,
    ) {
        println!("redistribute_into_underfull_leaf_from_neighbor");
        match node_position {
            UnderfullNodePosition::Other => {
                // this is the common case
                // we have the full left neighbor, and shift a key to the right
                unsafe {
                    (*full_leaf).move_last_to_front_of(underfull_leaf, parent);
                }
            }
            UnderfullNodePosition::Leftmost => {
                // this is the uncommon case
                // we have the full right neighbor, and shift a key to the left
                unsafe {
                    (*full_leaf).move_first_to_end_of(underfull_leaf, parent);
                }
            }
        }
    }

    fn adjust_root(&mut self) {
        println!("adjust_root");
        let root_node_ref = NodeRef::new(self.root, self.height);
        if root_node_ref.is_internal() {
            println!("root is an internal node");
            // root is an internal node
            let root_internal_node = root_node_ref.as_internal_node();
            unsafe {
                // if the (internal node) root has only one child, it's the new root
                if (*root_internal_node).num_keys == 0 {
                    println!("root is an internal node with one child");
                    let new_root = (*root_internal_node).children[0];
                    self.height -= 1;
                    self.root = new_root;
                    // TODO: make sure we've locked before doing this
                    drop(Box::from_raw(root_internal_node));
                }
            }
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

    pub fn insert(&mut self, key: K, value: V) {
        println!("top-level insert {}", key);
        let search_stack = self.find_leaf(&key);
        let leaf_node = search_stack.must_get_leaf();

        unsafe {
            // this should get us to 3 keys in the leaf
            if (*leaf_node).num_keys() < MAX_KEYS_PER_NODE {
                (*leaf_node).insert(key, value);
            } else {
                // if the key already exists, we don't need to split
                // so check for that case and exit early, but only bother checking
                // in the case where we otherwise would split
                // this is necessary for correctness, because split doesn't (and shouldn't)
                // handle the case where the key already exists
                if (*leaf_node).get(&key).is_some() {
                    (*leaf_node).insert(key, value);
                    return;
                }
                self.insert_into_leaf_after_splitting(search_stack, key, value);
            }
        }
        self.len += 1;
        println!("top-level insert done");
    }

    fn insert_into_leaf_after_splitting(
        &mut self,
        mut search_stack: SearchDequeue<K, V>,
        key_to_insert: K,
        value: V,
    ) {
        let leaf = search_stack.pop_lowest().as_leaf_node();
        let new_leaf = LeafNode::<K, V>::new();
        let mut temp_key_vec: SmallVec<KeyTempArray<K>> = SmallVec::new();
        let mut temp_value_vec: SmallVec<ValueTempArray<V>> = SmallVec::new();

        let mut key_to_insert = Some(key_to_insert);
        let mut value = Some(value);

        unsafe {
            (*leaf)
                .keys
                .drain(..)
                .zip((*leaf).values.drain(..))
                .for_each(|(k, v)| {
                    if let Some(key) = &key_to_insert {
                        if *key < k {
                            temp_key_vec.push(key_to_insert.take().unwrap());
                            temp_value_vec.push(value.take().unwrap());
                        }
                    }
                    temp_key_vec.push(k);
                    temp_value_vec.push(v);
                });

            if let Some(_) = &key_to_insert {
                temp_key_vec.push(key_to_insert.take().unwrap());
                temp_value_vec.push(value.take().unwrap());
            }

            (*new_leaf)
                .keys
                .extend(temp_key_vec.drain(KV_IDX_CENTER + 1..));
            (*new_leaf)
                .values
                .extend(temp_value_vec.drain(KV_IDX_CENTER + 1..));
            (*new_leaf).num_keys = (MAX_KEYS_PER_NODE + 1) / 2;

            (*leaf).keys.extend(temp_key_vec.drain(..));
            (*leaf).values.extend(temp_value_vec.drain(..));
            (*leaf).num_keys = (MAX_KEYS_PER_NODE + 1) / 2;

            // this clone is necessary because the key is moved into the parent
            let split_key = (*new_leaf).keys[0].clone();

            self.insert_into_parent(
                search_stack,
                NodePtr::from_leaf(leaf),
                split_key,
                NodePtr::from_leaf(new_leaf),
            );
        }
    }

    fn insert_into_parent(
        &mut self,
        mut search_stack: SearchDequeue<K, V>,
        left: NodePtr<K, V>,
        split_key: K,
        right: NodePtr<K, V>,
    ) {
        // TODO(ak): is this right?
        if search_stack.is_empty() {
            self.insert_into_new_root(left, split_key, right);
        } else {
            let parent = search_stack.pop_lowest().as_internal_node();
            unsafe {
                if (*parent).num_keys() < MAX_KEYS_PER_NODE {
                    (*parent).insert(split_key, right);
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
    }

    fn insert_into_internal_node_after_splitting(
        &mut self,
        parent_stack: SearchDequeue<K, V>,
        old_internal_node: *mut InternalNode<K, V>, // this is the node we're splitting
        split_key: K,                               // this is the key for the new child
        new_child: NodePtr<K, V>,                   // this is the new child we're inserting
    ) {
        let new_internal_node = InternalNode::<K, V>::new();

        let mut temp_keys_vec: SmallVec<KeyTempArray<K>> = SmallVec::new();
        let mut temp_children_vec: SmallVec<ChildTempArray<NodePtr<K, V>>> = SmallVec::new();

        unsafe {
            let new_key_index = (*old_internal_node)
                .keys
                .iter()
                .position(|k| k > &split_key)
                .unwrap_or(MAX_KEYS_PER_NODE);

            // drain both vectors, inserting the new child and split key in the right place
            temp_keys_vec.extend((*old_internal_node).keys.drain(..new_key_index));
            // you always split to the right, so the leftmost child is always included
            temp_children_vec.extend((*old_internal_node).children.drain(..new_key_index + 1));
            temp_keys_vec.push(split_key);
            temp_children_vec.push(new_child);

            // now take the rest
            temp_keys_vec.extend((*old_internal_node).keys.drain(..));
            temp_children_vec.extend((*old_internal_node).children.drain(..));

            // we have 4 keys and 5 children (1, "A", 2, "B", 3, "C", 4, "D", 5)
            // or [1, 2, 3, 4, 5] and ["A", "B", "C", "D"]
            // the keys are the minimum values for each child to their right
            // we need to split that into two nodes, one with (1, "A", 2) and one with (3, "C", 4, "D", 5)
            // or equivalently [1, 2] and [3, 4, 5] and ["A"] and ["C", "D"]
            // and "B" gets hoisted up to the parent (which was the minimum value for 3)

            (*new_internal_node)
                .keys
                .extend(temp_keys_vec.drain(KV_IDX_CENTER + 1..));
            (*new_internal_node)
                .children
                .extend(temp_children_vec.drain(KV_IDX_CENTER + 1..));
            (*new_internal_node).num_keys = KV_IDX_CENTER + 1;

            let new_split_key = temp_keys_vec.pop().unwrap();

            (*old_internal_node).keys.extend(temp_keys_vec.drain(..));
            (*old_internal_node)
                .children
                .extend(temp_children_vec.drain(..));
            (*old_internal_node).num_keys = KV_IDX_CENTER;

            self.insert_into_parent(
                parent_stack,
                NodePtr::from_internal(old_internal_node),
                new_split_key,
                NodePtr::from_internal(new_internal_node),
            );
        }
    }

    fn insert_into_new_root(&mut self, left: NodePtr<K, V>, split_key: K, right: NodePtr<K, V>) {
        let new_root = InternalNode::new();
        unsafe {
            (*new_root).keys.push(split_key);
            (*new_root).children.push(left);
            (*new_root).children.push(right);
            (*new_root).num_keys = 1;
        }
        self.root = NodePtr::from_internal(new_root);
        self.height += 1;
    }

    pub fn print_tree(&self) {
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree height: {}      |", self.height);
        println!("| Tree length: {}      |", self.len);
        println!("+----------------------+");
        let node_ref = NodeRef::new(self.root, self.height);
        node_ref.print_node();
    }

    pub fn check_invariants(&self) {
        // Check that the root is not null
        assert!(!self.root.is_null(), "Root should not be null");
        let root_node_ref = NodeRef::new(self.root, self.height);
        root_node_ref.check_invariants(self.height);
    }
}

#[cfg(test)]
mod tests {
    use crate::array_types::ORDER;

    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut tree = BTree::new();
        let n = ORDER.pow(2);
        for i in 1..=n {
            let value = format!("value{}", i);
            tree.insert(i, value.clone());
            tree.check_invariants();
            assert_eq!(tree.get(&i), Some(&value));
        }

        println!("tree should be full:");
        tree.print_tree();

        assert_eq!(tree.get(&1), Some(&"value1".to_string()));
        assert_eq!(tree.get(&2), Some(&"value2".to_string()));
        assert_eq!(tree.get(&3), Some(&"value3".to_string()));

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

    #[test]
    fn test_random_inserts_gets_and_removes_with_seed() {
        // Test with predefined interesting seeds
        for &seed in &INTERESTING_SEEDS {
            run_random_operations_with_seed(seed);
        }

        // Test with a random seed
        let random_seed: u64 = rand::thread_rng().gen();
        println!("Using random seed: {}", random_seed);
        run_random_operations_with_seed(random_seed);
    }

    fn run_random_operations_with_seed(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut tree = BTree::new();
        let mut reference_map = HashMap::new();
        println!("Using seed: {}", seed);

        // Perform random operations for a while
        for _ in 0..1000 {
            let operation = rng.gen_range(0..3);
            match operation {
                0 => {
                    // Random insert
                    let key = rng.gen_range(0..1000);
                    let value = format!("value{}", key);
                    tree.insert(key, value.clone());
                    reference_map.insert(key, value);
                }
                1 => {
                    // Random get
                    let key = rng.gen_range(0..1000);
                    let btree_result = tree.get(&key);
                    let hashmap_result = reference_map.get(&key);
                    if btree_result != hashmap_result {
                        println!("Mismatch for key {}", key);
                        println!("btree_result: {:?}", btree_result);
                        println!("hashmap_result: {:?}", hashmap_result);
                        tree.print_tree();
                        tree.check_invariants();
                    }
                    assert_eq!(btree_result, hashmap_result, "Mismatch for key {}", key);
                }
                2 => {
                    // Random remove
                    if !reference_map.is_empty() {
                        let key = *reference_map.keys().choose(&mut rng).unwrap();
                        tree.remove(&key);
                        reference_map.remove(&key);
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
                tree.get(key),
                reference_map.get(key),
                "Final verification failed for key {}",
                key
            );
        }
    }

    const INTERESTING_SEEDS: [u64; 1] = [13142251578868436595];
}
