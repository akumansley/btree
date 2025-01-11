use crate::{
    array_types::{ChildArray, KeyArray, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE},
    debug_println,
    node::{Height, NodeHeader},
    node_ptr::{
        marker::{self, NodeType},
        DiscriminatedNode, NodePtr,
    },
    tree::{BTreeKey, BTreeValue, ModificationType, UnderfullNodePosition},
};
use smallvec::SmallVec;
use std::{cell::UnsafeCell, ptr};

// this is the shared node data
pub struct InternalNodeInner<K: BTreeKey, V: BTreeValue> {
    pub keys: SmallVec<KeyArray<K>>,
    pub children: SmallVec<ChildArray<NodePtr<K, V, marker::Unlocked, marker::Unknown>>>,
    pub num_keys: usize,
}

#[repr(C)]
pub struct InternalNode<K: BTreeKey, V: BTreeValue> {
    pub header: NodeHeader,
    pub inner: UnsafeCell<InternalNodeInner<K, V>>,
}

impl<K: BTreeKey, V: BTreeValue> InternalNode<K, V> {
    pub fn new(height: Height) -> *mut InternalNode<K, V> {
        Box::into_raw(Box::new(InternalNode {
            header: NodeHeader::new(height),
            inner: UnsafeCell::new(InternalNodeInner {
                keys: SmallVec::new(),
                children: SmallVec::new(),
                num_keys: 0,
            }),
        }))
    }

    /// SAFETY: this is only safe to call when dropping the entire tree
    /// and there are no other live references to any data in the tree -- we don't take locks
    pub unsafe fn drop_node_recursively(&self) {
        let height = self.header.height();
        let child_height = height.one_level_lower();
        unsafe {
            for child in (*self.inner.get()).children.iter() {
                if child_height.is_internal() {
                    (*child.assert_internal().to_mut_internal_ptr()).drop_node_recursively();
                } else {
                    ptr::drop_in_place(child.assert_leaf().to_mut_leaf_ptr());
                }
            }
        }
        unsafe {
            ptr::drop_in_place(self as *const _ as *mut InternalNode<K, V>);
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> InternalNodeInner<K, V> {
    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    pub fn has_capacity_for_modification(&self, modification_type: ModificationType) -> bool {
        debug_println!(
            "has_capacity_for_modification - internal node has {:?}",
            self.num_keys
        );
        match modification_type {
            ModificationType::Insertion => self.num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => self.num_keys > MIN_KEYS_PER_NODE,
            ModificationType::NonModifying => true,
        }
    }

    // internal top of tree nodes are allowed to be as low as 1 key
    pub fn has_capacity_for_modification_as_top_of_tree(
        &self,
        modification_type: ModificationType,
    ) -> bool {
        debug_println!(
            "has_capacity_for_modification_as_top_of_tree - internal node has {:?}",
            self.num_keys
        );
        match modification_type {
            ModificationType::Insertion => self.num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => self.num_keys > 1,
            ModificationType::NonModifying => true,
        }
    }

    pub fn insert(&mut self, key: K, new_child: NodePtr<K, V, marker::Unlocked, marker::Unknown>) {
        debug_assert!(new_child.is_unlocked());
        let insertion_point = self
            .keys
            .iter()
            .position(|k| k > &key)
            .unwrap_or(self.num_keys);

        self.keys.insert(insertion_point, key);
        self.children.insert(insertion_point + 1, new_child);
        self.num_keys += 1;
    }

    pub fn find_child(&self, search_key: &K) -> NodePtr<K, V, marker::Unlocked, marker::Unknown> {
        let index = self
            .keys
            .iter()
            .position(|k| k > &search_key)
            .unwrap_or(self.num_keys); // self.children has one more element than self.keys, so the len is the index
        self.children[index]
    }

    pub fn get_key_for_non_leftmost_child(
        &self,
        child: NodePtr<K, V, marker::Unlocked, marker::Unknown>,
    ) -> K {
        let index = self.children.iter().position(|c| *c == child).unwrap();
        self.keys[index - 1].clone()
    }

    pub fn print_node(&self) {
        println!("InternalNode: {:p}", self);
        println!("+----------------------+");
        println!("| Keys and Children:   |");
        for i in 0..self.num_keys {
            println!("|  - NodePtr: {:?}     |", self.children[i]);
            println!("|  - Key: {:?}         |", self.keys[i]);
        }
        // Print the last child
        if self.num_keys < self.children.len() {
            println!("|  - NodePtr: {:?}     |", self.children[self.num_keys]);
        }
        println!("+----------------------+");
        println!("| Num Keys: {}           |", self.num_keys);
        println!("+----------------------+");
        for child in self.children.iter() {
            match child.force() {
                DiscriminatedNode::Internal(internal_child) => {
                    let locked_child = internal_child.lock_shared();
                    locked_child.print_node();
                    locked_child.unlock_shared();
                }
                DiscriminatedNode::Leaf(leaf_child) => {
                    let locked_child = leaf_child.lock_shared();
                    locked_child.print_node();
                    locked_child.unlock_shared();
                }
                _ => panic!("expected untagged internal or leaf node"),
            }
        }
    }

    fn get_neighbor(
        &self,
        child: NodePtr<K, V, marker::Unlocked, marker::Unknown>,
    ) -> (
        NodePtr<K, V, marker::Unlocked, marker::Unknown>,
        UnderfullNodePosition,
    ) {
        if child == self.children[0] {
            return (self.children[1], UnderfullNodePosition::Leftmost);
        }
        for i in 1..self.num_keys + 1 {
            if self.children[i] == child {
                return (self.children[i - 1], UnderfullNodePosition::Other);
            }
        }
        panic!("expected to find child in internal node");
    }

    pub(crate) fn get_neighboring_internal_node(
        &self,
        child: NodePtr<K, V, marker::Unlocked, marker::Internal>,
    ) -> (
        NodePtr<K, V, marker::Unlocked, marker::Internal>,
        UnderfullNodePosition,
    ) {
        debug_assert!(child.is_internal());
        let (neighbor, direction) = self.get_neighbor(child.erase_node_type());
        debug_assert!(neighbor.is_internal());
        (neighbor.assert_internal(), direction)
    }

    pub(crate) fn get_neighbor_of_underfull_leaf(
        &self,
        child: NodePtr<K, V, marker::Unlocked, marker::Leaf>,
    ) -> (
        NodePtr<K, V, marker::Unlocked, marker::Leaf>,
        UnderfullNodePosition,
    ) {
        debug_assert!(child.is_leaf());
        let (neighbor, direction) = self.get_neighbor(child.erase_node_type());
        debug_assert!(neighbor.is_leaf());
        (neighbor.assert_leaf(), direction)
    }

    pub(crate) fn remove(&mut self, child: NodePtr<K, V, marker::Unlocked, marker::Unknown>) {
        let index = self.children.iter().position(|c| *c == child).unwrap();
        self.children.remove(index);

        // we don't remove the leftmost element
        debug_assert!(index > 0);

        // we remove the split key for the removed node
        // one index to the left of the removed child
        // (1 "A" 2 "B" 3 "C" 4) -> remove 2 "A" -> (1 "B" 3 "C" 4)
        // or equivalently, [1, 2, 3, 4], ["A", "B", "C"] -> remove 2 -> [1, 3, 4], ["B", "C"]
        self.keys.remove(index - 1);
        self.num_keys -= 1;
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut from: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut to: NodePtr<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!(
            "moving from {:?} {:?} into {:?} {:?}",
            from.keys,
            from.children,
            to.keys,
            to.children,
        );
        let split_key = parent.get_key_for_non_leftmost_child(from.to_unlocked().erase_node_type());
        to.keys.push(split_key);
        to.keys.extend(from.keys.drain(..));
        debug_println!("moved keys {:?}", to.keys);

        for child in from.children.drain(..) {
            debug_println!("moving child {:?}", child);
            to.children.push(child);
        }
        to.num_keys = to.keys.len();
        parent.remove(from.to_unlocked().erase_node_type());
        // this is not necessary, but it lets is track the lock count correctly
        from.unlock_exclusive();
        unsafe {
            ptr::drop_in_place(from.to_mut_internal_ptr());
        }
    }

    pub fn move_last_to_front_of(
        mut left: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut right: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_last_to_front_of");
        let last_key = left.keys.pop().unwrap(); // if these don't exist, we've got bigger problems
        let last_child = left.children.pop().unwrap();

        // last key wants to become the _parent_ split key
        let old_split_key =
            parent.update_split_key(right.to_unlocked().erase_node_type(), last_key);

        right.keys.insert(0, old_split_key); // and the node's old parent split key is now its first key
        right.children.insert(0, last_child);
        left.num_keys -= 1;
        right.num_keys += 1;
    }

    // we'll only call this for children other than the leftmost
    pub(crate) fn update_split_key<N: NodeType>(
        &mut self,
        node: NodePtr<K, V, marker::Unlocked, N>,
        mut new_split_key: K,
    ) -> K {
        debug_assert!(node.is_unlocked());
        debug_println!(
            "InternalNode update_split_key of {:?} to {:?}",
            node,
            new_split_key
        );
        let stored_node = node.to_stored();
        let index = self
            .children
            .iter()
            .position(|c| *c == stored_node)
            .unwrap();
        debug_assert!(index > 0);
        std::mem::swap(&mut self.keys[index - 1], &mut new_split_key);
        new_split_key
    }

    pub fn move_first_to_end_of(
        mut right: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut left: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_first_to_end_of");
        let first_key = right.keys.remove(0);
        let first_child = right.children.remove(0);
        // Update the split key in the parent for self
        let old_split_key =
            parent.update_split_key(right.to_unlocked().erase_node_type(), first_key);

        left.keys.push(old_split_key);
        left.children.push(first_child);
        right.num_keys -= 1;
        left.num_keys += 1;
    }

    pub fn check_invariants(&self) {
        assert_eq!(
            self.num_keys,
            self.keys.len(),
            "num_keys does not match the actual number of keys"
        );
        assert_eq!(
            self.num_keys + 1,
            self.children.len(),
            "Number of keys and children are inconsistent"
        );

        // the top of tree is allowed to have as few as 1 key
        assert!(self.num_keys > 0);
        assert!(self.num_keys <= MAX_KEYS_PER_NODE);

        // Check invariants for each child
        for child in &self.children {
            match child.force() {
                DiscriminatedNode::Internal(internal_child) => {
                    let locked_child = internal_child.lock_shared();
                    locked_child.check_invariants();
                    locked_child.unlock_shared();
                }
                DiscriminatedNode::Leaf(leaf_child) => {
                    let locked_child = leaf_child.lock_shared();
                    locked_child.check_invariants();
                    locked_child.unlock_shared();
                }
                DiscriminatedNode::Root(_) => {
                    panic!("expected internal or leaf node");
                }
            }
        }
    }
}
