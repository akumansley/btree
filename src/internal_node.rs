use crate::{
    array_types::{
        InternalNodeStorage, MAX_CHILDREN_PER_NODE, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE,
    },
    debug_println,
    node::{Height, NodeHeader},
    node_ptr::{
        marker::{self, LockState},
        DiscriminatedNode, NodePtr, NodeRef,
    },
    tree::{BTreeKey, BTreeValue, ModificationType, UnderfullNodePosition},
    util::UnwrapEither,
};
use std::{cell::UnsafeCell, marker::PhantomData, ptr};

// this is the shared node data
pub struct InternalNodeInner<K: BTreeKey, V: BTreeValue> {
    pub storage: InternalNodeStorage<MAX_KEYS_PER_NODE, MAX_CHILDREN_PER_NODE, K, V>,
    pub phantom: PhantomData<V>,
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
                storage: InternalNodeStorage::<MAX_KEYS_PER_NODE, MAX_CHILDREN_PER_NODE, K, V>::new(
                ),
                phantom: PhantomData,
            }),
        }))
    }

    /// SAFETY: this is only safe to call when dropping the entire tree
    /// and there are no other live references to any data in the tree -- we don't take locks
    pub unsafe fn drop_node_recursively(&self) {
        let height = self.header.height();
        let child_height = height.one_level_lower();
        unsafe {
            for child in (*self.inner.get()).storage.iter_children() {
                let child_ref =
                    NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
                        child,
                    );
                if child_height.is_internal() {
                    let child_ref = child_ref.assert_internal();
                    (*child_ref.to_raw_internal_ptr()).drop_node_recursively();
                } else {
                    let child_ref = child_ref.assert_leaf();
                    ptr::drop_in_place(child_ref.to_raw_leaf_ptr());
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
        self.storage.num_keys()
    }

    pub fn has_capacity_for_modification(&self, modification_type: ModificationType) -> bool {
        let num_keys = self.num_keys();
        debug_println!(
            "has_capacity_for_modification - internal node has {:?}",
            num_keys
        );
        match modification_type {
            ModificationType::Insertion => num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => num_keys > MIN_KEYS_PER_NODE,
            ModificationType::NonModifying => true,
        }
    }

    // internal top of tree nodes are allowed to be as low as 1 key
    pub fn has_capacity_for_modification_as_top_of_tree(
        &self,
        modification_type: ModificationType,
    ) -> bool {
        let num_keys = self.num_keys();
        debug_println!(
            "has_capacity_for_modification_as_top_of_tree - internal node has {:?}",
            num_keys
        );
        match modification_type {
            ModificationType::Insertion => num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => num_keys > 1,
            ModificationType::NonModifying => true,
        }
    }

    pub fn insert(&mut self, key: *mut K, new_child: NodePtr) {
        let insertion_point = self
            .storage
            .binary_search_keys(unsafe { &*key })
            .unwrap_either();

        self.storage
            .insert_child_with_split_key(key, new_child, insertion_point);
    }

    pub fn find_child(&self, search_key: &K) -> NodePtr {
        let index = match self.storage.binary_search_keys(search_key) {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        self.storage.get_child(index)
    }

    pub fn get_key_for_non_leftmost_child(&self, child: NodePtr) -> *mut K {
        let index = self
            .storage
            .iter_children()
            .position(|c| c == child)
            .unwrap();

        self.storage.get_key(index - 1)
    }
    pub fn print_node_without_children(&self) {
        println!("InternalNode: {:p}", self);
        println!("+----------------------+");
        println!("| Keys and Children:   |");
        for i in 0..self.num_keys() {
            println!("|  - NodePtr: {:?}     |", self.storage.get_child(i));
            println!("|  - Key: {:?}         |", unsafe {
                &*self.storage.get_key(i)
            });
        }
        // Print the last child
        if self.num_keys() < self.storage.num_children() {
            println!(
                "|  - NodePtr: {:?}     |",
                self.storage.get_child(self.num_keys())
            );
        }
        println!("+----------------------+");
        println!("| Num Keys: {}           |", self.num_keys());
        println!("+----------------------+");
    }

    pub fn print_node(&self) {
        for child in self.storage.iter_children() {
            let child_ref =
                NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(child);
            match child_ref.force() {
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

    fn get_neighbor(&self, child: NodePtr) -> (NodePtr, UnderfullNodePosition) {
        if child == self.storage.get_child(0) {
            return (self.storage.get_child(1), UnderfullNodePosition::Leftmost);
        }
        for i in 1..self.num_keys() + 1 {
            if self.storage.get_child(i) == child {
                return (self.storage.get_child(i - 1), UnderfullNodePosition::Other);
            }
        }
        panic!("expected to find child in internal node");
    }

    pub(crate) fn get_neighboring_internal_node<L: LockState>(
        &self,
        child: NodeRef<K, V, L, marker::Internal>,
    ) -> (
        NodeRef<K, V, marker::Unlocked, marker::Internal>,
        UnderfullNodePosition,
    ) {
        debug_assert!(child.is_internal());
        let (neighbor, direction) = self.get_neighbor(child.node_ptr());
        let neighbor_ref =
            NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(neighbor);
        debug_println!(
            "InternalNode get_neighboring_internal_node on parent node {:?} for child {:?} found neighbor {:?}",
            self as *const _,
            child.node_ptr(),
            neighbor_ref.node_ptr()
        );
        (neighbor_ref.assert_internal(), direction)
    }

    pub(crate) fn get_neighbor_of_underfull_leaf<L: LockState>(
        &self,
        child: NodeRef<K, V, L, marker::Leaf>,
    ) -> (
        NodeRef<K, V, marker::Unlocked, marker::Leaf>,
        UnderfullNodePosition,
    ) {
        let (neighbor, direction) = self.get_neighbor(child.node_ptr());
        let neighbor_ref =
            NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(neighbor);
        (neighbor_ref.assert_leaf(), direction)
    }

    pub(crate) fn remove(&mut self, child: NodePtr) {
        debug_println!(
            "removing child {:?} from internal node {:?}",
            child,
            self as *const _
        );
        let index = self
            .storage
            .iter_children()
            .position(|c| c == child)
            .unwrap();

        self.storage.remove_child_at_index(index);

        // we don't remove the leftmost element
        debug_assert!(index > 0);
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        from: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        to: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_from_right_neighbor_into_left_node");

        let split_key = parent.get_key_for_non_leftmost_child(from.node_ptr());
        to.storage.extend(from.storage.drain(split_key));

        parent.remove(from.node_ptr());
        // this is not necessary, but it lets us track the lock count correctly
        from.unlock_exclusive();
        unsafe {
            ptr::drop_in_place(from.to_raw_internal_ptr());
        }
    }

    pub fn move_last_to_front_of(
        left: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        right: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_last_to_front_of");
        let (last_key, last_child) = left.storage.pop();

        // last key wants to become the _parent_ split key
        let old_split_key = parent.update_split_key(right.node_ptr(), last_key);

        right.storage.insert(old_split_key, last_child, 0);
    }

    // we'll only call this for children other than the leftmost
    pub(crate) fn update_split_key(&mut self, node: NodePtr, new_split_key: *mut K) -> *mut K {
        debug_println!(
            "InternalNode update_split_key on parent node {:?} for child {:?} num_keys: {:?}",
            self as *const _,
            node,
            self.num_keys()
        );

        let child_index = self
            .storage
            .iter_children()
            .position(|c| c == node)
            .unwrap();

        debug_println!(
            "InternalNode {:?} update_split_key of {:?} to {:?} at key index {:?}",
            self as *const _,
            node,
            unsafe { &*new_split_key },
            child_index - 1
        );

        debug_assert!(child_index > 0);

        let old_split_key = self.storage.get_key(child_index - 1);
        self.storage.set_key(child_index - 1, new_split_key);
        old_split_key
    }

    pub fn move_first_to_end_of(
        right: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        left: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!(
            "InternalNode move_first_to_end_of from first of right: {:?} to left:{:?} to parent:{:?}",
            right.node_ptr(),
            left.node_ptr(),
            parent.node_ptr()
        );
        let (first_key, first_child) = right.storage.remove(0);
        // Update the split key in the parent for the right child

        let old_split_key = parent.update_split_key(right.node_ptr(), first_key);

        left.storage.push(old_split_key, first_child);
    }

    pub fn check_invariants(&self) {
        debug_println!("InternalNode check_invariants {:?}", self as *const _);
        self.storage.check_invariants();

        // Check invariants for each child
        for (child_index, child) in self.storage.iter_children().enumerate() {
            let child_ref =
                NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(child);
            match child_ref.force() {
                DiscriminatedNode::Internal(internal_child) => {
                    let locked_child = internal_child.lock_shared();

                    if child_index > 0 {
                        assert!(
                            unsafe {
                                &*locked_child.storage.get_key(0)
                                    >= &*self.storage.get_key(child_index - 1)
                            },
                            "internal child {:?} first key {:?} is greater or equal to the parent's split key {:?} for that child",
                            locked_child.node_ptr(),
                            unsafe { &*locked_child.storage.get_key(0) },
                            unsafe { &*self.storage.get_key(child_index - 1) }
                        );
                    }
                    if child_index < self.num_keys() - 1 {
                        assert!(
                            unsafe {
                                &*locked_child.storage.get_key(locked_child.num_keys() - 1)
                                    <= &*self.storage.get_key(child_index)
                            },
                            "internal child {:?} last key {:?} is greater than the parent's split key {:?} for the next child",
                            locked_child.node_ptr(),
                            unsafe { &*locked_child.storage.get_key(locked_child.num_keys() - 1) },
                            unsafe { &*self.storage.get_key(child_index) }
                        );
                    }

                    locked_child.check_invariants();
                    locked_child.unlock_shared();
                }
                DiscriminatedNode::Leaf(leaf_child) => {
                    let locked_child = leaf_child.lock_shared();
                    // assert that the child's first key is after the child's split key
                    if child_index > 0 {
                        assert!(
                            unsafe { &*locked_child.storage.get_key(0) }
                                >= unsafe { &*self.storage.get_key(child_index - 1) },
                            "leaf child {:?} 's first key {:?} is before the child's split key {:?}",
                            leaf_child.node_ptr(),
                            unsafe { &*locked_child.storage.get_key(0) },
                            unsafe { &*self.storage.get_key(child_index - 1) }
                        );
                    }
                    // assert that the child's last key is before the next child's split key
                    if child_index < self.num_keys() - 1 {
                        assert!(
                            unsafe { &*locked_child.storage.get_key(locked_child.num_keys() - 1) }
                                <= unsafe { &*self.storage.get_key(child_index) },
                            "leaf child {:?} 's last key {:?} is after the next child's split key {:?}",
                            leaf_child.node_ptr(),
                            unsafe { &*locked_child.storage.get_key(locked_child.num_keys() - 1) },
                            unsafe { &*self.storage.get_key(child_index) }
                        );
                    }
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
