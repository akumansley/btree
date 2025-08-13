use crate::coalescing::UnderfullNodePosition;
use crate::pointers::OwnedNodeRef;
use crate::pointers::{SharedDiscriminatedNode, SharedNodeRef};
use crate::{
    array_types::{
        InternalNodeStorage, MAX_CHILDREN_PER_NODE, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE,
    },
    node::{Height, NodeHeader},
    pointers::node_ref::marker::{self, LockState},
    tree::{BTreeKey, BTreeValue, ModificationType},
    util::UnwrapEither,
};
use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::Deref;
use thin::{QsArc, QsOwned, QsShared, QsWeak};

// this is the shared node data
pub struct InternalNodeInner<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub storage: InternalNodeStorage<MAX_KEYS_PER_NODE, MAX_CHILDREN_PER_NODE, K, V>,
    pub phantom: PhantomData<V>,
}

#[repr(C)]
pub struct InternalNode<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub header: NodeHeader,
    pub inner: UnsafeCell<InternalNodeInner<K, V>>,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for InternalNode<K, V> {
    fn drop(&mut self) {
        assert!(matches!(self.header.height(), Height::Internal(_)));
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> InternalNode<K, V> {
    pub fn new(height: Height) -> InternalNode<K, V> {
        InternalNode {
            header: NodeHeader::new(height),
            inner: UnsafeCell::new(InternalNodeInner {
                storage: InternalNodeStorage::<MAX_KEYS_PER_NODE, MAX_CHILDREN_PER_NODE, K, V>::new(
                ),
                phantom: PhantomData,
            }),
        }
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> InternalNodeInner<K, V> {
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

    pub fn insert(&mut self, key: QsArc<K>, new_child: QsOwned<NodeHeader>) {
        let insertion_point = self.storage.binary_search_keys(key.deref()).unwrap_either();

        self.storage
            .insert_child_with_split_key(key, new_child, insertion_point);
    }

    pub fn get_first_child(&self) -> QsShared<NodeHeader> {
        self.storage.get_child(0)
    }

    pub fn get_last_child(&self) -> QsShared<NodeHeader> {
        // there's always (keys + 1) children
        self.storage.get_child(self.num_keys())
    }

    pub fn index_of_child_containing_key<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        match self.storage.binary_search_keys(key) {
            Ok(index) => index + 1,
            Err(index) => index,
        }
    }

    pub fn find_child<Q>(&self, search_key: &Q) -> QsShared<NodeHeader>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        let index = self.index_of_child_containing_key(search_key);
        // SAFETY: This may be an optimistic read, so we might be reading an index that exceeds the current number of children
        // but because node drops are protected by qsbr, that's okay -- we'll find a retired node.
        unsafe { self.storage.get_child_unchecked(index) }
    }

    pub fn get_key_for_non_leftmost_child(&self, child: QsShared<NodeHeader>) -> QsWeak<K> {
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
            println!("|  - Key: {:?}         |", self.storage.get_key(i));
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
                SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                    child,
                )
                .assume_unlocked();
            match child_ref.force() {
                SharedDiscriminatedNode::Internal(internal_child) => {
                    let locked_child = internal_child.lock_shared();
                    locked_child.print_node();
                    locked_child.unlock_shared();
                }
                SharedDiscriminatedNode::Leaf(leaf_child) => {
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
        child: QsShared<NodeHeader>,
    ) -> (QsShared<NodeHeader>, UnderfullNodePosition) {
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
        child: SharedNodeRef<K, V, L, marker::Internal>,
    ) -> (
        SharedNodeRef<K, V, marker::Unlocked, marker::Internal>,
        UnderfullNodePosition,
    ) {
        debug_assert!(child.is_internal());
        let (neighbor, direction) = self.get_neighbor(child.into_ptr());
        let neighbor_ref =
            SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                neighbor,
            );
        debug_println!(
            "InternalNode get_neighboring_internal_node on parent node {:?} for child {:?} found neighbor {:?}",
            self as *const _,
            child.node_ptr(),
            neighbor_ref.node_ptr()
        );
        (neighbor_ref.assume_unlocked().assert_internal(), direction)
    }

    pub(crate) fn get_neighbor_of_underfull_leaf<L: LockState>(
        &self,
        child: SharedNodeRef<K, V, L, marker::Leaf>,
    ) -> (
        SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>,
        UnderfullNodePosition,
    ) {
        let (neighbor, direction) = self.get_neighbor(child.into_ptr());
        let neighbor_ref =
            SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                neighbor,
            );
        (neighbor_ref.assert_leaf().assume_unlocked(), direction)
    }

    pub(crate) fn remove_child(
        &mut self,
        child: QsShared<NodeHeader>,
    ) -> (QsArc<K>, QsOwned<NodeHeader>) {
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

        let (key, node) = self.storage.remove_child_at_index(index);
        // we don't remove the leftmost element
        debug_assert!(index > 0);
        (key, node)
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        from: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        to: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_from_right_neighbor_into_left_node");

        let (key, from_owned) = parent.remove_child(from.into_ptr());
        let from_owned =
            OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                from_owned,
            )
            .assert_internal()
            .assert_exclusive();

        to.storage.extend(from_owned.storage.drain(key));

        from_owned.retire();
        drop(from_owned);
    }

    pub fn move_last_to_front_of(
        left: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        right: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    ) {
        debug_println!("InternalNode move_last_to_front_of");
        let (last_key, last_child) = left.storage.pop();

        // last key wants to become the _parent_ split key
        // no need to change the ref count -- it's just getting moved
        let old_split_key = parent.update_split_key(right.into_ptr(), last_key);

        right.storage.insert(old_split_key, last_child, 0);
    }

    // we'll only call this for children other than the leftmost
    // we also assume the caller has accounted for any reference count changes on the key
    pub(crate) fn update_split_key(
        &mut self,
        node: QsShared<NodeHeader>,
        new_split_key: QsArc<K>,
    ) -> QsArc<K> {
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

        self.storage.replace_key(child_index - 1, new_split_key)
    }

    pub fn move_first_to_end_of(
        right: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        left: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    ) {
        debug_println!(
            "InternalNode move_first_to_end_of from first of right: {:?} to left:{:?} to parent:{:?}",
            right.into_ptr(),
            left.into_ptr(),
            parent.into_ptr()
        );
        let (first_key, first_child) = right.storage.remove(0);
        // Update the split key in the parent for the right child

        let old_split_key = parent.update_split_key(right.into_ptr(), first_key);

        left.storage.push(old_split_key, first_child);
    }

    pub fn check_invariants(&self) {
        debug_println!("InternalNode check_invariants {:?}", self as *const _);
        self.storage.check_invariants();

        let mut height = None;
        // Check invariants for each child
        for (child_index, child) in self.storage.iter_children().enumerate() {
            let child_ref =
                SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                    child,
                )
                .assume_unlocked();
            if height.is_none() {
                height = Some(child_ref.height());
            } else {
                assert!(child_ref.height() == height.unwrap())
            }

            match child_ref.force() {
                SharedDiscriminatedNode::Internal(internal_child) => {
                    let locked_child = internal_child.lock_shared();

                    if child_index > 0 {
                        assert!(
                            locked_child.storage.get_key(0).deref()
                                >= self.storage.get_key(child_index - 1).deref(),
                            "internal child first key {:?} is greater or equal to the parent's split key {:?} for that child",
                            locked_child.storage.get_key(0),
                            self.storage.get_key(child_index - 1)
                        );
                    }
                    if child_index < self.num_keys() - 1 {
                        assert!(
                            locked_child.storage.get_key(locked_child.num_keys() - 1).deref()
                                <= self.storage.get_key(child_index).deref(),
                            "internal child last key {:?} is greater than the parent's split key {:?} for the next child",
                            locked_child.storage.get_key(locked_child.num_keys() - 1),
                            self.storage.get_key(child_index)
                        );
                    }

                    locked_child.check_invariants();
                    locked_child.unlock_shared();
                }
                SharedDiscriminatedNode::Leaf(leaf_child) => {
                    let locked_child = leaf_child.lock_shared();
                    // assert that the child's first key is after the child's split key
                    if child_index > 0 {
                        assert!(
                            locked_child.storage.get_key(0).deref()
                                >= self.storage.get_key(child_index - 1).deref(),
                            "leaf child's first key {:?} is before the child's split key {:?}",
                            locked_child.storage.get_key(0),
                            self.storage.get_key(child_index - 1)
                        );
                    }
                    // assert that the child's last key is before the next child's split key
                    if child_index < self.num_keys() - 1 {
                        assert!(
                            locked_child
                                .storage
                                .get_key(locked_child.num_keys() - 1)
                                .deref()
                                <= self.storage.get_key(child_index).deref(),
                            "leaf child's last key {:?} is after the next child's split key {:?}",
                            locked_child.storage.get_key(locked_child.num_keys() - 1),
                            self.storage.get_key(child_index)
                        );
                    }
                    assert!(locked_child.num_keys() >= MIN_KEYS_PER_NODE);
                    locked_child.check_invariants();
                    locked_child.unlock_shared();
                }
                SharedDiscriminatedNode::Root(_) => {
                    panic!("expected internal or leaf node");
                }
            }
        }
    }
}
