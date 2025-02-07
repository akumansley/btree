use crate::{
    array_types::{LeafNodeStorageArray, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE},
    debug_println,
    graceful_pointers::{GracefulArc, GracefulAtomicPointer, GracefulBox},
    node::{Height, NodeHeader},
    node_ptr::{
        marker::{self},
        NodeRef,
    },
    qsbr::qsbr_reclaimer,
    tree::{BTreeKey, BTreeValue, ModificationType},
};
use std::{cell::UnsafeCell, ptr, sync::atomic::Ordering};

#[repr(C)]
pub struct LeafNode<K: BTreeKey, V: BTreeValue> {
    header: NodeHeader,
    pub inner: UnsafeCell<LeafNodeInner<K, V>>,
}
pub struct LeafNodeInner<K: BTreeKey, V: BTreeValue> {
    pub storage: LeafNodeStorageArray<K, V>,
}

impl<K: BTreeKey, V: BTreeValue> LeafNode<K, V> {
    pub fn new() -> *mut Self {
        Box::into_raw(Box::new(LeafNode {
            header: NodeHeader::new(Height::Leaf),
            inner: UnsafeCell::new(LeafNodeInner {
                storage: LeafNodeStorageArray::new(),
            }),
        }))
    }
}

impl<K: BTreeKey, V: BTreeValue> LeafNodeInner<K, V> {
    fn binary_search_key(&self, search_key: &K) -> Result<usize, usize> {
        self.storage.binary_search_keys(search_key)
    }

    pub fn get(&self, search_key: &K) -> Option<*const V> {
        debug_println!("LeafNode get {:?}", search_key);
        match self.binary_search_key(search_key) {
            Ok(index) => Some(self.storage.values()[index].load(Ordering::Relaxed) as *const V),
            Err(_) => {
                debug_println!("LeafNode get {:?} not found", search_key);
                None
            }
        }
    }

    pub fn insert(&mut self, key_to_insert: GracefulArc<K>, value: *mut V) {
        match self.binary_search_key(&*key_to_insert) {
            Ok(index) => {
                let old_value = self.storage.set(index, value);
                if old_value != ptr::null_mut() {
                    // we always use share locks for leaf nodes, so this is safe to drop immediately
                    unsafe {
                        ptr::drop_in_place(old_value);
                    }
                }
                // no need to qsbr this key -- it can't have been published yet
                unsafe {
                    key_to_insert.drop_in_place();
                }
            }
            Err(index) => {
                self.storage.insert(key_to_insert, value, index);
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> bool {
        match self.binary_search_key(key) {
            Ok(index) => {
                let (key, value) = self.storage.remove(index);
                unsafe {
                    key.drop_in_place();
                    ptr::drop_in_place(value);
                }
                true
            }
            Err(_) => false,
        }
    }

    pub fn num_keys(&self) -> usize {
        self.storage.num_keys()
    }

    pub fn has_capacity_for_modification(&self, modification_type: ModificationType) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys() < MAX_KEYS_PER_NODE,
            ModificationType::Removal => self.num_keys() > MIN_KEYS_PER_NODE,
            ModificationType::NonModifying => true,
        }
    }

    // a leaf at the top of tree is allowed to get underfull -- as low as 0
    pub fn has_capacity_for_modification_as_top_of_tree(
        &self,
        modification_type: ModificationType,
    ) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys() < MAX_KEYS_PER_NODE,
            ModificationType::Removal => true,
            ModificationType::NonModifying => true,
        }
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
        from: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        to: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    ) {
        debug_println!("LeafNode move_from_right_neighbor_into_left_node");
        to.storage.extend(from.storage.drain());

        parent.remove(from.node_ptr());
        // TODO: make sure there's no live sibling reference to left
        // when we implement concurrency
        // retire the leaf -- this ensures that any optimistic readers will fail
        from.retire();
        // qsbr-drop the leaf
        let from_box = GracefulBox::new(from.to_raw_leaf_ptr());
        qsbr_reclaimer().add_callback(Box::new(move || {
            drop(from_box);
        }));
    }

    pub fn move_last_to_front_of(
        left: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        right: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_last_to_front_of");

        let (last_key, last_value) = left.storage.pop();

        right
            .storage
            .insert(last_key.clone_and_increment_ref_count(), last_value, 0);

        // Update the split key in the parent -- and we did need to increment the ref count above
        // because we're copying the first key to the parent
        let old_key = parent.update_split_key(right.node_ptr(), last_key);

        // and we need to decrement the ref count on the old key
        old_key.decrement_ref_count();
    }

    pub fn move_first_to_end_of(
        right: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        left: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
        mut parent: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_first_to_end_of ");
        let (first_key, first_value) = right.storage.remove(0);
        left.storage.push(first_key, first_value);

        // Update the split key in the parent for self, cloning it upwards
        let new_split_key = right.storage.keys()[0]
            .load(Ordering::Relaxed)
            .clone_and_increment_ref_count();

        // and we need to decrement the ref count on the old key
        let old_key = parent.update_split_key(right.node_ptr(), new_split_key);
        old_key.decrement_ref_count();
    }

    pub fn print_node(&self) {
        println!("LeafNode: {:p}", self);
        println!("+----------------------+");
        println!("| Num Keys: {}           |", self.num_keys());
        println!("+----------------------+");
        println!("| Keys and Values:     |");
        if self.num_keys() > 0 {
            for i in 0..self.num_keys() {
                println!(
                    "|  - Key: {:?}         |",
                    self.storage.keys()[i].load(Ordering::Relaxed).as_ref()
                );
                println!(
                    "|  - Value: {:?}       |",
                    self.storage.values()[i].load(Ordering::Relaxed)
                );
            }
        }
        println!("+----------------------+");
    }

    pub fn check_invariants(&self) {
        self.storage.check_invariants();
    }
}
