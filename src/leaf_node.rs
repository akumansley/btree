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
use std::{
    cell::UnsafeCell,
    ptr,
    sync::atomic::{AtomicPtr, Ordering},
};

#[repr(C)]
pub struct LeafNode<K: BTreeKey, V: BTreeValue> {
    header: NodeHeader,
    pub inner: UnsafeCell<LeafNodeInner<K, V>>,
}
pub struct LeafNodeInner<K: BTreeKey, V: BTreeValue> {
    pub storage: LeafNodeStorageArray<K, V>,
    pub next_leaf: AtomicPtr<LeafNode<K, V>>,
    pub prev_leaf: AtomicPtr<LeafNode<K, V>>,
}

impl<K: BTreeKey, V: BTreeValue> LeafNode<K, V> {
    pub fn new() -> *mut Self {
        Box::into_raw(Box::new(LeafNode {
            header: NodeHeader::new(Height::Leaf),
            inner: UnsafeCell::new(LeafNodeInner {
                storage: LeafNodeStorageArray::new(),
                next_leaf: AtomicPtr::new(ptr::null_mut()),
                prev_leaf: AtomicPtr::new(ptr::null_mut()),
            }),
        }))
    }

    pub unsafe fn get_inner(&mut self) -> &LeafNodeInner<K, V> {
        unsafe { &*self.inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> LeafNodeInner<K, V> {
    pub fn next_leaf(&self) -> Option<NodeRef<K, V, marker::Unlocked, marker::Leaf>> {
        let next_leaf_ptr = self.next_leaf.load(Ordering::Acquire);
        if next_leaf_ptr.is_null() {
            None
        } else {
            Some(NodeRef::from_leaf_unlocked(next_leaf_ptr as *mut _))
        }
    }
    pub fn prev_leaf(&self) -> Option<NodeRef<K, V, marker::Unlocked, marker::Leaf>> {
        let prev_leaf_ptr = self.prev_leaf.load(Ordering::Acquire);
        if prev_leaf_ptr.is_null() {
            None
        } else {
            Some(NodeRef::from_leaf_unlocked(prev_leaf_ptr as *mut _))
        }
    }

    pub fn binary_search_key(&self, search_key: &K) -> Result<usize, usize> {
        self.storage.binary_search_keys(search_key)
    }

    pub fn get(&self, search_key: &K) -> Option<(GracefulArc<K>, *const V)> {
        debug_println!("LeafNode get {:?}", search_key);
        match self.binary_search_key(search_key) {
            Ok(index) => Some((
                self.storage.keys()[index].load(Ordering::Relaxed),
                self.storage.values()[index].load(Ordering::Relaxed) as *const V,
            )),
            Err(_) => {
                debug_println!("LeafNode get {:?} not found", search_key);
                None
            }
        }
    }
    pub fn insert(&mut self, key_to_insert: GracefulArc<K>, value: *mut V) -> bool {
        match self.binary_search_key(&*key_to_insert) {
            Ok(index) => {
                let old_value = self.storage.set(index, value);
                if old_value != ptr::null_mut() {
                    let value_box = GracefulBox::new(old_value);
                    qsbr_reclaimer().add_callback(Box::new(move || {
                        drop(value_box);
                    }));
                }
                // no need to qsbr this key -- it can't have been published yet
                unsafe {
                    key_to_insert.drop_in_place();
                }
                false
            }
            Err(index) => {
                self.insert_new_value_at_index(key_to_insert, value, index);
                true
            }
        }
    }

    pub fn insert_new_value_at_index(
        &mut self,
        key_to_insert: GracefulArc<K>,
        value: *mut V,
        index: usize,
    ) {
        self.storage.insert(key_to_insert, value, index);
    }

    pub fn update(&mut self, index: usize, value: *mut V) {
        let old_value = self.storage.set(index, value);
        if old_value == ptr::null_mut() {
            println!(
                "update: old_value is null, thread: {:?}, index: {}, value: {:?}",
                std::thread::current().id(),
                index,
                unsafe { &*value }
            );
        }
        assert!(old_value != ptr::null_mut());
        let value_box = GracefulBox::new(old_value);
        qsbr_reclaimer().add_callback(Box::new(move || {
            drop(value_box);
        }));
    }

    pub fn remove(&mut self, key: &K) -> bool {
        match self.binary_search_key(key) {
            Ok(index) => {
                self.remove_at_index(index);
                true
            }
            Err(_) => false,
        }
    }
    pub fn remove_at_index(&mut self, index: usize) {
        let (stored_key, value) = self.storage.remove(index);
        stored_key.decrement_ref_count();
        let value_box = GracefulBox::new(value);
        qsbr_reclaimer().add_callback(Box::new(move || {
            drop(value_box);
        }));
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
        // update sibling pointers
        if let Some(right_neighbor_next_leaf) = from.next_leaf() {
            let right_neighbor_next_leaf = right_neighbor_next_leaf.lock_exclusive();

            right_neighbor_next_leaf
                .prev_leaf
                .store(to.to_raw_leaf_ptr(), Ordering::Relaxed);

            to.next_leaf.store(
                right_neighbor_next_leaf.to_raw_leaf_ptr(),
                Ordering::Relaxed,
            );
            right_neighbor_next_leaf.unlock_exclusive();
        } else {
            to.next_leaf.store(ptr::null_mut(), Ordering::Relaxed);
        }

        to.storage.extend(from.storage.drain());
        let key = parent.remove(from.node_ptr());
        key.decrement_ref_count();
        // retire the leaf -- this ensures that any optimistic readers will fail
        // we've already drained it, so any moved keys won't be freed (which is what we want)
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
