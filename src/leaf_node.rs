use crate::{
    array_types::{KeyArray, ValueArray, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE},
    debug_println,
    node::{Height, NodeHeader},
    node_ptr::{marker, NodePtr},
    tree::{BTreeKey, BTreeValue, ModificationType},
};
use smallvec::SmallVec;
use std::{cell::UnsafeCell, ptr};

#[repr(C)]
pub struct LeafNode<K: BTreeKey, V: BTreeValue> {
    header: NodeHeader,
    pub inner: UnsafeCell<LeafNodeInner<K, V>>,
}
pub struct LeafNodeInner<K: BTreeKey, V: BTreeValue> {
    pub keys: SmallVec<KeyArray<K>>,
    pub values: SmallVec<ValueArray<V>>,
    pub num_keys: usize,
}

impl<K: BTreeKey, V: BTreeValue> LeafNode<K, V> {
    pub fn new() -> *mut Self {
        Box::into_raw(Box::new(LeafNode {
            header: NodeHeader::new(Height::Leaf),
            inner: UnsafeCell::new(LeafNodeInner {
                keys: SmallVec::new(),
                values: SmallVec::new(),
                num_keys: 0,
            }),
        }))
    }
}

impl<K: BTreeKey, V: BTreeValue> LeafNodeInner<K, V> {
    pub fn get(&self, search_key: &K) -> Option<*const V> {
        debug_println!("LeafNode get {:?}", search_key);
        self.keys
            .iter()
            .zip(self.values.iter())
            .find(|(k, _)| k == &search_key)
            .map(|(_, v)| v as *const V)
    }

    pub fn insert(&mut self, key_to_insert: K, value: V) {
        debug_println!("LeafNode insert {:?} {:?}", key_to_insert, value);
        let mut insertion_point = None;
        for (i, k) in self.keys.iter().enumerate() {
            if k == &key_to_insert {
                debug_println!("LeafNode insert {:?} already exists", key_to_insert);
                self.values[i] = value;
                return;
            }
            if k > &key_to_insert && insertion_point.is_none() {
                insertion_point = Some(i);
            }
        }

        let insertion_point = insertion_point.unwrap_or(self.num_keys);
        self.keys.insert(insertion_point, key_to_insert);
        self.values.insert(insertion_point, value);
        self.num_keys += 1;
        debug_println!("LeafNode inserted at {:?} ", insertion_point);
    }

    pub fn remove(&mut self, key: &K) {
        debug_println!("LeafNode remove {:?}", key);
        if let Some(index) = self.keys.iter().position(|k| k == key) {
            self.keys.remove(index);
            self.values.remove(index);
            self.num_keys -= 1;
        } else {
            debug_println!("LeafNode remove {:?} not found", key);
        }
    }

    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    pub fn has_capacity_for_modification(&self, modification_type: ModificationType) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => self.num_keys > MIN_KEYS_PER_NODE,
            ModificationType::NonModifying => true,
        }
    }

    // a leaf at the top of tree is allowed to get underfull -- as low as 0
    pub fn has_capacity_for_modification_as_top_of_tree(
        &self,
        modification_type: ModificationType,
    ) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys < MAX_KEYS_PER_NODE,
            ModificationType::Removal => true,
            ModificationType::NonModifying => true,
        }
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
        mut from: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
        mut to: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
    ) {
        to.keys.extend(from.keys.drain(..));
        to.values.extend(from.values.drain(..));
        // TODO: factor out the len from the SmallVecs
        to.num_keys = to.keys.len();
        parent.remove(from.to_unlocked().erase_node_type());
        // this is not necessary, but it lets is track the lock count correctly
        from.unlock_exclusive();
        // TODO: make sure there's no live sibling reference to left
        // when we implement concurrency
        unsafe {
            ptr::drop_in_place(from.to_mut_leaf_ptr());
        }
    }

    pub fn move_last_to_front_of(
        mut left: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
        mut right: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_last_to_front_of");
        let last_key = left.keys.pop().unwrap();
        let last_value = left.values.pop().unwrap();
        right.keys.insert(0, last_key.clone());
        right.values.insert(0, last_value);
        right.num_keys += 1;
        left.num_keys -= 1;

        // Update the split key in the parent
        parent.update_split_key(right.to_stored(), last_key);
    }

    pub fn move_first_to_end_of(
        mut right: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
        mut left: NodePtr<K, V, marker::Exclusive, marker::Leaf>,
        mut parent: NodePtr<K, V, marker::Exclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_first_to_end_of");
        let first_key = right.keys.remove(0);
        let first_value = right.values.remove(0);

        left.keys.push(first_key);
        left.values.push(first_value);
        left.num_keys += 1;
        right.num_keys -= 1;

        // Update the split key in the parent for self
        parent.update_split_key(right.to_stored(), right.keys[0].clone());
    }

    pub fn print_node(&self) {
        println!("LeafNode: {:p}", self);
        println!("+----------------------+");
        println!("| Num Keys: {}           |", self.num_keys);
        println!("+----------------------+");
        println!("| Keys and Values:     |");
        if self.num_keys > 0 {
            for i in 0..self.num_keys {
                println!("|  - Key: {:?}         |", self.keys[i]);
                println!("|  - Value: {:?}       |", self.values[i]);
            }
        }
        println!("+----------------------+");
    }

    pub fn check_invariants(&self) {
        assert_eq!(
            self.num_keys,
            self.keys.len(),
            "num_keys does not match the actual number of keys"
        );
        assert_eq!(
            self.num_keys,
            self.values.len(),
            "Number of keys and values are inconsistent"
        );
    }
}
