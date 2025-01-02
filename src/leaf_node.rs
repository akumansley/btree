use crate::{array_types::KeyArray, array_types::ValueArray, internal_node::InternalNode};
use smallvec::SmallVec;
use std::fmt::Debug;
pub struct LeafNode<K, V> {
    pub keys: SmallVec<KeyArray<K>>,
    pub values: SmallVec<ValueArray<V>>,
    pub num_keys: usize,
}

impl<K, V> LeafNode<K, V> {
    pub fn new() -> *mut Self {
        Box::into_raw(Box::new(LeafNode {
            keys: SmallVec::new(),
            values: SmallVec::new(),
            num_keys: 0,
        }))
    }
}

impl<K: PartialOrd + Clone + Debug, V: Debug> LeafNode<K, V> {
    pub fn get(&self, search_key: &K) -> Option<&V> {
        println!("LeafNode get {:?}", search_key);
        self.keys
            .iter()
            .zip(self.values.iter())
            .find(|(k, _)| k == &search_key)
            .map(|(_, v)| v)
    }

    pub fn insert(&mut self, key_to_insert: K, value: V) {
        println!("LeafNode insert {:?} {:?}", key_to_insert, value);
        let mut insertion_point = None;
        for (i, k) in self.keys.iter().enumerate() {
            if k == &key_to_insert {
                println!("LeafNode insert {:?} already exists", key_to_insert);
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
        println!("LeafNode inserted at {:?} ", insertion_point);
    }

    pub fn remove(&mut self, key: &K) {
        println!("LeafNode remove {:?}", key);
        if let Some(index) = self.keys.iter().position(|k| k == key) {
            self.keys.remove(index);
            self.values.remove(index);
            self.num_keys -= 1;
        } else {
            println!("LeafNode remove {:?} not found", key);
        }
    }

    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    pub fn move_from_right_neighbor_into_left_node(
        parent: *mut InternalNode<K, V>,
        mut from: Box<LeafNode<K, V>>,
        to: *mut LeafNode<K, V>,
    ) {
        unsafe {
            (*to).keys.extend(from.keys.drain(..));
            (*to).values.extend(from.values.drain(..));
            // TODO: factor out the len from the SmallVecs
            (*to).num_keys = (*to).keys.len();
            (*parent).remove(from.as_ref().into());
            // TODO: make sure there's no live reference to left
            // when we implement concurrency
            drop(from);
        }
    }

    pub fn move_last_to_front_of(
        &mut self,
        other: *mut LeafNode<K, V>,
        parent: *mut InternalNode<K, V>,
    ) {
        println!("LeafNode move_last_to_front_of");
        let last_key = self.keys.pop().unwrap();
        let last_value = self.values.pop().unwrap();
        unsafe {
            (*other).keys.insert(0, last_key.clone());
            (*other).values.insert(0, last_value);
            (*other).num_keys += 1;
        }
        self.num_keys -= 1;

        // Update the split key in the parent
        unsafe {
            (*parent).update_split_key(other.into(), last_key);
        }
    }

    pub fn move_first_to_end_of(
        &mut self,
        other: *mut LeafNode<K, V>,
        parent: *mut InternalNode<K, V>,
    ) {
        println!("LeafNode move_first_to_end_of");
        let first_key = self.keys.remove(0);
        let first_value = self.values.remove(0);
        unsafe {
            (*other).keys.push(first_key);
            (*other).values.push(first_value);
            (*other).num_keys += 1;
        }
        self.num_keys -= 1;

        // Update the split key in the parent for self
        unsafe {
            (*parent).update_split_key(self.into(), self.keys[0].clone());
        }
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
