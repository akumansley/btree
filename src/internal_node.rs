use crate::{
    array_types::{ChildArray, KeyArray},
    leaf_node::LeafNode,
    pointer_types::{NodePtr, NodeRef},
    tree::UnderfullNodePosition,
};
use smallvec::SmallVec;
use std::fmt::Debug;
use std::ptr;

pub struct InternalNode<K, V> {
    pub keys: SmallVec<KeyArray<K>>,
    pub children: SmallVec<ChildArray<NodePtr<K, V>>>,
    pub parent: *mut InternalNode<K, V>,
    pub parent_index: Option<u16>,
    pub num_keys: usize,
}

impl<K: PartialOrd + Debug + Clone, V: Debug> InternalNode<K, V> {
    pub fn new() -> *mut Self {
        Box::into_raw(Box::new(InternalNode {
            keys: SmallVec::new(),
            children: SmallVec::new(),
            parent: ptr::null_mut(),
            parent_index: None,
            num_keys: 0,
        }))
    }

    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    pub fn insert(&mut self, key: K, node: NodePtr<K, V>) {
        let insertion_point = self
            .keys
            .iter()
            .position(|k| k > &key)
            .unwrap_or(self.num_keys);

        self.keys.insert(insertion_point, key);
        self.children.insert(insertion_point + 1, node);
        self.num_keys += 1;
    }

    pub fn find_child(&self, search_key: &K) -> NodePtr<K, V> {
        let index = self
            .keys
            .iter()
            .position(|k| k > &search_key)
            .unwrap_or(self.num_keys); // self.children has one more element than self.keys, so the len is the index
        self.children[index]
    }

    pub fn get_key_for_non_leftmost_child(&self, child: NodePtr<K, V>) -> K {
        let index = self.children.iter().position(|c| *c == child).unwrap();
        self.keys[index - 1].clone()
    }

    pub fn print_node(&self, height: usize) {
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
            let node_ref = NodeRef::new(*child, height - 1);
            node_ref.print_node();
        }
    }

    fn get_neighbor(&self, child: NodePtr<K, V>) -> (NodePtr<K, V>, UnderfullNodePosition) {
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
        child: *mut InternalNode<K, V>,
    ) -> (*mut InternalNode<K, V>, UnderfullNodePosition) {
        let (neighbor, direction) = self.get_neighbor(child.into());
        unsafe {
            return (neighbor.as_internal_node_ptr(), direction);
        }
    }

    pub(crate) fn get_neighbor_of_underfull_leaf(
        &self,
        child: *mut LeafNode<K, V>,
    ) -> (*mut LeafNode<K, V>, UnderfullNodePosition) {
        let (neighbor, direction) = self.get_neighbor(child.into());
        unsafe {
            return (neighbor.as_leaf_node_ptr(), direction);
        }
    }

    pub(crate) fn remove(&mut self, child: NodePtr<K, V>) {
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
        parent: *mut InternalNode<K, V>,
        height: usize,
        mut from: Box<InternalNode<K, V>>,
        to: *mut InternalNode<K, V>,
    ) {
        unsafe {
            println!(
                "moving from {:?} {:?} into {:?} {:?} at height {}",
                (*from).keys,
                (*from).children,
                (*to).keys,
                (*to).children,
                height
            );
            let split_key = (*parent).get_key_for_non_leftmost_child(from.as_ref().into());
            (*to).keys.push(split_key);
            (*to).keys.extend(from.keys.drain(..));
            println!("moved keys {:?}", (*to).keys);
            for child in from.children.drain(..) {
                println!("moving child {:?}", child);
                // the NodeRef lets us discriminate between internal and leaf nodes
                // which is why we need the height parameter
                let child_node_ref = NodeRef::new(child, height - 1);
                child_node_ref.set_parent(to);
                (*to).children.push(child);
            }
            (*to).num_keys = (*to).keys.len();
            (*parent).remove(from.as_ref().into());
            drop(from);
        }
    }

    pub fn move_last_to_front_of(&mut self, other: *mut InternalNode<K, V>, height: usize) {
        println!("InternalNode move_last_to_front_of");
        unsafe {
            let last_key = self.keys.pop().unwrap(); // if these don't exist, we've got bigger problems
            let last_child = self.children.pop().unwrap();

            // last key wants to become the _parent_ split key
            let parent = self.parent;
            let old_split_key = (*parent).update_split_key(other.into(), last_key);

            (*other).keys.insert(0, old_split_key); // and the node's old parent split key is now its first key
            (*other).children.insert(0, last_child);
            self.num_keys -= 1;
            (*other).num_keys += 1;

            // Update the parent pointer of the moved child
            let child_node_ref = NodeRef::new(last_child, height - 1);
            child_node_ref.set_parent(other);
        }
    }

    // we'll only call this for children other than the leftmost
    pub(crate) fn update_split_key(&mut self, node: NodePtr<K, V>, mut new_split_key: K) -> K {
        println!(
            "InternalNode update_split_key of {:?} to {:?}",
            node, new_split_key
        );
        let index = self.children.iter().position(|c| *c == node).unwrap();
        debug_assert!(index > 0);
        std::mem::swap(&mut self.keys[index - 1], &mut new_split_key);
        new_split_key
    }

    pub fn move_first_to_end_of(&mut self, other: *mut InternalNode<K, V>, height: usize) {
        println!("InternalNode move_first_to_end_of");
        unsafe {
            let first_key = self.keys.remove(0); // no! this is the split key for ourselves, not the other node
            let first_child = self.children.remove(0);
            // Update the split key in the parent for self
            let parent = self.parent;
            let old_split_key = (*parent).update_split_key(self.into(), first_key);

            (*other).keys.push(old_split_key);
            (*other).children.push(first_child);
            self.num_keys -= 1;
            (*other).num_keys += 1;

            // Update the parent pointer of the moved child
            let child_node_ref = NodeRef::new(first_child, height - 1);
            child_node_ref.set_parent(other);
        }
    }

    pub fn check_invariants(&self, height: usize) {
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

        // Check invariants for each child
        for child in &self.children {
            let node_ref = NodeRef::new(*child, height - 1);
            if node_ref.is_internal() {
                unsafe {
                    (*node_ref.as_internal_node()).check_invariants(height - 1);
                }
            } else {
                unsafe {
                    (*node_ref.as_leaf_node()).check_invariants();
                }
            }
        }
    }
}
