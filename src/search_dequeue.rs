use crate::{
    node_ptr::{
        marker::{self, LockState, NodeType},
        NodePtr,
    },
    tree::{BTreeKey, BTreeValue},
};
use std::mem::MaybeUninit;

const MAX_STACK_SIZE: usize = 16;
pub struct SearchDequeue<K: BTreeKey, V: BTreeValue> {
    stack: [MaybeUninit<NodePtr<K, V, marker::Unknown, marker::Unknown>>; MAX_STACK_SIZE],
    index_of_highest_node: usize,
    index_after_lowest_node: usize,
}

impl<K: BTreeKey, V: BTreeValue> SearchDequeue<K, V> {
    pub fn new() -> Self {
        SearchDequeue {
            stack: [MaybeUninit::uninit(); MAX_STACK_SIZE],
            index_of_highest_node: 0,
            index_after_lowest_node: 0,
        }
    }

    pub fn pop_highest(&mut self) -> NodePtr<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_of_highest_node < self.index_after_lowest_node);

        unsafe {
            let node_ptr = self.stack[self.index_of_highest_node].assume_init();
            self.index_of_highest_node += 1;
            node_ptr
        }
    }

    pub fn pop_highest_until<'a, L: LockState, N: NodeType>(
        &'a mut self,
        node_ptr: NodePtr<K, V, L, N>,
    ) -> impl Iterator<Item = NodePtr<K, V, marker::Unknown, marker::Unknown>> + 'a {
        let erased_node_ptr = node_ptr.erase();
        std::iter::from_fn(move || {
            if self.peek_highest() != erased_node_ptr {
                Some(self.pop_highest())
            } else {
                None
            }
        })
    }

    pub fn drain<'a>(
        &'a mut self,
    ) -> impl Iterator<Item = NodePtr<K, V, marker::Unknown, marker::Unknown>> + 'a {
        std::iter::from_fn(move || {
            if !self.is_empty() {
                Some(self.pop_highest())
            } else {
                None
            }
        })
    }

    pub fn peek_highest(&self) -> NodePtr<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_of_highest_node < self.index_after_lowest_node);
        unsafe {
            let node_ptr = self.stack[self.index_of_highest_node].assume_init();
            node_ptr
        }
    }

    pub fn push_node_on_bottom<L: LockState, N: NodeType>(&mut self, value: NodePtr<K, V, L, N>) {
        debug_assert!(self.index_after_lowest_node < MAX_STACK_SIZE);
        self.stack[self.index_after_lowest_node].write(value.erase());
        self.index_after_lowest_node += 1;
    }

    pub fn pop_lowest(&mut self) -> NodePtr<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_after_lowest_node > 0);
        debug_assert!(self.index_after_lowest_node > self.index_of_highest_node);
        unsafe {
            self.index_after_lowest_node -= 1;
            let node_ptr = self.stack[self.index_after_lowest_node].assume_init();
            node_ptr
        }
    }

    pub fn peek_lowest(&self) -> NodePtr<K, V, marker::Unknown, marker::Unknown> {
        unsafe {
            let node_ptr = self.stack[self.index_after_lowest_node - 1].assume_init();
            node_ptr
        }
    }

    pub fn is_empty(&self) -> bool {
        self.index_after_lowest_node == self.index_of_highest_node
    }

    pub fn len(&self) -> usize {
        self.index_after_lowest_node - self.index_of_highest_node
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::{
        leaf_node::LeafNode,
        node_ptr::{marker, NodePtr},
        tree::{BTreeKey, BTreeValue},
    };

    fn create_dummy_leaf_node_ptr<K: BTreeKey, V: BTreeValue>(
    ) -> NodePtr<K, V, marker::Unlocked, marker::Leaf> {
        let node = LeafNode::<K, V>::new();
        NodePtr::from_leaf_unlocked(node)
    }

    #[test]
    fn test_push_and_pop() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        let node_ptr = create_dummy_leaf_node_ptr();
        assert!(node_ptr.is_leaf());

        dequeue.push_node_on_bottom(node_ptr.erase());
        assert_eq!(dequeue.len(), 1);

        let popped_node = dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 0);
        assert!(popped_node.is_leaf());
        unsafe {
            ptr::drop_in_place(popped_node.assert_leaf().to_mut_leaf_ptr());
        }
    }

    #[test]
    fn test_is_empty_behavior() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        assert!(dequeue.is_empty());

        let node_ptr = create_dummy_leaf_node_ptr();
        dequeue.push_node_on_bottom(node_ptr.erase());
        assert!(!dequeue.is_empty());

        dequeue.pop_lowest();
        assert!(dequeue.is_empty());
        unsafe {
            ptr::drop_in_place(node_ptr.to_mut_leaf_ptr());
        }
    }

    #[test]
    fn test_len_behavior() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        assert_eq!(dequeue.len(), 0);

        let node_ptr = create_dummy_leaf_node_ptr();
        dequeue.push_node_on_bottom(node_ptr.erase());
        assert_eq!(dequeue.len(), 1);

        dequeue.push_node_on_bottom(node_ptr.erase());
        assert_eq!(dequeue.len(), 2);

        dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 1);

        dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 0);
        unsafe {
            ptr::drop_in_place(node_ptr.to_mut_leaf_ptr());
        }
    }
}
