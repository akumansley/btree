use crate::{
    pointers::node_ref::{
        marker::{self, LockState, NodeType},
        SharedNodeRef,
    },
    tree::{BTreeKey, BTreeValue},
};
use std::mem::MaybeUninit;

const MAX_STACK_SIZE: usize = 16;
pub struct SearchDequeue<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    stack: [MaybeUninit<SharedNodeRef<K, V, marker::Unknown, marker::Unknown>>; MAX_STACK_SIZE],
    index_of_highest_node: usize,
    index_after_lowest_node: usize,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> SearchDequeue<K, V> {
    pub fn new() -> Self {
        SearchDequeue {
            stack: [const { MaybeUninit::uninit() }; MAX_STACK_SIZE],
            index_of_highest_node: 0,
            index_after_lowest_node: 0,
        }
    }

    pub fn pop_highest(&mut self) -> SharedNodeRef<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_of_highest_node < self.index_after_lowest_node);

        unsafe {
            let node_ptr = self.stack[self.index_of_highest_node].assume_init();
            self.index_of_highest_node += 1;
            node_ptr
        }
    }

    pub fn pop_highest_until<'a, L: LockState + 'a, N: NodeType + 'a>(
        &'a mut self,
        node_ref: SharedNodeRef<K, V, L, N>,
    ) -> impl Iterator<Item = SharedNodeRef<K, V, marker::Unknown, marker::Unknown>> + 'a {
        std::iter::from_fn(move || {
            if self.peek_highest() != node_ref {
                Some(self.pop_highest())
            } else {
                None
            }
        })
    }

    pub fn drain<'a>(
        &'a mut self,
    ) -> impl Iterator<Item = SharedNodeRef<K, V, marker::Unknown, marker::Unknown>> + 'a {
        std::iter::from_fn(move || {
            if !self.is_empty() {
                Some(self.pop_highest())
            } else {
                None
            }
        })
    }

    pub fn peek_highest(&self) -> SharedNodeRef<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_of_highest_node < self.index_after_lowest_node);
        unsafe { self.stack[self.index_of_highest_node].assume_init() }
    }

    pub fn push_node_on_bottom<L: LockState, N: NodeType>(
        &mut self,
        value: SharedNodeRef<K, V, L, N>,
    ) {
        debug_assert!(self.index_after_lowest_node < MAX_STACK_SIZE);
        self.stack[self.index_after_lowest_node].write(value.erase());
        self.index_after_lowest_node += 1;
    }

    pub fn pop_lowest(&mut self) -> SharedNodeRef<K, V, marker::Unknown, marker::Unknown> {
        debug_assert!(self.index_after_lowest_node > 0);
        debug_assert!(self.index_after_lowest_node > self.index_of_highest_node);
        unsafe {
            self.index_after_lowest_node -= 1;
            self.stack[self.index_after_lowest_node].assume_init()
        }
    }

    pub fn peek_lowest(&self) -> SharedNodeRef<K, V, marker::Unknown, marker::Unknown> {
        unsafe { self.stack[self.index_after_lowest_node - 1].assume_init() }
    }

    pub fn is_empty(&self) -> bool {
        self.index_after_lowest_node == self.index_of_highest_node
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.index_after_lowest_node - self.index_of_highest_node
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        leaf_node::LeafNode,
        pointers::{node_ref::SharedNodeRef, OwnedThinPtr},
        tree::{BTreeKey, BTreeValue},
    };

    fn create_dummy_leaf_node_ptr<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    ) -> OwnedThinPtr<LeafNode<K, V>> {
        OwnedThinPtr::new(LeafNode::<K, V>::new())
    }

    #[test]
    fn test_push_and_pop() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        let node_ptr = create_dummy_leaf_node_ptr();
        let node_ref = SharedNodeRef::from_leaf_ptr(node_ptr.share());
        assert!(node_ref.is_leaf());

        dequeue.push_node_on_bottom(node_ref);
        assert_eq!(dequeue.len(), 1);

        let popped_node = dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 0);
        assert!(popped_node.is_leaf());
        OwnedThinPtr::drop_immediately(node_ptr);
    }

    #[test]
    fn test_is_empty_behavior() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        assert!(dequeue.is_empty());

        let node_ptr = create_dummy_leaf_node_ptr();
        let node_ref = SharedNodeRef::from_leaf_ptr(node_ptr.share());
        dequeue.push_node_on_bottom(node_ref);
        assert!(!dequeue.is_empty());

        dequeue.pop_lowest();
        assert!(dequeue.is_empty());
        OwnedThinPtr::drop_immediately(node_ptr);
    }

    #[test]
    fn test_len_behavior() {
        let mut dequeue: SearchDequeue<i32, i32> = SearchDequeue::new();
        assert_eq!(dequeue.len(), 0);

        let node_ptr = create_dummy_leaf_node_ptr();
        let node_ref = SharedNodeRef::from_leaf_ptr(node_ptr.share());
        dequeue.push_node_on_bottom(node_ref);
        assert_eq!(dequeue.len(), 1);

        dequeue.push_node_on_bottom(node_ref);
        assert_eq!(dequeue.len(), 2);

        dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 1);

        dequeue.pop_lowest();
        assert_eq!(dequeue.len(), 0);
        OwnedThinPtr::drop_immediately(node_ptr);
    }
}
