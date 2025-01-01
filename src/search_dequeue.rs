use crate::pointer_types::{NodePtr, NodeRef};
use std::{
    fmt::{Debug, Display},
    mem::MaybeUninit,
};

const MAX_STACK_SIZE: usize = 16;
pub struct SearchDequeue<K, V> {
    stack: [MaybeUninit<NodePtr<K, V>>; MAX_STACK_SIZE],
    highest_height: usize,
    index_of_highest_node: usize,
    index_after_lowest_node: usize,
}

impl<K: PartialOrd + Debug + Clone, V: Debug + Display> SearchDequeue<K, V> {
    pub fn new(height: usize) -> Self {
        SearchDequeue {
            stack: [MaybeUninit::uninit(); MAX_STACK_SIZE],
            highest_height: height,
            index_of_highest_node: 0,
            index_after_lowest_node: 0,
        }
    }

    pub fn pop_highest(&mut self) -> Option<NodeRef<K, V>> {
        debug_assert!(self.index_of_highest_node < self.index_after_lowest_node);
        self.index_of_highest_node -= 1;
        unsafe {
            let node_ptr = self.stack[self.index_of_highest_node].assume_init();
            let node_ref = NodeRef::new(node_ptr, self.highest_height - self.index_of_highest_node);
            Some(node_ref)
        }
    }

    pub fn push_node_on_bottom(&mut self, value: NodePtr<K, V>) {
        debug_assert!(self.index_after_lowest_node < MAX_STACK_SIZE);
        self.stack[self.index_after_lowest_node].write(value);
        self.index_after_lowest_node += 1;
    }

    pub fn pop_lowest(&mut self) -> NodeRef<K, V> {
        debug_assert!(self.index_after_lowest_node > 0);
        unsafe {
            let node_ptr = self.stack[self.index_after_lowest_node - 1].assume_init();
            let node_ref =
                NodeRef::new(node_ptr, self.highest_height - self.index_after_lowest_node);
            self.index_after_lowest_node -= 1;
            node_ref
        }
    }

    pub fn peek_lowest(&self) -> NodeRef<K, V> {
        unsafe {
            let node_ptr = self.stack[self.index_after_lowest_node - 1].assume_init();
            NodeRef::new(node_ptr, self.highest_height - self.index_after_lowest_node)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.index_after_lowest_node == 0
    }

    pub fn len(&self) -> usize {
        self.index_after_lowest_node
    }
}
