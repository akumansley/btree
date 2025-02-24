use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::{Height, NodeHeader};
use crate::node_ptr::{marker, DiscriminatedNode, NodePtr, NodeRef};
use crate::tree::BTreeValue;
use crate::BTreeKey;

#[repr(C)]
pub struct RootNode<K: BTreeKey, V: BTreeValue> {
    pub header: NodeHeader,
    pub inner: UnsafeCell<RootNodeInner<K, V>>,
    // this is updated atomically after insert/remove, so it's not perfectly consistent
    // but that lets us avoid some extra locking -- otherwise we'd need to hold a lock during any
    // insert/remove for the duration of the operation
    pub len: AtomicUsize,
}

pub struct RootNodeInner<K: BTreeKey, V: BTreeValue> {
    pub top_of_tree: AtomicPtr<NodeHeader>,
    phantom: PhantomData<(K, V)>,
}

impl<K: BTreeKey, V: BTreeValue> Drop for RootNodeInner<K, V> {
    fn drop(&mut self) {
        let top_of_tree_ref =
            NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
                NodePtr::from_raw_ptr(self.top_of_tree.load(Ordering::Relaxed)),
            );
        match top_of_tree_ref.force() {
            DiscriminatedNode::Leaf(leaf) => unsafe {
                drop(Box::from_raw(leaf.to_raw_leaf_ptr()));
            },
            DiscriminatedNode::Internal(internal) => {
                let internal_node_ptr = internal.to_raw_internal_ptr();
                unsafe { InternalNode::drop_node_recursively(internal_node_ptr) };
            }
            _ => panic!("RootNodeInner::drop: top_of_tree is not a leaf or internal node"),
        }
    }
}
impl<K: BTreeKey, V: BTreeValue> RootNode<K, V> {
    pub fn new() -> Self {
        let top_of_tree = LeafNode::<K, V>::new();
        RootNode {
            header: NodeHeader::new(Height::Root),
            inner: UnsafeCell::new(RootNodeInner {
                top_of_tree: AtomicPtr::new(top_of_tree as *mut NodeHeader),
                phantom: PhantomData,
            }),
            len: AtomicUsize::new(0),
        }
    }

    pub fn as_node_ref(&self) -> NodeRef<K, V, marker::Unlocked, marker::Root> {
        NodeRef::from_root_unlocked(self as *const _ as *mut _)
    }

    pub fn print_tree(&self) {
        let root = self.as_node_ref().lock_shared();
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree length: {}      |", self.len.load(Ordering::Relaxed));
        println!("+----------------------+");
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            NodePtr::from_raw_ptr(root.top_of_tree.load(Ordering::Relaxed)),
        )
        .force()
        {
            DiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.print_node();
                internal.unlock_shared();
            }
            DiscriminatedNode::Leaf(leaf) => {
                let leaf = leaf.lock_shared();
                leaf.print_node();
                leaf.unlock_shared();
            }
            _ => unreachable!(),
        }
        root.unlock_shared();
    }

    pub fn check_invariants(&self) {
        debug_println!("checking invariants");
        let root = self.as_node_ref().lock_shared();
        match NodeRef::<K, V, marker::Unlocked, marker::Unknown>::from_unknown_node_ptr(
            NodePtr::from_raw_ptr(root.top_of_tree.load(Ordering::Relaxed)),
        )
        .force()
        {
            DiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.check_invariants();
                internal.unlock_shared();
            }
            DiscriminatedNode::Leaf(leaf) => {
                let leaf = leaf.lock_shared();
                leaf.check_invariants();
                leaf.unlock_shared();
            }
            _ => unreachable!(),
        }
        root.unlock_shared();
    }
}
impl<K: BTreeKey, V: BTreeValue> RootNodeInner<K, V> {
    pub fn top_of_tree(&self) -> NodePtr {
        NodePtr::from_raw_ptr(self.top_of_tree.load(Ordering::Relaxed))
    }
}
