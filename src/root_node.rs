use std::marker::PhantomData;

use crate::leaf_node::LeafNode;
use crate::node::{Height, NodeHeader};
use crate::pointers::node_ref::{marker, OwnedNodeRef, SharedDiscriminatedNode, SharedNodeRef};
use crate::pointers::OwnedThinAtomicPtr;
use crate::sync::{AtomicUsize, Ordering};
use crate::tree::BTreeValue;
use crate::{BTreeKey, SharedThinPtr};
use std::cell::UnsafeCell;

#[repr(C)]
pub struct RootNode<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub header: NodeHeader,
    pub inner: UnsafeCell<RootNodeInner<K, V>>,
    // this is updated atomically after insert/remove, so it's not perfectly consistent
    // but that lets us avoid some extra locking -- otherwise we'd need to hold a lock during any
    // insert/remove for the duration of the operation
    pub len: AtomicUsize,
}

pub struct RootNodeInner<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub top_of_tree: OwnedThinAtomicPtr<NodeHeader>,
    phantom: PhantomData<(*const K, *const V)>,
}

unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Send for RootNode<K, V> {}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for RootNodeInner<K, V> {
    fn drop(&mut self) {
        let top_of_tree_ref =
            OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(unsafe {
                self.top_of_tree.load_owned(Ordering::Acquire).unwrap()
            });
        OwnedNodeRef::drop_immediately(top_of_tree_ref);
    }
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> RootNode<K, V> {
    pub fn new() -> Self {
        RootNode {
            header: NodeHeader::new(Height::Root),
            inner: UnsafeCell::new(RootNodeInner {
                top_of_tree: OwnedThinAtomicPtr::new_with(|| {
                    Box::into_raw(Box::new(LeafNode::<K, V>::new())) as *mut ()
                }),
                phantom: PhantomData,
            }),
            len: AtomicUsize::new(0),
        }
    }

    pub fn as_node_ref(&self) -> SharedNodeRef<K, V, marker::Unlocked, marker::Root> {
        let root_ptr = unsafe { SharedThinPtr::from_ptr(self as *const _ as *mut ()) };
        SharedNodeRef::from_root_ptr(root_ptr).assume_unlocked()
    }

    pub fn print_tree(&self) {
        let root = self.as_node_ref().lock_shared();
        println!("BTree:");
        println!("+----------------------+");
        println!("| Tree length: {}      |", self.len.load(Ordering::Relaxed));
        println!("+----------------------+");
        match SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree.load_shared(Ordering::Relaxed).unwrap(),
        )
        .assume_unlocked()
        .force()
        {
            SharedDiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.print_node();
                internal.unlock_shared();
            }
            SharedDiscriminatedNode::Leaf(leaf) => {
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
        match SharedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
            root.top_of_tree.load_shared(Ordering::Relaxed).unwrap(),
        )
        .assume_unlocked()
        .force()
        {
            SharedDiscriminatedNode::Internal(internal) => {
                let internal = internal.lock_shared();
                internal.check_invariants();
                internal.unlock_shared();
            }
            SharedDiscriminatedNode::Leaf(leaf) => {
                let leaf = leaf.lock_shared();
                leaf.check_invariants();
                leaf.unlock_shared();
            }
            _ => unreachable!(),
        }
        root.unlock_shared();
    }
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> RootNodeInner<K, V> {
    pub fn top_of_tree(&self) -> SharedThinPtr<NodeHeader> {
        self.top_of_tree.load_shared(Ordering::Relaxed).unwrap()
    }
}
