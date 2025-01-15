use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::debug_println;
use crate::hybrid_latch::LockInfo;
use crate::internal_node::{InternalNode, InternalNodeInner};
use crate::leaf_node::{LeafNode, LeafNodeInner};
use crate::node::{Height, NodeHeader};
use crate::tree::{BTreeKey, BTreeValue, RootNode, RootNodeInner};
use marker::{LockState, NodeType};

pub enum DiscriminatedNode<K: BTreeKey, V: BTreeValue, L: LockState> {
    Leaf(NodeRef<K, V, L, marker::Leaf>),
    Root(NodeRef<K, V, L, marker::Root>),
    Internal(NodeRef<K, V, L, marker::Internal>),
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Debug for NodeRef<K, V, L, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ node: {:?} tag: {:?} }}",
            self.node.0, self.lock_info.0,
        )
    }
}

pub mod marker {
    pub trait LockState {}
    pub struct Unlocked;
    impl LockState for Unlocked {}
    pub struct Exclusive;
    impl LockState for Exclusive {}
    pub struct Shared;
    impl LockState for Shared {}
    pub trait NodeType {}
    pub struct Internal;
    impl NodeType for Internal {}
    pub struct Leaf;
    impl NodeType for Leaf {}
    pub struct Root;
    impl NodeType for Root {}
    pub struct Unknown;
    impl NodeType for Unknown {}
    impl LockState for Unknown {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePtr(pub NonNull<NodeHeader>);

impl AsRef<NodeHeader> for NodePtr {
    fn as_ref(&self) -> &NodeHeader {
        unsafe { self.0.as_ref() }
    }
}

pub struct NodeRef<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> {
    node: NodePtr,
    lock_info: LockInfo,
    phantom: PhantomData<(K, V, L, N)>,
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Clone for NodeRef<K, V, L, N> {
    fn clone(&self) -> Self {
        NodeRef {
            node: self.node,
            lock_info: self.lock_info,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Copy for NodeRef<K, V, L, N> {}

// this is the impl for all NodePtrs
impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> NodeRef<K, V, L, N> {
    fn header(&self) -> &NodeHeader {
        self.node.as_ref()
    }

    pub fn height(&self) -> Height {
        self.header().height()
    }

    pub fn is_unlocked(&self) -> bool {
        self.lock_info.is_unlocked()
    }
    pub fn is_exclusive(&self) -> bool {
        self.lock_info.is_exclusive()
    }
    pub fn is_shared(&self) -> bool {
        self.lock_info.is_shared()
    }

    pub fn is_internal(&self) -> bool {
        matches!(self.header().height(), Height::Internal(_))
    }
    pub fn is_leaf(&self) -> bool {
        matches!(self.header().height(), Height::Leaf)
    }

    pub fn is_root(&self) -> bool {
        matches!(self.header().height(), Height::Root)
    }

    pub fn node_ptr(&self) -> NodePtr {
        self.node
    }

    pub fn erase(&self) -> NodeRef<K, V, marker::Unknown, marker::Unknown> {
        unsafe { self.cast::<marker::Unknown, marker::Unknown>() }
    }

    pub fn erase_node_type(&self) -> NodeRef<K, V, L, marker::Unknown> {
        unsafe { self.cast::<L, marker::Unknown>() }
    }

    pub fn erase_lock_state(&self) -> NodeRef<K, V, marker::Unknown, N> {
        unsafe { self.cast::<marker::Unknown, N>() }
    }

    pub unsafe fn cast<L2: LockState, N2: NodeType>(&self) -> NodeRef<K, V, L2, N2> {
        NodeRef {
            node: self.node,
            lock_info: self.lock_info,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodeRef<K, V, marker::Unknown, N> {
    pub fn assert_exclusive(&self) -> NodeRef<K, V, marker::Exclusive, N> {
        debug_assert!(self.is_exclusive());
        unsafe { self.cast::<marker::Exclusive, N>() }
    }
}

// Unknown type, any lock state

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodeRef<K, V, L, marker::Unknown> {
    pub fn force(&self) -> DiscriminatedNode<K, V, L> {
        debug_assert!(self.is_leaf() || self.is_internal() || self.is_root());
        let height = self.header().height();
        match height {
            Height::Leaf => DiscriminatedNode::Leaf(self.assert_leaf()),
            Height::Root => DiscriminatedNode::Root(self.assert_root()),
            Height::Internal(_) => DiscriminatedNode::Internal(self.assert_internal()),
        }
    }
    pub fn assert_leaf(&self) -> NodeRef<K, V, L, marker::Leaf> {
        debug_assert!(self.is_leaf());
        unsafe { self.cast::<L, marker::Leaf>() }
    }

    pub fn assert_internal(&self) -> NodeRef<K, V, L, marker::Internal> {
        debug_assert!(self.is_internal());
        unsafe { self.cast::<L, marker::Internal>() }
    }

    pub fn assert_root(&self) -> NodeRef<K, V, L, marker::Root> {
        debug_assert!(self.is_root());
        unsafe { self.cast::<L, marker::Root>() }
    }
}

// Any node, unlocked

impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodeRef<K, V, marker::Unlocked, N> {
    pub fn lock_exclusive(self) -> NodeRef<K, V, marker::Exclusive, N> {
        debug_println!("locking {:?} {:?} exclusive", self, self.header().height());
        self.header().lock_exclusive();
        debug_println!("locked {:?} {:?} exclusive", self, self.header().height());
        NodeRef {
            node: self.node,
            lock_info: LockInfo::exclusive(),
            phantom: PhantomData,
        }
    }

    pub fn lock_shared(self) -> NodeRef<K, V, marker::Shared, N> {
        debug_println!("locking {:?} {:?} shared", self, self.header().height());
        self.header().lock_shared();
        debug_println!("locked {:?} {:?} shared", self, self.header().height());
        NodeRef {
            node: self.node,
            lock_info: LockInfo::shared(),
            phantom: PhantomData,
        }
    }
}

// Any node, exclusive
impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodeRef<K, V, marker::Exclusive, N> {
    pub fn unlock_exclusive(self) -> NodeRef<K, V, marker::Unlocked, N> {
        debug_println!(
            "unlocking {:?} {:?} exclusive",
            self,
            self.header().height()
        );
        self.header().unlock_exclusive();
        debug_println!("unlocked {:?} {:?} exclusive", self, self.header().height());
        NodeRef {
            node: self.node,
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}

// Any node, shared
impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodeRef<K, V, marker::Shared, N> {
    pub fn unlock_shared(self) -> NodeRef<K, V, marker::Unlocked, N> {
        debug_println!("unlocking {:?} {:?} shared", self, self.header().height());
        self.header().unlock_shared();
        debug_println!("unlocked {:?} {:?} shared", self, self.header().height());
        NodeRef {
            node: self.node,
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}

// Get back raw node pointers
impl<K: BTreeKey, V: BTreeValue, L: LockState> NodeRef<K, V, L, marker::Leaf> {
    pub fn to_raw_leaf_ptr(&self) -> *mut LeafNode<K, V> {
        self.node.0.as_ptr() as *mut LeafNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodeRef<K, V, L, marker::Root> {
    pub fn to_raw_root_ptr(&self) -> *mut RootNode<K, V> {
        self.node.0.as_ptr() as *mut RootNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodeRef<K, V, L, marker::Internal> {
    pub fn to_raw_internal_ptr(&self) -> *mut InternalNode<K, V> {
        self.node.0.as_ptr() as *mut InternalNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Shared, marker::Leaf> {
    type Target = LeafNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_leaf_ptr();

        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Shared, marker::Root> {
    type Target = RootNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_root_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Shared, marker::Internal> {
    type Target = InternalNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_internal_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for internal writes
impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Exclusive, marker::Internal> {
    type Target = InternalNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_internal_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}
impl<K: BTreeKey, V: BTreeValue> DerefMut for NodeRef<K, V, marker::Exclusive, marker::Internal> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_raw_internal_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for leaf writes

impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Exclusive, marker::Leaf> {
    type Target = LeafNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_leaf_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> DerefMut for NodeRef<K, V, marker::Exclusive, marker::Leaf> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_raw_leaf_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for root writes

impl<K: BTreeKey, V: BTreeValue> Deref for NodeRef<K, V, marker::Exclusive, marker::Root> {
    type Target = RootNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_raw_root_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> DerefMut for NodeRef<K, V, marker::Exclusive, marker::Root> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_raw_root_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Constructors

impl<K: BTreeKey, V: BTreeValue> NodeRef<K, V, marker::Unlocked, marker::Unknown> {
    pub fn from_unknown_node_ptr(node: NodePtr) -> Self {
        NodeRef {
            node,
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> NodeRef<K, V, marker::Unlocked, marker::Internal> {
    pub fn from_internal_unlocked(node: *mut InternalNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        NodeRef {
            node: NodePtr(non_null_ptr),
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> NodeRef<K, V, marker::Unlocked, marker::Leaf> {
    pub fn from_leaf_unlocked(node: *mut LeafNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        NodeRef {
            node: NodePtr(non_null_ptr),
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> NodeRef<K, V, marker::Unlocked, marker::Root> {
    pub fn from_root_unlocked(node: *mut RootNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        NodeRef {
            node: NodePtr(non_null_ptr),
            lock_info: LockInfo::unlocked(),
            phantom: PhantomData,
        }
    }
}
