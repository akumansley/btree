use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use marker::{LockState, NodeType};
use tagptr::TagNonNull;

use crate::debug_println;
use crate::internal_node::{InternalNode, InternalNodeInner};
use crate::leaf_node::{LeafNode, LeafNodeInner};
use crate::node::{Height, NodeHeader};
use crate::tree::{BTreeKey, BTreeValue, RootNode, RootNodeInner};

pub enum DiscriminatedNode<K: BTreeKey, V: BTreeValue, L: LockState> {
    Leaf(NodePtr<K, V, L, marker::Leaf>),
    Root(NodePtr<K, V, L, marker::Root>),
    Internal(NodePtr<K, V, L, marker::Internal>),
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Debug for NodePtr<K, V, L, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ node: {:?} tag: {:?} }}",
            self.tagged_ptr.decompose_ptr(),
            self.tagged_ptr.decompose_tag()
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

pub struct NodePtr<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> {
    tagged_ptr: TagNonNull<NodeHeader, 2>,
    phantom: PhantomData<(K, V, L, N)>,
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Clone for NodePtr<K, V, L, N> {
    fn clone(&self) -> Self {
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Copy for NodePtr<K, V, L, N> {}

impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> PartialEq for NodePtr<K, V, L, N> {
    fn eq(&self, other: &Self) -> bool {
        self.tagged_ptr == other.tagged_ptr
    }
}

// this is the impl for all NodePtrs
impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> NodePtr<K, V, L, N> {
    fn header(&self) -> &NodeHeader {
        unsafe { self.tagged_ptr.as_ref() }
    }

    pub fn height(&self) -> Height {
        self.header().height()
    }

    pub fn is_unlocked(&self) -> bool {
        self.tagged_ptr.decompose_tag() == 0
    }
    pub fn is_exclusive(&self) -> bool {
        self.tagged_ptr.decompose_tag() == 1
    }
    pub fn is_shared(&self) -> bool {
        self.tagged_ptr.decompose_tag() == 2
    }

    pub fn is_internal(&self) -> bool {
        unsafe {
            let node_header = self.tagged_ptr.as_ref();
            let height = node_header.height();
            matches!(height, Height::Internal(_))
        }
    }
    pub fn is_leaf(&self) -> bool {
        unsafe {
            let node_header = self.tagged_ptr.as_ref();
            let height = node_header.height();
            matches!(height, Height::Leaf)
        }
    }

    pub fn is_root(&self) -> bool {
        unsafe {
            let node_header = self.tagged_ptr.as_ref();
            let height = node_header.height();
            matches!(height, Height::Root)
        }
    }

    pub fn erase(&self) -> NodePtr<K, V, marker::Unknown, marker::Unknown> {
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }

    pub fn erase_node_type(&self) -> NodePtr<K, V, L, marker::Unknown> {
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }

    pub fn erase_lock_state(&self) -> NodePtr<K, V, marker::Unknown, N> {
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }

    pub fn to_unlocked(&self) -> NodePtr<K, V, marker::Unlocked, N> {
        let raw_ptr = self.tagged_ptr.clear_tag();
        NodePtr {
            tagged_ptr: raw_ptr,
            phantom: PhantomData,
        }
    }

    pub fn to_stored(&self) -> NodePtr<K, V, marker::Unlocked, marker::Unknown> {
        let raw_ptr = self.tagged_ptr.clear_tag();
        NodePtr {
            tagged_ptr: raw_ptr,
            phantom: PhantomData,
        }
    }

    pub unsafe fn cast<L2: LockState, N2: NodeType>(&self) -> NodePtr<K, V, L2, N2> {
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodePtr<K, V, marker::Unknown, N> {
    pub fn assert_exclusive(&self) -> NodePtr<K, V, marker::Exclusive, N> {
        debug_assert!(self.is_exclusive());
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }
}

// Unknown type, any lock state

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodePtr<K, V, L, marker::Unknown> {
    pub fn force(&self) -> DiscriminatedNode<K, V, L> {
        debug_assert!(self.is_leaf() || self.is_internal() || self.is_root());
        let height = self.header().height();
        match height {
            Height::Leaf => DiscriminatedNode::Leaf(self.assert_leaf()),
            Height::Root => DiscriminatedNode::Root(self.assert_root()),
            Height::Internal(_) => DiscriminatedNode::Internal(self.assert_internal()),
        }
    }
    pub fn assert_leaf(&self) -> NodePtr<K, V, L, marker::Leaf> {
        debug_assert!(self.is_leaf());
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }

    pub fn assert_internal(&self) -> NodePtr<K, V, L, marker::Internal> {
        debug_assert!(self.is_internal());
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }

    pub fn assert_root(&self) -> NodePtr<K, V, L, marker::Root> {
        debug_assert!(self.is_root());
        NodePtr {
            tagged_ptr: self.tagged_ptr,
            phantom: PhantomData,
        }
    }
}
// Any node, unlocked

impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodePtr<K, V, marker::Unlocked, N> {
    pub fn lock_exclusive(&self) -> NodePtr<K, V, marker::Exclusive, N> {
        debug_println!("locking {:?} {:?} exclusive", self, self.header().height());
        self.header().lock_exclusive();
        debug_println!("locked {:?} {:?} exclusive", self, self.header().height());
        let locked_ptr = self.tagged_ptr.set_tag(1);
        NodePtr {
            tagged_ptr: locked_ptr,
            phantom: PhantomData,
        }
    }

    pub fn lock_shared(&self) -> NodePtr<K, V, marker::Shared, N> {
        debug_println!("locking {:?} {:?} shared", self, self.header().height());
        self.header().lock_shared();
        debug_println!("locked {:?} {:?} shared", self, self.header().height());
        let locked_ptr = self.tagged_ptr.set_tag(2);
        NodePtr {
            tagged_ptr: locked_ptr,
            phantom: PhantomData,
        }
    }
}

// Any node, exclusive
impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodePtr<K, V, marker::Exclusive, N> {
    pub fn unlock_exclusive(&self) -> NodePtr<K, V, marker::Unlocked, N> {
        debug_println!(
            "unlocking {:?} {:?} exclusive",
            self,
            self.header().height()
        );
        self.header().unlock_exclusive();
        debug_println!("unlocked {:?} {:?} exclusive", self, self.header().height());
        let unlocked_ptr = self.tagged_ptr.set_tag(0);
        NodePtr {
            tagged_ptr: unlocked_ptr,
            phantom: PhantomData,
        }
    }
}

// Any node, shared
impl<K: BTreeKey, V: BTreeValue, N: NodeType> NodePtr<K, V, marker::Shared, N> {
    pub fn unlock_shared(&self) -> NodePtr<K, V, marker::Unlocked, N> {
        debug_println!("unlocking {:?} {:?} shared", self, self.header().height());
        self.header().unlock_shared();
        debug_println!("unlocked {:?} {:?} shared", self, self.header().height());
        let unlocked_ptr = self.tagged_ptr.set_tag(0);
        NodePtr {
            tagged_ptr: unlocked_ptr,
            phantom: PhantomData,
        }
    }
}

// Get back raw node pointers
impl<K: BTreeKey, V: BTreeValue, L: LockState> NodePtr<K, V, L, marker::Leaf> {
    pub fn to_mut_leaf_ptr(&self) -> *mut LeafNode<K, V> {
        self.to_unlocked().tagged_ptr.decompose_ptr() as *mut LeafNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodePtr<K, V, L, marker::Root> {
    pub fn to_mut_root_ptr(&self) -> *mut RootNode<K, V> {
        self.to_unlocked().tagged_ptr.decompose_ptr() as *mut RootNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue, L: LockState> NodePtr<K, V, L, marker::Internal> {
    pub fn to_mut_internal_ptr(&self) -> *mut InternalNode<K, V> {
        self.to_unlocked().tagged_ptr.decompose_ptr() as *mut InternalNode<K, V>
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Shared, marker::Leaf> {
    type Target = LeafNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_leaf_ptr();

        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Shared, marker::Root> {
    type Target = RootNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_root_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Shared, marker::Internal> {
    type Target = InternalNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_internal_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for internal writes
impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Exclusive, marker::Internal> {
    type Target = InternalNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_internal_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}
impl<K: BTreeKey, V: BTreeValue> DerefMut for NodePtr<K, V, marker::Exclusive, marker::Internal> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_mut_internal_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for leaf writes

impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Exclusive, marker::Leaf> {
    type Target = LeafNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_leaf_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}
impl<K: BTreeKey, V: BTreeValue> DerefMut for NodePtr<K, V, marker::Exclusive, marker::Leaf> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_mut_leaf_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Deref and DerefMut for root writes

impl<K: BTreeKey, V: BTreeValue> Deref for NodePtr<K, V, marker::Exclusive, marker::Root> {
    type Target = RootNodeInner<K, V>;

    fn deref(&self) -> &Self::Target {
        let raw_ptr = self.to_mut_root_ptr();
        unsafe { &*(*raw_ptr).inner.get() }
    }
}

impl<K: BTreeKey, V: BTreeValue> DerefMut for NodePtr<K, V, marker::Exclusive, marker::Root> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let raw_ptr = self.to_mut_root_ptr();
        unsafe { &mut *(*raw_ptr).inner.get() }
    }
}

// Constructors

impl<K: BTreeKey, V: BTreeValue> NodePtr<K, V, marker::Unlocked, marker::Internal> {
    pub fn from_internal_unlocked(node: *mut InternalNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        let tagged_ptr = TagNonNull::compose(non_null_ptr, 0);
        NodePtr {
            tagged_ptr,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> NodePtr<K, V, marker::Unlocked, marker::Leaf> {
    pub fn from_leaf_unlocked(node: *mut LeafNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        let tagged_ptr = TagNonNull::compose(non_null_ptr, 0);
        NodePtr {
            tagged_ptr,
            phantom: PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> NodePtr<K, V, marker::Unlocked, marker::Root> {
    pub fn from_root_unlocked(node: *mut RootNode<K, V>) -> Self {
        let non_null_ptr: NonNull<NodeHeader> =
            NonNull::new(node as *const _ as *mut NodeHeader).unwrap();
        let tagged_ptr = TagNonNull::compose(non_null_ptr, 0);
        NodePtr {
            tagged_ptr,
            phantom: PhantomData,
        }
    }
}
