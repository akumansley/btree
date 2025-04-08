use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::ops::{Deref, DerefMut};

use crate::hybrid_latch::{LockError, LockInfo};
use crate::internal_node::{InternalNode, InternalNodeInner};
use crate::leaf_node::{LeafNode, LeafNodeInner};
use crate::node::{Height, NodeHeader};
use crate::root_node::{RootNode, RootNodeInner};
use crate::tree::{BTreeKey, BTreeValue};
use marker::{LockState, NodeType};

use super::atomic::{OwnedThinPtr, SharedThinPtr};

pub enum DescriminatedNode {
    Leaf,
    Root,
    Internal,
}
pub enum SharedDiscriminatedNode<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState> {
    Leaf(SharedNodeRef<K, V, L, marker::Leaf>),
    Root(SharedNodeRef<K, V, L, marker::Root>),
    Internal(SharedNodeRef<K, V, L, marker::Internal>),
}

macro_rules! impl_node_ref_traits {
    ($struct_name:ident, $discriminated_node:ident, $thin_ptr_type:ident) => {
        /// Shared trait impls for Owned and Shared NodeRefs:
        /// - Debug
        /// - PartialEq
        impl<K: BTreeKey, V: BTreeValue, L: LockState, N: NodeType> Debug
            for $struct_name<K, V, L, N>
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{{ node: {:?} lock_info: {:} }}",
                    self.node, self.lock_info,
                )
            }
        }
        impl<
                K: BTreeKey + ?Sized,
                V: BTreeValue + ?Sized,
                L1: LockState,
                L2: LockState,
                N1: NodeType,
                N2: NodeType,
            > PartialEq<$struct_name<K, V, L2, N2>> for $struct_name<K, V, L1, N1>
        {
            fn eq(&self, other: &$struct_name<K, V, L2, N2>) -> bool {
                self.node == other.node
            }
        }

        /// Shared impl for Owned and Shared NodeRefs
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType>
            $struct_name<K, V, L, N>
        {
            #[allow(unused)]
            pub fn erase(self) -> $struct_name<K, V, marker::Unknown, marker::Unknown> {
                unsafe { self.cast::<marker::Unknown, marker::Unknown>() }
            }

            #[allow(unused)]
            pub fn erase_node_type(self) -> $struct_name<K, V, L, marker::Unknown> {
                unsafe { self.cast::<L, marker::Unknown>() }
            }

            unsafe fn cast<L2: LockState, N2: NodeType>(self) -> $struct_name<K, V, L2, N2> {
                let lock_info = self.lock_info;
                let ptr = self.into_ptr();
                $struct_name {
                    node: ManuallyDrop::new(ptr),
                    lock_info,
                    phantom: PhantomData,
                }
            }
        }

        // Assert lock state for Unknown lock state
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType>
            $struct_name<K, V, marker::Unknown, N>
        {
            #[allow(unused)]
            pub fn assert_exclusive(mut self) -> $struct_name<K, V, marker::LockedExclusive, N> {
                debug_assert!(self.header().is_locked_exclusive());
                self.lock_info = LockInfo::exclusive();
                unsafe { self.cast::<marker::LockedExclusive, N>() }
            }
            #[allow(unused)]
            pub fn assume_unlocked(mut self) -> $struct_name<K, V, marker::Unlocked, N> {
                self.lock_info = LockInfo::unlocked();
                unsafe { self.cast::<marker::Unlocked, N>() }
            }
            #[allow(unused)]
            pub fn assert_shared(mut self) -> $struct_name<K, V, marker::LockedShared, N> {
                debug_assert!(self.header().is_locked_shared());
                self.lock_info = LockInfo::shared();
                unsafe { self.cast::<marker::LockedShared, N>() }
            }
        }

        // this is the impl for all NodePtrs
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType>
            $struct_name<K, V, L, N>
        {
            pub fn header(&self) -> &NodeHeader {
                self.node.deref()
            }
            pub fn lock_info(&self) -> LockInfo {
                self.lock_info
            }

            #[allow(unused)]
            pub fn height(&self) -> Height {
                self.header().height()
            }
            #[allow(unused)]
            pub fn is_unlocked(&self) -> bool {
                self.lock_info().is_unlocked()
            }
            #[allow(unused)]
            pub fn is_exclusive(&self) -> bool {
                self.lock_info().is_exclusive()
            }
            #[allow(unused)]
            pub fn is_shared(&self) -> bool {
                self.lock_info().is_shared()
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
            pub fn node_type(&self) -> DescriminatedNode {
                match self.header().height() {
                    Height::Leaf => DescriminatedNode::Leaf,
                    Height::Root => DescriminatedNode::Root,
                    Height::Internal(_) => DescriminatedNode::Internal,
                }
            }
        }

        // Unknown type, any lock state
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState>
            $struct_name<K, V, L, marker::Unknown>
        {
            pub fn assert_leaf(self) -> $struct_name<K, V, L, marker::Leaf> {
                debug_assert!(self.is_leaf());
                unsafe { self.cast::<L, marker::Leaf>() }
            }

            pub fn assert_internal(self) -> $struct_name<K, V, L, marker::Internal> {
                debug_assert!(self.is_internal());
                unsafe { self.cast::<L, marker::Internal>() }
            }

            #[allow(unused)]
            pub fn assert_root(self) -> $struct_name<K, V, L, marker::Root> {
                debug_assert!(self.is_root());
                unsafe { self.cast::<L, marker::Root>() }
            }
        }

        // Any node, unlocked

        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType>
            $struct_name<K, V, marker::Unlocked, N>
        {
            pub fn lock_exclusive(self) -> $struct_name<K, V, marker::LockedExclusive, N> {
                debug_println!("locking {:?} {:?} exclusive", self, self.header().height());
                self.header().lock_exclusive();
                debug_println!("locked {:?} {:?} exclusive", self, self.header().height());
                $struct_name {
                    node: ManuallyDrop::new(self.into_ptr()),
                    lock_info: LockInfo::exclusive(),
                    phantom: PhantomData,
                }
            }

            #[allow(unused)]
            pub fn lock_exclusive_if_not_retired(
                self,
            ) -> Result<$struct_name<K, V, marker::LockedExclusive, N>, LockError> {
                self.header().lock_exclusive_if_not_retired()?;
                Ok($struct_name {
                    node: ManuallyDrop::new(self.into_ptr()),
                    lock_info: LockInfo::exclusive(),
                    phantom: PhantomData,
                })
            }

            #[allow(unused)]
            pub fn lock_optimistic(self) -> Result<$struct_name<K, V, marker::Optimistic, N>, ()> {
                debug_println!("locking {:?} {:?} optimistic", self, self.header().height());
                let lock_info = self.header().lock_optimistic()?;
                debug_println!("locked {:?} {:?} optimistic", self, self.header().height());
                Ok($struct_name {
                    node: ManuallyDrop::new(self.into_ptr()),
                    lock_info,
                    phantom: PhantomData,
                })
            }

            #[allow(unused)]
            pub fn lock_shared(self) -> $struct_name<K, V, marker::LockedShared, N> {
                debug_println!("locking {:?} {:?} shared", self, self.header().height());
                self.header().lock_shared();
                debug_println!("locked {:?} {:?} shared", self, self.header().height());
                $struct_name {
                    node: ManuallyDrop::new(self.into_ptr()),
                    lock_info: LockInfo::shared(),
                    phantom: PhantomData,
                }
            }

            #[allow(unused)]
            pub fn lock_shared_if_not_retired(
                self,
            ) -> Result<$struct_name<K, V, marker::LockedShared, N>, LockError> {
                self.header().lock_shared_if_not_retired()?;
                Ok($struct_name {
                    node: ManuallyDrop::new(self.into_ptr()),
                    lock_info: LockInfo::shared(),
                    phantom: PhantomData,
                })
            }

            #[allow(unused)]
            pub fn try_lock_shared(
                mut self,
            ) -> Result<
                $struct_name<K, V, marker::LockedShared, N>,
                $struct_name<K, V, marker::Unlocked, N>,
            > {
                match self.header().try_lock_shared() {
                    Ok(_) => {
                        self.lock_info = LockInfo::shared();
                        Ok(unsafe { self.cast::<marker::LockedShared, N>() })
                    }
                    Err(_) => Err(self),
                }
            }

            #[allow(unused)]
            pub fn try_lock_exclusive(
                mut self,
            ) -> Result<
                $struct_name<K, V, marker::LockedExclusive, N>,
                $struct_name<K, V, marker::Unlocked, N>,
            > {
                match self.header().try_lock_exclusive() {
                    Ok(_) => {
                        self.lock_info = LockInfo::exclusive();
                        Ok(unsafe { self.cast::<marker::LockedExclusive, N>() })
                    }
                    Err(_) => Err(self),
                }
            }
        }

        // Any node, exclusive
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType>
            $struct_name<K, V, marker::LockedExclusive, N>
        {
            #[allow(unused)]
            pub fn unlock_exclusive(mut self) -> $struct_name<K, V, marker::Unlocked, N> {
                debug_println!(
                    "unlocking {:?} {:?} exclusive",
                    self,
                    self.header().height()
                );
                self.header().unlock_exclusive();
                debug_println!("unlocked {:?} {:?} exclusive", self, self.header().height());
                self.lock_info = LockInfo::unlocked();
                unsafe { self.cast::<marker::Unlocked, N>() }
            }
            #[allow(unused)]
            pub fn retire(&self) {
                self.header().retire();
            }
        }

        // Any node, shared
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType>
            $struct_name<K, V, marker::LockedShared, N>
        {
            #[allow(unused)]
            pub fn unlock_shared(mut self) -> $struct_name<K, V, marker::Unlocked, N> {
                debug_println!("unlocking {:?} {:?} shared", self, self.header().height());
                self.header().unlock_shared();
                debug_println!("unlocked {:?} {:?} shared", self, self.header().height());
                self.lock_info = LockInfo::unlocked();
                unsafe { self.cast::<marker::Unlocked, N>() }
            }
        }

        // Any node, optimistic
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType>
            $struct_name<K, V, marker::Optimistic, N>
        {
            #[allow(unused)]
            pub fn unlock_optimistic(
                mut self,
            ) -> Result<$struct_name<K, V, marker::Unlocked, N>, ()> {
                self.header().validate_optimistic(self.lock_info).map(|_| {
                    self.lock_info = LockInfo::unlocked();
                    unsafe { self.cast::<marker::Unlocked, N>() }
                })
            }

            #[allow(unused)]
            pub fn validate_lock(&self) -> Result<(), ()> {
                self.header().validate_optimistic(self.lock_info)
            }
        }
        unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType> Send
            for $struct_name<K, V, marker::LockedExclusive, N>
        {
        }
        unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, N: NodeType> Sync
            for $struct_name<K, V, marker::LockedExclusive, N>
        {
        }
    };
}

pub mod marker {
    pub trait LockState {}
    pub struct Unlocked;
    impl LockState for Unlocked {}
    pub struct LockedExclusive;
    impl LockState for LockedExclusive {}
    pub struct LockedShared;
    impl LockState for LockedShared {}
    pub struct Optimistic;
    impl LockState for Optimistic {}
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

pub struct OwnedNodeRef<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType> {
    node: ManuallyDrop<OwnedThinPtr<NodeHeader>>,
    lock_info: LockInfo,
    phantom: PhantomData<(*const K, *const V, L, N)>,
}
pub struct SharedNodeRef<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType> {
    node: ManuallyDrop<SharedThinPtr<NodeHeader>>,
    lock_info: LockInfo,
    phantom: PhantomData<(*const K, *const V, L, N)>,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType>
    OwnedNodeRef<K, V, L, N>
{
    pub fn into_ptr(mut self) -> OwnedThinPtr<NodeHeader> {
        let ptr = unsafe { ManuallyDrop::take(&mut self.node) };
        mem::forget(self);
        ptr
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType>
    SharedNodeRef<K, V, L, N>
{
    pub fn into_ptr(mut self) -> SharedThinPtr<NodeHeader> {
        unsafe { ManuallyDrop::take(&mut self.node) }
    }
}

impl_node_ref_traits!(OwnedNodeRef, OwnedDiscriminatedNode, OwnedThinPtr);
impl_node_ref_traits!(SharedNodeRef, SharedDiscriminatedNode, SharedThinPtr);

macro_rules! impl_deref_for_node_ref {
    ($node_type:ident, $inner_type:ident, $to_shared_fn:ident, $node_ref_type:ident, $lock_state:path) => {
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Deref
            for $node_ref_type<K, V, $lock_state, marker::$node_type>
        {
            type Target = $inner_type<K, V>;

            fn deref(&self) -> &Self::Target {
                let raw_ptr = self.$to_shared_fn();
                unsafe { &*raw_ptr.inner.get() }
            }
        }
    };
}

macro_rules! impl_deref_mut_for_node_ref {
    ($node_type:ident, $inner_type:ident, $to_shared_fn:ident, $node_ref_type:ident) => {
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> DerefMut
            for $node_ref_type<K, V, marker::LockedExclusive, marker::$node_type>
        {
            fn deref_mut(&mut self) -> &mut Self::Target {
                let raw_ptr = self.$to_shared_fn();
                unsafe { &mut *raw_ptr.deref().inner.get() }
            }
        }
    };
}

macro_rules! impl_deref_for_node_ref_all_locks {
    ($node_type:ident, $inner_type:ident, $to_shared_fn:ident, $node_ref_type:ident) => {
        // Shared lock implementation
        impl_deref_for_node_ref!(
            $node_type,
            $inner_type,
            $to_shared_fn,
            $node_ref_type,
            marker::LockedShared
        );

        // Optimistic lock implementation
        impl_deref_for_node_ref!(
            $node_type,
            $inner_type,
            $to_shared_fn,
            $node_ref_type,
            marker::Optimistic
        );

        // Exclusive lock implementation
        impl_deref_for_node_ref!(
            $node_type,
            $inner_type,
            $to_shared_fn,
            $node_ref_type,
            marker::LockedExclusive
        );
    };
}

macro_rules! impl_all_derefs_for_node_ref {
    ($node_type:ident, $inner_type:ident, $to_shared_fn:ident, $node_ref_type:ident) => {
        // Implement Deref for all lock states
        impl_deref_for_node_ref_all_locks!($node_type, $inner_type, $to_shared_fn, $node_ref_type);

        // Implement DerefMut for exclusive lock
        impl_deref_mut_for_node_ref!($node_type, $inner_type, $to_shared_fn, $node_ref_type);
    };
}

// Replace all the individual Deref and DerefMut implementations with the combined macro
impl_all_derefs_for_node_ref!(Leaf, LeafNodeInner, to_shared_leaf_ptr, OwnedNodeRef);
impl_all_derefs_for_node_ref!(Root, RootNodeInner, to_shared_root_ptr, OwnedNodeRef);
impl_all_derefs_for_node_ref!(
    Internal,
    InternalNodeInner,
    to_shared_internal_ptr,
    OwnedNodeRef
);
impl_all_derefs_for_node_ref!(Leaf, LeafNodeInner, to_shared_leaf_ptr, SharedNodeRef);
impl_all_derefs_for_node_ref!(Root, RootNodeInner, to_shared_root_ptr, SharedNodeRef);
impl_all_derefs_for_node_ref!(
    Internal,
    InternalNodeInner,
    to_shared_internal_ptr,
    SharedNodeRef
);

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType> Clone
    for SharedNodeRef<K, V, L, N>
{
    fn clone(&self) -> Self {
        Self {
            node: self.node,
            lock_info: self.lock_info,
            phantom: self.phantom,
        }
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType> Copy
    for SharedNodeRef<K, V, L, N>
{
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState>
    SharedNodeRef<K, V, L, marker::Unknown>
{
    pub fn force(self) -> SharedDiscriminatedNode<K, V, L> {
        debug_assert!(self.is_leaf() || self.is_internal() || self.is_root());
        let height = self.header().height();
        match height {
            Height::Leaf => SharedDiscriminatedNode::Leaf(self.assert_leaf()),
            Height::Root => SharedDiscriminatedNode::Root(self.assert_root()),
            Height::Internal(_) => SharedDiscriminatedNode::Internal(self.assert_internal()),
        }
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType>
    OwnedNodeRef<K, V, L, N>
{
    pub fn share(&self) -> SharedNodeRef<K, V, L, N> {
        SharedNodeRef {
            node: ManuallyDrop::new(self.node.share()),
            lock_info: self.lock_info.clone(),
            phantom: PhantomData,
        }
    }

    pub fn drop_immediately(mut self) {
        match self.node_type() {
            DescriminatedNode::Leaf => unsafe {
                OwnedThinPtr::drop_immediately(
                    ManuallyDrop::take(&mut self.node).cast::<LeafNode<K, V>>(),
                );
            },
            DescriminatedNode::Root => unsafe {
                OwnedThinPtr::drop_immediately(
                    ManuallyDrop::take(&mut self.node).cast::<RootNode<K, V>>(),
                );
            },
            DescriminatedNode::Internal => unsafe {
                OwnedThinPtr::drop_immediately(
                    ManuallyDrop::take(&mut self.node).cast::<InternalNode<K, V>>(),
                );
            },
        }
        mem::forget(self);
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState, N: NodeType> Drop
    for OwnedNodeRef<K, V, L, N>
{
    fn drop(&mut self) {
        // these are OwnedThinPtrs, so they'll drop using QSBR
        match self.node_type() {
            DescriminatedNode::Leaf => unsafe {
                drop(ManuallyDrop::take(&mut self.node).cast::<LeafNode<K, V>>())
            },
            DescriminatedNode::Root => unsafe {
                drop(ManuallyDrop::take(&mut self.node).cast::<RootNode<K, V>>())
            },
            DescriminatedNode::Internal => unsafe {
                drop(ManuallyDrop::take(&mut self.node).cast::<InternalNode<K, V>>())
            },
        }
    }
}

// Get back raw node pointers
macro_rules! impl_node_ptr_conversions {
    ($node_type:ident, $node_marker:path, $shared_fn_name:ident, $into_fn_name:ident) => {
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState>
            OwnedNodeRef<K, V, L, $node_marker>
        {
            pub fn $shared_fn_name(&self) -> SharedThinPtr<$node_type<K, V>> {
                let shared = self.node.share();
                unsafe { shared.cast::<$node_type<K, V>>() }
            }

            #[allow(unused)]
            pub fn $into_fn_name(mut self) -> OwnedThinPtr<$node_type<K, V>> {
                let ptr = unsafe { ManuallyDrop::take(&mut self.node).cast::<$node_type<K, V>>() };
                mem::forget(self);
                ptr
            }
        }

        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, L: LockState>
            SharedNodeRef<K, V, L, $node_marker>
        {
            pub fn $shared_fn_name(&self) -> SharedThinPtr<$node_type<K, V>> {
                let shared = self.node.share();
                unsafe { shared.cast::<$node_type<K, V>>() }
            }
        }
    };
}

impl_node_ptr_conversions!(LeafNode, marker::Leaf, to_shared_leaf_ptr, into_leaf_ptr);
impl_node_ptr_conversions!(RootNode, marker::Root, to_shared_root_ptr, into_root_ptr);
impl_node_ptr_conversions!(
    InternalNode,
    marker::Internal,
    to_shared_internal_ptr,
    into_internal_ptr
);

// Constructors

macro_rules! impl_from_unknown_node_ptr {
    ($struct_name:ident, $method_name:ident, $node_marker:path, $input_type:ty) => {
        impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>
            $struct_name<K, V, marker::Unknown, $node_marker>
        {
            pub fn $method_name(node: $input_type) -> Self {
                $struct_name {
                    node: ManuallyDrop::new(unsafe { node.cast::<NodeHeader>() }),
                    lock_info: LockInfo::unlocked(),
                    phantom: PhantomData,
                }
            }
        }
    };
}

impl_from_unknown_node_ptr!(
    OwnedNodeRef,
    from_unknown_node_ptr,
    marker::Unknown,
    OwnedThinPtr<NodeHeader>
);

impl_from_unknown_node_ptr!(
    SharedNodeRef,
    from_unknown_node_ptr,
    marker::Unknown,
    SharedThinPtr<NodeHeader>
);

impl_from_unknown_node_ptr!(
    OwnedNodeRef,
    from_internal_ptr,
    marker::Internal,
    OwnedThinPtr<InternalNode<K, V>>
);
impl_from_unknown_node_ptr!(
    OwnedNodeRef,
    from_leaf_ptr,
    marker::Leaf,
    OwnedThinPtr<LeafNode<K, V>>
);

impl_from_unknown_node_ptr!(
    SharedNodeRef,
    from_internal_ptr,
    marker::Internal,
    SharedThinPtr<InternalNode<K, V>>
);
impl_from_unknown_node_ptr!(
    SharedNodeRef,
    from_leaf_ptr,
    marker::Leaf,
    SharedThinPtr<LeafNode<K, V>>
);
impl_from_unknown_node_ptr!(
    SharedNodeRef,
    from_root_ptr,
    marker::Root,
    SharedThinPtr<RootNode<K, V>>
);
