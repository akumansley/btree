use std::fmt::{Debug, Display};
use std::ops::Deref;

use crate::node_ptr::{marker, NodeRef};
use crate::tree::{BTreeKey, BTreeValue};

pub struct Ref<K: BTreeKey, V: BTreeValue> {
    _leaf: NodeRef<K, V, marker::Shared, marker::Leaf>,
    value: *const V,
    _phantom: std::marker::PhantomData<V>,
}

impl<K: BTreeKey, V: BTreeValue> Ref<K, V> {
    pub fn new(leaf: NodeRef<K, V, marker::Shared, marker::Leaf>, value: *const V) -> Self {
        Ref {
            _leaf: leaf,
            value,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> Drop for Ref<K, V> {
    fn drop(&mut self) {
        self._leaf.unlock_shared();
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for Ref<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
    }
}

impl<K: BTreeKey, V: BTreeValue> Debug for Ref<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<K: BTreeKey, V: BTreeValue> Display for Ref<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<K: BTreeKey, V: BTreeValue> PartialEq for Ref<K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<K: BTreeKey, V: BTreeValue> PartialEq<&V> for Ref<K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        **self == **other
    }
}

pub struct ExclusiveRef<K: BTreeKey, V: BTreeValue> {
    _leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    value: *const V,
    _phantom: std::marker::PhantomData<V>,
}

impl<K: BTreeKey, V: BTreeValue> ExclusiveRef<K, V> {
    pub fn new(leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>, value: *const V) -> Self {
        ExclusiveRef {
            _leaf: leaf,
            value,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> Drop for ExclusiveRef<K, V> {
    fn drop(&mut self) {
        self._leaf.unlock_exclusive();
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for ExclusiveRef<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
    }
}

impl<K: BTreeKey, V: BTreeValue> Debug for ExclusiveRef<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<K: BTreeKey, V: BTreeValue> Display for ExclusiveRef<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<K: BTreeKey, V: BTreeValue> PartialEq for ExclusiveRef<K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<K: BTreeKey, V: BTreeValue> PartialEq<&V> for ExclusiveRef<K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        **self == **other
    }
}
