use std::fmt::{Debug, Display};
use std::ops::Deref;

use crate::graceful_pointers::GracefulArc;
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
pub struct Entry<K: BTreeKey, V: BTreeValue> {
    key: GracefulArc<K>,
    value: *const V,
}

impl<K: BTreeKey, V: BTreeValue> Entry<K, V> {
    pub fn new(key: GracefulArc<K>, value: *const V) -> Self {
        Entry { key, value }
    }
}

impl<K: BTreeKey, V: BTreeValue> Entry<K, V> {
    pub fn key(&self) -> &K {
        self.key.as_ref()
    }
    pub fn value(&self) -> &V {
        unsafe { &*self.value }
    }
    pub fn into_value(self) -> ValueRef<V> {
        ValueRef::new(self.value)
    }
}

pub struct ValueRef<V: BTreeValue> {
    value: *const V,
}

impl<V: BTreeValue> ValueRef<V> {
    pub fn new(value: *const V) -> Self {
        ValueRef { value }
    }
}

impl<V: BTreeValue> Deref for ValueRef<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
    }
}

impl<V: BTreeValue> Debug for ValueRef<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<V: BTreeValue> Display for ValueRef<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<V: BTreeValue> PartialEq for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue> PartialEq<&V> for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue> PartialEq<V> for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &V) -> bool {
        **self == *other
    }
}
