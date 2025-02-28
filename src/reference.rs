use std::fmt::{Debug, Display};
use std::ops::Deref;

use crate::graceful_pointers::GracefulArc;
use crate::tree::{BTreeKey, BTreeValue};

pub struct Ref<V: BTreeValue> {
    value: *const V,
    _phantom: std::marker::PhantomData<V>,
}

impl<V: BTreeValue> Ref<V> {
    pub fn new(value: *const V) -> Self {
        Ref {
            value,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V: BTreeValue> Deref for Ref<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
    }
}

impl<V: BTreeValue> Debug for Ref<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<V: BTreeValue + Display> Display for Ref<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<V: BTreeValue> PartialEq for Ref<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue> PartialEq<&V> for Ref<V>
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

impl<V: BTreeValue + Display> Debug for ValueRef<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<V: BTreeValue + Display> Display for ValueRef<V> {
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
