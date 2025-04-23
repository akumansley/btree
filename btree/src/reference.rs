use std::fmt::{Debug, Display};
use std::ops::Deref;

use crate::pointers::{SharedThinArc, SharedThinPtr};
use crate::tree::{BTreeKey, BTreeValue};

pub struct Ref<V: BTreeValue + ?Sized> {
    value: SharedThinPtr<V>,
    _phantom: std::marker::PhantomData<V>,
}

impl<V: BTreeValue + ?Sized> Ref<V> {
    pub fn new(value: SharedThinPtr<V>) -> Self {
        Ref {
            value,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V: BTreeValue + ?Sized> Deref for Ref<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.value.deref()
    }
}

impl<V: BTreeValue + ?Sized> Debug for Ref<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<V: BTreeValue + Display + ?Sized> Display for Ref<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<V: BTreeValue + ?Sized> PartialEq for Ref<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue + ?Sized> PartialEq<&V> for Ref<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        **self == **other
    }
}
pub struct Entry<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    key: SharedThinArc<K>,
    value: SharedThinPtr<V>,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Entry<K, V> {
    pub fn new(key: SharedThinArc<K>, value: SharedThinPtr<V>) -> Self {
        Entry { key, value }
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Entry<K, V> {
    pub fn key(&self) -> &K {
        self.key.deref()
    }
    pub fn value(&self) -> &V {
        self.value.deref()
    }
    pub fn into_value(self) -> ValueRef<V> {
        ValueRef::new(self.value)
    }
    pub fn value_shared_ptr(&self) -> SharedThinPtr<V> {
        self.value
    }
}

pub struct ValueRef<V: BTreeValue + ?Sized> {
    value: SharedThinPtr<V>,
}

impl<V: BTreeValue + ?Sized> ValueRef<V> {
    pub fn new(value: SharedThinPtr<V>) -> Self {
        ValueRef { value }
    }
}

impl<V: BTreeValue + ?Sized> Deref for ValueRef<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.value.deref()
    }
}

impl<V: BTreeValue + Display + ?Sized> Debug for ValueRef<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<V: BTreeValue + Display + ?Sized> Display for ValueRef<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl<V: BTreeValue + PartialEq + ?Sized> PartialEq for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue + PartialEq + ?Sized> PartialEq<&V> for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        **self == **other
    }
}

impl<V: BTreeValue + PartialEq + ?Sized> PartialEq<V> for ValueRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &V) -> bool {
        **self == *other
    }
}
