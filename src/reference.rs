use std::ops::Deref;

use crate::node_ptr::{marker, NodeRef};
use crate::tree::{BTreeKey, BTreeValue};

#[derive(Debug)]
pub struct Ref<K: BTreeKey, V: BTreeValue> {
    node_ptr: NodeRef<K, V, marker::Shared, marker::Leaf>,
    value: *const V,
}

impl<K: BTreeKey, V: BTreeValue> Ref<K, V> {
    pub fn new(node_ptr: NodeRef<K, V, marker::Shared, marker::Leaf>, value: *const V) -> Self {
        Ref { node_ptr, value }
    }
}

impl<K: BTreeKey, V: BTreeValue> Drop for Ref<K, V> {
    fn drop(&mut self) {
        self.node_ptr.unlock_shared();
    }
}

impl<K: BTreeKey, V: BTreeValue> Deref for Ref<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
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
