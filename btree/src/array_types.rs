use std::{marker::PhantomData, mem::MaybeUninit, ops::Deref};

use crate::{
    pointers::{
        marker, Arcable, AtomicPointerArrayValue, OwnedAtomicThinArc, OwnedNodeRef, OwnedThinArc,
        OwnedThinAtomicPtr, SharedThinArc,
    },
    sync::{AtomicUsize, Ordering},
};
use smallvec::Array;
use thin::{QsOwned, QsShared};

use crate::{node::NodeHeader, BTreeKey, BTreeValue};
pub const ORDER: usize = 2_usize.pow(6);
pub const MAX_KEYS_PER_NODE: usize = ORDER - 1; // number of search keys per node
pub const MIN_KEYS_PER_NODE: usize = ORDER / 2; // number of search keys per node
pub const KV_IDX_CENTER: usize = MAX_KEYS_PER_NODE / 2;
pub const MAX_CHILDREN_PER_NODE: usize = ORDER;
pub const VALUE_TEMP_ARRAY_SIZE: usize = ORDER;

macro_rules! define_array_types {
    ($name:ident, $size:expr) => {
        pub struct $name<T>(pub [T; $size]);

        unsafe impl<T> Array for $name<T> {
            type Item = T;
            fn size() -> usize {
                $size
            }
        }
    };
}

define_array_types!(InternalKeyTempArray, MAX_KEYS_PER_NODE);
define_array_types!(InternalChildTempArray, MAX_CHILDREN_PER_NODE);
define_array_types!(LeafTempArray, VALUE_TEMP_ARRAY_SIZE);

struct NodeStorageArray<
    const CAPACITY: usize,
    T: Send + 'static + ?Sized,
    P: AtomicPointerArrayValue<T>,
> {
    array: [MaybeUninit<P>; CAPACITY],
    phantom: PhantomData<T>,
}

impl<const CAPACITY: usize, T: Send + 'static + ?Sized, P: AtomicPointerArrayValue<T>>
    NodeStorageArray<CAPACITY, T, P>
{
    pub fn new() -> Self {
        Self {
            array: [const { MaybeUninit::<P>::uninit() }; CAPACITY],
            phantom: PhantomData,
        }
    }

    fn atomic_copy_ptr(&self, from: usize, to: usize) {
        unsafe {
            self.array.get_unchecked(to).assume_init_ref().store(
                // ORDERING: the writing thread must have sync'd when it exclusively locked the node,
                // so we can be relaxed
                self.array
                    .get_unchecked(from)
                    .assume_init_ref()
                    .must_load_for_move(Ordering::Relaxed),
                // ORDERING: an optimistic reader may or may not have seen the value whose pointer we're moving
                // (we might've added it anew and then shifted it), so we need to release that write
                Ordering::Release,
            );
        }
    }

    fn atomic_shift_left(&self, index: usize, num_elements: usize) {
        for i in index..(num_elements - 1) {
            self.atomic_copy_ptr(i + 1, i);
        }
        // we just leave the old value there! it should be hidden behind the num_elements
    }

    fn atomic_shift_right(&self, index: usize, num_elements: usize) {
        for i in (index..num_elements).rev() {
            self.atomic_copy_ptr(i, i + 1);
        }
    }

    fn atomic_write_ptr(&self, index: usize, ptr: P::OwnedPointer) {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                // ORDERING: this may be a newly initialized value, so we need to release the pointer write
                // for optimistic readers
                .store(ptr, Ordering::Release);
        }
    }

    fn set(&self, index: usize, ptr: P::OwnedPointer) {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                // ORDERING: this may be a newly initialized value, so we need to release the pointer write
                // for optimistic readers
                .store(ptr, Ordering::Release);
        }
    }
    fn replace(&self, index: usize, ptr: P::OwnedPointer) -> P::OwnedPointer {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                // ORDERING: this may be a newly initialized value, so we need to release the pointer write
                // for optimistic readers, but the writer should've synchronized when it acquired a lock on the node
                .swap(ptr, Ordering::Release)
                .unwrap()
        }
    }

    fn push(&self, ptr: P::OwnedPointer, num_elements: usize) {
        self.atomic_write_ptr(num_elements, ptr);
    }

    fn get(&self, index: usize, num_elements: usize) -> P::SharedPointer {
        debug_assert!(index < num_elements);
        unsafe { self.get_unchecked(index) }
    }

    unsafe fn get_unchecked(&self, index: usize) -> P::SharedPointer {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .load_shared(Ordering::Acquire)
                .unwrap()
        }
    }

    fn into_owned(&self, index: usize, num_elements: usize) -> P::OwnedPointer {
        debug_assert!(index < num_elements);

        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                // ORDERING: if we're claiming we own the value pointed herein, we should've synchronized using locks
                .into_owned(Ordering::Relaxed)
                .unwrap()
        }
    }

    fn insert(&self, ptr: P::OwnedPointer, index: usize, num_elements: usize) {
        self.atomic_shift_right(index, num_elements);
        self.atomic_write_ptr(index, ptr);
    }

    fn remove(&self, index: usize, num_elements: usize) -> P::OwnedPointer {
        let ptr = self.into_owned(index, num_elements);
        self.atomic_shift_left(index, num_elements);
        ptr
    }

    fn iter<'a>(&'a self, num_elements: usize) -> impl Iterator<Item = P::SharedPointer> + 'a {
        (0..num_elements).map(move |i| unsafe {
            self.array
                .get_unchecked(i)
                .assume_init_ref()
                // ORDERING: this may be an optimistic read, so we need to Acquire any associated values
                .load_shared(Ordering::Acquire)
                .unwrap()
        })
    }

    fn iter_owned<'a>(&'a self, num_elements: usize) -> impl Iterator<Item = P::OwnedPointer> + 'a {
        (0..num_elements).map(move |i| unsafe {
            self.array
                .get_unchecked(i)
                .assume_init_ref()
                .into_owned(Ordering::Relaxed)
                .unwrap()
        })
    }

    fn as_slice(&self, num_elements: usize) -> &[P] {
        unsafe { std::slice::from_raw_parts(self.array.as_ptr() as *const P, num_elements) }
    }
}

impl<const CAPACITY: usize, T: Send + 'static + ?Sized + Arcable>
    NodeStorageArray<CAPACITY, T, OwnedAtomicThinArc<T>>
{
    fn get_cloned(&self, index: usize, num_elements: usize) -> OwnedThinArc<T> {
        debug_assert!(index < num_elements);
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .load_cloned(Ordering::Relaxed)
        }
    }
}

// two generic parameters to work around missing const generic expressions
// does not drop keys or children when dropped
pub struct InternalNodeStorage<
    const CAPACITY: usize,
    const CAPACITY_PLUS_ONE: usize,
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
> {
    keys: NodeStorageArray<CAPACITY, K, OwnedAtomicThinArc<K>>,
    children: NodeStorageArray<CAPACITY_PLUS_ONE, NodeHeader, OwnedThinAtomicPtr<NodeHeader>>,
    num_keys: AtomicUsize,
    phantom: PhantomData<V>,
}

impl<
        const CAPACITY: usize,
        const CAPACITY_PLUS_ONE: usize,
        K: BTreeKey + ?Sized,
        V: BTreeValue + ?Sized,
    > Drop for InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
{
    fn drop(&mut self) {
        for key in self.keys.iter_owned(self.num_keys()) {
            // OK to drop immediately -- if we're dropping an internal node while it still has keys, we're dropping the entire tree
            unsafe { OwnedThinArc::drop_immediately(key) };
        }

        if self.num_keys() > 0 {
            for child in self.children.iter_owned(self.num_children()) {
                OwnedNodeRef::drop_immediately(
                    OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                        child,
                    ),
                )
            }
        }
    }
}

impl<
        const CAPACITY: usize,
        const CAPACITY_PLUS_ONE: usize,
        K: BTreeKey + ?Sized,
        V: BTreeValue + ?Sized,
    > InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
{
    pub fn new() -> Self {
        Self {
            keys: NodeStorageArray::new(),
            children: NodeStorageArray::new(),
            num_keys: AtomicUsize::new(0),
            phantom: PhantomData,
        }
    }
}

impl<
        const CAPACITY: usize,
        const CAPACITY_PLUS_ONE: usize,
        K: BTreeKey + ?Sized,
        V: BTreeValue + ?Sized,
    > InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
{
    pub fn num_keys(&self) -> usize {
        self.num_keys.load(Ordering::Acquire)
    }

    pub fn num_children(&self) -> usize {
        self.num_keys() + 1
    }

    fn num_keys_and_children(&self) -> (usize, usize) {
        let num_keys = self.num_keys();
        (num_keys, num_keys + 1)
    }

    pub fn iter_keys<'a>(&'a self) -> impl Iterator<Item = SharedThinArc<K>> + 'a {
        self.keys.iter(self.num_keys())
    }

    pub fn iter_keys_owned<'a>(&'a self) -> impl Iterator<Item = OwnedThinArc<K>> + 'a {
        self.keys.iter_owned(self.num_keys())
    }

    pub fn binary_search_keys(&self, key: &K) -> Result<usize, usize> {
        let num_keys = self.num_keys();
        self.keys
            .as_slice(num_keys)
            .binary_search_by(|k| k.load(Ordering::Acquire).unwrap().deref().cmp(key))
    }

    pub fn get_child(&self, index: usize) -> QsShared<NodeHeader> {
        self.children.get(index, self.num_children())
    }

    pub unsafe fn get_child_unchecked(&self, index: usize) -> QsShared<NodeHeader> {
        unsafe { self.children.get_unchecked(index) }
    }

    pub fn remove_only_child(&self) -> QsOwned<NodeHeader> {
        self.children.remove(0, self.num_children())
    }

    pub fn get_key(&self, index: usize) -> SharedThinArc<K> {
        self.keys.get(index, self.num_keys())
    }

    pub fn iter_children<'a>(&'a self) -> impl Iterator<Item = QsShared<NodeHeader>> + 'a {
        self.children.iter(self.num_children())
    }

    pub fn iter_children_owned<'a>(&'a self) -> impl Iterator<Item = QsOwned<NodeHeader>> + 'a {
        self.children.iter_owned(self.num_children())
    }

    pub fn insert(&self, key: OwnedThinArc<K>, child: QsOwned<NodeHeader>, index: usize) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children.insert(child, index, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn replace_key(&self, index: usize, key: OwnedThinArc<K>) -> OwnedThinArc<K> {
        self.keys.replace(index, key)
    }

    pub fn insert_child_with_split_key(
        &self,
        key: OwnedThinArc<K>,
        child: QsOwned<NodeHeader>,
        index: usize,
    ) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children.insert(child, index + 1, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn push(&self, key: OwnedThinArc<K>, child: QsOwned<NodeHeader>) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.push(key, num_keys);
        self.children.push(child, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }
    pub fn push_key(&self, key: OwnedThinArc<K>) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn pop(&self) -> (OwnedThinArc<K>, QsOwned<NodeHeader>) {
        let index = self.num_children() - 1;
        self.remove_child_at_index(index)
    }

    pub fn set_key(&self, index: usize, key: OwnedThinArc<K>) {
        self.keys.set(index, key);
    }

    pub fn remove_child_at_index(&self, index: usize) -> (OwnedThinArc<K>, QsOwned<NodeHeader>) {
        debug_assert!(index > 0);
        let (num_keys, num_children) = self.num_keys_and_children();
        let child = self.children.remove(index, num_children);
        let key = self.keys.remove(index - 1, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, child)
    }

    pub fn remove(&self, index: usize) -> (OwnedThinArc<K>, QsOwned<NodeHeader>) {
        let (num_keys, num_children) = self.num_keys_and_children();
        let key = self.keys.remove(index, num_keys);
        let child = self.children.remove(index, num_children);
        // I suspect this can be relaxed
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, child)
    }

    pub fn drain<'a>(
        &'a self,
        new_split_key: OwnedThinArc<K>,
    ) -> impl Iterator<Item = (OwnedThinArc<K>, QsOwned<NodeHeader>)> + 'a {
        let num_keys = self.num_keys.swap(0, Ordering::AcqRel);
        let keys = std::iter::once(new_split_key).chain(self.keys.iter_owned(num_keys));

        let children = self.children.iter_owned(num_keys + 1);
        keys.zip(children).map(|(key, child)| (key, child))
    }

    pub fn push_extra_child(&self, child: QsOwned<NodeHeader>) {
        self.children.set(self.num_keys(), child);
    }

    pub fn extend(&self, other: impl Iterator<Item = (OwnedThinArc<K>, QsOwned<NodeHeader>)>) {
        let num_keys = self.num_keys();
        let mut added = 0;

        for (key, child) in other {
            self.keys.insert(key, num_keys + added, num_keys + added);
            self.children
                .insert(child, num_keys + added + 1, num_keys + added + 1);
            added += 1;
        }
        self.num_keys.fetch_add(added, Ordering::Release);
    }

    pub fn extend_children(&self, other: impl Iterator<Item = QsOwned<NodeHeader>>) {
        let num_keys = self.num_keys();
        let mut added = 0;
        for child in other {
            self.children.push(child, num_keys + added);
            added += 1;
        }
    }

    pub fn extend_keys(&self, other: impl Iterator<Item = OwnedThinArc<K>>) {
        let num_keys = self.num_keys();
        let mut added = 0;
        for key in other {
            self.keys.push(key, num_keys + added);
            added += 1;
        }
        self.num_keys.fetch_add(added, Ordering::Release);
    }

    pub fn keys(&self) -> &[OwnedAtomicThinArc<K>] {
        self.keys.as_slice(self.num_keys())
    }

    pub fn children(&self) -> &[OwnedThinAtomicPtr<NodeHeader>] {
        self.children.as_slice(self.num_children())
    }

    pub fn truncate(&self) {
        self.num_keys.store(0, Ordering::Release);
    }

    pub fn check_invariants(&self) {
        assert!(self.num_keys() <= CAPACITY);
        for (key1, key2) in self.keys().into_iter().zip(self.keys().into_iter().skip(1)) {
            assert!(
                &*key1.load(Ordering::Relaxed).unwrap() < &*key2.load(Ordering::Relaxed).unwrap(),
                "key1: {:?} key2: {:?}",
                &*key1.load(Ordering::Relaxed).unwrap(),
                &*key2.load(Ordering::Relaxed).unwrap()
            );
        }
        for child in self.children() {
            assert!(child.load_shared(Ordering::Relaxed).is_some());
        }
    }
}

// drops keys and values when dropped
pub type LeafNodeStorageArray<K, V> = LeafNodeStorage<MAX_KEYS_PER_NODE, K, V>;
pub struct LeafNodeStorage<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    keys: NodeStorageArray<CAPACITY, K, OwnedAtomicThinArc<K>>,
    values: NodeStorageArray<CAPACITY, V, OwnedThinAtomicPtr<V>>,
    num_keys: AtomicUsize,
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop
    for LeafNodeStorage<CAPACITY, K, V>
{
    fn drop(&mut self) {
        let num_keys = self.num_keys();
        for i in 0..num_keys {
            let key = self.keys.into_owned(i, num_keys);
            unsafe { OwnedThinArc::drop_immediately(key) };

            let value = self.values.into_owned(i, num_keys);
            QsOwned::drop_immediately(value);
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>
    LeafNodeStorage<CAPACITY, K, V>
{
    pub fn new() -> Self {
        Self {
            keys: NodeStorageArray::new(),
            values: NodeStorageArray::new(),
            num_keys: AtomicUsize::new(0),
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>
    LeafNodeStorage<CAPACITY, K, V>
{
    pub fn num_keys(&self) -> usize {
        self.num_keys.load(Ordering::Acquire)
    }

    pub fn keys(&self) -> &[OwnedAtomicThinArc<K>] {
        self.keys.as_slice(self.num_keys())
    }

    pub fn values(&self) -> &[OwnedThinAtomicPtr<V>] {
        self.values.as_slice(self.num_keys())
    }

    pub fn iter_values<'a>(&'a self) -> impl Iterator<Item = QsShared<V>> + 'a {
        self.values.iter(self.num_keys())
    }
    pub fn push(&self, key: OwnedThinArc<K>, value: QsOwned<V>) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.values.push(value, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn get_key(&self, index: usize) -> SharedThinArc<K> {
        self.keys.get(index, self.num_keys())
    }

    pub fn clone_key(&self, index: usize) -> OwnedThinArc<K> {
        let arc = self.keys.get_cloned(index, self.num_keys());
        arc
    }

    pub fn get_value(&self, index: usize) -> QsShared<V> {
        self.values.get(index, self.num_keys())
    }

    pub fn pop(&self) -> (OwnedThinArc<K>, QsOwned<V>) {
        let index = self.num_keys() - 1;
        self.remove(index)
    }

    pub fn extend(&self, other: impl Iterator<Item = (OwnedThinArc<K>, QsOwned<V>)>) {
        for (key, value) in other {
            self.push(key, value);
        }
    }

    pub fn binary_search_keys(&self, key: &K) -> Result<usize, usize> {
        let num_keys = self.num_keys();
        self.keys
            .as_slice(num_keys)
            .binary_search_by(|k| k.load(Ordering::Acquire).unwrap().deref().cmp(key))
    }

    pub fn drain<'a>(&'a self) -> impl Iterator<Item = (OwnedThinArc<K>, QsOwned<V>)> + 'a {
        let num_keys = self.num_keys.swap(0, Ordering::AcqRel);
        self.keys
            .iter_owned(num_keys)
            .zip(self.values.iter_owned(num_keys))
    }

    pub fn set(&self, index: usize, value: QsOwned<V>) -> Option<QsOwned<V>> {
        if index < self.num_keys() {
            Some(self.values.replace(index, value))
        } else {
            self.values.set(index, value);
            None
        }
    }

    pub fn insert(&self, key: OwnedThinArc<K>, value: QsOwned<V>, index: usize) {
        let num_keys = self.num_keys();
        self.keys.insert(key, index, num_keys);
        self.values.insert(value, index, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn remove(&self, index: usize) -> (OwnedThinArc<K>, QsOwned<V>) {
        let num_keys = self.num_keys();
        let key = self.keys.remove(index, num_keys);
        let value = self.values.remove(index, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, value)
    }

    pub fn check_invariants(&self) {
        assert!(self.num_keys() <= CAPACITY);
        for (key1, key2) in self.keys().into_iter().zip(self.keys().into_iter().skip(1)) {
            assert!(
                &*key1.load(Ordering::Relaxed).unwrap() < &*key2.load(Ordering::Relaxed).unwrap(),
                "key1: {:?} key2: {:?}",
                &*key1.load(Ordering::Relaxed).unwrap(),
                &*key2.load(Ordering::Relaxed).unwrap()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::leaf_node::LeafNode;
    use btree_macros::qsbr_test;
    use thin::QsOwned;

    use super::*;

    #[qsbr_test]
    fn test_internal_node_storage_array() {
        let array = QsOwned::new(InternalNodeStorage::<3, 4, u32, String>::new());

        // Create some test data
        let key1 = OwnedThinArc::new(1u32);
        let key2 = OwnedThinArc::new(2u32);

        let node1 = unsafe { QsOwned::new(LeafNode::<u32, String>::new()).cast::<NodeHeader>() };
        let node1_shared = node1.share();
        let node2 = unsafe { QsOwned::new(LeafNode::<u32, String>::new()).cast::<NodeHeader>() };
        let node2_shared = node2.share();
        let node3 = unsafe { QsOwned::new(LeafNode::<u32, String>::new()).cast::<NodeHeader>() };
        let node3_shared = node3.share();

        // Test insert
        array.push_extra_child(node1);
        array.insert_child_with_split_key(key1, node2, 0);
        array.insert_child_with_split_key(key2, node3, 1);
        assert_eq!(array.num_keys(), 2);

        assert_eq!(*array.keys()[0].load(Ordering::Relaxed).unwrap(), 1u32);
        assert_eq!(*array.keys()[1].load(Ordering::Relaxed).unwrap(), 2u32);
        assert_eq!(
            array.children()[0].load_shared(Ordering::Relaxed).unwrap(),
            node1_shared
        );
        assert_eq!(
            array.children()[1].load_shared(Ordering::Relaxed).unwrap(),
            node2_shared
        );
        assert_eq!(
            array.children()[2].load_shared(Ordering::Relaxed).unwrap(),
            node3_shared
        );

        // Test remove
        let (_, child) = array.remove(0);
        OwnedNodeRef::drop_immediately(
            OwnedNodeRef::<u32, String, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                child,
            ),
        );
        // removes node1 and 1u32
        assert_eq!(array.num_keys(), 1);

        let (key, child) = array.remove_child_at_index(1);
        drop(key);
        drop(unsafe { child.cast::<LeafNode<u32, String>>() });
        // removes node3 and 2u32

        assert_eq!(array.num_keys(), 0);
        assert_eq!(array.keys().len(), 0);
        assert_eq!(array.children().len(), 1);
        assert_eq!(
            array.children()[0].load_shared(Ordering::Relaxed).unwrap(),
            node2_shared
        );
        let child = array.remove_only_child();
        OwnedNodeRef::drop_immediately(
            OwnedNodeRef::<u32, String, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                child,
            ),
        );
    }

    #[qsbr_test]
    fn test_leaf_node_storage_array() {
        let array = LeafNodeStorage::<3, u32, String>::new();

        // Create test data
        let key = OwnedThinArc::new(1u32);
        assert_eq!(*key, 1u32);
        let key_shared = key.share();
        assert_eq!(*key_shared, 1u32);
        let value = QsOwned::new(String::from("test"));

        // Test insert
        array.insert(key, value, 0);
        assert_eq!(array.num_keys(), 1);

        // Test keys() and values() methods
        let keys = array.keys();
        let values = array.values();
        assert_eq!(keys.len(), 1);
        assert_eq!(values.len(), 1);

        assert_eq!(*keys[0].load_shared(Ordering::Relaxed).unwrap(), 1u32);
        assert_eq!(*values[0].load_shared(Ordering::Relaxed).unwrap(), "test");

        // Test remove
        let (key, value) = array.remove(0);
        assert_eq!(array.num_keys(), 0);

        drop(key);
        drop(value);
    }
}
