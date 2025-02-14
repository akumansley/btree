use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use smallvec::Array;

use crate::{
    graceful_pointers::{AtomicGracefulArc, GracefulArc, GracefulAtomicPointer},
    node::NodeHeader,
    node_ptr::{marker, NodePtr, NodeRef},
    BTreeKey, BTreeValue,
};
pub const ORDER: usize = 2_usize.pow(6);
pub const MAX_KEYS_PER_NODE: usize = ORDER - 1; // number of search keys per node
pub const MIN_KEYS_PER_NODE: usize = ORDER / 2; // number of search keys per node
pub const KV_IDX_CENTER: usize = MAX_KEYS_PER_NODE / 2;
pub const MAX_CHILDREN_PER_NODE: usize = ORDER;

pub const KEY_TEMP_ARRAY_SIZE: usize = ORDER;
pub const VALUE_TEMP_ARRAY_SIZE: usize = ORDER;

pub const CHILD_TEMP_ARRAY_SIZE: usize = ORDER + 1;

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

struct NodeStorageArray<const CAPACITY: usize, T: Send + 'static, P: GracefulAtomicPointer<T>> {
    array: [MaybeUninit<P>; CAPACITY],
    phantom: PhantomData<T>,
}

impl<const CAPACITY: usize, T: Send + 'static, P: GracefulAtomicPointer<T>>
    NodeStorageArray<CAPACITY, T, P>
{
    pub fn new() -> Self {
        Self {
            array: [const { MaybeUninit::<P>::uninit() }; CAPACITY],
            phantom: PhantomData,
        }
    }

    // panics if from is null
    fn atomic_copy_ptr(&self, from: usize, to: usize) {
        unsafe {
            self.array.get_unchecked(to).assume_init_ref().store(
                self.array
                    .get_unchecked(from)
                    .assume_init_ref()
                    .load(Ordering::Relaxed),
                Ordering::Relaxed,
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

    fn atomic_write_ptr(&self, index: usize, ptr: P::GracefulPointer) {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .store(ptr, Ordering::Relaxed);
        }
    }

    fn set(&self, index: usize, ptr: P::GracefulPointer) {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .store(ptr, Ordering::Relaxed);
        }
    }
    fn replace(&self, index: usize, ptr: P::GracefulPointer) -> P::GracefulPointer {
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .swap(ptr, Ordering::Relaxed)
                .unwrap()
        }
    }

    fn push(&self, ptr: P::GracefulPointer, num_elements: usize) {
        self.atomic_write_ptr(num_elements, ptr);
    }

    fn get(&self, index: usize, num_elements: usize) -> P::GracefulPointer {
        debug_assert!(index < num_elements);
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .load(Ordering::Relaxed)
        }
    }

    fn insert(&self, ptr: P::GracefulPointer, index: usize, num_elements: usize) {
        self.atomic_shift_right(index, num_elements);
        self.atomic_write_ptr(index, ptr);
    }

    fn remove(&self, index: usize, num_elements: usize) -> P::GracefulPointer {
        let ptr = self.get(index, num_elements);
        self.atomic_shift_left(index, num_elements);
        ptr
    }

    fn iter<'a>(&'a self, num_elements: usize) -> impl Iterator<Item = P::GracefulPointer> + 'a {
        (0..num_elements).map(move |i| unsafe {
            self.array
                .get_unchecked(i)
                .assume_init_ref()
                .load(Ordering::Relaxed)
        })
    }

    fn as_slice(&self, num_elements: usize) -> &[P] {
        unsafe { std::slice::from_raw_parts(self.array.as_ptr() as *const P, num_elements) }
    }
}

// two generic parameters to work around missing const generic expressions
// does not drop keys or children when dropped
pub struct InternalNodeStorage<
    const CAPACITY: usize,
    const CAPACITY_PLUS_ONE: usize,
    K: BTreeKey,
    V: BTreeValue,
> {
    keys: NodeStorageArray<CAPACITY, K, AtomicGracefulArc<K>>,
    children: NodeStorageArray<CAPACITY_PLUS_ONE, NodeHeader, AtomicPtr<NodeHeader>>,
    num_keys: AtomicUsize,
    phantom: PhantomData<V>,
}

impl<const CAPACITY: usize, const CAPACITY_PLUS_ONE: usize, K: BTreeKey, V: BTreeValue> Drop
    for InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
{
    fn drop(&mut self) {
        for key in self.keys.iter(self.num_keys()) {
            unsafe { key.decrement_ref_count_and_drop_if_zero() };
        }
    }
}

impl<const CAPACITY: usize, const CAPACITY_PLUS_ONE: usize, K: BTreeKey, V: BTreeValue>
    InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
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

impl<const CAPACITY: usize, const CAPACITY_PLUS_ONE: usize, K: BTreeKey, V: BTreeValue>
    InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
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

    pub fn iter_keys<'a>(&'a self) -> impl Iterator<Item = GracefulArc<K>> + 'a {
        self.keys.iter(self.num_keys())
    }

    pub fn binary_search_keys(&self, key: &K) -> Result<usize, usize> {
        let num_keys = self.num_keys();
        self.keys
            .as_slice(num_keys)
            .binary_search_by(|k| k.load(Ordering::Relaxed).cmp(key))
    }
    pub fn get_child(&self, index: usize) -> NodePtr {
        NodePtr::from_raw_ptr(self.children.get(index, self.num_children()))
    }

    pub fn get_key(&self, index: usize) -> GracefulArc<K> {
        self.keys.get(index, self.num_keys())
    }

    pub fn iter_children<'a>(&'a self) -> impl Iterator<Item = NodePtr> + 'a {
        self.children
            .iter(self.num_children())
            .map(|ptr| NodePtr::from_raw_ptr(ptr))
    }

    pub fn insert(&self, key: GracefulArc<K>, child: NodePtr, index: usize) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children
            .insert(child.as_raw_ptr(), index, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }
    pub fn insert_child_with_split_key(&self, key: GracefulArc<K>, child: NodePtr, index: usize) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children
            .insert(child.as_raw_ptr(), index + 1, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn push(&self, key: GracefulArc<K>, child: NodePtr) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.push(key, num_keys);
        self.children.push(child.as_raw_ptr(), num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }
    pub fn push_key(&self, key: GracefulArc<K>) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn pop(&self) -> (GracefulArc<K>, NodePtr) {
        let index = self.num_children() - 1;
        self.remove_child_at_index(index)
    }

    pub fn set_key(&self, index: usize, key: GracefulArc<K>) {
        self.keys.set(index, key);
    }

    pub fn remove_child_at_index(&self, index: usize) -> (GracefulArc<K>, NodePtr) {
        debug_assert!(index > 0);
        let (num_keys, num_children) = self.num_keys_and_children();
        let child = self.children.remove(index, num_children);
        let key = self.keys.remove(index - 1, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, NodePtr::from_raw_ptr(child))
    }

    pub fn remove(&self, index: usize) -> (GracefulArc<K>, NodePtr) {
        let (num_keys, num_children) = self.num_keys_and_children();
        let key = self.keys.remove(index, num_keys);
        let child = self.children.remove(index, num_children);
        // I suspect this can be relaxed
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, NodePtr::from_raw_ptr(child))
    }

    pub fn drain<'a>(
        &'a self,
        new_split_key: GracefulArc<K>,
    ) -> impl Iterator<Item = (GracefulArc<K>, NodePtr)> + 'a {
        let num_keys = self.num_keys.swap(0, Ordering::Relaxed);
        let keys = std::iter::once(new_split_key).chain(self.keys.iter(num_keys));

        let children = self.children.iter(num_keys + 1);
        keys.zip(children)
            .map(|(key, child)| (key, NodePtr::from_raw_ptr(child)))
    }

    pub fn push_extra_child(&self, child: NodePtr) {
        self.children.set(self.num_keys(), child.as_raw_ptr());
    }

    pub fn extend(&self, other: impl Iterator<Item = (GracefulArc<K>, NodePtr)>) {
        let num_keys = self.num_keys();
        let mut added = 0;

        for (key, child) in other {
            self.keys.insert(key, num_keys + added, num_keys + added);
            self.children.insert(
                child.as_raw_ptr(),
                num_keys + added + 1,
                num_keys + added + 1,
            );
            added += 1;
        }
        self.num_keys.fetch_add(added, Ordering::Release);
    }

    pub fn extend_children(&self, other: impl Iterator<Item = NodePtr>) {
        let num_keys = self.num_keys();
        let mut added = 0;
        for child in other {
            self.children.push(child.as_raw_ptr(), num_keys + added);
            added += 1;
        }
    }

    pub fn extend_keys(&self, other: impl Iterator<Item = GracefulArc<K>>) {
        let num_keys = self.num_keys();
        let mut added = 0;
        for key in other {
            self.keys.push(key, num_keys + added);
            added += 1;
        }
        self.num_keys.fetch_add(added, Ordering::Release);
    }

    pub fn keys(&self) -> &[AtomicGracefulArc<K>] {
        self.keys.as_slice(self.num_keys())
    }

    pub fn children(&self) -> &[AtomicPtr<NodeHeader>] {
        self.children.as_slice(self.num_children())
    }

    pub fn truncate(&self) {
        self.num_keys.store(0, Ordering::Release);
    }

    pub fn check_invariants(&self) {
        assert!(self.num_keys() <= CAPACITY);
        for (key1, key2) in self.keys().into_iter().zip(self.keys().into_iter().skip(1)) {
            assert!(
                &*key1.load(Ordering::Relaxed) < &*key2.load(Ordering::Relaxed),
                "key1: {:?} key2: {:?}",
                &*key1.load(Ordering::Relaxed),
                &*key2.load(Ordering::Relaxed)
            );
        }
        for child in self.children() {
            assert!(child.load(Ordering::Relaxed) != ptr::null_mut());
        }
    }
}

// drops keys and values when dropped
pub type LeafNodeStorageArray<K, V> = LeafNodeStorage<MAX_KEYS_PER_NODE, K, V>;
pub struct LeafNodeStorage<const CAPACITY: usize, K: BTreeKey, V: BTreeValue> {
    keys: NodeStorageArray<CAPACITY, K, AtomicGracefulArc<K>>,
    values: NodeStorageArray<CAPACITY, V, AtomicPtr<V>>,
    num_keys: AtomicUsize,
}

impl<const CAPACITY: usize, K: BTreeKey, V: BTreeValue> Drop for LeafNodeStorage<CAPACITY, K, V> {
    fn drop(&mut self) {
        let num_keys = self.num_keys();
        for i in 0..num_keys {
            unsafe {
                let key = self.keys.get(i, num_keys);
                key.decrement_ref_count_and_drop_if_zero();

                let value = self.values.get(i, num_keys);
                drop(Box::from_raw(value));
            }
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey, V: BTreeValue> LeafNodeStorage<CAPACITY, K, V> {
    pub fn new() -> Self {
        Self {
            keys: NodeStorageArray::new(),
            values: NodeStorageArray::new(),
            num_keys: AtomicUsize::new(0),
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey, V: BTreeValue> LeafNodeStorage<CAPACITY, K, V> {
    pub fn num_keys(&self) -> usize {
        self.num_keys.load(Ordering::Acquire)
    }

    pub fn keys(&self) -> &[AtomicGracefulArc<K>] {
        self.keys.as_slice(self.num_keys())
    }

    pub fn values(&self) -> &[AtomicPtr<V>] {
        self.values.as_slice(self.num_keys())
    }

    pub fn push(&self, key: GracefulArc<K>, value: *mut V) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.values.push(value, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn get_key(&self, index: usize) -> GracefulArc<K> {
        self.keys.get(index, self.num_keys())
    }

    pub fn get_value(&self, index: usize) -> *mut V {
        self.values.get(index, self.num_keys())
    }

    pub fn pop(&self) -> (GracefulArc<K>, *mut V) {
        let index = self.num_keys() - 1;
        self.remove(index)
    }

    pub fn extend(&self, other: impl Iterator<Item = (GracefulArc<K>, *mut V)>) {
        for (key, value) in other {
            self.push(key, value);
        }
    }

    pub fn binary_search_keys(&self, key: &K) -> Result<usize, usize> {
        let num_keys = self.num_keys();
        self.keys
            .as_slice(num_keys)
            .binary_search_by(|k| (&*k.load(Ordering::Relaxed)).cmp(key))
    }

    pub fn binary_search_keys_optimistic(
        &self,
        key: &K,
        node_ref: NodeRef<K, V, marker::Optimistic, marker::Leaf>,
    ) -> Result<usize, usize> {
        let num_keys = self.num_keys();
        self.keys.as_slice(num_keys).binary_search_by(|k| {
            let read_ptr = k.load(Ordering::Relaxed);
            match node_ref.validate_lock() {
                Ok(()) => (*read_ptr).cmp(key),
                Err(_) => std::cmp::Ordering::Equal, // we say equal immediately and give up searching -- the reader will fail validation later
            }
        })
    }

    pub fn drain<'a>(&'a self) -> impl Iterator<Item = (GracefulArc<K>, *mut V)> + 'a {
        let num_keys = self.num_keys.swap(0, Ordering::Relaxed);
        self.keys.iter(num_keys).zip(self.values.iter(num_keys))
    }

    pub fn set(&self, index: usize, value: *mut V) -> *mut V {
        if index < self.num_keys() {
            self.values.replace(index, value)
        } else {
            self.values.set(index, value);
            ptr::null_mut()
        }
    }

    pub fn insert(&self, key: GracefulArc<K>, value: *mut V, index: usize) {
        let num_keys = self.num_keys();
        self.keys.insert(key, index, num_keys);
        self.values.insert(value, index, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn remove(&self, index: usize) -> (GracefulArc<K>, *mut V) {
        let num_keys = self.num_keys();
        let key = self.keys.remove(index, num_keys);
        let value = self.values.remove(index, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Relaxed);
        (key, value)
    }

    pub fn check_invariants(&self) {
        assert!(self.num_keys() <= CAPACITY);
        for (key1, key2) in self.keys().into_iter().zip(self.keys().into_iter().skip(1)) {
            assert!(
                &*key1.load(Ordering::Relaxed) < &*key2.load(Ordering::Relaxed),
                "key1: {:?} key2: {:?}",
                &*key1.load(Ordering::Relaxed),
                &*key2.load(Ordering::Relaxed)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::Height;

    use super::*;

    #[test]
    fn test_internal_node_storage_array() {
        let array = InternalNodeStorage::<3, 4, u32, String>::new();

        // Create some test data
        let key1 = GracefulArc::new(1u32);
        let key2 = GracefulArc::new(2u32);
        let node1 = Box::into_raw(Box::new(NodeHeader::new(Height::Leaf)));
        let node2 = Box::into_raw(Box::new(NodeHeader::new(Height::Leaf)));
        let node3 = Box::into_raw(Box::new(NodeHeader::new(Height::Leaf)));

        // Test insert
        array.push_extra_child(NodePtr::from_raw_ptr(node1));
        array.insert_child_with_split_key(
            key1.clone_without_incrementing_ref_count(),
            NodePtr::from_raw_ptr(node2),
            0,
        );
        array.insert_child_with_split_key(
            key2.clone_without_incrementing_ref_count(),
            NodePtr::from_raw_ptr(node3),
            1,
        );
        assert_eq!(array.num_keys(), 2);

        assert_eq!(*array.keys()[0].load(Ordering::Relaxed), 1u32);
        assert_eq!(*array.keys()[1].load(Ordering::Relaxed), 2u32);
        assert_eq!(array.children()[0].load(Ordering::Relaxed), node1);
        assert_eq!(array.children()[1].load(Ordering::Relaxed), node2);
        assert_eq!(array.children()[2].load(Ordering::Relaxed), node3);

        // Test remove
        array.remove(0);
        // removes node1 and 1u32
        assert_eq!(array.num_keys(), 1);

        array.remove_child_at_index(1);
        // removes node3 and 2u32

        assert_eq!(array.num_keys(), 0);
        assert_eq!(array.keys().len(), 0);
        assert_eq!(array.children().len(), 1);
        assert_eq!(array.children()[0].load(Ordering::Relaxed), node2);

        // AtomicKeyNodePtrArray does not drop keys or children when dropped
        unsafe {
            key1.drop_in_place();
            key2.drop_in_place();
            drop(Box::from_raw(node1));
            drop(Box::from_raw(node2));
            drop(Box::from_raw(node3));
        }
    }

    #[test]
    fn test_leaf_node_storage_array() {
        let array = LeafNodeStorage::<3, u32, String>::new();

        // Create test data
        let key = GracefulArc::new(1u32);
        let value = Box::into_raw(Box::new(String::from("test")));

        // Test insert
        array.insert(key.clone_without_incrementing_ref_count(), value, 0);
        assert_eq!(array.num_keys(), 1);

        // Test keys() and values() methods
        let keys = array.keys();
        let values = array.values();
        assert_eq!(keys.len(), 1);
        assert_eq!(values.len(), 1);

        unsafe {
            assert_eq!(*keys[0].load(Ordering::Relaxed), 1u32);
            assert_eq!(*values[0].load(Ordering::Relaxed), "test");
        }

        // Test remove
        array.remove(0);
        assert_eq!(array.num_keys(), 0);

        // AtomicKeyNodePtrArray does not drop keys or values when dropped
        unsafe {
            key.drop_in_place();
            drop(Box::from_raw(value));
        }
    }
}
