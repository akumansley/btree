use std::cmp::Ordering as CmpOrdering;
use std::{borrow::Borrow, marker::PhantomData, mem::MaybeUninit, ops::Deref};

use crate::{
    pointers::{
        marker, AtomicPointerArrayValue, OwnedAtomicThinArc, OwnedNodeRef, OwnedThinAtomicPtr,
    },
    sync::{AtomicU64, AtomicUsize, Ordering},
    tree::KeyHead,
    util::prefetch_node,
};
use smallvec::Array;
use thin::{Arcable, QsArc, QsOwned, QsShared, QsWeak};

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

    /// SAFETY: the caller must ensure that they have exclusive access to the value at the given index
    /// that the value is initialized, and they must clean up the slot after calling this -- the method
    /// leaves the existing pointer in place as a landing pad for optimistic readers
    unsafe fn load_owned(&self, index: usize, num_elements: usize) -> P::OwnedPointer {
        debug_assert!(index < num_elements);

        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                // ORDERING: if we're claiming we own the value pointed herein, we should've synchronized using locks
                .load_owned(Ordering::Relaxed)
                .unwrap()
        }
    }

    fn insert(&self, ptr: P::OwnedPointer, index: usize, num_elements: usize) {
        self.atomic_shift_right(index, num_elements);
        self.atomic_write_ptr(index, ptr);
    }

    fn remove(&self, index: usize, num_elements: usize) -> P::OwnedPointer {
        // SAFETY: we're about to clobber the slot, so this is fine
        let ptr = unsafe { self.load_owned(index, num_elements) };
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
                .load_owned(Ordering::Relaxed)
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
    fn get_cloned(&self, index: usize, num_elements: usize) -> QsArc<T> {
        debug_assert!(index < num_elements);
        unsafe {
            self.array
                .get_unchecked(index)
                .assume_init_ref()
                .load_cloned(Ordering::Relaxed)
        }
    }
}

// Stores keys alongside their head prefixes (first 8 bytes as u64).
// Heads enable a branchless binary search that resolves most comparisons
// without loading the full key.
// Layout ordered for cache efficiency: heads are accessed first during
// binary search, so they're placed at the start of the struct.
#[repr(C)]
struct KeysWithHeads<const CAPACITY: usize, K: BTreeKey + ?Sized> {
    heads: [AtomicU64; CAPACITY],
    keys: NodeStorageArray<CAPACITY, K, OwnedAtomicThinArc<K>>,
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized> KeysWithHeads<CAPACITY, K> {
    fn new() -> Self {
        Self {
            heads: [const { AtomicU64::new(0) }; CAPACITY],
            keys: NodeStorageArray::new(),
        }
    }

    fn shift_heads_right(&self, index: usize, num_elements: usize) {
        debug_assert!(num_elements < CAPACITY);
        for i in (index..num_elements).rev() {
            // SAFETY: i < num_elements < CAPACITY, and i+1 <= num_elements < CAPACITY
            let val = unsafe { self.heads.get_unchecked(i) }.load(Ordering::Relaxed);
            unsafe { self.heads.get_unchecked(i + 1) }.store(val, Ordering::Relaxed);
        }
    }

    fn shift_heads_left(&self, index: usize, num_elements: usize) {
        debug_assert!(num_elements <= CAPACITY);
        for i in index..(num_elements - 1) {
            // SAFETY: i+1 < num_elements <= CAPACITY, and i < num_elements - 1 < CAPACITY
            let val = unsafe { self.heads.get_unchecked(i + 1) }.load(Ordering::Relaxed);
            unsafe { self.heads.get_unchecked(i) }.store(val, Ordering::Relaxed);
        }
    }

    #[inline]
    fn store_head(&self, index: usize, head: u64) {
        // SAFETY: callers ensure index < CAPACITY
        unsafe { self.heads.get_unchecked(index) }.store(head, Ordering::Relaxed);
    }

    /// Branchless binary search on heads only. Returns the lower bound index
    /// where `head >= search_head`. This is an approximation of the full search
    /// result, useful for prefetching before the expensive key comparisons.
    #[inline]
    fn head_lower_bound(&self, search_head: u64, num_keys: usize) -> usize {
        debug_assert!(num_keys > 0 && num_keys <= CAPACITY);
        // SAFETY: mid is always < num_keys <= CAPACITY throughout the loop because
        // mid = base + half where base < len and half = len/2, so mid < len <= num_keys
        let mut base = 0usize;
        let mut len = num_keys;
        while len > 1 {
            let half = len / 2;
            let mid = base + half;
            let mid_head = unsafe { self.heads.get_unchecked(mid) }.load(Ordering::Relaxed);
            base = if mid_head < search_head { mid } else { base };
            len -= half;
        }
        // SAFETY: base < num_keys <= CAPACITY (loop invariant: base < len, and len >= 1 at exit)
        let base_head = unsafe { self.heads.get_unchecked(base) }.load(Ordering::Relaxed);
        if base_head < search_head {
            base + 1
        } else {
            base
        }
    }

    /// Full key comparison scan starting from a head lower bound.
    /// `base` should be the result of `head_lower_bound`.
    #[inline]
    fn key_scan<Q>(
        &self,
        key: &Q,
        search_head: u64,
        mut base: usize,
        num_keys: usize,
    ) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + KeyHead + ?Sized,
    {
        let keys_slice = self.keys.as_slice(num_keys);
        // SAFETY: base <= num_keys is checked by the while condition; num_keys <= CAPACITY
        while base < num_keys {
            let stored_head = unsafe { self.heads.get_unchecked(base) }.load(Ordering::Relaxed);
            if stored_head > search_head {
                break;
            }
            let cmp = unsafe { keys_slice.get_unchecked(base) }
                .load(Ordering::Relaxed)
                .unwrap()
                .deref()
                .borrow()
                .cmp(key);
            match cmp {
                CmpOrdering::Less => base += 1,
                CmpOrdering::Equal => return Ok(base),
                CmpOrdering::Greater => return Err(base),
            }
        }
        Err(base)
    }

    #[inline]
    pub fn binary_search<Q>(&self, key: &Q, num_keys: usize) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + KeyHead + ?Sized,
    {
        if num_keys == 0 {
            return Err(0);
        }
        let search_head = key.key_head();
        let base = self.head_lower_bound(search_head, num_keys);
        self.key_scan(key, search_head, base, num_keys)
    }

    #[inline]
    fn get(&self, index: usize, num_keys: usize) -> QsWeak<K> {
        self.keys.get(index, num_keys)
    }

    #[inline]
    fn get_cloned(&self, index: usize, num_keys: usize) -> QsArc<K> {
        self.keys.get_cloned(index, num_keys)
    }

    #[inline]
    fn insert(&self, key: QsArc<K>, index: usize, num_keys: usize) {
        self.shift_heads_right(index, num_keys);
        self.store_head(index, key.key_head());
        self.keys.insert(key, index, num_keys);
    }

    #[inline]
    fn remove(&self, index: usize, num_keys: usize) -> QsArc<K> {
        let key = self.keys.remove(index, num_keys);
        self.shift_heads_left(index, num_keys);
        key
    }

    #[inline]
    fn replace(&self, index: usize, key: QsArc<K>) -> QsArc<K> {
        self.store_head(index, key.key_head());
        self.keys.replace(index, key)
    }

    #[inline]
    fn set(&self, index: usize, key: QsArc<K>) {
        self.store_head(index, key.key_head());
        self.keys.set(index, key);
    }

    #[inline]
    fn push(&self, key: QsArc<K>, num_keys: usize) {
        self.store_head(num_keys, key.key_head());
        self.keys.push(key, num_keys);
    }

    #[inline]
    fn as_slice(&self, num_keys: usize) -> &[OwnedAtomicThinArc<K>] {
        self.keys.as_slice(num_keys)
    }

    #[inline]
    fn iter<'a>(&'a self, num_keys: usize) -> impl Iterator<Item = QsWeak<K>> + 'a {
        self.keys.iter(num_keys)
    }

    #[inline]
    fn iter_owned<'a>(&'a self, num_keys: usize) -> impl Iterator<Item = QsArc<K>> + 'a {
        self.keys.iter_owned(num_keys)
    }

    /// SAFETY: the caller must ensure exclusive access to the value at the given index
    #[inline]
    unsafe fn load_owned(&self, index: usize, num_keys: usize) -> QsArc<K> {
        unsafe { self.keys.load_owned(index, num_keys) }
    }

    fn extend(&self, iter: impl Iterator<Item = QsArc<K>>, num_keys: usize) -> usize {
        let mut added = 0;
        for key in iter {
            self.store_head(num_keys + added, key.key_head());
            self.keys.push(key, num_keys + added);
            added += 1;
        }
        added
    }

    fn check_invariants(&self, num_keys: usize) {
        assert!(num_keys <= CAPACITY);
        let keys = self.as_slice(num_keys);
        for (key1, key2) in keys.iter().zip(keys.iter().skip(1)) {
            assert!(
                *key1.load(Ordering::Relaxed).unwrap() < *key2.load(Ordering::Relaxed).unwrap(),
                "key1: {:?} key2: {:?}",
                &*key1.load(Ordering::Relaxed).unwrap(),
                &*key2.load(Ordering::Relaxed).unwrap()
            );
        }
    }
}

// two generic parameters to work around missing const generic expressions
// does not drop keys or children when dropped
#[repr(C)]
pub struct InternalNodeStorage<
    const CAPACITY: usize,
    const CAPACITY_PLUS_ONE: usize,
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
> {
    num_keys: AtomicUsize,
    keys: KeysWithHeads<CAPACITY, K>,
    children: NodeStorageArray<CAPACITY_PLUS_ONE, NodeHeader, OwnedThinAtomicPtr<NodeHeader>>,
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
            unsafe { QsArc::drop_immediately(key) };
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
    > Default for InternalNodeStorage<CAPACITY, CAPACITY_PLUS_ONE, K, V>
{
    fn default() -> Self {
        Self::new()
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
            num_keys: AtomicUsize::new(0),
            keys: KeysWithHeads::new(),
            children: NodeStorageArray::new(),
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

    pub fn iter_keys<'a>(&'a self) -> impl Iterator<Item = QsWeak<K>> + 'a {
        self.keys.iter(self.num_keys())
    }

    pub fn iter_keys_owned<'a>(&'a self) -> impl Iterator<Item = QsArc<K>> + 'a {
        self.keys.iter_owned(self.num_keys())
    }

    pub fn binary_search_keys<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + KeyHead + ?Sized,
    {
        self.keys.binary_search(key, self.num_keys())
    }

    /// Find the child for a given key, prefetching the child node's memory
    /// between the fast head-only narrowing and the slower full key comparisons.
    pub fn find_child<Q>(&self, key: &Q) -> QsShared<NodeHeader>
    where
        K: Borrow<Q>,
        Q: Ord + KeyHead + ?Sized,
    {
        let num_keys = self.num_keys();
        if num_keys == 0 {
            // SAFETY: internal node always has at least one child
            return unsafe { self.children.get_unchecked(0) };
        }

        // Phase 1: branchless search on heads to get approximate child index
        let search_head = key.key_head();
        let approx_index = self.keys.head_lower_bound(search_head, num_keys);

        // Prefetch the likely child before doing expensive key comparisons
        // SAFETY: approx_index <= num_keys, so it's a valid child index
        let approx_child = unsafe { self.children.get_unchecked(approx_index) };
        prefetch_node(&*approx_child as *const NodeHeader as *const u8);

        // Phase 2: full key comparison starting from head lower bound (no repeated head search)
        let exact_index = match self.keys.key_scan(key, search_head, approx_index, num_keys) {
            Ok(index) => index + 1,
            Err(index) => index,
        };

        if exact_index == approx_index {
            approx_child
        } else {
            // SAFETY: may be an optimistic read with out-of-bounds index,
            // but node drops are protected by qsbr
            unsafe { self.children.get_unchecked(exact_index) }
        }
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

    pub fn get_key(&self, index: usize) -> QsWeak<K> {
        self.keys.get(index, self.num_keys())
    }

    pub fn iter_children<'a>(&'a self) -> impl Iterator<Item = QsShared<NodeHeader>> + 'a {
        self.children.iter(self.num_children())
    }

    pub fn iter_children_owned<'a>(&'a self) -> impl Iterator<Item = QsOwned<NodeHeader>> + 'a {
        self.children.iter_owned(self.num_children())
    }

    pub fn insert(&self, key: QsArc<K>, child: QsOwned<NodeHeader>, index: usize) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children.insert(child, index, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn replace_key(&self, index: usize, key: QsArc<K>) -> QsArc<K> {
        self.keys.replace(index, key)
    }

    pub fn insert_child_with_split_key(
        &self,
        key: QsArc<K>,
        child: QsOwned<NodeHeader>,
        index: usize,
    ) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.insert(key, index, num_keys);
        self.children.insert(child, index + 1, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn push(&self, key: QsArc<K>, child: QsOwned<NodeHeader>) {
        let (num_keys, num_children) = self.num_keys_and_children();
        self.keys.push(key, num_keys);
        self.children.push(child, num_children);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn push_key(&self, key: QsArc<K>) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn pop(&self) -> (QsArc<K>, QsOwned<NodeHeader>) {
        let index = self.num_children() - 1;
        self.remove_child_at_index(index)
    }

    pub fn set_key(&self, index: usize, key: QsArc<K>) {
        self.keys.set(index, key);
    }

    pub fn remove_child_at_index(&self, index: usize) -> (QsArc<K>, QsOwned<NodeHeader>) {
        debug_assert!(index > 0);
        let (num_keys, num_children) = self.num_keys_and_children();
        let child = self.children.remove(index, num_children);
        let key = self.keys.remove(index - 1, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, child)
    }

    pub fn remove(&self, index: usize) -> (QsArc<K>, QsOwned<NodeHeader>) {
        let (num_keys, num_children) = self.num_keys_and_children();
        let key = self.keys.remove(index, num_keys);
        let child = self.children.remove(index, num_children);
        // I suspect this can be relaxed
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, child)
    }

    pub fn drain<'a>(
        &'a self,
        new_split_key: QsArc<K>,
    ) -> impl Iterator<Item = (QsArc<K>, QsOwned<NodeHeader>)> + 'a {
        let num_keys = self.num_keys.swap(0, Ordering::AcqRel);
        let keys = std::iter::once(new_split_key).chain(self.keys.iter_owned(num_keys));

        let children = self.children.iter_owned(num_keys + 1);
        keys.zip(children)
    }

    pub fn push_extra_child(&self, child: QsOwned<NodeHeader>) {
        self.children.set(self.num_keys(), child);
    }

    pub fn extend(&self, other: impl Iterator<Item = (QsArc<K>, QsOwned<NodeHeader>)>) {
        let num_keys = self.num_keys();
        let mut added = 0;

        for (key, child) in other {
            self.keys.push(key, num_keys + added);
            self.children
                .insert(child, num_keys + added + 1, num_keys + added + 1);
            added += 1;
        }
        self.num_keys.fetch_add(added, Ordering::Release);
    }

    pub fn extend_children(&self, other: impl Iterator<Item = QsOwned<NodeHeader>>) {
        let num_keys = self.num_keys();
        for (added, child) in other.enumerate() {
            self.children.push(child, num_keys + added);
        }
    }

    pub fn extend_keys(&self, other: impl Iterator<Item = QsArc<K>>) {
        let num_keys = self.num_keys();
        let added = self.keys.extend(other, num_keys);
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
        self.keys.check_invariants(self.num_keys());
        for child in self.children() {
            assert!(child.load_shared(Ordering::Relaxed).is_some());
        }
    }
}

// drops keys and values when dropped
pub type LeafNodeStorageArray<K, V> = LeafNodeStorage<MAX_KEYS_PER_NODE, K, V>;
#[repr(C)]
pub struct LeafNodeStorage<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    num_keys: AtomicUsize,
    keys: KeysWithHeads<CAPACITY, K>,
    values: NodeStorageArray<CAPACITY, V, OwnedThinAtomicPtr<V>>,
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop
    for LeafNodeStorage<CAPACITY, K, V>
{
    fn drop(&mut self) {
        let num_keys = self.num_keys();
        for i in 0..num_keys {
            let key = unsafe { self.keys.load_owned(i, num_keys) };
            unsafe { QsArc::drop_immediately(key) };

            let value = unsafe { self.values.load_owned(i, num_keys) };
            QsOwned::drop_immediately(value);
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Default
    for LeafNodeStorage<CAPACITY, K, V>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>
    LeafNodeStorage<CAPACITY, K, V>
{
    pub fn new() -> Self {
        Self {
            num_keys: AtomicUsize::new(0),
            keys: KeysWithHeads::new(),
            values: NodeStorageArray::new(),
        }
    }
}

impl<const CAPACITY: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>
    LeafNodeStorage<CAPACITY, K, V>
{
    pub fn num_keys(&self) -> usize {
        self.num_keys.load(Ordering::Acquire)
    }

    pub fn num_keys_relaxed(&self) -> usize {
        self.num_keys.load(Ordering::Relaxed)
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

    pub fn push(&self, key: QsArc<K>, value: QsOwned<V>) {
        let num_keys = self.num_keys();
        self.keys.push(key, num_keys);
        self.values.push(value, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn get_key(&self, index: usize) -> QsWeak<K> {
        self.keys.get(index, self.num_keys_relaxed())
    }

    pub fn clone_key(&self, index: usize) -> QsArc<K> {
        self.keys.get_cloned(index, self.num_keys())
    }

    pub fn get_value(&self, index: usize) -> QsShared<V> {
        self.values.get(index, self.num_keys_relaxed())
    }

    pub unsafe fn load_owned(&self, index: usize) -> QsOwned<V> {
        unsafe { self.values.load_owned(index, self.num_keys()) }
    }

    /// SAFETY: this will clobber an existing value and not invoke drop, so
    /// the caller must ensure the value has been moved out of the slot eg with load_owned
    pub unsafe fn clobber(&self, index: usize, value: QsOwned<V>) {
        self.values.set(index, value);
    }

    pub fn pop(&self) -> (QsArc<K>, QsOwned<V>) {
        let index = self.num_keys() - 1;
        self.remove(index)
    }

    pub fn extend(&self, other: impl Iterator<Item = (QsArc<K>, QsOwned<V>)>) {
        for (key, value) in other {
            self.push(key, value);
        }
    }

    pub fn binary_search_keys<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + KeyHead + ?Sized,
    {
        self.keys.binary_search(key, self.num_keys())
    }

    pub fn drain<'a>(&'a self) -> impl Iterator<Item = (QsArc<K>, QsOwned<V>)> + 'a {
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

    pub fn insert(&self, key: QsArc<K>, value: QsOwned<V>, index: usize) {
        let num_keys = self.num_keys();
        debug_assert!(index <= num_keys && num_keys < CAPACITY);
        self.keys.insert(key, index, num_keys);
        self.values.insert(value, index, num_keys);
        self.num_keys.fetch_add(1, Ordering::Release);
    }

    pub fn remove(&self, index: usize) -> (QsArc<K>, QsOwned<V>) {
        let num_keys = self.num_keys();
        debug_assert!(index < num_keys && num_keys <= CAPACITY);
        let key = self.keys.remove(index, num_keys);
        let value = self.values.remove(index, num_keys);
        self.num_keys.fetch_sub(1, Ordering::Release);
        (key, value)
    }

    pub fn check_invariants(&self) {
        self.keys.check_invariants(self.num_keys());
    }
}

#[cfg(test)]
mod tests {
    use crate::leaf_node::LeafNode;
    use btree_macros::qsbr_test;
    use thin::{QsArc, QsOwned};

    use super::*;

    #[qsbr_test]
    fn test_internal_node_storage_array() {
        let array = QsOwned::new(InternalNodeStorage::<3, 4, u32, String>::new());

        // Create some test data
        let key1 = QsArc::new(1u32);
        let key2 = QsArc::new(2u32);

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
        let key = QsArc::new(1u32);
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
