use crate::pointers::atomic::SharedThinAtomicPtr;
use crate::pointers::node_ref::SharedNodeRef;
use crate::pointers::OwnedNodeRef;
use crate::sync::Ordering;
use crate::{
    array_types::{LeafNodeStorageArray, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE},
    node::{Height, NodeHeader},
    pointers::node_ref::marker,
    tree::{BTreeKey, BTreeValue, ModificationType},
};
use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::{fmt, ptr};
use thin::{QsArc, QsOwned, QsShared, QsWeak};

#[repr(C)]
pub struct LeafNode<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    header: NodeHeader,
    pub inner: UnsafeCell<LeafNodeInner<K, V>>,
}

unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Send for LeafNode<K, V> {}
unsafe impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Sync for LeafNode<K, V> {}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> PartialEq for LeafNode<K, V> {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self as *const _, other as *const _)
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Eq for LeafNode<K, V> {}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> fmt::Debug for LeafNode<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeafNode({:p})", self)
    }
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Drop for LeafNode<K, V> {
    fn drop(&mut self) {
        assert!(matches!(self.header.height(), Height::Leaf));
    }
}

pub struct LeafNodeInner<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub storage: LeafNodeStorageArray<K, V>,
    pub next_leaf: SharedThinAtomicPtr<LeafNode<K, V>>,
    pub prev_leaf: SharedThinAtomicPtr<LeafNode<K, V>>,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> LeafNode<K, V> {
    pub fn new() -> Self {
        LeafNode {
            header: NodeHeader::new(Height::Leaf),
            inner: UnsafeCell::new(LeafNodeInner {
                storage: LeafNodeStorageArray::new(),
                next_leaf: SharedThinAtomicPtr::null(),
                prev_leaf: SharedThinAtomicPtr::null(),
            }),
        }
    }

    pub unsafe fn get_inner(&self) -> &LeafNodeInner<K, V> {
        unsafe { &*self.inner.get() }
    }
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> LeafNodeInner<K, V> {
    pub fn next_leaf(&self) -> Option<SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>> {
        let next_leaf_ptr = self.next_leaf.load_shared(Ordering::Acquire);
        next_leaf_ptr.map(|next_leaf| SharedNodeRef::from_leaf_ptr(next_leaf).assume_unlocked())
    }
    pub fn prev_leaf(&self) -> Option<SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>> {
        let prev_leaf_ptr = self.prev_leaf.load_shared(Ordering::Acquire);
        prev_leaf_ptr.map(|prev_leaf| SharedNodeRef::from_leaf_ptr(prev_leaf).assume_unlocked())
    }

    pub fn binary_search_key<Q>(&self, search_key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.storage.binary_search_keys(search_key)
    }

    pub fn get<Q>(&self, search_key: &Q) -> Option<(QsWeak<K>, QsShared<V>)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        debug_println!("LeafNode get {:?}", search_key);
        match self.binary_search_key(search_key) {
            Ok(index) => Some((
                self.storage.keys()[index].load(Ordering::Acquire).unwrap(),
                self.storage.values()[index]
                    .load_shared(Ordering::Acquire)
                    .unwrap(),
            )),
            Err(_) => {
                debug_println!("LeafNode get {:?} not found", search_key);
                None
            }
        }
    }
    #[must_use = "must check if insertion occurred to update tree len"]
    pub fn insert(&mut self, key_to_insert: QsArc<K>, value: QsOwned<V>) -> bool {
        match self.binary_search_key(key_to_insert.deref()) {
            Ok(index) => {
                let old_value = self.storage.set(index, value);
                if old_value.is_some() {
                    let owned_value = old_value.unwrap();
                    drop(owned_value);
                }
                // no need to qsbr this key -- it can't have been published yet
                drop(key_to_insert);
                false
            }
            Err(index) => {
                self.insert_new_value_at_index(key_to_insert, value, index);
                true
            }
        }
    }

    pub fn insert_new_value_at_index(
        &mut self,
        key_to_insert: QsArc<K>,
        value: QsOwned<V>,
        index: usize,
    ) {
        self.storage.insert(key_to_insert, value, index);
    }

    pub fn update(&mut self, index: usize, value: QsOwned<V>) {
        let old_value = self.storage.set(index, value);
        let owned_value = old_value.unwrap();
        drop(owned_value);
    }

    pub fn modify_value<E>(&mut self, index: usize, modify_fn: impl FnOnce(QsOwned<V>) -> Result<QsOwned<V>, (QsOwned<V>, E)>) -> Result<(), E> {
        let old_value = unsafe { self.storage.into_owned(index) };
        match modify_fn(old_value) {
            Ok(modified_value) => {
                unsafe { self.storage.clobber(index, modified_value) };
                Ok(())
            }
            Err((original_value, error)) => {
                unsafe { self.storage.clobber(index, original_value) };
                Err(error)
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> bool {
        match self.binary_search_key(key) {
            Ok(index) => {
                self.remove_at_index(index);
                true
            }
            Err(_) => false,
        }
    }
    pub fn remove_at_index(&mut self, index: usize) {
        let (key, value) = self.storage.remove(index);

        drop(key);
        drop(value);
    }

    pub fn num_keys(&self) -> usize {
        self.storage.num_keys()
    }

    pub fn num_keys_relaxed(&self) -> usize {
        self.storage.num_keys_relaxed()
    }

    pub fn has_capacity_for_modification(&self, modification_type: ModificationType) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys() < MAX_KEYS_PER_NODE,
            ModificationType::Removal => self.num_keys() > MIN_KEYS_PER_NODE,
            ModificationType::NonModifying => true,
        }
    }

    // a leaf at the top of tree is allowed to get underfull -- as low as 0
    pub fn has_capacity_for_modification_as_top_of_tree(
        &self,
        modification_type: ModificationType,
    ) -> bool {
        match modification_type {
            ModificationType::Insertion => self.num_keys() < MAX_KEYS_PER_NODE,
            ModificationType::Removal => true,
            ModificationType::NonModifying => true,
        }
    }

    pub(crate) fn move_from_right_neighbor_into_left_node(
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
        from: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        to: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    ) {
        // update sibling pointers
        if let Some(right_neighbor_next_leaf) = from.next_leaf() {
            let right_neighbor_next_leaf = right_neighbor_next_leaf.lock_exclusive();

            right_neighbor_next_leaf
                .prev_leaf
                .store(to.to_shared_leaf_ptr(), Ordering::Release);

            to.next_leaf.store(
                right_neighbor_next_leaf.to_shared_leaf_ptr(),
                Ordering::Release,
            );
            right_neighbor_next_leaf.unlock_exclusive();
        } else {
            to.next_leaf.clear(Ordering::Release);
        }

        to.storage.extend(from.storage.drain());
        let (key, from_owned) = parent.remove_child(from.into_ptr());
        let from_owned =
            OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                from_owned,
            )
            .assert_leaf()
            .assert_exclusive();
        // retire the leaf -- this ensures that any optimistic readers will fail
        // we've already drained it, so any moved keys won't be freed (which is what we want)
        from_owned.retire();
        drop(key);
        // qsbr-drop the leaf
        drop(from_owned);
    }

    pub fn move_last_to_front_of(
        left: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        right: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_last_to_front_of");

        let (last_key, last_value) = left.storage.pop();
        right.storage.insert(last_key.clone(), last_value, 0);

        // Update the split key in the parent -- and we did need to increment the ref count above
        // because we're copying the first key to the parent
        let old_key = parent.update_split_key(right.into_ptr(), last_key);

        // and we need to decrement the ref count on the old key
        drop(old_key);
    }

    pub fn move_first_to_end_of(
        right: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        left: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        mut parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    ) {
        debug_println!("LeafNode move_first_to_end_of ");
        let (first_key, first_value) = right.storage.remove(0);
        left.storage.push(first_key, first_value);

        // Update the split key in the parent for self, cloning it upwards
        let new_split_key = right.storage.keys()[0].load_cloned(Ordering::Acquire);

        // and we need to decrement the ref count on the old key
        let old_key = parent.update_split_key(right.into_ptr(), new_split_key);

        drop(old_key);
    }

    pub fn print_node(&self) {
        println!("LeafNode: {:p}", self);
        println!("+----------------------+");
        println!("| Num Keys: {}           |", self.num_keys());
        println!("+----------------------+");
        println!("| Keys and Values:     |");
        if self.num_keys() > 0 {
            for i in 0..self.num_keys() {
                println!(
                    "|  - Key: {:?}         |",
                    self.storage.keys()[i].load(Ordering::Relaxed).as_ref()
                );
                println!(
                    "|  - Value: {:?}       |",
                    self.storage.values()[i].load_shared(Ordering::Relaxed)
                );
            }
        }
        println!("+----------------------+");
    }

    pub fn check_invariants(&self) {
        self.storage.check_invariants();
    }
}
