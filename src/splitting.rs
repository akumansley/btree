use crate::array_types::{
    InternalChildTempArray, InternalKeyTempArray, LeafTempArray, KV_IDX_CENTER, MAX_KEYS_PER_NODE,
};
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::NodeHeader;
use crate::pointers::node_ref::{marker, OwnedNodeRef};
use crate::pointers::{OwnedThinArc, OwnedThinPtr, SharedNodeRef};
use crate::search_dequeue::SearchDequeue;
use crate::sync::Ordering;
use crate::util::UnwrapEither;
use crate::{BTreeKey, BTreeValue};
use smallvec::SmallVec;
use std::ops::Deref;

pub struct EntryLocation<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    pub leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    pub index: usize,
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> EntryLocation<K, V> {
    pub fn new(
        leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
        index: usize,
    ) -> Self {
        Self { leaf, index }
    }
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Clone for EntryLocation<K, V> {
    fn clone(&self) -> Self {
        Self {
            leaf: self.leaf,
            index: self.index,
        }
    }
}

enum Unlocker<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    UnlockAll,
    RetainLock(SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>),
}

impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> Unlocker<K, V> {
    fn unlock<N: marker::NodeType>(
        &self,
        node_ref: SharedNodeRef<K, V, marker::LockedExclusive, N>,
    ) {
        match self {
            Unlocker::UnlockAll => {
                node_ref.unlock_exclusive();
            }
            Unlocker::RetainLock(node_to_retain_lock) => {
                if *node_to_retain_lock != node_ref {
                    node_ref.unlock_exclusive();
                }
            }
        }
    }
}

fn split_leaf<K, V>(
    left_leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    key_to_insert: OwnedThinArc<K>,
    value_to_insert: OwnedThinPtr<V>,
) -> (
    SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    OwnedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    OwnedThinArc<K>,
    EntryLocation<K, V>,
)
where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    let right_leaf: OwnedThinPtr<LeafNode<K, V>> = OwnedThinPtr::new(LeafNode::<K, V>::new());
    let right_leaf = OwnedNodeRef::from_leaf_ptr(right_leaf).assume_unlocked();
    let right_leaf = right_leaf.lock_exclusive();
    // update sibling pointers
    if let Some(next_leaf) = left_leaf.next_leaf() {
        // if we have a right sibling, it's new sibling is right_leaf
        let next_leaf = next_leaf.lock_exclusive();
        right_leaf
            .next_leaf
            .store(next_leaf.to_shared_leaf_ptr(), Ordering::Release);
        next_leaf
            .prev_leaf
            .store(right_leaf.to_shared_leaf_ptr(), Ordering::Release);
        next_leaf.unlock_exclusive();
    } else {
        right_leaf.next_leaf.clear(Ordering::Release);
    }
    // the split nodes always point to one another
    left_leaf
        .next_leaf
        .store(right_leaf.to_shared_leaf_ptr(), Ordering::Release);
    right_leaf
        .prev_leaf
        .store(left_leaf.to_shared_leaf_ptr(), Ordering::Release);

    let mut temp_leaf_vec: SmallVec<LeafTempArray<(OwnedThinArc<K>, OwnedThinPtr<V>)>> =
        SmallVec::new();

    let mut maybe_key_to_insert = Some(key_to_insert);
    let mut maybe_value_to_insert = Some(value_to_insert);

    let mut index_of_new_entry = 0;
    left_leaf.storage.drain().for_each(|(k, v)| {
        if let Some(ref key_to_insert) = maybe_key_to_insert {
            if **key_to_insert < *k {
                temp_leaf_vec.push((
                    maybe_key_to_insert.take().unwrap(),
                    maybe_value_to_insert.take().unwrap(),
                ));
                index_of_new_entry = temp_leaf_vec.len() - 1;
            }
        }
        temp_leaf_vec.push((k, v));
    });

    if let Some(_) = maybe_key_to_insert {
        temp_leaf_vec.push((
            maybe_key_to_insert.take().unwrap(),
            maybe_value_to_insert.take().unwrap(),
        ));
        index_of_new_entry = temp_leaf_vec.len() - 1;
    }

    let leaf_containing_new_entry = match index_of_new_entry / (KV_IDX_CENTER + 1) {
        0 => left_leaf,
        1 => right_leaf.share(),
        _ => {
            unreachable!();
        }
    };
    index_of_new_entry = index_of_new_entry % (KV_IDX_CENTER + 1);
    let entry_location = EntryLocation {
        leaf: leaf_containing_new_entry,
        index: index_of_new_entry,
    };

    right_leaf
        .storage
        .extend(temp_leaf_vec.drain(KV_IDX_CENTER + 1..));

    left_leaf.storage.extend(temp_leaf_vec.drain(..));

    // this clone is necessary because the key is moved into the parent
    // it's just a reference count increment, so it's relatively cheap
    let split_key = right_leaf.storage.clone_key(0);
    (left_leaf, right_leaf, split_key, entry_location)
}

pub fn insert_into_leaf_after_splitting<K, V>(
    mut search_stack: SearchDequeue<K, V>,
    key_to_insert: OwnedThinArc<K>,
    value_to_insert: OwnedThinPtr<V>,
) where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    debug_println!("insert_into_leaf_after_splitting");
    let left_leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();
    debug_assert!(left_leaf.num_keys() == MAX_KEYS_PER_NODE);
    let (left, right, split_key, _) = split_leaf(left_leaf, key_to_insert, value_to_insert);
    debug_assert!(left.num_keys() + right.num_keys() == MAX_KEYS_PER_NODE + 1);

    insert_into_parent(search_stack, left, split_key, right, Unlocker::UnlockAll);
}

pub fn insert_into_leaf_after_splitting_returning_leaf_with_new_entry<K, V>(
    mut search_stack: SearchDequeue<K, V>,
    key_to_insert: OwnedThinArc<K>,
    value_to_insert: OwnedThinPtr<V>,
) -> EntryLocation<K, V>
where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    let left_leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();
    let (left, right, split_key, entry_location) =
        split_leaf(left_leaf, key_to_insert, value_to_insert);
    insert_into_parent(
        search_stack,
        left,
        split_key,
        right,
        Unlocker::RetainLock(entry_location.leaf),
    );
    entry_location
}

fn insert_into_parent<K, V, N: marker::NodeType>(
    mut search_stack: SearchDequeue<K, V>,
    left: SharedNodeRef<K, V, marker::LockedExclusive, N>,
    split_key: OwnedThinArc<K>,
    right: OwnedNodeRef<K, V, marker::LockedExclusive, N>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    debug_println!("insert_into_parent");
    // if the parent is the root, we need to create a top of tree
    if search_stack.peek_lowest().is_root() {
        insert_into_new_top_of_tree(
            search_stack.pop_lowest().assert_root().assert_exclusive(),
            left,
            split_key,
            right,
            unlocker,
        );
    } else {
        unlocker.unlock(left);
        let mut parent = search_stack
            .pop_lowest()
            .assert_internal()
            .assert_exclusive();

        if parent.num_keys() < MAX_KEYS_PER_NODE {
            debug_println!(
                "{:?} inserting split key {:?} for {:?} into existing parent",
                parent.node_ptr(),
                unsafe { &*split_key },
                right.node_ptr()
            );
            let right_shared = right.share();
            parent.insert(split_key, right.into_ptr());
            parent.unlock_exclusive();
            unlocker.unlock(right_shared);

            // we may still have a root / top of tree node on the stack
            search_stack.drain().for_each(|n| {
                unlocker.unlock(n.assert_exclusive());
            });
        } else {
            insert_into_internal_node_after_splitting(
                search_stack,
                parent,
                split_key,
                right,
                unlocker,
            );
        }
    }
}

fn insert_into_internal_node_after_splitting<K, V, ChildType: marker::NodeType>(
    parent_stack: SearchDequeue<K, V>,
    old_internal_node: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    split_key: OwnedThinArc<K>,
    new_child: OwnedNodeRef<K, V, marker::LockedExclusive, ChildType>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    let new_child_shared = new_child.share();
    debug_println!("insert_into_internal_node_after_splitting");
    let new_internal_node = OwnedNodeRef::from_internal_ptr(OwnedThinPtr::new(
        InternalNode::<K, V>::new(old_internal_node.height()),
    ))
    .assume_unlocked()
    .lock_exclusive();

    let mut temp_keys_vec: SmallVec<InternalKeyTempArray<OwnedThinArc<K>>> = SmallVec::new();
    let mut temp_children_vec: SmallVec<InternalChildTempArray<OwnedThinPtr<NodeHeader>>> =
        SmallVec::new();

    let new_key_index = old_internal_node
        .storage
        .binary_search_keys(split_key.deref())
        .unwrap_either();

    let is_last_element = new_key_index == old_internal_node.storage.num_keys();

    let mut split_key = Some(split_key);
    let mut new_child = Some(new_child);

    for (i, key) in old_internal_node.storage.iter_keys_owned().enumerate() {
        if i == new_key_index {
            temp_keys_vec.push(split_key.take().unwrap());
        }
        temp_keys_vec.push(key);
    }

    for (i, child) in old_internal_node.storage.iter_children_owned().enumerate() {
        if i == new_key_index + 1 {
            temp_children_vec.push(new_child.take().unwrap().into_ptr());
        }
        temp_children_vec.push(child);
    }

    if is_last_element {
        temp_keys_vec.push(split_key.take().unwrap());
        temp_children_vec.push(new_child.take().unwrap().into_ptr());
    }

    old_internal_node.storage.truncate();

    new_internal_node
        .storage
        .extend_children(temp_children_vec.drain(KV_IDX_CENTER + 1..));
    new_internal_node
        .storage
        .extend_keys(temp_keys_vec.drain(KV_IDX_CENTER + 1..));

    let new_split_key = temp_keys_vec.pop().unwrap();

    old_internal_node
        .storage
        .extend_children(temp_children_vec.drain(..));
    old_internal_node
        .storage
        .extend_keys(temp_keys_vec.drain(..));

    unlocker.unlock(new_child_shared);

    insert_into_parent(
        parent_stack,
        old_internal_node,
        new_split_key,
        new_internal_node,
        Unlocker::UnlockAll,
    );
}

fn insert_into_new_top_of_tree<K, V, N: marker::NodeType>(
    root: SharedNodeRef<K, V, marker::LockedExclusive, marker::Root>,
    left: SharedNodeRef<K, V, marker::LockedExclusive, N>,
    split_key: OwnedThinArc<K>,
    right: OwnedNodeRef<K, V, marker::LockedExclusive, N>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey + ?Sized,
    V: crate::tree::BTreeValue + ?Sized,
{
    debug_println!("insert_into_new_top_of_tree");
    let new_top_of_tree = OwnedNodeRef::from_internal_ptr(OwnedThinPtr::new(
        InternalNode::<K, V>::new(left.height().one_level_higher()),
    ))
    .assume_unlocked()
    .lock_exclusive();
    let new_top_of_tree_shared = new_top_of_tree.share();

    let left_owned = root
        .top_of_tree
        .swap(new_top_of_tree.into_ptr(), Ordering::AcqRel)
        .unwrap();

    let right_shared = right.share();

    new_top_of_tree_shared.storage.push_extra_child(left_owned);
    new_top_of_tree_shared
        .storage
        .push(split_key, right.into_ptr());

    root.unlock_exclusive(); // the root lock is never retained
    new_top_of_tree_shared.unlock_exclusive();
    unlocker.unlock(left);
    unlocker.unlock(right_shared);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_types::ORDER;
    use crate::pointers::{OwnedThinArc, OwnedThinPtr};
    use crate::qsbr::qsbr_reclaimer;

    fn create_full_leaf() -> OwnedNodeRef<usize, str, marker::LockedExclusive, marker::Leaf> {
        let leaf = OwnedNodeRef::from_leaf_ptr(OwnedThinPtr::new(LeafNode::<usize, str>::new()))
            .assume_unlocked()
            .lock_exclusive();
        for i in 1..ORDER {
            leaf.storage.push(
                OwnedThinArc::new(i * 2),
                OwnedThinPtr::new_from_str(&format!("value{}", i * 2)),
            );
        }
        leaf
    }
    fn get_key_for_index(index: usize) -> usize {
        (index + 1) * 2
    }

    #[test]
    fn test_split_leaf_entry_positioning() {
        qsbr_reclaimer().register_thread();

        // Case 1: Insert at start of left node (key = 1)
        {
            let leaf = create_full_leaf();
            let key_to_insert = OwnedThinArc::new(1);
            let value_to_insert = OwnedThinPtr::new_from_str("value1");
            let (left, right, split_key, entry_location) =
                split_leaf(leaf.share(), key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.into_ptr(), left.into_ptr());
            assert_eq!(entry_location.index, 0);

            // Verify node contents
            assert_eq!(*left.storage.get_key(0), 1);
            assert_eq!(*right.storage.get_key(0), *split_key);

            // Verify sibling pointers
            assert_eq!(
                left.next_leaf.load_shared(Ordering::Relaxed).unwrap(),
                right.to_shared_leaf_ptr(),
            );
            assert_eq!(
                right.prev_leaf.load_shared(Ordering::Relaxed).unwrap(),
                left.to_shared_leaf_ptr(),
            );

            left.unlock_exclusive();
            right.unlock_exclusive();
        }

        // Case 2: Insert at end of left node (just before split point)
        {
            let leaf = create_full_leaf();
            let mid_point = KV_IDX_CENTER;
            let mid_point_key = get_key_for_index(mid_point) - 1;
            let key_to_insert = OwnedThinArc::new(mid_point_key);
            let value_to_insert = OwnedThinPtr::new_from_str(&format!("value{}", mid_point_key));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf.share(), key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.into_ptr(), left.into_ptr());
            assert_eq!(entry_location.index, mid_point);

            // Verify node contents
            assert_eq!(*left.storage.get_key(mid_point), mid_point_key);
            assert_eq!(*right.storage.get_key(0), *split_key);

            // Verify left node size
            assert_eq!(left.storage.num_keys(), KV_IDX_CENTER + 1);

            left.unlock_exclusive();
            right.unlock_exclusive();
        }

        // Case 3: Insert at start of right node (just after split point)
        {
            let leaf = create_full_leaf();
            let mid_point = KV_IDX_CENTER;
            let mid_point_key = get_key_for_index(mid_point) + 1;
            let key_to_insert = OwnedThinArc::new(mid_point_key);
            let value_to_insert = OwnedThinPtr::new_from_str(&format!("value{}", mid_point_key));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf.share(), key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.into_ptr(), right.share().into_ptr());
            assert_eq!(entry_location.index, 0);

            // Verify node contents
            assert_eq!(*right.storage.get_key(0), mid_point_key);
            assert_eq!(*split_key, mid_point_key);

            // Verify left node size
            assert_eq!(left.storage.num_keys(), KV_IDX_CENTER + 1);

            left.unlock_exclusive();
            right.unlock_exclusive();
        }

        // Case 4: Insert at end of right node
        {
            let leaf = create_full_leaf();
            let key_to_insert = OwnedThinArc::new(ORDER * 2);
            let value_to_insert = OwnedThinPtr::new_from_str(&format!("value{}", ORDER * 2));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf.share(), key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.into_ptr(), right.share().into_ptr());
            let right_size = right.storage.num_keys();
            assert_eq!(entry_location.index, right_size - 1);

            // Verify node contents
            assert_eq!(*right.storage.get_key(right_size - 1), ORDER * 2);
            assert_ne!(*split_key, ORDER * 2); // Split key should be first key in right node

            // Verify node sizes
            assert_eq!(left.storage.num_keys(), KV_IDX_CENTER + 1);
            assert!(right_size > 0);

            left.unlock_exclusive();
            right.unlock_exclusive();
        }

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }
}
