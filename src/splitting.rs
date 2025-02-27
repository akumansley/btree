use crate::sync::Ordering;
use std::ptr;

use crate::array_types::{
    InternalChildTempArray, InternalKeyTempArray, LeafTempArray, KV_IDX_CENTER, MAX_KEYS_PER_NODE,
};
use crate::graceful_pointers::GracefulArc;
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::NodeHeader;
use crate::node_ptr::{marker, NodePtr, NodeRef};
use crate::search_dequeue::SearchDequeue;
use crate::util::UnwrapEither;
use crate::{BTreeKey, BTreeValue};
use smallvec::SmallVec;

pub struct EntryLocation<K: BTreeKey, V: BTreeValue> {
    pub leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    pub index: usize,
}

impl<K: BTreeKey, V: BTreeValue> EntryLocation<K, V> {
    pub fn new(leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>, index: usize) -> Self {
        Self { leaf, index }
    }
}

enum Unlocker<K: BTreeKey, V: BTreeValue> {
    UnlockAll,
    RetainLock(NodeRef<K, V, marker::Exclusive, marker::Leaf>),
}

impl<K: BTreeKey, V: BTreeValue> Unlocker<K, V> {
    fn unlock<N: marker::NodeType>(&self, node_ref: NodeRef<K, V, marker::Exclusive, N>) {
        match self {
            Unlocker::UnlockAll => {
                node_ref.unlock_exclusive();
            }
            Unlocker::RetainLock(node_to_retain_lock) => {
                if node_to_retain_lock.node_ptr() != node_ref.node_ptr() {
                    node_ref.unlock_exclusive();
                }
            }
        }
    }
}

fn split_leaf<K, V>(
    left_leaf: NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    key_to_insert: GracefulArc<K>,
    value_to_insert: *mut V,
) -> (
    NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    NodeRef<K, V, marker::Exclusive, marker::Leaf>,
    GracefulArc<K>,
    EntryLocation<K, V>,
)
where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
{
    let right_leaf = NodeRef::from_leaf_unlocked(LeafNode::<K, V>::new()).lock_exclusive();

    // update sibling pointers
    if let Some(next_leaf) = left_leaf.next_leaf() {
        // if we have a right sibling, it's new sibling is right_leaf
        let next_leaf = next_leaf.lock_exclusive();
        right_leaf
            .next_leaf
            .store(next_leaf.to_raw_leaf_ptr(), Ordering::Relaxed);
        next_leaf
            .prev_leaf
            .store(right_leaf.to_raw_leaf_ptr(), Ordering::Relaxed);
        next_leaf.unlock_exclusive();
    } else {
        right_leaf
            .next_leaf
            .store(ptr::null_mut(), Ordering::Relaxed);
    }
    // the split nodes always point to one another
    left_leaf
        .next_leaf
        .store(right_leaf.to_raw_leaf_ptr(), Ordering::Relaxed);
    right_leaf
        .prev_leaf
        .store(left_leaf.to_raw_leaf_ptr(), Ordering::Relaxed);

    let mut temp_leaf_vec: SmallVec<LeafTempArray<(GracefulArc<K>, *mut V)>> = SmallVec::new();

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
        1 => right_leaf,
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
    let split_key = right_leaf
        .storage
        .get_key(0)
        .clone_and_increment_ref_count();

    (left_leaf, right_leaf, split_key, entry_location)
}

pub fn insert_into_leaf_after_splitting<K, V>(
    mut search_stack: SearchDequeue<K, V>,
    key_to_insert: GracefulArc<K>,
    value_to_insert: *mut V,
) where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
{
    debug_println!("insert_into_leaf_after_splitting");
    let left_leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();
    let (left, right, split_key, _) = split_leaf(left_leaf, key_to_insert, value_to_insert);

    insert_into_parent(search_stack, left, split_key, right, Unlocker::UnlockAll);
}

pub fn insert_into_leaf_after_splitting_returning_leaf_with_new_entry<K, V>(
    mut search_stack: SearchDequeue<K, V>,
    key_to_insert: GracefulArc<K>,
    value_to_insert: *mut V,
) -> EntryLocation<K, V>
where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
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
    left: NodeRef<K, V, marker::Exclusive, N>,
    split_key: GracefulArc<K>,
    right: NodeRef<K, V, marker::Exclusive, N>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
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
        // need to unlock left?
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
            parent.insert(split_key, right.node_ptr());
            parent.unlock_exclusive();
            unlocker.unlock(right);

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
    old_internal_node: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    split_key: GracefulArc<K>,
    new_child: NodeRef<K, V, marker::Exclusive, ChildType>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
{
    debug_println!("insert_into_internal_node_after_splitting");
    let new_internal_node =
        NodeRef::from_internal_unlocked(InternalNode::<K, V>::new(old_internal_node.height()))
            .lock_exclusive();

    let mut temp_keys_vec: SmallVec<InternalKeyTempArray<GracefulArc<K>>> = SmallVec::new();
    let mut temp_children_vec: SmallVec<InternalChildTempArray<NodePtr>> = SmallVec::new();

    let new_key_index = old_internal_node
        .storage
        .binary_search_keys(split_key.as_ref())
        .unwrap_either();

    let is_last_element = new_key_index == old_internal_node.storage.num_keys();

    let mut split_key = Some(split_key);

    for (i, key) in old_internal_node.storage.iter_keys().enumerate() {
        if i == new_key_index {
            temp_keys_vec.push(split_key.take().unwrap());
        }
        temp_keys_vec.push(key);
    }

    for (i, child) in old_internal_node.storage.iter_children().enumerate() {
        if i == new_key_index + 1 {
            temp_children_vec.push(new_child.node_ptr());
        }
        temp_children_vec.push(child);
    }

    if is_last_element {
        temp_keys_vec.push(split_key.take().unwrap());
        temp_children_vec.push(new_child.node_ptr());
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

    unlocker.unlock(new_child);

    insert_into_parent(
        parent_stack,
        old_internal_node,
        new_split_key,
        new_internal_node,
        Unlocker::UnlockAll,
    );
}

fn insert_into_new_top_of_tree<K, V, N: marker::NodeType>(
    root: NodeRef<K, V, marker::Exclusive, marker::Root>,
    left: NodeRef<K, V, marker::Exclusive, N>,
    split_key: GracefulArc<K>,
    right: NodeRef<K, V, marker::Exclusive, N>,
    unlocker: Unlocker<K, V>,
) where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
{
    debug_println!("insert_into_new_top_of_tree");
    let new_top_of_tree = NodeRef::from_internal_unlocked(InternalNode::<K, V>::new(
        left.height().one_level_higher(),
    ))
    .lock_exclusive();

    new_top_of_tree.storage.push_extra_child(left.node_ptr());
    new_top_of_tree.storage.push(split_key, right.node_ptr());

    root.top_of_tree.store(
        new_top_of_tree.to_raw_internal_ptr() as *mut NodeHeader,
        Ordering::Relaxed,
    );
    root.unlock_exclusive(); // the root lock is never retained
    new_top_of_tree.unlock_exclusive();
    unlocker.unlock(left);
    unlocker.unlock(right);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_types::ORDER;
    use crate::qsbr::qsbr_reclaimer;

    fn create_full_leaf() -> NodeRef<usize, String, marker::Exclusive, marker::Leaf> {
        let leaf = NodeRef::from_leaf_unlocked(LeafNode::<usize, String>::new()).lock_exclusive();
        for i in 1..ORDER {
            leaf.storage.push(
                GracefulArc::new(i * 2),
                Box::into_raw(Box::new(format!("value{}", i * 2))),
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
            let key_to_insert = GracefulArc::new(1);
            let value_to_insert = Box::into_raw(Box::new("value1".to_string()));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf, key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.node_ptr(), left.node_ptr());
            assert_eq!(entry_location.index, 0);

            // Verify node contents
            assert_eq!(*left.storage.get_key(0), 1);
            assert_eq!(*right.storage.get_key(0), *split_key);

            // Verify sibling pointers
            assert_eq!(
                left.next_leaf.load(Ordering::Relaxed),
                right.to_raw_leaf_ptr()
            );
            assert_eq!(
                right.prev_leaf.load(Ordering::Relaxed),
                left.to_raw_leaf_ptr()
            );

            left.unlock_exclusive();
            right.unlock_exclusive();
        }

        // Case 2: Insert at end of left node (just before split point)
        {
            let leaf = create_full_leaf();
            let mid_point = KV_IDX_CENTER;
            let mid_point_key = get_key_for_index(mid_point) - 1;
            let key_to_insert = GracefulArc::new(mid_point_key);
            let value_to_insert = Box::into_raw(Box::new(format!("value{}", mid_point_key)));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf, key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.node_ptr(), left.node_ptr());
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
            let key_to_insert = GracefulArc::new(mid_point_key);
            let value_to_insert = Box::into_raw(Box::new(format!("value{}", mid_point_key)));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf, key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.node_ptr(), right.node_ptr());
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
            let key_to_insert = GracefulArc::new(ORDER * 2);
            let value_to_insert = Box::into_raw(Box::new(format!("value{}", ORDER * 2)));
            let (left, right, split_key, entry_location) =
                split_leaf(leaf, key_to_insert, value_to_insert);

            // Verify entry location
            assert_eq!(entry_location.leaf.node_ptr(), right.node_ptr());
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
