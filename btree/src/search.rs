use crate::hybrid_latch::LockError;
use crate::pointers::node_ref::marker::LockState;
use crate::pointers::node_ref::{marker, SharedDiscriminatedNode, SharedNodeRef};
use crate::search_dequeue::SearchDequeue;
use crate::tree::{BTreeKey, BTreeValue, ModificationType};
use crate::util::retry;

pub fn get_last_leaf_shared_using_optimistic_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> Result<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_last_child()).assume_unlocked(),
        |node| node.lock_shared_if_not_retired(),
        |node| node.unlock_shared(),
    )
}
pub fn get_last_leaf_shared_using_shared_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> SharedNodeRef<K, V, marker::LockedShared, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_last_child()).assume_unlocked(),
        |node| node.lock_shared(),
    )
}

pub fn get_first_leaf_shared_using_optimistic_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> Result<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_first_child()).assume_unlocked(),
        |node| node.lock_shared_if_not_retired(),
        |node| node.unlock_shared(),
    )
}

pub fn get_first_leaf_shared_using_shared_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> SharedNodeRef<K, V, marker::LockedShared, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_first_child()).assume_unlocked(),
        |node| node.lock_shared(),
    )
}

fn get_leaf_shared_using_optimistic_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> Result<SharedNodeRef<K, V, marker::LockedShared, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.find_child(search_key)).assume_unlocked(),
        |node| node.lock_shared_if_not_retired(),
        |node| node.unlock_shared(),
    )
}

pub fn get_leaf_shared_using_optimistic_search_with_fallback<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> SharedNodeRef<K, V, marker::LockedShared, marker::Leaf> {
    match get_leaf_shared_using_optimistic_search(root, search_key) {
        Ok(leaf) => leaf,
        Err(_) => get_leaf_shared_using_shared_search(root, search_key),
    }
}

pub fn do_optimistic_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
    LeafLockState: LockState,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    descend: impl Fn(
        SharedNodeRef<K, V, marker::Optimistic, marker::Internal>,
    ) -> SharedNodeRef<K, V, marker::Unlocked, marker::Unknown>,
    lock_leaf: impl Fn(
        SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>,
    ) -> Result<SharedNodeRef<K, V, LeafLockState, marker::Leaf>, LockError>,
    unlock_leaf: impl Fn(
        SharedNodeRef<K, V, LeafLockState, marker::Leaf>,
    ) -> SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>,
) -> Result<SharedNodeRef<K, V, LeafLockState, marker::Leaf>, ()> {
    retry::<_, _, _, 3>(|| {
        let locked_root = root.lock_optimistic()?;
        let top_of_tree =
            SharedNodeRef::from_unknown_node_ptr(locked_root.top_of_tree()).assume_unlocked();

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_optimistic()?.assert_internal();
            prev_node.unlock_optimistic()?;
            current_node = descend(locked_current_node);
            prev_node = locked_current_node.erase_node_type();
        }
        let leaf = lock_leaf(current_node.assert_leaf()).map_err(|_| ())?;
        match prev_node.unlock_optimistic() {
            Ok(_) => Ok(leaf),
            Err(_) => {
                unlock_leaf(leaf);
                Err(())
            }
        }
    })
}

fn do_shared_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, LeafLockState: LockState>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    descend: impl Fn(
        SharedNodeRef<K, V, marker::LockedShared, marker::Internal>,
    ) -> SharedNodeRef<K, V, marker::Unlocked, marker::Unknown>,
    lock_leaf: impl Fn(
        SharedNodeRef<K, V, marker::Unlocked, marker::Leaf>,
    ) -> SharedNodeRef<K, V, LeafLockState, marker::Leaf>,
) -> SharedNodeRef<K, V, LeafLockState, marker::Leaf> {
    let locked_root = root.lock_shared();
    let top_of_tree =
        SharedNodeRef::from_unknown_node_ptr(locked_root.top_of_tree()).assume_unlocked();
    let mut prev_node = locked_root.erase_node_type();
    let mut current_node = top_of_tree;
    while current_node.is_internal() {
        let locked_current_node = current_node.lock_shared().assert_internal();
        prev_node.unlock_shared();
        current_node = descend(locked_current_node);
        prev_node = locked_current_node.erase_node_type();
    }
    let leaf = lock_leaf(current_node.assert_leaf());
    prev_node.unlock_shared();
    leaf
}

pub fn get_leaf_shared_using_shared_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> SharedNodeRef<K, V, marker::LockedShared, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.find_child(search_key)).assume_unlocked(),
        |node| node.lock_shared(),
    )
}

fn get_leaf_exclusively_using_optimistic_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> Result<SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.find_child(search_key)).assume_unlocked(),
        |node| node.lock_exclusive_if_not_retired(),
        |node| node.unlock_exclusive(),
    )
}

fn get_leaf_exclusively_using_shared_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.find_child(search_key)).assume_unlocked(),
        |node| node.lock_exclusive(),
    )
}

pub fn get_leaf_exclusively_using_optimistic_search_with_fallback<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf> {
    match get_leaf_exclusively_using_optimistic_search(root, search_key) {
        Ok(leaf) => leaf,
        Err(_) => get_leaf_exclusively_using_shared_search(root, search_key),
    }
}

pub fn get_leaf_exclusively_using_exclusive_search<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    locked_root: SharedNodeRef<K, V, marker::LockedExclusive, marker::Root>,
    search_key: &K,
    modification_type: ModificationType,
) -> SearchDequeue<K, V> {
    let mut search = SearchDequeue::new();

    search.push_node_on_bottom(locked_root);
    let top_of_tree =
        SharedNodeRef::from_unknown_node_ptr(locked_root.top_of_tree()).assume_unlocked();
    let top_of_tree = top_of_tree.lock_exclusive();
    search.push_node_on_bottom(top_of_tree);
    match top_of_tree.force() {
        SharedDiscriminatedNode::Leaf(leaf) => {
            if leaf.has_capacity_for_modification_as_top_of_tree(modification_type) {
                search
                    .pop_highest()
                    .assert_root()
                    .assert_exclusive()
                    .unlock_exclusive();
            }
        }
        SharedDiscriminatedNode::Internal(internal) => {
            if internal.has_capacity_for_modification_as_top_of_tree(modification_type) {
                search
                    .pop_highest()
                    .assert_root()
                    .assert_exclusive()
                    .unlock_exclusive();
            }
        }
        _ => unreachable!(),
    }

    let mut current_node = search.peek_lowest();

    while current_node.is_internal() {
        let current_exclusive = current_node.assert_internal().assert_exclusive();
        let found_child =
            SharedNodeRef::from_unknown_node_ptr(current_exclusive.find_child(search_key))
                .assume_unlocked();
        let found_child = found_child.lock_exclusive();
        search.push_node_on_bottom(found_child);
        match found_child.force() {
            SharedDiscriminatedNode::Leaf(leaf) => {
                if leaf.has_capacity_for_modification(modification_type) {
                    search.pop_highest_until(leaf).for_each(|n| {
                        n.assert_exclusive().unlock_exclusive();
                    });
                }
            }
            SharedDiscriminatedNode::Internal(internal) => {
                if internal.has_capacity_for_modification(modification_type) {
                    search.pop_highest_until(internal).for_each(|n| {
                        n.assert_exclusive().unlock_exclusive();
                    });
                }
            }
            _ => unreachable!(),
        }
        current_node = search.peek_lowest();
    }
    search
}

pub fn get_first_leaf_exclusively_using_optimistic_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> Result<SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_first_child()).assume_unlocked(),
        |node| node.lock_exclusive_if_not_retired(),
        |node| node.unlock_exclusive(),
    )
}

pub fn get_first_leaf_exclusively_using_shared_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_first_child()).assume_unlocked(),
        |node| node.lock_exclusive(),
    )
}

pub fn get_last_leaf_exclusively_using_optimistic_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> Result<SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>, ()> {
    do_optimistic_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_last_child()).assume_unlocked(),
        |node| node.lock_exclusive_if_not_retired(),
        |node| node.unlock_exclusive(),
    )
}

pub fn get_last_leaf_exclusively_using_shared_search<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    root: SharedNodeRef<K, V, marker::Unlocked, marker::Root>,
) -> SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf> {
    do_shared_search(
        root,
        |node| SharedNodeRef::from_unknown_node_ptr(node.get_last_child()).assume_unlocked(),
        |node| node.lock_exclusive(),
    )
}
