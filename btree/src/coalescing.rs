use crate::array_types::{MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE};
use crate::internal_node::InternalNodeInner;
use crate::leaf_node::LeafNodeInner;
use crate::pointers::node_ref::{marker, SharedNodeRef};
use crate::pointers::OwnedNodeRef;
use crate::search_dequeue::SearchDequeue;
use crate::sync::Ordering;
use crate::tree::{BTreeKey, BTreeValue};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UnderfullNodePosition {
    Leftmost,
    Other,
}
pub fn coalesce_or_redistribute_leaf_node<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    mut search_stack: SearchDequeue<K, V>,
) {
    debug_println!("coalesce_or_redistribute_leaf_node");
    let locked_underfull_leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();

    if search_stack.peek_lowest().is_root() {
        debug_println!("not coalescing or redistributing leaf at top of tree");
        locked_underfull_leaf.unlock_exclusive();
        debug_assert!(search_stack.is_empty());
        return;
    }

    let locked_parent = search_stack
        .peek_lowest()
        .assert_internal()
        .assert_exclusive();

    let (full_leaf_node, node_position) =
        locked_parent.get_neighbor_of_underfull_leaf(locked_underfull_leaf);

    let locked_full_leaf = full_leaf_node.lock_exclusive_jittered();

    if locked_underfull_leaf.num_keys() + locked_full_leaf.num_keys() <= MAX_KEYS_PER_NODE {
        match node_position {
            UnderfullNodePosition::Other => {
                coalesce_into_left_leaf_from_right_neighbor(
                    locked_full_leaf,
                    locked_underfull_leaf,
                    locked_parent,
                    search_stack,
                );
            }
            UnderfullNodePosition::Leftmost => {
                coalesce_into_left_leaf_from_right_neighbor(
                    locked_underfull_leaf,
                    locked_full_leaf,
                    locked_parent,
                    search_stack,
                );
            }
        }
    } else {
        search_stack.pop_highest_until(locked_parent).for_each(|n| {
            n.assert_exclusive().unlock_exclusive();
        });
        redistribute_into_underfull_leaf_from_neighbor(
            locked_underfull_leaf,
            locked_full_leaf,
            node_position,
            locked_parent,
        );
    }
}

pub fn coalesce_into_left_leaf_from_right_neighbor<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    left_leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    right_leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    search_stack: SearchDequeue<K, V>,
) {
    debug_println!("coalesce_into_left_leaf_from_right_neighbor");
    debug_assert!(search_stack.peek_lowest() == parent);

    LeafNodeInner::move_from_right_neighbor_into_left_node(parent, right_leaf, left_leaf);
    left_leaf.unlock_exclusive();
    if parent.num_keys() < MIN_KEYS_PER_NODE {
        coalesce_or_redistribute_internal_node(search_stack);
    } else {
        parent.unlock_exclusive();
    }
}

pub fn coalesce_or_redistribute_internal_node<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    mut search_stack: SearchDequeue<K, V>,
) {
    debug_println!("coalesce_or_redistribute_internal_node");
    let underfull_internal = search_stack
        .pop_lowest()
        .assert_exclusive()
        .assert_internal();

    if search_stack.peek_lowest().is_root() {
        adjust_top_of_tree(search_stack, underfull_internal.erase_node_type());
        return;
    }
    let parent = search_stack
        .peek_lowest()
        .assert_internal()
        .assert_exclusive();

    let (full_internal, node_position) = parent.get_neighboring_internal_node(underfull_internal);

    let full_internal = full_internal.lock_exclusive();
    if full_internal.num_keys() + underfull_internal.num_keys() < MAX_KEYS_PER_NODE {
        match node_position {
            UnderfullNodePosition::Other => {
                coalesce_into_left_internal_from_right_neighbor(
                    full_internal,
                    underfull_internal,
                    parent,
                    search_stack,
                );
            }
            UnderfullNodePosition::Leftmost => {
                coalesce_into_left_internal_from_right_neighbor(
                    underfull_internal,
                    full_internal,
                    parent,
                    search_stack,
                );
            }
        }
    } else {
        search_stack.pop_highest_until(parent).for_each(|n| {
            n.assert_exclusive().unlock_exclusive();
        });
        redistribute_into_underfull_internal_from_neighbor(
            underfull_internal,
            full_internal,
            node_position,
            parent,
        );
    }
}

pub fn coalesce_into_left_internal_from_right_neighbor<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    left_internal: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    right_internal: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    search_stack: SearchDequeue<K, V>,
) {
    debug_println!("coalesce_into_left_internal_from_right_neighbor");
    InternalNodeInner::move_from_right_neighbor_into_left_node(
        parent,
        right_internal,
        left_internal,
    );
    left_internal.unlock_exclusive();
    if parent.num_keys() < MIN_KEYS_PER_NODE {
        coalesce_or_redistribute_internal_node(search_stack);
    } else {
        parent.unlock_exclusive();
    }
}

pub fn redistribute_into_underfull_internal_from_neighbor<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    underfull_internal: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    full_internal: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
    node_position: UnderfullNodePosition,
    parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
) {
    debug_println!("redistribute_into_underfull_internal_from_neighbor");
    match node_position {
        UnderfullNodePosition::Other => {
            InternalNodeInner::move_last_to_front_of(full_internal, underfull_internal, parent);
        }
        UnderfullNodePosition::Leftmost => {
            InternalNodeInner::move_first_to_end_of(full_internal, underfull_internal, parent);
        }
    }
    parent.unlock_exclusive();
    full_internal.unlock_exclusive();
    underfull_internal.unlock_exclusive();
}

pub fn redistribute_into_underfull_leaf_from_neighbor<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
>(
    underfull_leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    full_leaf: SharedNodeRef<K, V, marker::LockedExclusive, marker::Leaf>,
    node_position: UnderfullNodePosition,
    parent: SharedNodeRef<K, V, marker::LockedExclusive, marker::Internal>,
) {
    debug_println!("redistribute_into_underfull_leaf_from_neighbor");
    match node_position {
        UnderfullNodePosition::Other => {
            LeafNodeInner::move_last_to_front_of(full_leaf, underfull_leaf, parent);
        }
        UnderfullNodePosition::Leftmost => {
            LeafNodeInner::move_first_to_end_of(full_leaf, underfull_leaf, parent);
        }
    }
    parent.unlock_exclusive();
    full_leaf.unlock_exclusive();
    underfull_leaf.unlock_exclusive();
}

pub fn adjust_top_of_tree<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    mut search_stack: SearchDequeue<K, V>,
    top_of_tree: SharedNodeRef<K, V, marker::LockedExclusive, marker::Unknown>,
) {
    debug_println!("adjust_top_of_tree");
    if top_of_tree.is_internal() {
        debug_println!("top_of_tree is an internal node");
        let top_of_tree = top_of_tree.assert_internal();
        if top_of_tree.num_keys() == 0 {
            let root = search_stack.pop_lowest().assert_root().assert_exclusive();
            debug_println!("top_of_tree is an internal node with one child");
            let new_top_of_tree = top_of_tree.storage.remove_only_child();

            let old_top_of_tree = root
                .top_of_tree
                .swap(new_top_of_tree, Ordering::Release)
                .unwrap();

            let old_top_of_tree =
                OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(
                    old_top_of_tree,
                )
                .assert_exclusive()
                .assert_internal();

            old_top_of_tree.retire();

            old_top_of_tree.storage.truncate();
            drop(old_top_of_tree);
            root.unlock_exclusive();
        } else {
            debug_println!("top_of_tree is an internal node with multiple children -- we're done");
            top_of_tree.unlock_exclusive();
        }
    } else {
        top_of_tree.unlock_exclusive();
    }
}
