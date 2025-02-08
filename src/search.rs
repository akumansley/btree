use crate::debug_println;
use crate::node_ptr::{marker, DiscriminatedNode, NodeRef};
use crate::search_dequeue::SearchDequeue;
use crate::tree::{BTreeKey, BTreeValue, ModificationType};
use crate::util::retry;

pub fn get_leaf_shared_using_optimistic_search<K: BTreeKey, V: BTreeValue>(
    root: NodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> Result<NodeRef<K, V, marker::Shared, marker::Leaf>, ()> {
    retry::<_, _, _, 3>(|| {
        let locked_root = root.lock_optimistic()?;
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_optimistic()?.assert_internal();
            prev_node.unlock_optimistic()?;
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }
        let leaf = current_node.lock_shared();
        Ok(leaf.assert_leaf())
    })
}

pub fn get_leaf_shared_using_shared_search<K: BTreeKey, V: BTreeValue>(
    locked_root: NodeRef<K, V, marker::Shared, marker::Root>,
    search_key: &K,
) -> NodeRef<K, V, marker::Shared, marker::Leaf> {
    let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

    let mut prev_node = locked_root.erase_node_type();
    let mut current_node = top_of_tree;
    while current_node.is_internal() {
        let locked_current_node = current_node.lock_shared().assert_internal();
        prev_node.unlock_shared();
        current_node = NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
        prev_node = locked_current_node.erase_node_type();
    }

    let leaf = current_node.lock_shared().assert_leaf();
    prev_node.unlock_shared();
    leaf
}

pub fn get_leaf_exclusively_using_optimistic_search<K: BTreeKey, V: BTreeValue>(
    root: NodeRef<K, V, marker::Unlocked, marker::Root>,
    search_key: &K,
) -> Result<NodeRef<K, V, marker::Exclusive, marker::Leaf>, ()> {
    retry::<_, _, _, 3>(|| {
        let locked_root = root.lock_optimistic()?;
        let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

        let mut prev_node = locked_root.erase_node_type();
        let mut current_node = top_of_tree;
        while current_node.is_internal() {
            let locked_current_node = current_node.lock_optimistic()?.assert_internal();
            prev_node.unlock_optimistic()?;
            current_node =
                NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
            prev_node = locked_current_node.erase_node_type();
        }

        let leaf = current_node.lock_exclusive().assert_leaf();
        match prev_node.unlock_optimistic() {
            Ok(_) => Ok(leaf),
            Err(_) => {
                leaf.unlock_exclusive();
                Err(())
            }
        }
    })
}

pub fn get_leaf_exclusively_using_shared_search<K: BTreeKey, V: BTreeValue>(
    locked_root: NodeRef<K, V, marker::Shared, marker::Root>,
    search_key: &K,
) -> NodeRef<K, V, marker::Exclusive, marker::Leaf> {
    let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);

    let mut prev_node = locked_root.erase_node_type();
    let mut current_node = top_of_tree;
    while current_node.is_internal() {
        let locked_current_node = current_node.lock_shared().assert_internal();
        prev_node.unlock_shared();
        current_node = NodeRef::from_unknown_node_ptr(locked_current_node.find_child(search_key));
        prev_node = locked_current_node.erase_node_type();
    }

    let leaf = current_node.lock_exclusive().assert_leaf();
    prev_node.unlock_shared();
    leaf
}

pub fn get_leaf_exclusively_using_exclusive_search<K: BTreeKey, V: BTreeValue>(
    locked_root: NodeRef<K, V, marker::Exclusive, marker::Root>,
    search_key: &K,
    modification_type: ModificationType,
) -> SearchDequeue<K, V> {
    let mut search = SearchDequeue::new();

    search.push_node_on_bottom(locked_root);
    let top_of_tree = NodeRef::from_unknown_node_ptr(locked_root.top_of_tree);
    let top_of_tree = top_of_tree.lock_exclusive();
    search.push_node_on_bottom(top_of_tree);
    match top_of_tree.force() {
        DiscriminatedNode::Leaf(leaf) => {
            if leaf.has_capacity_for_modification_as_top_of_tree(modification_type) {
                search
                    .pop_highest()
                    .assert_root()
                    .assert_exclusive()
                    .unlock_exclusive();
            }
        }
        DiscriminatedNode::Internal(internal) => {
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
        let found_child = NodeRef::from_unknown_node_ptr(current_exclusive.find_child(search_key));
        let found_child = found_child.lock_exclusive();
        search.push_node_on_bottom(found_child);
        match found_child.force() {
            DiscriminatedNode::Leaf(leaf) => {
                if leaf.has_capacity_for_modification(modification_type) {
                    search.pop_highest_until(found_child).for_each(|n| {
                        n.assert_exclusive().unlock_exclusive();
                    });
                }
            }
            DiscriminatedNode::Internal(internal) => {
                if internal.has_capacity_for_modification(modification_type) {
                    search.pop_highest_until(found_child).for_each(|n| {
                        n.assert_exclusive().unlock_exclusive();
                    });
                }
            }
            _ => unreachable!(),
        }
        current_node = search.peek_lowest();
    }
    debug_println!(
        "find_leaf {:?} found {:?}",
        search_key,
        search.peek_lowest()
    );
    search
}
