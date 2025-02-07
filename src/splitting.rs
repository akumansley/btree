use crate::array_types::{
    InternalChildTempArray, InternalKeyTempArray, LeafTempArray, KV_IDX_CENTER, MAX_KEYS_PER_NODE,
};
use crate::debug_println;
use crate::graceful_pointers::GracefulArc;
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node_ptr::{marker, NodePtr, NodeRef};
use crate::search_dequeue::SearchDequeue;
use crate::util::UnwrapEither;
use smallvec::SmallVec;

pub fn insert_into_leaf_after_splitting<K, V>(
    mut search_stack: SearchDequeue<K, V>,
    key_to_insert: GracefulArc<K>,
    value_to_insert: *mut V,
) where
    K: crate::tree::BTreeKey,
    V: crate::tree::BTreeValue,
{
    debug_println!("insert_into_leaf_after_splitting");
    let leaf = search_stack.pop_lowest().assert_leaf().assert_exclusive();
    let new_leaf = NodeRef::from_leaf_unlocked(LeafNode::<K, V>::new()).lock_exclusive();

    let mut temp_leaf_vec: SmallVec<LeafTempArray<(GracefulArc<K>, *mut V)>> = SmallVec::new();

    let mut key_to_insert = Some(key_to_insert);
    let mut value_to_insert = Some(value_to_insert);

    leaf.storage.drain().for_each(|(k, v)| {
        if let Some(ref key) = key_to_insert {
            if **key < *k {
                temp_leaf_vec.push((
                    key_to_insert.take().unwrap(),
                    value_to_insert.take().unwrap(),
                ));
            }
        }
        temp_leaf_vec.push((k, v));
    });

    if let Some(_) = &key_to_insert {
        temp_leaf_vec.push((
            key_to_insert.take().unwrap(),
            value_to_insert.take().unwrap(),
        ));
    }

    new_leaf
        .storage
        .extend(temp_leaf_vec.drain(KV_IDX_CENTER + 1..));

    leaf.storage.extend(temp_leaf_vec.drain(..));

    // this clone is necessary because the key is moved into the parent
    // it's just a reference count increment, so it's relatively cheap
    let split_key = new_leaf.storage.get_key(0).clone_and_increment_ref_count();

    insert_into_parent(search_stack, leaf, split_key, new_leaf);
}

pub fn insert_into_parent<K, V, N: marker::NodeType>(
    mut search_stack: SearchDequeue<K, V>,
    left: NodeRef<K, V, marker::Exclusive, N>,
    split_key: GracefulArc<K>,
    right: NodeRef<K, V, marker::Exclusive, N>,
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
        );
    } else {
        // need to unlock left?
        left.unlock_exclusive();
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
            right.unlock_exclusive();
            // we may still have a root / top of tree node on the stack
            // TODO: complicate the logic of has_capacity_for_modification to handle this
            search_stack.drain().for_each(|n| {
                n.assert_exclusive().unlock_exclusive();
            });
        } else {
            insert_into_internal_node_after_splitting(search_stack, parent, split_key, right);
        }
    }
}

pub fn insert_into_internal_node_after_splitting<K, V, ChildType: marker::NodeType>(
    parent_stack: SearchDequeue<K, V>,
    old_internal_node: NodeRef<K, V, marker::Exclusive, marker::Internal>,
    split_key: GracefulArc<K>,
    new_child: NodeRef<K, V, marker::Exclusive, ChildType>,
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

    new_child.unlock_exclusive();

    insert_into_parent(
        parent_stack,
        old_internal_node,
        new_split_key,
        new_internal_node,
    );
}

pub fn insert_into_new_top_of_tree<K, V, N: marker::NodeType>(
    mut root: NodeRef<K, V, marker::Exclusive, marker::Root>,
    left: NodeRef<K, V, marker::Exclusive, N>,
    split_key: GracefulArc<K>,
    right: NodeRef<K, V, marker::Exclusive, N>,
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

    root.top_of_tree = new_top_of_tree.node_ptr();
    root.unlock_exclusive();
    new_top_of_tree.unlock_exclusive();
    left.unlock_exclusive();
    right.unlock_exclusive();
}
