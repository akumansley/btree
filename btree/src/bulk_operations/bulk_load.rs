use crate::array_types::{MIN_KEYS_PER_NODE, ORDER};
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::NodeHeader;
use crate::pointers::{marker, OwnedNodeRef, OwnedThinArc};
use crate::sync::Ordering;
use crate::tree::{BTree, BTreeKey, BTreeValue};
use qsbr::qsbr_pool;
use rand::Rng;
use rayon::prelude::*;
use thin::QsOwned;

fn calculate_chunk_size(
    target_utilization: f64,
    jitter_range: f64,
    remaining_pairs: usize,
) -> usize {
    let mut rng = rand::rng();
    let base_chunk_size = (ORDER as f64 * target_utilization) as usize;
    let jitter = (ORDER as f64 * jitter_range * (2.0 * rng.random::<f64>() - 1.0)) as usize;

    // Calculate desired size with jitter
    let desired_size = base_chunk_size.saturating_add(jitter);

    if remaining_pairs < ORDER {
        // If we have less than ORDER items left, put them all in one node
        remaining_pairs
    } else if remaining_pairs < 2 * ORDER {
        // If we have less than 2*ORDER items left,
        // split them in half to ensure both nodes meet minimum
        remaining_pairs / 2
    } else {
        // Normal case: clamp the size between MIN_KEYS_PER_NODE and ORDER-1
        // while ensuring we don't leave too few items for the next node
        let max_allowed = remaining_pairs - MIN_KEYS_PER_NODE;
        desired_size.clamp(MIN_KEYS_PER_NODE, max_allowed.min(ORDER - 1))
    }
}

fn construct_leaf_level<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    pairs_iter: impl ExactSizeIterator<Item = (OwnedThinArc<K>, QsOwned<V>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(QsOwned<LeafNode<K, V>>, OwnedThinArc<K>)> {
    let mut leaves: Vec<(QsOwned<LeafNode<K, V>>, OwnedThinArc<K>)> = Vec::new();
    let mut pairs_iter = pairs_iter.peekable();

    while pairs_iter.peek().is_some() {
        let chunk_size = calculate_chunk_size(target_utilization, jitter_range, pairs_iter.len());

        // Create a new leaf node
        let leaf = QsOwned::new(LeafNode::new());
        let leaf_inner = unsafe { &mut *leaf.inner.get() };

        // Add pairs for this chunk
        for _ in 0..chunk_size {
            if let Some((key, value)) = pairs_iter.next() {
                leaf_inner.storage.push(key, value);
            } else {
                break;
            }
        }

        // Get the first key of this leaf to use as the split key
        let key = leaf_inner.storage.clone_key(0);

        // Link to previous leaf if any
        if let Some((prev_leaf, _)) = leaves.last() {
            leaf_inner
                .prev_leaf
                .store(prev_leaf.share(), Ordering::Release);
            unsafe {
                (*(*prev_leaf).inner.get())
                    .next_leaf
                    .store(leaf.share(), Ordering::Release);
            }
        }

        leaves.push((leaf, key));
    }

    leaves
}

fn construct_internal_level<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    children_with_split_keys: Vec<(QsOwned<NodeHeader>, OwnedThinArc<K>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(QsOwned<InternalNode<K, V>>, OwnedThinArc<K>)> {
    let mut internal_nodes = Vec::new();
    let mut children_iter = children_with_split_keys.into_iter().peekable();

    // Process children in chunks to create internal nodes
    while children_iter.peek().is_some() {
        let chunk_size =
            calculate_chunk_size(target_utilization, jitter_range, children_iter.len());
        let (first_child, _) = children_iter.peek().unwrap();
        let height = (*first_child).height().one_level_higher();
        let node_ptr = QsOwned::new(InternalNode::<K, V>::new(height));
        let node_inner = unsafe { &mut *node_ptr.inner.get() };

        // Get the first child and its split key
        let (first_child, first_child_split_key) = children_iter.next().unwrap();

        let new_node_split_key = first_child_split_key;

        // Store first child
        node_inner.storage.push_extra_child(first_child);

        // Add remaining children in this chunk
        for _ in 1..chunk_size {
            if let Some((child, child_split_key)) = children_iter.next() {
                node_inner.storage.push(child_split_key, child);
            } else {
                break;
            }
        }
        // If we've only got one more, we don't have enough for another internal node, so just add it here
        // we clamp the chunk size to ORDER-2 so we should have room
        if children_iter.len() == 1 {
            let (child, split_key) = children_iter.next().unwrap();
            node_inner.storage.push(split_key, child);
        }

        internal_nodes.push((node_ptr, new_node_split_key));
    }

    internal_nodes
}

fn construct_tree_from_leaves<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    mut leaves: Vec<(QsOwned<LeafNode<K, V>>, OwnedThinArc<K>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> QsOwned<NodeHeader> {
    // If we only have one leaf, it becomes the root
    if leaves.len() == 1 {
        return unsafe { leaves.remove(0).0.cast::<NodeHeader>() };
    }

    // Convert leaves to NodeHeaders for the first level
    let mut current_level: Vec<(QsOwned<NodeHeader>, OwnedThinArc<K>)> = leaves
        .into_iter()
        .map(|(node, split_key)| (unsafe { node.cast::<NodeHeader>() }, split_key))
        .collect();

    // Keep building levels until we have a single node
    while current_level.len() > 1 {
        let internal_nodes =
            construct_internal_level::<K, V>(current_level, target_utilization, jitter_range);

        current_level = internal_nodes
            .into_iter()
            .map(|(node, split_key)| (unsafe { node.cast::<NodeHeader>() }, split_key))
            .collect();
    }

    let (top_of_tree, extra_key) = current_level.pop().unwrap();
    drop(extra_key);
    top_of_tree
}

pub fn bulk_load_from_sorted_kv_pairs<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    sorted_kv_pairs: Vec<(OwnedThinArc<K>, QsOwned<V>)>,
) -> BTree<K, V> {
    let target_utilization = 0.69;
    let jitter_range = 0.1;

    // Store the number of pairs for setting the length
    let num_pairs = sorted_kv_pairs.len();

    // Create leaf nodes with target utilization
    let leaves = construct_leaf_level(
        sorted_kv_pairs.into_iter(),
        target_utilization,
        jitter_range,
    );

    // Build the tree from leaves up
    let root_node = construct_tree_from_leaves(leaves, target_utilization, jitter_range);

    // Create and return the tree
    let tree = BTree::new();
    unsafe {
        let root_inner = &mut *tree.root.inner.get();
        let old_leaf = root_inner
            .top_of_tree
            .swap(root_node, Ordering::Release)
            .unwrap();
        OwnedNodeRef::drop_immediately(
            OwnedNodeRef::<K, V, marker::Unknown, marker::Unknown>::from_unknown_node_ptr(old_leaf)
                .assert_leaf(),
        );
    }

    // Set the tree length to the number of pairs
    tree.root.len.store(num_pairs, Ordering::Relaxed);

    tree
}

pub fn bulk_load_from_sorted_kv_pairs_parallel<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    sorted_kv_pairs: Vec<(OwnedThinArc<K>, QsOwned<V>)>,
) -> BTree<K, V> {
    let target_utilization = 0.69;
    let jitter_range = 0.1;

    // Store the number of pairs for setting the length
    let num_pairs = sorted_kv_pairs.len();

    let pool = qsbr_pool();
    let leaves: Vec<(QsOwned<LeafNode<K, V>>, OwnedThinArc<K>)> = pool.install(|| {
        let chunks = sorted_kv_pairs.into_par_iter().chunks(ORDER * 8);
        chunks
            .map(|chunk| construct_leaf_level(chunk.into_iter(), target_utilization, jitter_range))
            .reduce(
                || Vec::new(),
                |mut acc, mut leaves| {
                    if let Some((last_leaf, _)) = acc.last_mut() {
                        if let Some((first_leaf, _)) = leaves.first_mut() {
                            unsafe {
                                last_leaf
                                    .get_inner()
                                    .next_leaf
                                    .store(first_leaf.share(), Ordering::Relaxed);
                                first_leaf
                                    .get_inner()
                                    .prev_leaf
                                    .store(last_leaf.share(), Ordering::Relaxed);
                            }
                        }
                    }
                    acc.extend(leaves);
                    acc
                },
            )
    });

    let root_node = construct_tree_from_leaves(leaves, target_utilization, jitter_range);

    let tree = BTree::new();
    unsafe {
        let root_inner = &mut *tree.root.inner.get();
        root_inner.top_of_tree.store(root_node, Ordering::Release);
    }

    // Set the tree length to the number of pairs
    tree.root.len.store(num_pairs, Ordering::Relaxed);

    tree
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;
    use qsbr::qsbr_reclaimer;

    #[test]
    #[cfg(not(miri))]
    fn test_bulk_load() {
        qsbr_reclaimer().register_thread();

        // Create sorted key-value pairs
        let mut pairs = Vec::new();
        let mut pairs_for_comparison = Vec::new();
        for i in 0..10_000 {
            pairs.push((OwnedThinArc::new(i), QsOwned::new(format!("value{}", i))));
            pairs_for_comparison.push((i, format!("value{}", i)));
        }

        // Bulk load the tree
        let tree: BTree<i32, String> = bulk_load_from_sorted_kv_pairs_parallel(pairs);

        // Verify the tree contains all pairs in order
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, value)) in pairs_for_comparison.iter().enumerate() {
            let entry_from_get = tree
                .get(&key)
                .expect(format!("key not found: {}", key).as_str());
            let entry = cursor.current().unwrap();
            assert_eq!(key, entry.key());
            assert_eq!(value, entry.value());
            assert_eq!(value, entry_from_get.deref());

            if i < pairs_for_comparison.len() - 1 {
                cursor.move_next();
            }
        }

        cursor.move_next();
        assert!(cursor.current().is_none());

        // Verify tree invariants
        tree.check_invariants();

        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }
}
