use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::array_types::ORDER;
use crate::graceful_pointers::GracefulArc;
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::NodeHeader;
use crate::node_ptr::NodePtr;
use crate::qsbr::qsbr_pool;
use crate::sync::Ordering;
use crate::tree::{BTree, BTreeKey, BTreeValue};
use rand::Rng;
use rayon::prelude::*;

struct AliasableBox<T> {
    non_null: NonNull<T>,
}

impl<T> AliasableBox<T> {
    fn new(ptr: *mut T) -> Self {
        Self {
            non_null: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
    fn as_ptr(&self) -> *mut T {
        self.non_null.as_ptr()
    }
}

impl<T> Deref for AliasableBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.non_null.as_ptr() }
    }
}
impl<T> DerefMut for AliasableBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.non_null.as_ptr() }
    }
}

unsafe impl<K: BTreeKey, V: BTreeValue> Send for AliasableBox<LeafNode<K, V>> {}

fn calculate_chunk_size(target_utilization: f64, jitter_range: f64) -> usize {
    let mut rng = rand::rng();
    let base_chunk_size = (ORDER as f64 * target_utilization) as usize;
    let jitter = (ORDER as f64 * jitter_range * (2.0 * rng.random::<f64>() - 1.0)) as usize;
    base_chunk_size
        .saturating_add(jitter)
        .clamp(ORDER / 4, ORDER - 2)
}

fn construct_leaf_level<K: BTreeKey, V: BTreeValue>(
    pairs_iter: impl Iterator<Item = (K, V)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(AliasableBox<LeafNode<K, V>>, GracefulArc<K>)> {
    let mut leaves: Vec<(AliasableBox<LeafNode<K, V>>, GracefulArc<K>)> = Vec::new();
    let mut pairs_iter = pairs_iter.peekable();

    while pairs_iter.peek().is_some() {
        let chunk_size = calculate_chunk_size(target_utilization, jitter_range);

        // Create a new leaf node
        let leaf = AliasableBox::new(LeafNode::new());
        let leaf_inner = unsafe { &mut *leaf.inner.get() };

        // Add pairs for this chunk
        for _ in 0..chunk_size {
            if let Some((key, value)) = pairs_iter.next() {
                leaf_inner
                    .storage
                    .push(GracefulArc::new(key), Box::into_raw(Box::new(value)));
            } else {
                break;
            }
        }

        // Get the first key of this leaf to use as the split key
        let key = leaf_inner.storage.get_key(0);
        let split_key = key.clone_and_increment_ref_count();

        // Link to previous leaf if any
        if let Some((prev_leaf, _)) = leaves.last() {
            leaf_inner
                .prev_leaf
                .store(prev_leaf.as_ptr(), Ordering::Release);
            unsafe {
                (*(*prev_leaf).inner.get())
                    .next_leaf
                    .store(leaf.as_ptr(), Ordering::Release);
            }
        }

        leaves.push((leaf, split_key));
    }

    leaves
}

fn construct_internal_level<K: BTreeKey, V: BTreeValue>(
    children_with_split_keys: Vec<(*mut NodeHeader, GracefulArc<K>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(*mut InternalNode<K, V>, GracefulArc<K>)> {
    let mut internal_nodes = Vec::new();
    let mut children_iter = children_with_split_keys.into_iter().peekable();

    // Process children in chunks to create internal nodes
    while children_iter.peek().is_some() {
        let chunk_size = calculate_chunk_size(target_utilization, jitter_range);
        let (first_child, _) = children_iter.peek().unwrap();
        let height = unsafe { (**first_child).height().one_level_higher() };
        let node_ptr = InternalNode::<K, V>::new(height);
        let node = unsafe { &mut *node_ptr };
        let node_inner = unsafe { &mut *node.inner.get() };

        // Get the first child and its split key
        let (first_child, first_child_split_key) = children_iter.next().unwrap();

        let new_node_split_key = first_child_split_key;

        // Store first child
        node_inner
            .storage
            .push_extra_child(NodePtr::from_raw_ptr(first_child));

        // Add remaining children in this chunk
        for _ in 1..chunk_size {
            if let Some((child, child_split_key)) = children_iter.next() {
                node_inner
                    .storage
                    .push(child_split_key, NodePtr::from_raw_ptr(child));
            } else {
                break;
            }
        }
        // If we've only got one more, we don't have enough for another internal node, so just add it here
        // we clamp the chunk size to ORDER-2 so we should have room
        if children_iter.len() == 1 {
            let (child, split_key) = children_iter.next().unwrap();
            node_inner
                .storage
                .push(split_key, NodePtr::from_raw_ptr(child));
        }

        internal_nodes.push((node_ptr, new_node_split_key));
    }

    internal_nodes
}

fn construct_tree_from_leaves<K: BTreeKey, V: BTreeValue>(
    leaves: Vec<(AliasableBox<LeafNode<K, V>>, GracefulArc<K>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> *mut NodeHeader {
    // If we only have one leaf, it becomes the root
    if leaves.len() == 1 {
        return leaves[0].0.as_ptr() as *mut NodeHeader;
    }

    // Convert leaves to NodeHeaders for the first level
    let mut current_level: Vec<(*mut NodeHeader, GracefulArc<K>)> = leaves
        .into_iter()
        .map(|(node, split_key)| (node.as_ptr() as *mut NodeHeader, split_key))
        .collect();

    // Keep building levels until we have a single node
    while current_level.len() > 1 {
        let internal_nodes =
            construct_internal_level::<K, V>(current_level, target_utilization, jitter_range);

        current_level = internal_nodes
            .into_iter()
            .map(|(node, split_key)| (node as *mut NodeHeader, split_key))
            .collect();
    }

    let (top_of_tree, extra_key) = current_level.pop().unwrap();
    unsafe { extra_key.decrement_ref_count_and_drop_if_zero() };
    top_of_tree
}

pub fn bulk_load_from_sorted_kv_pairs<K: BTreeKey, V: BTreeValue>(
    sorted_kv_pairs: Vec<(K, V)>,
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
        root_inner.top_of_tree.store(root_node, Ordering::Release);
    }

    // Set the tree length to the number of pairs
    tree.root.len.store(num_pairs, Ordering::Relaxed);

    tree
}

pub fn bulk_load_from_sorted_kv_pairs_parallel<K: BTreeKey, V: BTreeValue>(
    sorted_kv_pairs: Vec<(K, V)>,
) -> BTree<K, V> {
    let target_utilization = 0.69;
    let jitter_range = 0.1;

    // Store the number of pairs for setting the length
    let num_pairs = sorted_kv_pairs.len();

    let pool = qsbr_pool();
    let leaves: Vec<(AliasableBox<LeafNode<K, V>>, GracefulArc<K>)> = pool.install(|| {
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
                                    .store(first_leaf.as_ptr(), Ordering::Relaxed);
                                first_leaf
                                    .get_inner()
                                    .prev_leaf
                                    .store(last_leaf.as_ptr(), Ordering::Relaxed);
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
    use super::*;
    use crate::qsbr::qsbr_reclaimer;

    #[test]
    fn test_bulk_load() {
        qsbr_reclaimer().register_thread();

        // Create sorted key-value pairs
        let mut pairs = Vec::new();
        for i in 0..10_000 {
            pairs.push((i, format!("value{}", i)));
        }

        // Bulk load the tree
        let tree = bulk_load_from_sorted_kv_pairs_parallel(pairs.clone());

        // Verify the tree contains all pairs in order
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, value)) in pairs.iter().enumerate() {
            let entry_from_get = tree
                .get(key)
                .expect(format!("key not found: {}", key).as_str());
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), value);
            assert_eq!(entry_from_get, value);

            if i < pairs.len() - 1 {
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
