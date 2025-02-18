use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::array_types::ORDER;
use crate::graceful_pointers::GracefulArc;
use crate::internal_node::InternalNode;
use crate::leaf_node::LeafNode;
use crate::node::NodeHeader;
use crate::node_ptr::NodePtr;
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
    let mut rng = rand::thread_rng();
    let base_chunk_size = (ORDER as f64 * target_utilization) as usize;
    let jitter = (ORDER as f64 * jitter_range * (2.0 * rng.gen::<f64>() - 1.0)) as usize;
    base_chunk_size
        .saturating_add(jitter)
        .clamp(ORDER / 4, ORDER - 1)
}

fn construct_leaf_level<K: BTreeKey, V: BTreeValue>(
    pairs_iter: impl Iterator<Item = (K, V)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(AliasableBox<LeafNode<K, V>>, Option<GracefulArc<K>>)> {
    let mut leaves: Vec<(AliasableBox<LeafNode<K, V>>, Option<GracefulArc<K>>)> = Vec::new();
    let mut pairs_iter = pairs_iter.peekable();
    let mut is_leftmost = true;

    while pairs_iter.peek().is_some() {
        let chunk_size = calculate_chunk_size(target_utilization, jitter_range);

        // Create a new leaf node
        let leaf = AliasableBox::new(LeafNode::new());
        let leaf_inner = unsafe { &mut *leaf.inner.get() };

        // Get the first key of this chunk to use as the split key (None if leftmost)
        let maybe_split_key = if is_leftmost {
            is_leftmost = false;
            None
        } else if let Some((key, _)) = pairs_iter.peek() {
            Some(GracefulArc::new(key.clone()))
        } else {
            None
        };

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

        // Link to previous leaf if any
        if let Some((prev_leaf, _)) = leaves.last() {
            leaf_inner
                .prev_leaf
                .store(prev_leaf.as_ptr(), std::sync::atomic::Ordering::Release);
            unsafe {
                (&mut *(*prev_leaf).inner.get())
                    .next_leaf
                    .store(leaf.as_ptr(), std::sync::atomic::Ordering::Release);
            }
        }

        leaves.push((leaf, maybe_split_key));
    }

    leaves
}

fn construct_internal_level<K: BTreeKey, V: BTreeValue>(
    children_with_split_keys: Vec<(*mut NodeHeader, Option<GracefulArc<K>>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> Vec<(*mut InternalNode<K, V>, Option<GracefulArc<K>>)> {
    let mut internal_nodes = Vec::new();
    let mut children_iter = children_with_split_keys.into_iter().peekable();
    let mut is_leftmost = true;

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

        // Get the split key for this node (None if leftmost)
        let maybe_split_key = if is_leftmost {
            is_leftmost = false;
            None
        } else {
            first_child_split_key
        };

        // Store first child
        node_inner
            .storage
            .push_extra_child(NodePtr::from_raw_ptr(first_child));

        // Add remaining children in this chunk
        for _ in 1..chunk_size {
            if let Some((child, child_split_key)) = children_iter.next() {
                // Use the child's split key as the split key in this node
                if let Some(split_key) = child_split_key {
                    node_inner
                        .storage
                        .push(split_key, NodePtr::from_raw_ptr(child));
                }
            } else {
                break;
            }
        }

        internal_nodes.push((node_ptr, maybe_split_key));
    }

    internal_nodes
}

fn construct_tree_from_leaves<K: BTreeKey, V: BTreeValue>(
    leaves: Vec<(AliasableBox<LeafNode<K, V>>, Option<GracefulArc<K>>)>,
    target_utilization: f64,
    jitter_range: f64,
) -> *mut NodeHeader {
    // If we only have one leaf, it becomes the root
    if leaves.len() == 1 {
        return leaves[0].0.as_ptr() as *mut NodeHeader;
    }

    // Convert leaves to NodeHeaders for the first level
    let mut current_level: Vec<(*mut NodeHeader, Option<GracefulArc<K>>)> = leaves
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

    current_level[0].0
}

pub fn bulk_load_from_sorted_kv_pairs<K: BTreeKey, V: BTreeValue>(
    sorted_kv_pairs: Vec<(K, V)>,
) -> BTree<K, V> {
    let target_utilization = 0.69;
    let jitter_range = 0.1;

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
        root_inner
            .top_of_tree
            .store(root_node, std::sync::atomic::Ordering::Release);
    }
    tree
}

pub fn bulk_load_from_sorted_kv_pairs_parallel<K: BTreeKey, V: BTreeValue>(
    sorted_kv_pairs: Vec<(K, V)>,
) -> BTree<K, V> {
    let target_utilization = 0.69;
    let jitter_range = 0.1;

    let chunks = sorted_kv_pairs.into_par_iter().chunks(ORDER * 8);
    let leaves: Vec<(AliasableBox<LeafNode<K, V>>, Option<GracefulArc<K>>)> = chunks
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
                                .store(first_leaf.as_ptr(), std::sync::atomic::Ordering::Release);
                            first_leaf
                                .get_inner()
                                .prev_leaf
                                .store(last_leaf.as_ptr(), std::sync::atomic::Ordering::Release);
                        }
                    }
                }
                acc.extend(leaves);
                acc
            },
        );

    let root_node = construct_tree_from_leaves(leaves, target_utilization, jitter_range);

    let tree = BTree::new();
    unsafe {
        let root_inner = &mut *tree.root.inner.get();
        root_inner
            .top_of_tree
            .store(root_node, std::sync::atomic::Ordering::Release);
    }
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
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), value);

            if i < pairs.len() - 1 {
                cursor.move_next();
            }
        }

        cursor.move_next();
        assert!(cursor.current().is_none());

        // Verify tree invariants
        tree.check_invariants();

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
