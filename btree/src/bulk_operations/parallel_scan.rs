use crate::{
    BTree, BTreeKey, BTreeValue, SharedThinPtr,
    node::Height,
    pointers::{
        SharedNodeRef, SharedThinArc,
        marker::{Internal, LockedShared, Root, Unknown, Unlocked},
    },
};
use qsbr::qsbr_pool;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

// maybe avoid allocating into a vec
// test the subranges to see if they're any good

pub fn scan_parallel<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    start_key: Option<SharedThinArc<K>>,
    end_key: Option<SharedThinArc<K>>,
    predicate: impl Fn(&V) -> bool + Sync,
    tree: &BTree<K, V>,
) -> Vec<SharedThinPtr<V>> {
    let root = tree.root.as_node_ref();
    let (lca, start_child_index, end_child_index) =
        find_least_common_ancestor(start_key, end_key, root.lock_shared());
    if lca.is_leaf() {
        // scan the leaf
        let mut result = vec![];
        let lca_leaf = lca.assert_leaf();
        for v in lca_leaf.storage.iter_values() {
            if predicate(&v) {
                result.push(v);
            }
        }
        lca_leaf.unlock_shared();
        result
    } else {
        let internal = lca.assert_internal();
        let separator_keys = try_to_find_n_subranges::<8, K, V>(
            start_key,
            end_key,
            internal,
            start_child_index,
            end_child_index,
        );
        let subranges = separator_keys
            .windows(2)
            .map(|w| (w[0], w[1]))
            .collect::<Vec<_>>();

        qsbr_pool().install(|| {
            subranges
                .into_par_iter()
                .flat_map_iter(|(start, end)| {
                    let mut cursor = tree.cursor();

                    match start {
                        Some(start) => {
                            cursor.seek(&start);
                        }
                        None => {
                            cursor.seek_to_start();
                        }
                    }

                    let mut result = vec![];
                    loop {
                        let maybe_curr = cursor.current();
                        match maybe_curr {
                            Some(curr) => {
                                if let Some(end_key) = end {
                                    if curr.key() >= &end_key {
                                        break;
                                    }
                                }
                                if predicate(curr.value()) {
                                    result.push(curr.value_shared_ptr());
                                }
                                cursor.move_next();
                            }
                            None => {
                                break;
                            }
                        }
                    }
                    result.into_iter()
                })
                .collect()
        })
    }
}

// returns N+1 keys, which can be used to split the range [start_key, end_key) into N subranges
fn try_to_find_n_subranges<const N: usize, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    start_key: Option<SharedThinArc<K>>,
    end_key: Option<SharedThinArc<K>>,
    lca: SharedNodeRef<K, V, LockedShared, Internal>,
    mut start_child_index: usize,
    mut end_child_index: usize,
) -> Vec<Option<SharedThinArc<K>>> {
    let mut prev_split_key_candidates = Vec::new();
    let mut split_key_candidates = Vec::new();
    let mut nodes_under_consideration = Vec::new();
    let mut current_height = lca.height();
    nodes_under_consideration.push(lca);

    loop {
        for (i, node) in nodes_under_consideration.iter().enumerate() {
            let start_index = if i == 0 {
                start_child_index
            } else {
                // the first node from the previous level doesn't have a lower key
                // but every other node should
                split_key_candidates.push(prev_split_key_candidates[i - 1]);
                0
            };
            let end_index = if i == nodes_under_consideration.len() - 1 {
                end_child_index
            } else {
                node.storage.num_keys()
            };

            for i in start_index..end_index {
                split_key_candidates.push(node.storage.get_key(i));
            }
        }

        // cool! we've got enough split keys
        if split_key_candidates.len() >= N {
            break;
        }
        // expand the set of nodes under consideration
        current_height = current_height.one_level_lower();

        // if we're at the leaf level and we still don't have our split keys, we can return
        // what we've got, but we can't go any further
        if current_height == Height::Leaf {
            break;
        }

        let mut new_nodes_under_consideration = Vec::new();

        for (i, node) in nodes_under_consideration.iter().enumerate() {
            let start_index = if i == 0 { start_child_index } else { 0 };
            let end_index = if i == nodes_under_consideration.len() - 1 {
                end_child_index
            } else {
                node.storage.num_children()
            };
            for i in start_index..end_index + 1 {
                let child: SharedNodeRef<K, V, Unlocked, Unknown> =
                    SharedNodeRef::from_unknown_node_ptr(node.storage.get_child(i))
                        .assume_unlocked();
                new_nodes_under_consideration.push(child.assert_internal().lock_shared());
            }
        }
        start_child_index = match start_key {
            Some(key) => new_nodes_under_consideration[0].index_of_child_containing_key(&key),
            None => 0,
        };
        let end_child_node = new_nodes_under_consideration[new_nodes_under_consideration.len() - 1];
        end_child_index = match end_key {
            Some(key) => end_child_node.index_of_child_containing_key(&key),
            None => end_child_node.storage.num_children() - 1,
        };
        for node in nodes_under_consideration {
            node.unlock_shared();
        }
        nodes_under_consideration = new_nodes_under_consideration;
        prev_split_key_candidates = split_key_candidates;
        split_key_candidates = Vec::new();
    }

    for node in nodes_under_consideration {
        node.unlock_shared();
    }

    // we probably have too many split keys, so we need to select N of them

    let num_split_key_candidates = split_key_candidates.len();
    let stride = num_split_key_candidates / N;
    let mut selected_split_keys: Vec<Option<SharedThinArc<K>>> = Vec::new();
    selected_split_keys.push(start_key);
    for i in 1..N {
        selected_split_keys.push(Some(split_key_candidates[i * stride]));
    }
    selected_split_keys.push(end_key);

    selected_split_keys
}

fn find_least_common_ancestor<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    start_key: Option<SharedThinArc<K>>,
    end_key: Option<SharedThinArc<K>>,
    root: SharedNodeRef<K, V, LockedShared, Root>,
) -> (SharedNodeRef<K, V, LockedShared, Unknown>, usize, usize) {
    let top_of_tree: SharedNodeRef<K, V, LockedShared, Unknown> =
        SharedNodeRef::from_unknown_node_ptr(root.top_of_tree())
            .assume_unlocked()
            .lock_shared();

    root.unlock_shared();
    let mut current_node = top_of_tree;
    let mut start_child_index = 0;
    let mut end_child_index = 0;
    while current_node.is_internal() {
        let internal = current_node.assert_internal();
        start_child_index = match start_key {
            Some(key) => internal.index_of_child_containing_key(&key),
            None => 0,
        };

        end_child_index = match end_key {
            Some(key) => internal.index_of_child_containing_key(&key),
            None => internal.storage.num_children() - 1,
        };
        if start_child_index == end_child_index {
            current_node =
                SharedNodeRef::from_unknown_node_ptr(internal.storage.get_child(start_child_index))
                    .assume_unlocked()
                    .lock_shared();
        } else {
            break;
        }
    }
    (current_node, start_child_index, end_child_index)
}

#[cfg(test)]
mod test {
    use btree_macros::qsbr_test;

    use crate::BTree;
    use crate::array_types::ORDER;
    use crate::node::Height;
    use crate::pointers::{OwnedThinArc, OwnedThinPtr};

    use super::*;

    fn make_tree(count: usize) -> BTree<usize, usize> {
        let tree: BTree<usize, usize> = BTree::new();
        for i in 0..count {
            tree.insert(OwnedThinArc::new(i), OwnedThinPtr::new(i));
        }
        tree
    }

    fn assert_subranges<K: BTreeKey + Eq + std::fmt::Debug + ?Sized>(
        result: &[Option<SharedThinArc<K>>],
        expected_start: Option<SharedThinArc<K>>,
        expected_end: Option<SharedThinArc<K>>,
        expected_min_len: usize,
        expected_max_len: usize,
    ) {
        assert!(result.len() >= expected_min_len, "Result length too short");
        assert!(result.len() <= expected_max_len, "Result length too long");

        assert_eq!(
            *result.first().unwrap(),
            expected_start,
            "First key mismatch"
        );
        assert_eq!(*result.last().unwrap(), expected_end, "Last key mismatch");

        if result.len() > 1 {
            for i in 0..result.len() - 1 {
                assert!(result[i] < result[i + 1], "Result is not sorted");
            }
        }
    }

    #[qsbr_test]
    fn test_find_least_common_ancestor() {
        {
            let tree = make_tree(ORDER - 1);

            // should be in the same (only) leaf
            let start_key1 = OwnedThinArc::new(7);
            let end_key1 = OwnedThinArc::new(38);

            let locked_root = tree.root.as_node_ref().lock_shared();

            let (lca, _start_idx, _end_idx) = find_least_common_ancestor(
                Some(start_key1.share()),
                Some(end_key1.share()),
                locked_root,
            );

            assert_eq!(
                lca.height(),
                Height::Leaf,
                "Top of tree is a leaf; the LCA is the leaf"
            );
            lca.unlock_shared();
        }

        {
            let tree = make_tree(ORDER * 2 - 1);

            // should be in the two different leaves
            let start_key = OwnedThinArc::new(11);
            let end_key = OwnedThinArc::new(ORDER + 11);

            let locked_root = tree.root.as_node_ref().lock_shared();

            let (lca, _start_idx, _end_idx) = find_least_common_ancestor(
                Some(start_key.share()),
                Some(end_key.share()),
                locked_root,
            );

            assert_eq!(
                lca.height(),
                Height::Internal(1),
                "LCA should be an internal node"
            );

            lca.unlock_shared();
        }
    }

    #[qsbr_test]
    fn test_try_to_find_n_subranges() {
        const N: usize = 4;

        {
            let tree = make_tree((ORDER * 5) - 5);
            let start_key = OwnedThinArc::new(ORDER / 2); // leaf 0
            let end_key = OwnedThinArc::new(ORDER * 4 + 11); // leaf 4

            let locked_root = tree.root.as_node_ref().lock_shared();
            let (lca_locked_unknown, start_idx, end_idx) = find_least_common_ancestor(
                Some(start_key.share()),
                Some(end_key.share()),
                locked_root,
            );

            let lca_internal = lca_locked_unknown.assert_internal();

            let result = try_to_find_n_subranges::<N, usize, usize>(
                Some(start_key.share()),
                Some(end_key.share()),
                lca_internal,
                start_idx,
                end_idx,
            );

            println!("Scenario 1 Result: {:?}", result);
            assert_subranges(
                &result,
                Some(start_key.share()),
                Some(end_key.share()),
                N + 1,
                N + 1,
            );
        }

        {
            let tree = make_tree(ORDER * 64);
            let start_key = OwnedThinArc::new(ORDER / 2 - 1);
            let end_key = OwnedThinArc::new(ORDER * 64 - 1);

            let locked_root = tree.root.as_node_ref().lock_shared();
            let (lca_locked_unknown, start_idx, end_idx) = find_least_common_ancestor(
                Some(start_key.share()),
                Some(end_key.share()),
                locked_root,
            );

            let lca_internal = lca_locked_unknown.assert_internal();

            let result = try_to_find_n_subranges::<N, usize, usize>(
                Some(start_key.share()),
                Some(end_key.share()),
                lca_internal,
                start_idx,
                end_idx,
            );

            println!("Scenario 2 Result: {:?}", result);
            assert_subranges(
                &result,
                Some(start_key.share()),
                Some(end_key.share()),
                N + 1,
                N + 1,
            );
        }
    }

    #[qsbr_test]
    fn it_parallel_scans() {
        let num_rows = 100_000;
        let tree = make_tree(num_rows);
        let results = scan_parallel(
            Some(OwnedThinArc::new(0).share()),
            Some(OwnedThinArc::new(100_000).share()),
            |v: &usize| v % 100 == 0,
            &tree,
        );

        let expected_values: Vec<usize> = (0..num_rows).filter(|&i| i % 100 == 0).collect();

        for (actual, expected) in results.into_iter().zip(expected_values) {
            assert_eq!(
                actual, expected,
                "The collected results do not match the expected values."
            );
        }
    }
}
