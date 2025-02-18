use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    array_types::ORDER,
    qsbr::{qsbr_pool, qsbr_reclaimer},
    BTree, BTreeKey, BTreeValue,
};

pub fn bulk_update_from_sorted_kv_pairs_parallel<K: BTreeKey, V: BTreeValue>(
    sorted_kv_pairs: Vec<(K, V)>,
    tree: &BTree<K, V>,
) {
    let pool = qsbr_pool();
    pool.install(|| {
        sorted_kv_pairs
            .into_par_iter()
            .chunks(ORDER * 8) // the cursors can still overlap leaves, but that shouldn't cause problems
            .for_each(|chunk| {
                let mut cursor = tree.cursor_mut();
                for (key, value) in chunk {
                    cursor.seek(&key);
                    cursor.update_value(Box::new(value));
                }
            });
    });
    pool.broadcast(|_| {
        qsbr_reclaimer().mark_current_thread_quiescent();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qsbr::qsbr_reclaimer;

    #[test]
    fn test_bulk_update() {
        qsbr_reclaimer().register_thread();

        // Create a tree with initial values
        let tree = BTree::<usize, String>::new();
        let num_elements = ORDER * 4;

        // Insert initial values
        for i in 0..num_elements {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
        }

        // Create updates with modified values
        let updates: Vec<(usize, String)> = (0..num_elements)
            .map(|i| (i, format!("updated_value{}", i)))
            .collect();

        // Perform bulk update
        bulk_update_from_sorted_kv_pairs_parallel(updates.clone(), &tree);

        // Verify all values were updated correctly
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, value)) in updates.iter().enumerate() {
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), value);

            if i < updates.len() - 1 {
                cursor.move_next();
            }
        }

        // Verify we can't move past the last element
        cursor.move_next();
        assert!(cursor.current().is_none());

        // Verify tree invariants
        tree.check_invariants();

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
