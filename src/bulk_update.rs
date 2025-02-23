use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    array_types::ORDER,
    graceful_pointers::GracefulArc,
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

pub fn bulk_insert_or_update_from_sorted_kv_pairs_parallel<
    K: BTreeKey,
    V: BTreeValue,
    F: Fn(*mut V) -> *mut V + Send + Sync,
>(
    sorted_kv_pairs: Vec<(K, V)>,
    update_fn: &F,
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
                    cursor.insert_or_update(
                        GracefulArc::new(key),
                        Box::into_raw(Box::new(value)),
                        update_fn,
                    );
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

    #[test]
    fn test_bulk_insert_or_update() {
        qsbr_reclaimer().register_thread();

        // Create a tree with initial values (even numbers)
        let tree = BTree::<usize, String>::new();
        let num_elements = ORDER * 4;

        // Insert initial values (even numbers)
        for i in 0..num_elements {
            if i % 2 == 0 {
                tree.insert(Box::new(i), Box::new(format!("value{}", i)));
            }
        }

        // Create a mix of updates and inserts
        // - Even numbers: update existing values
        // - Odd numbers: insert new values
        let entries: Vec<(usize, String)> = (0..num_elements)
            .map(|i| {
                if i % 2 == 0 {
                    // Update: append "_updated" to existing values
                    (i, format!("value{}_updated", i))
                } else {
                    // Insert: new values for odd numbers
                    (i, format!("value{}_new", i))
                }
            })
            .collect();

        // Define update function that appends "_updated" to existing values
        let update_fn = |old_value: *mut String| {
            let old_string = unsafe { Box::from_raw(old_value) };
            Box::into_raw(Box::new(format!("{}_updated", old_string)))
        };

        // Perform bulk insert/update
        tree.bulk_insert_or_update_parallel(entries.clone(), &update_fn);

        // Verify all values are correct
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, expected_value)) in entries.iter().enumerate() {
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), expected_value);

            if i < entries.len() - 1 {
                cursor.move_next();
            }
        }

        // Verify we can't move past the last element
        cursor.move_next();
        assert!(cursor.current().is_none());

        // Verify the total number of elements
        let mut count = 0;
        cursor.seek_to_start();
        while cursor.current().is_some() {
            count += 1;
            cursor.move_next();
        }
        assert_eq!(count, num_elements);

        // Verify tree invariants
        tree.check_invariants();

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
