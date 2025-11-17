use crate::sync::AtomicUsize;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::sync::Arc;
use thin::{QsArc, QsOwned};

use crate::{array_types::ORDER, BTree, BTreeKey, BTreeValue};
use qsbr::{qsbr_pool, qsbr_reclaimer};

pub fn bulk_update_from_sorted_kv_pairs_parallel<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized>(
    sorted_kv_pairs: Vec<(QsArc<K>, QsOwned<V>)>,
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
                    cursor.update_value(value);
                }
            });
    });
    pool.broadcast(|_| {
        unsafe { qsbr_reclaimer().mark_current_thread_quiescent() };
    });
}

pub fn bulk_insert_or_update_from_sorted_kv_pairs_parallel<
    K: BTreeKey + ?Sized,
    V: BTreeValue + ?Sized,
    F: Fn(QsOwned<V>, QsOwned<V>) -> QsOwned<V> + Send + Sync,
>(
    sorted_kv_pairs: Vec<(QsArc<K>, QsOwned<V>)>,
    update_fn: &F,
    tree: &BTree<K, V>,
) {
    // Create an atomic counter to track the number of new insertions
    let insertion_count = Arc::new(AtomicUsize::new(0));

    let pool = qsbr_pool();
    pool.install(|| {
        sorted_kv_pairs
            .into_par_iter()
            .chunks(ORDER * 8) // the cursors can still overlap leaves, but that shouldn't cause problems
            .for_each(|chunk| {
                let insertion_count = Arc::clone(&insertion_count);
                let mut cursor = tree.cursor_mut();
                for (key, value) in chunk {
                    let was_inserted = cursor.insert_or_modify(key, value, update_fn);

                    // Increment the insertion count if a new key was inserted
                    if was_inserted {
                        insertion_count.fetch_add(1, crate::sync::Ordering::Relaxed);
                    }
                }
            });
    });

    // Update the tree length with the number of new insertions
    let new_insertions = insertion_count.load(crate::sync::Ordering::Relaxed);
    if new_insertions > 0 {
        tree.root
            .len
            .fetch_add(new_insertions, crate::sync::Ordering::Relaxed);
    }

    pool.broadcast(|_| {
        unsafe { qsbr_reclaimer().mark_current_thread_quiescent() };
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use btree_macros::qsbr_test;
    use std::ops::Deref;

    #[qsbr_test]
    #[cfg(not(miri))]
    fn test_bulk_update() {
        // Create a tree with initial values
        let tree = BTree::<usize, String>::new();
        let num_elements = ORDER * 4;

        // Insert initial values
        for i in 0..num_elements {
            tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Create updates with modified values
        let updates_for_comparison: Vec<(usize, String)> = (0..num_elements)
            .map(|i| (i, format!("updated_value{}", i)))
            .collect();
        let updates: Vec<(QsArc<usize>, QsOwned<String>)> = updates_for_comparison
            .iter()
            .map(|(key, value)| (QsArc::new(*key), QsOwned::new(value.clone())))
            .collect();

        // Perform bulk update
        bulk_update_from_sorted_kv_pairs_parallel(updates, &tree);

        // Verify all values were updated correctly
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, value)) in updates_for_comparison.iter().enumerate() {
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), value);

            if i < updates_for_comparison.len() - 1 {
                cursor.move_next();
            }
        }

        // Verify we can't move past the last element
        cursor.move_next();
        assert!(cursor.current().is_none());

        // Verify tree invariants
        tree.check_invariants();
    }

    #[qsbr_test]
    #[cfg(not(miri))]
    fn test_bulk_insert_or_update() {
        // Create a tree with initial values (even numbers)

        use thin::QsArc;
        let tree = BTree::<usize, String>::new();
        let num_elements = ORDER * 4;

        // Insert initial values (even numbers)
        for i in 0..num_elements {
            if i % 2 == 0 {
                tree.insert(QsArc::new(i), QsOwned::new(format!("value{}", i)));
            }
        }

        // Create a mix of updates and inserts
        // - Even numbers: update existing values
        // - Odd numbers: insert new values
        let entries_for_comparison: Vec<(usize, String)> = (0..num_elements)
            .map(|i| {
                if i % 2 == 0 {
                    (i, format!("value{}_updated", i))
                } else {
                    (i, format!("value{}_new", i))
                }
            })
            .collect();
        let entries: Vec<(QsArc<usize>, QsOwned<String>)> = entries_for_comparison
            .iter()
            .map(|(key, value)| (QsArc::new(*key), QsOwned::new(value.clone())))
            .collect();

        // Define update function that appends "_updated" to existing values
        let update_fn = |old_value: QsOwned<String>, _: QsOwned<String>| {
            let old_string = old_value.deref();
            QsOwned::new(format!("{}_updated", old_string))
        };

        // Perform bulk insert/update
        tree.bulk_insert_or_update_parallel(entries, &update_fn);

        // Verify all values are correct
        let mut cursor = tree.cursor();
        cursor.seek_to_start();

        for (i, (key, expected_value)) in entries_for_comparison.iter().enumerate() {
            let entry = cursor.current().unwrap();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), expected_value);

            if i < entries_for_comparison.len() - 1 {
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
    }
}
