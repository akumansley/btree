/// Policy trait for per-leaf metadata in the B-tree.
///
/// All methods are associated functions (no `&self`) so the compiler can
/// monomorphize and inline every call site. The policy type `P` is carried
/// as a type parameter only (`PhantomData<P>` on `BTree`); nothing is stored
/// at runtime.
///
/// Callbacks receive `&M` for reused nodes (update via interior mutability)
/// and return `M` only for newly created nodes.
pub trait LeafMetadata: 'static {
    type M: Send + Sync + 'static;

    /// Create metadata for a newly created leaf.
    fn new_leaf() -> Self::M;

    /// A leaf is splitting. The original (left) leaf is reused and its
    /// metadata is passed by reference — update it in place if needed.
    /// Returns metadata for the newly created right leaf.
    fn on_split(original: &Self::M) -> Self::M;

    /// Two leaves are merging (right into left). Both are existing nodes;
    /// the left leaf survives and the right is retired after this call.
    /// Update the surviving left leaf's metadata in place if needed.
    fn on_merge(left: &Self::M, right: &Self::M);

    /// An entry is being redistributed between two existing leaves.
    /// Both nodes are reused — update their metadata in place if needed.
    fn on_redistribute(donor: &Self::M, recipient: &Self::M);
}

/// Default no-op policy: no metadata, zero cost.
pub struct NoMetadata;

impl LeafMetadata for NoMetadata {
    type M = ();

    #[inline(always)]
    fn new_leaf() {}

    #[inline(always)]
    fn on_split(_: &()) {}

    #[inline(always)]
    fn on_merge(_: &(), _: &()) {}

    #[inline(always)]
    fn on_redistribute(_: &(), _: &()) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_types::ORDER;
    use crate::tree::BTree;
    use btree_macros::qsbr_test;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    use thin::{QsArc, QsOwned};

    /// Counter-based metadata: each leaf tracks how many times it has been
    /// involved in structural operations. Useful for verifying that the
    /// policy hooks are actually invoked during splits/merges/redistributions.
    struct CounterPolicy;

    impl LeafMetadata for CounterPolicy {
        type M = AtomicUsize;

        fn new_leaf() -> AtomicUsize {
            AtomicUsize::new(0)
        }

        fn on_split(original: &AtomicUsize) -> AtomicUsize {
            // Bump the original (left) leaf's counter in place
            original.fetch_add(1, Relaxed);
            // New right leaf starts at original's pre-bump value + 1
            AtomicUsize::new(original.load(Relaxed))
        }

        fn on_merge(left: &AtomicUsize, right: &AtomicUsize) {
            // Absorb right's counter into left
            left.fetch_add(right.load(Relaxed) + 1, Relaxed);
        }

        fn on_redistribute(donor: &AtomicUsize, recipient: &AtomicUsize) {
            donor.fetch_add(1, Relaxed);
            recipient.fetch_add(1, Relaxed);
        }
    }

    #[qsbr_test]
    fn test_metadata_accessible_via_cursor() {
        let tree = BTree::<usize, usize, CounterPolicy>::new();
        tree.insert(QsArc::new(1), QsOwned::new(100));

        let mut cursor = tree.cursor();
        cursor.seek(&1);
        // The initial leaf gets counter=0 from new_leaf()
        let meta = cursor.leaf_metadata().unwrap();
        assert_eq!(meta.load(Relaxed), 0);
    }

    #[qsbr_test]
    fn test_metadata_survives_splits() {
        let tree = BTree::<usize, usize, CounterPolicy>::new();

        // Insert enough to trigger at least one split
        for i in 0..(ORDER + 1) {
            tree.insert(QsArc::new(i), QsOwned::new(i));
        }

        // The original (left) leaf was split, so on_split bumped its counter
        let mut cursor = tree.cursor();
        cursor.seek_to_start();
        let left_meta = cursor.leaf_metadata().unwrap();
        assert!(left_meta.load(Relaxed) >= 1);

        // The new (right) leaf also got a non-zero counter from on_split's return
        cursor.seek(&ORDER);
        let right_meta = cursor.leaf_metadata().unwrap();
        assert!(right_meta.load(Relaxed) >= 1);
    }

    #[qsbr_test]
    fn test_metadata_survives_merges() {
        let tree = BTree::<usize, usize, CounterPolicy>::new();
        let n = ORDER * 3;

        // Insert then remove to trigger merges
        for i in 0..n {
            tree.insert(QsArc::new(i), QsOwned::new(i));
        }
        // Remove from the front to trigger coalescing
        for i in 0..n {
            tree.remove(&i);
        }
        assert!(tree.is_empty());
    }

    #[qsbr_test]
    fn test_no_metadata_tree_works_unchanged() {
        // Verify the default NoMetadata path still works
        let tree = BTree::<usize, String>::new();
        for i in 0..ORDER * 2 {
            tree.insert(QsArc::new(i), QsOwned::new(format!("v{i}")));
        }
        for i in 0..ORDER * 2 {
            let val = tree.get(&i).unwrap();
            assert_eq!(*val, format!("v{i}"));
        }
        for i in 0..ORDER * 2 {
            tree.remove(&i);
        }
        assert!(tree.is_empty());
    }
}
