use std::cmp::Ordering;
use std::marker::PhantomData;

use crate::cursor::Cursor;
use crate::reference::ValueRef;
use crate::tree::{BTreeKey, BTreeValue};
use crate::BTree;

pub trait IterDirection<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    fn advance(cursor: &mut Cursor<K, V>);
    fn start(cursor: &mut Cursor<K, V>);
    fn compare_key(current_cursor_key: &K, boundary_key: &K) -> Ordering;
}

pub struct ForwardIterDirection<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    phantom: PhantomData<(*mut K, *mut V)>,
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> IterDirection<K, V>
    for ForwardIterDirection<K, V>
{
    fn advance(cursor: &mut Cursor<K, V>) {
        cursor.move_next();
    }

    fn start(cursor: &mut Cursor<K, V>) {
        cursor.seek_to_start();
    }

    fn compare_key(current_cursor_key: &K, boundary_key: &K) -> Ordering {
        current_cursor_key.cmp(boundary_key)
    }
}
pub struct BackwardIterDirection<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> {
    phantom: PhantomData<(*mut K, *mut V)>,
}
impl<K: BTreeKey + ?Sized, V: BTreeValue + ?Sized> IterDirection<K, V>
    for BackwardIterDirection<K, V>
{
    fn advance(cursor: &mut Cursor<K, V>) {
        cursor.move_prev();
    }

    fn start(cursor: &mut Cursor<K, V>) {
        cursor.seek_to_end();
    }

    fn compare_key(current_cursor_key: &K, boundary_key: &K) -> Ordering {
        current_cursor_key.cmp(boundary_key).reverse()
    }
}

pub struct BTreeIterator<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, D: IterDirection<K, V>> {
    cursor: Option<Cursor<'a, K, V>>,
    tree: &'a BTree<K, V>,
    phantom: PhantomData<D>,
}

impl<'a, K: BTreeKey + ?Sized, V: BTreeValue + ?Sized, D: IterDirection<K, V>>
    BTreeIterator<'a, K, V, D>
{
    pub fn new(tree: &'a BTree<K, V>) -> Self {
        BTreeIterator {
            cursor: None,
            tree,
            phantom: PhantomData,
        }
    }
}

pub type ForwardBTreeIterator<'a, K, V> = BTreeIterator<'a, K, V, ForwardIterDirection<K, V>>;
pub type BackwardBTreeIterator<'a, K, V> = BTreeIterator<'a, K, V, BackwardIterDirection<K, V>>;

impl<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> Iterator
    for BTreeIterator<'a, K, V, D>
{
    type Item = ValueRef<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = if self.cursor.is_none() {
            let mut cursor = self.tree.cursor();
            D::start(&mut cursor);
            self.cursor = Some(cursor);
            self.cursor.as_mut().unwrap()
        } else {
            let cursor = self.cursor.as_mut().unwrap();
            D::advance(cursor);
            cursor
        };
        match cursor.current() {
            Some(entry) => Some(entry.into_value()),
            None => None,
        }
    }
}

pub struct RangeBTreeIterator<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> {
    start: Option<&'a K>,
    end: Option<&'a K>,
    tree: &'a BTree<K, V>,
    direction: PhantomData<D>,
    cursor: Option<Cursor<'a, K, V>>,
}

#[cfg(test)]
impl<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> RangeBTreeIterator<'a, K, V, D> {
    pub fn new(tree: &'a BTree<K, V>, start: Option<&'a K>, end: Option<&'a K>) -> Self {
        RangeBTreeIterator {
            start,
            end,
            tree,
            direction: PhantomData,
            cursor: None,
        }
    }
}

impl<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> Iterator
    for RangeBTreeIterator<'a, K, V, D>
{
    type Item = ValueRef<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = if self.cursor.is_none() {
            let mut cursor = self.tree.cursor();
            match self.start {
                Some(start) => {
                    cursor.seek(start);
                }
                None => D::start(&mut cursor),
            }
            self.cursor = Some(cursor);
            self.cursor.as_ref().unwrap().current()
        } else {
            let cursor = self.cursor.as_mut().unwrap();
            D::advance(cursor);
            cursor.current()
        };
        if let Some(entry) = entry {
            if self.end.is_some()
                && D::compare_key(entry.key(), self.end.unwrap()) != Ordering::Less
            {
                return None;
            }
            return Some(entry.into_value());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use btree_macros::qsbr_test;

    use crate::array_types::ORDER;
    use crate::iter::{BackwardIterDirection, ForwardIterDirection, RangeBTreeIterator};
    use crate::pointers::OwnedThinArc;
    use crate::qsbr_reclaimer;
    use crate::tree::BTree;
    use std::sync::Barrier;
    use thin::QsOwned;

    #[qsbr_test]
    fn test_forward_iterator() {
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3; // Multiple leaves
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test forward iteration
        let mut iter = tree.iter();
        for i in 0..n {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);
    }

    #[qsbr_test]
    fn test_backward_iterator() {
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3; // Multiple leaves
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test backward iteration
        let mut iter = tree.iter_rev();
        for i in (0..n).rev() {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);
    }

    #[qsbr_test]
    fn test_range_iterator_forward() {
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3;
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test various range scenarios
        let start = n / 3;
        let end = 2 * n / 3;

        // Test with both bounds
        let mut iter = RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(
            &tree,
            Some(&start),
            Some(&end),
        );
        for i in start..end {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        // Test with only start bound
        let mut iter =
            RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(&tree, Some(&start), None);
        for i in start..n {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        // Test with only end bound
        let mut iter =
            RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(&tree, None, Some(&end));
        for i in 0..end {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);
    }

    #[qsbr_test]
    fn test_range_iterator_backward() {
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3;
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test various range scenarios
        let start = n / 3;
        let end = 2 * n / 3;

        // Test with both bounds
        let mut iter = RangeBTreeIterator::<_, _, BackwardIterDirection<_, _>>::new(
            &tree,
            Some(&end),
            Some(&start),
        );
        for i in (start + 1..=end).rev() {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        // Test with only start bound
        let mut iter =
            RangeBTreeIterator::<_, _, BackwardIterDirection<_, _>>::new(&tree, Some(&start), None);
        for i in (0..=start).rev() {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        // Test with only end bound
        let mut iter =
            RangeBTreeIterator::<_, _, BackwardIterDirection<_, _>>::new(&tree, None, Some(&end));
        for i in (end + 1..n).rev() {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);
    }

    #[qsbr_test]
    fn test_empty_tree_iterators() {
        let tree = BTree::<usize, String>::new();

        // Test all iterator types on empty tree
        assert_eq!(tree.iter().next(), None);
        assert_eq!(tree.iter_rev().next(), None);

        let mut range_iter =
            RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(&tree, Some(&0), Some(&10));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = RangeBTreeIterator::<_, _, BackwardIterDirection<_, _>>::new(
            &tree,
            Some(&0),
            Some(&10),
        );
        assert_eq!(range_iter.next(), None);
    }

    #[qsbr_test]
    fn test_single_element_iterators() {
        let tree = BTree::<usize, String>::new();
        tree.insert(OwnedThinArc::new(1), QsOwned::new("value1".to_string()));

        // Test forward iterator
        let mut iter = tree.iter();
        assert_eq!(*iter.next().unwrap(), "value1");
        assert_eq!(iter.next(), None);

        // Test backward iterator
        let mut iter = tree.iter_rev();
        assert_eq!(*iter.next().unwrap(), "value1");
        assert_eq!(iter.next(), None);

        // Test range iterator with bounds including the element
        let mut range_iter =
            RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(&tree, Some(&0), Some(&2));
        assert_eq!(*range_iter.next().unwrap(), "value1");
        assert_eq!(range_iter.next(), None);

        // Test range iterator with bounds excluding the element
        let mut range_iter =
            RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(&tree, Some(&2), Some(&3));
        assert_eq!(range_iter.next(), None);
    }

    #[qsbr_test]
    fn test_iterator_with_different_tree_sizes() {
        // Test with a single leaf
        let tree = BTree::<usize, String>::new();
        for i in 0..ORDER {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }
        let mut iter = tree.iter();
        for i in 0..ORDER {
            assert_eq!(*iter.next().unwrap(), format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        // Test with multiple leaves
        let tree = BTree::<usize, String>::new();
        for i in 0..ORDER * 3 {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }
        let mut iter = tree.iter();
        for i in 0..ORDER * 3 {
            assert_eq!(*iter.next().unwrap(), format!("value{}", i));
        }
        assert_eq!(iter.next(), None);
    }

    #[qsbr_test]
    fn test_concurrent_iteration() {
        let tree = BTree::<usize, String>::new();
        let n = ORDER * 3;

        // Insert test data
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        let barrier = Barrier::new(2);
        let tree_ref = &tree;
        let barrier_ref = &barrier;

        std::thread::scope(|s| {
            // First thread iterates forward
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut iter = tree_ref.iter();
                    barrier_ref.wait();
                    for i in 0..n {
                        assert_eq!(*iter.next().unwrap(), format!("value{}", i));
                        std::thread::yield_now();
                    }
                    assert_eq!(iter.next(), None);
                }
            });

            // Second thread iterates backward
            s.spawn(move || {
                let _guard = qsbr_reclaimer().guard();
                {
                    let mut iter = tree_ref.iter_rev();
                    barrier_ref.wait();
                    for i in (0..n).rev() {
                        assert_eq!(*iter.next().unwrap(), format!("value{}", i));
                        std::thread::yield_now();
                    }
                    assert_eq!(iter.next(), None);
                }
            });
        });
    }

    #[qsbr_test]
    fn test_range_iterator_edge_cases() {
        let tree = BTree::<usize, String>::new();
        let n = ORDER * 3;

        // Insert test data
        for i in 0..n {
            tree.insert(OwnedThinArc::new(i), QsOwned::new(format!("value{}", i)));
        }

        // Test range with start > end
        let start = n / 2;
        let end = n / 4;
        let mut iter = RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(
            &tree,
            Some(&start),
            Some(&end),
        );
        assert_eq!(iter.next(), None);

        // Test range with start == end
        let mid = n / 2;
        let mut iter = RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(
            &tree,
            Some(&mid),
            Some(&mid),
        );
        assert_eq!(iter.next(), None);

        // Test range with start > max value
        let too_large = n + 1;
        let mut iter = RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(
            &tree,
            Some(&too_large),
            None,
        );
        assert_eq!(iter.next(), None);

        // Test range with end < min value
        let too_small = 0;
        let mut iter = RangeBTreeIterator::<_, _, ForwardIterDirection<_, _>>::new(
            &tree,
            None,
            Some(&too_small),
        );
        assert_eq!(iter.next(), None);
    }
}
