use std::cmp::Ordering;
use std::marker::PhantomData;

use crate::cursor::Cursor;
use crate::reference::ValueRef;
use crate::tree::{BTreeKey, BTreeValue};
use crate::BTree;

pub trait IterDirection<K: BTreeKey, V: BTreeValue> {
    fn advance(cursor: &mut Cursor<K, V>);
    fn start(cursor: &mut Cursor<K, V>);
    fn compare_key(current_cursor_key: &K, boundary_key: &K) -> Ordering;
}

pub struct ForwardIterDirection<K: BTreeKey, V: BTreeValue> {
    phantom: PhantomData<(K, V)>,
}
impl<K: BTreeKey, V: BTreeValue> IterDirection<K, V> for ForwardIterDirection<K, V> {
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
pub struct BackwardIterDirection<K: BTreeKey, V: BTreeValue> {
    phantom: PhantomData<(K, V)>,
}
impl<K: BTreeKey, V: BTreeValue> IterDirection<K, V> for BackwardIterDirection<K, V> {
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

pub struct BTreeIterator<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> {
    cursor: Option<Cursor<'a, K, V>>,
    tree: &'a BTree<K, V>,
    phantom: PhantomData<D>,
}

impl<'a, K: BTreeKey, V: BTreeValue, D: IterDirection<K, V>> BTreeIterator<'a, K, V, D> {
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
                Some(start) => cursor.seek(start),
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
    use super::*;
    use crate::array_types::ORDER;
    use crate::qsbr::qsbr_reclaimer;

    #[test]
    fn test_forward_iterator() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3; // Multiple leaves
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
        }

        // Test forward iteration
        let mut iter = tree.iter();
        for i in 0..n {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_backward_iterator() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3; // Multiple leaves
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
        }

        // Test backward iteration
        let mut iter = tree.iter_rev();
        for i in (0..n).rev() {
            let value = iter.next().unwrap();
            assert_eq!(*value, format!("value{}", i));
        }
        assert_eq!(iter.next(), None);

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_range_iterator_forward() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3;
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
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

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_range_iterator_backward() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();

        // Insert test data
        let n = ORDER * 3;
        for i in 0..n {
            tree.insert(Box::new(i), Box::new(format!("value{}", i)));
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

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_empty_tree_iterators() {
        qsbr_reclaimer().register_thread();
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

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }

    #[test]
    fn test_single_element_iterators() {
        qsbr_reclaimer().register_thread();
        let tree = BTree::<usize, String>::new();
        tree.insert(Box::new(1), Box::new("value1".to_string()));

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

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
