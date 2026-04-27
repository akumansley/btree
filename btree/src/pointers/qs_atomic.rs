use thin::{Pointable, QsOwned, QsShared};

use super::OwnedThinAtomicPtr;
use crate::sync::Ordering;

/// Lock-free atomic cell that reclaims replaced values via QSBR.
/// Writers are single-writer and must serialize themselves.
pub struct QsAtomic<T: ?Sized + Pointable> {
    slot: OwnedThinAtomicPtr<T>,
}

impl<T: Pointable + Send + 'static> QsAtomic<T> {
    pub fn new(value: T) -> Self {
        Self {
            slot: OwnedThinAtomicPtr::new(QsOwned::new(value)),
        }
    }

    /// Atomically replaces the stored value, returning the previous one.
    ///
    /// Dropping the returned `QsOwned` (or `let _ = atom.swap(...)`) schedules
    /// QSBR reclamation; keep it alive if you need to inspect the old value
    /// before it's reclaimed.
    #[inline]
    pub fn swap(&self, value: T, order: Ordering) -> QsOwned<T> {
        self.slot
            .swap(QsOwned::new(value), order)
            .expect("QsAtomic is always populated")
    }
}

impl<T: Send + 'static + Clone> QsAtomic<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Self {
            slot: OwnedThinAtomicPtr::new(QsOwned::new_from_slice(init)),
        }
    }

    /// Atomically replaces the stored value, returning the previous one.
    ///
    /// Dropping the returned `QsOwned` (or `let _ = atom.swap(...)`) schedules
    /// QSBR reclamation; keep it alive if you need to inspect the old value
    /// before it's reclaimed.
    #[inline]
    pub fn swap_from_slice(&self, value: &[T], order: Ordering) -> QsOwned<[T]> {
        self.slot
            .swap(QsOwned::new_from_slice(value), order)
            .expect("QsAtomic is always populated")
    }
}

impl QsAtomic<str> {
    pub fn new_from_str(init: &str) -> Self {
        Self {
            slot: OwnedThinAtomicPtr::new(QsOwned::new_from_str(init)),
        }
    }

    /// Atomically replaces the stored value, returning the previous one.
    ///
    /// Dropping the returned `QsOwned` (or `let _ = atom.swap(...)`) schedules
    /// QSBR reclamation; keep it alive if you need to inspect the old value
    /// before it's reclaimed.
    #[inline]
    pub fn swap_from_str(&self, value: &str, order: Ordering) -> QsOwned<str> {
        self.slot
            .swap(QsOwned::new_from_str(value), order)
            .expect("QsAtomic is always populated")
    }
}

impl<T: ?Sized + Pointable + Send + 'static> QsAtomic<T> {
    #[inline]
    pub fn load(&self, order: Ordering) -> QsShared<T> {
        self.slot
            .load_shared(order)
            .expect("QsAtomic is always populated")
    }
}

impl<T: ?Sized + Pointable + Send + 'static> Drop for QsAtomic<T> {
    fn drop(&mut self) {
        // SAFETY: `&mut self` rules out concurrent writers, so we can take
        // ownership of the slot exactly once and let the resulting `QsOwned`
        // schedule QSBR reclamation.
        //
        // Ordering: Drop is a pure load, so the only thing that matters is
        // what it pairs with. If the last writer used `Release`/`AcqRel`/
        // `SeqCst` on `swap`, our `Acquire` pairs with the Release on its
        // store side and establishes happens-before through the atomic
        // itself.
        let _owned = unsafe {
            self.slot
                .load_owned(Ordering::Acquire)
                .expect("QsAtomic is always populated")
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btree_macros::qsbr_test;

    #[qsbr_test]
    fn test_load_returns_initial_value() {
        let atom = QsAtomic::new(42usize);
        assert_eq!(*atom.load(Ordering::Acquire), 42);
    }

    #[qsbr_test]
    fn test_swap_publishes_new_value() {
        let atom = QsAtomic::new(10usize);

        let next = *atom.load(Ordering::Acquire) + 1;
        let _old = atom.swap(next, Ordering::AcqRel);
        assert_eq!(*atom.load(Ordering::Acquire), 11);

        let next = *atom.load(Ordering::Acquire) * 2;
        let _old = atom.swap(next, Ordering::AcqRel);
        assert_eq!(*atom.load(Ordering::Acquire), 22);
    }

    #[qsbr_test]
    fn test_swap_returns_previous_value() {
        let atom = QsAtomic::new(10usize);
        let old = atom.swap(20, Ordering::AcqRel);
        assert_eq!(*old, 10);
        assert_eq!(*atom.load(Ordering::Acquire), 20);
    }

    #[qsbr_test]
    fn test_prior_loads_stay_valid_across_swap() {
        let atom = QsAtomic::new(100usize);
        let before = atom.load(Ordering::Acquire);

        let _old = atom.swap(200, Ordering::AcqRel);
        let after = atom.load(Ordering::Acquire);

        assert_eq!(*before, 100);
        assert_eq!(*after, 200);
    }

    #[qsbr_test]
    fn test_drop_does_not_leak_or_panic() {
        let atom = QsAtomic::new(vec![1u32, 2, 3]);
        assert_eq!(atom.load(Ordering::Acquire).len(), 3);
        drop(atom);
    }

    #[qsbr_test]
    fn test_new_from_str() {
        let atom: QsAtomic<str> = QsAtomic::new_from_str("hello");
        assert_eq!(&*atom.load(Ordering::Acquire), "hello");
    }

    #[qsbr_test]
    fn test_new_from_slice() {
        let atom: QsAtomic<[u32]> = QsAtomic::new_from_slice(&[1u32, 2, 3]);
        assert_eq!(&*atom.load(Ordering::Acquire), [1u32, 2, 3].as_slice());
    }

    #[qsbr_test]
    fn test_swap_from_str() {
        let atom: QsAtomic<str> = QsAtomic::new_from_str("hello");
        let old = atom.swap_from_str("world", Ordering::AcqRel);
        assert_eq!(&*old, "hello");
        assert_eq!(&*atom.load(Ordering::Acquire), "world");
    }

    #[qsbr_test]
    fn test_swap_from_slice() {
        let atom: QsAtomic<[u32]> = QsAtomic::new_from_slice(&[1u32, 2, 3]);
        let old = atom.swap_from_slice(&[4u32, 5], Ordering::AcqRel);
        assert_eq!(&*old, [1u32, 2, 3].as_slice());
        assert_eq!(&*atom.load(Ordering::Acquire), [4u32, 5].as_slice());
    }
}
