use std::{
    marker::PhantomData,
    mem,
    ptr,
};

use crate::{
    arcable::Arcable,
    pointers::{qs_arc_owned::QsArcOwned, qs_arc_shared::QsArcShared},
    sync::{AtomicPtr, Ordering},
};

pub struct QsAtomicArc<T: ?Sized + Arcable> {
    inner: AtomicPtr<()>,
    _marker: PhantomData<T>,
}

impl<T: ?Sized + Arcable> QsAtomicArc<T> {
    pub fn new() -> Self {
        Self {
            inner: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    pub fn load_owned(&self, ordering: Ordering) -> Option<QsArcOwned<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArcOwned::from_ptr(ptr) })
        }
    }

    pub fn load_cloned(&self, ordering: Ordering) -> QsArcOwned<T> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load_cloned null pointer");
        }
        let owned: QsArcOwned<T> = unsafe { QsArcOwned::from_ptr(ptr) };
        let cloned = owned.clone();
        mem::forget(owned);
        cloned
    }

    pub fn store(&self, arc: QsArcOwned<T>, ordering: Ordering) {
        let ptr = arc.into_ptr();
        self.inner.store(ptr, ordering);
    }

    pub fn load(&self, ordering: Ordering) -> Option<QsArcShared<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArcShared::from_ptr(ptr) })
        }
    }

    pub fn swap(&self, arc: QsArcOwned<T>, ordering: Ordering) -> Option<QsArcOwned<T>> {
        let ptr = arc.into_ptr();
        let old_ptr = self.inner.swap(ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArcOwned::from_ptr(old_ptr) })
        }
    }
}

impl<T: ?Sized + Arcable> Default for QsAtomicArc<T> {
    fn default() -> Self {
        Self::new()
    }
}
