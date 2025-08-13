use std::{marker::PhantomData, mem, ptr, sync::atomic::Ordering};

use thin::{Arcable, QsArc, QsWeak};

use crate::{pointers::AtomicPointerArrayValue, sync::AtomicPtr};

pub struct ThinAtomicArc<T: ?Sized + Arcable> {
    inner: AtomicPtr<()>,
    _marker: PhantomData<T>,
}

impl<T: ?Sized + Arcable> ThinAtomicArc<T> {
    pub fn load_owned(&self, ordering: Ordering) -> Option<QsArc<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArc::from_ptr(ptr) })
        }
    }

    pub fn load_cloned(&self, ordering: Ordering) -> QsArc<T> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load_cloned null pointer");
        }
        let owned: QsArc<T> = unsafe { QsArc::from_ptr(ptr) };
        let cloned = owned.clone();
        mem::forget(owned);
        cloned
    }
}

impl<T: ?Sized + Arcable> ThinAtomicArc<T> {
    pub fn new() -> Self {
        Self {
            inner: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    pub fn store(&self, arc: QsArc<T>, ordering: Ordering) {
        let ptr = arc.into_ptr();
        self.inner.store(ptr, ordering);
    }

    pub fn load(&self, ordering: Ordering) -> Option<QsWeak<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsWeak::from_ptr(ptr) })
        }
    }

    pub fn swap(&self, arc: QsArc<T>, ordering: Ordering) -> Option<QsArc<T>> {
        let ptr = arc.into_ptr();
        let old_ptr = self.inner.swap(ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArc::from_ptr(old_ptr) })
        }
    }
}

pub type OwnedAtomicThinArc<T> = ThinAtomicArc<T>;

impl<T: Send + 'static + ?Sized + Arcable> AtomicPointerArrayValue<T> for ThinAtomicArc<T> {
    type OwnedPointer = QsArc<T>;
    type SharedPointer = QsWeak<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer");
        }
        unsafe { QsArc::from_ptr(ptr) }
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        self.load(ordering)
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArc::from_ptr(ptr) })
        }
    }

    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering) {
        let ptr = matching_ptr.into_ptr();
        self.inner.store(ptr, ordering);
    }

    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer> {
        let ptr = matching_ptr.into_ptr();
        let old_ptr = self.inner.swap(ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { QsArc::from_ptr(old_ptr) })
        }
    }
}
