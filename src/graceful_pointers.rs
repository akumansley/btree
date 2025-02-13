use std::{
    marker::PhantomData,
    ops::Deref,
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::qsbr::qsbr_reclaimer;

pub trait GracefulAtomicPointer<T: Send + 'static> {
    type GracefulPointer: 'static;

    fn new(inner: T) -> Self;
    fn load(&self, ordering: Ordering) -> Self::GracefulPointer;
    fn store(&self, gbox: Self::GracefulPointer, ordering: Ordering);
    fn swap(
        &self,
        gbox: Self::GracefulPointer,
        ordering: Ordering,
    ) -> Option<Self::GracefulPointer>;
}

impl<T: Send + 'static> GracefulAtomicPointer<T> for AtomicPtr<T> {
    type GracefulPointer = *mut T;

    fn new(inner: T) -> Self {
        Self::new(Box::into_raw(Box::new(inner)))
    }
    fn load(&self, ordering: Ordering) -> *mut T {
        self.load(ordering)
    }
    fn store(&self, gbox: *mut T, ordering: Ordering) {
        self.store(gbox, ordering);
    }
    fn swap(&self, gbox: *mut T, ordering: Ordering) -> Option<*mut T> {
        Some(self.swap(gbox, ordering))
    }
}

// GracefulBox works around the fact that Box can't be aliased
// but we might be immutably aliased by concurrent readers
pub struct GracefulBox<T: Send + 'static> {
    inner: NonNull<T>,
    phantom: PhantomData<T>,
}

impl<T: Send + 'static> GracefulBox<T> {
    pub fn new(inner: *mut T) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(inner) },
            phantom: PhantomData,
        }
    }
    pub fn as_ptr(&self) -> *mut T {
        self.inner.as_ptr()
    }
}
unsafe impl<T: Send + 'static> Send for GracefulBox<T> {}

impl<T: Send + 'static> Drop for GracefulBox<T> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.inner.as_ptr());
        }
    }
}

pub struct AtomicGracefulBox<T: Send + 'static> {
    inner: AtomicPtr<T>,
    phantom: PhantomData<T>,
}

impl<T: Send + 'static> GracefulAtomicPointer<T> for AtomicGracefulBox<T> {
    type GracefulPointer = GracefulBox<T>;

    fn new(inner: T) -> Self {
        let inner_ptr = Box::into_raw(Box::new(inner));
        Self::new(inner_ptr)
    }
    fn load(&self, ordering: Ordering) -> GracefulBox<T> {
        let ptr = self.inner.load(ordering);
        GracefulBox::new(ptr)
    }
    fn store(&self, gbox: GracefulBox<T>, ordering: Ordering) {
        self.inner.store(gbox.as_ptr(), ordering);
    }
    fn swap(&self, gbox: GracefulBox<T>, ordering: Ordering) -> Option<GracefulBox<T>> {
        let old_ptr = self.inner.swap(gbox.as_ptr(), ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(GracefulBox::new(old_ptr))
        }
    }
}

impl<T: Send + 'static> AtomicGracefulBox<T> {
    pub fn new(inner: *mut T) -> Self {
        Self {
            inner: AtomicPtr::new(inner),
            phantom: PhantomData,
        }
    }
}

pub struct AtomicGracefulArc<T: Send + 'static> {
    inner: AtomicPtr<GracefulArcInner<T>>,
    phantom: PhantomData<T>,
}

impl<T: Send + 'static> GracefulAtomicPointer<T> for AtomicGracefulArc<T> {
    type GracefulPointer = GracefulArc<T>;

    fn new(inner: T) -> Self {
        let inner = GracefulArcInner {
            ref_count: AtomicUsize::new(1),
            inner,
        };
        let inner_ptr = Box::into_raw(Box::new(inner));
        AtomicGracefulArc {
            inner: AtomicPtr::new(inner_ptr),
            phantom: PhantomData,
        }
    }
    fn load(&self, ordering: Ordering) -> GracefulArc<T> {
        let ptr = self.inner.load(ordering);
        GracefulArc::from_ptr(ptr)
    }
    fn store(&self, garc: GracefulArc<T>, ordering: Ordering) {
        self.inner.store(garc.as_ptr(), ordering);
    }
    fn swap(&self, garc: GracefulArc<T>, ordering: Ordering) -> Option<GracefulArc<T>> {
        let old_ptr = self.inner.swap(garc.as_ptr(), ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(GracefulArc::from_ptr(old_ptr))
        }
    }
}

pub struct GracefulArcInner<T: Send + 'static> {
    ref_count: AtomicUsize,
    inner: T,
}

pub struct GracefulArc<T: Send + 'static> {
    inner: NonNull<GracefulArcInner<T>>,
    phantom: PhantomData<T>,
}

unsafe impl<T: Send + 'static> Send for GracefulArc<T> {}

impl<T: Send + 'static> GracefulArc<T> {
    fn from_ptr(ptr: *mut GracefulArcInner<T>) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr) },
            phantom: PhantomData,
        }
    }
    pub fn as_ptr(&self) -> *mut GracefulArcInner<T> {
        self.inner.as_ptr()
    }
    pub fn as_ref(&self) -> &T {
        &unsafe { &*self.inner.as_ptr() }.inner
    }
    pub fn new(inner: T) -> Self {
        let inner = Box::new(GracefulArcInner {
            ref_count: AtomicUsize::new(1),
            inner,
        });
        Self {
            inner: unsafe { NonNull::new_unchecked(Box::into_raw(inner)) },
            phantom: PhantomData,
        }
    }
    pub fn clone_without_incrementing_ref_count(&self) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(self.inner.as_ptr()) },
            phantom: PhantomData,
        }
    }
    pub fn clone_and_increment_ref_count(&self) -> Self {
        if self.increment_ref_count() {
            self.clone_without_incrementing_ref_count()
        } else {
            panic!("attempted to clone a graceful arc with a ref_count of 0");
        }
    }
    pub fn increment_ref_count(&self) -> bool {
        let inner = self.inner.as_ptr();

        // need to use a CAS loop instead of fetch_add because we need to check if the ref_count is 0
        // in which case we can't increment it
        loop {
            let old_ref_count = unsafe { (*inner).ref_count.load(Ordering::Relaxed) };
            if old_ref_count == 0 {
                return false;
            }
            if unsafe {
                (*inner)
                    .ref_count
                    .compare_exchange(
                        old_ref_count,
                        old_ref_count + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
            } {
                continue;
            } else {
                return true;
            }
        }
    }
    pub fn decrement_ref_count(&self) {
        let inner = self.inner.as_ptr();
        unsafe {
            let old_ref_count = (*inner).ref_count.fetch_sub(1, Ordering::Relaxed);
            if old_ref_count == 1 {
                // there can be readers after this point, but there can't be any more references
                let inner_box = GracefulBox::new(inner);
                qsbr_reclaimer().add_callback(Box::new(move || {
                    drop(inner_box);
                }));
            }
        }
    }
    pub unsafe fn drop_in_place(&self) {
        let inner = self.inner.as_ptr();
        unsafe {
            ptr::drop_in_place(inner);
        }
    }
}

impl<T: Send + 'static> Deref for GracefulArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let inner = self.inner.as_ptr();
        unsafe { &(*inner).inner }
    }
}
