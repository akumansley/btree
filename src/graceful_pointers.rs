use crate::sync::{AtomicPtr, AtomicUsize, Ordering};
use std::{marker::PhantomData, ops::Deref, ptr::NonNull};

use crate::qsbr::qsbr_reclaimer;

pub trait GracefulAtomicPointer<T: Send + 'static> {
    type GracefulPointer: 'static;

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
}
unsafe impl<T: Send + 'static> Send for GracefulBox<T> {}

impl<T: Send + 'static> Drop for GracefulBox<T> {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.inner.as_ptr()));
        }
    }
}

pub struct AtomicGracefulArc<T: Send + 'static> {
    inner: AtomicPtr<GracefulArcInner<T>>,
    phantom: PhantomData<GracefulArcInner<T>>,
}

impl<T: Send + 'static> GracefulAtomicPointer<T> for AtomicGracefulArc<T> {
    type GracefulPointer = GracefulArc<T>;
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

pub struct GracefulArcInner<T: Send + 'static + ?Sized> {
    ref_count: AtomicUsize,
    data: T,
}

pub struct GracefulArc<T: Send + 'static + ?Sized> {
    ptr_to_inner: NonNull<GracefulArcInner<T>>,
    phantom: PhantomData<T>,
}

unsafe impl<T: Send + 'static> Send for GracefulArc<T> {}

impl<T: Send + 'static> GracefulArc<T> {
    fn from_ptr(ptr: *mut GracefulArcInner<T>) -> Self {
        Self {
            ptr_to_inner: unsafe { NonNull::new_unchecked(ptr) },
            phantom: PhantomData,
        }
    }
    pub fn as_ptr(&self) -> *mut GracefulArcInner<T> {
        self.ptr_to_inner.as_ptr()
    }
    pub fn as_ref(&self) -> &T {
        &unsafe { &*self.ptr_to_inner.as_ptr() }.data
    }
    pub fn new(data: T) -> Self {
        let ptr_to_inner = Box::into_raw(Box::new(GracefulArcInner {
            ref_count: AtomicUsize::new(1),
            data,
        }));
        Self {
            ptr_to_inner: unsafe { NonNull::new_unchecked(ptr_to_inner) },
            phantom: PhantomData,
        }
    }
    pub fn clone_without_incrementing_ref_count(&self) -> Self {
        Self {
            ptr_to_inner: unsafe { NonNull::new_unchecked(self.ptr_to_inner.as_ptr()) },
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
        let inner = self.ptr_to_inner.as_ptr();

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
        let ptr_to_inner = self.ptr_to_inner.as_ptr();
        unsafe {
            let old_ref_count = (*ptr_to_inner).ref_count.fetch_sub(1, Ordering::Relaxed);
            if old_ref_count == 1 {
                // there can be readers after this point, but there can't be any more references
                let inner_box = GracefulBox::new(ptr_to_inner);
                qsbr_reclaimer().add_callback(Box::new(move || {
                    drop(inner_box);
                }));
            } else if old_ref_count == 0 {
                panic!(
                    "attempted to decrement the ref_count of a graceful arc at {:?} with a ref_count of 0",
                    self.ptr_to_inner.as_ptr()
                );
            }
        }
    }
    pub unsafe fn decrement_ref_count_and_drop_if_zero(self) {
        let inner = self.ptr_to_inner.as_ptr();
        unsafe {
            let old_ref_count = (*inner).ref_count.fetch_sub(1, Ordering::Relaxed);
            if old_ref_count == 1 {
                self.drop_in_place();
            } else if old_ref_count == 0 {
                panic!(
                    "attempted to decrement the ref_count of a graceful arc with a ref_count of 0"
                );
            }
        }
    }

    pub unsafe fn drop_in_place(self) {
        unsafe {
            drop(Box::from_raw(self.ptr_to_inner.as_ptr()));
        }
    }
}

impl<T: Send + 'static> Deref for GracefulArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let inner = self.ptr_to_inner.as_ptr();
        unsafe { &(*inner).data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qsbr::qsbr_reclaimer;

    #[test]
    fn test_graceful_arc_ref_counting() {
        qsbr_reclaimer().register_thread();

        // Create a new GracefulArc
        let input = Box::new(42);
        let arc1 = GracefulArc::new(*input);
        assert_eq!(*arc1, 42);

        // Test clone_and_increment_ref_count -- we're at 2
        let arc2 = arc1.clone_and_increment_ref_count();
        assert_eq!(*arc2, 42);

        // Test clone_without_incrementing_ref_count -- we're at 2
        let arc3 = arc2.clone_without_incrementing_ref_count();
        assert_eq!(*arc3, 42);

        // Test decrement_ref_count -- we're at 1
        arc2.decrement_ref_count();

        // Test decrement_ref_count_and_drop_if_zero -- we're at 0; we should
        // drop the arcinner
        unsafe { arc3.decrement_ref_count_and_drop_if_zero() };

        qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
    }
}
