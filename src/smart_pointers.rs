use std::{
    marker::PhantomData,
    ops::Deref,
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::qsbr::qsbr_reclaimer;

// GracefulBox works around the fact that Box can't be aliased
// but we might be immutably aliased by concurrent readers
pub struct GracefulBox<T: Send + 'static> {
    inner: NonNull<T>,
    phantom: PhantomData<T>,
}

impl<T: Send + 'static> GracefulBox<T> {
    pub fn new(inner: *mut T) -> Self {
        Self {
            inner: NonNull::new(inner).unwrap(),
            phantom: PhantomData,
        }
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

pub struct GracefulArc<T: Send + 'static> {
    inner: AtomicPtr<GracefulArcInner<T>>,
    phantom: PhantomData<T>,
}

unsafe impl<T: Send + 'static> Send for GracefulArc<T> {}

struct GracefulArcInner<T: Send + 'static> {
    ref_count: AtomicUsize,
    inner: T,
}

impl<T: Send + 'static> GracefulArc<T> {
    pub fn new(inner: T) -> Self {
        let inner = Box::new(GracefulArcInner {
            ref_count: AtomicUsize::new(1),
            inner,
        });
        Self {
            inner: AtomicPtr::new(Box::into_raw(inner)),
            phantom: PhantomData,
        }
    }
}

impl<T: Send + 'static> Drop for GracefulArc<T> {
    fn drop(&mut self) {
        let inner = self.inner.load(Ordering::Relaxed);
        unsafe {
            if (*inner).ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
                let inner_box = Box::from_raw(inner);
                qsbr_reclaimer().add_callback(Box::new(move || {
                    drop(inner_box);
                }));
            }
        }
    }
}

impl<T: Send + 'static> Deref for GracefulArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let inner = self.inner.load(Ordering::Relaxed);
        unsafe { &(*inner).inner }
    }
}

impl<T: Send + 'static> Clone for GracefulArc<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.load(Ordering::Relaxed);
        unsafe {
            (*inner).ref_count.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            inner: AtomicPtr::new(inner),
            phantom: PhantomData,
        }
    }
}
