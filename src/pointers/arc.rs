use std::{
    alloc::{self, Layout},
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ops::Deref,
    ptr, slice,
};

use crate::qsbr_reclaimer;
use crate::sync::{AtomicPtr, AtomicUsize, Ordering};

use super::{traits::SendPtr, AtomicPointerArrayValue};

struct RefCount {
    count: AtomicUsize,
}

impl RefCount {
    fn new() -> Self {
        let result = Self {
            count: AtomicUsize::new(1),
        };
        result
    }

    fn increment(&self) -> bool {
        loop {
            let old_ref_count = self.count.load(Ordering::Relaxed);
            if old_ref_count == 0 {
                panic!("attempted to increment a ref_count of 0");
            }
            if self
                .count
                .compare_exchange(
                    old_ref_count,
                    old_ref_count + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            } else {
                return true;
            }
        }
    }

    fn decrement(&self) -> bool {
        let old_count = self.count.fetch_sub(1, Ordering::Relaxed);
        if old_count == 0 {
            panic!("attempted to decrement a ref_count of 0 {:p}", self);
        }
        let result = old_count == 1;
        result
    }

    fn load(&self) -> usize {
        let count = self.count.load(Ordering::Relaxed);
        count
    }
}

pub trait Arcable {
    fn ref_count(ptr: *mut ()) -> usize;
    fn increment_ref_count(ptr: *mut ());
    fn decrement_ref_count(ptr: *mut ()) -> bool;
    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self;
    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self;
    unsafe fn drop_arc(ptr: *mut ());
}

#[repr(C)]
struct ArcArray<T> {
    ref_count: RefCount,
    /// The number of elements (not the number of bytes).
    len: usize,
    elements: [MaybeUninit<T>; 0],
}

impl<T> ArcArray<T> {
    fn layout(len: usize) -> Layout {
        Layout::new::<Self>()
            .extend(Layout::array::<MaybeUninit<T>>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }
}

// Arcable impl for Sized types

impl<T: Sized> Arcable for T {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &(*arc_inner_ptr).data }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut (*arc_inner_ptr).data }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { drop(Box::from_raw(arc_inner_ptr)) };
    }
}

struct ArcInner<T: Sized> {
    ref_count: RefCount,
    data: T,
}

fn init_thin_sized<T: Sized + Arcable>(init: T) -> *mut () {
    let ptr = Box::into_raw(Box::new(ArcInner {
        ref_count: RefCount::new(),
        data: init,
    })) as *mut ();
    ptr
}

/// Arcable impl for str

fn init_thin_str(init: &str) -> *mut () {
    let layout = ArcArray::<u8>::layout(init.len());
    let ptr = unsafe { alloc::alloc(layout).cast::<ArcArray<u8>>() };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    unsafe {
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<u8>;
        ptr::copy_nonoverlapping(
            init.as_ptr() as *const MaybeUninit<u8>,
            elements_ptr,
            init.len(),
        );
        ptr as *mut ()
    }
}

impl Arcable for str {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let array = &*(ptr as *const ArcArray<u8>);
        unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                array.elements.as_ptr() as *const _,
                array.len,
            ))
        }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = &mut *(ptr as *mut ArcArray<u8>);
        unsafe {
            std::str::from_utf8_unchecked_mut(slice::from_raw_parts_mut(
                array.elements.as_mut_ptr() as *mut _,
                array.len,
            ))
        }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let len = (*(ptr as *mut ArcArray<u8>)).len;
        let layout = ArcArray::<u8>::layout(len);
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

/// Arcable impl for slices
fn init_thin_slice<T>(init: &[T]) -> *mut () {
    let layout = ArcArray::<T>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<ArcArray<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<T>;
        ptr::copy_nonoverlapping(
            init.as_ptr() as *const MaybeUninit<T>,
            elements_ptr,
            init.len(),
        );
        ptr as *mut ()
    }
}

impl<T> Arcable for [T] {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let array = &*(ptr as *const ArcArray<T>);
        unsafe { slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len) }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = &mut *(ptr as *mut ArcArray<T>);
        unsafe { slice::from_raw_parts_mut(array.elements.as_mut_ptr() as *mut _, array.len) }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let len = (*(ptr as *mut ArcArray<T>)).len;
        let layout = ArcArray::<T>::layout(len);
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

/// OwnedThinArc is a thin pointer that owns its target.
///
/// When an OwnedThinArc is dropped, it will immediately decrement the pointee's reference count.
/// When the reference count reaches 0, the pointee will be dropped via QSBR.
///
/// If the pointee should be dropped immediately, use `OwnedThinArc::drop_immediately`.
pub struct OwnedThinArc<T: ?Sized + Arcable + 'static> {
    ptr: *mut (),
    _marker: PhantomData<*mut T>,
}

pub struct SharedThinArc<T: ?Sized + Arcable + 'static> {
    ptr: *mut (),
    _marker: PhantomData<*mut T>,
}

macro_rules! impl_thin_arc_traits {
    ($arc_type:ident) => {
        unsafe impl<T: ?Sized + Arcable> Send for $arc_type<T> {}

        impl<T: ?Sized + Arcable> std::ops::Deref for $arc_type<T> {
            type Target = T;
            fn deref(&self) -> &T {
                unsafe { T::deref_arc(self.ptr) }
            }
        }

        impl<T: ?Sized + Arcable + std::fmt::Debug> std::fmt::Debug for $arc_type<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + Arcable + std::fmt::Display> std::fmt::Display for $arc_type<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + Arcable> $arc_type<T> {
            unsafe fn from_ptr(ptr: *mut ()) -> $arc_type<T> {
                Self {
                    ptr,
                    _marker: PhantomData,
                }
            }

            pub fn share(&self) -> SharedThinArc<T> {
                let ptr = self.ptr;
                unsafe { SharedThinArc::from_ptr(ptr) }
            }
        }
        impl<T: ?Sized + Arcable + PartialEq> PartialEq<T> for $arc_type<T> {
            fn eq(&self, other: &T) -> bool {
                T::eq(self.deref(), other)
            }
        }

        impl<T: ?Sized + Arcable + PartialEq> PartialEq<&T> for $arc_type<T> {
            fn eq(&self, other: &&T) -> bool {
                self.deref() == *other
            }
        }
    };
}

impl_thin_arc_traits!(OwnedThinArc);
impl_thin_arc_traits!(SharedThinArc);

impl<T: ?Sized + Arcable> OwnedThinArc<T> {
    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let ptr = init();
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    pub fn ref_count(&self) -> usize {
        T::ref_count(self.ptr)
    }

    pub unsafe fn drop_immediately(thin_arc: OwnedThinArc<T>) {
        let ptr = thin_arc.ptr;
        mem::forget(thin_arc);
        if T::decrement_ref_count(ptr) {
            T::drop_arc(ptr);
        }
    }
}

impl<T: ?Sized + Arcable> Clone for OwnedThinArc<T> {
    fn clone(&self) -> Self {
        T::increment_ref_count(self.ptr);
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Clone for SharedThinArc<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Copy for SharedThinArc<T> {}

impl<T: Sized + Arcable> OwnedThinArc<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }
}

impl OwnedThinArc<str> {
    pub fn new_from_str(init: &str) -> Self {
        Self::new_with(|| init_thin_str(init))
    }
}

impl<T> OwnedThinArc<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Self::new_with(|| init_thin_slice(init))
    }
}

impl<T: ?Sized + Arcable + 'static> Drop for OwnedThinArc<T> {
    fn drop(&mut self) {
        if T::decrement_ref_count(self.ptr) {
            let send_ptr = SendPtr::new(self.ptr);
            qsbr_reclaimer().add_callback(Box::new(move || {
                unsafe { T::drop_arc(send_ptr.into_ptr()) };
            }));
        }
    }
}

pub struct ThinAtomicArc<T: ?Sized + Arcable> {
    inner: AtomicPtr<()>,
    _marker: PhantomData<T>,
}

impl<T: ?Sized + Arcable> ThinAtomicArc<T> {
    pub fn load_owned(&self, ordering: Ordering) -> Option<OwnedThinArc<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(ptr) })
        }
    }

    pub fn load_cloned(&self, ordering: Ordering) -> OwnedThinArc<T> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load_cloned null pointer");
        }
        let owned: OwnedThinArc<T> = unsafe { OwnedThinArc::from_ptr(ptr) };
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

    pub fn store(&self, arc: OwnedThinArc<T>, ordering: Ordering) {
        let ptr = arc.ptr;
        if ptr.is_null() {
            panic!("Attempted to store null pointer");
        }
        mem::forget(arc);
        self.inner.store(ptr, ordering);
    }

    pub fn load(&self, ordering: Ordering) -> Option<SharedThinArc<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { SharedThinArc::from_ptr(ptr) })
        }
    }

    pub fn swap(&self, arc: OwnedThinArc<T>, ordering: Ordering) -> Option<OwnedThinArc<T>> {
        let ptr = arc.ptr;
        if ptr.is_null() {
            panic!("Attempted to swap null pointer");
        }
        mem::forget(arc);
        let old_ptr = self.inner.swap(ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(old_ptr) })
        }
    }
}

pub type OwnedAtomicThinArc<T> = ThinAtomicArc<T>;

impl<T: Send + 'static + ?Sized + Arcable> AtomicPointerArrayValue<T> for ThinAtomicArc<T> {
    type OwnedPointer = OwnedThinArc<T>;
    type SharedPointer = SharedThinArc<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer");
        }
        unsafe { OwnedThinArc::from_ptr(ptr) }
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        self.load(ordering)
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(ptr) })
        }
    }

    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering) {
        let ptr = matching_ptr.ptr;
        mem::forget(matching_ptr);
        self.inner.store(ptr, ordering);
    }

    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer> {
        let ptr = matching_ptr.ptr;
        mem::forget(matching_ptr);
        let old_ptr = self.inner.swap(ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(old_ptr) })
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc as StdArc;

    #[test]
    fn test_owned_arc_basic() {
        qsbr_reclaimer().register_thread();
        {
            // Create a new OwnedArc
            let arc = OwnedThinArc::new(42);

            // Test dereferencing
            assert_eq!(*arc, 42);

            // Test cloning
            let arc_clone = arc.clone();
            assert_eq!(*arc_clone, 42);

            let shared = arc.share();
            assert_eq!(*shared, 42);

            // Test equality with values
            assert_eq!(arc, &42);
            assert_eq!(&*arc, &42);
        }
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_owned_arc_drop() {
        qsbr_reclaimer().register_thread();
        {
            // Use a structure with a flag to check if it's dropped
            struct DropTest {
                counter: StdArc<AtomicUsize>,
            }

            impl Drop for DropTest {
                fn drop(&mut self) {
                    self.counter.fetch_add(1, Ordering::SeqCst);
                }
            }

            let counter = StdArc::new(AtomicUsize::new(0));

            {
                let arc = OwnedThinArc::new(DropTest {
                    counter: counter.clone(),
                });
                let arc_clone = arc.clone();
                unsafe { OwnedThinArc::drop_immediately(arc) };
                unsafe { OwnedThinArc::drop_immediately(arc_clone) };
            }

            // The counter should be 1 because the value should be dropped exactly once
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        }
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_shared_arc() {
        qsbr_reclaimer().register_thread();
        {
            // Create an OwnedArc and convert to SharedArc
            let owned = OwnedThinArc::new(42);
            let shared = owned.share();

            // Test dereferencing
            assert_eq!(*shared, 42);

            // Test cloning
            let shared_clone = shared.clone();
            assert_eq!(*shared_clone, 42);
        }
        unsafe {
            qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
        }
    }

    #[test]
    fn test_atomic_arc() {
        qsbr_reclaimer().register_thread();
        {
            // Create an AtomicArc
            let atomic = ThinAtomicArc::<i32>::new();

            // Store a value
            let owned = OwnedThinArc::new(42);
            atomic.store(owned, Ordering::Relaxed);

            // Load the value
            let loaded = atomic.load(Ordering::Relaxed);
            assert!(loaded.is_some());
            assert_eq!(*loaded.unwrap(), 42);

            // Test load_cloned
            let cloned = atomic.load_cloned(Ordering::Relaxed);
            assert_eq!(*cloned, 42);
            drop(cloned);

            // Test swap
            let new_owned = OwnedThinArc::new(84);
            let old = atomic.swap(new_owned, Ordering::Relaxed).unwrap();
            assert_eq!(*old, 42);

            // Verify the new value
            let loaded = atomic.load(Ordering::Relaxed);
            assert!(loaded.is_some());
            assert_eq!(*loaded.unwrap(), 84);
            drop(atomic.load_owned(Ordering::Relaxed).unwrap());
        }
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_partial_eq_implementations() {
        qsbr_reclaimer().register_thread();
        {
            let arc = OwnedThinArc::new(42);
            let value = 42;
            let value_ref = &value;

            // Test OwnedArc == T
            assert_eq!(arc, value);

            // Test OwnedArc == &T
            assert_eq!(arc, value_ref);

            // Note: &T == OwnedArc comparison doesn't work due to Rust's orphan rule
            // We can't implement PartialEq<OwnedArc<T>> for &T because both &T and PartialEq
            // are defined in the standard library
            // assert_eq!(value_ref, arc); // This would fail to compile

            // Workaround: Use explicit dereferencing
            assert_eq!(*value_ref, *arc);
        }
        unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
    }
}
