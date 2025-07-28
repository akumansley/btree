use std::{
    alloc::{self, Layout},
    mem::MaybeUninit,
    ptr,
    slice,
};

use crate::{
    pointers::SendPtr,
    sync::{AtomicUsize, Ordering},
};

pub struct RefCount {
    count: AtomicUsize,
}

impl RefCount {
    pub fn new() -> Self {
        let result = Self {
            count: AtomicUsize::new(1),
        };
        result
    }

    pub fn increment(&self) -> bool {
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

    pub fn decrement(&self) -> bool {
        let old_count = self.count.fetch_sub(1, Ordering::Relaxed);
        if old_count == 0 {
            panic!("attempted to decrement a ref_count of 0 {:p}", self);
        }
        let result = old_count == 1;
        result
    }

    pub fn load(&self) -> usize {
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

#[repr(C)]
struct ArcInner<T: Sized> {
    ref_count: RefCount,
    data: T,
}

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

pub fn init_arc_sized<T: Sized + Arcable>(init: T) -> *mut () {
    let ptr = Box::into_raw(Box::new(ArcInner {
        ref_count: RefCount::new(),
        data: init,
    })) as *mut ();
    ptr
}

/// Arcable impl for str

pub fn init_arc_str(init: &str) -> *mut () {
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
        let array = unsafe { &*(ptr as *const ArcArray<u8>) };
        unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                array.elements.as_ptr() as *const _,
                array.len,
            ))
        }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = unsafe { &mut *(ptr as *mut ArcArray<u8>) };
        unsafe {
            std::str::from_utf8_unchecked_mut(slice::from_raw_parts_mut(
                array.elements.as_mut_ptr() as *mut _,
                array.len,
            ))
        }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        unsafe {
            let len = (*(ptr as *mut ArcArray<u8>)).len;
            let layout = ArcArray::<u8>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

/// Arcable impl for slices
pub fn init_arc_slice<T: Clone>(init: &[T]) -> *mut () {
    let layout = ArcArray::<T>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<ArcArray<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<T>;
        let slice = slice::from_raw_parts_mut(elements_ptr, init.len());
        for (i, elem) in slice.iter_mut().enumerate() {
            elem.write(init[i].clone());
        }
        ptr as *mut ()
    }
}

pub fn init_arc_slice_uninitialized<T>(len: usize) -> *mut () {
    let layout = ArcArray::<T>::layout(len);
    let ptr = unsafe { alloc::alloc(layout).cast::<ArcArray<T>>() };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    unsafe {
        ptr::addr_of_mut!((*ptr).len).write(len);
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
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
        unsafe {
            let array = &*(ptr as *const ArcArray<T>);
            slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len)
        }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        unsafe {
            let array = &mut *(ptr as *mut ArcArray<T>);
            slice::from_raw_parts_mut(array.elements.as_mut_ptr() as *mut _, array.len)
        }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        for elem in unsafe { <[T] as Arcable>::deref_mut_arc(ptr) } {
            ptr::drop_in_place(elem);
        }
        unsafe {
            let len = (*(ptr as *mut ArcArray<T>)).len;
            let layout = ArcArray::<T>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}
