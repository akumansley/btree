use std::slice;
use std::{alloc, mem::MaybeUninit, ptr};

use crate::arcable::{ArcArray, Arcable, RefCount};

/// Arcable impl for slices
pub fn init_thin_slice<T: Clone>(init: &[T]) -> *mut () {
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

pub fn init_thin_slice_uninitialized<T>(len: usize) -> *mut () {
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
