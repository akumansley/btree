use std::{alloc, mem::MaybeUninit, ptr, slice};

use crate::arcable::{ArcArray, Arcable, RefCount};

/// Arcable impl for str

pub fn init_thin_str(init: &str) -> *mut () {
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
