use std::{alloc, mem::MaybeUninit, ptr, slice};

use super::{Array, Pointable, PointableClone};

impl Pointable for str {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        let array = unsafe { &*(ptr as *const Array<u8>) };
        unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                array.elements.as_ptr() as *const _,
                array.len,
            ))
        }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = unsafe { &mut *(ptr as *mut Array<u8>) };
        unsafe {
            std::str::from_utf8_unchecked_mut(slice::from_raw_parts_mut(
                array.elements.as_mut_ptr() as *mut _,
                array.len,
            ))
        }
    }

    unsafe fn drop(ptr: *mut ()) {
        unsafe {
            let len = (*(ptr as *mut Array<u8>)).len;
            let layout = Array::<u8>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

pub fn init_thin_str<'a>(init: &'a str) -> *mut () {
    let layout = Array::<u8>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<u8>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
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

impl PointableClone for str {
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let s = unsafe { <str as Pointable>::deref(ptr) };
        init_thin_str(s)
    }
}
