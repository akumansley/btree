use std::{alloc, mem::MaybeUninit, ptr, slice};

use super::{Array, Pointable, PointableClone};

impl<T: Send + 'static> Pointable for [T] {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        unsafe {
            let array = &*(ptr as *const Array<T>);
            slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len)
        }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        unsafe {
            let array = &mut *(ptr as *mut Array<T>);
            slice::from_raw_parts_mut(array.elements.as_mut_ptr() as *mut _, array.len)
        }
    }

    unsafe fn drop(ptr: *mut ()) {
        for elem in unsafe { <[T] as Pointable>::deref_mut(ptr) } {
            ptr::drop_in_place(elem);
        }
        unsafe {
            let len = (*(ptr as *mut Array<T>)).len;
            let layout = Array::<T>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

impl<T: Clone> PointableClone for [T]
where
    [T]: Pointable,
{
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let slice = unsafe { <[T] as Pointable>::deref(ptr) };
        init_thin_slice(slice)
    }
}

pub fn init_thin_slice<'a, T: Clone>(init: &'a [T]) -> *mut () {
    let layout = Array::<T>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<T>;
        let slice = slice::from_raw_parts_mut(elements_ptr, init.len());
        for (i, elem) in slice.iter_mut().enumerate() {
            elem.write(init[i].clone());
        }
        ptr as *mut ()
    }
}

pub fn init_thin_slice_uninitialized<T>(len: usize) -> *mut () {
    let layout = Array::<T>::layout(len);
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(len);
        ptr as *mut ()
    }
}
