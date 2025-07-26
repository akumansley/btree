use super::{Pointable, PointableClone};

impl<T: Pointable + Sized + Clone> PointableClone for T {
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let value = unsafe { T::deref(ptr).clone() };
        init_thin_sized(value)
    }
}

pub fn init_thin_sized<T: Sized>(init: T) -> *mut () {
    Box::into_raw(Box::new(init)) as *mut ()
}

// the sized bound is implicit, but I'm adding it here for clarity
impl<T: Sized + Send + 'static> Pointable for T {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        unsafe { &*(ptr as *const T) }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        unsafe { &mut *(ptr as *mut T) }
    }

    unsafe fn drop(ptr: *mut ()) {
        unsafe { drop(Box::from_raw(ptr as *mut T)) };
    }
}
