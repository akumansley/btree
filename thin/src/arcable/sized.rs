// Arcable impl for Sized types

use crate::arcable::{Arcable, RefCount};

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

pub fn init_thin_sized<T: Sized + Arcable>(init: T) -> *mut () {
    let ptr = Box::into_raw(Box::new(ArcInner {
        ref_count: RefCount::new(),
        data: init,
    })) as *mut ();
    ptr
}
