mod sized;
mod slice;
mod str;
pub use sized::init_thin_sized;
pub use slice::{init_thin_slice, init_thin_slice_uninitialized};
pub use str::init_thin_str;

use std::{
    alloc::Layout,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

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
