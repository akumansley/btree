use std::{alloc::Layout, mem::MaybeUninit};

mod sized;
mod slice;
mod str;
pub use sized::init_thin_sized;
pub use slice::{init_thin_slice, init_thin_slice_uninitialized};
pub use str::init_thin_str;

/// See crossbeam-epoch::Pointable.
pub trait Pointable: Send + 'static {
    /// Dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be mutably dereferenced by [`Pointable::deref_mut`] concurrently.
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self;

    /// Mutably dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self;

    /// Drops the object pointed to by the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn drop(ptr: *mut ());
}

pub trait PointableClone: Pointable {
    unsafe fn clone(ptr: *mut ()) -> *mut ();
}

/// Also copied from crossbeam-epoch::Pointable
#[repr(C)]
pub struct Array<T> {
    /// The number of elements (not the number of bytes).
    pub len: usize,
    pub elements: [MaybeUninit<T>; 0],
}

impl<T> Array<T> {
    pub fn layout(len: usize) -> Layout {
        Layout::new::<Self>()
            .extend(Layout::array::<MaybeUninit<T>>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }
}
