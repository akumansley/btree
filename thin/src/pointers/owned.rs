use crate::pointers::common::impl_thin_ptr_traits;

use std::{
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
};

use crate::Pointable;

pub struct Owned<T: ?Sized + Pointable> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

// Use macro with provided structs:
impl_thin_ptr_traits!(Owned);

impl<T: Pointable + ?Sized> Owned<T> {
    pub fn into_ptr(self) -> *mut () {
        self.as_ptr()
    }
}
