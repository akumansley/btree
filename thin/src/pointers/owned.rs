use super::QsOwned;
use crate::{
    pointable::{init_thin_slice_uninitialized, init_thin_str},
    pointers::common::impl_thin_ptr_traits,
};

use std::{
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::MaybeUninit,
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

    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let ptr = init();
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Pointable> From<QsOwned<T>> for Owned<T> {
    fn from(qs_owned: QsOwned<T>) -> Self {
        unsafe { Owned::from_ptr(qs_owned.into_ptr()) }
    }
}

impl Owned<str> {
    pub fn new_from_str(init: &str) -> Self {
        Owned::new_with(|| init_thin_str(init))
    }
}

impl<T: Send + 'static> Owned<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        Owned::new_with(|| init_thin_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> Owned<[T]> {
        unsafe { Owned::from_ptr(self.into_ptr()) }
    }
}
