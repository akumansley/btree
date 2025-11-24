use serde::{Serialize, Serializer};

use super::common::impl_thin_ptr_traits;
use std::{
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
};

use crate::Pointable;
pub struct QsShared<T: ?Sized + Pointable> {
    ptr: NonNull<()>,
    _marker: PhantomData<*const T>,
}

impl_thin_ptr_traits!(QsShared);

impl<T: Pointable + ?Sized> Clone for QsShared<T> {
    fn clone(&self) -> Self {
        QsShared {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: Pointable + ?Sized> QsShared<T> {
    pub fn into_ptr(self) -> *mut () {
        self.as_ptr()
    }
    pub fn share(&self) -> QsShared<T> {
        unsafe { QsShared::from_ptr(self.as_ptr()) }
    }
}

impl<T: Pointable + ?Sized> Copy for QsShared<T> {}

// QsShared implements Serialize but not Deserialize, because it
// must be derived from a QsOwned

impl<T: Serialize + ?Sized + Pointable> Serialize for QsShared<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}
