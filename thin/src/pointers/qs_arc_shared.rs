use std::{
    cmp::Ordering as CmpOrdering,
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
};

use crate::arcable::Arcable;

pub struct QsArcShared<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*const T>,
}

unsafe impl<T: ?Sized + Arcable + Send> Send for QsArcShared<T> {}
unsafe impl<T: ?Sized + Arcable + Sync> Sync for QsArcShared<T> {}

impl<T: ?Sized + Arcable> Deref for QsArcShared<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { T::deref_arc(self.ptr.as_ptr()) }
    }
}

impl<T: ?Sized + Arcable + fmt::Debug> fmt::Debug for QsArcShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + Arcable + fmt::Display> fmt::Display for QsArcShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + Arcable + Hash> Hash for QsArcShared<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq<T> for QsArcShared<T> {
    fn eq(&self, other: &T) -> bool {
        T::eq(self.deref(), other)
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq<&T> for QsArcShared<T> {
    fn eq(&self, other: &&T) -> bool {
        self.deref() == *other
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq for QsArcShared<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T: ?Sized + Arcable + Eq> Eq for QsArcShared<T> {}

impl<T: ?Sized + Arcable + PartialOrd> PartialOrd for QsArcShared<T> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T: ?Sized + Arcable + Ord> Ord for QsArcShared<T> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (**self).cmp(&**other)
    }
}

impl<T: ?Sized + Arcable> Clone for QsArcShared<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Copy for QsArcShared<T> {}

impl<T: ?Sized + Arcable> QsArcShared<T> {
    pub fn into_ptr(self) -> *mut () {
        self.ptr.as_ptr()
    }

    pub fn share(&self) -> QsArcShared<T> {
        unsafe { QsArcShared::from_ptr(self.ptr.as_ptr()) }
    }

    pub unsafe fn from_ptr(ptr: *mut ()) -> Self {
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }
}
