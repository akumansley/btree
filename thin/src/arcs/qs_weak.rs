use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use std::{marker::PhantomData, ptr::NonNull};

use serde::{Serialize, Serializer};

use crate::QsArc;
use crate::{arcs::common::impl_thin_arc_traits, Arcable};

pub struct QsWeak<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_arc_traits!(QsWeak);

impl<T: ?Sized + Arcable> QsWeak<T> {
    pub fn must_upgrade(self) -> QsArc<T> {
        let arc_inner_ptr = self.into_ptr();
        T::increment_ref_count(arc_inner_ptr);
        unsafe { QsArc::from_ptr(arc_inner_ptr) }
    }

    pub fn into_ptr(self) -> *mut () {
        let ptr = self.ptr;
        ptr.as_ptr()
    }
}

impl<T: ?Sized + Arcable> Clone for QsWeak<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Copy for QsWeak<T> {}

// it makes sense for QsWeak to implement serialize, but not deserialize, because
// a QsWeak must be derived from a QsArc

impl<T: Serialize + ?Sized + Arcable> Serialize for QsWeak<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}
