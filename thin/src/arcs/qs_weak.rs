use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use std::{marker::PhantomData, ptr::NonNull};

use crate::{arcs::common::impl_thin_arc_traits, Arcable};

pub struct QsWeak<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_arc_traits!(QsWeak);

impl<T: ?Sized + Arcable> Clone for QsWeak<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Copy for QsWeak<T> {}
