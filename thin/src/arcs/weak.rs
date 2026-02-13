use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use std::{marker::PhantomData, ptr::NonNull};

use serde::{Serialize, Serializer};

use crate::{
    arcs::common::{impl_thin_arc_traits, impl_thin_arc_weak},
    Arcable,
};

pub struct Weak<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_arc_traits!(Weak);
impl_thin_arc_weak!(Weak, Arc);
