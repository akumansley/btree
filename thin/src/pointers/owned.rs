use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::QsOwned;
use crate::{
    pointable::{
        init_thin_sized, init_thin_slice, init_thin_slice_uninitialized, init_thin_str, Array,
        PointableClone,
    },
    pointers::common::impl_thin_ptr_traits,
};

use std::{
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{forget, MaybeUninit},
    ops::{Deref, DerefMut},
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
        let ptr = self.as_ptr();
        forget(self);
        ptr
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

impl<T: PointableClone + ?Sized> Clone for Owned<T> {
    fn clone(&self) -> Self {
        unsafe { Owned::new_with(|| T::clone(self.as_ptr())) }
    }
}

/* Constructors */

impl Owned<str> {
    pub fn new_from_str(init: &str) -> Self {
        Owned::new_with(|| init_thin_str(init))
    }
}

impl<T: Send + 'static + Clone> Owned<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Owned::new_with(|| init_thin_slice(init))
    }
}

impl<T: Pointable> Owned<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
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

impl<T: ?Sized + Pointable> Drop for Owned<T> {
    fn drop(&mut self) {
        unsafe { T::drop(self.as_ptr()) }
    }
}

/* IntoIter */

pub struct ThinSliceIntoIter<T: Send + 'static> {
    ptr: Owned<[T]>,
    position: usize,
}

impl<T: Pointable> Iterator for ThinSliceIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let array_ptr = self.ptr.as_ptr() as *const Array<T>;
        unsafe {
            let len = (*array_ptr).len;
            if self.position < len {
                let elements_ptr = (*array_ptr).elements.as_ptr();
                let item = elements_ptr.add(self.position).read().assume_init_read();
                self.position += 1;
                Some(item)
            } else {
                None
            }
        }
    }
}

impl<T: Send + 'static> IntoIterator for Owned<[T]> {
    type Item = T;
    type IntoIter = ThinSliceIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        ThinSliceIntoIter {
            ptr: self,
            position: 0,
        }
    }
}

impl<T: ?Sized + Pointable> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { T::deref_mut(self.as_ptr()) }
    }
}

/** Serde **/

impl<'de, T> Deserialize<'de> for Owned<T>
where
    T: Deserialize<'de> + Pointable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Owned::new)
    }
}

impl<T: Serialize + ?Sized + Pointable> Serialize for Owned<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

struct ThinArrayDeserializer<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<'de, T> Visitor<'de> for ThinArrayDeserializer<T>
where
    T: Deserialize<'de> + Send + 'static + Clone,
{
    type Value = Owned<[T]>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(len) = seq.size_hint() {
            let mut uninit = Owned::new_uninitialized(len);
            for i in 0..len {
                if let Some(value) = seq.next_element()? {
                    uninit[i].write(value);
                } else {
                    return Err(serde::de::Error::invalid_length(
                        i,
                        &format!("expected {} elements", len).as_str(),
                    ));
                }
            }
            Ok(unsafe { uninit.assume_init() })
        } else {
            let mut vec = Vec::new();
            while let Some(value) = seq.next_element()? {
                vec.push(value);
            }
            Ok(Owned::new_from_slice(&vec))
        }
    }
}

impl<'de, T> Deserialize<'de> for Owned<[T]>
where
    T: Deserialize<'de> + Send + 'static + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ThinArrayDeserializer {
            _phantom: std::marker::PhantomData,
        })
    }
}
