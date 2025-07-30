use super::common::impl_thin_ptr_traits;
use super::Owned;

use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{forget, MaybeUninit},
    ops::Deref,
    ptr::NonNull,
};

use crate::{
    pointable::{
        init_thin_sized, init_thin_slice, init_thin_slice_uninitialized, init_thin_str,
        PointableClone,
    },
    pointers::{qs_shared::QsShared, SendPtr},
    Pointable,
};
use qsbr::qsbr_reclaimer;

/// QsOwned is a thin pointer that owns its target. Unlike `Box<T>` it can be aliased.
/// By default, they drop their pointee via qsbr when dropped.
pub struct QsOwned<T: ?Sized + Pointable> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_ptr_traits!(QsOwned);

impl<T: Send + 'static + Clone> QsOwned<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        QsOwned::new_with(|| init_thin_slice(init))
    }
}

impl<T: Send + 'static> QsOwned<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        QsOwned::new_with(|| init_thin_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> QsOwned<[T]> {
        unsafe { QsOwned::from_ptr(self.into_ptr()) }
    }
}

impl QsOwned<str> {
    pub fn new_from_str(init: &str) -> Self {
        QsOwned::new_with(|| init_thin_str(init))
    }
}

impl<T: Pointable + ?Sized> QsOwned<T> {
    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let ptr = init();
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }
    pub fn into_ptr(self) -> *mut () {
        let ptr = self.as_ptr();
        forget(self);
        ptr
    }

    pub fn drop_immediately(thin_ptr: Self) {
        let ptr = thin_ptr.as_ptr();
        forget(thin_ptr);
        unsafe { T::drop(ptr) };
    }
    pub fn share(&self) -> QsShared<T> {
        unsafe { QsShared::from_ptr(self.as_ptr()) }
    }
}

impl<T: PointableClone + ?Sized> Clone for QsOwned<T> {
    fn clone(&self) -> Self {
        unsafe { QsOwned::new_with(|| T::clone(self.as_ptr())) }
    }
}

impl<T: Pointable> QsOwned<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }
}

impl<T: ?Sized + Pointable> Drop for QsOwned<T> {
    fn drop(&mut self) {
        let send_ptr = SendPtr::new(self.ptr);
        qsbr_reclaimer().add_callback(Box::new(move || {
            unsafe { T::drop(send_ptr.into_ptr().as_ptr()) };
        }));
    }
}

/** Serde **/

impl<'de, T> Deserialize<'de> for QsOwned<T>
where
    T: Deserialize<'de> + Pointable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(QsOwned::new)
    }
}

impl<T: Serialize + ?Sized + Pointable> Serialize for QsOwned<T> {
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
    type Value = QsOwned<[T]>;

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
            Ok(unsafe { uninit.assume_init().into() })
        } else {
            let mut vec = Vec::new();
            while let Some(value) = seq.next_element()? {
                vec.push(value);
            }
            Ok(QsOwned::new_from_slice(&vec))
        }
    }
}

impl<'de, T> Deserialize<'de> for QsOwned<[T]>
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

impl<T: ?Sized + Pointable> From<Owned<T>> for QsOwned<T> {
    fn from(owned: Owned<T>) -> Self {
        unsafe { QsOwned::from_ptr(owned.into_ptr()) }
    }
}
