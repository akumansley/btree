use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    cmp::Ordering as CmpOrdering,
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use crate::{
    arcable::{
        init_arc_sized, init_arc_slice, init_arc_slice_uninitialized, init_arc_str, Arcable,
    },
    pointers::{qs_arc_shared::QsArcShared, SendPtr},
};
use qsbr::qsbr_reclaimer;

/// QsArcOwned is a thin pointer that owns its target with reference counting.
///
/// When a QsArcOwned is dropped, it will immediately decrement the pointee's reference count.
/// When the reference count reaches 0, the pointee will be dropped via QSBR.
///
/// If the pointee should be dropped immediately, use `QsArcOwned::drop_immediately`.
pub struct QsArcOwned<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: ?Sized + Arcable + Send> Send for QsArcOwned<T> {}
unsafe impl<T: ?Sized + Arcable + Sync> Sync for QsArcOwned<T> {}

impl<T: ?Sized + Arcable> Deref for QsArcOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { T::deref_arc(self.ptr.as_ptr()) }
    }
}

impl<T: ?Sized + Arcable> DerefMut for QsArcOwned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { T::deref_mut_arc(self.ptr.as_ptr()) }
    }
}

impl<T: ?Sized + Arcable + fmt::Debug> fmt::Debug for QsArcOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + Arcable + fmt::Display> fmt::Display for QsArcOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + Arcable + Hash> Hash for QsArcOwned<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq<T> for QsArcOwned<T> {
    fn eq(&self, other: &T) -> bool {
        T::eq(self.deref(), other)
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq<&T> for QsArcOwned<T> {
    fn eq(&self, other: &&T) -> bool {
        self.deref() == *other
    }
}

impl<T: ?Sized + Arcable + PartialEq> PartialEq for QsArcOwned<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T: ?Sized + Arcable + Eq> Eq for QsArcOwned<T> {}

impl<T: ?Sized + Arcable + PartialOrd> PartialOrd for QsArcOwned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T: ?Sized + Arcable + Ord> Ord for QsArcOwned<T> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (**self).cmp(&**other)
    }
}

impl<T: ?Sized + Arcable> QsArcOwned<T> {
    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let ptr = init();
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }

    pub fn ref_count(&self) -> usize {
        T::ref_count(self.ptr.as_ptr())
    }

    pub unsafe fn drop_immediately(thin_arc: QsArcOwned<T>) {
        let ptr = thin_arc.ptr;
        mem::forget(thin_arc);
        if T::decrement_ref_count(ptr.as_ptr()) {
            unsafe {
                T::drop_arc(ptr.as_ptr());
            }
        }
    }

    pub fn into_ptr(self) -> *mut () {
        let ptr = self.ptr;
        mem::forget(self);
        ptr.as_ptr()
    }

    pub fn share(&self) -> QsArcShared<T> {
        let ptr = self.ptr;
        unsafe { QsArcShared::from_ptr(ptr.as_ptr()) }
    }

    pub unsafe fn from_ptr(ptr: *mut ()) -> QsArcOwned<T> {
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Clone for QsArcOwned<T> {
    fn clone(&self) -> Self {
        T::increment_ref_count(self.ptr.as_ptr());
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: Sized + Arcable> QsArcOwned<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_arc_sized(init))
    }
}

impl QsArcOwned<str> {
    pub fn new_from_str(init: &str) -> Self {
        Self::new_with(|| init_arc_str(init))
    }
}

impl<T: Clone> QsArcOwned<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Self::new_with(|| init_arc_slice(init))
    }
}

impl<T> QsArcOwned<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        Self::new_with(|| init_arc_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> QsArcOwned<[T]> {
        unsafe { QsArcOwned::from_ptr(self.into_ptr()) }
    }
}

impl<T: ?Sized + Arcable + 'static> Drop for QsArcOwned<T> {
    fn drop(&mut self) {
        if T::decrement_ref_count(self.ptr.as_ptr()) {
            let send_ptr = SendPtr::new(self.ptr);
            qsbr_reclaimer().add_callback(Box::new(move || {
                unsafe { T::drop_arc(send_ptr.into_ptr().as_ptr()) };
            }));
        }
    }
}

// Serde implementations specifically for QsArcOwned
impl<'de, T> Deserialize<'de> for QsArcOwned<T>
where
    T: Deserialize<'de> + Arcable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(QsArcOwned::new)
    }
}

impl<T: Serialize + ?Sized + Arcable> Serialize for QsArcOwned<T> {
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
    type Value = QsArcOwned<[T]>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(len) = seq.size_hint() {
            let mut uninit = QsArcOwned::new_uninitialized(len);
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
            Ok(QsArcOwned::new_from_slice(&vec))
        }
    }
}

// Add array implementations
impl<'de, T> Deserialize<'de> for QsArcOwned<[T]>
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::cmp::Ordering as CmpOrdering;
    use std::ops::Deref;

    #[test]
    fn test_arc_basics() {
        let arc_str = QsArcOwned::new_from_str("hello");
        assert_eq!(arc_str.len(), 5);
        assert_eq!(format!("hello {}", arc_str.deref()), "hello hello");

        let arc_usize = QsArcOwned::new(42);
        assert_eq!(*arc_usize, 42);

        let arc_slice = QsArcOwned::new_from_slice(&[1, 2, 3]);
        assert_eq!(arc_slice.len(), 3);
        assert_eq!(arc_slice[0], 1);

        unsafe {
            QsArcOwned::drop_immediately(arc_str);
            QsArcOwned::drop_immediately(arc_slice);
            QsArcOwned::drop_immediately(arc_usize);
        }
    }

    #[test]
    fn test_arc_sharing_and_ref_counting() {
        let arc = QsArcOwned::new(100);
        assert_eq!(arc.ref_count(), 1);
        
        let cloned = arc.clone();
        assert_eq!(arc.ref_count(), 2);
        assert_eq!(cloned.ref_count(), 2);
        
        let shared = arc.share();
        // shared references don't increase ref count
        assert_eq!(arc.ref_count(), 2);
        
        drop(cloned);
        assert_eq!(arc.ref_count(), 1);
        
        unsafe {
            QsArcOwned::drop_immediately(arc);
        }
    }

    #[test]
    fn test_derived_traits() {
        let ptr1 = QsArcOwned::new(42);
        let ptr2 = QsArcOwned::new(42);
        let ptr3 = QsArcOwned::new(43);

        // Test Eq
        assert!(ptr1 == ptr2);
        assert!(ptr1 != ptr3);

        // Test Ord
        assert!(ptr1 < ptr3);
        assert!(ptr3 > ptr1);
        assert_eq!(ptr1.cmp(&ptr2), CmpOrdering::Equal);

        // Test Hash
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        ptr1.hash(&mut hasher1);
        ptr2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }
}
