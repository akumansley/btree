use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::{marker::PhantomData, mem, ptr::NonNull};

use qsbr::qsbr_reclaimer;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::arcable::{
    init_thin_sized, init_thin_slice, init_thin_slice_uninitialized, init_thin_str,
};
use crate::SendPtr;
use crate::{arcs::common::impl_thin_arc_traits, Arcable, QsWeak};

/// QsArc is a thin pointer that maintains a reference to a pointee.
///
/// When an QsArc is dropped, it will immediately decrement the pointee's reference count.
/// When the reference count reaches 0, the pointee will be dropped via QSBR.
///
/// If the pointee should be dropped immediately, use `QsArc::drop_immediately`.
pub struct QsArc<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_arc_traits!(QsArc);

impl<T: ?Sized + Arcable> QsArc<T> {
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

    pub unsafe fn drop_immediately(thin_arc: QsArc<T>) {
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

    pub fn share(&self) -> QsWeak<T> {
        let ptr = self.ptr;
        unsafe { QsWeak::from_ptr(ptr.as_ptr()) }
    }
}

impl<T: ?Sized + Arcable> Clone for QsArc<T> {
    fn clone(&self) -> Self {
        T::increment_ref_count(self.ptr.as_ptr());
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}
impl<T: Sized + Arcable> QsArc<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }
}

impl QsArc<str> {
    pub fn new_from_str(init: &str) -> Self {
        Self::new_with(|| init_thin_str(init))
    }
}

impl From<&str> for QsArc<str> {
    fn from(value: &str) -> Self {
        Self::new_from_str(value)
    }
}

impl From<String> for QsArc<str> {
    fn from(value: String) -> Self {
        Self::new_from_str(&value)
    }
}

impl From<Box<str>> for QsArc<str> {
    fn from(value: Box<str>) -> Self {
        Self::new_from_str(&value)
    }
}

impl<T: Clone> QsArc<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Self::new_with(|| init_thin_slice(init))
    }
}

impl<T> QsArc<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        Self::new_with(|| init_thin_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> QsArc<[T]> {
        unsafe { QsArc::from_ptr(self.into_ptr()) }
    }
}

impl<T: ?Sized + Arcable + 'static> Drop for QsArc<T> {
    fn drop(&mut self) {
        if T::decrement_ref_count(self.ptr.as_ptr()) {
            let send_ptr = SendPtr::new(self.ptr);
            qsbr_reclaimer().add_callback(Box::new(move || {
                unsafe { T::drop_arc(send_ptr.into_ptr().as_ptr()) };
            }));
        }
    }
}

impl<T: ?Sized + Arcable> std::ops::DerefMut for QsArc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { T::deref_mut_arc(self.ptr.as_ptr()) }
    }
}

// Serde implementations specifically for QsArc
impl<'de, T> Deserialize<'de> for QsArc<T>
where
    T: Deserialize<'de> + Arcable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(QsArc::new)
    }
}

impl<T: Serialize + ?Sized + Arcable> Serialize for QsArc<T> {
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
    type Value = QsArc<[T]>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(len) = seq.size_hint() {
            let mut uninit = QsArc::new_uninitialized(len);
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
            Ok(QsArc::new_from_slice(&vec))
        }
    }
}

// Add array implementations
impl<'de, T> Deserialize<'de> for QsArc<[T]>
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
    use btree_macros::qsbr_test;
    use serde_json;
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::ops::Deref;

    use super::QsArc;

    #[qsbr_test]
    fn test_thin_arc() {
        let thin_str = QsArc::new_from_str("hello");
        assert_eq!(thin_str.len(), 5);
        assert_eq!(format!("hello {}", thin_str.deref()), "hello hello");

        let thin_usize = QsArc::new(42);
        assert_eq!(*thin_usize, 42);

        let thin_slice = QsArc::new_from_slice(&[1, 2, 3]);
        assert_eq!(thin_slice.len(), 3);
        assert_eq!(thin_slice[0], 1);

        let mut thin_slice_uninitialized = QsArc::new_uninitialized(3);
        assert_eq!(thin_slice_uninitialized.len(), 3);
        for i in 0..3 {
            thin_slice_uninitialized[i].write(i as usize);
        }
        let thin_slice_init = unsafe { thin_slice_uninitialized.assume_init() };

        assert_eq!(thin_slice_init.len(), 3);
        assert_eq!(thin_slice_init[1], 1);

        unsafe {
            QsArc::drop_immediately(thin_str);
            QsArc::drop_immediately(thin_slice);
            QsArc::drop_immediately(thin_usize);
            QsArc::drop_immediately(thin_slice_init);
        }
    }

    #[qsbr_test]
    fn test_derived_traits() {
        let ptr1 = QsArc::new(42);
        let ptr2 = QsArc::new(42);
        let ptr3 = QsArc::new(43);

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

        // Different values should hash differently
        let mut hasher3 = DefaultHasher::new();
        ptr3.hash(&mut hasher3);
        assert_ne!(hasher1.finish(), hasher3.finish());
    }

    #[qsbr_test]
    fn test_serde() {
        let ptr = QsArc::new(42);
        let serialized = serde_json::to_string(&ptr).unwrap();
        let deserialized: QsArc<i32> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&*ptr, &*deserialized);
    }

    #[qsbr_test]
    fn test_serde_array() {
        let array = QsArc::new_from_slice(&[1usize, 2, 3, 4, 5]);
        let serialized = serde_json::to_string(&array).unwrap();
        let deserialized: QsArc<[usize]> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(array.len(), deserialized.len());
        assert_eq!(&*array, &*deserialized);
    }

    #[qsbr_test]
    fn test_new_from_slice_with_box_str() {
        let vec = vec![
            String::from("hello").into_boxed_str(),
            String::from("world").into_boxed_str(),
            String::from("test").into_boxed_str(),
        ];

        let thin_slice = QsArc::new_from_slice(&vec);
        assert_eq!(thin_slice.len(), 3);
        assert_eq!(thin_slice[0].as_ref(), "hello");
        assert_eq!(thin_slice[1].as_ref(), "world");
        assert_eq!(thin_slice[2].as_ref(), "test");

        // Test iteration
        let mut iter = thin_slice.iter();
        assert_eq!(iter.next().unwrap().as_ref(), "hello");
        assert_eq!(iter.next().unwrap().as_ref(), "world");
        assert_eq!(iter.next().unwrap().as_ref(), "test");
        assert_eq!(iter.next(), None);

        unsafe {
            QsArc::drop_immediately(thin_slice);
        }
    }
}
