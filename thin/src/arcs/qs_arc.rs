use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::{marker::PhantomData, mem, ptr::NonNull};

use qsbr::qsbr_reclaimer;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::SendPtr;
use crate::{
    arcs::common::{impl_thin_arc_strong, impl_thin_arc_traits},
    Arcable,
};

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
impl_thin_arc_strong!(QsArc, QsWeak);

impl<T: ?Sized + Arcable> QsArc<T> {
    pub unsafe fn drop_immediately(thin_arc: QsArc<T>) {
        let ptr = thin_arc.into_ptr();
        if T::decrement_ref_count(ptr) {
            unsafe {
                T::drop_arc(ptr);
            }
        }
    }
}

impl<T: ?Sized + Arcable + 'static> From<crate::Arc<T>> for QsArc<T> {
    fn from(arc: crate::Arc<T>) -> Self {
        unsafe { QsArc::from_ptr(arc.into_ptr()) }
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
