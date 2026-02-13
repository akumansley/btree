use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::{marker::PhantomData, mem, ptr::NonNull};

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    arcs::common::{impl_thin_arc_strong, impl_thin_arc_traits},
    Arcable, QsArc,
};

/// Arc is a thin pointer that maintains a reference to a pointee.
///
/// When an Arc is dropped, it will immediately decrement the pointee's reference count.
/// When the reference count reaches 0, the pointee will be dropped immediately.
///
/// This is the non-QSBR counterpart of `QsArc`.
pub struct Arc<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

impl_thin_arc_traits!(Arc);
impl_thin_arc_strong!(Arc, Weak);

impl<T: ?Sized + Arcable + 'static> Drop for Arc<T> {
    fn drop(&mut self) {
        if T::decrement_ref_count(self.ptr.as_ptr()) {
            unsafe { T::drop_arc(self.ptr.as_ptr()) };
        }
    }
}

impl<T: ?Sized + Arcable + 'static> From<QsArc<T>> for Arc<T> {
    fn from(qs_arc: QsArc<T>) -> Self {
        unsafe { Arc::from_ptr(qs_arc.into_ptr()) }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::ops::Deref;

    use super::Arc;

    #[test]
    fn test_thin_arc() {
        let thin_str = Arc::new_from_str("hello");
        assert_eq!(thin_str.len(), 5);
        assert_eq!(format!("hello {}", thin_str.deref()), "hello hello");

        let thin_usize = Arc::new(42);
        assert_eq!(*thin_usize, 42);

        let thin_slice = Arc::new_from_slice(&[1, 2, 3]);
        assert_eq!(thin_slice.len(), 3);
        assert_eq!(thin_slice[0], 1);

        let mut thin_slice_uninitialized = Arc::new_uninitialized(3);
        assert_eq!(thin_slice_uninitialized.len(), 3);
        for i in 0..3 {
            thin_slice_uninitialized[i].write(i as usize);
        }
        let thin_slice_init = unsafe { thin_slice_uninitialized.assume_init() };

        assert_eq!(thin_slice_init.len(), 3);
        assert_eq!(thin_slice_init[1], 1);
    }

    #[test]
    fn test_clone_and_drop() {
        let arc1 = Arc::new(42);
        assert_eq!(arc1.ref_count(), 1);

        let arc2 = arc1.clone();
        assert_eq!(arc1.ref_count(), 2);
        assert_eq!(*arc1, *arc2);

        drop(arc2);
        assert_eq!(arc1.ref_count(), 1);
    }

    #[test]
    fn test_derived_traits() {
        let ptr1 = Arc::new(42);
        let ptr2 = Arc::new(42);
        let ptr3 = Arc::new(43);

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

    #[test]
    fn test_serde() {
        let ptr = Arc::new(42);
        let serialized = serde_json::to_string(&ptr).unwrap();
        let deserialized: Arc<i32> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&*ptr, &*deserialized);
    }

    #[test]
    fn test_serde_array() {
        let array = Arc::new_from_slice(&[1usize, 2, 3, 4, 5]);
        let serialized = serde_json::to_string(&array).unwrap();
        let deserialized: Arc<[usize]> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(array.len(), deserialized.len());
        assert_eq!(&*array, &*deserialized);
    }
}
