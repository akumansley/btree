use std::{
    marker::PhantomData,
    ptr::{self},
};
use thin::{QsOwned, QsShared};

use crate::sync::{AtomicPtr, Ordering};

use thin::Pointable;

use super::AtomicPointerArrayValue;

pub struct OwnedThinAtomicPtr<T: ?Sized + Pointable> {
    ptr: AtomicPtr<()>,
    _marker: PhantomData<Box<T>>,
}
pub struct SharedThinAtomicPtr<T: ?Sized + Pointable> {
    ptr: AtomicPtr<()>,
    _marker: PhantomData<*const T>,
}

macro_rules! impl_thin_atomic_ptr_traits {
    ($struct_name:ident, $thin_ptr_type:ident) => {
        impl<T: ?Sized + Pointable + Send + 'static> $struct_name<T> {
            pub const fn null() -> Self {
                Self {
                    ptr: AtomicPtr::new(ptr::null_mut()),
                    _marker: PhantomData,
                }
            }

            pub unsafe fn must_load_for_move(&self, order: Ordering) -> $thin_ptr_type<T> {
                let ptr = self.ptr.load(order);
                unsafe { $thin_ptr_type::from_ptr(ptr) }
            }

            pub fn load_shared(&self, order: Ordering) -> Option<QsShared<T>> {
                let ptr = self.ptr.load(order);
                if ptr.is_null() {
                    None
                } else {
                    Some(unsafe { QsShared::from_ptr(ptr) })
                }
            }

            pub fn store(&self, ptr: $thin_ptr_type<T>, order: Ordering) {
                self.ptr.store(ptr.into_ptr(), order);
            }

            pub fn clear(&self, order: Ordering) {
                self.ptr.store(ptr::null_mut(), order);
            }

            pub fn swap(
                &self,
                ptr: $thin_ptr_type<T>,
                order: Ordering,
            ) -> Option<$thin_ptr_type<T>> {
                let old_ptr = self.ptr.swap(ptr.into_ptr(), order);
                if old_ptr.is_null() {
                    None
                } else {
                    Some(unsafe { $thin_ptr_type::from_ptr(old_ptr) })
                }
            }
        }
    };
}

impl_thin_atomic_ptr_traits!(OwnedThinAtomicPtr, QsOwned);
impl_thin_atomic_ptr_traits!(SharedThinAtomicPtr, QsShared);

impl<T: Sized + Pointable + Send + 'static> OwnedThinAtomicPtr<T> {
    pub fn new(ptr: QsOwned<T>) -> Self {
        Self {
            ptr: AtomicPtr::new(ptr.into_ptr()),
            _marker: PhantomData,
        }
    }

    pub unsafe fn load_owned(&self, order: Ordering) -> Option<QsOwned<T>> {
        let ptr = self.ptr.load(order);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsOwned::from_ptr(ptr) })
        }
    }

    pub unsafe fn into_owned(&self, order: Ordering) -> Option<QsOwned<T>> {
        let ptr = self.ptr.load(order);
        if ptr.is_null() {
            panic!("Attempted to load null pointer into owned");
        } else {
            unsafe { Some(QsOwned::from_ptr(ptr)) }
        }
    }
}

impl<T: Send + 'static + ?Sized + Pointable> AtomicPointerArrayValue<T> for OwnedThinAtomicPtr<T> {
    type OwnedPointer = QsOwned<T>;
    type SharedPointer = QsShared<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer");
        }
        unsafe { QsOwned::from_ptr(ptr) }
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { QsShared::from_ptr(ptr) })
        }
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer into owned");
        } else {
            Some(unsafe { QsOwned::from_ptr(ptr) })
        }
    }

    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering) {
        self.ptr.store(matching_ptr.into_ptr(), ordering);
    }

    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer> {
        let old_ptr = self.ptr.swap(matching_ptr.into_ptr(), ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { QsOwned::from_ptr(old_ptr) })
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
    use thin::Owned;

    use super::QsOwned;

    #[qsbr_test]
    fn test_thin_box() {
        let thin_str = QsOwned::new_from_str("hello");
        assert_eq!(thin_str.len(), 5);
        assert_eq!(format!("hello {}", thin_str.deref()), "hello hello");

        let thin_usize = QsOwned::new(42);
        assert_eq!(*thin_usize, 42);

        let thin_slice = QsOwned::new_from_slice(&[1, 2, 3]);
        assert_eq!(thin_slice.len(), 3);
        assert_eq!(thin_slice[0], 1);

        let mut thin_slice_uninitialized = Owned::new_uninitialized(3);
        assert_eq!(thin_slice_uninitialized.len(), 3);
        for i in 0..3 {
            thin_slice_uninitialized[i].write(i as usize);
        }
        let thin_slice_init: QsOwned<[usize]> =
            unsafe { thin_slice_uninitialized.assume_init().into() };

        assert_eq!(thin_slice_init.len(), 3);
        assert_eq!(thin_slice_init[1], 1);

        QsOwned::drop_immediately(thin_str);
        QsOwned::drop_immediately(thin_slice);
        QsOwned::drop_immediately(thin_usize);
        QsOwned::drop_immediately(thin_slice_init);
    }

    #[qsbr_test]
    fn test_derived_traits() {
        let ptr1 = QsOwned::new(42);
        let ptr2 = QsOwned::new(42);
        let ptr3 = QsOwned::new(43);

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
        let ptr = QsOwned::new(42);
        let serialized = serde_json::to_string(&ptr).unwrap();
        let deserialized: QsOwned<i32> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&*ptr, &*deserialized);
    }

    #[qsbr_test]
    fn test_serde_array() {
        let array = QsOwned::new_from_slice(&[1usize, 2, 3, 4, 5]);
        let serialized = serde_json::to_string(&array).unwrap();
        let deserialized: QsOwned<[usize]> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(array.len(), deserialized.len());
        assert_eq!(&*array, &*deserialized);
    }
}
