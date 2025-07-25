use std::{
    alloc::{self, Layout},
    cmp::Ordering as CmpOrdering,
    fmt::{self},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::{self, NonNull},
    slice,
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
};

use crate::{
    pointers::traits::SendPtr,
    qsbr_reclaimer,
    sync::{AtomicPtr, Ordering},
};

use super::AtomicPointerArrayValue;

/// See crossbeam-epoch::Pointable.
pub trait Pointable: Send + 'static {
    /// Dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be mutably dereferenced by [`Pointable::deref_mut`] concurrently.
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self;

    /// Mutably dereferences the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self;

    /// Drops the object pointed to by the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` should not have yet been dropped by [`Pointable::drop`].
    /// - `ptr` should not be dereferenced by [`Pointable::deref`] or [`Pointable::deref_mut`]
    ///   concurrently.
    unsafe fn drop(ptr: *mut ());
}

pub trait ThinClone: Pointable {
    unsafe fn clone(ptr: *mut ()) -> *mut ();
}

impl<T: Pointable + Sized + Clone> ThinClone for T {
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let value = unsafe { T::deref(ptr).clone() };
        init_thin_sized(value)
    }
}

fn init_thin_sized<T: Sized>(init: T) -> *mut () {
    Box::into_raw(Box::new(init)) as *mut ()
}

// the sized bound is implicit, but I'm adding it here for clarity
impl<T: Sized + Send + 'static> Pointable for T {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        unsafe { &*(ptr as *const T) }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        unsafe { &mut *(ptr as *mut T) }
    }

    unsafe fn drop(ptr: *mut ()) {
        unsafe { drop(Box::from_raw(ptr as *mut T)) };
    }
}

/// Also copied from crossbeam-epoch::Pointable
#[repr(C)]
struct Array<T> {
    /// The number of elements (not the number of bytes).
    len: usize,
    elements: [MaybeUninit<T>; 0],
}

impl<T> Array<T> {
    fn layout(len: usize) -> Layout {
        Layout::new::<Self>()
            .extend(Layout::array::<MaybeUninit<T>>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }
}

fn init_thin_slice<'a, T: Clone>(init: &'a [T]) -> *mut () {
    let layout = Array::<T>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<T>;
        let slice = slice::from_raw_parts_mut(elements_ptr, init.len());
        for (i, elem) in slice.iter_mut().enumerate() {
            elem.write(init[i].clone());
        }
        ptr as *mut ()
    }
}

fn init_thin_slice_uninitialized<T>(len: usize) -> *mut () {
    let layout = Array::<T>::layout(len);
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(len);
        ptr as *mut ()
    }
}

impl<T: Send + 'static> Pointable for [T] {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        unsafe {
            let array = &*(ptr as *const Array<T>);
            slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len)
        }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        unsafe {
            let array = &mut *(ptr as *mut Array<T>);
            slice::from_raw_parts_mut(array.elements.as_mut_ptr() as *mut _, array.len)
        }
    }

    unsafe fn drop(ptr: *mut ()) {
        unsafe {
            let len = (*(ptr as *mut Array<T>)).len;
            let layout = Array::<T>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

impl<T: Clone> ThinClone for [T]
where
    [T]: Pointable,
{
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let slice = unsafe { <[T] as Pointable>::deref(ptr) };
        init_thin_slice(slice)
    }
}

fn init_thin_str<'a>(init: &'a str) -> *mut () {
    let layout = Array::<u8>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<Array<u8>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<u8>;
        ptr::copy_nonoverlapping(
            init.as_ptr() as *const MaybeUninit<u8>,
            elements_ptr,
            init.len(),
        );
        ptr as *mut ()
    }
}

impl Pointable for str {
    unsafe fn deref<'a>(ptr: *mut ()) -> &'a Self {
        let array = unsafe { &*(ptr as *const Array<u8>) };
        unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                array.elements.as_ptr() as *const _,
                array.len,
            ))
        }
    }

    unsafe fn deref_mut<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = unsafe { &mut *(ptr as *mut Array<u8>) };
        unsafe {
            std::str::from_utf8_unchecked_mut(slice::from_raw_parts_mut(
                array.elements.as_mut_ptr() as *mut _,
                array.len,
            ))
        }
    }

    unsafe fn drop(ptr: *mut ()) {
        unsafe {
            let len = (*(ptr as *mut Array<u8>)).len;
            let layout = Array::<u8>::layout(len);
            alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

impl ThinClone for str {
    unsafe fn clone(ptr: *mut ()) -> *mut () {
        let s = unsafe { <str as Pointable>::deref(ptr) };
        init_thin_str(s)
    }
}

/// OwnedThinPtr is a thin pointer that owns its target. Unlike `Box<T>` it can be aliased.
/// By default, they drop their pointee via qsbr when dropped.
pub struct OwnedThinPtr<T: ?Sized + Pointable> {
    ptr: NonNull<()>,
    _marker: PhantomData<Box<T>>,
}

pub struct SharedThinPtr<T: ?Sized + Pointable> {
    ptr: NonNull<()>,
    _marker: PhantomData<*const T>,
}

macro_rules! impl_thin_ptr_traits {
    ($struct_name:ident) => {
        impl<T: ?Sized + Pointable> fmt::Debug for $struct_name<T> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.debug_struct(stringify!($struct_name))
                    .field("ptr", &self.ptr)
                    .finish()
            }
        }

        impl<T: ?Sized + Pointable> Deref for $struct_name<T> {
            type Target = T;
            fn deref(&self) -> &Self::Target {
                unsafe { T::deref(self.as_ptr()) }
            }
        }

        impl<T: ?Sized + Pointable + Hash> Hash for $struct_name<T> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                (**self).hash(state)
            }
        }

        impl<T: ?Sized + Pointable + PartialEq> PartialEq<T> for $struct_name<T> {
            fn eq(&self, other: &T) -> bool {
                T::eq(self.deref(), other)
            }
        }

        impl<T: ?Sized + Pointable + PartialEq> PartialEq<&T> for $struct_name<T> {
            fn eq(&self, other: &&T) -> bool {
                self.deref() == *other
            }
        }

        impl<T: ?Sized + Pointable + PartialEq> PartialEq for $struct_name<T> {
            fn eq(&self, other: &Self) -> bool {
                **self == **other
            }
        }

        impl<T: ?Sized + Pointable + Eq> Eq for $struct_name<T> {}

        impl<T: ?Sized + Pointable + PartialOrd> PartialOrd for $struct_name<T> {
            fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
                (**self).partial_cmp(&**other)
            }
        }

        impl<T: ?Sized + Pointable + Ord> Ord for $struct_name<T> {
            fn cmp(&self, other: &Self) -> CmpOrdering {
                (**self).cmp(&**other)
            }
        }

        impl<T: ?Sized + Pointable> $struct_name<T> {
            fn as_ptr(&self) -> *mut () {
                self.ptr.as_ptr()
            }

            pub unsafe fn from_ptr(ptr: *mut ()) -> Self {
                Self {
                    ptr: NonNull::new(ptr).unwrap(),
                    _marker: PhantomData,
                }
            }
            pub unsafe fn cast<U: Pointable>(self) -> $struct_name<U> {
                let ptr = self.into_ptr();
                unsafe { $struct_name::<U>::from_ptr(ptr) }
            }
            pub fn share(&self) -> SharedThinPtr<T> {
                unsafe { SharedThinPtr::from_ptr(self.as_ptr()) }
            }
        }

        unsafe impl<T: ?Sized + Pointable + Send> Send for $struct_name<T> {}
        unsafe impl<T: ?Sized + Pointable + Sync> Sync for $struct_name<T> {}
    };
}

// Use macro with provided structs:
impl_thin_ptr_traits!(OwnedThinPtr);
impl_thin_ptr_traits!(SharedThinPtr);

impl<T: Send + 'static + Clone> OwnedThinPtr<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        OwnedThinPtr::new_with(|| init_thin_slice(init))
    }
}

impl<T: Send + 'static> OwnedThinPtr<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        OwnedThinPtr::new_with(|| init_thin_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> OwnedThinPtr<[T]> {
        unsafe { OwnedThinPtr::from_ptr(self.into_ptr()) }
    }
}

impl OwnedThinPtr<str> {
    pub fn new_from_str(init: &str) -> Self {
        OwnedThinPtr::new_with(|| init_thin_str(init))
    }
}

impl<T: Pointable + ?Sized> OwnedThinPtr<T> {
    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let ptr = init();
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _marker: PhantomData,
        }
    }
    pub fn into_ptr(self) -> *mut () {
        let ptr = self.as_ptr();
        mem::forget(self);
        ptr
    }

    pub fn drop_immediately(thin_ptr: Self) {
        let ptr = thin_ptr.as_ptr();
        mem::forget(thin_ptr);
        unsafe { T::drop(ptr) };
    }
}

impl<T: ThinClone + ?Sized> Clone for OwnedThinPtr<T> {
    fn clone(&self) -> Self {
        unsafe { OwnedThinPtr::new_with(|| T::clone(self.as_ptr())) }
    }
}

impl<T: Pointable + ?Sized> Clone for SharedThinPtr<T> {
    fn clone(&self) -> Self {
        SharedThinPtr {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: Pointable + ?Sized> SharedThinPtr<T> {
    pub fn into_ptr(self) -> *mut () {
        self.as_ptr()
    }
}

impl<T: Pointable + ?Sized> Copy for SharedThinPtr<T> {}

impl<T: Pointable> OwnedThinPtr<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }
}

impl<T: ?Sized + Pointable> Drop for OwnedThinPtr<T> {
    fn drop(&mut self) {
        let send_ptr = SendPtr::new(self.ptr);
        qsbr_reclaimer().add_callback(Box::new(move || {
            unsafe { T::drop(send_ptr.into_ptr().as_ptr()) };
        }));
    }
}

impl<T: ?Sized + Pointable> DerefMut for OwnedThinPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { T::deref_mut(self.as_ptr()) }
    }
}

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

            pub fn load_shared(&self, order: Ordering) -> Option<SharedThinPtr<T>> {
                let ptr = self.ptr.load(order);
                if ptr.is_null() {
                    None
                } else {
                    Some(unsafe { SharedThinPtr::from_ptr(ptr) })
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

impl_thin_atomic_ptr_traits!(OwnedThinAtomicPtr, OwnedThinPtr);
impl_thin_atomic_ptr_traits!(SharedThinAtomicPtr, SharedThinPtr);

impl<T: Sized + Pointable + Send + 'static> OwnedThinAtomicPtr<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }

    pub fn new_with<C: FnOnce() -> *mut ()>(init: C) -> Self {
        let value = init();
        Self {
            ptr: AtomicPtr::new(value),
            _marker: PhantomData,
        }
    }

    pub unsafe fn load_owned(&self, order: Ordering) -> Option<OwnedThinPtr<T>> {
        let ptr = self.ptr.load(order);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinPtr::from_ptr(ptr) })
        }
    }

    pub unsafe fn into_owned(&self, order: Ordering) -> Option<OwnedThinPtr<T>> {
        let ptr = self.ptr.load(order);
        if ptr.is_null() {
            panic!("Attempted to load null pointer into owned");
        } else {
            unsafe { Some(OwnedThinPtr::from_ptr(ptr)) }
        }
    }
}

impl<T: Send + 'static + ?Sized + Pointable> AtomicPointerArrayValue<T> for OwnedThinAtomicPtr<T> {
    type OwnedPointer = OwnedThinPtr<T>;
    type SharedPointer = SharedThinPtr<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer");
        }
        unsafe { OwnedThinPtr::from_ptr(ptr) }
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { SharedThinPtr::from_ptr(ptr) })
        }
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        let ptr = self.ptr.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer into owned");
        } else {
            Some(unsafe { OwnedThinPtr::from_ptr(ptr) })
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
            Some(unsafe { OwnedThinPtr::from_ptr(old_ptr) })
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

    use super::OwnedThinPtr;

    #[qsbr_test]
    fn test_thin_box() {
        let thin_str = OwnedThinPtr::new_from_str("hello");
        assert_eq!(thin_str.len(), 5);
        assert_eq!(format!("hello {}", thin_str.deref()), "hello hello");

        let thin_usize = OwnedThinPtr::new(42);
        assert_eq!(*thin_usize, 42);

        let thin_slice = OwnedThinPtr::new_from_slice(&[1, 2, 3]);
        assert_eq!(thin_slice.len(), 3);
        assert_eq!(thin_slice[0], 1);

        let mut thin_slice_uninitialized = OwnedThinPtr::new_uninitialized(3);
        assert_eq!(thin_slice_uninitialized.len(), 3);
        for i in 0..3 {
            thin_slice_uninitialized[i].write(i as usize);
        }
        let thin_slice_init = unsafe { thin_slice_uninitialized.assume_init() };

        assert_eq!(thin_slice_init.len(), 3);
        assert_eq!(thin_slice_init[1], 1);

        OwnedThinPtr::drop_immediately(thin_str);
        OwnedThinPtr::drop_immediately(thin_slice);
        OwnedThinPtr::drop_immediately(thin_usize);
        OwnedThinPtr::drop_immediately(thin_slice_init);
    }

    #[qsbr_test]
    fn test_derived_traits() {
        let ptr1 = OwnedThinPtr::new(42);
        let ptr2 = OwnedThinPtr::new(42);
        let ptr3 = OwnedThinPtr::new(43);

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
        let ptr = OwnedThinPtr::new(42);
        let serialized = serde_json::to_string(&ptr).unwrap();
        let deserialized: OwnedThinPtr<i32> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&*ptr, &*deserialized);
    }

    #[qsbr_test]
    fn test_serde_array() {
        let array = OwnedThinPtr::new_from_slice(&[1usize, 2, 3, 4, 5]);
        let serialized = serde_json::to_string(&array).unwrap();
        let deserialized: OwnedThinPtr<[usize]> = serde_json::from_str(&serialized).unwrap();

        assert_eq!(array.len(), deserialized.len());
        assert_eq!(&*array, &*deserialized);
    }
}

// Serde implementations for OwnedThinPtr
impl<'de, T> Deserialize<'de> for OwnedThinPtr<T>
where
    T: Deserialize<'de> + Pointable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(OwnedThinPtr::new)
    }
}

impl<T: Serialize + ?Sized + Pointable> Serialize for OwnedThinPtr<T> {
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
    type Value = OwnedThinPtr<[T]>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(len) = seq.size_hint() {
            let mut uninit = OwnedThinPtr::new_uninitialized(len);
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
            Ok(OwnedThinPtr::new_from_slice(&vec))
        }
    }
}

impl<'de, T> Deserialize<'de> for OwnedThinPtr<[T]>
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
