use std::{
    alloc::{self, Layout},
    cmp::Ordering as CmpOrdering,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ops::Deref,
    ptr::{self, NonNull},
    slice,
};

use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::qsbr_reclaimer;
use crate::sync::{AtomicPtr, AtomicUsize, Ordering};

use super::{traits::SendPtr, AtomicPointerArrayValue};

struct RefCount {
    count: AtomicUsize,
}

impl RefCount {
    fn new() -> Self {
        let result = Self {
            count: AtomicUsize::new(1),
        };
        result
    }

    fn increment(&self) -> bool {
        loop {
            let old_ref_count = self.count.load(Ordering::Relaxed);
            if old_ref_count == 0 {
                panic!("attempted to increment a ref_count of 0");
            }
            if self
                .count
                .compare_exchange(
                    old_ref_count,
                    old_ref_count + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            } else {
                return true;
            }
        }
    }

    fn decrement(&self) -> bool {
        let old_count = self.count.fetch_sub(1, Ordering::Relaxed);
        if old_count == 0 {
            panic!("attempted to decrement a ref_count of 0 {:p}", self);
        }
        let result = old_count == 1;
        result
    }

    fn load(&self) -> usize {
        let count = self.count.load(Ordering::Relaxed);
        count
    }
}

pub trait Arcable {
    fn ref_count(ptr: *mut ()) -> usize;
    fn increment_ref_count(ptr: *mut ());
    fn decrement_ref_count(ptr: *mut ()) -> bool;
    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self;
    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self;
    unsafe fn drop_arc(ptr: *mut ());
}

#[repr(C)]
struct ArcArray<T> {
    ref_count: RefCount,
    /// The number of elements (not the number of bytes).
    len: usize,
    elements: [MaybeUninit<T>; 0],
}

impl<T> ArcArray<T> {
    fn layout(len: usize) -> Layout {
        Layout::new::<Self>()
            .extend(Layout::array::<MaybeUninit<T>>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }
}

// Arcable impl for Sized types

impl<T: Sized> Arcable for T {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &(*arc_inner_ptr).data }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { &mut (*arc_inner_ptr).data }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcInner<T>;
        unsafe { drop(Box::from_raw(arc_inner_ptr)) };
    }
}

struct ArcInner<T: Sized> {
    ref_count: RefCount,
    data: T,
}

fn init_thin_sized<T: Sized + Arcable>(init: T) -> *mut () {
    let ptr = Box::into_raw(Box::new(ArcInner {
        ref_count: RefCount::new(),
        data: init,
    })) as *mut ();
    ptr
}

/// Arcable impl for str

fn init_thin_str(init: &str) -> *mut () {
    let layout = ArcArray::<u8>::layout(init.len());
    let ptr = unsafe { alloc::alloc(layout).cast::<ArcArray<u8>>() };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    unsafe {
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
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

impl Arcable for str {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcArray<u8>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let array = &*(ptr as *const ArcArray<u8>);
        unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                array.elements.as_ptr() as *const _,
                array.len,
            ))
        }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = &mut *(ptr as *mut ArcArray<u8>);
        unsafe {
            std::str::from_utf8_unchecked_mut(slice::from_raw_parts_mut(
                array.elements.as_mut_ptr() as *mut _,
                array.len,
            ))
        }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let len = (*(ptr as *mut ArcArray<u8>)).len;
        let layout = ArcArray::<u8>::layout(len);
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

/// Arcable impl for slices
fn init_thin_slice<T>(init: &[T]) -> *mut () {
    let layout = ArcArray::<T>::layout(init.len());
    unsafe {
        let ptr = alloc::alloc(layout).cast::<ArcArray<T>>();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr::addr_of_mut!((*ptr).len).write(init.len());
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
        let elements_ptr = ptr::addr_of_mut!((*ptr).elements) as *mut MaybeUninit<T>;
        ptr::copy_nonoverlapping(
            init.as_ptr() as *const MaybeUninit<T>,
            elements_ptr,
            init.len(),
        );
        ptr as *mut ()
    }
}

fn init_thin_slice_uninitialized<T>(len: usize) -> *mut () {
    let layout = ArcArray::<T>::layout(len);
    let ptr = unsafe { alloc::alloc(layout).cast::<ArcArray<T>>() };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    unsafe {
        ptr::addr_of_mut!((*ptr).len).write(len);
        ptr::addr_of_mut!((*ptr).ref_count).write(RefCount::new());
        ptr as *mut ()
    }
}

impl<T> Arcable for [T] {
    fn ref_count(ptr: *mut ()) -> usize {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &*arc_inner_ptr }.ref_count.load()
    }

    fn increment_ref_count(ptr: *mut ()) {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.increment();
    }

    fn decrement_ref_count(ptr: *mut ()) -> bool {
        let arc_inner_ptr = ptr as *mut ArcArray<T>;
        unsafe { &mut *arc_inner_ptr }.ref_count.decrement()
    }

    unsafe fn deref_arc<'a>(ptr: *mut ()) -> &'a Self {
        let array = &*(ptr as *const ArcArray<T>);
        unsafe { slice::from_raw_parts(array.elements.as_ptr() as *const _, array.len) }
    }

    unsafe fn deref_mut_arc<'a>(ptr: *mut ()) -> &'a mut Self {
        let array = &mut *(ptr as *mut ArcArray<T>);
        unsafe { slice::from_raw_parts_mut(array.elements.as_mut_ptr() as *mut _, array.len) }
    }

    unsafe fn drop_arc(ptr: *mut ()) {
        let len = (*(ptr as *mut ArcArray<T>)).len;
        let layout = ArcArray::<T>::layout(len);
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

/// OwnedThinArc is a thin pointer that owns its target.
///
/// When an OwnedThinArc is dropped, it will immediately decrement the pointee's reference count.
/// When the reference count reaches 0, the pointee will be dropped via QSBR.
///
/// If the pointee should be dropped immediately, use `OwnedThinArc::drop_immediately`.
pub struct OwnedThinArc<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

pub struct SharedThinArc<T: ?Sized + Arcable + 'static> {
    ptr: NonNull<()>,
    _marker: PhantomData<*mut T>,
}

macro_rules! impl_thin_arc_traits {
    ($arc_type:ident) => {
        unsafe impl<T: ?Sized + Arcable + Send> Send for $arc_type<T> {}
        unsafe impl<T: ?Sized + Arcable + Sync> Sync for $arc_type<T> {}

        impl<T: ?Sized + Arcable> std::ops::Deref for $arc_type<T> {
            type Target = T;
            fn deref(&self) -> &T {
                unsafe { T::deref_arc(self.ptr.as_ptr()) }
            }
        }

        impl<T: ?Sized + Arcable + std::fmt::Debug> std::fmt::Debug for $arc_type<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + Arcable + std::fmt::Display> std::fmt::Display for $arc_type<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + Arcable> $arc_type<T> {
            unsafe fn from_ptr(ptr: *mut ()) -> $arc_type<T> {
                Self {
                    ptr: NonNull::new(ptr).unwrap(),
                    _marker: PhantomData,
                }
            }

            pub fn share(&self) -> SharedThinArc<T> {
                let ptr = self.ptr;
                unsafe { SharedThinArc::from_ptr(ptr.as_ptr()) }
            }
        }

        impl<T: ?Sized + Arcable + Hash> Hash for $arc_type<T> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                (**self).hash(state)
            }
        }

        impl<T: ?Sized + Arcable + PartialEq> PartialEq<T> for $arc_type<T> {
            fn eq(&self, other: &T) -> bool {
                T::eq(self.deref(), other)
            }
        }

        impl<T: ?Sized + Arcable + PartialEq> PartialEq<&T> for $arc_type<T> {
            fn eq(&self, other: &&T) -> bool {
                self.deref() == *other
            }
        }

        impl<T: ?Sized + Arcable + PartialEq> PartialEq for $arc_type<T> {
            fn eq(&self, other: &Self) -> bool {
                **self == **other
            }
        }

        impl<T: ?Sized + Arcable + Eq> Eq for $arc_type<T> {}

        impl<T: ?Sized + Arcable + PartialOrd> PartialOrd for $arc_type<T> {
            fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
                (**self).partial_cmp(&**other)
            }
        }

        impl<T: ?Sized + Arcable + Ord> Ord for $arc_type<T> {
            fn cmp(&self, other: &Self) -> CmpOrdering {
                (**self).cmp(&**other)
            }
        }
    };
}

impl_thin_arc_traits!(OwnedThinArc);
impl_thin_arc_traits!(SharedThinArc);

impl<T: ?Sized + Arcable> OwnedThinArc<T> {
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

    pub unsafe fn drop_immediately(thin_arc: OwnedThinArc<T>) {
        let ptr = thin_arc.ptr;
        mem::forget(thin_arc);
        if T::decrement_ref_count(ptr.as_ptr()) {
            T::drop_arc(ptr.as_ptr());
        }
    }

    pub fn into_ptr(self) -> *mut () {
        let ptr = self.ptr;
        mem::forget(self);
        ptr.as_ptr()
    }
}

impl<T: ?Sized + Arcable> Clone for OwnedThinArc<T> {
    fn clone(&self) -> Self {
        T::increment_ref_count(self.ptr.as_ptr());
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Clone for SharedThinArc<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<T: ?Sized + Arcable> Copy for SharedThinArc<T> {}

impl<T: Sized + Arcable> OwnedThinArc<T> {
    pub fn new(init: T) -> Self {
        Self::new_with(|| init_thin_sized(init))
    }
}

impl OwnedThinArc<str> {
    pub fn new_from_str(init: &str) -> Self {
        Self::new_with(|| init_thin_str(init))
    }
}

impl<T> OwnedThinArc<[T]> {
    pub fn new_from_slice(init: &[T]) -> Self {
        Self::new_with(|| init_thin_slice(init))
    }
}

impl<T> OwnedThinArc<[MaybeUninit<T>]> {
    pub fn new_uninitialized(len: usize) -> Self {
        Self::new_with(|| init_thin_slice_uninitialized::<T>(len))
    }

    pub unsafe fn assume_init(self) -> OwnedThinArc<[T]> {
        OwnedThinArc::from_ptr(self.into_ptr())
    }
}

impl<T: ?Sized + Arcable + 'static> Drop for OwnedThinArc<T> {
    fn drop(&mut self) {
        if T::decrement_ref_count(self.ptr.as_ptr()) {
            let send_ptr = SendPtr::new(self.ptr);
            qsbr_reclaimer().add_callback(Box::new(move || {
                unsafe { T::drop_arc(send_ptr.into_ptr().as_ptr()) };
            }));
        }
    }
}

impl<T: ?Sized + Arcable> std::ops::DerefMut for OwnedThinArc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { T::deref_mut_arc(self.ptr.as_ptr()) }
    }
}

pub struct ThinAtomicArc<T: ?Sized + Arcable> {
    inner: AtomicPtr<()>,
    _marker: PhantomData<T>,
}

impl<T: ?Sized + Arcable> ThinAtomicArc<T> {
    pub fn load_owned(&self, ordering: Ordering) -> Option<OwnedThinArc<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(ptr) })
        }
    }

    pub fn load_cloned(&self, ordering: Ordering) -> OwnedThinArc<T> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load_cloned null pointer");
        }
        let owned: OwnedThinArc<T> = unsafe { OwnedThinArc::from_ptr(ptr) };
        let cloned = owned.clone();
        mem::forget(owned);
        cloned
    }
}

impl<T: ?Sized + Arcable> ThinAtomicArc<T> {
    pub fn new() -> Self {
        Self {
            inner: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    pub fn store(&self, arc: OwnedThinArc<T>, ordering: Ordering) {
        let ptr = arc.ptr;
        mem::forget(arc);
        self.inner.store(ptr.as_ptr(), ordering);
    }

    pub fn load(&self, ordering: Ordering) -> Option<SharedThinArc<T>> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { SharedThinArc::from_ptr(ptr) })
        }
    }

    pub fn swap(&self, arc: OwnedThinArc<T>, ordering: Ordering) -> Option<OwnedThinArc<T>> {
        let ptr = arc.ptr;
        mem::forget(arc);
        let old_ptr = self.inner.swap(ptr.as_ptr(), ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(old_ptr) })
        }
    }
}

pub type OwnedAtomicThinArc<T> = ThinAtomicArc<T>;

impl<T: Send + 'static + ?Sized + Arcable> AtomicPointerArrayValue<T> for ThinAtomicArc<T> {
    type OwnedPointer = OwnedThinArc<T>;
    type SharedPointer = SharedThinArc<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            panic!("Attempted to load null pointer");
        }
        unsafe { OwnedThinArc::from_ptr(ptr) }
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        self.load(ordering)
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        let ptr = self.inner.load(ordering);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(ptr) })
        }
    }

    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering) {
        let ptr = matching_ptr.ptr;
        mem::forget(matching_ptr);
        self.inner.store(ptr.as_ptr(), ordering);
    }

    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer> {
        let ptr = matching_ptr.ptr;
        mem::forget(matching_ptr);
        let old_ptr = self.inner.swap(ptr.as_ptr(), ordering);
        if old_ptr.is_null() {
            None
        } else {
            Some(unsafe { OwnedThinArc::from_ptr(old_ptr) })
        }
    }
}

// Serde implementations specifically for OwnedThinArc
impl<'de, T> Deserialize<'de> for OwnedThinArc<T>
where
    T: Deserialize<'de> + Arcable + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(OwnedThinArc::new)
    }
}

impl<T: Serialize + ?Sized + Arcable> Serialize for OwnedThinArc<T> {
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
    T: Deserialize<'de> + Send + 'static,
{
    type Value = OwnedThinArc<[T]>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(len) = seq.size_hint() {
            let mut uninit = OwnedThinArc::new_uninitialized(len);
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
            Ok(OwnedThinArc::new_from_slice(&vec))
        }
    }
}

// Add array implementations
impl<'de, T> Deserialize<'de> for OwnedThinArc<[T]>
where
    T: Deserialize<'de> + Send + 'static,
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
    use serde_json;
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::ops::Deref;

    use super::OwnedThinArc;
    use crate::qsbr_reclaimer;

    #[test]
    fn test_thin_arc() {
        qsbr_reclaimer().with(|| {
            let thin_str = OwnedThinArc::new_from_str("hello");
            assert_eq!(thin_str.len(), 5);
            assert_eq!(format!("hello {}", thin_str.deref()), "hello hello");

            let thin_usize = OwnedThinArc::new(42);
            assert_eq!(*thin_usize, 42);

            let thin_slice = OwnedThinArc::new_from_slice(&[1, 2, 3]);
            assert_eq!(thin_slice.len(), 3);
            assert_eq!(thin_slice[0], 1);

            let mut thin_slice_uninitialized = OwnedThinArc::new_uninitialized(3);
            assert_eq!(thin_slice_uninitialized.len(), 3);
            for i in 0..3 {
                thin_slice_uninitialized[i].write(i as usize);
            }
            let thin_slice_init = unsafe { thin_slice_uninitialized.assume_init() };

            assert_eq!(thin_slice_init.len(), 3);
            assert_eq!(thin_slice_init[1], 1);

            unsafe {
                OwnedThinArc::drop_immediately(thin_str);
                OwnedThinArc::drop_immediately(thin_slice);
                OwnedThinArc::drop_immediately(thin_usize);
                OwnedThinArc::drop_immediately(thin_slice_init);
            }
        });
    }

    #[test]
    fn test_derived_traits() {
        qsbr_reclaimer().with(|| {
            let ptr1 = OwnedThinArc::new(42);
            let ptr2 = OwnedThinArc::new(42);
            let ptr3 = OwnedThinArc::new(43);

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
        });
    }

    #[test]
    fn test_serde() {
        qsbr_reclaimer().with(|| {
            let ptr = OwnedThinArc::new(42);
            let serialized = serde_json::to_string(&ptr).unwrap();
            let deserialized: OwnedThinArc<i32> = serde_json::from_str(&serialized).unwrap();
            assert_eq!(&*ptr, &*deserialized);
        });
    }

    #[test]
    fn test_serde_array() {
        qsbr_reclaimer().with(|| {
            let array = OwnedThinArc::new_from_slice(&[1usize, 2, 3, 4, 5]);
            let serialized = serde_json::to_string(&array).unwrap();
            let deserialized: OwnedThinArc<[usize]> = serde_json::from_str(&serialized).unwrap();

            assert_eq!(array.len(), deserialized.len());
            assert_eq!(&*array, &*deserialized);
        });
    }
}
