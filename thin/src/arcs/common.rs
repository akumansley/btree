/// Implements common traits for all thin arc types (strong and weak).
///
/// Implemented traits:
/// - `Debug` - Debug formatting showing the dereferenced value (when T: Debug)
/// - `Display` - Display formatting showing the dereferenced value (when T: Display)
/// - `Deref` - Dereferences to the pointed-to type T
/// - `Hash` - Hashes the dereferenced value (when T: Hash)
/// - `PartialEq<T>` - Compares dereferenced value with T
/// - `PartialEq<&T>` - Compares dereferenced value with &T
/// - `PartialEq` - Compares two thin arcs by their dereferenced values
/// - `Eq` - Marker trait for equality (when T: Eq)
/// - `PartialOrd` - Partial ordering of dereferenced values (when T: PartialOrd)
/// - `Ord` - Total ordering of dereferenced values (when T: Ord)
/// - `Send` - Safe to send between threads (when T: Send)
/// - `Sync` - Safe to share between threads (when T: Sync)
///
/// Also provides utility methods:
/// - `from_ptr()` - Constructs from a raw pointer (unsafe)
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
            pub unsafe fn from_ptr(ptr: *mut ()) -> $arc_type<T> {
                Self {
                    ptr: NonNull::new(ptr).unwrap(),
                    _marker: PhantomData,
                }
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

/// Implements shared functionality for strong arc types (Arc and QsArc).
///
/// Provides constructors, Clone, DerefMut, serde, and conversion to weak type.
/// The caller must still implement Drop and any type-specific methods.
///
/// Requires the caller to import: NonNull, PhantomData, mem, MaybeUninit,
/// Arcable, Hash, Hasher, CmpOrdering, Deref, Serialize, Serializer,
/// Deserialize, Deserializer, SeqAccess, Visitor.
macro_rules! impl_thin_arc_strong {
    ($arc_type:ident, $weak_type:ident) => {
        impl<T: ?Sized + Arcable> $arc_type<T> {
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

            pub fn into_ptr(self) -> *mut () {
                let ptr = self.ptr;
                mem::forget(self);
                ptr.as_ptr()
            }

            pub fn share(&self) -> crate::$weak_type<T> {
                let ptr = self.ptr;
                unsafe { crate::$weak_type::from_ptr(ptr.as_ptr()) }
            }
        }

        impl<T: ?Sized + Arcable> Clone for $arc_type<T> {
            fn clone(&self) -> Self {
                T::increment_ref_count(self.ptr.as_ptr());
                Self {
                    ptr: self.ptr,
                    _marker: PhantomData,
                }
            }
        }

        impl<T: Sized + Arcable> $arc_type<T> {
            pub fn new(init: T) -> Self {
                Self::new_with(|| crate::arcable::init_thin_sized(init))
            }
        }

        impl $arc_type<str> {
            pub fn new_from_str(init: &str) -> Self {
                Self::new_with(|| crate::arcable::init_thin_str(init))
            }

            pub fn as_str(&self) -> &str {
                self
            }
        }

        impl From<&str> for $arc_type<str> {
            fn from(value: &str) -> Self {
                Self::new_from_str(value)
            }
        }

        impl From<String> for $arc_type<str> {
            fn from(value: String) -> Self {
                Self::new_from_str(&value)
            }
        }

        impl From<Box<str>> for $arc_type<str> {
            fn from(value: Box<str>) -> Self {
                Self::new_from_str(&value)
            }
        }

        impl<T: Clone> $arc_type<[T]> {
            pub fn new_from_slice(init: &[T]) -> Self {
                Self::new_with(|| crate::arcable::init_thin_slice(init))
            }
        }

        impl<T> $arc_type<[MaybeUninit<T>]> {
            pub fn new_uninitialized(len: usize) -> Self {
                Self::new_with(|| crate::arcable::init_thin_slice_uninitialized::<T>(len))
            }

            pub unsafe fn assume_init(self) -> $arc_type<[T]> {
                unsafe { $arc_type::from_ptr(self.into_ptr()) }
            }
        }

        impl<T: ?Sized + Arcable> std::ops::DerefMut for $arc_type<T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                unsafe { T::deref_mut_arc(self.ptr.as_ptr()) }
            }
        }

        impl<'de, T> Deserialize<'de> for $arc_type<T>
        where
            T: Deserialize<'de> + Arcable + Sized,
        {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                T::deserialize(deserializer).map($arc_type::new)
            }
        }

        impl<T: Serialize + ?Sized + Arcable> Serialize for $arc_type<T> {
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
            type Value = $arc_type<[T]>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if let Some(len) = seq.size_hint() {
                    let mut uninit = $arc_type::new_uninitialized(len);
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
                    Ok($arc_type::new_from_slice(&vec))
                }
            }
        }

        impl<'de, T> Deserialize<'de> for $arc_type<[T]>
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
    };
}

/// Implements shared functionality for weak arc types (Weak and QsWeak).
///
/// Provides must_upgrade, into_ptr, Clone, Copy, and Serialize.
macro_rules! impl_thin_arc_weak {
    ($weak_type:ident, $arc_type:ident) => {
        impl<T: ?Sized + Arcable> $weak_type<T> {
            pub fn must_upgrade(self) -> crate::$arc_type<T> {
                let arc_inner_ptr = self.into_ptr();
                T::increment_ref_count(arc_inner_ptr);
                unsafe { crate::$arc_type::from_ptr(arc_inner_ptr) }
            }

            pub fn into_ptr(self) -> *mut () {
                let ptr = self.ptr;
                ptr.as_ptr()
            }
        }

        impl<T: ?Sized + Arcable> Clone for $weak_type<T> {
            fn clone(&self) -> Self {
                Self {
                    ptr: self.ptr,
                    _marker: PhantomData,
                }
            }
        }

        impl<T: ?Sized + Arcable> Copy for $weak_type<T> {}

        impl<T: Serialize + ?Sized + Arcable> Serialize for $weak_type<T> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                (**self).serialize(serializer)
            }
        }
    };
}

pub(crate) use impl_thin_arc_strong;
pub(crate) use impl_thin_arc_traits;
pub(crate) use impl_thin_arc_weak;
