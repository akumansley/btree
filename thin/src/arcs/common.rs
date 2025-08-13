/// Implements common traits for thin arc types.
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
/// - `share()` - Creates a shared reference
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

pub(crate) use impl_thin_arc_traits;
