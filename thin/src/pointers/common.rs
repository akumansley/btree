/// Implements common traits for thin pointer types.
///
/// Implemented traits:
/// - `Debug` - Debug formatting showing the struct name and ptr field
/// - `Deref` - Dereferences to the pointed-to type T
/// - `Hash` - Hashes the dereferenced value (when T: Hash)
/// - `PartialEq<T>` - Compares dereferenced value with T
/// - `PartialEq<&T>` - Compares dereferenced value with &T
/// - `PartialEq` - Compares two thin pointers by their dereferenced values
/// - `Eq` - Marker trait for equality (when T: Eq)
/// - `PartialOrd` - Partial ordering of dereferenced values (when T: PartialOrd)
/// - `Ord` - Total ordering of dereferenced values (when T: Ord)
/// - `Send` - Safe to send between threads (when T: Send)
/// - `Sync` - Safe to share between threads (when T: Sync)
///
/// Also provides utility methods:
/// - `as_ptr()` - Returns the raw pointer
/// - `from_ptr()` - Constructs from a raw pointer (unsafe)
/// - `cast()` - Casts to a different pointable type (unsafe)
/// - `share()` - Creates a shared reference
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
        }

        unsafe impl<T: ?Sized + Pointable + Send> Send for $struct_name<T> {}
        unsafe impl<T: ?Sized + Pointable + Sync> Sync for $struct_name<T> {}
    };
}

pub(crate) use impl_thin_ptr_traits;
