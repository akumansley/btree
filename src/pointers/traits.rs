use std::sync::atomic::Ordering;

pub struct SendPtr {
    ptr: *mut (),
}

unsafe impl Send for SendPtr {}
impl SendPtr {
    pub fn new(ptr: *mut ()) -> Self {
        Self { ptr }
    }
    pub fn into_ptr(self) -> *mut () {
        self.ptr
    }
}

pub trait AtomicPointerArrayValue<T: Send + 'static + ?Sized> {
    type OwnedPointer: 'static;
    type SharedPointer: 'static;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer;
    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer>;
    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer>;
    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering);
    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer>;
}
