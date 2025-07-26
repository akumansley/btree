use std::sync::atomic::Ordering;

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
