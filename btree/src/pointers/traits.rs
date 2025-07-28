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

use thin::{Arcable, QsAtomicArc, QsArcOwned, QsArcShared};

impl<T: Send + 'static + ?Sized + Arcable> AtomicPointerArrayValue<T> for QsAtomicArc<T> {
    type OwnedPointer = QsArcOwned<T>;
    type SharedPointer = QsArcShared<T>;

    unsafe fn must_load_for_move(&self, ordering: Ordering) -> Self::OwnedPointer {
        self.load_owned(ordering).expect("Attempted to load null pointer")
    }

    fn load_shared(&self, ordering: Ordering) -> Option<Self::SharedPointer> {
        self.load(ordering)
    }

    fn into_owned(&self, ordering: Ordering) -> Option<Self::OwnedPointer> {
        self.load_owned(ordering)
    }

    fn store(&self, matching_ptr: Self::OwnedPointer, ordering: Ordering) {
        self.store(matching_ptr, ordering);
    }

    fn swap(
        &self,
        matching_ptr: Self::OwnedPointer,
        ordering: Ordering,
    ) -> Option<Self::OwnedPointer> {
        self.swap(matching_ptr, ordering)
    }
}
