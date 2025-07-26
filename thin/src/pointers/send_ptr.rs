use std::ptr::NonNull;

pub struct SendPtr {
    ptr: NonNull<()>,
}

unsafe impl Send for SendPtr {}
impl SendPtr {
    pub fn new(ptr: NonNull<()>) -> Self {
        Self { ptr }
    }
    pub fn into_ptr(self) -> NonNull<()> {
        self.ptr
    }
}
