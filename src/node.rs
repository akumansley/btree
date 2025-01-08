use parking_lot::lock_api::RawRwLock;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Height {
    Root,
    Internal(u8),
    Leaf,
}

impl Height {
    pub fn one_level_higher(&self) -> Height {
        match *self {
            Height::Leaf => Height::Internal(1),
            Height::Internal(h) => Height::Internal(h + 1),
            Height::Root => panic!("can't increment root"),
        }
    }
    pub fn one_level_lower(&self) -> Height {
        match *self {
            Height::Internal(h) => {
                if h == 1 {
                    Height::Leaf
                } else {
                    Height::Internal(h - 1)
                }
            }
            _ => panic!("can't decrement root or leaf"),
        }
    }

    pub(crate) fn is_internal(&self) -> bool {
        matches!(self, Height::Internal(_))
    }
}

#[cfg(debug_assertions)]
pub fn debug_perchance_yield() {
    use rand::Rng;

    if rand::thread_rng().gen_bool(0.001) {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
pub fn debug_perchance_yield() {}

#[repr(C)]
pub struct NodeHeader {
    height: Height,
    lock: parking_lot::RawRwLock,
}

impl NodeHeader {
    pub fn new(height: Height) -> Self {
        Self {
            height,
            lock: parking_lot::RawRwLock::INIT,
        }
    }
    pub fn lock_exclusive(&self) {
        debug_perchance_yield();
        self.lock.lock_exclusive();
        debug_perchance_yield();
    }
    pub fn unlock_exclusive(&self) {
        unsafe {
            self.lock.unlock_exclusive();
        }
    }
    pub fn lock_shared(&self) {
        debug_perchance_yield();
        self.lock.lock_shared();
        debug_perchance_yield();
    }
    pub fn unlock_shared(&self) {
        unsafe {
            self.lock.unlock_shared();
        }
    }
    pub fn height(&self) -> Height {
        self.height
    }
}
