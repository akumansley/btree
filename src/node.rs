use parking_lot::lock_api::RawRwLock;
use std::cell::Cell;

// Define thread-local counters for shared and exclusive locks
thread_local! {
    static SHARED_LOCK_COUNT: Cell<usize> = Cell::new(0);
    static EXCLUSIVE_LOCK_COUNT: Cell<usize> = Cell::new(0);
}

// Function to increment the shared lock counter
fn increment_shared_lock_count() {
    SHARED_LOCK_COUNT.with(|count| {
        count.set(count.get() + 1);
    });
}

// Function to decrement the shared lock counter
fn decrement_shared_lock_count() {
    SHARED_LOCK_COUNT.with(|count| {
        count.set(count.get() - 1);
    });
}

// Function to increment the exclusive lock counter
fn increment_exclusive_lock_count() {
    EXCLUSIVE_LOCK_COUNT.with(|count| {
        count.set(count.get() + 1);
    });
}

// Function to decrement the exclusive lock counter
fn decrement_exclusive_lock_count() {
    EXCLUSIVE_LOCK_COUNT.with(|count| {
        count.set(count.get() - 1);
    });
}

pub fn assert_no_locks_held() {
    let (shared_count, exclusive_count) = get_lock_counts();
    assert_eq!(shared_count, 0);
    assert_eq!(exclusive_count, 0);
}
pub fn assert_one_exclusive_lock_held() {
    let (shared_count, exclusive_count) = get_lock_counts();
    assert_eq!(shared_count, 0);
    assert_eq!(exclusive_count, 1);
}

// Function to get the current lock counts
fn get_lock_counts() -> (usize, usize) {
    let shared_count = SHARED_LOCK_COUNT.with(|count| count.get());
    let exclusive_count = EXCLUSIVE_LOCK_COUNT.with(|count| count.get());
    (shared_count, exclusive_count)
}

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
        increment_exclusive_lock_count();
    }
    pub fn unlock_exclusive(&self) {
        unsafe {
            self.lock.unlock_exclusive();
        }
        decrement_exclusive_lock_count();
    }
    pub fn lock_shared(&self) {
        debug_perchance_yield();
        self.lock.lock_shared();
        debug_perchance_yield();
        increment_shared_lock_count();
    }
    pub fn unlock_shared(&self) {
        unsafe {
            self.lock.unlock_shared();
        }
        decrement_shared_lock_count();
    }
    pub fn height(&self) -> Height {
        self.height
    }
}
