use crate::hybrid_latch::{HybridLatch, LockInfo};
use std::cell::Cell;

// Define thread-local counters for shared and exclusive locks
thread_local! {
    static SHARED_LOCK_COUNT: Cell<usize> = Cell::new(0);
    static EXCLUSIVE_LOCK_COUNT: Cell<usize> = Cell::new(0);
}

// Function to increment the shared lock counter
fn increment_shared_lock_count() {
    #[cfg(debug_assertions)]
    {
        SHARED_LOCK_COUNT.with(|count| {
            count.set(count.get() + 1);
        });
    }
}

// Function to decrement the shared lock counter
fn decrement_shared_lock_count() {
    #[cfg(debug_assertions)]
    {
        SHARED_LOCK_COUNT.with(|count| {
            count.set(count.get() - 1);
        });
    }
}

// Function to increment the exclusive lock counter
fn increment_exclusive_lock_count() {
    #[cfg(debug_assertions)]
    {
        EXCLUSIVE_LOCK_COUNT.with(|count| {
            count.set(count.get() + 1);
        });
    }
}

// Function to decrement the exclusive lock counter
fn decrement_exclusive_lock_count() {
    #[cfg(debug_assertions)]
    {
        EXCLUSIVE_LOCK_COUNT.with(|count| {
            count.set(count.get() - 1);
        });
    }
}

pub fn debug_assert_no_locks_held<const METHOD: char>() {
    #[cfg(debug_assertions)]
    {
        let (shared_count, exclusive_count) = get_lock_counts();
        assert_eq!(shared_count, 0);
        assert_eq!(exclusive_count, 0, "method: {:?}", METHOD);
    }
}

pub fn debug_assert_one_shared_lock_held() {
    #[cfg(debug_assertions)]
    {
        let (shared_count, exclusive_count) = get_lock_counts();
        assert_eq!(shared_count, 1);
        assert_eq!(exclusive_count, 0);
    }
}

#[cfg(debug_assertions)]
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
    lock: HybridLatch,
}

impl NodeHeader {
    pub fn new(height: Height) -> Self {
        Self {
            height,
            lock: HybridLatch::new(),
        }
    }
    pub fn lock_exclusive(&self) {
        debug_perchance_yield();
        self.lock.lock_exclusive();
        debug_perchance_yield();
        increment_exclusive_lock_count();
    }
    pub fn unlock_exclusive(&self) {
        self.lock.unlock_exclusive();
        decrement_exclusive_lock_count();
    }
    pub fn lock_shared(&self) {
        debug_perchance_yield();
        self.lock.lock_shared();
        debug_perchance_yield();
        increment_shared_lock_count();
    }
    pub fn try_lock_shared(&self) -> Result<(), ()> {
        self.lock.try_lock_shared()
    }
    pub fn try_lock_exclusive(&self) -> Result<(), ()> {
        self.lock.try_lock_exclusive()
    }
    pub fn unlock_shared(&self) {
        self.lock.unlock_shared();
        decrement_shared_lock_count();
    }
    pub fn lock_optimistic(&self) -> Result<LockInfo, ()> {
        self.lock.lock_optimistic()
    }
    pub fn unlock_optimistic(&self) {
        self.lock.unlock_exclusive();
    }

    pub fn validate_optimistic(&self, version: LockInfo) -> Result<(), ()> {
        if self.lock.validate_optimistic_read(version) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn retire(&self) {
        self.lock.retire();
    }

    pub fn height(&self) -> Height {
        self.height
    }
}
