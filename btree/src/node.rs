use std::ptr;

use crate::hybrid_latch::{HybridLatch, LockError, LockInfo};

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
}

#[repr(C)]
#[derive(Debug)]
pub struct NodeHeader {
    height: Height,
    lock: HybridLatch,
}

impl PartialEq for NodeHeader {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self, other)
    }
}

impl NodeHeader {
    pub fn new(height: Height) -> Self {
        Self {
            height,
            lock: HybridLatch::new(),
        }
    }
    pub fn lock_exclusive(&self) {
        self.lock.lock_exclusive();
    }
    pub fn lock_exclusive_if_not_retired(&self) -> Result<(), LockError> {
        self.lock.lock_exclusive_if_not_retired()
    }
    pub fn lock_exclusive_jittered(&self) {
        self.lock.lock_exclusive_jittered();
    }
    pub fn lock_exclusive_if_not_retired_jittered(&self) -> Result<(), LockError> {
        self.lock.lock_exclusive_if_not_retired_jittered()
    }
    pub fn is_locked_exclusive(&self) -> bool {
        self.lock.is_locked_exclusive()
    }
    pub fn unlock_exclusive(&self) {
        debug_assert!(self.is_locked_exclusive());
        self.lock.unlock_exclusive();
    }
    pub fn lock_shared(&self) {
        self.lock.lock_shared();
    }
    pub fn lock_shared_if_not_retired(&self) -> Result<(), LockError> {
        self.lock.lock_shared_if_not_retired()
    }
    pub fn is_locked_shared(&self) -> bool {
        self.lock.is_locked_shared()
    }
    pub fn is_unlocked(&self) -> bool {
        self.lock.is_unlocked()
    }
    pub fn try_lock_shared(&self) -> Result<(), ()> {
        match self.lock.try_lock_shared() {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }
    pub fn try_lock_exclusive(&self) -> Result<(), ()> {
        match self.lock.try_lock_exclusive() {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }
    pub fn unlock_shared(&self) {
        debug_assert!(self.is_locked_shared());
        self.lock.unlock_shared();
    }
    pub fn lock_optimistic(&self) -> Result<LockInfo, ()> {
        self.lock.lock_optimistic()
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

    pub fn is_retired(&self) -> bool {
        self.lock.is_retired()
    }

    pub fn version(&self) -> u64 {
        self.lock.version()
    }

    pub fn height(&self) -> Height {
        self.height
    }
}
