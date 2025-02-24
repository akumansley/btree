pub use std::sync::atomic::Ordering;

/// Very similar to `lock_api::RawRwLock`, but without the const constructor constraint
pub trait RawRwLock {
    fn new() -> Self;
    fn lock_shared(&self);
    fn try_lock_shared(&self) -> bool;
    fn is_locked(&self) -> bool;
    fn is_locked_exclusive(&self) -> bool;

    fn unlock_shared(&self);
    fn lock_exclusive(&self);
    fn try_lock_exclusive(&self) -> bool;
    fn unlock_exclusive(&self);
}

// Everyone (miri, shuttle, normal) uses normal atomics --
// in the shuttle tests, we're mostly interested in
// correctness issues stemming from issues with the locks
// and it seems to cause issues with the shuttle runtime if I use
// the shuttle atomics for the node storage arrays; still, keep them
// factored for now
pub type AtomicUsize = std::sync::atomic::AtomicUsize;
pub type AtomicPtr<T> = std::sync::atomic::AtomicPtr<T>;
pub type AtomicU64 = std::sync::atomic::AtomicU64;

// RwLock has two implementations: one for miri and shuttle,
// and one for normal
#[cfg(all(not(miri), not(feature = "shuttle")))]
pub type RwLock = WrappedUsyncRwLock;
#[cfg(any(miri, feature = "shuttle"))]
pub type RwLock = BasicSpinRwLock;

use usync::lock_api::RawRwLock as UsyncRawRwLock;
pub struct WrappedUsyncRwLock {
    inner: usync::RawRwLock,
}
impl RawRwLock for WrappedUsyncRwLock {
    fn new() -> Self {
        Self {
            inner: UsyncRawRwLock::INIT,
        }
    }
    fn lock_shared(&self) {
        self.inner.lock_shared();
    }
    fn try_lock_shared(&self) -> bool {
        self.inner.try_lock_shared()
    }
    fn unlock_shared(&self) {
        unsafe { self.inner.unlock_shared() };
    }
    fn lock_exclusive(&self) {
        self.inner.lock_exclusive();
    }
    fn try_lock_exclusive(&self) -> bool {
        self.inner.try_lock_exclusive()
    }
    fn unlock_exclusive(&self) {
        unsafe { self.inner.unlock_exclusive() };
    }
    fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }
    fn is_locked_exclusive(&self) -> bool {
        self.inner.is_locked_exclusive()
    }
}

#[cfg(feature = "shuttle")]
type SpinLockAtomicU64 = shuttle::sync::atomic::AtomicU64;
#[cfg(miri)]
type SpinLockAtomicU64 = std::sync::atomic::AtomicU64;

#[cfg(any(feature = "shuttle", miri))]
pub struct BasicSpinRwLock {
    lock: SpinLockAtomicU64,
}

#[cfg(any(feature = "shuttle", miri))]
/// A `RawRwLock` implementation for shuttle and miri. We don't care about performance, just simplicity and correctness.
impl RawRwLock for BasicSpinRwLock {
    fn new() -> Self {
        Self {
            lock: SpinLockAtomicU64::new(0),
        }
    }

    fn lock_shared(&self) {
        loop {
            let current = self.lock.load(Ordering::Acquire);
            // If exclusively locked, spin
            if current == 1 {
                std::hint::spin_loop();
                #[cfg(feature = "shuttle")]
                shuttle::hint::spin_loop();
                continue;
            }

            // Lock is either unlocked (0) or held by readers (even number > 0)
            // Try to increment reader count by 2
            match self.lock.compare_exchange(
                current,
                current + 2,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => {
                    std::hint::spin_loop();
                    #[cfg(feature = "shuttle")]
                    shuttle::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    fn try_lock_shared(&self) -> bool {
        let current = self.lock.load(Ordering::Acquire);
        // If exclusively locked, fail
        if current == 1 {
            return false;
        }

        // Lock is either unlocked or held by readers
        // Try to increment reader count
        self.lock
            .compare_exchange(current, current + 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn unlock_shared(&self) {
        // Decrement reader count - this should always be an even number >= 2
        let prev = self.lock.fetch_sub(2, Ordering::Release);
        debug_assert!(
            prev >= 2 && prev % 2 == 0,
            "unlock_shared: lock value was {}, which is invalid",
            prev
        );
    }

    fn lock_exclusive(&self) {
        loop {
            // If unlocked, try to acquire exclusive lock
            match self
                .lock
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break, // Acquired exclusive lock
                Err(_) => {
                    // Either readers hold the lock or another writer holds it
                    // Spin and try again
                    std::hint::spin_loop();
                    #[cfg(feature = "shuttle")]
                    shuttle::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    fn try_lock_exclusive(&self) -> bool {
        // Try to set exclusive lock bit if currently unlocked
        self.lock
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn unlock_exclusive(&self) {
        let res = self
            .lock
            .compare_exchange(1, 0, Ordering::Release, Ordering::Relaxed);
        assert!(
            res.is_ok(),
            "unlock_exclusive called on non-exclusively locked RwLock - value was {}",
            res.unwrap_err()
        );
    }

    fn is_locked(&self) -> bool {
        self.lock.load(Ordering::Acquire) != 0
    }

    fn is_locked_exclusive(&self) -> bool {
        self.lock.load(Ordering::Acquire) == 1
    }
}
