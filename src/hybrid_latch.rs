use lock_api::RawRwLock;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub struct LockInfo(pub u64);

impl LockInfo {
    const UNLOCKED: u64 = 0;
    const EXCLUSIVE: u64 = 1;
    const SHARED: u64 = 2;
    const RETIRED: u64 = 3;
    const LOWEST_VERSION: u64 = 4;

    pub fn from_version(version: u64) -> Self {
        Self(version)
    }
    pub fn unlocked() -> Self {
        Self(Self::UNLOCKED)
    }
    pub fn exclusive() -> Self {
        Self(Self::EXCLUSIVE)
    }
    pub fn shared() -> Self {
        Self(Self::SHARED)
    }
    pub fn retired() -> Self {
        Self(Self::RETIRED)
    }

    pub fn is_unlocked(&self) -> bool {
        self.0 == Self::UNLOCKED
    }
    pub fn is_exclusive(&self) -> bool {
        self.0 == Self::EXCLUSIVE
    }
    pub fn is_shared(&self) -> bool {
        self.0 == Self::SHARED
    }
    pub fn is_retired(&self) -> bool {
        self.0 == Self::RETIRED
    }
}

impl Display for LockInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            LockInfo::UNLOCKED => write!(f, "UNLOCKED"),
            LockInfo::EXCLUSIVE => write!(f, "EXCLUSIVE"),
            LockInfo::SHARED => write!(f, "SHARED"),
            LockInfo::RETIRED => write!(f, "RETIRED"),
            LockInfo::LOWEST_VERSION => write!(f, "LOWEST_VERSION"),
            _ => write!(f, "{}", self.0),
        }
    }
}
// doesn't handle poisoning
pub(crate) struct HybridLatch {
    #[cfg(not(miri))]
    rw_lock: usync::RawRwLock,
    #[cfg(miri)]
    rw_lock: spin::RwLock<()>,
    version: AtomicU64,
}

impl HybridLatch {
    pub fn new() -> Self {
        Self {
            #[cfg(not(miri))]
            rw_lock: usync::RawRwLock::INIT,
            #[cfg(miri)]
            rw_lock: spin::RwLock::new(()),
            version: AtomicU64::new(LockInfo::LOWEST_VERSION),
        }
    }

    pub fn lock_shared(&self) {
        debug_assert!(!self.is_retired());
        self.rw_lock.lock_shared();
    }

    pub fn try_lock_shared(&self) -> Result<(), ()> {
        if self.rw_lock.try_lock_shared() {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn lock_exclusive(&self) {
        debug_assert!(!self.is_retired());
        self.rw_lock.lock_exclusive();
    }

    pub fn is_locked_exclusive(&self) -> bool {
        self.rw_lock.is_locked_exclusive()
    }

    pub fn is_locked_shared(&self) -> bool {
        self.rw_lock.is_locked()
    }

    pub fn try_lock_exclusive(&self) -> Result<(), ()> {
        if self.rw_lock.try_lock_exclusive() {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn unlock_shared(&self) {
        unsafe {
            self.rw_lock.unlock_shared();
        }
    }

    pub fn unlock_exclusive(&self) {
        self.version.fetch_add(1, Ordering::Release);
        unsafe {
            self.rw_lock.unlock_exclusive();
        }
    }

    pub fn retire(&self) {
        self.version.store(LockInfo::RETIRED, Ordering::Release);
        unsafe {
            self.rw_lock.unlock_exclusive();
        }
    }
    pub fn is_retired(&self) -> bool {
        self.version.load(Ordering::Acquire) == LockInfo::RETIRED
    }

    pub fn lock_optimistic(&self) -> Result<LockInfo, ()> {
        if self.rw_lock.is_locked_exclusive() {
            return Err(());
        }
        let lock_info = LockInfo::from_version(self.version.load(Ordering::Acquire));
        if lock_info.is_retired() {
            return Err(());
        }
        Ok(lock_info)
    }

    pub fn validate_optimistic_read(&self, version: LockInfo) -> bool {
        if self.rw_lock.is_locked_exclusive() {
            return false;
        }
        self.version.load(Ordering::Acquire) == version.0
    }
}
