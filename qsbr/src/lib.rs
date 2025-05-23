use fxhash::FxHashSet;
use rayon::{ThreadPool, ThreadPoolBuilder};

use std::sync::Mutex;
use std::{collections::VecDeque, sync::OnceLock};

/// A memory reclaimer using intervals to defer resource reclamation until all threads are quiescent
/// Threads must register before using the reclaimer
pub struct MemoryReclaimer {
    inner: Mutex<MemoryReclaimerInner>,
}

type ThreadId = u64;

fn gettid() -> ThreadId {
    #[cfg(feature = "shuttle")]
    {
        unsafe { std::mem::transmute(shuttle::thread::current().id()) }
    }
    #[cfg(not(feature = "shuttle"))]
    {
        unsafe { std::mem::transmute(std::thread::current().id()) }
    }
}

struct MemoryReclaimerInner {
    /// Callbacks added during the current interval
    current_interval_callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
    /// Callbacks accumulated in the previous interval; these are executed
    /// when an interval completes
    previous_interval_callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
    /// Threads that have registered with the reclaimer
    registered_threads: FxHashSet<ThreadId>,
    /// Registered threads that have signaled quiescence in the current interval
    quiesced_threads: FxHashSet<ThreadId>,
}

#[cfg(not(feature = "shuttle"))]
std::thread_local! {
    /// Each thread buffers callbacks locally until it quiesces
    static THREAD_STATE: std::cell::RefCell<ThreadState> = std::cell::RefCell::new(ThreadState {
        local_callbacks: VecDeque::new(),
        is_registered: false,
    });
}

#[cfg(feature = "shuttle")]
shuttle::thread_local! {
    /// Each thread buffers callbacks locally until it quiesces
    static THREAD_STATE: std::cell::RefCell<ThreadState> = std::cell::RefCell::new(ThreadState {
        local_callbacks: VecDeque::new(),
        is_registered: false,
    });
}

struct ThreadState {
    local_callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
    is_registered: bool,
}

impl MemoryReclaimer {
    fn new() -> Self {
        Self {
            inner: Mutex::new(MemoryReclaimerInner {
                current_interval_callbacks: VecDeque::new(),
                previous_interval_callbacks: VecDeque::new(),
                registered_threads: FxHashSet::default(),
                quiesced_threads: FxHashSet::default(),
            }),
        }
    }

    /// Registers the current thread
    pub fn register_thread(&self) -> ThreadId {
        let thread_id = gettid();
        let mut inner = self.inner.lock().unwrap();
        inner.registered_threads.insert(thread_id);
        THREAD_STATE.with(|state| {
            state.borrow_mut().is_registered = true;
        });
        thread_id
    }

    /// Adds a deferred callback
    pub fn add_callback(&self, callback: Box<dyn FnOnce() + Send>) {
        THREAD_STATE.with(|state| {
            assert!(
                state.borrow().is_registered,
                "Thread {} is not registered",
                gettid()
            );
            state.borrow_mut().local_callbacks.push_back(callback);
        });
    }

    /// Marks the current thread as quiescent
    /// Panics if not registered
    pub unsafe fn mark_current_thread_quiescent(&self) {
        let thread_id = gettid();
        let mut inner = self.inner.lock().unwrap();

        if !inner.registered_threads.contains(&thread_id) {
            panic!(
                "Thread {} not registered! Call register_thread() before calling mark_current_thread_quiescent().",
                thread_id
            );
        }

        // Flush thread-local callbacks into the current interval.
        THREAD_STATE.with(|state| {
            inner
                .current_interval_callbacks
                .append(&mut state.borrow_mut().local_callbacks);
        });

        // Record this thread's quiescence for the current interval.
        inner.quiesced_threads.insert(thread_id);

        // Attempt to complete the interval if all registered threads have quiesced.
        Self::complete_interval_if_possible(&mut inner);
    }

    /// Deregisters the current thread and marks it quiescent
    /// Panics if not registered
    pub unsafe fn deregister_current_thread_and_mark_quiescent(&self) {
        let thread_id = gettid();
        let mut inner = self.inner.lock().unwrap();

        if !inner.registered_threads.contains(&thread_id) {
            panic!(
                "Thread {} not registered! Call register_thread() before deregistering.",
                thread_id
            );
        }

        // Flush thread-local callbacks into the current interval.
        THREAD_STATE.with(|state| {
            inner
                .current_interval_callbacks
                .append(&mut state.borrow_mut().local_callbacks);
        });

        inner.quiesced_threads.insert(thread_id);

        // Attempt to complete the interval if possible.
        // (If there are no remaining registered threads, or if all remaining have quiesced,
        // the interval is complete.)
        Self::complete_interval_if_possible(&mut inner);

        THREAD_STATE.with(|state| {
            state.borrow_mut().is_registered = false;
        });

        // Remove the thread from registration (it will no longer participate in future intervals).
        inner.registered_threads.remove(&thread_id);
        // Also remove it from the quiescence set, if present.
        inner.quiesced_threads.remove(&thread_id);
    }

    /// Completes the interval if all threads are quiescent
    fn complete_interval_if_possible(inner: &mut MemoryReclaimerInner) {
        if inner.quiesced_threads.len() == inner.registered_threads.len() {
            // Always execute callbacks from the previous interval.
            while let Some(callback) = inner.previous_interval_callbacks.pop_front() {
                callback();
            }
            // If there's only a single thread (or none) in the interval, free the current interval's garbage
            // immediately. Otherwise, promote the current interval's callbacks to be freed in the next interval.
            if inner.registered_threads.len() <= 1 {
                while let Some(callback) = inner.current_interval_callbacks.pop_front() {
                    callback();
                }
            } else {
                inner.previous_interval_callbacks =
                    std::mem::take(&mut inner.current_interval_callbacks);
            }
            inner.quiesced_threads.clear();
        }
    }

    /// Creates a new QSBR guard for this reclaimer.
    /// The guard will automatically register the current thread and handle cleanup when dropped.
    pub fn guard(&self) -> QsbrGuard {
        self.register_thread();
        QsbrGuard { reclaimer: self }
    }
}

static RECLAIMER: OnceLock<MemoryReclaimer> = OnceLock::new();
static POOL: OnceLock<ThreadPool> = OnceLock::new();

pub fn qsbr_reclaimer() -> &'static MemoryReclaimer {
    RECLAIMER.get_or_init(MemoryReclaimer::new)
}

pub fn qsbr_pool() -> &'static ThreadPool {
    POOL.get_or_init(|| {
        ThreadPoolBuilder::new()
            .num_threads(8)
            .start_handler(|_| {
                qsbr_reclaimer().register_thread();
                ()
            })
            .exit_handler(|_| {
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                ()
            })
            .build()
            .unwrap()
    })
}

/// A guard that automatically handles QSBR registration and deregistration.
/// When dropped, it will automatically deregister the thread and mark it as quiescent.
pub struct QsbrGuard<'a> {
    reclaimer: &'a MemoryReclaimer,
}

impl<'a> Drop for QsbrGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            self.reclaimer
                .deregister_current_thread_and_mark_quiescent()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        thread,
    };

    /// In a single-thread scenario, callbacks added in one interval are executed
    /// immediately when the thread quiesces.
    #[test]
    fn test_single_thread_interval_optimization() {
        let reclaimer = MemoryReclaimer::new();
        reclaimer.register_thread();

        let counter = Arc::new(AtomicUsize::new(0));
        {
            let counter_clone = Arc::clone(&counter);
            reclaimer.add_callback(Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
        }

        // First interval: flush thread-local callbacks; since only one thread is registered,
        // both previous (empty) and current callbacks are executed immediately.
        unsafe { reclaimer.mark_current_thread_quiescent() };
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Now add another callback and quiesce again.
        {
            let counter_clone = Arc::clone(&counter);
            reclaimer.add_callback(Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
        }
        unsafe { reclaimer.mark_current_thread_quiescent() };
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    /// Multiple threads must all be registered and then signal quiescence.
    /// Callbacks are deferred for one interval as usual.
    #[test]
    fn test_multi_thread_interval() {
        let reclaimer = MemoryReclaimer::new();
        let num_threads = 4;
        let counter = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(num_threads));
        thread::scope(|s| {
            for _ in 0..num_threads {
                let counter_clone = Arc::clone(&counter);
                let barrier_clone = Arc::clone(&barrier);
                let reclaimer = &reclaimer;
                s.spawn(move || {
                    reclaimer.register_thread();
                    barrier_clone.wait();
                    // start of interval

                    reclaimer.add_callback(Box::new(move || {
                        counter_clone.fetch_add(1, Ordering::SeqCst);
                    }));
                    unsafe { reclaimer.mark_current_thread_quiescent() };

                    // start of interval 2
                    barrier_clone.wait();
                    unsafe { reclaimer.mark_current_thread_quiescent() };
                });
            }
        });

        // interval 2 should have ended so all callbacks in interval 1should have been executed
        assert_eq!(counter.load(Ordering::SeqCst), num_threads);
    }

    /// If an unregistered thread calls a quiescence method, it should panic.
    #[test]
    #[should_panic(expected = "not registered")]
    fn test_unregistered_thread_panics() {
        let reclaimer = MemoryReclaimer::new();
        unsafe { reclaimer.mark_current_thread_quiescent() };
    }

    /// A thread can deregister and mark quiescence.
    #[test]
    fn test_deregister_and_mark_quiescent() {
        let reclaimer = MemoryReclaimer::new();
        reclaimer.register_thread();
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    fn test_qsbr_guard() {
        let reclaimer = MemoryReclaimer::new();
        let counter = Arc::new(AtomicUsize::new(0));

        // Test manual guard creation and drop
        {
            let _guard = reclaimer.guard();
            let counter_clone = Arc::clone(&counter);
            reclaimer.add_callback(Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
        }

        // The callback should have been executed since the guard was dropped
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
