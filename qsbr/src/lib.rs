use fxhash::FxHashSet;
use rayon::{ThreadPool, ThreadPoolBuilder};

use std::sync::{Arc, Condvar, Mutex};
use std::{collections::VecDeque, sync::OnceLock};

/// A memory reclaimer using intervals to defer resource reclamation until all threads are quiescent.
/// Threads must register before using the reclaimer.
///
/// ## Background garbage collection
///
/// When constructed with [`MemoryReclaimer::new_with_background`], a dedicated
/// garbage collection thread is spawned. The background thread sleeps on a
/// [`Condvar`] and is notified whenever callbacks are ready to run. Callbacks
/// are queued in a shared list protected by a mutex so the background thread
/// can drain them without blocking application threads.
///
/// If callbacks from a previous interval remain queued when a new interval
/// completes, the producing thread drains the stalled callbacks and executes
/// them itself before waking the background worker. This ensures threads help
/// out when the collector falls behind without imposing a hard backlog
/// threshold.
pub struct MemoryReclaimer {
    inner: Arc<Mutex<MemoryReclaimerInner>>,
    condvar: Arc<Condvar>,
    background_thread: Option<std::thread::JoinHandle<()>>,
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
    /// Callbacks ready for execution by the background thread
    ready_callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
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
        registration_count: 0,
    });
}

#[cfg(feature = "shuttle")]
shuttle::thread_local! {
    /// Each thread buffers callbacks locally until it quiesces
    static THREAD_STATE: std::cell::RefCell<ThreadState> = std::cell::RefCell::new(ThreadState {
        local_callbacks: VecDeque::new(),
        registration_count: 0,
    });
}

struct ThreadState {
    local_callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
    registration_count: usize,
}

impl MemoryReclaimer {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryReclaimerInner {
                current_interval_callbacks: VecDeque::new(),
                previous_interval_callbacks: VecDeque::new(),
                ready_callbacks: VecDeque::new(),
                registered_threads: FxHashSet::default(),
                quiesced_threads: FxHashSet::default(),
            })),
            condvar: Arc::new(Condvar::new()),
            background_thread: None,
        }
    }

    /// Creates a reclaimer with a background garbage collection thread.
    pub fn new_with_background() -> Self {
        let mut r = Self {
            inner: Arc::new(Mutex::new(MemoryReclaimerInner {
                current_interval_callbacks: VecDeque::new(),
                previous_interval_callbacks: VecDeque::new(),
                ready_callbacks: VecDeque::new(),
                registered_threads: FxHashSet::default(),
                quiesced_threads: FxHashSet::default(),
            })),
            condvar: Arc::new(Condvar::new()),
            background_thread: None,
        };
        r.start_background_thread();
        r
    }

    /// Spawns the worker responsible for reclaiming memory in the background.
    ///
    /// The worker waits on a [`Condvar`] whenever the queue of ready callbacks
    /// is empty.  Producers notify this condition variable when they enqueue
    /// new callbacks.  The worker drains the queue outside the lock and then
    /// goes back to sleep, minimizing contention with mutator threads.
    fn start_background_thread(&mut self) {
        let inner = Arc::clone(&self.inner);
        let condvar = Arc::clone(&self.condvar);
        self.background_thread = Some(std::thread::spawn(move || loop {
            let callbacks = {
                let mut inner = inner.lock().unwrap();
                while inner.ready_callbacks.is_empty() {
                    inner = condvar.wait(inner).unwrap();
                }
                std::mem::take(&mut inner.ready_callbacks)
            };
            for cb in callbacks {
                cb();
            }
        }));
    }

    /// Registers the current thread
    pub fn register_thread(&self) -> ThreadId {
        let thread_id = gettid();

        THREAD_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.registration_count += 1;

            // Only add to the shared set if this is the first registration
            if state.registration_count == 1 {
                let mut inner = self.inner.lock().unwrap();
                inner.registered_threads.insert(thread_id);
            }
        });

        thread_id
    }

    /// Adds a deferred callback
    pub fn add_callback(&self, callback: Box<dyn FnOnce() + Send>) {
        THREAD_STATE.with(|state| {
            assert!(
                state.borrow().registration_count > 0,
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
        self.complete_interval_if_possible(&mut inner);
    }



    /// Deregisters the current thread and marks it quiescent
    /// Panics if not registered
    pub unsafe fn deregister_current_thread_and_mark_quiescent(&self) {
        let thread_id = gettid();
        
        THREAD_STATE.with(|state| {
            let mut state = state.borrow_mut();
            
            if state.registration_count == 0 {
                panic!(
                    "Thread {} not registered! Call register_thread() before deregistering.",
                    thread_id
                );
            }
            
            state.registration_count -= 1;
            
            // Only remove from shared set and mark quiescent if this is the last deregistration
            if state.registration_count == 0 {
                let mut inner = self.inner.lock().unwrap();
                
                // Flush thread-local callbacks into the current interval.
                inner
                    .current_interval_callbacks
                    .append(&mut state.local_callbacks);

                inner.quiesced_threads.insert(thread_id);

                // Attempt to complete the interval if possible.
                // (If there are no remaining registered threads, or if all remaining have quiesced,
                // the interval is complete.)
                self.complete_interval_if_possible(&mut inner);

                // Remove the thread from registration (it will no longer participate in future intervals).
                inner.registered_threads.remove(&thread_id);
                // Also remove it from the quiescence set, if present.
                inner.quiesced_threads.remove(&thread_id);
            }
        });
    }

    /// Completes the interval if all threads are quiescent
    fn complete_interval_if_possible(&self, inner: &mut MemoryReclaimerInner) {
        if inner.quiesced_threads.len() == inner.registered_threads.len() {
            let mut ready = std::mem::take(&mut inner.previous_interval_callbacks);
            if inner.registered_threads.len() <= 1 {
                ready.append(&mut inner.current_interval_callbacks);
            } else {
                inner.previous_interval_callbacks =
                    std::mem::take(&mut inner.current_interval_callbacks);
            }
            inner.quiesced_threads.clear();
            self.notify_gc_thread_or_perform_gc_if_stalled(ready);
        }
    }

    /// Adds callbacks to the background queue and wakes the worker.
    ///
    /// If callbacks from a previous interval remain in the queue when this
    /// method is called, those callbacks are drained and executed on the caller
    /// thread.  This cooperative approach prevents the background worker from
    /// falling too far behind when many intervals complete quickly.
    fn notify_gc_thread_or_perform_gc_if_stalled(
        &self,
        mut callbacks: VecDeque<Box<dyn FnOnce() + Send>>,
    ) {
        if let Some(_handle) = &self.background_thread {
            let mut stalled_callbacks = VecDeque::new();
            {
                let mut inner = self.inner.lock().unwrap();
                if !inner.ready_callbacks.is_empty() {
                    stalled_callbacks = std::mem::take(&mut inner.ready_callbacks);
                }
                inner.ready_callbacks.append(&mut callbacks);
            }
            self.condvar.notify_one();
            for cb in stalled_callbacks {
                cb();
            }
        } else {
            for cb in callbacks {
                cb();
            }
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

    #[test]
    fn test_reference_counted_registration() {
        let reclaimer = MemoryReclaimer::new();
        
        // Register multiple times
        let tid1 = reclaimer.register_thread();
        let tid2 = reclaimer.register_thread();
        let tid3 = reclaimer.register_thread();
        
        // All should return the same thread ID
        assert_eq!(tid1, tid2);
        assert_eq!(tid2, tid3);
        
        // Should be able to add callbacks since we're registered
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        reclaimer.add_callback(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        
        // Should still be able to add callbacks after multiple registrations
        let counter_clone = Arc::clone(&counter);
        reclaimer.add_callback(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        
        // First deregister - should still be registered (count goes from 3 to 2)
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
        
        // Register again to test we can still add callbacks
        reclaimer.register_thread();
        let counter_clone = Arc::clone(&counter);
        reclaimer.add_callback(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        
        // Final deregisters to bring count to 0
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
        
        // Three callbacks should have executed
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    #[should_panic(expected = "not registered")]
    fn test_deregister_without_register_panics() {
        let reclaimer = MemoryReclaimer::new();
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
    }

    #[test]
    #[should_panic(expected = "not registered")]
    fn test_over_deregister_panics() {
        let reclaimer = MemoryReclaimer::new();
        reclaimer.register_thread();
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() };
        unsafe { reclaimer.deregister_current_thread_and_mark_quiescent() }; // Should panic
    }

    #[test]
    fn test_nested_guards() {
        let reclaimer = MemoryReclaimer::new();
        let counter = Arc::new(AtomicUsize::new(0));

        {
            let _guard1 = reclaimer.guard();
            {
                let _guard2 = reclaimer.guard();
                let counter_clone = Arc::clone(&counter);
                reclaimer.add_callback(Box::new(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }));
                // _guard2 drops here, but thread should still be registered
            }
            
            // Should still be able to add callbacks
            let counter_clone = Arc::clone(&counter);
            reclaimer.add_callback(Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
            // _guard1 drops here, fully deregistering the thread
        }

        // Both callbacks should have executed
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
