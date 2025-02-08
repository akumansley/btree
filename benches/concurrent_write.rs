use btree::qsbr::qsbr_reclaimer;
use btree::{debug_println, BTree};
use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use std::thread::Scope;
use std::time::Duration;

const NUM_OPERATIONS: usize = 1_000_000;

#[cfg(feature = "deadlock_detection")]
pub fn check_deadlocks<'a, 'b>(
    s: &'a Scope<'a, 'b>,
    threads_done: Arc<AtomicUsize>,
    num_threads: usize,
) {
    use parking_lot::deadlock;

    s.spawn(move || loop {
        if threads_done.load(Ordering::Relaxed) == num_threads {
            break;
        }
        thread::sleep(Duration::from_secs(1));
        let deadlocks = deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{}", i);
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });
}

#[cfg(not(feature = "deadlock_detection"))]
fn check_deadlocks(_: &Scope, _: Arc<AtomicUsize>, _: usize) {}

fn pure_insert_benchmark(c: &mut BenchmarkGroup<'_, WallTime>, num_threads: usize) {
    c.bench_with_input(
        BenchmarkId::new("concurrent_pure_insert", num_threads),
        &num_threads,
        |b: &mut Bencher, &num_threads| {
            b.iter_custom(|iters| {
                let ops_per_thread = NUM_OPERATIONS / num_threads;
                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    let tree = BTree::<usize, String>::new();
                    let threads_done = Arc::new(AtomicUsize::new(0));

                    let start = std::time::Instant::now();
                    thread::scope(|s| {
                        for thread_id in 0..num_threads {
                            let tree = &tree;
                            let threads_done = threads_done.clone();
                            s.spawn(move || {
                                qsbr_reclaimer().register_thread();
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                let start = thread_id * ops_per_thread;
                                let end = start + ops_per_thread;

                                for _ in start..end {
                                    let key = rng.gen_range(0..NUM_OPERATIONS);
                                    let value = format!("value{}", key);
                                    tree.insert(Box::new(key), Box::new(value));
                                }
                                debug_println!("pure_insert_benchmark: thread done");
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                            });
                        }
                        check_deadlocks(s, threads_done, num_threads);
                    });
                    sum += start.elapsed();
                }
                println!(
                    "done - iters: {}, elapsed: {:?}, num_threads: {}",
                    iters, sum, num_threads
                );
                sum
            })
        },
    );
}

fn mixed_operations_benchmark(c: &mut BenchmarkGroup<'_, WallTime>, num_threads: usize) {
    c.bench_with_input(
        BenchmarkId::new("concurrent_mixed_operations", num_threads),
        &num_threads,
        |b: &mut Bencher, &num_threads| {
            b.iter_custom(|iters| {
                let ops_per_thread = NUM_OPERATIONS / num_threads;
                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    let threads_done = Arc::new(AtomicUsize::new(0));
                    let tree = BTree::<usize, String>::new();
                    qsbr_reclaimer().register_thread();

                    // Pre-populate the tree with some data
                    for i in 0..NUM_OPERATIONS / 10 {
                        tree.insert(Box::new(i), Box::new(format!("value{}", i)));
                    }
                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();

                    let start = std::time::Instant::now();
                    thread::scope(|s| {
                        for thread_id in 0..num_threads {
                            let tree = &tree;
                            let threads_done = threads_done.clone();
                            s.spawn(move || {
                                qsbr_reclaimer().register_thread();
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                for _ in 0..ops_per_thread {
                                    let operation = rng.gen_range(0..3);
                                    let key = rng.gen_range(0..NUM_OPERATIONS);

                                    match operation {
                                        0 => {
                                            // Insert/Update (40%)
                                            let value = format!("value{}", key);
                                            tree.insert(Box::new(key), Box::new(value));
                                        }
                                        1 => {
                                            // Remove (30%)
                                            tree.remove(&key);
                                        }
                                        2 => {
                                            // Get (30%)
                                            let _ = tree.get(&key);
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                                debug_println!("mixed_operations_benchmark: thread done");
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                            });
                        }
                        check_deadlocks(s, threads_done, num_threads);
                    });
                    sum += start.elapsed();
                }

                println!(
                    "done - iters: {}, elapsed: {:?}, num_threads: {}",
                    iters, sum, num_threads
                );
                sum
            })
        },
    );
}

fn read_heavy_benchmark(c: &mut BenchmarkGroup<'_, WallTime>, num_threads: usize) {
    c.bench_with_input(
        BenchmarkId::new("concurrent_read_heavy", num_threads),
        &num_threads,
        |b: &mut Bencher, &num_threads| {
            b.iter_custom(|iters| {
                let ops_per_thread = NUM_OPERATIONS / num_threads;
                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    let tree = BTree::<usize, String>::new();
                    let threads_done = Arc::new(AtomicUsize::new(0));
                    qsbr_reclaimer().register_thread();

                    // Pre-populate the tree with some data
                    for i in 0..NUM_OPERATIONS / 10 {
                        tree.insert(Box::new(i), Box::new(format!("value{}", i)));
                    }

                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();

                    let start = std::time::Instant::now();

                    thread::scope(|s| {
                        for thread_id in 0..num_threads {
                            let tree = &tree;
                            let threads_done = threads_done.clone();
                            s.spawn(move || {
                                qsbr_reclaimer().register_thread();
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);

                                for _ in 0..ops_per_thread {
                                    let operation = rng.gen_range(0..100);
                                    let key = rng.gen_range(0..NUM_OPERATIONS);

                                    match operation {
                                        0 => {
                                            // Insert (1%)
                                            let value = format!("value{}", key);
                                            tree.insert(Box::new(key), Box::new(value));
                                        }
                                        1 => {
                                            // Remove (1%)
                                            tree.remove(&key);
                                        }
                                        2 => {
                                            // Update (1%)
                                            let value = format!("newvalue{}", key);
                                            tree.insert(Box::new(key), Box::new(value));
                                        }
                                        _ => {
                                            // Get (97%)
                                            let _ = tree.get(&key);
                                        }
                                    }
                                }
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                debug_println!("read_heavy_benchmark: thread done");
                                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                            });
                        }
                        check_deadlocks(s, threads_done, num_threads);
                    });
                    sum += start.elapsed();
                }

                println!(
                    "done - iters: {}, elapsed: {:?}, num_threads: {}",
                    iters, sum, num_threads
                );
                sum
            })
        },
    );
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_write");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    for num_threads in [4, 8] {
        mixed_operations_benchmark(&mut group, num_threads);
        pure_insert_benchmark(&mut group, num_threads);
        read_heavy_benchmark(&mut group, num_threads);
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
