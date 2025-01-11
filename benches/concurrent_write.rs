use btree::{debug_println, BTree};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, SamplingMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use std::thread::Scope;
use std::time::Duration;

const NUM_THREADS: usize = 8;
const NUM_OPERATIONS: usize = 1_000_000;
const OPS_PER_THREAD: usize = NUM_OPERATIONS / NUM_THREADS;

#[cfg(feature = "deadlock_detection")]
pub fn check_deadlocks<'a, 'b>(s: &'a Scope<'a, 'b>, threads_done: Arc<AtomicUsize>) {
    use parking_lot::deadlock;

    s.spawn(move || loop {
        if threads_done.load(Ordering::Relaxed) == NUM_THREADS {
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
fn check_deadlocks(_: &Scope, _: Arc<AtomicUsize>) {}

fn pure_insert_benchmark(c: &mut BenchmarkGroup<'_, WallTime>) {
    c.bench_function("concurrent_pure_insert", |b| {
        b.iter_custom(|iters| {
            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let tree = BTree::<usize, String>::new();
                let threads_done = Arc::new(AtomicUsize::new(0));

                let start = std::time::Instant::now();
                thread::scope(|s| {
                    for thread_id in 0..NUM_THREADS {
                        let tree = &tree;
                        let threads_done = threads_done.clone();
                        s.spawn(move || {
                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                            let start = thread_id * OPS_PER_THREAD;
                            let end = start + OPS_PER_THREAD;

                            for _ in start..end {
                                let key = rng.gen_range(0..NUM_OPERATIONS);
                                let value = format!("value{}", key);
                                tree.insert(key, value);
                            }
                            debug_println!("pure_insert_benchmark: thread done");
                            threads_done.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                    check_deadlocks(s, threads_done);
                });
                sum += start.elapsed();
            }
            println!("done - iters: {}, elapsed: {:?}", iters, sum);
            sum
        })
    });
}

fn mixed_operations_benchmark(c: &mut BenchmarkGroup<'_, WallTime>) {
    c.bench_function("concurrent_mixed_operations", |b| {
        b.iter_custom(|iters| {
            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let threads_done = Arc::new(AtomicUsize::new(0));
                let tree = BTree::<usize, String>::new();

                // Pre-populate the tree with some data
                for i in 0..NUM_OPERATIONS / 10 {
                    tree.insert(i, format!("value{}", i));
                }

                let start = std::time::Instant::now();
                thread::scope(|s| {
                    for thread_id in 0..NUM_THREADS {
                        let tree = &tree;
                        let threads_done = threads_done.clone();
                        s.spawn(move || {
                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                            for _ in 0..OPS_PER_THREAD {
                                let operation = rng.gen_range(0..3);
                                let key = rng.gen_range(0..NUM_OPERATIONS);

                                match operation {
                                    0 => {
                                        // Insert/Update (40%)
                                        let value = format!("value{}", key);
                                        tree.insert(key, value);
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
                        });
                    }
                    check_deadlocks(s, threads_done);
                });
                sum += start.elapsed();
            }

            println!("done - iters: {}, elapsed: {:?}", iters, sum);
            sum
        })
    });
}

fn read_heavy_benchmark(c: &mut BenchmarkGroup<'_, WallTime>) {
    c.bench_function("concurrent_read_heavy", |b| {
        b.iter_custom(|iters| {
            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let tree = BTree::<usize, String>::new();
                let threads_done = Arc::new(AtomicUsize::new(0));

                // Pre-populate the tree with some data
                for i in 0..NUM_OPERATIONS / 10 {
                    tree.insert(i, format!("value{}", i));
                }

                let start = std::time::Instant::now();

                thread::scope(|s| {
                    for thread_id in 0..NUM_THREADS {
                        let tree = &tree;
                        let threads_done = threads_done.clone();
                        s.spawn(move || {
                            let mut rng = StdRng::seed_from_u64(thread_id as u64);

                            for _ in 0..OPS_PER_THREAD {
                                let operation = rng.gen_range(0..100);
                                let key = rng.gen_range(0..NUM_OPERATIONS);

                                match operation {
                                    0 => {
                                        // Insert (1%)
                                        let value = format!("value{}", key);
                                        tree.insert(key, value);
                                    }
                                    1 => {
                                        // Remove (1%)
                                        tree.remove(&key);
                                    }
                                    2 => {
                                        // Update (1%)
                                        let value = format!("newvalue{}", key);
                                        tree.insert(key, value);
                                    }
                                    _ => {
                                        // Get (97%)
                                        let _ = tree.get(&key);
                                    }
                                }
                            }
                            threads_done.fetch_add(1, Ordering::Relaxed);
                            debug_println!("read_heavy_benchmark: thread done");
                        });
                    }
                    check_deadlocks(s, threads_done);
                });
                sum += start.elapsed();
            }

            println!("done - iters: {}, elapsed: {:?}", iters, sum);
            sum
        })
    });
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_write");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    pure_insert_benchmark(&mut group);
    mixed_operations_benchmark(&mut group);
    read_heavy_benchmark(&mut group);
}

criterion_group!(benches, bench);
criterion_main!(benches);
