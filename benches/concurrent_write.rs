use btree::{qsbr_reclaimer, BTree, OwnedThinArc, OwnedThinPtr};
use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use std::time::Duration;

const NUM_OPERATIONS: usize = 1_000_000;

fn pure_insert_benchmark(c: &mut BenchmarkGroup<'_, WallTime>, num_threads: usize) {
    c.bench_with_input(
        BenchmarkId::new("concurrent_pure_insert", num_threads),
        &num_threads,
        |b: &mut Bencher, &num_threads| {
            b.iter_custom(|iters| {
                qsbr_reclaimer().register_thread();
                let ops_per_thread = NUM_OPERATIONS / num_threads;
                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    let tree = BTree::<usize, str>::new();
                    let threads_done = OwnedThinArc::new(AtomicUsize::new(0));

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
                                    let key = rng.random_range(0..NUM_OPERATIONS);
                                    let value = format!("value{}", key);
                                    tree.insert(
                                        OwnedThinArc::new(key),
                                        OwnedThinPtr::new_from_str(&value),
                                    );
                                }
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                unsafe {
                                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent()
                                };
                            });
                        }
                    });
                    sum += start.elapsed();
                    drop(tree);
                }
                println!(
                    "done - iters: {}, elapsed: {:?}, num_threads: {}",
                    iters, sum, num_threads
                );
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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
                    qsbr_reclaimer().register_thread();

                    // Pre-populate the tree
                    let mut pairs = Vec::new();
                    for i in 0..NUM_OPERATIONS / 10 {
                        pairs.push((
                            OwnedThinArc::new(i),
                            OwnedThinPtr::new_from_str(&format!("value{}", i)),
                        ));
                    }
                    let tree = BTree::bulk_load(pairs);

                    let start = std::time::Instant::now();
                    thread::scope(|s| {
                        for thread_id in 0..num_threads {
                            let tree = &tree;
                            let threads_done = threads_done.clone();
                            s.spawn(move || {
                                qsbr_reclaimer().register_thread();
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                for _ in 0..ops_per_thread {
                                    let operation = rng.random_range(0..3);
                                    let key = rng.random_range(0..NUM_OPERATIONS);

                                    match operation {
                                        0 => {
                                            // Insert/Update (40%)
                                            let value = format!("value{}", key);
                                            tree.insert(
                                                OwnedThinArc::new(key),
                                                OwnedThinPtr::new_from_str(&value),
                                            );
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
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                unsafe {
                                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent()
                                };
                            });
                        }
                    });
                    tree.check_invariants();
                    drop(tree);
                    unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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
                    let threads_done = OwnedThinArc::new(AtomicUsize::new(0));
                    qsbr_reclaimer().register_thread();

                    // Pre-populate the tree with bulk load
                    let mut pairs = Vec::new();
                    for i in 0..NUM_OPERATIONS / 10 {
                        pairs.push((
                            OwnedThinArc::new(i),
                            OwnedThinPtr::new_from_str(&format!("value{}", i)),
                        ));
                    }
                    let tree = BTree::bulk_load(pairs);

                    let start = std::time::Instant::now();

                    thread::scope(|s| {
                        for thread_id in 0..num_threads {
                            let tree = &tree;
                            let threads_done = threads_done.clone();
                            s.spawn(move || {
                                qsbr_reclaimer().register_thread();
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);

                                for _ in 0..ops_per_thread {
                                    let operation = rng.random_range(0..100);
                                    let key = rng.random_range(0..NUM_OPERATIONS);

                                    match operation {
                                        0 => {
                                            // Insert (1%)
                                            let value = format!("value{}", key);
                                            tree.insert(
                                                OwnedThinArc::new(key),
                                                OwnedThinPtr::new_from_str(&value),
                                            );
                                        }
                                        1 => {
                                            // Remove (1%)
                                            tree.remove(&key);
                                        }
                                        2 => {
                                            // Update (1%)
                                            let value = format!("newvalue{}", key);
                                            tree.insert(
                                                OwnedThinArc::new(key),
                                                OwnedThinPtr::new_from_str(&value),
                                            );
                                        }
                                        _ => {
                                            // Get (97%)
                                            let _ = tree.get(&key);
                                        }
                                    }
                                }
                                threads_done.fetch_add(1, Ordering::Relaxed);
                                unsafe {
                                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent()
                                };
                            });
                        }
                    });
                    sum += start.elapsed();
                }

                println!(
                    "done - iters: {}, elapsed: {:?}, num_threads: {}",
                    iters, sum, num_threads
                );
                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
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
