use btree::{qsbr_reclaimer, BTree, OwnedThinArc, OwnedThinPtr};
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion};
use std::time::Duration;

const NUM_ELEMENTS: usize = 10_000_000;

fn bulk_load_benchmark(c: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    c.bench_with_input(
        BenchmarkId::new("bulk_load", NUM_ELEMENTS),
        &NUM_ELEMENTS,
        |b: &mut Bencher, &num_elements| {
            b.iter_custom(|iters| {
                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    // Generate sorted key-value pairs
                    let mut pairs = Vec::with_capacity(num_elements);
                    for i in 0..num_elements {
                        pairs.push((
                            OwnedThinArc::new(i),
                            OwnedThinPtr::new_from_str(&format!("value{}", i)),
                        ));
                    }

                    qsbr_reclaimer().register_thread();
                    let start = std::time::Instant::now();
                    let _tree = BTree::bulk_load_parallel(pairs);
                    sum += start.elapsed();
                    unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                }
                println!(
                    "done - bulk_load iters: {}, elapsed: {:?}, num_elements: {}",
                    iters, sum, num_elements
                );
                sum
            })
        },
    );
}

fn bulk_update_benchmark(c: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    c.bench_with_input(
        BenchmarkId::new("bulk_update", NUM_ELEMENTS),
        &NUM_ELEMENTS,
        |b: &mut Bencher, &num_elements| {
            b.iter_custom(|iters| {
                let mut sum = Duration::ZERO;

                // Create a tree with initial values (outside the timing loop)
                qsbr_reclaimer().register_thread();
                let mut pairs = Vec::with_capacity(num_elements);
                for i in 0..num_elements {
                    pairs.push((
                        OwnedThinArc::new(i),
                        OwnedThinPtr::new_from_str(&format!("value{}", i)),
                    ));
                }
                let tree: BTree<usize, str> = BTree::bulk_load_parallel(pairs);

                // Time the updates
                for iter in 0..iters {
                    // Generate update pairs
                    let mut updates = Vec::with_capacity(num_elements);
                    for i in 0..num_elements {
                        updates.push((
                            OwnedThinArc::new(i),
                            OwnedThinPtr::new_from_str(&format!("updated_value - {}, {}", iter, i)),
                        ));
                    }

                    let start = std::time::Instant::now();
                    tree.bulk_update_parallel(updates);
                    sum += start.elapsed();
                }

                unsafe { qsbr_reclaimer().deregister_current_thread_and_mark_quiescent() };
                println!(
                    "done - bulk_update iters: {}, elapsed: {:?}, num_elements: {}",
                    iters, sum, num_elements
                );
                sum
            })
        },
    );
}

fn scan_parallel_benchmark(c: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    c.bench_with_input(
        BenchmarkId::new("scan_parallel", NUM_ELEMENTS),
        &NUM_ELEMENTS,
        |b: &mut Bencher, &num_elements| {
            b.iter_custom(|iters| {
                // Setup: Create tree outside the timing loop for repeated iterations
                let _guard = qsbr_reclaimer().guard();
                let mut pairs = Vec::with_capacity(num_elements);
                for i in 0..num_elements {
                    pairs.push((
                        OwnedThinArc::new(i),                               // K: usize
                        OwnedThinPtr::new_from_str(&format!("value{}", i)), // V: str
                    ));
                }
                let tree: BTree<usize, str> = BTree::bulk_load_parallel(pairs);

                // Define start_key, end_key, and predicate
                // Scan the entire tree or a significant portion
                let start_key_val = 0;
                let end_key_val = if num_elements > 0 {
                    num_elements - 1
                } else {
                    0
                };

                let start_key = OwnedThinArc::new(start_key_val);
                let end_key = OwnedThinArc::new(end_key_val);
                let predicate = |_v: &str| true; // Simple predicate that always returns true

                let mut sum = Duration::ZERO;
                for _ in 0..iters {
                    let start_instant = std::time::Instant::now();
                    // tree.scan_parallel is a method on BTree that calls the actual parallel scan logic
                    let _result = tree.scan_parallel(
                        start_key.share()..end_key.share(),
                        &predicate,
                    );
                    sum += start_instant.elapsed();
                }

                println!(
                    "done - scan_parallel iters: {}, elapsed: {:?}, num_elements: {}",
                    iters, sum, num_elements
                );
                sum
            })
        },
    );
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_ops");
    group.sample_size(10); // Consider adjusting sample_size if benchmarks are too long/short
    bulk_load_benchmark(&mut group);
    bulk_update_benchmark(&mut group);
    scan_parallel_benchmark(&mut group);
}

criterion_group!(benches, bench);
criterion_main!(benches);
