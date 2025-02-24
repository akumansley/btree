use btree::qsbr_reclaimer;
use btree::BTree;
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
                        pairs.push((i, format!("value{}", i)));
                    }

                    qsbr_reclaimer().register_thread();
                    let start = std::time::Instant::now();
                    let _tree = BTree::bulk_load_parallel(pairs);
                    sum += start.elapsed();
                    qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                }
                println!(
                    "done - iters: {}, elapsed: {:?}, num_elements: {}",
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
                    pairs.push((i, format!("value{}", i)));
                }
                let tree: BTree<usize, String> = BTree::bulk_load_parallel(pairs);

                // Time the updates
                for iter in 0..iters {
                    // Generate update pairs
                    let mut updates = Vec::with_capacity(num_elements);
                    for i in 0..num_elements {
                        updates.push((i, format!("updated_value - {}, {}", iter, i)));
                    }

                    let start = std::time::Instant::now();
                    tree.bulk_update_parallel(updates);
                    sum += start.elapsed();
                }

                qsbr_reclaimer().deregister_current_thread_and_mark_quiescent();
                println!(
                    "done - iters: {}, elapsed: {:?}, num_elements: {}",
                    iters, sum, num_elements
                );
                sum
            })
        },
    );
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_ops");
    group.sample_size(10);
    bulk_load_benchmark(&mut group);
    bulk_update_benchmark(&mut group);
}

criterion_group!(benches, bench);
criterion_main!(benches);
