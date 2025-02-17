use btree::qsbr::qsbr_reclaimer;
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
                    let _tree = BTree::bulk_load(pairs);
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

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load");
    group.sample_size(10);
    bulk_load_benchmark(&mut group);
}

criterion_group!(benches, bench);
criterion_main!(benches);
