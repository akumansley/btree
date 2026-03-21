use btree::{qsbr_reclaimer, BTree};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::time::Duration;
use thin::{QsArc, QsOwned};

const TREE_SIZE: usize = 100_000;
const NUM_OPS: usize = 500_000;

fn random_string_id(rng: &mut StdRng) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut id = String::with_capacity(20);
    id.push_str("usr_");
    for _ in 0..16 {
        id.push(HEX[rng.random_range(0..16usize)] as char);
    }
    id
}

fn usize_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("usize");
    group.sample_size(100);

    group.bench_function("get_hit", |b| {
        b.iter_custom(|iters| {
            let _guard = qsbr_reclaimer().guard();
            let mut pairs = Vec::with_capacity(TREE_SIZE);
            for i in 0..TREE_SIZE {
                pairs.push((QsArc::new(i), QsOwned::new_from_str("v")));
            }
            let tree: BTree<usize, str> = BTree::bulk_load(pairs);

            let mut rng = StdRng::seed_from_u64(42);
            let keys: Vec<usize> = (0..NUM_OPS)
                .map(|_| rng.random_range(0..TREE_SIZE))
                .collect();

            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                for &key in &keys {
                    black_box(tree.get(&key));
                }
                sum += start.elapsed();
            }
            sum
        });
    });

    group.bench_function("get_miss", |b| {
        b.iter_custom(|iters| {
            let _guard = qsbr_reclaimer().guard();
            let mut pairs = Vec::with_capacity(TREE_SIZE);
            for i in 0..TREE_SIZE {
                pairs.push((QsArc::new(i * 2), QsOwned::new_from_str("v")));
            }
            let tree: BTree<usize, str> = BTree::bulk_load(pairs);

            let mut rng = StdRng::seed_from_u64(42);
            let keys: Vec<usize> = (0..NUM_OPS)
                .map(|_| rng.random_range(0..TREE_SIZE) * 2 + 1)
                .collect();

            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                for &key in &keys {
                    black_box(tree.get(&key));
                }
                sum += start.elapsed();
            }
            sum
        });
    });

    group.bench_function("insert", |b| {
        b.iter_custom(|iters| {
            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let _guard = qsbr_reclaimer().guard();
                let tree = BTree::<usize, str>::new();
                let mut rng = StdRng::seed_from_u64(42);

                let start = std::time::Instant::now();
                for _ in 0..TREE_SIZE {
                    let key = rng.random_range(0..TREE_SIZE * 2);
                    tree.insert(QsArc::new(key), QsOwned::new_from_str("v"));
                }
                sum += start.elapsed();
                drop(tree);
            }
            sum
        });
    });
}

fn string_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("string");
    group.sample_size(50);

    let mut rng = StdRng::seed_from_u64(123);
    let all_keys: Vec<String> = (0..TREE_SIZE).map(|_| random_string_id(&mut rng)).collect();

    group.bench_function("get_hit", |b| {
        b.iter_custom(|iters| {
            let _guard = qsbr_reclaimer().guard();
            let mut pairs: Vec<(QsArc<str>, QsOwned<str>)> = all_keys
                .iter()
                .map(|k| (QsArc::new_from_str(k), QsOwned::new_from_str("v")))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let tree: BTree<str, str> = BTree::bulk_load(pairs);

            let mut rng = StdRng::seed_from_u64(42);
            let lookup_keys: Vec<String> = (0..NUM_OPS)
                .map(|_| all_keys[rng.random_range(0..TREE_SIZE)].clone())
                .collect();

            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                for key in &lookup_keys {
                    black_box(tree.get(key.as_str()));
                }
                sum += start.elapsed();
            }
            sum
        });
    });

    group.bench_function("get_miss", |b| {
        b.iter_custom(|iters| {
            let _guard = qsbr_reclaimer().guard();
            let mut pairs: Vec<(QsArc<str>, QsOwned<str>)> = all_keys
                .iter()
                .map(|k| (QsArc::new_from_str(k), QsOwned::new_from_str("v")))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let tree: BTree<str, str> = BTree::bulk_load(pairs);

            // Different seed = different random IDs = guaranteed misses
            let mut rng = StdRng::seed_from_u64(99999);
            let miss_keys: Vec<String> = (0..NUM_OPS).map(|_| random_string_id(&mut rng)).collect();

            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                for key in &miss_keys {
                    black_box(tree.get(key.as_str()));
                }
                sum += start.elapsed();
            }
            sum
        });
    });

    group.bench_function("insert", |b| {
        b.iter_custom(|iters| {
            let mut sum = Duration::ZERO;
            for _ in 0..iters {
                let _guard = qsbr_reclaimer().guard();
                let tree = BTree::<str, str>::new();

                let start = std::time::Instant::now();
                for key in &all_keys {
                    tree.insert(QsArc::new_from_str(key), QsOwned::new_from_str("v"));
                }
                sum += start.elapsed();
                drop(tree);
            }
            sum
        });
    });
}

criterion_group!(benches, usize_benchmarks, string_benchmarks);
criterion_main!(benches);
