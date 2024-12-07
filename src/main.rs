use aider_tree::BTree;

fn main() {
    let mut tree: BTree<i32, String> = BTree::new();
    tree.insert(1, "one".to_string());
    tree.insert(2, "two".to_string());

    assert_eq!(tree.get(&1), Some(&"one".to_string()));
    assert_eq!(tree.get(&2), Some(&"two".to_string()));
    assert_eq!(tree.get(&3), None);
}
#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::seq::IteratorRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;

    #[test]
    fn test_random_inserts_gets_and_removes_with_seed() {
        // Test with predefined interesting seeds
        for &seed in &INTERESTING_SEEDS {
            run_random_operations_with_seed(seed);
        }

        // Test with a random seed
        let random_seed: u64 = rand::thread_rng().gen();
        println!("Using random seed: {}", random_seed);
        run_random_operations_with_seed(random_seed);
    }

    fn run_random_operations_with_seed(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut tree = BTree::new();
        let mut reference_map = HashMap::new();
        println!("Using seed: {}", seed);

        // Perform random operations for a while
        for _ in 0..1000000 {
            let operation = rng.gen_range(0..3);
            match operation {
                0 => {
                    // Random insert
                    let key = rng.gen_range(0..1000);
                    let value = format!("value{}", key);
                    tree.insert(key, value.clone());
                    reference_map.insert(key, value);
                }
                1 => {
                    // Random get
                    let key = rng.gen_range(0..1000);
                    let btree_result = tree.get(&key);
                    let hashmap_result = reference_map.get(&key);
                    if btree_result != hashmap_result {
                        println!("Mismatch for key {}", key);
                        println!("btree_result: {:?}", btree_result);
                        println!("hashmap_result: {:?}", hashmap_result);
                        tree.print_tree();
                        tree.check_invariants();
                    }
                    assert_eq!(btree_result, hashmap_result, "Mismatch for key {}", key);
                }
                2 => {
                    // Random remove
                    if !reference_map.is_empty() {
                        let key = *reference_map.keys().choose(&mut rng).unwrap();
                        tree.remove(&key);
                        reference_map.remove(&key);
                    }
                }
                _ => unreachable!(),
            }
        }

        println!("tree.print_tree()");
        tree.check_invariants();
        tree.print_tree();
        // Verify all keys at the end
        for key in reference_map.keys() {
            assert_eq!(
                tree.get(key),
                reference_map.get(key),
                "Final verification failed for key {}",
                key
            );
        }
    }

    const INTERESTING_SEEDS: [u64; 1] = [13142251578868436595];
}
