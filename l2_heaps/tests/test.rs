use std::collections::{BTreeMap, HashMap};

use rand::{rngs::StdRng, Rng, SeedableRng};

use l2_heaps::L2Heaps;

#[test]
fn test_equivalence() {
    let mut rng: StdRng = SeedableRng::seed_from_u64(31416);

    let mut l2_heaps = L2Heaps::<i32, i32, i32>::new();
    let mut hash_map = HashMap::new();

    for _ in 0..1000 {
        let key: i32 = rng.gen_range(0..10);
        let key2: i32 = rng.gen_range(0..100);
        let value: i32 = rng.gen_range(0..1000);

        let heaps_inserted = l2_heaps.insert(key, key2, value);
        let map_inserted = hash_map
            .entry(key)
            .or_insert_with(BTreeMap::new)
            .insert(key2, value);
        assert_eq!(heaps_inserted, map_inserted);

        for k in 0..10 {
            assert_eq!(
                l2_heaps.get_max(&k),
                hash_map.get(&k).and_then(|set| set.iter().next_back()),
            );
        }
    }

    for _ in 0..500 {
        let key: i32 = rng.gen_range(0..10);
        let value: i32 = rng.gen_range(0..100);

        let heaps_removed = l2_heaps.remove(&key, &value);
        let map_removed = if let Some(set) = hash_map.get_mut(&key) {
            let result = set.remove(&value);
            if set.is_empty() {
                hash_map.remove(&key);
            }
            result
        } else {
            None
        };
        assert_eq!(heaps_removed, map_removed);

        for k in 0..10 {
            assert_eq!(
                l2_heaps.get_max(&k),
                hash_map.get(&k).and_then(|set| set.iter().next_back()),
            );
        }
    }
}
