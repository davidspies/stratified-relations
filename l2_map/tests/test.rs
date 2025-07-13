use l2_map::L2Map;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::{HashMap, HashSet};

#[test]
fn test_l2map_equivalence() {
    let mut rng: StdRng = SeedableRng::seed_from_u64(31416);

    let mut l2_map: L2Map<i32, i32, i32> = L2Map::new();
    let mut hash_map: HashMap<i32, HashMap<i32, i32>> = HashMap::new();

    for _ in 0..1000 {
        let k1: i32 = rng.random_range(0..10);
        let k2: i32 = rng.random_range(0..10);
        let v: i32 = rng.random_range(0..100);

        let l2_map_replaced = l2_map.insert(k1, k2, v);
        let hash_map_replaced = hash_map.entry(k1).or_default().insert(k2, v);
        assert_eq!(l2_map_replaced, hash_map_replaced);

        assert_eq!(
            l2_map.get(&k1, &k2),
            hash_map.get(&k1).and_then(|inner_map| inner_map.get(&k2))
        );
    }

    for _ in 0..500 {
        let k1: i32 = rng.random_range(0..10);
        let k2: i32 = rng.random_range(0..10);

        let l2_map_removed = l2_map.remove(&k1, &k2);
        let hash_map_removed = match hash_map.get_mut(&k1) {
            Some(inner_map) => {
                let result = inner_map.remove(&k2);
                if inner_map.is_empty() {
                    hash_map.remove(&k1);
                }
                result
            }
            None => None,
        };
        assert_eq!(l2_map_removed, hash_map_removed);

        assert_eq!(
            l2_map.get(&k1, &k2),
            hash_map.get(&k1).and_then(|inner_map| inner_map.get(&k2))
        );
    }

    for k1 in 0..10 {
        let l2_iter: HashSet<_> = l2_map.get_iter(&k1).copied().collect();
        let hash_map_iter: HashSet<_> = hash_map.get(&k1).map_or(HashSet::new(), |inner_map| {
            inner_map.iter().map(|(&k2, &v)| (k2, v)).collect()
        });
        assert_eq!(l2_iter, hash_map_iter);
    }
}
