use std::hash::Hash;

use l2_map::L2Map;

pub(super) fn add<K, V>(kvs: &mut L2Map<K, V, i64>, k: K, v: V, n1: i64)
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
{
    match kvs.get_mut(&k, &v) {
        Some(n) => {
            *n += n1;
            if *n == 0 {
                kvs.remove(&k, &v);
            }
        }
        None => {
            kvs.insert(k, v, n1);
        }
    }
}
