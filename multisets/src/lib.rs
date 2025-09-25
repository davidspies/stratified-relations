use std::collections::HashMap;
use std::hash::Hash;

use l2_heaps::L2Heaps;
use l2_map::L2Map;

pub trait MultiSet {
    type Item: Eq;

    fn count(&self, item: &Self::Item) -> i64;
    fn add(&mut self, item: Self::Item, count: i64) -> i64;
}

impl<T: Eq + Hash> MultiSet for HashMap<T, i64> {
    type Item = T;

    fn count(&self, item: &Self::Item) -> i64 {
        *self.get(item).unwrap_or(&0)
    }

    fn add(&mut self, item: Self::Item, count: i64) -> i64 {
        if count == 0 {
            return self.count(&item);
        }
        let mut entry = hashmap_tools::or_insert(self.entry(item), 0);
        *entry.get_mut() += count;
        let new_count = *entry.get();
        if new_count == 0 {
            entry.remove();
        }
        new_count
    }
}

impl<K1: Clone + Eq + Hash, K2: Clone + Eq + Hash, const LIM: usize> MultiSet
    for L2Map<K1, K2, i64, LIM>
{
    type Item = (K1, K2);

    fn count(&self, (k1, k2): &Self::Item) -> i64 {
        self.get(k1, k2).copied().unwrap_or(0)
    }

    fn add(&mut self, (k1, k2): Self::Item, count: i64) -> i64 {
        if count == 0 {
            return self.count(&(k1, k2));
        }
        match self.get_mut(&k1, &k2) {
            Some(v) => {
                *v += count;
                let new_count = *v;
                if new_count == 0 {
                    self.remove(&k1, &k2);
                }
                new_count
            }
            None => {
                self.insert(k1, k2, count);
                count
            }
        }
    }
}

impl<K1: Clone + Eq + Hash, K2: Clone + Ord + Hash, const LIM: usize> MultiSet
    for L2Heaps<K1, K2, i64, LIM>
{
    type Item = (K1, K2);

    fn count(&self, (k1, k2): &Self::Item) -> i64 {
        self.get(k1, k2).copied().unwrap_or(0)
    }

    fn add(&mut self, (k1, k2): Self::Item, count: i64) -> i64 {
        if count == 0 {
            return self.count(&(k1, k2));
        }
        match self.get_mut(&k1, &k2) {
            Some(v) => {
                *v += count;
                let new_count = *v;
                if new_count == 0 {
                    self.remove(&k1, &k2);
                }
                new_count
            }
            None => {
                self.insert(k1, k2, count);
                count
            }
        }
    }
}
