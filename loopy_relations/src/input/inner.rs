use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use l2_heaps::L2Heaps;
use relation_pipeline::RelationalOp;

use self::inner::InputInnerInner;

#[allow(clippy::module_inception)]
mod inner;

pub(super) struct InputInner<K: Eq + Hash + Clone, V: Ord + Hash + Clone> {
    inner: InputInnerInner<K, V>,
    counts: L2Heaps<K, V, i64>,
    pending_counts: HashMap<(K, V), i64>,
    unvisited_keys: HashSet<K>,
}

impl<K: Eq + Hash + Clone, V: Ord + Hash + Clone> InputInner<K, V> {
    pub(super) fn new(inner: relation_pipeline::Input<(K, V)>) -> InputInner<K, V> {
        InputInner {
            inner: InputInnerInner::new(inner),
            counts: L2Heaps::new(),
            pending_counts: HashMap::new(),
            unvisited_keys: HashSet::new(),
        }
    }

    pub(super) fn matches_context(&self, context: &relation_pipeline::CreationContext) -> bool {
        self.inner.matches_context(context)
    }

    pub(super) fn push_frame(&mut self) {
        self.inner.push_frame();
    }

    #[track_caller]
    pub(super) fn pop_frame(&mut self) {
        self.inner.pop_frame();
    }

    pub(super) fn update(&mut self, key: K, value: V) -> bool {
        // Don't add to counts for user-insertions.
        self.inner.update(key, value)
    }

    pub(super) fn insert_all(
        &mut self,
        output: &mut relation_pipeline::Output<(K, V), impl RelationalOp<T = (K, V)>>,
    ) -> bool {
        output.dump_to_map(&mut self.pending_counts);
        for ((k, v), count) in self.pending_counts.drain() {
            if add_to_counts(&mut self.counts, k.clone(), v, count) {
                self.unvisited_keys.insert(k);
            }
        }
        let mut any_inserted = false;
        for key in self.unvisited_keys.drain() {
            let (max_val, _) = self.counts.get_max(&key).unwrap();
            any_inserted |= self.inner.update(key, max_val.clone());
        }
        any_inserted
    }
}

fn add_to_counts<K: Clone + Eq + Hash, V: Clone + Ord + Hash>(
    counts: &mut L2Heaps<K, V, i64>,
    key: K,
    value: V,
    add_count: i64,
) -> bool {
    if add_count == 0 {
        return false;
    }
    if let Some(count) = counts.get_mut(&key, &value) {
        *count += add_count;
        if *count == 0 {
            counts.remove(&key, &value);
        }
        return false;
    }
    counts.insert(key, value, add_count);
    true
}
