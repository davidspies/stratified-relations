use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use l2_map::L2Map;

pub(super) struct InputInnerInner<K: Eq + Hash + Clone, V: Ord + Hash + Clone> {
    inner: relation_pipeline::Input<(K, V)>,
    sent: HashMap<K, V>,
    phases: L2Map<usize, K, V>,
    next_phase: usize,
    popped_keys: Vec<K>,
}

impl<K: Eq + Hash + Clone, V: Ord + Hash + Clone> InputInnerInner<K, V> {
    pub(super) fn new(inner: relation_pipeline::Input<(K, V)>) -> Self {
        Self {
            inner,
            sent: HashMap::new(),
            phases: L2Map::new(),
            next_phase: 0,
            popped_keys: Vec::new(),
        }
    }

    pub(super) fn update(&mut self, key: K, value: V) -> bool {
        match self.sent.entry(key.clone()) {
            hash_map::Entry::Occupied(_) => return false,
            hash_map::Entry::Vacant(vac) => vac.insert(value.clone()),
        };
        if self.next_phase > 0 {
            self.phases
                .insert(self.next_phase - 1, key.clone(), value.clone());
        }
        self.inner.update((key, value), 1);
        true
    }

    pub(super) fn push_frame(&mut self) {
        self.next_phase += 1;
    }

    #[track_caller]
    pub(super) fn pop_frame(&mut self) {
        assert!(self.next_phase > 0, "no frame to pop");
        let current_phase = self.next_phase - 1;
        for (key, value) in self.phases.get_iter(&current_phase) {
            self.sent.remove(key);
            self.inner.update((key.clone(), value.clone()), -1);
            self.popped_keys.push(key.clone());
        }
        for key in self.popped_keys.drain(..) {
            self.phases.remove(&current_phase, &key);
        }
        self.next_phase = current_phase;
    }

    pub(super) fn matches_context(&self, context: &relation_pipeline::CreationContext) -> bool {
        context.matches_input(&self.inner)
    }
}
