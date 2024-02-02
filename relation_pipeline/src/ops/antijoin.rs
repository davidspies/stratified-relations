use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use l2_map::L2Map;

use crate::op::{CommitId, RelationalOp};

use super::l2_util::add;

pub(crate) struct Antijoin<
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V)>,
    J: RelationalOp<T = K>,
> {
    input1: I,
    kvs1: L2Map<K, V, i64>,
    input2: J,
    kvs2: HashMap<K, i64>,
}

impl<K, V, I, J> Antijoin<K, V, I, J>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V)>,
    J: RelationalOp<T = K>,
{
    pub(crate) fn new(input1: I, input2: J) -> Self {
        Self {
            input1,
            kvs1: L2Map::new(),
            input2,
            kvs2: HashMap::new(),
        }
    }
}

impl<K, V, I, J> RelationalOp for Antijoin<K, V, I, J>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V)>,
    J: RelationalOp<T = K>,
{
    type T = (K, V);
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut((K, V), i64)) {
        self.input2.for_each(commit_id, |k, n2| {
            if n2 == 0 {
                return;
            }
            match self.kvs2.entry(k) {
                hash_map::Entry::Occupied(mut e) => {
                    let count = e.get_mut();
                    *count += n2;
                    if *count == 0 {
                        let (k, _) = e.remove_entry();
                        for (v1, n1) in self.kvs1.get_iter(&k) {
                            f((k.clone(), v1.clone()), *n1);
                        }
                    }
                }
                hash_map::Entry::Vacant(e) => {
                    for (v1, n1) in self.kvs1.get_iter(e.key()) {
                        f((e.key().clone(), v1.clone()), -*n1);
                    }
                    e.insert(n2);
                }
            }
        });
        self.input1.for_each(commit_id, |(k, v1), n1| {
            if n1 == 0 {
                return;
            }
            if !self.kvs2.contains_key(&k) {
                f((k.clone(), v1.clone()), n1);
            }
            add(&mut self.kvs1, k, v1, n1);
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
