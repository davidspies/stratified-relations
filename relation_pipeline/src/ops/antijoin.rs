use std::{collections::HashMap, hash::Hash};

use l2_map::L2Map;
use multisets::MultiSet;

use crate::op::{CommitId, RelationalOp};

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
            assert!(n2 != 0);
            let new_val = self.kvs2.add(k.clone(), n2);
            let old_val = new_val - n2;
            if old_val == 0 {
                for (v1, n1) in self.kvs1.get_iter(&k) {
                    f((k.clone(), v1.clone()), -*n1);
                }
            } else if new_val == 0 {
                for (v1, n1) in self.kvs1.get_iter(&k) {
                    f((k.clone(), v1.clone()), *n1);
                }
            }
        });
        self.input1.for_each(commit_id, |(k, v1), n1| {
            assert!(n1 != 0);
            if !self.kvs2.contains_key(&k) {
                f((k.clone(), v1.clone()), n1);
            }
            self.kvs1.add((k, v1), n1);
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
