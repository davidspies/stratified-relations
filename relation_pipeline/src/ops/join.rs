use std::hash::Hash;

use l2_map::L2Map;

use crate::op::{CommitId, RelationalOp};

use super::l2_util::add;

pub(crate) struct Join<
    K: Clone + Eq + Hash,
    V1: Clone + Eq + Hash,
    V2: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V1)>,
    J: RelationalOp<T = (K, V2)>,
> {
    input1: I,
    kvs1: L2Map<K, V1, i64>,
    input2: J,
    kvs2: L2Map<K, V2, i64>,
}

impl<K, V1, V2, I, J> Join<K, V1, V2, I, J>
where
    K: Clone + Eq + Hash,
    V1: Clone + Eq + Hash,
    V2: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V1)>,
    J: RelationalOp<T = (K, V2)>,
{
    pub(crate) fn new(input1: I, input2: J) -> Self {
        Self {
            input1,
            kvs1: L2Map::new(),
            input2,
            kvs2: L2Map::new(),
        }
    }
}

impl<K, V1, V2, I, J> RelationalOp for Join<K, V1, V2, I, J>
where
    K: Clone + Eq + Hash,
    V1: Clone + Eq + Hash,
    V2: Clone + Eq + Hash,
    I: RelationalOp<T = (K, V1)>,
    J: RelationalOp<T = (K, V2)>,
{
    type T = (K, (V1, V2));
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut((K, (V1, V2)), i64)) {
        self.input1.for_each(commit_id, |(k, v1), n1| {
            if n1 == 0 {
                return;
            }
            let v2s = self.kvs2.get_iter(&k);
            for (v2, n2) in v2s {
                f((k.clone(), (v1.clone(), v2.clone())), n1 * *n2);
            }
            add(&mut self.kvs1, k, v1, n1);
        });
        self.input2.for_each(commit_id, |(k, v2), n2| {
            let v1s = self.kvs1.get_iter(&k);
            for (v1, n1) in v1s {
                f((k.clone(), (v1.clone(), v2.clone())), *n1 * n2);
            }
            add(&mut self.kvs2, k, v2, n2);
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
