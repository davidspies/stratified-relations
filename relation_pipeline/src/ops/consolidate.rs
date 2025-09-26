use std::{collections::HashMap, hash::Hash};

use crate::op::{CommitId, RelationalOp};

pub struct Consolidate<T: Eq + Hash, Op: RelationalOp<T = T>> {
    relation: Op,
    counts: HashMap<T, i64>,
}

impl<T: Eq + Hash, Op: RelationalOp<T = T>> Consolidate<T, Op> {
    pub(crate) fn new(relation: Op) -> Self {
        Self {
            relation,
            counts: HashMap::new(),
        }
    }
}

impl<T: Eq + Hash, Op: RelationalOp<T = T>> RelationalOp for Consolidate<T, Op> {
    type T = T;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.relation.dump_to_map(commit_id, &mut self.counts);
        for (x, count) in self.counts.drain() {
            f(x, count);
        }
    }
    fn consolidate<'a>(self) -> impl RelationalOp<T = T> + 'a
    where
        Self: 'a,
    {
        self
    }
}
