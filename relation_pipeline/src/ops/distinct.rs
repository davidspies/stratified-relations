use std::{collections::HashMap, hash::Hash};

use multisets::MultiSet;

use crate::op::{CommitId, RelationalOp};

pub(crate) struct Distinct<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> {
    relation: Op,
    counts: HashMap<T, i64>,
}

impl<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> Distinct<T, Op> {
    pub fn new(relation: Op) -> Self {
        Self {
            relation,
            counts: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> RelationalOp for Distinct<T, Op> {
    type T = T;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.relation.for_each(commit_id, |t, n| {
            assert!(n != 0);
            let new_count = self.counts.add(t.clone(), n);
            let old_count = new_count - n;
            if old_count == 0 {
                f(t, 1);
            } else if new_count == 0 {
                f(t, -1);
            }
        })
    }
    fn consolidate<'a>(self) -> impl RelationalOp<T = T> + 'a
    where
        Self: 'a,
    {
        Distinct::new(self.relation.consolidate())
    }
}
