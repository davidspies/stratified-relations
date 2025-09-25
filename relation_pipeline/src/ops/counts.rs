use std::{collections::HashMap, hash::Hash};

use multisets::MultiSet;

use crate::op::{CommitId, RelationalOp};

pub(crate) struct Counts<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> {
    relation: Op,
    counts: HashMap<T, i64>,
}

impl<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> Counts<T, Op> {
    pub fn new(relation: Op) -> Self {
        Self {
            relation,
            counts: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> RelationalOp for Counts<T, Op> {
    type T = (T, i64);

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut((T, i64), i64)) {
        self.relation.for_each(commit_id, |t, n| {
            assert!(n != 0);
            let new_count = self.counts.add(t.clone(), n);
            let old_count = new_count - n;
            if old_count != 0 {
                f((t.clone(), old_count), -1);
            }
            if new_count != 0 {
                f((t, new_count), 1);
            }
        })
    }
    fn consolidate(self) -> impl RelationalOp<T = (T, i64)> {
        Counts::new(self.relation.consolidate())
    }
}
