use std::{collections::HashMap, hash::Hash};

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
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.relation.for_each(commit_id, |t, n| {
            if n == 0 {
                return;
            }
            let count = self.counts.entry(t.clone()).or_insert(0);
            let old_count = *count;
            *count += n;
            let new_count = *count;
            if new_count == 0 {
                self.counts.remove(&t);
                f(t, -1);
            } else if old_count == 0 {
                f(t, 1);
            }
        })
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
