use std::{collections::HashMap, hash::Hash};

use multisets::MultiSet;

use crate::ops::Consolidate;

pub(crate) type CommitId = u64;

pub trait RelationalOp {
    type T;

    fn for_each(&mut self, commit_id: CommitId, f: impl FnMut(Self::T, i64));
    fn dump_to_map(&mut self, commit_id: u64, counts: &mut HashMap<Self::T, i64>)
    where
        Self::T: Eq + Hash,
    {
        self.for_each(commit_id, |x, n| {
            counts.add(x, n);
        });
    }
    fn consolidate<'a>(self) -> impl RelationalOp<T = Self::T> + 'a
    where
        Self: Sized + 'a,
        Self::T: Eq + Hash,
    {
        Consolidate::new(self)
    }
}
