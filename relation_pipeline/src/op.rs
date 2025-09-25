use std::{collections::HashMap, hash::Hash};

use multisets::MultiSet;

pub(crate) type CommitId = u64;

pub trait RelationalOp {
    type T;
    type Unconsolidated: RelationalOp<T = Self::T>;

    fn for_each(&mut self, commit_id: CommitId, f: impl FnMut(Self::T, i64));
    fn send_all(
        &mut self,
        commit_id: CommitId,
        sender: &mut broadcast_channel::Sender<(Self::T, i64)>,
    ) where
        Self::T: Clone,
    {
        self.for_each(commit_id, |x, n| sender.send((x, n)));
    }
    fn dump_to_map(&mut self, commit_id: u64, counts: &mut HashMap<Self::T, i64>)
    where
        Self::T: Eq + Hash,
    {
        self.for_each(commit_id, |x, n| {
            counts.add(x, n);
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated;
}
