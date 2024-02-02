use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

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
        self.for_each(commit_id, |x, n| match counts.entry(x) {
            hash_map::Entry::Vacant(e) => {
                e.insert(n);
            }
            hash_map::Entry::Occupied(mut e) => {
                let count = e.get_mut();
                *count += n;
                if *count == 0 {
                    e.remove();
                }
            }
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated;
}
