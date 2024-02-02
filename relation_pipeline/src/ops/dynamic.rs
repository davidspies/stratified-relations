use std::{collections::HashMap, hash::Hash};

use crate::op::{CommitId, RelationalOp};

pub struct Dynamic<'a, T>(Box<dyn RelationalOpDyn<'a, T = T> + 'a>);
impl<'a, T> Dynamic<'a, T> {
    pub(crate) fn new(op: impl RelationalOp<T = T> + 'a) -> Self {
        Self(Box::new(op))
    }
}

trait RelationalOpDyn<'a> {
    type T;
    fn for_each(&mut self, commit_id: CommitId, f: &mut dyn FnMut(Self::T, i64));
    fn send_all(
        &mut self,
        commit_id: CommitId,
        sender: &mut broadcast_channel::Sender<(Self::T, i64)>,
    ) where
        Self::T: Clone;
    fn dump_to_map(&mut self, commit_id: u64, counts: &mut HashMap<Self::T, i64>)
    where
        Self::T: Eq + Hash;
    fn unconsolidate(self: Box<Self>) -> Box<dyn RelationalOpDyn<'a, T = Self::T> + 'a>;
}

impl<'a, T, Op: RelationalOp<T = T>> RelationalOpDyn<'a> for Op
where
    Op::Unconsolidated: 'a,
{
    type T = T;
    fn for_each(&mut self, commit_id: CommitId, f: &mut dyn FnMut(T, i64)) {
        self.for_each(commit_id, f);
    }
    fn send_all(
        &mut self,
        commit_id: CommitId,
        sender: &mut broadcast_channel::Sender<(Self::T, i64)>,
    ) where
        Self::T: Clone,
    {
        self.send_all(commit_id, sender);
    }
    fn dump_to_map(&mut self, commit_id: u64, counts: &mut HashMap<Self::T, i64>)
    where
        Self::T: Eq + Hash,
    {
        self.dump_to_map(commit_id, counts);
    }
    fn unconsolidate(self: Box<Self>) -> Box<dyn RelationalOpDyn<'a, T = Self::T> + 'a> {
        Box::new((*self).unconsolidate())
    }
}

impl<T> RelationalOp for Dynamic<'_, T> {
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.0.for_each(commit_id, &mut f);
    }
    fn send_all(
        &mut self,
        commit_id: CommitId,
        sender: &mut broadcast_channel::Sender<(Self::T, i64)>,
    ) where
        T: Clone,
    {
        self.0.send_all(commit_id, sender);
    }
    fn dump_to_map(&mut self, commit_id: u64, counts: &mut HashMap<Self::T, i64>)
    where
        T: Eq + Hash,
    {
        self.0.dump_to_map(commit_id, counts);
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        Self(self.0.unconsolidate())
    }
}
