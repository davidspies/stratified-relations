use std::{cell::Cell, hash::Hash, rc::Rc};

use crate::{
    op::{CommitId, RelationalOp},
    ops::Dynamic,
};

pub(crate) struct RelationInner<T, Op: RelationalOp<T = T>> {
    pub(crate) op: Op,
}

pub struct Relation<T, Op: RelationalOp<T = T> = Dynamic<'static, T>> {
    pub(crate) relation: RelationInner<T, Op>,
    pub(crate) current_commit_id: Rc<Cell<u64>>,
}

impl<T, Op: RelationalOp<T = T>> Relation<T, Op> {
    pub(crate) fn new(op: Op, commit_id: Rc<Cell<u64>>) -> Self {
        Self {
            relation: RelationInner::new(op),
            current_commit_id: commit_id,
        }
    }

    pub(crate) fn for_each(&mut self, f: impl FnMut(T, i64)) {
        self.relation.for_each(self.current_commit_id.get(), f);
    }

    pub(crate) fn dump_to_map(&mut self, counts: &mut std::collections::HashMap<T, i64>)
    where
        T: Eq + std::hash::Hash,
    {
        self.relation
            .op
            .dump_to_map(self.current_commit_id.get(), counts);
    }
    pub fn consolidate(self) -> Relation<T, impl RelationalOp<T = T>>
    where
        T: Eq + Hash,
    {
        Relation {
            relation: RelationInner::new(self.relation.op.consolidate()),
            current_commit_id: self.current_commit_id,
        }
    }
}

impl<T, Op: RelationalOp<T = T>> RelationInner<T, Op> {
    pub(crate) fn new(op: Op) -> Self {
        Self { op }
    }
}

impl<T, Op: RelationalOp<T = T>> RelationalOp for RelationInner<T, Op> {
    type T = T;

    fn for_each(&mut self, commit_id: CommitId, f: impl FnMut(Self::T, i64)) {
        self.op.for_each(commit_id, f);
    }
    fn consolidate(self) -> impl RelationalOp<T = T>
    where
        T: Eq + Hash,
    {
        RelationInner::new(self.op.consolidate())
    }
}
