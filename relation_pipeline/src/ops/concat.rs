use crate::op::{CommitId, RelationalOp};

pub(crate) struct Concat<T, I: RelationalOp<T = T>, J: RelationalOp<T = T>> {
    input1: I,
    input2: J,
}
impl<T, I: RelationalOp<T = T>, J: RelationalOp<T = T>> Concat<T, I, J> {
    pub(crate) fn new(input1: I, input2: J) -> Self {
        Self { input1, input2 }
    }
}

impl<T, I: RelationalOp<T = T>, J: RelationalOp<T = T>> RelationalOp for Concat<T, I, J> {
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.input1.for_each(commit_id, &mut f);
        self.input2.for_each(commit_id, &mut f);
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
