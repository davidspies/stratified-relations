use crate::op::{CommitId, RelationalOp};

pub(crate) struct FlatMap<S, T, I: RelationalOp<T = S>, R: IntoIterator<Item = T>, F: FnMut(S) -> R>
{
    input: I,
    f: F,
}

impl<S, T, I, R, F> FlatMap<S, T, I, R, F>
where
    I: RelationalOp<T = S>,
    R: IntoIterator<Item = T>,
    F: FnMut(S) -> R,
{
    pub(crate) fn new(input: I, f: F) -> Self {
        Self { input, f }
    }
}

impl<S, T, I, R, F> RelationalOp for FlatMap<S, T, I, R, F>
where
    I: RelationalOp<T = S>,
    R: IntoIterator<Item = T>,
    F: FnMut(S) -> R,
{
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        self.input.for_each(commit_id, |x, n| {
            for y in (self.f)(x) {
                f(y, n);
            }
        });
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
