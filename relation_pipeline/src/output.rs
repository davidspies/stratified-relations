use crate::{ops::Dynamic, Relation, RelationalOp};

pub struct Output<T, Op: RelationalOp<T = T> = Dynamic<'static, T>>(pub(crate) Relation<T, Op>);

impl<T, Op: RelationalOp<T = T>> Output<T, Op> {
    pub fn for_each(&mut self, f: impl FnMut(T, i64)) {
        self.0.for_each(f)
    }

    pub fn dump_to_map(&mut self, counts: &mut std::collections::HashMap<T, i64>)
    where
        T: Eq + std::hash::Hash,
    {
        self.0.dump_to_map(counts)
    }
}
