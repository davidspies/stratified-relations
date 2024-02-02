use std::{collections::HashMap, hash::Hash};

use relation_pipeline::{ops::Dynamic, RelationalOp};

pub struct Output<T, Op: RelationalOp<T = T> = Dynamic<'static, T>> {
    inner: relation_pipeline::Output<T, Op>,
    values: HashMap<T, i64>,
}

impl<T: Eq + Hash + Clone, Op: RelationalOp<T = T>> Output<T, Op> {
    pub fn new(inner: relation_pipeline::Output<T, Op>) -> Self {
        Self {
            inner,
            values: HashMap::new(),
        }
    }

    pub fn iter(&mut self) -> impl ExactSizeIterator<Item = &T> {
        self.inner.dump_to_map(&mut self.values);
        self.values.keys()
    }

    pub fn is_empty(&mut self) -> bool {
        self.inner.dump_to_map(&mut self.values);
        self.values.is_empty()
    }
}
