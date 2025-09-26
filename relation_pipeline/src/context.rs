use std::{cell::Cell, hash::Hash, rc::Rc};

use crate::{Input, InputRelation, Output, Relation, RelationalOp, ops::InputOp};

#[derive(Default)]
pub struct CreationContext {
    commit_id: Rc<Cell<u64>>,
}

impl CreationContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn new_input_<T>(&self) -> (Input<T>, InputRelation<T>) {
        let (sender, receiver) = swap_channel::new();
        (
            Input::new(sender, Rc::clone(&self.commit_id)),
            Relation::new(InputOp::new(receiver), self.commit_id.clone()),
        )
    }
    pub fn new_input<'a, T: Clone + Eq + Hash + 'a>(
        &self,
    ) -> (Input<T>, Relation<T, impl RelationalOp<T = T> + 'a>) {
        let (input, input_relation) = self.new_input_();
        (input, input_relation.consolidate().distinct_h())
    }

    #[track_caller]
    pub fn output<T, Op: RelationalOp<T = T>>(&self, relation: Relation<T, Op>) -> Output<T, Op> {
        assert!(self.matches_relation(&relation));
        Output(relation)
    }

    pub fn begin(self) -> ExecutionContext {
        ExecutionContext {
            commit_id: self.commit_id,
        }
    }

    pub fn constant<'a, T: Clone + Eq + Hash + 'a>(
        &self,
        values: impl IntoIterator<Item = T>,
    ) -> Relation<T, impl RelationalOp<T = T> + 'a> {
        let (input, relation) = self.new_input();
        for x in values {
            input.update(x, 1);
        }
        relation
    }

    pub fn matches_relation<T, Op: RelationalOp<T = T>>(&self, relation: &Relation<T, Op>) -> bool {
        Rc::ptr_eq(&self.commit_id, &relation.current_commit_id)
    }

    pub fn matches_input<T>(&self, input: &Input<T>) -> bool {
        Rc::ptr_eq(&self.commit_id, input.commit_id())
    }

    pub fn matches_output<T, Op: RelationalOp<T = T>>(&self, output: &Output<T, Op>) -> bool {
        self.matches_relation(&output.0)
    }
}

pub struct ExecutionContext {
    commit_id: Rc<Cell<u64>>,
}

impl ExecutionContext {
    pub fn commit(&mut self) {
        self.commit_id.set(self.commit_id.get() + 1);
    }
}
