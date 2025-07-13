use std::{cell::Cell, rc::Rc};

use crate::{Input, InputRelation, Output, Relation, RelationalOp, ops::InputOp};

#[derive(Default)]
pub struct CreationContext {
    commit_id: Rc<Cell<u64>>,
}

impl CreationContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn new_input<T>(&self) -> (Input<T>, InputRelation<T>) {
        let (sender, receiver) = swap_channel::new();
        (
            Input::new(sender, Rc::clone(&self.commit_id)),
            Relation::new(InputOp::new(receiver), self.commit_id.clone()),
        )
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

    pub fn constant<T>(&self, values: impl IntoIterator<Item = (T, i64)>) -> InputRelation<T> {
        let (input, relation) = self.new_input();
        for (x, count) in values {
            input.update(x, count);
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
