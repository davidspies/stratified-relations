use std::{cell::Cell, rc::Rc};

use derive_where::derive_where;
use swap_channel::Sender;

use crate::op::CommitId;

#[derive_where(Clone)]
pub struct Input<T> {
    sender: Sender<(T, CommitId, i64)>,
    commit_id: Rc<Cell<u64>>,
}

impl<T> Input<T> {
    pub(crate) fn new(sender: Sender<(T, CommitId, i64)>, commit_id: Rc<Cell<u64>>) -> Self {
        Self { sender, commit_id }
    }

    pub(crate) fn commit_id(&self) -> &Rc<Cell<u64>> {
        &self.commit_id
    }

    pub fn update(&self, value: T, count: i64) {
        self.sender.send((value, self.commit_id.get() + 1, count));
    }
}
