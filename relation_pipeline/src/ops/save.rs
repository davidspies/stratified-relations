use std::{
    cell::{Cell, RefCell},
    hash::Hash,
    rc::Rc,
};

use crate::{
    op::{CommitId, RelationalOp},
    Relation,
};

use super::{Consolidate, Dynamic};

struct SaveInner<T: Clone, R: RelationalOp<T = T>> {
    relation: R,
    sender: broadcast_channel::Sender<(T, i64)>,
    prev_commit_id: CommitId,
}

pub struct SaveOp<T: Clone, R: RelationalOp<T = T> = Dynamic<'static, T>> {
    input: Rc<RefCell<SaveInner<T, R>>>,
    receiver: broadcast_channel::Receiver<(T, i64)>,
}

impl<T: Clone, R: RelationalOp<T = T>> RelationalOp for SaveOp<T, R> {
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        {
            let SaveInner {
                relation,
                sender,
                prev_commit_id,
            } = &mut *self.input.borrow_mut();
            if commit_id > *prev_commit_id {
                *prev_commit_id = commit_id;
                relation.send_all(commit_id, sender);
            }
        }
        for (x, n) in self.receiver.drain() {
            f(x, n);
        }
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}

pub struct Save<T: Clone, R: RelationalOp<T = T> = Dynamic<'static, T>> {
    inner: Rc<RefCell<SaveInner<T, R>>>,
    current_commit_id: Rc<Cell<u64>>,
}

impl<T: Clone, Op: RelationalOp<T = T>> Save<T, Op> {
    pub(crate) fn new(relation: Op, commit_id: Rc<Cell<u64>>) -> Self {
        let sender = broadcast_channel::Sender::new();
        let inner = SaveInner {
            relation,
            sender,
            prev_commit_id: 0,
        };
        Save {
            inner: Rc::new(RefCell::new(inner)),
            current_commit_id: commit_id,
        }
    }
    pub fn get_(&self) -> Relation<T, SaveOp<T, Op>> {
        let input = self.inner.clone();
        let receiver = self.inner.borrow().sender.subscribe();
        Relation::new(
            SaveOp { input, receiver },
            Rc::clone(&self.current_commit_id),
        )
    }
    pub fn get(&self) -> Relation<T, Consolidate<T, SaveOp<T, Op>>>
    where
        T: Eq + Hash,
    {
        self.get_().consolidate()
    }
}
