use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    hash::Hash,
    rc::{Rc, Weak},
};

use multisets::MultiSet;

use crate::{
    Relation,
    op::{CommitId, RelationalOp},
};

use super::Dynamic;

struct SaveInner<T: Clone, R: RelationalOp<T = T>> {
    relation: R,
    listeners: Vec<Weak<RefCell<HashMap<T, i64>>>>,
    prev_commit_id: CommitId,
    scratch: HashMap<T, i64>,
}

pub struct SaveOp<T: Clone, R: RelationalOp<T = T> = Dynamic<'static, T>> {
    input: Rc<RefCell<SaveInner<T, R>>>,
    receiver: Rc<RefCell<HashMap<T, i64>>>,
}

impl<T: Clone + Eq + Hash, R: RelationalOp<T = T>> RelationalOp for SaveOp<T, R> {
    type T = T;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        {
            let mut input = self.input.borrow_mut();
            let SaveInner {
                relation,
                listeners,
                prev_commit_id,
                scratch,
            } = &mut *input;
            if commit_id > *prev_commit_id {
                *prev_commit_id = commit_id;
                relation.dump_to_map(commit_id, scratch);
                listeners.retain_mut(|listener| {
                    let Some(listener) = listener.upgrade() else {
                        return false;
                    };
                    for (x, n) in scratch.iter() {
                        listener.borrow_mut().add(x.clone(), *n);
                    }
                    true
                });
                scratch.clear();
            }
        }
        for (x, n) in self.receiver.borrow_mut().drain() {
            f(x, n);
        }
    }
    fn consolidate<'a>(self) -> impl RelationalOp<T = Self::T> + 'a
    where
        Self: 'a,
    {
        self
    }
}

pub struct Save<T: Clone, R: RelationalOp<T = T> = Dynamic<'static, T>> {
    inner: Rc<RefCell<SaveInner<T, R>>>,
    current_commit_id: Rc<Cell<u64>>,
}

impl<T: Clone + Eq + Hash, Op: RelationalOp<T = T>> Save<T, Op> {
    pub(crate) fn new(relation: Op, commit_id: Rc<Cell<u64>>) -> Self {
        let inner = SaveInner {
            relation,
            listeners: Vec::new(),
            prev_commit_id: 0,
            scratch: HashMap::new(),
        };
        Save {
            inner: Rc::new(RefCell::new(inner)),
            current_commit_id: commit_id,
        }
    }
    pub fn get(&self) -> Relation<T, SaveOp<T, Op>> {
        let input = self.inner.clone();
        let receiver = Rc::new(RefCell::new(HashMap::new()));
        self.inner
            .borrow_mut()
            .listeners
            .push(Rc::downgrade(&receiver));
        let op = SaveOp { input, receiver };
        Relation::new(op, Rc::clone(&self.current_commit_id))
    }
}
