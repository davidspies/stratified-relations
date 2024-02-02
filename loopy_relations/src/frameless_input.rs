use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::rc::Rc;

use derive_where::derive_where;

use relation_pipeline::RelationalOp;

#[derive_where(Clone)]
pub struct FramelessInput<T>(Rc<RefCell<FramelessInputInner<T>>>);

struct FramelessInputInner<T> {
    input: relation_pipeline::Input<T>,
    sent: HashSet<T>,
    pending_counts: HashMap<T, i64>,
}

impl<T: Eq + Hash + Clone> FramelessInput<T> {
    pub(crate) fn new(input: relation_pipeline::Input<T>) -> Self {
        Self(Rc::new(RefCell::new(FramelessInputInner {
            input,
            sent: HashSet::new(),
            pending_counts: HashMap::new(),
        })))
    }

    pub fn insert(&self, value: T) {
        self.0.borrow_mut().insert(value)
    }

    pub(crate) fn insert_all(
        &self,
        output: &mut relation_pipeline::Output<T, impl RelationalOp<T = T>>,
    ) -> bool {
        self.0.borrow_mut().insert_all(output)
    }

    pub(crate) fn matches_context(&self, context: &relation_pipeline::CreationContext) -> bool {
        context.matches_input(&self.0.borrow().input)
    }
}

impl<T: Eq + Hash + Clone> FramelessInputInner<T> {
    fn insert(&mut self, value: T) {
        if self.sent.insert(value.clone()) {
            self.input.update(value, 1);
        }
    }

    fn insert_all(
        &mut self,
        output: &mut relation_pipeline::Output<T, impl RelationalOp<T = T>>,
    ) -> bool {
        let mut any_sent = false;
        output.dump_to_map(&mut self.pending_counts);
        for (value, _count) in self.pending_counts.drain() {
            if self.sent.insert(value.clone()) {
                any_sent = true;
                self.input.update(value, 1);
            }
        }
        any_sent
    }
}
