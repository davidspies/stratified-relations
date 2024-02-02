use std::{cell::RefCell, hash::Hash, rc::Rc};

use relation_pipeline::RelationalOp;

use self::inner::InputInner;

mod inner;

#[derive(Clone)]
pub struct Input<T: Eq + Hash + Clone>(pub FirstOccurrencesInput<T, ()>);

impl<T: Eq + Hash + Clone> Input<T> {
    pub fn insert(&self, value: T) -> bool {
        self.0.insert(value, ())
    }
}

#[derive(Clone)]
pub struct FirstOccurrencesInput<K: Eq + Hash + Clone, V: Ord + Hash + Clone>(
    Rc<RefCell<InputInner<K, V>>>,
);

pub(crate) trait IsTrackedInput {
    fn push_frame(&mut self);
    #[track_caller]
    fn pop_frame(&mut self);
}

impl<K: Eq + Hash + Clone, V: Ord + Hash + Clone> IsTrackedInput for FirstOccurrencesInput<K, V> {
    fn push_frame(&mut self) {
        self.0.borrow_mut().push_frame();
    }

    #[track_caller]
    fn pop_frame(&mut self) {
        self.0.borrow_mut().pop_frame();
    }
}

impl<K: Eq + Hash + Clone, V: Ord + Hash + Clone> FirstOccurrencesInput<K, V> {
    pub(crate) fn new(inner: relation_pipeline::Input<(K, V)>) -> Self {
        Self(Rc::new(RefCell::new(InputInner::new(inner))))
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        self.0.borrow_mut().update(key, value)
    }

    pub(crate) fn insert_all(
        &self,
        output: &mut relation_pipeline::Output<(K, V), impl RelationalOp<T = (K, V)>>,
    ) -> bool {
        self.0.borrow_mut().insert_all(output)
    }

    pub(crate) fn matches_context(&self, context: &relation_pipeline::CreationContext) -> bool {
        self.0.borrow().matches_context(context)
    }
}
