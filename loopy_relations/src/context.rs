use std::hash::Hash;

use relation_pipeline::{InputRelation, Relation, RelationalOp};

use crate::{
    FirstOccurrencesInput, Input, InterruptId, Output,
    feeder::{FeedResult, Feeder, Interrupter},
    frameless_input::FramelessInput,
    input::IsTrackedInput,
};

#[derive(Default)]
pub struct Context<C> {
    inner: C,
    feeders: Vec<Box<dyn Feeder>>,
    inputs: Vec<Box<dyn IsTrackedInput>>,
}

pub type CreationContext = Context<relation_pipeline::CreationContext>;
pub type ExecutionContext = Context<relation_pipeline::ExecutionContext>;

impl CreationContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_first_occurrences_input<
        K: Eq + Hash + Clone + 'static,
        V: Ord + Hash + Clone + 'static,
    >(
        &mut self,
    ) -> (FirstOccurrencesInput<K, V>, InputRelation<(K, V)>) {
        let (inner, rel) = self.inner.new_input();
        let input = FirstOccurrencesInput::new(inner);
        self.inputs.push(Box::new(input.clone()));
        (input, rel)
    }

    pub fn new_frameless_input<T: Eq + Hash + Clone + 'static>(
        &mut self,
    ) -> (FramelessInput<T>, InputRelation<T>) {
        let (inp, rel) = self.inner.new_input();
        (FramelessInput::new(inp), rel)
    }

    pub fn new_input<T: Eq + Hash + Clone + 'static>(
        &mut self,
    ) -> (Input<T>, Relation<T, impl RelationalOp<T = T> + use<T>>) {
        let (inner, rel) = self.new_first_occurrences_input::<T, ()>();
        (Input(inner), rel.fsts())
    }

    pub fn set_first_occurrences_feedback<
        K: Eq + Hash + Clone + 'static,
        V: Ord + Hash + Clone + 'static,
    >(
        &mut self,
        output: Relation<(K, V), impl RelationalOp<T = (K, V)> + 'static>,
        input: FirstOccurrencesInput<K, V>,
    ) {
        assert!(self.inner.matches_relation(&output));
        assert!(input.matches_context(&self.inner));
        self.feeders
            .push(Box::new((self.inner.output(output), input)));
    }

    pub fn set_feedback<I: FeedbackableFrom<O>, O>(&mut self, output: O, input: I) {
        input.feedback_from(self, output);
    }

    pub fn set_interrupt<T: Eq + Hash + Clone + 'static, Op: RelationalOp<T = T> + 'static>(
        &mut self,
        relation: Relation<T, Op>,
        interrupt_id: InterruptId,
    ) {
        assert!(self.inner.matches_relation(&relation));
        self.feeders.push(Box::new(Interrupter {
            output: self.output(relation),
            interrupt_id,
        }));
    }

    pub fn begin(self) -> ExecutionContext {
        ExecutionContext {
            inner: self.inner.begin(),
            feeders: self.feeders,
            inputs: self.inputs,
        }
    }

    #[must_use]
    pub fn output<T: Eq + Hash + Clone, Op: RelationalOp<T = T>>(
        &self,
        relation: Relation<T, Op>,
    ) -> Output<T, Op::Unconsolidated> {
        Output::new(self.inner.output(relation.unconsolidate()))
    }

    pub fn constant<T>(&self, values: impl IntoIterator<Item = T>) -> InputRelation<T> {
        self.inner.constant(values.into_iter().map(|x| (x, 1)))
    }
}

impl ExecutionContext {
    pub fn commit(&mut self) -> Option<InterruptId> {
        'outer: loop {
            self.inner.commit();
            for feeder in &mut self.feeders {
                match feeder.feed() {
                    FeedResult::Unchanged => {}
                    FeedResult::Changed => continue 'outer,
                    FeedResult::Interrupt(id) => return Some(id),
                }
            }
            return None;
        }
    }

    pub fn push_frame(&mut self) {
        for input in &mut self.inputs {
            input.push_frame();
        }
    }

    #[track_caller]
    pub fn pop_frame_(&mut self) {
        for input in &mut self.inputs {
            input.pop_frame();
        }
    }

    #[track_caller]
    pub fn pop_frame(&mut self) {
        self.pop_frame_();
        self.commit();
    }

    pub fn with_frame_<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        self.push_frame();
        let result = f(self);
        self.pop_frame_();
        result
    }

    pub fn with_frame<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let result = self.with_frame_(f);
        self.commit();
        result
    }
}

pub trait FeedbackableFrom<O> {
    fn feedback_from(self, context: &mut CreationContext, output: O);
}

impl<
    K: Eq + Hash + Clone + 'static,
    V: Ord + Hash + Clone + 'static,
    Op: RelationalOp<T = (K, V)> + 'static,
> FeedbackableFrom<Relation<(K, V), Op>> for FirstOccurrencesInput<K, V>
{
    fn feedback_from(self, context: &mut CreationContext, output: Relation<(K, V), Op>) {
        context.set_first_occurrences_feedback(output, self)
    }
}

impl<T: Eq + Hash + Clone + 'static, Op: RelationalOp<T = T> + 'static>
    FeedbackableFrom<Relation<T, Op>> for Input<T>
{
    fn feedback_from(self, context: &mut CreationContext, output: Relation<T, Op>) {
        context.set_first_occurrences_feedback(output.map_h(|x| (x, ())), self.0)
    }
}

impl<T: Eq + Hash + Clone + 'static, Op: RelationalOp<T = T> + 'static>
    FeedbackableFrom<Relation<T, Op>> for FramelessInput<T>
{
    fn feedback_from(self, context: &mut CreationContext, output: Relation<T, Op>) {
        assert!(context.inner.matches_relation(&output));
        assert!(self.matches_context(&context.inner));
        context
            .feeders
            .push(Box::new((context.inner.output(output), self)));
    }
}
