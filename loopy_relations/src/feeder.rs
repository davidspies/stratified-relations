use std::hash::Hash;

use relation_pipeline::RelationalOp;

use crate::{FirstOccurrencesInput, Output, frameless_input::FramelessInput};

pub type InterruptId = usize;

pub(crate) enum FeedResult {
    Unchanged,
    Changed,
    Interrupt(InterruptId),
}

pub(crate) trait Feeder {
    fn feed(&mut self) -> FeedResult;
}

impl<K: Eq + Hash + Clone, V: Ord + Hash + Clone, Op: RelationalOp<T = (K, V)>> Feeder
    for (
        relation_pipeline::Output<(K, V), Op>,
        FirstOccurrencesInput<K, V>,
    )
{
    fn feed(&mut self) -> FeedResult {
        let any_sent = self.1.insert_all(&mut self.0);
        if any_sent {
            FeedResult::Changed
        } else {
            FeedResult::Unchanged
        }
    }
}

impl<T: Eq + Hash + Clone, Op: RelationalOp<T = T>> Feeder
    for (relation_pipeline::Output<T, Op>, FramelessInput<T>)
{
    fn feed(&mut self) -> FeedResult {
        let any_sent = self.1.insert_all(&mut self.0);
        if any_sent {
            FeedResult::Changed
        } else {
            FeedResult::Unchanged
        }
    }
}

pub(crate) struct Interrupter<T, Op: RelationalOp<T = T>> {
    pub(crate) output: Output<T, Op>,
    pub(crate) interrupt_id: InterruptId,
}

impl<T: Eq + Hash + Clone, Op: RelationalOp<T = T>> Feeder for Interrupter<T, Op> {
    fn feed(&mut self) -> FeedResult {
        if self.output.is_empty() {
            FeedResult::Unchanged
        } else {
            FeedResult::Interrupt(self.interrupt_id)
        }
    }
}
