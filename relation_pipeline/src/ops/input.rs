use std::collections::VecDeque;

use crate::op::{CommitId, RelationalOp};

pub struct InputOp<T> {
    receiver: swap_channel::Receiver<(T, CommitId, i64)>,
    pending: VecDeque<(T, CommitId, i64)>,
}
impl<T> InputOp<T> {
    pub(crate) fn new(receiver: swap_channel::Receiver<(T, CommitId, i64)>) -> Self {
        InputOp {
            receiver,
            pending: VecDeque::new(),
        }
    }
}

impl<T> RelationalOp for InputOp<T> {
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        while let Some(&(_, id, _)) = self.pending.front() {
            if id > commit_id {
                return;
            }
            let (x, _, n) = self.pending.pop_front().unwrap();
            f(x, n);
        }
        let mut iter = self.receiver.drain();
        for (x, id, n) in &mut iter {
            if id <= commit_id {
                f(x, n);
            } else {
                self.pending.push_back((x, id, n));
                break;
            }
        }
        self.pending.extend(iter);
    }
    fn unconsolidate(self) -> Self::Unconsolidated {
        self
    }
}
