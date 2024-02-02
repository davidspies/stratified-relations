use std::{cell::RefCell, rc::Rc};

use crate::op::{CommitId, RelationalOp};

struct SplitInner<L, R, Op: RelationalOp<T = (L, R)>> {
    relation: Op,
    left_sender: swap_channel::Sender<(L, i64)>,
    right_sender: swap_channel::Sender<(R, i64)>,
    prev_commit_id: CommitId,
}

pub struct Split<T, L, R, Op: RelationalOp<T = (L, R)>> {
    input: Rc<RefCell<SplitInner<L, R, Op>>>,
    receiver: swap_channel::Receiver<(T, i64)>,
}

#[allow(clippy::type_complexity)]
pub(crate) fn split<L, R, Op: RelationalOp<T = (L, R)>>(
    relation: Op,
) -> (Split<L, L, R, Op>, Split<R, L, R, Op>) {
    let (left_sender, left_receiver) = swap_channel::new();
    let (right_sender, right_receiver) = swap_channel::new();
    let input = Rc::new(RefCell::new(SplitInner {
        relation,
        left_sender,
        right_sender,
        prev_commit_id: 0,
    }));
    (
        Split {
            input: input.clone(),
            receiver: left_receiver,
        },
        Split {
            input,
            receiver: right_receiver,
        },
    )
}

impl<T, L, R, Op: RelationalOp<T = (L, R)>> RelationalOp for Split<T, L, R, Op> {
    type T = T;
    type Unconsolidated = Self;

    fn for_each(&mut self, commit_id: CommitId, mut f: impl FnMut(T, i64)) {
        {
            let SplitInner {
                relation,
                left_sender,
                right_sender,
                prev_commit_id,
            } = &mut *self.input.borrow_mut();
            if commit_id > *prev_commit_id {
                *prev_commit_id = commit_id;
                relation.for_each(commit_id, |(l, r), n| {
                    left_sender.send((l, n));
                    right_sender.send((r, n))
                });
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
