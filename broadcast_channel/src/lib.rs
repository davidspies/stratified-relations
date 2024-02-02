use std::cell::RefCell;

use derive_where::derive_where;

pub use swap_channel::Receiver;

#[derive_where(Default)]
pub struct Sender<T: Clone>(RefCell<Vec<swap_channel::Sender<T>>>);

impl<T: Clone> Sender<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self) -> Receiver<T> {
        let (tx, rx) = swap_channel::new();
        self.0.borrow_mut().push(tx);
        rx
    }

    pub fn send(&self, x: T) {
        let senders = self.0.borrow();
        for (i, tx) in senders.iter().enumerate() {
            if i == senders.len() - 1 {
                tx.send(x);
                break;
            } else {
                tx.send(x.clone());
            }
        }
    }
}

pub fn new<T: Clone>() -> (Sender<T>, Receiver<T>) {
    let sender = Sender::new();
    let receiver = sender.subscribe();
    (sender, receiver)
}
