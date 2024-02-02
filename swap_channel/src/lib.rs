use std::{cell::RefCell, collections::VecDeque, mem, rc::Rc};

use derive_where::derive_where;

#[derive_where(Clone)]
pub struct Sender<T>(Rc<RefCell<VecDeque<T>>>);

pub struct Receiver<T> {
    // Should always be empty
    receive_queue: VecDeque<T>,
    send_queue: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Sender<T> {
    pub fn send(&self, t: T) {
        self.0.borrow_mut().push_back(t);
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        self.send_queue.borrow_mut().pop_front()
    }
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        mem::swap(&mut self.receive_queue, &mut self.send_queue.borrow_mut());
        self.receive_queue.drain(..)
    }
}

pub fn new<T>() -> (Sender<T>, Receiver<T>) {
    let send_queue = Rc::new(RefCell::new(VecDeque::new()));
    let sender = Sender(send_queue.clone());
    let receiver = Receiver {
        receive_queue: VecDeque::new(),
        send_queue,
    };
    (sender, receiver)
}
