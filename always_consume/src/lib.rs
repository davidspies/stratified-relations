pub struct AlwaysConsume<I: Iterator>(I);

impl<I: Iterator> AlwaysConsume<I> {
    pub fn new(iter: I) -> Self {
        Self(iter)
    }
}

impl<I: Iterator> Iterator for AlwaysConsume<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<I: Iterator> Drop for AlwaysConsume<I> {
    fn drop(&mut self) {
        for _ in self {}
    }
}
