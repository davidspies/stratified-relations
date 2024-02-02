use std::fmt::Debug;

pub struct Scanner<L: Iterator> {
    line_scanner: L,
    next_tokens: Vec<String>,
}

fn split_tokens(line: String) -> Vec<String> {
    let mut v: Vec<String> = line.split_whitespace().map(String::from).collect();
    v.reverse();
    v
}

impl<Err: Debug, L: Iterator<Item = Result<String, Err>>> Scanner<L> {
    pub fn new(mut line_scanner: L) -> Self {
        let next_line = line_scanner.next().unwrap().unwrap();
        Self::with_header(next_line, line_scanner)
    }

    pub fn with_header(next_line: String, line_scanner: L) -> Self {
        Scanner {
            line_scanner,
            next_tokens: split_tokens(next_line),
        }
    }

    pub fn next_token(&mut self) -> Option<String> {
        loop {
            if let Some(token) = self.next_tokens.pop() {
                return Some(token);
            }
            let next_line = self.line_scanner.next()?.unwrap();
            self.next_tokens = split_tokens(next_line);
        }
    }

    #[track_caller]
    pub fn next_usize(&mut self) -> usize {
        self.next_token().unwrap().parse::<usize>().unwrap()
    }

    #[track_caller]
    pub fn next_isize(&mut self) -> isize {
        self.next_token().unwrap().parse::<isize>().unwrap()
    }
}
