use std::io::BufRead;

use crate::Literal;

pub use self::scanner::Scanner;

mod scanner;

pub type Rule = Vec<Literal>;

pub fn rules(input: impl BufRead) -> impl Iterator<Item = Rule> {
    let mut iter = input.lines();
    let header = loop {
        let line: String = iter.next().unwrap().unwrap().trim().into();
        if !(line.is_empty() || line.starts_with('c')) {
            break line;
        }
    };
    let mut scanner = Scanner::with_header(header, iter);
    assert_eq!(scanner.next_token(), Some("p".into()));
    assert_eq!(scanner.next_token(), Some("cnf".into()));
    let _nvars = scanner.next_usize();
    let nclauses = scanner.next_usize();
    (0..nclauses).map(move |_| {
        let mut clause = Vec::new();
        loop {
            let x = scanner.next_isize();
            if x == 0 {
                break;
            } else {
                clause.push(Literal(x))
            }
        }
        clause
    })
}
