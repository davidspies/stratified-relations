use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::io::{BufRead, BufReader, Write};
use std::{collections::HashMap, fs::File, path::PathBuf};

use clap::Parser;
use sat::{Atom, Literal, RuleIndex, format_rule, parse, sanitize_rule};

/// Translates an EDRAT proof int a DRAT proof.
#[derive(Parser)]
struct Opts {
    /// Sets the input CNF file to use
    #[clap()]
    cnf: PathBuf,

    /// Sets the input EDRAT proof file to use
    #[clap()]
    edrat: PathBuf,

    /// Sets the output DRAT proof file to write to
    #[clap()]
    drat: PathBuf,
}

fn main() {
    let opts = Opts::parse();
    let base_rules = Vec::from_iter(parse::rules(BufReader::new(File::open(opts.cnf).unwrap())));
    let edrat_stream = parse::Scanner::new(BufReader::new(File::open(opts.edrat).unwrap()).lines());
    let output = File::create(opts.drat).unwrap();
    Runner::new(base_rules, edrat_stream, output).run();
}

struct Runner<I: Iterator<Item = std::io::Result<String>>> {
    next_rule_index: usize,
    literal_to_rule: HashMap<Literal, HashSet<RuleIndex>>,
    index_to_rule: HashMap<RuleIndex, Vec<Literal>>,
    rule_to_index: HashMap<Vec<Literal>, RuleIndex>,
    edrat_stream: parse::Scanner<I>,
    output: File,
}

impl<I: Iterator<Item = std::io::Result<String>>> Runner<I> {
    fn new(base_rules: Vec<Vec<Literal>>, edrat_stream: parse::Scanner<I>, output: File) -> Self {
        let mut next_rule_index = 0;
        let mut index_to_rule = HashMap::new();
        let mut rule_to_index = HashMap::new();
        let mut literal_to_rule: HashMap<Literal, HashSet<RuleIndex>> = HashMap::new();
        for r in base_rules {
            let Ok(r) = sanitize_rule(r) else { continue };
            let Entry::Vacant(entry) = rule_to_index.entry(r.clone()) else {
                continue;
            };
            let ri = RuleIndex(next_rule_index);
            entry.insert(ri);
            next_rule_index += 1;
            for &x in &r {
                literal_to_rule.entry(x).or_default().insert(ri);
            }
            index_to_rule.insert(ri, r);
        }
        Self {
            next_rule_index,
            literal_to_rule,
            index_to_rule,
            rule_to_index,
            edrat_stream,
            output,
        }
    }

    fn run(&mut self) {
        while let Some(first_token) = self.edrat_stream.next_token() {
            match first_token.as_str() {
                "d" => {
                    let mut rule = vec![];
                    loop {
                        let n = self.edrat_stream.next_isize();
                        if n == 0 {
                            break;
                        }
                        rule.push(Literal(n));
                    }
                    let rule = sanitize_rule(rule).unwrap();
                    writeln!(self.output, "d {}", format_rule(&rule)).unwrap();
                    let index = self.rule_to_index.remove(&rule).unwrap();
                    self.index_to_rule.remove(&index);
                    for &lit in &rule {
                        self.literal_to_rule
                            .get_mut(&lit)
                            .unwrap()
                            .retain(|&ri| ri != index);
                    }
                }
                "=" => {
                    let a = Atom(self.edrat_stream.next_usize());
                    let b = Literal(self.edrat_stream.next_isize());
                    let zero = self.edrat_stream.next_isize();
                    assert_eq!(zero, 0);
                    writeln!(self.output, "{} {} 0", a.neg().0, b.0).unwrap();
                    writeln!(self.output, "{} {} 0", a.pos().0, (!b).0).unwrap();
                    self.replace(a.neg(), !b);
                    self.replace(a.pos(), b);
                    writeln!(self.output, "d {} {} 0", a.neg().0, b.0).unwrap();
                    writeln!(self.output, "d {} {} 0", a.pos().0, (!b).0).unwrap();
                }
                "0" => {
                    writeln!(self.output, "0").unwrap();
                    break;
                }
                n => {
                    let mut rule = vec![Literal(n.parse::<isize>().unwrap())];
                    loop {
                        let n = self.edrat_stream.next_isize();
                        if n == 0 {
                            break;
                        }
                        rule.push(Literal(n));
                    }
                    // It may be a RAT clause so we can't sanitize it when emitting
                    writeln!(self.output, "{}", format_rule(&rule)).unwrap();
                    let rule = sanitize_rule(rule).unwrap();
                    let rule_index = RuleIndex(self.next_rule_index);
                    self.next_rule_index += 1;
                    self.index_to_rule.insert(rule_index, rule.clone());
                    for &lit in &rule {
                        self.literal_to_rule
                            .entry(lit)
                            .or_default()
                            .insert(rule_index);
                    }
                    self.rule_to_index.insert(rule, rule_index);
                }
            }
        }
    }

    fn replace(&mut self, a: Literal, b: Literal) {
        for ri in self.literal_to_rule.remove(&a).unwrap_or_default() {
            let Entry::Occupied(mut rule) = self.index_to_rule.entry(ri) else {
                unreachable!()
            };
            let old_rule = rule.get().clone();
            assert!(old_rule.contains(&a));
            let new_rule =
                Vec::from_iter(old_rule.iter().copied().map(|x| if x == a { b } else { x }));
            let new_rule = sanitize_rule(new_rule)
                .ok()
                .filter(|new_rule| !self.rule_to_index.contains_key(new_rule));
            match new_rule {
                Some(new_rule) => {
                    writeln!(self.output, "{}", format_rule(&new_rule)).unwrap();
                    *rule.get_mut() = new_rule.clone();
                    self.literal_to_rule.entry(b).or_default().insert(ri);
                    self.rule_to_index.insert(new_rule, ri);
                }
                None => {
                    rule.remove();
                    for &lit in &old_rule {
                        if lit != a {
                            self.literal_to_rule
                                .get_mut(&lit)
                                .unwrap()
                                .retain(|&index| index != ri);
                        }
                    }
                }
            }
            writeln!(self.output, "d {}", format_rule(&old_rule)).unwrap();
            let index = self.rule_to_index.remove(&old_rule);
            assert_eq!(index, Some(ri));
        }
    }
}
