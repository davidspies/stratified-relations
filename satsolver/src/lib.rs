use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::Write;
use std::rc::Rc;

use loopy_relations::{CreationContext, ExecutionContext};
use once_cell::unsync::Lazy;
use sat::{Atom, Literal, RuleIndex, Sign, sanitize_rule};
use satsolver_relgraph::{RelGraph, signal};

pub struct Solver {
    context: ExecutionContext,
    next_rule_index: usize,
    relgraph: RelGraph,
    proof_output: Box<dyn Write>,
    required_atoms: HashSet<Atom>,
    equivalence_graph: HashMap<Literal, Literal>,
    learnt_rules: HashSet<Vec<Literal>>,
}

impl Solver {
    pub fn new(
        rules: impl IntoIterator<Item = Vec<Literal>>,
        mut proof_output: Box<dyn Write>,
    ) -> Option<Self> {
        let mut context = CreationContext::new();
        let relgraph = RelGraph::construct(&mut context);
        let mut next_rule_index = 0;
        let mut required_atoms = HashSet::new();
        for rule in rules {
            let rule = match sanitize_rule(rule) {
                Ok(rule) => rule,
                Err(req_atom) => {
                    required_atoms.insert(req_atom);
                    continue;
                }
            };
            if rule.is_empty() {
                writeln!(proof_output, "0").unwrap();
                return None;
            }
            relgraph.add_rule(RuleIndex(next_rule_index), &rule);
            next_rule_index += 1;
        }
        Some(Self {
            context: context.begin(),
            next_rule_index,
            relgraph,
            proof_output,
            required_atoms,
            equivalence_graph: HashMap::new(),
            learnt_rules: HashSet::new(),
        })
    }

    pub fn solve(mut self) -> Option<Vec<Literal>> {
        let mut level = 0;
        let mut selected_literals: HashMap<Atom, Sign> = HashMap::new();
        let mut literal_at_level: Vec<Literal> = Vec::new();
        loop {
            match self.context.commit() {
                Some(signal::ASSIGNMENT_CONFLICT | signal::VIOLATED_RULE) => {
                    let (new_rule, new_rule_level) = self.relgraph.derive_conflict_rule();
                    self.add_rule(new_rule);
                    while level >= new_rule_level {
                        if level == 0 {
                            return None;
                        }
                        self.context.pop_frame_();
                        let lit = literal_at_level.pop().unwrap();
                        let removed = selected_literals.remove(&lit.atom());
                        assert_eq!(removed, Some(lit.sign()));
                        level -= 1;
                    }
                }
                Some(signal::SELECTION_INVALIDATED) => {
                    self.context.pop_frame_();
                    let lit = literal_at_level.pop().unwrap();
                    let removed = selected_literals.remove(&lit.atom());
                    assert_eq!(removed, Some(lit.sign()));
                    level -= 1;
                }
                Some(signal::ROOT_CONFLICT) => {
                    writeln!(self.proof_output, "0").unwrap();
                    return None;
                }
                Some(
                    code @ (signal::SINGLETON_DISCOVERED_1
                    | signal::SINGLETON_DISCOVERED_2
                    | signal::BINARY_DISCOVERED),
                ) => {
                    let discovered_rule = self.relgraph.get_discovered_rule(code);
                    self.add_rule(discovered_rule);
                }
                Some(signal::EQUIVALENCE_DISCOVERED) => {
                    let (atom, lit) = self.relgraph.get_discovered_equivalence();
                    if lit.atom() == atom {
                        assert!(lit.0 < 0);
                        writeln!(self.proof_output, "{} 0", atom.0).unwrap();
                        writeln!(self.proof_output, "0").unwrap();
                        return None;
                    }
                    writeln!(self.proof_output, "= {} {} 0", atom.0, lit.0).unwrap();
                    self.relgraph.add_equivalence(atom, lit);
                    self.equivalence_graph.insert(atom.pos(), lit);
                    self.equivalence_graph.insert(atom.neg(), !lit);
                }
                Some(_) => unreachable!(),
                None => {
                    let Some(next_selection) = self.relgraph.next_literal() else {
                        return Some(self.construct_solution());
                    };
                    let (atom, sign) = next_selection.atom_and_sign();
                    let replaced = selected_literals.insert(atom, sign);
                    assert!(replaced.is_none(), "atom {} already selected", atom.0);
                    literal_at_level.push(next_selection);
                    self.context.push_frame();
                    level += 1;
                    self.relgraph.select_literal(next_selection, level);
                }
            }
        }
    }

    fn construct_solution(self) -> Vec<Literal> {
        let mut result = self.relgraph.all_assignments();
        let compressed_equivalence_graph = path_compress(&self.equivalence_graph);
        for (&x, &y) in compressed_equivalence_graph.iter() {
            let &mut ysign = result.entry(y.atom()).or_insert(Sign::Neg);
            if y.sign() == ysign {
                result.insert(x.atom(), x.sign());
            }
        }
        for &atom in &self.required_atoms {
            result.entry(atom).or_insert(Sign::Neg);
        }
        let mut result =
            Vec::from_iter(result.into_iter().map(|(atom, sign)| atom.with_sign(sign)));
        result.sort();
        result
    }

    #[track_caller]
    fn add_rule(&mut self, new_rule: Vec<Literal>) {
        self.relgraph
            .add_rule(RuleIndex(self.next_rule_index), &new_rule);
        writeln!(self.proof_output, "{}", sat::format_rule(&new_rule)).unwrap();
        let inserted = self.learnt_rules.insert(new_rule.clone());
        assert!(inserted, "learnt rule already learnt: {new_rule:?}");
        self.next_rule_index += 1;
    }
}

fn path_compress<T: Clone + Eq + Hash>(uf: &HashMap<T, T>) -> HashMap<T, T> {
    let result = Rc::<HashMap<T, Lazy<T, Box<dyn FnOnce() -> T>>>>::new_cyclic(|result| {
        HashMap::from_iter(uf.keys().map(|k| {
            let result = result.clone();
            let thunk = Lazy::new(Box::new(move || {
                let result = result.upgrade().unwrap();
                let v = &uf[k];
                result.get(v).map_or_else(|| v.clone(), |v| (*v).clone())
            }) as Box<dyn FnOnce() -> T>);
            (k.clone(), thunk)
        }))
    });
    result
        .iter()
        .map(|(k, v)| (k.clone(), (*v).clone()))
        .collect()
}
