use std::collections::HashMap;

use loopy_relations::{FirstOccurrencesInput, FramelessInput, Input, InterruptId, Output};
use sat::{Atom, Level, Literal, LiteralCause, RuleIndex, Sign};

mod construct;

pub mod signal;

pub struct RelGraph {
    rules_input: FramelessInput<(RuleIndex, Literal)>,
    assigned_input: FirstOccurrencesInput<Literal, LiteralCause>,
    level_input: Input<Level>,
    equivalence_input: FramelessInput<(Atom, Literal)>,
    next_literal: Output<Literal>,
    resolution_output: Output<Literal>,
    resolution_level_output: Output<Level>,
    assigned_output: Output<Literal>,
    discovered_singleton_output1: Output<Literal>,
    discovered_equivalence_output: Output<(Atom, Literal)>,
    discovered_singleton_output2: Output<Literal>,
    discovered_binary_output: Output<Literal>,
}

impl RelGraph {
    pub fn derive_conflict_rule(&mut self) -> (Vec<Literal>, Level) {
        let mut rule = Vec::from_iter(self.resolution_output.iter().copied());
        rule.sort();
        let level = self
            .resolution_level_output
            .iter()
            .next()
            .copied()
            .unwrap_or(0);
        (rule, level)
    }

    pub fn add_rule(&self, rule_index: RuleIndex, new_rule: &[Literal]) {
        for &x in new_rule {
            self.rules_input.insert((rule_index, x));
        }
    }

    pub fn next_literal(&mut self) -> Option<Literal> {
        self.next_literal.iter().next().copied()
    }

    pub fn select_literal(&mut self, literal: Literal, level: Level) {
        self.assigned_input
            .insert(literal, LiteralCause::DecisionLiteral(level));
        self.level_input.insert(level);
    }

    pub fn all_assignments(&mut self) -> HashMap<Atom, Sign> {
        HashMap::from_iter(self.assigned_output.iter().map(Literal::atom_and_sign))
    }

    pub fn get_discovered_rule(&mut self, interrupt_code: InterruptId) -> Vec<Literal> {
        let output = match interrupt_code {
            signal::SINGLETON_DISCOVERED_1 => &mut self.discovered_singleton_output1,
            signal::SINGLETON_DISCOVERED_2 => &mut self.discovered_singleton_output2,
            signal::BINARY_DISCOVERED => &mut self.discovered_binary_output,
            _ => panic!("unexpected interrupt code {}", interrupt_code),
        };
        let mut result = Vec::from_iter(output.iter().copied());
        assert!(
            [1, 2].contains(&result.len()),
            "unexpected rule length {} on code {}",
            result.len(),
            interrupt_code
        );
        result.sort();
        result
    }

    pub fn get_discovered_equivalence(&mut self) -> (Atom, Literal) {
        self.discovered_equivalence_output
            .iter()
            .next()
            .copied()
            .unwrap()
    }

    pub fn add_equivalence(&self, atom: Atom, literal: Literal) {
        self.equivalence_input.insert((atom, literal));
    }
}
