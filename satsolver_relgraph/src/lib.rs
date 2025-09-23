use std::collections::HashMap;

use loopy_relations::{FramelessInput, Input, Output};
use relation_pipeline::ops::SaveOp;
use sat::{Atom, Level, Literal, LiteralCause, RuleIndex, Sign};

mod construct;

pub mod signal;

pub struct RelGraph {
    rules_input: FramelessInput<(RuleIndex, Literal)>,
    level_input: Input<Level>,
    assign_input: Input<Literal>,
    assign_output: Output<(Literal, LiteralCause), SaveOp<(Literal, LiteralCause)>>,
    decision_literals: HashMap<Literal, Level>,
    violated_output: Output<RuleIndex, SaveOp<RuleIndex>>,
    conflict_output: Output<Literal, SaveOp<Literal>>,
}

impl RelGraph {
    pub fn derive_conflict_rule(&mut self) -> (Vec<Literal>, Level) {
        //     let mut rule = Vec::from_iter(self.resolution_output.iter().copied());
        //     rule.sort();
        //     let level = self
        //         .resolution_level_output
        //         .iter()
        //         .next()
        //         .copied()
        //         .unwrap_or(0);
        //     (rule, level)
        todo!("dspyz")
    }

    pub fn add_rule(&self, rule_index: RuleIndex, new_rule: &[Literal]) {
        for &x in new_rule {
            self.rules_input.insert((rule_index, x));
        }
    }

    pub fn next_literal(&mut self) -> Option<Literal> {
        //     self.next_literal.iter().next().copied()
        todo!("dspyz")
    }

    pub fn select_literal(&mut self, literal: Literal, level: Level) {
        self.assign_input.insert(literal);
        self.level_input.insert(level);
        self.decision_literals.insert(literal, level);
    }

    pub fn all_assignments(mut self) -> HashMap<Atom, Sign> {
        HashMap::from_iter(
            self.decision_literals
                .keys()
                .chain(self.assign_output.iter().map(|(lit, _)| lit))
                .map(Literal::atom_and_sign),
        )
    }
}
