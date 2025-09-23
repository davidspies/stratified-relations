use std::{collections::HashMap, ops::Not};

use loopy_relations::CreationContext;
use sat::{Level, Literal, LiteralCause, RuleIndex};

use crate::{RelGraph, signal};

impl RelGraph {
    pub fn construct(context: &mut CreationContext) -> Self {
        let (rules_input, rules) = context.new_frameless_input::<(RuleIndex, Literal)>();
        let rules = rules.save();
        let rule_inds = rules.get().fsts().distinct();
        let (level_input, levels) = context.new_input::<Level>();
        let level = levels.global_max();
        let (assign_input, assigned) = context.new_input::<Literal>();
        let assigned = assigned.save();
        let satisfied_rule_inds = rules.get().swaps().semijoin(assigned.get()).snds();
        let unsatisfied_rule_inds = rule_inds.set_minus(satisfied_rule_inds).collect();
        let reduced_rules = rules
            .get()
            .semijoin(unsatisfied_rule_inds.get())
            .swaps()
            .antijoin(assigned.get().map(Not::not))
            .swaps()
            .collect();
        let reduced_rule_inds = reduced_rules.get().fsts().save();
        let violated_rules = unsatisfied_rule_inds
            .get()
            .set_minus(reduced_rule_inds.get())
            .collect();
        let violated_output = context.output(violated_rules.get());
        context.set_interrupt(violated_rules.get(), signal::VIOLATED_RULE);
        let rule_sizes = reduced_rule_inds.get().counts();
        let unit_rules = rule_sizes.filter(|&(_, c)| c == 1).fsts();
        let unit_literal_causes = reduced_rules
            .get()
            .semijoin(unit_rules)
            .cartesian_product(level)
            .map(|((ri, l), lvl)| (l, LiteralCause::Propogated(ri, lvl)))
            .collect();
        let unit_literals = unit_literal_causes.get().fsts().save();
        let conflict_literals = unit_literals
            .get()
            .intersection(unit_literals.get().map(Not::not))
            .collect();
        let conflict_output = context.output(conflict_literals.get());
        context.set_interrupt(conflict_literals.get(), signal::ASSIGNMENT_CONFLICT);
        let assign_output = context.output(unit_literal_causes.get());
        context.set_feedback(unit_literal_causes.get().fsts(), assign_input.clone());
        Self {
            rules_input,
            level_input,
            assign_input,
            assign_output,
            violated_output,
            conflict_output,
            decision_literals: HashMap::new(),
        }
    }
}
