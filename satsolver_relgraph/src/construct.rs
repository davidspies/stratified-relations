use std::ops::Not;

use either::Either::{Left, Right};
use loopy_relations::CreationContext;
use sat::{Atom, Level, Literal, LiteralCause, RuleIndex, Sign};

use crate::{signal, RelGraph};

impl RelGraph {
    pub fn construct(context: &mut CreationContext) -> Self {
        let (rules_input, base_rules) = context.new_frameless_input::<(RuleIndex, Literal)>();
        let base_rules = base_rules.save();

        let (equivalence_input, base_equivalences) =
            context.new_frameless_input::<(Atom, Literal)>();
        let base_equivalences = base_equivalences.flat_map(|(x, y)| [(x.pos(), y), (x.neg(), !y)]);
        let (equivalence_closure_input, equivalence_closure) =
            context.new_frameless_input::<(Literal, Literal)>();
        context.set_feedback(base_equivalences, equivalence_closure_input.clone());
        let equivalence_closure = equivalence_closure.mins().collect();
        let next_equivalences = equivalence_closure
            .get()
            .swaps()
            .join_values(equivalence_closure.get())
            .dynamic();
        context.set_feedback(next_equivalences, equivalence_closure_input);

        let used_literals = base_rules
            .get()
            .snds()
            .map(|x| x.atom())
            .distinct()
            .flat_map(|x| [x.pos(), x.neg()])
            .dynamic()
            .set_minus(equivalence_closure.get().fsts())
            .collect();
        let equivalences = equivalence_closure
            .get()
            .concat(used_literals.get().map(|x| (x, x)))
            .collect();
        let rules1 = base_rules
            .get()
            .swaps()
            .join_values(equivalences.get())
            .dynamic()
            .distinct()
            .collect();
        let noop_rules = rules1
            .get()
            .intersection(rules1.get().map(|(ri, x)| (ri, !x)))
            .fsts()
            .dynamic();
        let rules1 = rules1.get().antijoin(noop_rules).collect();

        let (singleton_input, singletons) = context.new_frameless_input::<Literal>();
        let singletons = singletons.intersection(used_literals.get()).collect();
        let satisfied_rules2 = rules1
            .get()
            .swaps()
            .semijoin(singletons.get())
            .snds()
            .dynamic();
        let unsatisfied_rules2 = rules1.get().antijoin(satisfied_rules2).collect();
        let rules2_inds = unsatisfied_rules2.get().fsts().distinct().dynamic();
        let rules2 = unsatisfied_rules2
            .get()
            .swaps()
            .antijoin(singletons.get().map(Not::not))
            .swaps()
            .collect();
        context.set_interrupt(
            rules2_inds.set_minus(rules2.get().fsts()),
            signal::ROOT_CONFLICT,
        );
        let rules2_counts = rules2.get().fsts().counts().collect();
        let singleton_inds = rules2_counts
            .get()
            .filter(|&(_, count)| count == 1)
            .fsts()
            .dynamic();
        let new_singletons = rules2.get().semijoin(singleton_inds).snds().collect();
        context.set_interrupt(
            new_singletons
                .get()
                .intersection(new_singletons.get().map(Not::not)),
            signal::ROOT_CONFLICT,
        );
        context.set_feedback(new_singletons.get(), singleton_input.clone());

        let eliminated = singletons
            .get()
            .concat(singletons.get().map(Not::not))
            .dynamic();
        let used_literals = used_literals.get().set_minus(eliminated).collect();

        let binary_inds = rules2_counts.get().filter(|&(_, count)| count == 2).fsts();
        let base_implication = rules2
            .get()
            .semijoin(binary_inds)
            .dynamic()
            .top_ns::<2>()
            .consolidate()
            .flat_map(|(_, v)| [(!v[0], v[1]), (!v[1], v[0])])
            .dynamic();
        let (implication_input, implication) = context.new_frameless_input::<(Literal, Literal)>();
        context.set_feedback(base_implication, implication_input.clone());
        let implication = implication
            .semijoin(used_literals.get())
            .swaps()
            .dynamic()
            .semijoin(used_literals.get())
            .swaps()
            .collect();

        let discovered_singletons = implication.get().filter(|&(x, y)| x == !y).snds().collect();
        let discovered_singleton_output1 =
            context.output(discovered_singletons.get().global_min().dynamic());
        context.set_interrupt(discovered_singletons.get(), signal::SINGLETON_DISCOVERED_1);

        let discovered_equivalences = implication
            .get()
            .intersection(implication.get().swaps())
            .map(|(x, y)| {
                let (x, y) = (x.min(y), x.max(y));
                let (atom, sign) = y.atom_and_sign();
                match sign {
                    Sign::Pos => (atom, x),
                    Sign::Neg => (atom, !x),
                }
            })
            .collect();
        let discovered_equivalence_output =
            context.output(discovered_equivalences.get().global_min().dynamic());
        context.set_interrupt(
            discovered_equivalences.get(),
            signal::EQUIVALENCE_DISCOVERED,
        );

        context.set_feedback(
            implication.get().swaps().join_values(implication.get()),
            implication_input,
        );

        let implication_with_self = implication
            .get()
            .concat(used_literals.get().map(|x| (x, x)))
            .collect();

        let multiary_inds = rules2_counts.get().filter(|&(_, count)| count > 2).fsts();
        let impl_candidates = rules2
            .get()
            .semijoin(multiary_inds)
            .dynamic()
            .random_ns::<2>(327423983)
            .consolidate()
            .flat_map(|(ri, v)| [(v[0], ri), (v[1], ri)])
            .join_values(implication_with_self.get())
            .dynamic()
            .distinct()
            .dynamic();
        let impl_counts = rules2
            .get()
            .join(impl_candidates)
            .swaps()
            .dynamic()
            .semijoin(implication_with_self.get())
            .map(|((_, y), ri)| (ri, y))
            .dynamic()
            .counts()
            .map(|((ri, y), count)| (ri, (y, count)))
            .dynamic();
        let (discovered_singletons, rule_implication) = impl_counts
            .join(rules2_counts.get())
            .consolidate()
            .filter(|&(_, ((_, count), total))| {
                assert!(count <= total);
                count + 1 >= total
            })
            .map(|(ri, ((y, count), total))| {
                if count == total {
                    Left(y)
                } else {
                    Right((ri, y))
                }
            })
            .partition();
        let discovered_singletons = discovered_singletons.collect();
        let top_discovered_singletons = discovered_singletons
            .get()
            .set_minus(
                implication
                    .get()
                    .semijoin(discovered_singletons.get())
                    .snds()
                    .dynamic(),
            )
            .collect();

        let discovered_singleton_output2 =
            context.output(top_discovered_singletons.get().global_min().dynamic());
        context.set_interrupt(discovered_singletons.get(), signal::SINGLETON_DISCOVERED_2);

        let discovered_impl = rules2
            .get()
            .join_values(rule_implication.dynamic())
            .distinct()
            .dynamic()
            .set_minus(implication_with_self.get())
            .map(|(x, y)| (!x, y))
            .dynamic()
            .set_minus(implication_with_self.get())
            .collect();
        let innermost_discovered_impls = discovered_impl
            .get()
            .set_minus(discovered_impl.get().swaps().join_values(implication.get()))
            .dynamic()
            .set_minus(implication.get().swaps().join_values(discovered_impl.get()))
            .collect();
        let discovered_binary_output = context.output(
            innermost_discovered_impls
                .get()
                .map(|(x, y)| (!x, y))
                .global_min()
                .flat_map(|(x, y)| [x, y])
                .dynamic(),
        );
        context.set_interrupt(discovered_impl.get(), signal::BINARY_DISCOVERED);

        let (assigned_input, assigned_literal_causes) =
            context.new_first_occurrences_input::<Literal, LiteralCause>();
        let assigned_literal_causes = equivalences
            .get()
            .join_values(assigned_literal_causes)
            .collect();
        context.set_interrupt(
            assigned_literal_causes
                .get()
                .fsts()
                .intersection(singletons.get().map(Not::not)),
            signal::SELECTION_INVALIDATED,
        );
        let assigned_literal_causes = assigned_literal_causes
            .get()
            .semijoin(used_literals.get())
            .collect();

        let (level_input, level) = context.new_input::<Level>();
        let current_level = level.concat(context.constant([0])).global_max();

        let (inspect_assigned_input, inspect_assigned) = context.new_input::<Literal>();
        let inspect_assigned = inspect_assigned.save();
        let conflict_level = assigned_literal_causes
            .get()
            .semijoin(inspect_assigned.get())
            .snds()
            .map(LiteralCause::level)
            .global_max()
            .dynamic();
        let (propagate_cause, retained) = assigned_literal_causes
            .get()
            .semijoin(inspect_assigned.get())
            .cartesian_product(conflict_level)
            .consolidate()
            .map(|((lit, cause), conflict_level)| match cause {
                LiteralCause::Propogated(ri, level) => {
                    assert!(level <= conflict_level, "{:?} {:?}", level, conflict_level);
                    if level == conflict_level {
                        Left((ri, lit))
                    } else {
                        Right(lit)
                    }
                }
                LiteralCause::DecisionLiteral(_) => Right(lit),
            })
            .partition();
        let propagate_cause = propagate_cause.dynamic();
        let retained = retained.collect();
        let resolution_level_output = context.output(
            assigned_literal_causes
                .get()
                .semijoin(retained.get())
                .map(|(_lit, cause)| cause.level())
                .global_max()
                .dynamic(),
        );
        let next_step = propagate_cause
            .join_values(rules2.get())
            .flat_map(|(caused, causer)| (caused != causer).then_some(!causer))
            .dynamic();
        context.set_feedback(next_step, inspect_assigned_input.clone());

        let resolution_output = context.output(retained.get().map(Not::not).dynamic());

        let assigned_literals = assigned_literal_causes.get().fsts().save();
        let assign_conflict = assigned_literals
            .get()
            .intersection(assigned_literals.get().map(Not::not))
            .collect();

        let conflicted_atoms = assign_conflict
            .get()
            .map(Literal::atom)
            .distinct()
            .dynamic();
        let min_assign_conflict = conflicted_atoms
            .global_min()
            .flat_map(|atom| [atom.neg(), atom.pos()]);
        context.set_feedback(min_assign_conflict, inspect_assigned_input.clone());

        context.set_interrupt(assign_conflict.get(), signal::ASSIGNMENT_CONFLICT);

        let satisfied_rules = rules2
            .get()
            .swaps()
            .semijoin(assigned_literals.get())
            .snds()
            .dynamic();
        let unsatisfied_rules = rules2.get().antijoin(satisfied_rules).collect();
        let reduced_rules = unsatisfied_rules
            .get()
            .swaps()
            .antijoin(assigned_literals.get().map(Not::not))
            .swaps()
            .collect();

        let unsatisfied_rule_indices = unsatisfied_rules.get().fsts().distinct().dynamic();
        let violated_rules = unsatisfied_rule_indices
            .set_minus(reduced_rules.get().fsts())
            .collect();

        context.set_feedback(
            rules2
                .get()
                .semijoin(violated_rules.get().global_min().dynamic())
                .snds()
                .map(Not::not),
            inspect_assigned_input,
        );

        context.set_interrupt(violated_rules.get(), signal::VIOLATED_RULE);

        let rule_counts = reduced_rules.get().fsts().counts().dynamic();
        let singleton_rules = rule_counts.filter(|&(_, count)| count == 1).fsts();

        let propogated_literals = reduced_rules
            .get()
            .semijoin(singleton_rules)
            .dynamic()
            .cartesian_product(current_level)
            .map(|((ri, lit), level)| (lit, LiteralCause::Propogated(ri, level)))
            .dynamic();
        context.set_feedback(propogated_literals, assigned_input.clone());

        let next_literal = context.output(
            reduced_rules
                .get()
                .snds()
                .counts()
                .swaps()
                .dynamic()
                .global_max()
                .snds()
                .dynamic(),
        );

        let assigned_output =
            context.output(assigned_literals.get().concat(singletons.get()).dynamic());

        RelGraph {
            rules_input,
            assigned_input,
            level_input,
            next_literal,
            equivalence_input,
            resolution_output,
            resolution_level_output,
            assigned_output,
            discovered_singleton_output1,
            discovered_equivalence_output,
            discovered_singleton_output2,
            discovered_binary_output,
        }
    }
}
