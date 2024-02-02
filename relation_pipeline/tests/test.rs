//! Tests written almost entirely by ChatGPT

use std::collections::{HashMap, HashSet};

use relation_pipeline::CreationContext;

#[test]
fn simple_commit_and_retrieve_test() {
    // Step 1: Create a new context
    let context = CreationContext::new();

    // Step 2: Create a new input and immediately create a distinct relation from it
    let (input, relation) = context.new_input::<i32>();
    let mut distinct_relation = context.output(relation.distinct());

    let mut context = context.begin();

    // Step 3: Send some data to the input, including some repeated values
    input.update(1, 1);
    input.update(2, 2);
    input.update(3, 1);
    input.update(2, -1); // This should cancel one of the previous +2 count for the number 2

    // Step 4: Commit the changes
    context.commit();

    // Step 5: Retrieve and validate the data from the distinct relation
    let mut result = Vec::new();
    distinct_relation.for_each(|num, count| {
        result.push((num, count));
    });

    // Step 6: Assert that we receive each number once, and with the correct count
    assert_eq!(result, vec![(1, 1), (2, 1), (3, 1)]);
}

#[test]
fn test_concat() {
    let context = CreationContext::new();

    let (input1, relation1) = context.new_input::<i32>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut concat_relation = context.output(relation1.concat(relation2));

    let mut context = context.begin();

    input1.update(1, 1);
    input2.update(2, 1);

    context.commit();

    let mut result = Vec::new();
    concat_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 1), (2, 1)]);
}

#[test]
fn test_consolidate() {
    let context = CreationContext::new();

    let (input, relation) = context.new_input::<i32>();
    let mut consolidated_relation = context.output(relation.consolidate());

    let mut context = context.begin();

    input.update(1, 1);
    input.update(1, 1);

    context.commit();

    let mut result = Vec::new();
    consolidated_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 2)]);
}

#[test]
fn test_flat_map() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<i32>();
    let mut flat_map_relation = context.output(relation.flat_map(|x| vec![x, x * 2]));

    let mut context = context.begin();

    input.update(1, 1);
    input.update(2, 1);

    context.commit();

    let mut result = Vec::new();
    flat_map_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 1), (2, 1), (2, 1), (4, 1)]);
}

#[test]
fn test_global_max() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<i32>();
    let mut max_relation = context.output(relation.global_max().consolidate());

    let mut context = context.begin();

    input.update(1, 1);
    input.update(2, 1);
    input.update(3, 1);

    context.commit();

    let mut result = Vec::new();
    max_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(3, 1)]);
}

#[test]
fn test_global_min() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<i32>();
    let mut min_relation = context.output(relation.global_min());

    let mut context = context.begin();

    input.update(1, 1);
    input.update(2, 1);
    input.update(3, 1);

    context.commit();

    let mut result = Vec::new();
    min_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 1)]);
}

#[test]
fn test_intersection() {
    let context = CreationContext::new();
    let (input1, relation1) = context.new_input::<i32>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut intersection_relation = context.output(relation1.intersection(relation2));

    let mut context = context.begin();

    input1.update(1, 1);
    input1.update(2, 1);
    input2.update(2, 1);
    input2.update(3, 1);

    context.commit();

    let mut result = Vec::new();
    intersection_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(2, 1)]);
}

#[test]
fn test_map() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<i32>();
    let mut map_relation = context.output(relation.map(|x| x * 2));

    let mut context = context.begin();

    input.update(1, 1);
    input.update(2, 1);

    context.commit();

    let mut result = Vec::new();
    map_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(2, 1), (4, 1)]);
}

#[test]
fn test_map_h() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<i32>();
    let mut map_h_relation = context.output(relation.map_h(|x| x * 2));

    let mut context = context.begin();

    input.update(1, 1);
    input.update(2, 1);

    context.commit();

    let mut result = Vec::new();
    map_h_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(2, 1), (4, 1)]);
}

#[test]
fn test_set_minus() {
    let context = CreationContext::new();
    let (input1, relation1) = context.new_input::<i32>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut set_minus_relation = context.output(relation1.set_minus(relation2));

    let mut context = context.begin();

    input1.update(1, 1);
    input1.update(2, 1);
    input2.update(2, 1);
    input2.update(3, 1);

    context.commit();

    let mut result = Vec::new();
    set_minus_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 1)]);
}

#[test]
fn test_save_and_get() {
    let context = CreationContext::new();

    // Step 1: Create a new input and a relation
    let (input, relation) = context.new_input::<i32>();
    let relation = relation.save();

    // Step 2: Create a saved snapshot of a relation derived from the initial relation
    let saved_relation = relation.get_().distinct().consolidate().save();

    // Step 3: Create two new relations deriving from the saved relation
    let mut distinct_relation = context.output(saved_relation.get_().distinct());
    let mut concat_relation = context.output(saved_relation.get_().concat(relation.get_()));

    let mut context = context.begin();

    // Step 4: Send data to the input
    input.update(1, 1);
    input.update(2, 2);
    input.update(3, 1);
    input.update(2, -1); // This will cancel one of the previous updates for the number 2

    // Step 5: Commit the changes
    context.commit();

    // Step 6: Retrieve data from the derived relations and validate it
    let mut distinct_result = HashMap::new();
    distinct_relation.dump_to_map(&mut distinct_result);
    assert_eq!(
        distinct_result,
        HashMap::from_iter([(1, 1), (2, 1), (3, 1)])
    );

    let mut concat_result = HashMap::new();
    concat_relation.dump_to_map(&mut concat_result);
    assert_eq!(concat_result, HashMap::from_iter([(1, 2), (2, 2), (3, 2)]));
}

#[test]
fn test_antijoin() {
    let context = CreationContext::new();
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut antijoin_relation = context.output(relation1.antijoin(relation2));

    let mut context = context.begin();

    input1.update((1, 1), 1);
    input1.update((2, 2), 1);
    input2.update(2, 1);

    context.commit();

    let mut result = Vec::new();
    antijoin_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![((1, 1), 1)]);
}

#[test]
fn test_join() {
    let context = CreationContext::new();
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<(i32, i32)>();
    let mut join_relation = context.output(relation1.join(relation2));

    let mut context = context.begin();

    input1.update((1, 2), 1);
    input1.update((2, 3), 1);
    input2.update((1, 4), 1);
    input2.update((2, 5), 1);

    context.commit();

    let mut result = Vec::new();
    join_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![((1, (2, 4)), 1), ((2, (3, 5)), 1)]);
}

#[test]
fn test_semijoin() {
    let context = CreationContext::new();
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut semijoin_relation = context.output(relation1.semijoin(relation2));

    let mut context = context.begin();

    input1.update((1, 1), 1);
    input1.update((2, 2), 1);
    input2.update(1, 1);

    context.commit();

    let mut result = Vec::new();
    semijoin_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![((1, 1), 1)]);
}

#[test]
fn test_fsts() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut fsts_relation = context.output(relation.fsts());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((3, 4), 1);

    context.commit();

    let mut result = Vec::new();
    fsts_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(1, 1), (3, 1)]);
}

#[test]
fn test_snds() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut snds_relation = context.output(relation.snds());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((3, 4), 1);

    context.commit();

    let mut result = Vec::new();
    snds_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![(2, 1), (4, 1)]);
}

#[test]
fn test_maxes() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut maxes_relation = context.output(relation.maxes().consolidate());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((1, 3), 1);
    input.update((2, 1), 1);

    context.commit();

    let mut result = HashSet::new();
    maxes_relation.for_each(|num, count| {
        result.insert((num, count));
    });

    assert_eq!(result, HashSet::from_iter([((1, 3), 1), ((2, 1), 1)]));
}

#[test]
fn test_mins() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut mins_relation = context.output(relation.mins().consolidate());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((1, 1), 1);
    input.update((2, 3), 1);

    context.commit();

    let mut result = HashSet::new();
    mins_relation.for_each(|num, count| {
        result.insert((num, count));
    });

    assert_eq!(result, HashSet::from_iter([((1, 1), 1), ((2, 3), 1)]));
}

#[test]
fn test_split() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let (fsts_relation, snds_relation) = relation.split();

    let mut fsts_relation = context.output(fsts_relation);
    let mut snds_relation = context.output(snds_relation);

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((3, 4), 1);

    context.commit();

    let mut result_fsts = Vec::new();
    fsts_relation.for_each(|num, count| {
        result_fsts.push((num, count));
    });

    let mut result_snds = Vec::new();
    snds_relation.for_each(|num, count| {
        result_snds.push((num, count));
    });

    assert_eq!(result_fsts, vec![(1, 1), (3, 1)]);
    assert_eq!(result_snds, vec![(2, 1), (4, 1)]);
}

#[test]
fn test_swaps() {
    let context = CreationContext::new();
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut swaps_relation = context.output(relation.swaps());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((3, 4), 1);

    context.commit();

    let mut result = Vec::new();
    swaps_relation.for_each(|num, count| {
        result.push((num, count));
    });

    assert_eq!(result, vec![((2, 1), 1), ((4, 3), 1)]);
}

#[test]
fn test_join_differential() {
    let context = CreationContext::new();

    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<(i32, i32)>();
    let mut join_relation = context.output(relation1.join(relation2).consolidate());

    let mut context = context.begin();

    input1.update((1, 2), 1);
    input2.update((1, 3), 1);

    context.commit();

    let mut result = HashMap::new();
    join_relation.dump_to_map(&mut result);

    assert_eq!(result, HashMap::from_iter([((1, (2, 3)), 1)]));

    input1.update((1, 2), -1);
    context.commit();

    join_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::new());
}

#[test]
fn test_maxes_differential() {
    let context = CreationContext::new();

    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut maxes_relation = context.output(relation.maxes().consolidate());

    let mut context = context.begin();

    input.update((1, 2), 1);
    input.update((1, 3), 1);
    input.update((2, 1), 1);

    context.commit();

    let mut result = HashMap::new();
    maxes_relation.dump_to_map(&mut result);

    assert_eq!(result, HashMap::from_iter([((1, 3), 1), ((2, 1), 1)]));

    input.update((1, 3), -1);
    context.commit();

    maxes_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, 2), 1), ((2, 1), 1)]));
}

#[test]
fn test_antijoin_differential() {
    let context = CreationContext::new();

    // Step 1: Create an input and an associated antijoin relation
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut antijoin_relation = context.output(relation1.antijoin(relation2).consolidate());

    let mut context = context.begin();

    // Step 2: Send initial data to the inputs
    input1.update((1, 2), 1);
    input1.update((1, 3), 1);
    input1.update((2, 1), 1);

    input2.update(1, 1);

    // Step 3: Commit the changes and dump the result to a map
    context.commit();

    let mut result = HashMap::new();
    antijoin_relation.dump_to_map(&mut result);

    // Step 4: Validate the initial state of the antijoin relation
    assert_eq!(result, HashMap::from_iter([((2, 1), 1)]));

    // Step 5: Perform an update removing an entry from input2, potentially affecting the antijoin relation
    input2.update(1, -1);

    // Step 6: Commit the changes and dump the updated state of the antijoin relation to the map
    context.commit();
    antijoin_relation.dump_to_map(&mut result);

    // Step 7: Validate the updated state of the antijoin relation
    assert_eq!(
        result,
        HashMap::from_iter([((1, 2), 1), ((1, 3), 1), ((2, 1), 1)])
    );
}

#[test]
fn test_other_differential_join() {
    let context = CreationContext::new();

    // Step 1: Create two new inputs and a join relation
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<(i32, i32)>();
    let mut join_relation = context.output(relation1.join(relation2));

    let mut context = context.begin();

    // Step 2: Send data to the inputs
    input1.update((1, 1), 1);
    input1.update((2, 2), 1);
    input2.update((1, 3), 1);

    // Step 3: Commit the changes
    context.commit();

    // Step 4: Retrieve data from the join relation and validate it
    let mut result = HashMap::new();
    join_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, (1, 3)), 1)]));

    // Step 5: Send more data to the inputs
    input1.update((1, 1), -1);
    input2.update((1, 3), -1);
    input2.update((2, 4), 1);

    // Step 6: Commit the changes
    context.commit();

    // Step 7: Retrieve data from the join relation and validate it
    join_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((2, (2, 4)), 1)]));
}

#[test]
fn test_other_differential_antijoin() {
    let context = CreationContext::new();

    // Step 1: Create two new inputs and an antijoin relation
    let (input1, relation1) = context.new_input::<(i32, i32)>();
    let (input2, relation2) = context.new_input::<i32>();
    let mut antijoin_relation = context.output(relation1.antijoin(relation2));

    let mut context = context.begin();

    // Step 2: Send data to the inputs
    input1.update((1, 1), 1);
    input1.update((2, 2), 1);
    input2.update(1, 1);

    // Step 3: Commit the changes
    context.commit();

    // Step 4: Retrieve data from the antijoin relation and validate it
    let mut result = HashMap::new();
    antijoin_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((2, 2), 1)]));

    // Step 5: Send more data to the inputs
    input1.update((2, 2), -1);
    input2.update(1, -1);
    input2.update(2, 1);

    // Step 6: Commit the changes
    context.commit();

    // Step 7: Retrieve data from the antijoin relation and validate it
    antijoin_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, 1), 1)]));
}

#[test]
fn test_maxes_with_high_volume() {
    let context = CreationContext::new();

    // Step 1: Create a new input and a maxes relation
    let (input, relation) = context.new_input::<(i32, i32)>();
    let mut maxes_relation = context.output(relation.maxes().consolidate());

    let mut context = context.begin();

    // Step 2: Send a lot of data to the input, including a large number of updates to a single key
    for i in 0..1000 {
        input.update((1, i), 1);
    }

    // Step 3: Commit the changes
    context.commit();

    // Step 4: Retrieve data from the maxes relation and validate it
    let mut result = HashMap::new();
    maxes_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, 999), 1)]));

    // Step 5: Remove the existing maximum value
    input.update((1, 999), -1);

    // Step 6: Commit the changes
    context.commit();

    // Step 7: Retrieve data from the maxes relation and validate it
    maxes_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, 998), 1)]));

    // Step 8: Now remove all values and add a new value which should become the new max
    for i in 0..999 {
        input.update((1, i), -1);
    }
    input.update((1, 1000), 1);

    // Step 9: Commit the changes
    context.commit();

    // Step 10: Retrieve data from the maxes relation and validate it
    maxes_relation.dump_to_map(&mut result);
    assert_eq!(result, HashMap::from_iter([((1, 1000), 1)]));
}
