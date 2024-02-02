use std::{fmt::Debug, hash::Hash};

use loopy_relations::CreationContext;

fn dijkstra<Node: Debug + Ord + Hash + Clone + 'static>(
    start: Node,
    end: Node,
    edge_weights: impl IntoIterator<Item = (Node, Node, usize)>,
) -> Option<usize> {
    let mut context = CreationContext::new();

    let (start_input, start_rel) = context.new_input::<Node>();
    let (end_input, end_rel) = context.new_input::<Node>();
    let (edges_input, edges_rel) = context.new_input::<(Node, Node, usize)>();

    let (distances_input, distances) = context.new_input::<(Node, usize)>();
    let distances = distances.save();
    context.set_feedback(start_rel.map(|n| (n, 0)), distances_input.clone());

    let distance_to_end = distances.get().semijoin(end_rel).snds().save();
    let mut end_distance_output = context.output(distance_to_end.get());
    context.set_interrupt(distance_to_end.get(), 0);

    let next_distances = distances
        .get()
        .join_values(edges_rel.map(|(from, to, dist)| (from, (to, dist))))
        .map(|(prev_dist, (to, edge_dist))| (to, prev_dist + edge_dist))
        .antijoin(distances.get().fsts())
        .collect();

    let selection_distance = next_distances.get().snds().consolidate().global_min();

    let selected_next_distances = next_distances
        .get()
        .swaps()
        .semijoin(selection_distance)
        .swaps();

    context.set_feedback(selected_next_distances, distances_input);

    let mut context = context.begin();

    start_input.insert(start);
    end_input.insert(end);
    for edge in edge_weights {
        edges_input.insert(edge);
    }

    match context.commit()? {
        0 => {
            let mut iter = end_distance_output.iter();
            let &k = iter.next().unwrap();
            assert!(iter.next().is_none());
            Some(k)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_dijkstra() {
    let dist = dijkstra(
        'A',
        'F',
        vec![
            ('A', 'B', 1),
            ('A', 'C', 2),
            ('A', 'F', 7),
            ('B', 'D', 2),
            ('C', 'E', 3),
            ('D', 'A', 1),
            ('D', 'E', 1),
            ('E', 'A', 1),
            ('E', 'F', 1),
        ],
    );

    assert_eq!(dist, Some(5));
}
