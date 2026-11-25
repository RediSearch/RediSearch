/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Naming a terminal by its owned path, one tag value per level: what a garbage collector lists,
//! to find each terminal again later.

use std::ops::ControlFlow;

use compound_index::Constraint;

use crate::util::{CountsChecks, Index, orders, post, tag, tag_level};

fn tag_step(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

/// How many nodes a walk leaving both levels open enters: the walk checks the timeout once per
/// node entered.
fn nodes_entered(index: &Index) -> usize {
    let mut checks = CountsChecks(0);
    index
        .collect_terminals(&[Constraint::Any, Constraint::Any], usize::MAX, &mut checks)
        .unwrap();
    checks.0
}

#[test]
fn every_terminal_is_listed_in_order_with_its_path() {
    let index = orders();
    let mut listed = Vec::new();
    index.for_each_terminal(|path, docs| listed.push((path.to_vec(), docs.clone())));
    assert_eq!(
        listed,
        [
            (vec![tag_step("blue"), tag_step("L")], vec![1]),
            (vec![tag_step("blue"), tag_step("M")], vec![4]),
            (vec![tag_step("red"), tag_step("L")], vec![2]),
            (vec![tag_step("red"), tag_step("M")], vec![1, 3, 3]),
        ]
    );
}

#[test]
fn a_listed_path_finds_its_terminal_again() {
    let mut index = orders();
    let mut paths = Vec::new();
    index.for_each_terminal(|path, _| paths.push(path.to_vec()));
    assert_eq!(paths[1], [tag_step("blue"), tag_step("M")]);
    assert_eq!(index.terminal_at(&paths[1]), Some(&vec![4]));
    index.terminal_at_mut(&paths[1]).unwrap().push(5);
    assert_eq!(index.terminal_at(&paths[1]), Some(&vec![4, 5]));
    // Borrowed values find it too.
    assert_eq!(
        index.terminal_at(&[tag("blue"), tag("M")]),
        Some(&vec![4, 5])
    );
}

#[test]
fn a_path_that_does_not_lead_to_a_terminal_finds_nothing() {
    let mut index = orders();
    for path in [
        vec![tag_step("green"), tag_step("M")],
        // Too short or too long.
        vec![tag_step("red")],
        vec![tag_step("red"), tag_step("M"), tag_step("x")],
    ] {
        assert_eq!(index.terminal_at(&path), None, "{path:?}");
        assert_eq!(index.terminal_at_mut(&path), None, "{path:?}");
    }
}

#[test]
fn removing_a_terminal_drops_the_nodes_it_leaves_empty() {
    let mut index = orders();
    let blue_l = vec![tag_step("blue"), tag_step("L")];
    let blue_m = vec![tag_step("blue"), tag_step("M")];
    // Kept when asked, or when nothing is at the path.
    assert!(!index.remove_terminal_if(&blue_l, |_| false));
    assert!(!index.remove_terminal_if(&[tag_step("green"), tag_step("L")], |_| true));
    assert!(!index.remove_terminal_if(&[tag_step("blue")], |_| true));
    assert_eq!(index.num_terminals(), 4);

    assert!(index.remove_terminal_if(&blue_l, |docs| docs == &vec![1]));
    assert_eq!(index.num_terminals(), 3);
    assert!(
        index.terminal_at(&blue_m).is_some(),
        "blue keeps its other child"
    );
    assert!(index.remove_terminal_if(&blue_m, |_| true));
    let mut colors = Vec::new();
    index.for_each_terminal(|path, _| colors.push(path[0].clone()));
    assert!(
        colors.iter().all(|color| *color == tag_step("red")),
        "blue went with its last child"
    );
    assert_eq!(index.num_terminals(), 2);
}

#[test]
fn removing_a_terminal_leaves_no_node_a_walk_would_enter() {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    post(&mut index, &[tag("red"), tag("x")], 1);
    post(&mut index, &[tag("blue"), tag("x")], 2);

    assert!(index.remove_terminal_if(&[tag_step("red"), tag_step("x")], |_| true));
    assert_eq!(index.num_terminals(), 1);
    assert_eq!(index.get(&[tag("red"), tag("x")]), None);
    // A `red` node left without children would still be entered by a walk; it is gone only if
    // the walk costs what it costs on an index that never had that path.
    let mut fresh = Index::new(vec![tag_level(), tag_level()]).unwrap();
    post(&mut fresh, &[tag("blue"), tag("x")], 2);
    assert_eq!(nodes_entered(&index), nodes_entered(&fresh));

    // The removed value takes a new path again.
    post(&mut index, &[tag("red"), tag("y")], 3);
    assert_eq!(index.get(&[tag("red"), tag("y")]), Some(&vec![3]));
    assert_eq!(index.num_terminals(), 2);
}

#[test]
fn a_walk_resumes_after_the_last_path_it_saw() {
    let index = orders();
    let mut all = Vec::new();
    index.for_each_terminal(|path, _| all.push(path.to_vec()));
    // Resuming after each path finds the next one, and nothing after the last.
    let next = |after: Option<&[Vec<u8>]>| {
        let mut next = None;
        index.for_each_terminal_after(after, |path, _| {
            next = Some(path.to_vec());
            ControlFlow::Break(())
        });
        next
    };
    assert_eq!(next(None).as_ref(), all.first());
    for pair in all.windows(2) {
        assert_eq!(next(Some(&pair[0])).as_ref(), Some(&pair[1]));
    }
    assert_eq!(next(all.last().map(Vec::as_slice)), None);
    // A path that names no terminal resumes at the first one after it.
    assert_eq!(
        next(Some(&[tag_step("blue"), tag_step("Lz")])),
        Some(vec![tag_step("blue"), tag_step("M")])
    );
    assert_eq!(
        next(Some(&[tag_step("green")])),
        Some(vec![tag_step("red"), tag_step("L")])
    );
}
