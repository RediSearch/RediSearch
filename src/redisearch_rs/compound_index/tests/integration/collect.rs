/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{CollectError, Constraint};
use timeout::NoTimeoutChecker;

use crate::util::{TimesOutAfter, orders, tag_in, tag_not_in, union};

/// Every shape a query can take on the [`orders`] index, with the docs that have one element
/// satisfying both constraints.
#[test]
fn every_query_shape_returns_the_docs_with_one_matching_element() {
    let index = orders();
    let cases: &[(&str, [Constraint<'_>; 2], &[u32])] = &[
        (
            "both levels fixed",
            [tag_in(&["red"]), tag_in(&["M"])],
            &[1, 3],
        ),
        (
            "a set on the first level",
            [tag_in(&["red", "blue"]), tag_in(&["M"])],
            &[1, 3, 4],
        ),
        (
            "first level skipped",
            [Constraint::Any, tag_in(&["L"])],
            &[1, 2],
        ),
        (
            "second level skipped",
            [tag_in(&["blue"]), Constraint::Any],
            &[1, 4],
        ),
        ("negation", [tag_not_in(&["red"]), tag_in(&["M"])], &[4]),
        (
            "prefix",
            [Constraint::TagPrefix(b"bl"), Constraint::Any],
            &[1, 4],
        ),
        (
            "no matching value",
            [tag_in(&["green"]), Constraint::Any],
            &[],
        ),
        (
            "everything",
            [Constraint::Any, Constraint::Any],
            &[1, 2, 3, 4],
        ),
    ];
    for (name, constraints, expected) in cases {
        let terminals = index
            .collect_terminals(constraints, usize::MAX, &mut NoTimeoutChecker)
            .unwrap();
        assert_eq!(union(&terminals), *expected, "{name}");
    }
}

#[test]
fn constraints_never_pair_values_from_different_elements() {
    let index = orders();
    // Doc 1 has a blue item and an M item, but no blue M item.
    let terminals = index
        .collect_terminals(
            &[tag_in(&["blue"]), tag_in(&["M"])],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    assert_eq!(union(&terminals), vec![4]);
}

#[test]
fn repeated_values_in_a_set_visit_a_terminal_once() {
    let index = orders();
    let terminals = index
        .collect_terminals(
            &[tag_in(&["red", "red"]), tag_in(&["M"])],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    assert_eq!(terminals.len(), 1);
}

#[test]
fn terminals_come_out_in_byte_order_of_their_path() {
    let index = orders();
    let terminals = index
        .collect_terminals(
            &[Constraint::Any, Constraint::Any],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    // (blue, L), (blue, M), (red, L), (red, M).
    assert_eq!(
        terminals,
        vec![&vec![1], &vec![4], &vec![2], &vec![1, 3, 3]]
    );
}

#[test]
fn a_walk_past_the_terminal_limit_fails_instead_of_truncating() {
    let index = orders();
    let all = [Constraint::Any, Constraint::Any];
    assert_eq!(
        index
            .collect_terminals(&all, 3, &mut NoTimeoutChecker)
            .err(),
        Some(CollectError::TooManyTerminals { max: 3 })
    );
    assert_eq!(
        index
            .collect_terminals(&all, 4, &mut NoTimeoutChecker)
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn a_zero_terminal_limit_only_accepts_walks_that_reach_nothing() {
    let index = orders();
    assert_eq!(
        index
            .collect_terminals(
                &[tag_in(&["green"]), Constraint::Any],
                0,
                &mut NoTimeoutChecker
            )
            .unwrap(),
        Vec::<&Vec<u32>>::new()
    );
    assert_eq!(
        index
            .collect_terminals(
                &[tag_in(&["red"]), Constraint::Any],
                0,
                &mut NoTimeoutChecker
            )
            .err(),
        Some(CollectError::TooManyTerminals { max: 0 })
    );
}

#[test]
fn the_timeout_bounds_a_walk_that_returns_nothing() {
    let index = orders();
    // Both colors are entered, then neither has a size "XL": no terminal, four checks, one per
    // color and one per lookup of XL.
    let open_then_missing = [Constraint::Any, tag_in(&["XL"])];
    assert_eq!(
        index
            .collect_terminals(&open_then_missing, usize::MAX, &mut TimesOutAfter(3))
            .err(),
        Some(CollectError::TimedOut)
    );
    assert!(
        index
            .collect_terminals(&open_then_missing, usize::MAX, &mut TimesOutAfter(4))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn the_timeout_is_checked_for_every_constraint_kind() {
    let index = orders();
    // Each case considers exactly `nodes` children, terminals included, so it checks the
    // timeout `nodes` times.
    let cases: &[(&str, [Constraint<'_>; 2], usize)] = &[
        // blue, then (blue, L) and (blue, M); red is skipped, which also counts.
        ("negation", [tag_not_in(&["red"]), Constraint::Any], 4),
        // red, then (red, L) and (red, M).
        ("prefix", [Constraint::TagPrefix(b"re"), Constraint::Any], 3),
    ];
    for (name, constraints, nodes) in cases {
        assert_eq!(
            index
                .collect_terminals(constraints, usize::MAX, &mut TimesOutAfter(nodes - 1))
                .err(),
            Some(CollectError::TimedOut),
            "{name}"
        );
        assert!(
            index
                .collect_terminals(constraints, usize::MAX, &mut TimesOutAfter(*nodes))
                .is_ok(),
            "{name}"
        );
    }
}

#[test]
fn the_timeout_is_checked_on_values_that_enter_nothing() {
    let index = orders();
    // Three colors the index does not have, and every color it has excluded: the walk enters
    // no node, and still checks once per value looked up or child skipped.
    let cases: &[(&str, [Constraint<'_>; 2], usize)] = &[
        (
            "absent values",
            [tag_in(&["green", "pink", "white"]), Constraint::Any],
            3,
        ),
        (
            "every value excluded",
            [tag_not_in(&["blue", "red"]), Constraint::Any],
            2,
        ),
    ];
    for (name, constraints, checks) in cases {
        assert_eq!(
            index
                .collect_terminals(constraints, usize::MAX, &mut TimesOutAfter(checks - 1))
                .err(),
            Some(CollectError::TimedOut),
            "{name}"
        );
        assert!(
            index
                .collect_terminals(constraints, usize::MAX, &mut TimesOutAfter(*checks))
                .unwrap()
                .is_empty(),
            "{name}"
        );
    }
}

#[test]
fn the_timeout_is_checked_on_terminals() {
    let index = orders();
    // (red) then (red, M): two nodes.
    let red_m = [tag_in(&["red"]), tag_in(&["M"])];
    assert_eq!(
        index
            .collect_terminals(&red_m, usize::MAX, &mut TimesOutAfter(1))
            .err(),
        Some(CollectError::TimedOut)
    );
    assert_eq!(
        index
            .collect_terminals(&red_m, usize::MAX, &mut TimesOutAfter(2))
            .unwrap()
            .len(),
        1
    );
}

#[test]
#[should_panic(expected = "a walk takes one constraint per level")]
fn a_walk_with_the_wrong_number_of_constraints_panics() {
    let _ = orders().collect_terminals(&[Constraint::Any], usize::MAX, &mut NoTimeoutChecker);
}
