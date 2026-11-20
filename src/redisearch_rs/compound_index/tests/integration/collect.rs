/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Bound::{self, Excluded, Included, Unbounded};

use compound_index::{BucketRange, CollectError, Constraint, Key, WalkLimits};

use crate::util::{Index, bucket_level, orders, post, tag, tag_in, tag_level, union};

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
        (
            "negation",
            [Constraint::TagNotIn(vec![b"red"]), tag_in(&["M"])],
            &[4],
        ),
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
            .collect_terminals(constraints, WalkLimits::UNLIMITED)
            .unwrap();
        assert_eq!(union(&terminals), *expected, "{name}");
    }
}

#[test]
fn constraints_never_pair_values_from_different_elements() {
    let index = orders();
    // Doc 1 has a blue item and an M item, but no blue M item.
    let terminals = index
        .collect_terminals(&[tag_in(&["blue"]), tag_in(&["M"])], WalkLimits::UNLIMITED)
        .unwrap();
    assert_eq!(union(&terminals), vec![4]);
}

#[test]
fn repeated_values_in_a_set_visit_a_terminal_once() {
    let index = orders();
    let terminals = index
        .collect_terminals(
            &[tag_in(&["red", "red"]), tag_in(&["M"])],
            WalkLimits::UNLIMITED,
        )
        .unwrap();
    assert_eq!(terminals.len(), 1);
}

#[test]
fn terminals_come_out_in_byte_order_of_their_path() {
    let index = orders();
    let terminals = index
        .collect_terminals(&[Constraint::Any, Constraint::Any], WalkLimits::UNLIMITED)
        .unwrap();
    // (blue, L), (blue, M), (red, L), (red, M).
    assert_eq!(
        terminals,
        vec![&vec![1], &vec![4], &vec![2], &vec![1, 3, 3]]
    );
}

const fn terminals_at_most(max_terminals: usize) -> WalkLimits {
    WalkLimits {
        max_terminals,
        max_nodes: usize::MAX,
    }
}

const fn nodes_at_most(max_nodes: usize) -> WalkLimits {
    WalkLimits {
        max_terminals: usize::MAX,
        max_nodes,
    }
}

#[test]
fn a_walk_past_the_terminal_limit_fails_instead_of_truncating() {
    let index = orders();
    let all = [Constraint::Any, Constraint::Any];
    assert_eq!(
        index.collect_terminals(&all, terminals_at_most(3)).err(),
        Some(CollectError::TooManyTerminals { max: 3 })
    );
    assert_eq!(
        index
            .collect_terminals(&all, terminals_at_most(4))
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
            .collect_terminals(&[tag_in(&["green"]), Constraint::Any], terminals_at_most(0))
            .unwrap(),
        Vec::<&Vec<u32>>::new()
    );
    assert_eq!(
        index
            .collect_terminals(&[tag_in(&["red"]), Constraint::Any], terminals_at_most(0))
            .err(),
        Some(CollectError::TooManyTerminals { max: 0 })
    );
}

#[test]
fn the_node_limit_bounds_a_walk_that_returns_nothing() {
    let index = orders();
    // Both colors are entered, then neither has a size "XL": no terminal, two nodes.
    let open_then_missing = [Constraint::Any, tag_in(&["XL"])];
    assert_eq!(
        index
            .collect_terminals(&open_then_missing, nodes_at_most(1))
            .err(),
        Some(CollectError::TooManyNodes { max: 1 })
    );
    assert!(
        index
            .collect_terminals(&open_then_missing, nodes_at_most(2))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn the_node_limit_applies_to_every_constraint_kind() {
    let index = orders();
    // Each case enters exactly `nodes` nodes, terminals included.
    let cases: &[(&str, [Constraint<'_>; 2], usize)] = &[
        // blue, then (blue, L) and (blue, M).
        (
            "negation",
            [Constraint::TagNotIn(vec![b"red"]), Constraint::Any],
            3,
        ),
        // red, then (red, L) and (red, M).
        ("prefix", [Constraint::TagPrefix(b"re"), Constraint::Any], 3),
    ];
    for (name, constraints, nodes) in cases {
        assert_eq!(
            index
                .collect_terminals(constraints, nodes_at_most(nodes - 1))
                .err(),
            Some(CollectError::TooManyNodes { max: nodes - 1 }),
            "{name}"
        );
        assert!(
            index
                .collect_terminals(constraints, nodes_at_most(*nodes))
                .is_ok(),
            "{name}"
        );
    }

    // Three bucket children, one terminal under each.
    let all_years = [Constraint::Any, Constraint::Any];
    assert_eq!(
        years()
            .collect_terminals(&all_years, nodes_at_most(5))
            .err(),
        Some(CollectError::TooManyNodes { max: 5 })
    );
    assert_eq!(
        years()
            .collect_terminals(&all_years, nodes_at_most(6))
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn the_node_limit_counts_terminals() {
    let index = orders();
    // (red) then (red, M): two nodes.
    let red_m = [tag_in(&["red"]), tag_in(&["M"])];
    assert_eq!(
        index.collect_terminals(&red_m, nodes_at_most(1)).err(),
        Some(CollectError::TooManyNodes { max: 1 })
    );
    assert_eq!(
        index
            .collect_terminals(&red_m, nodes_at_most(2))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn invalid_queries_are_rejected() {
    let index = years();
    let cases: &[(&str, &[Constraint<'_>], CollectError)] = &[
        (
            "too few constraints",
            &[Constraint::Any],
            CollectError::WrongLength {
                expected: 2,
                got: 1,
            },
        ),
        (
            "tag constraint on a bucket level",
            &[tag_in(&["2022"]), Constraint::Any],
            CollectError::ConstraintMismatch { level: 0 },
        ),
        (
            "bucket range on a tag level",
            &[Constraint::Any, range(Included(0.0), Excluded(1.0))],
            CollectError::ConstraintMismatch { level: 1 },
        ),
    ];
    for (name, constraints, expected) in cases {
        assert_eq!(
            index
                .collect_terminals(constraints, WalkLimits::UNLIMITED)
                .err(),
            Some(expected.clone()),
            "{name}"
        );
    }
}

const fn range(start: Bound<f64>, end: Bound<f64>) -> Constraint<'static> {
    Constraint::BucketRange(BucketRange { start, end })
}

/// A bucket level on years, `[2020, 2022) [2022, 2024) [2024, 2026)`, over a tag level. Doc `n`
/// has one element in bucket `n`.
fn years() -> Index {
    let mut index = Index::new(vec![
        bucket_level(&[2020.0, 2022.0, 2024.0, 2026.0]),
        tag_level(),
    ])
    .unwrap();
    for (doc, year) in [(0, 2021.0), (1, 2022.0), (2, 2025.5)] {
        post(&mut index, &[Key::Numeric(year), tag("x")], doc);
    }
    index
}

#[test]
fn numeric_ranges_select_the_buckets_wholly_inside_them() {
    let index = years();
    let cases: &[(&str, Constraint<'_>, &[u32])] = &[
        (
            "[2022 (2024]",
            range(Included(2022.0), Excluded(2024.0)),
            &[1],
        ),
        (
            "[2022 (2026]",
            range(Included(2022.0), Excluded(2026.0)),
            &[1, 2],
        ),
        (
            "upper end on the last boundary, inclusive",
            range(Included(2022.0), Included(2026.0)),
            &[1, 2],
        ),
        ("[-inf (2024]", range(Unbounded, Excluded(2024.0)), &[0, 1]),
        ("[2024 +inf]", range(Included(2024.0), Unbounded), &[2]),
        (
            "(2019 (2022]: exclusive start below the first boundary",
            range(Excluded(2019.0), Excluded(2022.0)),
            &[0],
        ),
        (
            "ends outside every bucket",
            range(Included(3000.0), Included(5000.0)),
            &[],
        ),
        ("empty", range(Included(2022.0), Excluded(2022.0)), &[]),
        (
            "empty inside a bucket",
            range(Excluded(2023.0), Included(2023.0)),
            &[],
        ),
        ("inverted", range(Included(2024.0), Included(2022.0)), &[]),
        ("unbounded", range(Unbounded, Unbounded), &[0, 1, 2]),
        (
            "[-inf +inf] as inclusive infinities",
            range(Included(f64::NEG_INFINITY), Included(f64::INFINITY)),
            &[0, 1, 2],
        ),
        (
            "upper end one float below a boundary",
            range(Included(2022.0), Included(2024f64.next_down())),
            &[1],
        ),
        (
            "exclusive start one float below a boundary",
            range(Excluded(2022f64.next_down()), Excluded(2024.0)),
            &[1],
        ),
        (
            "empty between adjacent floats",
            range(Excluded(2023.0), Excluded(2023f64.next_up())),
            &[],
        ),
    ];
    for (name, constraint, expected) in cases {
        let terminals = index
            .collect_terminals(
                &[constraint.clone(), Constraint::Any],
                WalkLimits::UNLIMITED,
            )
            .unwrap();
        assert_eq!(union(&terminals), *expected, "{name}");
    }
}

#[test]
fn numeric_ranges_covering_part_of_a_bucket_are_rejected() {
    let index = years();
    let cases: &[(&str, Constraint<'_>)] = &[
        (
            "[2022 2024]: includes 2024, the first value of the next bucket",
            range(Included(2022.0), Included(2024.0)),
        ),
        (
            "(2022 (2024]: excludes 2022, the first value of its bucket",
            range(Excluded(2022.0), Excluded(2024.0)),
        ),
        (
            "start inside a bucket",
            range(Included(2021.0), Excluded(2024.0)),
        ),
        (
            "end inside a bucket",
            range(Included(2022.0), Excluded(2025.0)),
        ),
        (
            "upper end one float above a boundary",
            range(Included(2022.0), Included(2024f64.next_up())),
        ),
        ("NaN start", range(Included(f64::NAN), Unbounded)),
        ("NaN end", range(Unbounded, Excluded(f64::NAN))),
        (
            "NaN on both ends",
            range(Excluded(f64::NAN), Included(f64::NAN)),
        ),
    ];
    for (name, constraint) in cases {
        assert_eq!(
            index
                .collect_terminals(
                    &[constraint.clone(), Constraint::Any],
                    WalkLimits::UNLIMITED
                )
                .err(),
            Some(CollectError::UnalignedBucketRange { level: 0 }),
            "{name}"
        );
    }
}

#[test]
fn numeric_keys_route_to_their_bucket() {
    let index = years();
    // 2023.5 shares the bucket [2022, 2024) with doc 1's 2022.
    assert_eq!(
        index.terminal(&[Key::Numeric(2023.5), tag("x")]).unwrap(),
        Some(&vec![1])
    );
    assert_eq!(
        index.terminal(&[Key::Numeric(2024.0), tag("y")]).unwrap(),
        None
    );
}
