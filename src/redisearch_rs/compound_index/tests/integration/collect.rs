/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{BucketRange, CollectError, Constraint, Key};

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
        let terminals = index.collect_terminals(constraints, usize::MAX).unwrap();
        assert_eq!(union(&terminals), *expected, "{name}");
    }
}

#[test]
fn constraints_never_pair_values_from_different_elements() {
    let index = orders();
    // Doc 1 has a blue item and an M item, but no blue M item.
    let terminals = index
        .collect_terminals(&[tag_in(&["blue"]), tag_in(&["M"])], usize::MAX)
        .unwrap();
    assert_eq!(union(&terminals), vec![4]);
}

#[test]
fn repeated_values_in_a_set_visit_a_terminal_once() {
    let index = orders();
    let terminals = index
        .collect_terminals(&[tag_in(&["red", "red"]), tag_in(&["M"])], usize::MAX)
        .unwrap();
    assert_eq!(terminals.len(), 1);
}

#[test]
fn terminals_come_out_in_byte_order_of_their_path() {
    let index = orders();
    let terminals = index
        .collect_terminals(&[Constraint::Any, Constraint::Any], usize::MAX)
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
        index.collect_terminals(&all, 3).err(),
        Some(CollectError::TooManyTerminals { max: 3 })
    );
    assert_eq!(index.collect_terminals(&all, 4).unwrap().len(), 4);
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
            &[Constraint::Any, range(0.0, 1.0)],
            CollectError::ConstraintMismatch { level: 1 },
        ),
        (
            "range start inside a bucket",
            &[range(2021.0, 2024.0), Constraint::Any],
            CollectError::UnalignedBucketRange { level: 0 },
        ),
        (
            "range end inside a bucket",
            &[range(2022.0, 2025.0), Constraint::Any],
            CollectError::UnalignedBucketRange { level: 0 },
        ),
    ];
    for (name, constraints, expected) in cases {
        assert_eq!(
            index.collect_terminals(constraints, usize::MAX).err(),
            Some(expected.clone()),
            "{name}"
        );
    }
}

const fn range(start: f64, end: f64) -> Constraint<'static> {
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
fn aligned_bucket_ranges_select_whole_buckets() {
    let index = years();
    let cases: &[(&str, Constraint<'_>, &[u32])] = &[
        ("one bucket", range(2022.0, 2024.0), &[1]),
        ("two buckets", range(2022.0, 2026.0), &[1, 2]),
        ("open start", range(f64::NEG_INFINITY, 2024.0), &[0, 1]),
        ("open end", range(2024.0, f64::INFINITY), &[2]),
        ("empty", range(2022.0, 2022.0), &[]),
        ("inverted", range(2024.0, 2022.0), &[]),
    ];
    for (name, constraint, expected) in cases {
        let terminals = index
            .collect_terminals(&[constraint.clone(), Constraint::Any], usize::MAX)
            .unwrap();
        assert_eq!(union(&terminals), *expected, "{name}");
    }
}
