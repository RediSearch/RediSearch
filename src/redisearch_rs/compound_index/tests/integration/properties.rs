/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `collect_terminals` checked against a brute-force filter over the inserted elements.

use std::collections::BTreeSet;

use compound_index::{BucketRange, CompoundIndex, Constraint, Key};
use proptest::prelude::*;

use crate::util::{bucket_level, tag_level};

/// Overlapping prefixes ("a", "ab") make the prefix constraint meaningful.
const COLORS: [&str; 4] = ["a", "ab", "b", "c"];
const SIZES: [&str; 3] = ["S", "M", "L"];
const PREFIXES: [&str; 4] = ["", "a", "b", "z"];
const YEARS: [f64; 4] = [0.0, 10.0, 20.0, 30.0];

#[derive(Debug, Clone)]
struct Element {
    color: &'static str,
    size: &'static str,
    year: f64,
}

fn element() -> impl Strategy<Value = Element> {
    (
        prop::sample::select(&COLORS[..]),
        prop::sample::select(&SIZES[..]),
        0.0f64..30.0,
    )
        .prop_map(|(color, size, year)| Element { color, size, year })
}

fn tag_constraint(values: &'static [&'static str]) -> impl Strategy<Value = Constraint<'static>> {
    let subset = prop::sample::subsequence(values, 0..=values.len())
        .prop_map(|vs| vs.into_iter().map(str::as_bytes).collect::<Vec<_>>());
    prop_oneof![
        Just(Constraint::Any),
        subset.clone().prop_map(Constraint::TagIn),
        subset.prop_map(Constraint::TagNotIn),
        prop::sample::select(&PREFIXES[..]).prop_map(|p| Constraint::TagPrefix(p.as_bytes())),
    ]
}

fn year_constraint() -> impl Strategy<Value = Constraint<'static>> {
    let ends = [
        f64::NEG_INFINITY,
        YEARS[0],
        YEARS[1],
        YEARS[2],
        YEARS[3],
        f64::INFINITY,
    ];
    prop_oneof![
        Just(Constraint::Any),
        (
            prop::sample::select(ends.to_vec()),
            prop::sample::select(ends.to_vec())
        )
            .prop_map(|(start, end)| Constraint::BucketRange(BucketRange { start, end })),
    ]
}

fn tag_matches(constraint: &Constraint<'_>, value: &str) -> bool {
    let value = value.as_bytes();
    match constraint {
        Constraint::Any => true,
        Constraint::TagIn(set) => set.contains(&value),
        Constraint::TagNotIn(set) => !set.contains(&value),
        Constraint::TagPrefix(prefix) => value.starts_with(prefix),
        Constraint::BucketRange(_) => unreachable!("tag levels get tag constraints"),
    }
}

/// Aligned ranges cover whole buckets, so a value matches when its bucket's start is inside
/// the range.
fn year_matches(constraint: &Constraint<'_>, year: f64) -> bool {
    match constraint {
        Constraint::Any => true,
        Constraint::BucketRange(range) => {
            let bucket_start = YEARS.iter().rev().find(|b| **b <= year).unwrap();
            range.start <= *bucket_start && *bucket_start < range.end
        }
        _ => unreachable!("the bucket level gets bucket constraints"),
    }
}

proptest! {
    // No failure file: Miri's isolation forbids the file system access it needs.
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn collect_returns_exactly_the_elements_matching_every_constraint(
        elements in prop::collection::vec(element(), 0..40),
        color in tag_constraint(&COLORS),
        size in tag_constraint(&SIZES),
        year in year_constraint(),
    ) {
        let mut index: CompoundIndex<Vec<usize>> =
            CompoundIndex::new(vec![tag_level(), tag_level(), bucket_level(&YEARS)]).unwrap();
        for (id, e) in elements.iter().enumerate() {
            let path = [Key::Tag(e.color.as_bytes()), Key::Tag(e.size.as_bytes()), Key::Numeric(e.year)];
            index.terminal_or_insert_with(&path, Vec::new).unwrap().push(id);
        }

        let constraints = [color.clone(), size.clone(), year.clone()];
        let terminals = index.collect_terminals(&constraints, usize::MAX).unwrap();
        let found: Vec<usize> = terminals.iter().flat_map(|t| t.iter().copied()).collect();
        let found_set: BTreeSet<usize> = found.iter().copied().collect();

        let expected: BTreeSet<usize> = elements
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                tag_matches(&color, e.color) && tag_matches(&size, e.size) && year_matches(&year, e.year)
            })
            .map(|(id, _)| id)
            .collect();

        prop_assert_eq!(&found_set, &expected);
        // Each element lives in exactly one terminal, and no terminal is returned twice.
        prop_assert_eq!(found.len(), found_set.len());
    }
}
