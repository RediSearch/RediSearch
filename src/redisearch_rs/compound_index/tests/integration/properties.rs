/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `collect_terminals` and `retain_terminals` checked against brute-force filters over the
//! inserted elements.

use std::{
    collections::BTreeSet,
    ops::{Bound, RangeBounds},
};

use compound_index::{BucketRange, CollectError, CompoundIndex, Constraint, Key};
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

/// Ends on bucket boundaries and on bucket midpoints, so that some ranges cover whole buckets
/// and some cut one.
fn year_bound() -> impl Strategy<Value = Bound<f64>> {
    let points = vec![0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0];
    prop_oneof![
        Just(Bound::Unbounded),
        prop::sample::select(points.clone()).prop_map(Bound::Included),
        prop::sample::select(points).prop_map(Bound::Excluded),
    ]
}

fn year_constraint() -> impl Strategy<Value = Constraint<'static>> {
    prop_oneof![
        Just(Constraint::Any),
        (year_bound(), year_bound())
            .prop_map(|(start, end)| Constraint::BucketRange(BucketRange { start, end })),
    ]
}

fn in_range(range: &BucketRange, value: f64) -> bool {
    (range.start, range.end).contains(&value)
}

/// Whether `range` covers part of a bucket. Ends drawn by [`year_bound`] sit on a bucket's
/// boundaries or its middle, so sampling its start, quarters, middle and last representable value
/// is enough to tell.
fn cuts_a_bucket(range: &BucketRange) -> bool {
    YEARS.windows(2).any(|bucket| {
        let (lo, hi) = (bucket[0], bucket[1]);
        let step = (hi - lo) / 4.0;
        let samples = [
            lo,
            lo + step,
            lo + 2.0 * step,
            lo + 3.0 * step,
            hi.next_down(),
        ];
        let inside = samples.iter().filter(|v| in_range(range, **v)).count();
        inside != 0 && inside != samples.len()
    })
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

fn year_matches(constraint: &Constraint<'_>, year: f64) -> bool {
    match constraint {
        Constraint::Any => true,
        Constraint::BucketRange(range) => in_range(range, year),
        _ => unreachable!("the bucket level gets bucket constraints"),
    }
}

fn index_of(elements: &[Element]) -> CompoundIndex<Vec<usize>> {
    let mut index =
        CompoundIndex::new(vec![tag_level(), tag_level(), bucket_level(&YEARS)]).unwrap();
    for (id, e) in elements.iter().enumerate() {
        index
            .terminal_or_insert_with(&path_of(e), Vec::new)
            .unwrap()
            .push(id);
    }
    index
}

const fn path_of(e: &Element) -> [Key<'static>; 3] {
    [
        Key::Tag(e.color.as_bytes()),
        Key::Tag(e.size.as_bytes()),
        Key::Numeric(e.year),
    ]
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
        let index = index_of(&elements);
        let constraints = [color.clone(), size.clone(), year.clone()];
        let result = index.collect_terminals(&constraints, usize::MAX);

        if let Constraint::BucketRange(range) = &year
            && cuts_a_bucket(range)
        {
            prop_assert_eq!(result.err(), Some(CollectError::UnalignedBucketRange { level: 2 }));
            return Ok(());
        }
        let terminals = result.unwrap();
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

    #[test]
    fn retain_keeps_exactly_the_surviving_elements_and_counts_their_paths(
        elements in prop::collection::vec(element(), 0..40),
        deleted in prop::collection::btree_set(0usize..40, 0..40),
    ) {
        let mut index = index_of(&elements);
        index.retain_terminals(|ids| {
            ids.retain(|id| !deleted.contains(id));
            !ids.is_empty()
        });

        let all = [Constraint::Any, Constraint::Any, Constraint::Any];
        let terminals = index.collect_terminals(&all, usize::MAX).unwrap();
        let found: BTreeSet<usize> = terminals.iter().flat_map(|t| t.iter().copied()).collect();
        let survivors: BTreeSet<usize> =
            (0..elements.len()).filter(|id| !deleted.contains(id)).collect();
        prop_assert_eq!(found, survivors.clone());

        // One terminal per distinct path among the survivors: (color, size, year bucket).
        let paths: BTreeSet<(&str, &str, usize)> = survivors
            .iter()
            .map(|&id| {
                let e = &elements[id];
                (e.color, e.size, YEARS.iter().filter(|b| **b <= e.year).count())
            })
            .collect();
        prop_assert_eq!(index.num_terminals(), paths.len());
        prop_assert_eq!(terminals.len(), paths.len());
    }
}
