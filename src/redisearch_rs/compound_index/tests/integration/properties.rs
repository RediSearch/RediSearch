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

use compound_index::{CompoundIndex, Constraint, TagSet};
use proptest::prelude::*;
use timeout::NoTimeoutChecker;

use crate::util::tag_level;

/// Overlapping prefixes ("a", "ab") make the prefix constraint meaningful.
const COLORS: [&str; 4] = ["a", "ab", "b", "c"];
const SIZES: [&str; 3] = ["S", "M", "L"];
const MATERIALS: [&str; 3] = ["steel", "stone", "wood"];
const PREFIXES: [&str; 5] = ["", "a", "b", "st", "z"];

#[derive(Debug, Clone)]
struct Element {
    color: &'static str,
    size: &'static str,
    material: &'static str,
}

fn element() -> impl Strategy<Value = Element> {
    (
        prop::sample::select(&COLORS[..]),
        prop::sample::select(&SIZES[..]),
        prop::sample::select(&MATERIALS[..]),
    )
        .prop_map(|(color, size, material)| Element {
            color,
            size,
            material,
        })
}

fn tag_constraint(values: &'static [&'static str]) -> impl Strategy<Value = Constraint<'static>> {
    let subset = prop::sample::subsequence(values, 0..=values.len())
        .prop_map(|vs| TagSet::new(vs.into_iter().map(str::as_bytes)));
    prop_oneof![
        Just(Constraint::Any),
        subset.clone().prop_map(Constraint::TagIn),
        subset.prop_map(Constraint::TagNotIn),
        prop::sample::select(&PREFIXES[..]).prop_map(|p| Constraint::TagPrefix(p.as_bytes())),
    ]
}

fn matches(constraint: &Constraint<'_>, value: &str) -> bool {
    let value = value.as_bytes();
    match constraint {
        Constraint::Any => true,
        Constraint::TagIn(set) => set.values().contains(&value),
        Constraint::TagNotIn(set) => !set.values().contains(&value),
        Constraint::TagPrefix(prefix) => value.starts_with(prefix),
    }
}

fn index_of(elements: &[Element]) -> CompoundIndex<Vec<usize>> {
    let mut index = CompoundIndex::new(vec![tag_level(), tag_level(), tag_level()]).unwrap();
    for (id, e) in elements.iter().enumerate() {
        index
            .get_or_insert_with(&path_of(e), Vec::new)
            .unwrap()
            .push(id);
    }
    index
}

const fn path_of(e: &Element) -> [&'static [u8]; 3] {
    [e.color.as_bytes(), e.size.as_bytes(), e.material.as_bytes()]
}

/// The most elements one case indexes. Miri runs a handful of small cases, as each is slow there.
const MAX_ELEMENTS: usize = if cfg!(miri) { 10 } else { 40 };

proptest! {
    // No failure file: Miri's isolation forbids the file system access it needs.
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 2 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn collect_returns_exactly_the_elements_matching_every_constraint(
        elements in prop::collection::vec(element(), 0..MAX_ELEMENTS),
        color in tag_constraint(&COLORS),
        size in tag_constraint(&SIZES),
        material in tag_constraint(&MATERIALS),
    ) {
        let index = index_of(&elements);
        let constraints = [color.clone(), size.clone(), material.clone()];
        let terminals = index
            .collect_terminals(&constraints, usize::MAX, &mut NoTimeoutChecker)
            .unwrap();
        let found: Vec<usize> = terminals.iter().flat_map(|t| t.iter().copied()).collect();
        let found_set: BTreeSet<usize> = found.iter().copied().collect();

        let expected: BTreeSet<usize> = elements
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                matches(&color, e.color) && matches(&size, e.size) && matches(&material, e.material)
            })
            .map(|(id, _)| id)
            .collect();

        prop_assert_eq!(&found_set, &expected);
        // Each element lives in exactly one terminal, and no terminal is returned twice.
        prop_assert_eq!(found.len(), found_set.len());
    }

}
