/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A document is indexed whole or not at all, checked over generated documents.

use std::collections::{BTreeMap, BTreeSet};

use nested_index::{Constraint, NestedIndex};
use proptest::prelude::*;
use timeout::NoTimeoutChecker;

use crate::util::{PARAMS, elements, item, priced};

const COLORS: [&str; 3] = ["a", "b", "c"];
const SIZES: [&str; 3] = ["S", "M", "L"];

/// One element: a color, a size, and a price that is NaN one time in eight.
fn element() -> impl Strategy<Value = (&'static str, &'static str, f64)> {
    (
        prop::sample::select(&COLORS[..]),
        prop::sample::select(&SIZES[..]),
        prop_oneof![7 => 0.0f64..100.0, 1 => Just(f64::NAN)],
    )
}

/// The numeric entries across every terminal.
fn total_entries(index: &NestedIndex) -> usize {
    index
        .routing()
        .collect_terminals(
            &[Constraint::Any, Constraint::Any],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap()
        .iter()
        .map(|terminal| {
            terminal
                .numeric()
                .expect("the index has numeric terminals")
                .num_entries()
        })
        .sum()
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Each case is slow under Miri, which runs a handful of small ones.
        cases: if cfg!(miri) { 2 } else { 128 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn every_document_is_indexed_whole_or_not_at_all(
        docs in prop::collection::vec(
            prop::collection::vec(element(), 0..5),
            0..if cfg!(miri) { 4 } else { 12 },
        ),
    ) {
        // Two sizes per color, so some documents break the limit.
        let mut index = priced(Some(2));
        // The model: the sizes indexed under each color so far.
        let mut sizes: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (position, doc) in docs.iter().enumerate() {
            let items: Vec<_> = doc.iter().map(|(color, size, price)| item(color, size, *price)).collect();
            let entries_before = total_entries(&index);
            let terminals_before = index.routing().num_terminals();

            let checked = index.check_document(&elements(&items));
            let result = index.index_document(position as u64 + 1, &elements(&items), PARAMS);
            // The check run before the spec lock decides what indexing does under it.
            prop_assert_eq!(checked.is_ok(), result.is_ok());

            // Accepted exactly when no price is NaN and no color would end up with more than two
            // sizes, counting the sizes indexed before and those this document brings.
            let mut after = sizes.clone();
            for (color, size, _) in doc {
                after.entry(*color).or_default().insert(*size);
            }
            let accepted = doc.iter().all(|(_, _, price)| !price.is_nan())
                && after.values().all(|s| s.len() <= 2);
            prop_assert_eq!(result.is_ok(), accepted);
            if accepted {
                sizes = after;
            }

            match result {
                Ok(_) => prop_assert_eq!(total_entries(&index), entries_before + items.len()),
                Err(_) => {
                    prop_assert_eq!(total_entries(&index), entries_before);
                    prop_assert_eq!(index.routing().num_terminals(), terminals_before);
                }
            }
        }
    }
}
