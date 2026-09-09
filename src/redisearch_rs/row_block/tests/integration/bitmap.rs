/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The presence bitmap: which columns a row carries, at column counts that straddle the
//! bitmap's byte boundaries.

use crate::harness::{Decoded, decode, encode, lookup, row};
use pretty_assertions::assert_eq;
use value::SharedValue;

/// Column counts either side of every bitmap byte boundary this format can hit.
const COLUMN_COUNTS: [usize; 6] = [1, 7, 8, 9, 16, 17];

/// `c0`, `c1`, ... up to `count` columns.
fn names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("c{i}")).collect()
}

/// The columns of a row that holds a value only where `present` says so, keyed by name.
fn expected(present: &[bool]) -> Vec<(String, Decoded)> {
    names(present.len())
        .into_iter()
        .zip(present)
        .filter(|(_, present)| **present)
        .enumerate()
        .map(|(nth, (name, _))| (name, Decoded::Number(nth as f64)))
        .collect()
}

/// Encodes and decodes one row holding a value exactly where `present` says so.
fn round_trip(present: &[bool]) -> Vec<(String, Decoded)> {
    let names = names(present.len());
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let lookup = lookup(&refs);

    let values: Vec<(&str, SharedValue)> = refs
        .iter()
        .zip(present)
        .filter(|(_, present)| **present)
        .enumerate()
        .map(|(nth, (name, _))| (*name, SharedValue::new_num(nth as f64)))
        .collect();

    let block = encode(&lookup, &[row(&lookup, &values)]);
    let mut rows = decode(&block);
    assert_eq!(rows.len(), 1);
    rows.pop().expect("one row")
}

#[test]
fn every_column_present() {
    for count in COLUMN_COUNTS {
        let present = vec![true; count];
        assert_eq!(round_trip(&present), expected(&present), "{count} columns");
    }
}

#[test]
fn no_column_present() {
    // A row with an all-zero bitmap is still a row: it is the bitmap bytes and nothing else,
    // which is why a reader can tell it from the end of the block.
    for count in COLUMN_COUNTS {
        let present = vec![false; count];
        assert_eq!(round_trip(&present), vec![], "{count} columns");
    }
}

#[test]
fn sparse_columns_land_on_the_right_bits() {
    // Every other column, starting from the second, so the set bits cross byte boundaries at
    // a different offset for each count.
    for count in COLUMN_COUNTS {
        let present: Vec<bool> = (0..count).map(|i| i % 2 == 1).collect();
        assert_eq!(round_trip(&present), expected(&present), "{count} columns");
    }
}

#[test]
fn only_the_last_column_present() {
    // The highest bit of the last bitmap byte is the one an off-by-one in the byte count
    // would drop.
    for count in COLUMN_COUNTS {
        let mut present = vec![false; count];
        present[count - 1] = true;
        assert_eq!(round_trip(&present), expected(&present), "{count} columns");
    }
}

#[test]
fn rows_with_different_column_sets_stay_independent() {
    let lookup = lookup(&["a", "b", "c"]);
    let rows = [
        row(&lookup, &[("a", SharedValue::new_num(1.0))]),
        row(&lookup, &[]),
        row(
            &lookup,
            &[
                ("b", SharedValue::new_num(2.0)),
                ("c", SharedValue::new_num(3.0)),
            ],
        ),
    ];

    assert_eq!(
        decode(&encode(&lookup, &rows)),
        vec![
            vec![("a".to_owned(), Decoded::Number(1.0))],
            vec![],
            vec![
                ("b".to_owned(), Decoded::Number(2.0)),
                ("c".to_owned(), Decoded::Number(3.0)),
            ],
        ]
    );
}
