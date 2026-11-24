/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{InsertError, Key, PathError};
use proptest::prelude::*;

use crate::util::{Index, bucket_level, post, tag, tag_level, tag_level_max};

/// The batch of one document's elements, as `check_paths` takes it.
fn check(index: &Index, rows: &[[Key<'_>; 2]]) -> Result<(), InsertError> {
    let paths: Vec<&[Key<'_>]> = rows.iter().map(|row| row.as_slice()).collect();
    index.check_paths(&paths)
}

#[test]
fn two_new_values_under_an_existing_parent_count_together() {
    let mut index = Index::new(vec![tag_level(), tag_level_max(2)]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);

    // Each of L and S alone fits red's limit of 2; together they don't.
    assert!(check(&index, &[[tag("red"), tag("L")]]).is_ok());
    assert_eq!(
        check(&index, &[[tag("red"), tag("L")], [tag("red"), tag("S")]]),
        Err(InsertError::CardinalityExceeded { level: 1, max: 2 })
    );
}

#[test]
fn new_values_under_a_parent_the_batch_creates_count_together() {
    let index = Index::new(vec![tag_level(), tag_level_max(1)]).unwrap();
    assert_eq!(
        check(&index, &[[tag("blue"), tag("M")], [tag("blue"), tag("L")]]),
        Err(InsertError::CardinalityExceeded { level: 1, max: 1 })
    );
}

#[test]
fn a_value_repeated_in_the_batch_counts_once() {
    let index = Index::new(vec![tag_level(), tag_level_max(1)]).unwrap();
    assert!(check(&index, &[[tag("blue"), tag("M")], [tag("blue"), tag("M")]]).is_ok());
}

#[test]
fn values_already_present_take_no_new_room() {
    let mut index = Index::new(vec![tag_level(), tag_level_max(1)]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);
    assert!(check(&index, &[[tag("red"), tag("M")]]).is_ok());
}

#[test]
fn an_invalid_path_in_the_batch_is_reported() {
    let index = Index::new(vec![tag_level(), bucket_level(&[0.0, 10.0])]).unwrap();
    assert_eq!(
        check(
            &index,
            &[
                [tag("red"), Key::Numeric(5.0)],
                [tag("red"), Key::Numeric(50.0)]
            ]
        ),
        Err(InsertError::Path(PathError::OutsideBuckets { level: 1 }))
    );
}

const VALUES: [&str; 3] = ["a", "b", "c"];

fn row() -> impl Strategy<Value = (&'static str, &'static str)> {
    (
        prop::sample::select(&VALUES[..]),
        prop::sample::select(&VALUES[..]),
    )
}

const fn path(row: &(&'static str, &'static str)) -> [Key<'static>; 2] {
    [tag(row.0), tag(row.1)]
}

proptest! {
    // No failure file: Miri's isolation forbids the file system access it needs.
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// `check_paths` promises that an accepted batch inserts in any order, so the batch is
    /// inserted in a different order from the one it was checked in.
    #[test]
    fn check_paths_accepts_exactly_the_batches_that_insert_whole(
        existing in prop::collection::vec(row(), 0..6),
        (batch, insert_order) in prop::collection::vec(row(), 0..6)
            .prop_flat_map(|batch| (Just(batch.clone()), Just(batch).prop_shuffle())),
    ) {
        let mut index = Index::new(vec![tag_level_max(2), tag_level_max(2)]).unwrap();
        for row in &existing {
            // Rows past a limit are simply not inserted; the rest is the starting index.
            let _ = index.get_or_insert_with(&path(row), Vec::new);
        }

        let rows: Vec<[Key<'static>; 2]> = batch.iter().map(path).collect();
        let checked = check(&index, &rows);
        let inserted_whole = insert_order
            .iter()
            .all(|row| index.get_or_insert_with(&path(row), Vec::new).is_ok());
        prop_assert_eq!(checked.is_ok(), inserted_whole);
    }
}
