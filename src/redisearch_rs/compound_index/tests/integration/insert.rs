/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{CardinalityExceeded, Constraint};
use timeout::NoTimeoutChecker;

use crate::util::{Index, orders, post, tag, tag_in, tag_level, tag_level_max, union};

#[test]
fn elements_with_the_same_path_share_a_terminal() {
    let index = orders();
    // Doc 3 has two (red, M) items, and doc 1 one: all three land in one terminal.
    let red_m = index.get(&[tag("red"), tag("M")]).unwrap();
    assert_eq!(red_m, &vec![1, 3, 3]);
}

#[test]
fn each_distinct_path_gets_its_own_terminal() {
    let index = orders();
    // (red, M), (red, L), (blue, L), (blue, M).
    assert_eq!(index.num_terminals(), 4);
    assert_eq!(index.get(&[tag("blue"), tag("L")]), Some(&vec![1]));
}

#[test]
fn a_path_never_inserted_has_no_terminal() {
    let index = orders();
    assert_eq!(index.get(&[tag("green"), tag("M")]), None);
    assert_eq!(index.get(&[tag("red"), tag("S")]), None);
}

#[test]
#[should_panic(expected = "a path has one value per level")]
fn an_insert_with_a_path_of_the_wrong_length_panics() {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    let _ = index.get_or_insert_with(&[tag("red")], Vec::new);
}

#[test]
#[should_panic(expected = "a path has one value per level")]
fn a_lookup_with_a_path_of_the_wrong_length_panics() {
    let index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    let _ = index.get(&[tag("red"), tag("M"), tag("x")]);
}

#[test]
fn cardinality_limits_children_per_parent_not_per_level() {
    let mut index = Index::new(vec![tag_level(), tag_level_max(1)]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);
    // Another parent may hold its own single child.
    post(&mut index, &[tag("blue"), tag("L")], 2);
    // An existing child is not a new value.
    post(&mut index, &[tag("red"), tag("M")], 3);

    assert_eq!(
        index
            .get_or_insert_with(&[tag("red"), tag("L")], Vec::new)
            .err(),
        Some(CardinalityExceeded { level: 1, max: 1 })
    );
}

#[test]
fn a_rejected_insert_leaves_the_index_unchanged() {
    let mut index = Index::new(vec![tag_level_max(1), tag_level()]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);

    assert!(
        index
            .get_or_insert_with(&[tag("blue"), tag("M")], Vec::new)
            .is_err()
    );
    assert_eq!(index.num_terminals(), 1);
    let all = index
        .collect_terminals(
            &[Constraint::Any, Constraint::Any],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    assert_eq!(union(&all), vec![1]);
}

#[test]
fn a_panicking_init_leaves_the_index_unchanged() {
    let mut index = Index::new(vec![tag_level_max(1), tag_level()]).unwrap();
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = index.get_or_insert_with(&[tag("red"), tag("M")], || panic!("init failed"));
    }));
    assert!(attempt.is_err());

    assert!(index.is_empty());
    // The failed attempt took no slot under the cardinality limit.
    post(&mut index, &[tag("blue"), tag("M")], 1);
    assert_eq!(index.num_terminals(), 1);
}

#[test]
fn an_empty_tag_value_is_an_ordinary_value() {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    post(&mut index, &[tag(""), tag("M")], 1);
    post(&mut index, &[tag("red"), tag("M")], 2);

    assert_eq!(index.get(&[tag(""), tag("M")]), Some(&vec![1]));
    let empty_only = index
        .collect_terminals(
            &[tag_in(&[""]), Constraint::Any],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    assert_eq!(union(&empty_only), vec![1]);
    // Every value starts with the empty prefix, the empty value included.
    let every_value = index
        .collect_terminals(
            &[Constraint::TagPrefix(b""), Constraint::Any],
            usize::MAX,
            &mut NoTimeoutChecker,
        )
        .unwrap();
    assert_eq!(union(&every_value), vec![1, 2]);
}
