/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{Constraint, InsertError, Key, PathError, WalkLimits};

use crate::util::{Index, bucket_level, orders, post, tag, tag_level, tag_level_max, union};

#[test]
fn elements_with_the_same_path_share_a_terminal() {
    let index = orders();
    // Doc 3 has two (red, M) items, and doc 1 one: all three land in one terminal.
    let red_m = index.get(&[tag("red"), tag("M")]).unwrap().unwrap();
    assert_eq!(red_m, &vec![1, 3, 3]);
}

#[test]
fn each_distinct_path_gets_its_own_terminal() {
    let index = orders();
    // (red, M), (red, L), (blue, L), (blue, M).
    assert_eq!(index.num_terminals(), 4);
    assert_eq!(index.get(&[tag("blue"), tag("L")]).unwrap(), Some(&vec![1]));
}

#[test]
fn a_path_never_inserted_has_no_terminal() {
    let index = orders();
    assert_eq!(index.get(&[tag("green"), tag("M")]).unwrap(), None);
    assert_eq!(index.get(&[tag("red"), tag("S")]).unwrap(), None);
}

#[test]
fn invalid_paths_are_rejected() {
    let mut index = Index::new(vec![tag_level(), bucket_level(&[0.0, 10.0])]).unwrap();
    let cases: &[(&str, &[Key<'_>], PathError)] = &[
        (
            "too short",
            &[tag("red")],
            PathError::WrongLength {
                expected: 2,
                got: 1,
            },
        ),
        (
            "numeric key on a tag level",
            &[Key::Numeric(1.0), Key::Numeric(1.0)],
            PathError::KeyMismatch { level: 0 },
        ),
        (
            "tag key on a bucket level",
            &[tag("red"), tag("1")],
            PathError::KeyMismatch { level: 1 },
        ),
        (
            "value outside the buckets",
            &[tag("red"), Key::Numeric(10.0)],
            PathError::OutsideBuckets { level: 1 },
        ),
        (
            "infinite value",
            &[tag("red"), Key::Numeric(f64::INFINITY)],
            PathError::OutsideBuckets { level: 1 },
        ),
    ];
    for (name, path, expected) in cases {
        assert_eq!(
            index.get_or_insert_with(path, Vec::new).err(),
            Some(InsertError::Path(expected.clone())),
            "{name}"
        );
    }
    assert!(index.is_empty());
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
        Some(InsertError::CardinalityExceeded { level: 1, max: 1 })
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
        .collect_terminals(&[Constraint::Any, Constraint::Any], WalkLimits::UNLIMITED)
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
fn lookups_reject_invalid_paths() {
    let index = Index::new(vec![tag_level(), bucket_level(&[0.0, 10.0])]).unwrap();
    let cases: &[(&str, &[Key<'_>], PathError)] = &[
        (
            "too long",
            &[tag("red"), Key::Numeric(1.0), tag("x")],
            PathError::WrongLength {
                expected: 2,
                got: 3,
            },
        ),
        (
            "tag key on a bucket level",
            &[tag("red"), tag("1")],
            PathError::KeyMismatch { level: 1 },
        ),
        (
            "value outside the buckets",
            &[tag("red"), Key::Numeric(-1.0)],
            PathError::OutsideBuckets { level: 1 },
        ),
    ];
    for (name, path, expected) in cases {
        assert_eq!(index.get(path), Err(expected.clone()), "{name}");
    }
}

#[test]
fn an_empty_tag_value_is_an_ordinary_value() {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    post(&mut index, &[tag(""), tag("M")], 1);
    post(&mut index, &[tag("red"), tag("M")], 2);

    assert_eq!(index.get(&[tag(""), tag("M")]).unwrap(), Some(&vec![1]));
    let empty_only = index
        .collect_terminals(
            &[Constraint::TagIn(vec![b""]), Constraint::Any],
            WalkLimits::UNLIMITED,
        )
        .unwrap();
    assert_eq!(union(&empty_only), vec![1]);
    // Every value starts with the empty prefix, the empty value included.
    let every_value = index
        .collect_terminals(
            &[Constraint::TagPrefix(b""), Constraint::Any],
            WalkLimits::UNLIMITED,
        )
        .unwrap();
    assert_eq!(union(&every_value), vec![1, 2]);

    assert_eq!(index.retain_terminals(|docs| docs != &vec![1]), 1);
    assert_eq!(index.get(&[tag(""), tag("M")]).unwrap(), None);
}
