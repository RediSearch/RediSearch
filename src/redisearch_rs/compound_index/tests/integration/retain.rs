/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{Constraint, Key, WalkLimits};

use crate::util::{Index, bucket_level, orders, post, tag, tag_level, tag_level_max, union};

#[test]
fn retain_removes_the_rejected_terminals() {
    let mut index = orders();
    // Drop doc 1, as a garbage collector would after its deletion.
    let removed = index.retain_terminals(|docs| {
        docs.retain(|&doc| doc != 1);
        !docs.is_empty()
    });

    // (blue, L) held only doc 1; (red, M) keeps doc 3.
    assert_eq!(removed, 1);
    assert_eq!(index.num_terminals(), 3);
    assert_eq!(index.terminal(&[tag("blue"), tag("L")]).unwrap(), None);
    let all = index
        .collect_terminals(&[Constraint::Any, Constraint::Any], WalkLimits::UNLIMITED)
        .unwrap();
    assert_eq!(union(&all), vec![2, 3, 4]);
}

#[test]
fn retain_drops_parents_left_without_children() {
    // A cardinality of one on the first level makes a leftover empty parent observable: it
    // would keep blocking a second value.
    let mut index = Index::new(vec![tag_level_max(1), tag_level()]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);

    assert_eq!(index.retain_terminals(|_| false), 1);
    assert!(index.is_empty());
    post(&mut index, &[tag("blue"), tag("M")], 2);
    assert_eq!(index.num_terminals(), 1);
}

#[test]
fn retain_keeps_everything_when_asked_to() {
    let mut index = Index::new(vec![tag_level(), tag_level()]).unwrap();
    post(&mut index, &[tag("red"), tag("M")], 1);
    assert_eq!(index.retain_terminals(|_| true), 0);
    assert_eq!(index.num_terminals(), 1);
}

#[test]
fn retain_drops_emptied_bucket_children() {
    let mut index = Index::new(vec![bucket_level(&[0.0, 10.0, 20.0]), tag_level()]).unwrap();
    post(&mut index, &[Key::Numeric(5.0), tag("x")], 1);
    post(&mut index, &[Key::Numeric(15.0), tag("x")], 2);

    assert_eq!(index.retain_terminals(|docs| docs != &vec![1]), 1);
    assert_eq!(
        index.terminal(&[Key::Numeric(5.0), tag("x")]).unwrap(),
        None
    );
    let all = index
        .collect_terminals(&[Constraint::Any, Constraint::Any], WalkLimits::UNLIMITED)
        .unwrap();
    assert_eq!(union(&all), vec![2]);

    // The emptied bucket takes a new path again.
    post(&mut index, &[Key::Numeric(6.0), tag("y")], 3);
    assert_eq!(index.num_terminals(), 2);
}

#[test]
fn a_panicking_keep_leaves_the_terminal_count_exact() {
    let mut index = orders();
    let mut seen = 0;
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        index.retain_terminals(|_| {
            seen += 1;
            assert!(seen < 3, "keep failed");
            false
        });
    }));
    assert!(attempt.is_err());

    // Two terminals were rejected before the panic.
    assert_eq!(index.num_terminals(), 2);
    let all = index
        .collect_terminals(&[Constraint::Any, Constraint::Any], WalkLimits::UNLIMITED)
        .unwrap();
    assert_eq!(all.len(), 2);
}
