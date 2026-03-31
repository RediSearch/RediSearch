/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use rqe_iterators::{
    Intersection, RQEIterator, Wildcard,
    not::Not,
    optional::Optional,
    union::{Union, UnionQuickFlat},
};

use crate::utils::Mock;

/// A leaf iterator's `into_profiled` wraps it in a single `Profile` layer.
#[test]
fn leaf_wraps_self() {
    let child = Mock::new([1, 3, 5]);
    let mut profiled = child.into_profiled();

    // The outer layer is a Profile — counters start at zero.
    assert_eq!(profiled.counters().read, 0);
    assert_eq!(profiled.counters().skip_to, 0);

    // Reads are delegated through the Profile wrapper and counted.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    assert_eq!(profiled.counters().read, 2);
}

/// `Not::into_profiled` wraps the child in `Profile`, then wraps `self`.
#[test]
fn not_wraps_child_and_self() {
    let child = Mock::new([2, 4]);
    let not = Not::new(child, 5, 1.0, Duration::ZERO, true);
    let mut profiled = not.into_profiled();

    // Outer Profile wraps the Not.
    assert_eq!(profiled.counters().read, 0);

    // Read: NOT yields 1 (not in child), then 3, then 5.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    assert_eq!(profiled.counters().read, 2);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert_eq!(profiled.counters().read, 3);

    // EOF
    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `Not::into_profiled` with an empty child — all docs are yielded.
#[test]
fn not_empty_child() {
    let child = Mock::new([]);
    let not = Not::new(child, 3, 1.0, Duration::ZERO, true);
    let mut profiled = not.into_profiled();

    for expected in 1..=3 {
        let r = profiled.read().unwrap().unwrap();
        assert_eq!(r.doc_id, expected);
    }
    assert!(profiled.read().unwrap().is_none());
    assert_eq!(profiled.counters().read, 4);
}

/// `Optional::into_profiled` wraps the child in `Profile`, then wraps `self`.
#[test]
fn optional_wraps_child_and_self() {
    let child = Mock::new([2, 4]);
    let opt = Optional::new(5, 2.0, child);
    let mut profiled = opt.into_profiled();

    // Outer Profile layer.
    assert_eq!(profiled.counters().read, 0);

    // Optional yields every doc 1..=5; docs 2,4 get weight from child.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 2);
    assert_eq!(r.weight, 2.0); // real result from child
    assert_eq!(profiled.counters().read, 2);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3); // virtual
    assert_eq!(profiled.counters().read, 3);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 4);
    assert_eq!(r.weight, 2.0); // real result from child
    assert_eq!(profiled.counters().read, 4);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5); // virtual
    assert_eq!(profiled.counters().read, 5);

    // EOF
    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `Intersection::into_profiled` wraps each child in `Profile`, then wraps `self`.
#[cfg(not(miri))] // calls ffi::RSYieldableMetric_Concat
#[test]
fn intersection_wraps_children_and_self() {
    let a = Mock::new([1, 3, 5, 7]);
    let b = Mock::new([3, 5, 7, 9]);
    let inter = Intersection::new(vec![a, b], 1.0, false);
    let mut profiled = inter.into_profiled();

    // Outer Profile layer.
    assert_eq!(profiled.counters().read, 0);

    // Intersection yields {3, 5, 7}.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert_eq!(profiled.counters().read, 2);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 7);
    assert_eq!(profiled.counters().read, 3);

    // EOF
    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `Intersection::into_profiled` with no children is immediately at EOF.
#[test]
fn intersection_empty_children() {
    let inter: Intersection<Mock<0>> = Intersection::new(vec![], 1.0, false);
    let mut profiled = inter.into_profiled();

    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `Union::into_profiled` (full mode) wraps each child in `Profile`, then wraps `self`.
#[cfg_attr(miri, ignore = "call ffi::RSYieldableMetric_Concat")]
#[test]
fn union_full_wraps_children_and_self() {
    let a = Mock::new([1, 3, 5]);
    let b = Mock::new([2, 3, 6]);
    let union = Union::new(vec![a, b]);
    let mut profiled = union.into_profiled();

    // Outer Profile layer.
    assert_eq!(profiled.counters().read, 0);

    // Union yields {1, 2, 3, 5, 6}.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 2);
    assert_eq!(profiled.counters().read, 2);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    assert_eq!(profiled.counters().read, 3);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert_eq!(profiled.counters().read, 4);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 6);
    assert_eq!(profiled.counters().read, 5);

    // EOF
    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `UnionQuickFlat::into_profiled` wraps children and returns after first match.
#[cfg_attr(miri, ignore = "call ffi::RSYieldableMetric_Concat")]
#[test]
fn union_quick_wraps_children_and_self() {
    let a = Mock::new([1, 5]);
    let b = Mock::new([3, 5]);
    let union = UnionQuickFlat::new(vec![a, b]);
    let mut profiled = union.into_profiled();

    assert_eq!(profiled.counters().read, 0);

    // Quick mode: skip_to finds first match.
    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 1);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);

    // EOF
    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `Union::into_profiled` with no children is immediately at EOF.
#[test]
fn union_empty_children() {
    let union: Union<Mock<0>> = Union::new(vec![]);
    let mut profiled = union.into_profiled();

    assert!(profiled.read().unwrap().is_none());
    assert!(profiled.counters().eof);
}

/// `into_profiled` on a leaf iterator still supports `skip_to`.
#[test]
fn leaf_skip_to() {
    let child = Wildcard::new(10, 1.0);
    let mut profiled = child.into_profiled();

    let r = profiled.skip_to(5).unwrap().unwrap();
    assert!(matches!(r, rqe_iterators::SkipToOutcome::Found(r) if r.doc_id == 5));
    assert_eq!(profiled.counters().skip_to, 1);
    assert_eq!(profiled.counters().read, 0);
}

/// Profiled `Not` supports `skip_to`.
#[test]
fn not_skip_to() {
    let child = Mock::new([3, 7]);
    let not = Not::new(child, 10, 1.0, Duration::ZERO, true);
    let mut profiled = not.into_profiled();

    // skip_to(5): doc 5 is not in child {3,7}, so NOT yields it.
    let r = profiled.skip_to(5).unwrap().unwrap();
    assert!(matches!(r, rqe_iterators::SkipToOutcome::Found(r) if r.doc_id == 5));
    assert_eq!(profiled.counters().skip_to, 1);
}

/// Profiled `Not` supports `rewind`.
#[test]
fn not_rewind() {
    let child = Mock::new([2]);
    let not = Not::new(child, 5, 1.0, Duration::ZERO, true);
    let mut profiled = not.into_profiled();

    let _ = profiled.read().unwrap(); // doc 1
    let _ = profiled.read().unwrap(); // doc 3
    assert_eq!(profiled.counters().read, 2);

    profiled.rewind();
    assert_eq!(profiled.last_doc_id(), 0);
    assert!(!profiled.at_eof());

    let r = profiled.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1);
    assert_eq!(profiled.counters().read, 3);
}
