/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The score-bound hooks' defaults, and their forwarding through wrappers and the C ABI, over
//! [`Blocked`] leaves.

use rqe_core::DocId;
use rqe_iterators::{
    RQEIterator,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::RQEIteratorWrapper,
    maybe_empty::MaybeEmpty,
    not::Not,
    optional::Optional,
    profile::Profile,
    score_bound::{BlockBound, MinCompetitiveScore},
    utils::NoTimeoutChecker,
};

use crate::utils::blocked::{Block, Blocked, drain_above};

/// The threshold `value`, which the test knows to be positive.
fn min_score(value: f64) -> MinCompetitiveScore {
    MinCompetitiveScore::new(value).expect("a positive threshold")
}

fn read_doc<'index>(it: &mut impl RQEIterator<'index>) -> DocId {
    it.read()
        .expect("read failed")
        .expect("unexpected EOF")
        .doc_id
}

#[test]
fn leaves_report_no_bound_by_default() {
    let mut it = IdListSorted::new(vec![1, 2]);
    read_doc(&mut it);
    assert!(!it.reports_score_bounds());
    assert_eq!(it.current_max_score(), f64::INFINITY);
    assert_eq!(it.current_block_bound(), None);
}

#[test]
fn not_optional_and_wrappers_report_score_bounds() {
    let not = Not::new(Blocked::single([1], 1.0), 5, 1.0, NoTimeoutChecker);
    assert!(!not.reports_score_bounds());
    let optional = Optional::new(5, 1.0, Blocked::single([1], 1.0));
    assert!(!optional.reports_score_bounds());

    // Wrappers pass the answer through.
    assert!(Profile::new(MaybeEmpty::new(Blocked::single([1], 1.0))).reports_score_bounds());
    assert!(!Profile::new(MaybeEmpty::new(IdListSorted::new(vec![1]))).reports_score_bounds());
}

#[test]
fn not_and_optional_report_no_bound() {
    let mut not = Not::new(Blocked::single([2], 1.0), 5, 1.0, NoTimeoutChecker);
    read_doc(&mut not);
    assert_eq!(not.current_max_score(), f64::INFINITY);

    let mut optional = Optional::new(5, 1.0, Blocked::single([1], 1.0));
    assert_eq!(read_doc(&mut optional), 1);
    assert_eq!(optional.current_max_score(), f64::INFINITY);
}

#[test]
fn default_read_with_threshold_is_read() {
    let ids = vec![1, 4, 9];
    let mut plain = IdListSorted::new(ids.clone());
    let mut thresholded = IdListSorted::new(ids);
    loop {
        let expected = plain.read().expect("read failed").map(|r| r.doc_id);
        let actual = thresholded
            .read_with_threshold(min_score(f64::MAX))
            .expect("read failed")
            .map(|r| r.doc_id);
        assert_eq!(actual, expected);
        if expected.is_none() {
            break;
        }
    }
}

#[test]
fn wrappers_forward_every_hook() {
    // A block maximum above the document bound tells the two hooks apart.
    let leaf = Blocked::new(vec![Block::loose(&[(1, 2.5), (2, 2.5)], 3.0)]);
    let log = leaf.log();

    let mut it = Profile::new(MaybeEmpty::new(leaf));
    let doc = it
        .read_with_threshold(min_score(1.25))
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(1));
    assert_eq!(log.borrow().last_threshold, Some(min_score(1.25)));
    assert_eq!(it.current_max_score(), 2.5);
    assert_eq!(
        it.current_block_bound(),
        Some(BlockBound {
            last_doc_id: 2,
            max_score: 3.0,
        })
    );
    // A thresholded read is still a read for the profile.
    assert_eq!(it.counters().read, 1);
}

/// Six documents in three blocks: under a threshold of 2 the middle block is jumped whole, and
/// documents 2 and 5 are stepped over inside blocks that are kept.
fn three_blocks() -> Blocked<'static> {
    Blocked::new(vec![
        Block::tight(&[(1, 5.0), (2, 1.0)]),
        Block::tight(&[(3, 0.5)]),
        Block::tight(&[(4, 3.0), (5, 0.1), (6, 2.0)]),
    ])
}

/// A thresholded leaf reached through the C vtable and a profile wrapper, as a query root is,
/// yields only the competitive documents and positions on no other.
#[test]
fn thresholded_leaf_through_the_c_abi() {
    let leaf = three_blocks();
    let log = leaf.log();
    let raw = RQEIteratorWrapper::boxed_new(leaf);
    // SAFETY: `boxed_new` returns a valid, owning header with every callback populated.
    let child = unsafe { CRQEIterator::new(raw) };
    let mut root = Profile::new(child);
    assert!(root.reports_score_bounds());
    assert_eq!(drain_above(&mut root, min_score(2.0)), vec![1, 4, 6]);
    // Three thresholded reads and the one that hit EOF.
    assert_eq!(root.counters().read, 4);
    assert_eq!(log.borrow().visited, vec![1, 4, 6]);
}

/// Only a positive threshold can drop a candidate, so it is the only one there is.
#[test]
fn only_positive_thresholds_exist() {
    for value in [0.0, -0.0, -1.0, f64::NEG_INFINITY, f64::NAN] {
        assert_eq!(MinCompetitiveScore::new(value), None, "{value}");
    }
    for value in [f64::MIN_POSITIVE, 1.25, f64::INFINITY] {
        assert_eq!(
            MinCompetitiveScore::new(value).map(MinCompetitiveScore::get),
            Some(value)
        );
    }
}

/// A C caller that passes a threshold that cannot drop anything gets the plain read it asks for,
/// and one that can is handed on as is.
#[test]
fn c_threshold_that_cannot_drop_is_a_plain_read() {
    let leaf = Blocked::single(1..=5, 2.0);
    let log = leaf.log();
    let raw = RQEIteratorWrapper::boxed_new(leaf);
    // SAFETY: `boxed_new` populates every callback.
    let read_with_threshold = unsafe { raw.as_ref() }
        .ReadWithThreshold
        .expect("set by boxed_new");
    for value in [0.0, -1.0, f64::NEG_INFINITY, f64::NAN] {
        // SAFETY: `raw` is a valid, uniquely-owned header.
        let status = unsafe { read_with_threshold(raw.as_ptr(), value) };
        assert_eq!(status, ffi::IteratorStatus_ITERATOR_OK, "{value}");
        assert_eq!(log.borrow().thresholded_reads, 0, "{value}");
        assert_eq!(log.borrow().last_threshold, None, "{value}");
    }
    // SAFETY: As above.
    let status = unsafe { read_with_threshold(raw.as_ptr(), 1.5) };
    assert_eq!(status, ffi::IteratorStatus_ITERATOR_OK);
    assert_eq!(log.borrow().last_threshold, Some(min_score(1.5)));
    // SAFETY: `raw` is still a valid, uniquely-owned header; this takes ownership to free it.
    let it = unsafe { CRQEIterator::new(raw) };
    assert_eq!(it.last_doc_id(), 5);
}
