/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Composition of [`RQEIterator::current_max_score`] across composite iterators, and the
//! forwarding of the other score-bound hooks through wrappers, over [`Blocked`] leaves.

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    Intersection, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    UnionFullFlat, UnionFullHeap, UnionQuickFlat,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::{RQEIteratorWrapper, patch_vtable},
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

/// A composite reports bounds only when every child does.
#[test]
fn reports_score_bounds_composes() {
    let term = || TermOrPlain::Term(Blocked::single([1], 1.0));
    let plain = || TermOrPlain::Plain(IdListSorted::new(vec![1]));

    assert!(Intersection::new(vec![term(), term()], 1.0, false).reports_score_bounds());
    assert!(!Intersection::new(vec![term(), plain()], 1.0, false).reports_score_bounds());
    assert!(!Intersection::<TermOrPlain>::new(vec![], 1.0, false).reports_score_bounds());

    assert!(UnionFullFlat::new(vec![term(), term()]).reports_score_bounds());
    assert!(UnionFullHeap::new(vec![term(), term()]).reports_score_bounds());
    assert!(UnionQuickFlat::new(vec![term(), term()]).reports_score_bounds());
    assert!(!UnionFullFlat::new(vec![term(), plain()]).reports_score_bounds());
    assert!(!UnionFullHeap::new(vec![plain(), term()]).reports_score_bounds());
    assert!(!UnionFullFlat::<TermOrPlain>::new(vec![]).reports_score_bounds());

    let not = Not::new(Blocked::single([1], 1.0), 5, 1.0, NoTimeoutChecker);
    assert!(!not.reports_score_bounds());
    let optional = Optional::new(5, 1.0, Blocked::single([1], 1.0));
    assert!(!optional.reports_score_bounds());

    // Wrappers pass the answer through.
    assert!(
        Profile::new(MaybeEmpty::new(Box::new(Blocked::single([1], 1.0)))).reports_score_bounds()
    );
    assert!(
        !Profile::new(MaybeEmpty::new(Box::new(IdListSorted::new(vec![1])))).reports_score_bounds()
    );
}

#[test]
fn intersection_sums_children() {
    let mut it = Intersection::new(
        vec![
            Blocked::single([1, 2, 3], 1.5),
            Blocked::single([2, 3], 2.0),
        ],
        1.0,
        false,
    );
    assert_eq!(read_doc(&mut it), 2);
    assert_eq!(it.current_max_score(), 3.5);
}

#[test]
fn intersection_with_unbounded_child_is_unbounded() {
    let mut it = Intersection::new(
        vec![
            Blocked::single([1, 2], 1.5),
            Blocked::single([2], f64::INFINITY),
        ],
        1.0,
        false,
    );
    read_doc(&mut it);
    assert_eq!(it.current_max_score(), f64::INFINITY);
}

#[test]
fn weight_scales_up_but_never_down() {
    let children = || vec![Blocked::single([1], 1.0), Blocked::single([1], 2.0)];

    let mut heavy = Intersection::new(children(), 2.0, false);
    read_doc(&mut heavy);
    assert_eq!(heavy.current_max_score(), 6.0);

    // DOCSCORE ignores the weight, so a weight below 1 must not shrink the bound.
    let mut light = Intersection::new(children(), 0.5, false);
    read_doc(&mut light);
    assert_eq!(light.current_max_score(), 3.0);

    let mut union = UnionFullFlat::new(children());
    union.current().expect("unread union holds a result").weight = 2.0;
    read_doc(&mut union);
    assert_eq!(union.current_max_score(), 6.0);
}

/// Every union variant sums only the children positioned on the current document.
#[test]
fn union_sums_children_on_current_doc() {
    fn check<'index, I: RQEIterator<'index>>(mut it: I, on_shared_doc: impl Fn(f64) -> bool) {
        // Children: a = {1, 3} bound 1, b = {2, 3} bound 2, c = {3} bound 4.
        assert_eq!(read_doc(&mut it), 1);
        assert_eq!(it.current_max_score(), 1.0);
        assert_eq!(read_doc(&mut it), 2);
        assert_eq!(it.current_max_score(), 2.0);
        assert_eq!(read_doc(&mut it), 3);
        let bound = it.current_max_score();
        assert!(on_shared_doc(bound), "bound {bound} on the shared document");
    }
    let children = || {
        vec![
            Blocked::single([1, 3], 1.0),
            Blocked::single([2, 3], 2.0),
            Blocked::single([3], 4.0),
        ]
    };
    check(UnionFullFlat::new(children()), |bound| bound == 7.0);
    check(UnionFullHeap::new(children()), |bound| bound == 7.0);
    // A quick-exit union stops at the first child on the document and leaves the others behind,
    // so only that child is in its result. The bound covers at least that child.
    check(UnionQuickFlat::new(children()), |bound| {
        (1.0..=7.0).contains(&bound)
    });
}

#[test]
fn not_and_optional_report_no_bound() {
    let mut not = Not::new(Blocked::single([2], 1.0), 5, 1.0, NoTimeoutChecker);
    read_doc(&mut not);
    assert_eq!(not.current_max_score(), f64::INFINITY);

    let mut optional = Optional::new(5, 1.0, Blocked::single([1], 1.0));
    assert_eq!(read_doc(&mut optional), 1);
    assert_eq!(optional.current_max_score(), f64::INFINITY);

    // An intersection with a NOT or optional child can no longer be bounded.
    let mut and_not = Intersection::new(
        vec![
            TermOrNot::Term(Blocked::single([1, 3], 1.0)),
            TermOrNot::Not(Not::new(
                Blocked::single([2], 1.0),
                5,
                1.0,
                NoTimeoutChecker,
            )),
        ],
        1.0,
        false,
    );
    read_doc(&mut and_not);
    assert_eq!(and_not.current_max_score(), f64::INFINITY);
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

/// Children get plain reads: the threshold bounds the composite's score, not theirs.
#[test]
fn composites_do_not_pass_the_threshold_on() {
    let leaf = Blocked::single([1, 2], 1.0);
    let log = leaf.log();
    let mut and = Intersection::new(vec![leaf], 1.0, false);
    let doc = and
        .read_with_threshold(min_score(0.5))
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(1));
    assert_eq!(log.borrow().thresholded_reads, 0);
    assert_eq!(log.borrow().last_threshold, None);

    let leaf = Blocked::single([3], 1.0);
    let log = leaf.log();
    let mut or = UnionFullFlat::new(vec![leaf]);
    let doc = or
        .read_with_threshold(min_score(0.5))
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(3));
    assert_eq!(log.borrow().thresholded_reads, 0);
    assert_eq!(log.borrow().last_threshold, None);
}

#[test]
fn wrappers_forward_every_hook() {
    // A block maximum above the document bound tells the two hooks apart.
    let leaf = Blocked::new(vec![Block::loose(&[(1, 2.5), (2, 2.5)], 3.0)]);
    let log = leaf.log();

    let mut it = Profile::new(MaybeEmpty::new(Box::new(leaf)));
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
fn three_blocks() -> Blocked {
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

/// A C iterator without `ReadWithThreshold` falls back to `Read`.
#[test]
fn c_iterator_without_read_with_threshold_falls_back_to_read() {
    let leaf = three_blocks();
    let log = leaf.log();
    let raw = RQEIteratorWrapper::boxed_new(leaf);
    // SAFETY: `raw` was just returned by `boxed_new` and has no other alias.
    unsafe { patch_vtable(raw, |header| header.ReadWithThreshold = None) };
    // SAFETY: `boxed_new` returns a valid, owning header with `Read` populated.
    let mut child = unsafe { CRQEIterator::new(raw) };
    let mut seen = Vec::new();
    while let Some(doc) = child
        .read_with_threshold(min_score(2.0))
        .expect("read failed")
        .map(|r| r.doc_id)
    {
        seen.push(doc);
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(log.borrow().thresholded_reads, 0);
}

/// Mixes a leaf that reports bounds with one that does not under one composite child type.
enum TermOrPlain {
    Term(Blocked),
    Plain(IdListSorted<'static>),
}

/// Mixes a scored leaf with a NOT under one composite child type.
enum TermOrNot {
    Term(Blocked),
    Not(Not<'static, Blocked, NoTimeoutChecker>),
}

macro_rules! forward_either {
    ($ty:ident, $a:ident, $b:ident) => {
        impl RQEIterator<'static> for $ty {
            fn current(&mut self) -> Option<&mut RSIndexResult<'static>> {
                match self {
                    Self::$a(it) => it.current(),
                    Self::$b(it) => it.current(),
                }
            }

            fn read(&mut self) -> Result<Option<&mut RSIndexResult<'static>>, RQEIteratorError> {
                match self {
                    Self::$a(it) => it.read(),
                    Self::$b(it) => it.read(),
                }
            }

            fn skip_to(
                &mut self,
                doc_id: DocId,
            ) -> Result<Option<SkipToOutcome<'_, 'static>>, RQEIteratorError> {
                match self {
                    Self::$a(it) => it.skip_to(doc_id),
                    Self::$b(it) => it.skip_to(doc_id),
                }
            }

            fn revalidate(
                &mut self,
                spec: &IndexSpecReadGuard,
            ) -> Result<RQEValidateStatus<'_, 'static>, RQEIteratorError> {
                match self {
                    Self::$a(it) => it.revalidate(spec),
                    Self::$b(it) => it.revalidate(spec),
                }
            }

            fn rewind(&mut self) {
                match self {
                    Self::$a(it) => it.rewind(),
                    Self::$b(it) => it.rewind(),
                }
            }

            fn num_estimated(&self) -> usize {
                match self {
                    Self::$a(it) => it.num_estimated(),
                    Self::$b(it) => it.num_estimated(),
                }
            }

            fn last_doc_id(&self) -> DocId {
                match self {
                    Self::$a(it) => it.last_doc_id(),
                    Self::$b(it) => it.last_doc_id(),
                }
            }

            fn at_eof(&self) -> bool {
                match self {
                    Self::$a(it) => it.at_eof(),
                    Self::$b(it) => it.at_eof(),
                }
            }

            fn type_(&self) -> IteratorType {
                IteratorType::Mock
            }

            fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
                1.0
            }

            fn reports_score_bounds(&self) -> bool {
                match self {
                    Self::$a(it) => it.reports_score_bounds(),
                    Self::$b(it) => it.reports_score_bounds(),
                }
            }

            fn current_max_score(&self) -> f64 {
                match self {
                    Self::$a(it) => it.current_max_score(),
                    Self::$b(it) => it.current_max_score(),
                }
            }
        }
    };
}

forward_either!(TermOrPlain, Term, Plain);
forward_either!(TermOrNot, Term, Not);
