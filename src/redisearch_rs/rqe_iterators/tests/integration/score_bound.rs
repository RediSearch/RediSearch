/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Composition of [`RQEIterator::current_max_score`] across composite iterators, and the
//! forwarding of the other score-bound hooks through wrappers.

use std::cell::Cell;
use std::rc::Rc;

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    Intersection, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, ScoreBoundCtx,
    SkipToOutcome, UnionFullFlat, UnionFullHeap, UnionQuickFlat,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::{RQEIteratorWrapper, patch_vtable},
    maybe_empty::MaybeEmpty,
    not::Not,
    optional::Optional,
    profile::Profile,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::NoTimeoutChecker,
};

const CTX: ScoreBoundCtx = ScoreBoundCtx {
    filterLeafBound: 0.0,
};

/// A leaf over fixed doc ids that reports a fixed score bound and block boundary, and records the
/// last threshold passed to [`RQEIterator::read_with_threshold`].
struct Bounded<'index> {
    inner: IdListSorted<'index>,
    bound: f64,
    block_last_doc_id: Option<DocId>,
    threshold: Rc<Cell<f64>>,
}

impl<'index> Bounded<'index> {
    fn new(ids: Vec<DocId>, bound: f64) -> Self {
        Self {
            inner: IdListSorted::new(ids),
            bound,
            block_last_doc_id: None,
            threshold: Rc::new(Cell::new(f64::NEG_INFINITY)),
        }
    }
}

impl<'index> RQEIterator<'index> for Bounded<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.inner.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.inner.skip_to(doc_id)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.inner.revalidate(spec)
    }

    fn rewind(&mut self) {
        self.inner.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.inner.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.inner.at_eof()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }

    fn current_max_score(&self, _ctx: &ScoreBoundCtx) -> f64 {
        self.bound
    }

    fn current_block_last_doc_id(&self) -> Option<DocId> {
        self.block_last_doc_id
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: f64,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.threshold.set(min_competitive_score);
        self.inner.read()
    }
}

/// A leaf standing for a numeric, tag or geo filter.
struct Filter<'index>(Bounded<'index>);

impl<'index> Filter<'index> {
    fn new(ids: Vec<DocId>) -> Self {
        Self(Bounded::new(ids, f64::NAN))
    }
}

impl<'index> RQEIterator<'index> for Filter<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.0.revalidate(spec)
    }

    fn rewind(&mut self) {
        self.0.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.0.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }

    fn current_max_score(&self, ctx: &ScoreBoundCtx) -> f64 {
        ctx.filterLeafBound
    }
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
    assert_eq!(it.current_max_score(&CTX), f64::INFINITY);
    assert_eq!(it.current_block_last_doc_id(), None);
}

#[test]
fn intersection_sums_children() {
    let mut it = Intersection::new(
        vec![
            Bounded::new(vec![1, 2, 3], 1.5),
            Bounded::new(vec![2, 3], 2.0),
        ],
        1.0,
        false,
    );
    assert_eq!(read_doc(&mut it), 2);
    assert_eq!(it.current_max_score(&CTX), 3.5);
}

#[test]
fn intersection_with_filter_adds_filter_leaf_bound() {
    let mut it = Intersection::new(
        vec![
            TermOrFilter::Term(Bounded::new(vec![1, 2], 1.5)),
            TermOrFilter::Filter(Filter::new(vec![2])),
        ],
        1.0,
        false,
    );
    assert_eq!(read_doc(&mut it), 2);
    assert_eq!(it.current_max_score(&CTX), 1.5);
    let unknown = ScoreBoundCtx {
        filterLeafBound: f64::INFINITY,
    };
    assert_eq!(it.current_max_score(&unknown), f64::INFINITY);
}

#[test]
fn intersection_with_unbounded_child_is_unbounded() {
    let mut it = Intersection::new(
        vec![
            Bounded::new(vec![1, 2], 1.5),
            Bounded::new(vec![2], f64::INFINITY),
        ],
        1.0,
        false,
    );
    read_doc(&mut it);
    assert_eq!(it.current_max_score(&CTX), f64::INFINITY);
}

#[test]
fn weight_scales_up_but_never_down() {
    let children = || vec![Bounded::new(vec![1], 1.0), Bounded::new(vec![1], 2.0)];

    let mut heavy = Intersection::new(children(), 2.0, false);
    read_doc(&mut heavy);
    assert_eq!(heavy.current_max_score(&CTX), 6.0);

    // DOCSCORE ignores the weight, so a weight below 1 must not shrink the bound.
    let mut light = Intersection::new(children(), 0.5, false);
    read_doc(&mut light);
    assert_eq!(light.current_max_score(&CTX), 3.0);

    let mut union = UnionFullFlat::new(children());
    union.current().expect("unread union holds a result").weight = 2.0;
    read_doc(&mut union);
    assert_eq!(union.current_max_score(&CTX), 6.0);
}

/// Every union variant sums only the children positioned on the current document.
#[test]
fn union_sums_children_on_current_doc() {
    fn check<'index, I: RQEIterator<'index>>(mut it: I, on_shared_doc: impl Fn(f64) -> bool) {
        // Children: a = {1, 3} bound 1, b = {2, 3} bound 2, c = {3} bound 4.
        assert_eq!(read_doc(&mut it), 1);
        assert_eq!(it.current_max_score(&CTX), 1.0);
        assert_eq!(read_doc(&mut it), 2);
        assert_eq!(it.current_max_score(&CTX), 2.0);
        assert_eq!(read_doc(&mut it), 3);
        let bound = it.current_max_score(&CTX);
        assert!(on_shared_doc(bound), "bound {bound} on the shared document");
    }
    let children = || {
        vec![
            Bounded::new(vec![1, 3], 1.0),
            Bounded::new(vec![2, 3], 2.0),
            Bounded::new(vec![3], 4.0),
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
fn union_with_filter_child_on_doc_reports_filter_leaf_bound() {
    let mut it = UnionFullFlat::new(vec![
        TermOrFilter::Term(Bounded::new(vec![2], 1.5)),
        TermOrFilter::Filter(Filter::new(vec![1])),
    ]);
    assert_eq!(read_doc(&mut it), 1);
    assert_eq!(it.current_max_score(&CTX), 0.0);
    assert_eq!(read_doc(&mut it), 2);
    assert_eq!(it.current_max_score(&CTX), 1.5);
}

#[test]
fn not_and_optional_report_no_bound() {
    let mut not = Not::new(Bounded::new(vec![2], 1.0), 5, 1.0, NoTimeoutChecker);
    read_doc(&mut not);
    assert_eq!(not.current_max_score(&CTX), f64::INFINITY);

    let mut optional = Optional::new(5, 1.0, Bounded::new(vec![1], 1.0));
    assert_eq!(read_doc(&mut optional), 1);
    assert_eq!(optional.current_max_score(&CTX), f64::INFINITY);

    // An intersection with a NOT or optional child can no longer be bounded.
    let mut and_not = Intersection::new(
        vec![
            TermOrNot::Term(Bounded::new(vec![1, 3], 1.0)),
            TermOrNot::Not(Not::new(
                Bounded::new(vec![2], 1.0),
                5,
                1.0,
                NoTimeoutChecker,
            )),
        ],
        1.0,
        false,
    );
    read_doc(&mut and_not);
    assert_eq!(and_not.current_max_score(&CTX), f64::INFINITY);
}

#[test]
fn default_read_with_threshold_is_read() {
    let ids = vec![1, 4, 9];
    let mut plain = IdListSorted::new(ids.clone());
    let mut thresholded = IdListSorted::new(ids);
    loop {
        let expected = plain.read().expect("read failed").map(|r| r.doc_id);
        let actual = thresholded
            .read_with_threshold(f64::MAX)
            .expect("read failed")
            .map(|r| r.doc_id);
        assert_eq!(actual, expected);
        if expected.is_none() {
            break;
        }
    }
}

#[test]
fn composites_ignore_the_threshold() {
    let leaf = Bounded::new(vec![1, 2], 1.0);
    let threshold = Rc::clone(&leaf.threshold);
    let mut and = Intersection::new(vec![leaf], 1.0, false);
    let doc = and
        .read_with_threshold(10.0)
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(1));
    assert_eq!(threshold.get(), f64::NEG_INFINITY);

    let leaf = Bounded::new(vec![3], 1.0);
    let threshold = Rc::clone(&leaf.threshold);
    let mut or = UnionFullFlat::new(vec![leaf]);
    let doc = or
        .read_with_threshold(10.0)
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(3));
    assert_eq!(threshold.get(), f64::NEG_INFINITY);
}

#[test]
fn wrappers_forward_every_hook() {
    let mut leaf = Bounded::new(vec![1, 2], 2.5);
    leaf.block_last_doc_id = Some(7);
    let threshold = Rc::clone(&leaf.threshold);

    let mut it = Profile::new(MaybeEmpty::new(Box::new(leaf)));
    let doc = it
        .read_with_threshold(1.25)
        .expect("read failed")
        .map(|r| r.doc_id);
    assert_eq!(doc, Some(1));
    assert_eq!(threshold.get(), 1.25);
    assert_eq!(it.current_max_score(&CTX), 2.5);
    assert_eq!(it.current_block_last_doc_id(), Some(7));
    // A thresholded read is still a read for the profile.
    assert_eq!(it.counters().read, 1);
}

/// A leaf whose documents have their own bounds and that skips, inside
/// [`RQEIterator::read_with_threshold`], the ones whose bound is below the threshold, up to
/// `max_skips` in a row.
struct Skipping<'index> {
    inner: IdListSorted<'index>,
    bounds: Vec<f64>,
    position: Option<usize>,
    max_skips: usize,
}

impl<'index> Skipping<'index> {
    fn new(docs: &[(DocId, f64)], max_skips: usize) -> Self {
        Self {
            inner: IdListSorted::new(docs.iter().map(|&(id, _)| id).collect::<Vec<_>>()),
            bounds: docs.iter().map(|&(_, bound)| bound).collect(),
            position: None,
            max_skips,
        }
    }

    fn step(&mut self) -> Result<Option<DocId>, RQEIteratorError> {
        let doc = self.inner.read()?.map(|r| r.doc_id);
        self.position = doc.map(|_| self.position.map_or(0, |p| p + 1));
        Ok(doc)
    }
}

impl<'index> RQEIterator<'index> for Skipping<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.step()?;
        Ok(self.inner.current())
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: f64,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let mut skipped = 0;
        while self.step()?.is_some() {
            let below = self.current_max_score(&CTX) < min_competitive_score;
            if !below || skipped == self.max_skips {
                return Ok(self.inner.current());
            }
            skipped += 1;
        }
        Ok(None)
    }

    fn skip_to(
        &mut self,
        _doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        unimplemented!("not used by these tests")
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.inner.revalidate(spec)
    }

    fn rewind(&mut self) {
        self.inner.rewind();
        self.position = None;
    }

    fn num_estimated(&self) -> usize {
        self.inner.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.inner.at_eof()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }

    fn current_max_score(&self, _ctx: &ScoreBoundCtx) -> f64 {
        self.position.map_or(f64::INFINITY, |p| self.bounds[p])
    }
}

impl ProfilePrint for Skipping<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, _ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}

/// The root loop of the index result processor: read with the threshold, then drop whatever
/// still comes back below it.
fn root_survivors<'index>(
    it: &mut impl RQEIterator<'index>,
    min_competitive_score: f64,
) -> Vec<DocId> {
    let mut survivors = Vec::new();
    while let Some(doc) = it
        .read_with_threshold(min_competitive_score)
        .expect("read failed")
        .map(|r| r.doc_id)
    {
        if it.current_max_score(&CTX) >= min_competitive_score {
            survivors.push(doc);
        }
    }
    survivors
}

const DOCS: [(DocId, f64); 6] = [(1, 5.0), (2, 1.0), (3, 0.5), (4, 3.0), (5, 0.1), (6, 2.0)];

/// A leaf that skips below the threshold, reached through the C vtable and a profile wrapper as
/// a query root is, yields the same survivors as one that does not skip at all, and only the
/// competitive documents.
#[test]
fn skipping_leaf_through_the_c_abi() {
    for max_skips in [0, 1, usize::MAX] {
        let raw = RQEIteratorWrapper::boxed_new(Skipping::new(&DOCS, max_skips));
        // SAFETY: `boxed_new` returns a valid, owning header with every callback populated.
        let child = unsafe { CRQEIterator::new(raw) };
        let mut root = Profile::new(child);
        assert_eq!(
            root_survivors(&mut root, 2.0),
            vec![1, 4, 6],
            "max_skips {max_skips}"
        );
        if max_skips == usize::MAX {
            // Every below-threshold document was skipped by the leaf, none by the root.
            assert_eq!(root.counters().read, 4);
        }
    }
}

/// A C iterator without `ReadWithThreshold` falls back to `Read`, and the root check alone keeps
/// the result correct.
#[test]
fn c_iterator_without_read_with_threshold_falls_back_to_read() {
    let raw = RQEIteratorWrapper::boxed_new(Skipping::new(&DOCS, usize::MAX));
    // SAFETY: `raw` was just returned by `boxed_new` and has no other alias.
    unsafe { patch_vtable(raw, |header| header.ReadWithThreshold = None) };
    // SAFETY: `boxed_new` returns a valid, owning header with `Read` populated.
    let mut child = unsafe { CRQEIterator::new(raw) };
    let mut seen = Vec::new();
    while let Some(doc) = child
        .read_with_threshold(2.0)
        .expect("read failed")
        .map(|r| r.doc_id)
    {
        seen.push(doc);
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 6]);
    child.rewind();
    assert_eq!(root_survivors(&mut child, 2.0), vec![1, 4, 6]);
}

/// Mixes a scored leaf with a filter leaf under one composite child type.
enum TermOrFilter<'index> {
    Term(Bounded<'index>),
    Filter(Filter<'index>),
}

/// Mixes a scored leaf with a NOT under one composite child type.
enum TermOrNot<'index> {
    Term(Bounded<'index>),
    Not(Not<'index, Bounded<'index>, NoTimeoutChecker>),
}

macro_rules! forward_either {
    ($ty:ident, $a:ident, $b:ident) => {
        impl<'index> RQEIterator<'index> for $ty<'index> {
            fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
                match self {
                    Self::$a(it) => it.current(),
                    Self::$b(it) => it.current(),
                }
            }

            fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
                match self {
                    Self::$a(it) => it.read(),
                    Self::$b(it) => it.read(),
                }
            }

            fn skip_to(
                &mut self,
                doc_id: DocId,
            ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
                match self {
                    Self::$a(it) => it.skip_to(doc_id),
                    Self::$b(it) => it.skip_to(doc_id),
                }
            }

            fn revalidate(
                &mut self,
                spec: &IndexSpecReadGuard,
            ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
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

            fn current_max_score(&self, ctx: &ScoreBoundCtx) -> f64 {
                match self {
                    Self::$a(it) => it.current_max_score(ctx),
                    Self::$b(it) => it.current_max_score(ctx),
                }
            }
        }
    };
}

forward_either!(TermOrFilter, Term, Filter);
forward_either!(TermOrNot, Term, Not);
