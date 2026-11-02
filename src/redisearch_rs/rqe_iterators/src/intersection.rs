/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Intersection`].
//!
//! The intersection iterator supports proximity constraints via two parameters:
//! - `max_slop`: Maximum allowed slop between term positions (`None` = no constraint)
//! - `in_order`: Require terms to appear in order

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place, suspend_child_slot_in_place},
    composite::{FreeSuspendedShell, repush_borrowed_entries},
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

/// Yields documents appearing in ALL child iterators using a merge (AND) algorithm.
///
/// Parameterised over a [`Ref`] mode — see [`Intersection`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
///
/// Children are sorted by estimated result count (smallest first) to minimize iterations,
/// unless `in_order` is set (which preserves the original child order for positional checks).
/// A document is only yielded when ALL children have a matching entry for it and the
/// term positions satisfy the `max_slop` / `in_order` proximity constraints:
///
/// - `max_slop`: Maximum allowed slop (distance) between term positions. `None` disables proximity
///   validation entirely.
/// - `in_order`: When `true`, terms must appear in the same order as the child iterators.
///
/// # Invariants
///
/// 1. **Layout compatibility across the child swap.** `RawIntersection` is
///    `#[repr(C)]` over a `Vec` of children and a [`RawIndexResult`], so
///    `RawIntersection<Active, I>` and `RawIntersection<Suspended, I::Suspended>`
///    are layout-identical given that each child `I` and its `I::Suspended` are.
///    This is what lets [`suspend`](RQEIteratorBoxed::suspend) /
///    [`resume`](RQESuspendedIterator::resume) reuse the intersection's
///    allocation and its children buffer across the cycle.
#[repr(C)]
pub struct RawIntersection<'query, Rf: Ref, I> {
    /// Child iterators, sorted by estimated count (smallest first) unless `in_order` is set.
    children: Vec<I>,
    /// Last doc_id successfully found in ALL children (returned by [`last_doc_id()`](Self::last_doc_id)).
    last_doc_id: DocId,
    num_expected: usize,
    is_eof: bool,
    /// Maximum allowed slop (distance) between term positions. `None` disables proximity
    /// validation entirely.
    max_slop: Option<u32>,
    /// When `true`, terms must appear in the same order as the child iterators.
    in_order: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RawIndexResult<'query, Rf>,
}

/// Alias for an [`Active`] [`RawIntersection`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type Intersection<'index, I> = RawIntersection<'index, Active<'index>, I>;

// Compile-time proof of invariant 1 on `RawIntersection`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. The child elements' own compatibility is their invariant 1
// (enforced generically by the slot helpers); `result` is layout-compatible
// across `Rf` (proven in `index_result`); the remaining fields are `Rf`-free.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawIntersection<'static, Active<'static>, AChild>;
    type S = RawIntersection<'static, Suspended, SChild>;
    assert!(offset_of!(A, children) == offset_of!(S, children));
    assert!(offset_of!(A, last_doc_id) == offset_of!(S, last_doc_id));
    assert!(offset_of!(A, num_expected) == offset_of!(S, num_expected));
    assert!(offset_of!(A, is_eof) == offset_of!(S, is_eof));
    assert!(offset_of!(A, max_slop) == offset_of!(S, max_slop));
    assert!(offset_of!(A, in_order) == offset_of!(S, in_order));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Whether an intersection is unread or rewound, so its result holds no
/// document. Children positioned by a failed first read count as read.
fn is_unread<'a, Rf: Ref, R>(
    last_doc_id: DocId,
    result: &RawIndexResult<'_, Rf>,
    children: &[R],
) -> bool
where
    R: RQEIterator<'a>,
{
    last_doc_id == 0
        && result.doc_id == 0
        && result
            .as_aggregate()
            .is_some_and(|aggregate| aggregate.is_empty())
        && children.iter().all(|child| child.last_doc_id() == 0)
}

/// Whether `result` is a borrowed intersection aggregate, the only
/// representation
/// [`build_aggregate_result`](RawIntersection::build_aggregate_result) can
/// rebuild in place.
///
/// [`current`](RQEIterator::current) hands the result out mutably, so a
/// consumer may have replaced it: another kind has no child entries to
/// rebuild, and an owned aggregate cannot take borrowed ones. A borrowed
/// intersection put there passes, and is rebuilt with its own weight.
fn result_is_borrowed_intersection<Rf: Ref>(result: &RawIndexResult<'_, Rf>) -> bool {
    result.kind() == RSResultKind::Intersection
        && result
            .as_aggregate()
            .is_some_and(|aggregate| aggregate.as_borrowed().is_some())
}

/// Result of attempting to get all children to agree on a target document ID.
enum AgreeResult {
    /// All children have the target doc_id as their current position.
    Agreed,
    /// A child skipped past the target to a higher doc_id; consensus must restart from this new ID.
    Ahead(DocId),
    /// A child reached EOF; no more documents can match.
    Eof,
}

impl<'index, I> Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new intersection iterator without proximity constraints.
    ///
    /// Every document matching all children is yielded. Children are sorted by estimated result
    /// count (smallest first) to minimize iterations.
    ///
    /// - `weight`: Weight to apply to the term results.
    /// - `prioritize_union_children`: When `true`, union children are weighted by their child
    ///   count when sorting (corresponds to `RSGlobalConfig.prioritizeIntersectUnionChildren`).
    ///
    /// If `children` is empty, returns an iterator immediately at EOF.
    #[must_use]
    pub fn new(children: Vec<I>, weight: f64, prioritize_union_children: bool) -> Self {
        Self::new_with_slop_order(children, weight, prioritize_union_children, None, false)
    }

    /// Creates a new intersection iterator with proximity constraints.
    ///
    /// - `weight`: Weight to apply to the term results.
    /// - `prioritize_union_children`: When `true`, union children are weighted by their child
    ///   count when sorting (corresponds to `RSGlobalConfig.prioritizeIntersectUnionChildren`).
    /// - `max_slop`: Maximum allowed distance between term positions. `None` disables proximity
    ///   validation (every document matching all children is yielded).
    /// - `in_order`: When `true`, terms must appear in the order of the child iterators and
    ///   children are **not** re-sorted by estimated count (their order is meaningful).
    ///
    /// If `children` is empty, returns an iterator immediately at EOF.
    #[must_use]
    pub fn new_with_slop_order(
        children: Vec<I>,
        weight: f64,
        prioritize_union_children: bool,
        max_slop: Option<u32>,
        in_order: bool,
    ) -> Self {
        Self::new_sorted_by(
            children,
            weight,
            |a, b| {
                let wa = a.num_estimated() as f64
                    * a.intersection_sort_weight(prioritize_union_children);
                let wb = b.num_estimated() as f64
                    * b.intersection_sort_weight(prioritize_union_children);
                wa.total_cmp(&wb)
            },
            max_slop,
            in_order,
        )
    }

    /// Creates a new intersection iterator, sorting children with a custom comparator.
    ///
    /// Identical to [`Intersection::new_with_slop_order`] but sorts children using the provided
    /// `compare` function instead of by estimated count. Use this when the caller has a
    /// domain-specific sort key (e.g. a weighted heuristic based on iterator type).
    ///
    /// When `in_order` is `true`, sorting is skipped because child order is semantically
    /// significant for proximity checks.
    ///
    /// If `children` is empty, returns an iterator immediately at EOF.
    #[must_use]
    fn new_sorted_by(
        mut children: Vec<I>,
        weight: f64,
        compare: impl FnMut(&I, &I) -> std::cmp::Ordering,
        max_slop: Option<u32>,
        in_order: bool,
    ) -> Self {
        if !in_order {
            children.sort_by(compare);
        }
        let Some(num_expected) = children.iter().map(|c| c.num_estimated()).min() else {
            return Self {
                children,
                last_doc_id: 0,
                num_expected: 0,
                is_eof: true,
                max_slop,
                in_order,
                result: RSIndexResult::build_intersect(0).weight(weight).build(),
            };
        };
        let num_children = children.len();
        Self {
            children,
            last_doc_id: 0,
            num_expected,
            is_eof: false,
            max_slop,
            in_order,
            result: RSIndexResult::build_intersect(num_children)
                .weight(weight)
                .build(),
        }
    }

    /// Dynamically append a new child iterator.
    ///
    /// Updates `num_expected` if the new child has a lower estimate than the current minimum.
    ///
    /// # Note
    ///
    /// Unlike the constructor, this method does **not** re-sort the child list after insertion.
    ///
    /// Only valid before the first read: a child added later would sit behind
    /// the documents already yielded.
    pub fn push_child(&mut self, child: I) {
        debug_assert!(
            self.last_doc_id == 0,
            "children can only be added before iteration"
        );
        let est = child.num_estimated();
        if est < self.num_expected {
            self.num_expected = est;
        }
        self.children.push(child);
    }

    /// Returns the number of child iterators.
    pub const fn num_children(&self) -> usize {
        self.children.len()
    }

    /// Returns a shared reference to the child at `idx`.
    pub fn child_at(&self, idx: usize) -> &I {
        &self.children[idx]
    }

    /// Returns a mutable iterator over all child iterators.
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.children.iter_mut()
    }

    /// Returns `true` if the current result needs a proximity check after consensus.
    ///
    /// The check is skipped only when `max_slop` is `None` and `in_order` is `false` — the
    /// only case where every document matching all children is trivially within range.
    const fn needs_relevancy_check(&self) -> bool {
        self.max_slop.is_some() || self.in_order
    }

    /// Check if the current aggregate result satisfies the proximity constraints.
    ///
    /// Delegates to [`RSIndexResult::is_within_range`], which handles union children (e.g.
    /// stemmed/synonym expansions) by recursively merging offset positions across synonyms.
    fn current_is_relevant(&self) -> bool {
        self.result.is_within_range(self.max_slop, self.in_order)
    }

    /// Find consensus on a doc_id and verify that the result satisfies the proximity constraints.
    ///
    /// If the agreed-upon document doesn't satisfy the slop/order constraints, advances the first
    /// child and retries until a relevant result is found or EOF is reached.
    fn find_consensus_with_relevancy_check(
        &mut self,
        mut target: DocId,
    ) -> Result<Option<DocId>, RQEIteratorError> {
        loop {
            match self.find_consensus(target)? {
                Some(doc_id) => {
                    self.build_aggregate_result(doc_id);
                    if self.current_is_relevant() {
                        self.last_doc_id = doc_id;
                        return Ok(Some(doc_id));
                    }
                    // Not relevant — advance past this document and retry.
                    let Some(next) = self.read_from_first_child()? else {
                        return Ok(None);
                    };
                    target = next;
                }
                None => return Ok(None),
            }
        }
    }

    /// The document of the child furthest ahead, from which a re-seek carries
    /// on as a read would.
    ///
    /// After a child error the children can stand on different documents, so
    /// any target behind one of them would seek that child backwards.
    fn furthest_child_doc_id(&self) -> DocId {
        self.children
            .iter()
            .map(|child| child.last_doc_id())
            .max()
            .unwrap_or(0)
    }

    /// Re-seeks to `target` after revalidation, rebuilding the aggregate on the
    /// consensus found.
    fn reseek(&mut self, target: DocId) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        debug_assert!(target > self.last_doc_id, "re-seek must move forward");
        match self.skip_to(target)? {
            Some(_) => Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            }),
            None => Ok(RQEValidateStatus::Moved { current: None }),
        }
    }

    /// Settles the intersection after [`revalidate`](RQEIterator::revalidate)
    /// has walked every child.
    ///
    /// [`resume`](RQESuspendedIterator::resume) cannot call this: it has to
    /// settle the result before the cast, so it calls [`settle_after_walk`]
    /// there and [`apply_verdict`](Self::apply_verdict) after it.
    fn settle(
        &mut self,
        walk: ChildWalk,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: the two `Ref` modes of `RawIndexResult` are layout-identical
        // (the const proof in `index_result`'s core), and viewing the result as
        // suspended only weakens its pointers. The settle derives every entry
        // afresh from a live borrow of a child this intersection owns for
        // `'index`, and nothing takes `&mut` to a child before the result is
        // read back as active, except a re-seek, which rebuilds the aggregate
        // itself. `self.result` is a field disjoint from `self.children`.
        let result =
            unsafe { &mut *(&raw mut self.result).cast::<RawIndexResult<'index, Suspended>>() };
        match settle_after_walk(
            result,
            &mut self.children,
            self.last_doc_id,
            self.is_eof,
            walk,
        ) {
            Some(verdict) => self.apply_verdict(verdict),
            None => Ok(RQEValidateStatus::Aborted),
        }
    }

    /// Carries out what [`settle_after_walk`] decided.
    fn apply_verdict(
        &mut self,
        verdict: SettleVerdict,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match verdict {
            SettleVerdict::Idle | SettleVerdict::Refreshed => Ok(RQEValidateStatus::Ok),
            SettleVerdict::Ended => {
                self.is_eof = true;
                Ok(RQEValidateStatus::Moved { current: None })
            }
            // The re-seek rebuilds the aggregate the settle cleared.
            SettleVerdict::Reseek => self.reseek(self.furthest_child_doc_id()),
        }
    }

    /// Reads from the first child. Returns the doc_id or None if EOF.
    fn read_from_first_child(&mut self) -> Result<Option<DocId>, RQEIteratorError> {
        match self.children[0].read()? {
            Some(r) => Ok(Some(r.doc_id)),
            None => {
                self.is_eof = true;
                Ok(None)
            }
        }
    }

    /// Tries to get all children to agree on the target doc_id.
    fn agree_on_doc_id(&mut self, target: DocId) -> Result<AgreeResult, RQEIteratorError> {
        for child in &mut self.children {
            // Use child's cached last_doc_id instead of maintaining a parallel array
            if child.last_doc_id() == target {
                continue;
            }

            match child.skip_to(target)? {
                None => {
                    self.is_eof = true;
                    return Ok(AgreeResult::Eof);
                }
                Some(SkipToOutcome::Found(_)) => {
                    // Child's last_doc_id is automatically updated by skip_to
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    return Ok(AgreeResult::Ahead(r.doc_id));
                }
            }
        }
        Ok(AgreeResult::Agreed)
    }

    /// Loops until all children agree on a doc_id, or EOF is reached.
    fn find_consensus(&mut self, mut target: DocId) -> Result<Option<DocId>, RQEIteratorError> {
        loop {
            match self.agree_on_doc_id(target)? {
                AgreeResult::Agreed => return Ok(Some(target)),
                AgreeResult::Ahead(new_target) => target = new_target,
                AgreeResult::Eof => return Ok(None),
            }
        }
    }

    /// Builds the aggregate result from all children's current results.
    ///
    /// # Why `unsafe` is used here
    ///
    /// We attempted to solve this with safe split borrows (standalone function taking
    /// `&mut self.result` and `&mut self.children` separately), but it doesn't work due
    /// to a fundamental lifetime mismatch:
    ///
    /// - [`push_borrowed`](RSIndexResult::push_borrowed) requires `&'index RSIndexResult` - a reference with `'index` lifetime
    /// - [`child.current()`](RQEIterator::current) returns `&mut RSIndexResult<'index>` - the *data* has `'index`
    ///   lifetime, but the *reference* is bounded by `&mut self`
    ///
    /// The compiler cannot verify that children's internal results live for `'index`,
    /// even though we know they reference index data that does. Splitting the borrow
    /// doesn't help because [`current()`](RQEIterator::current) still returns a reference bounded by the call.
    ///
    /// # TODO
    ///
    /// Explore removing the unsafe code by using one of the following alternatives (as suggested in the PR):
    ///
    /// - Store owned copies instead of borrowed references (memory/perf tradeoff)
    /// - Restructure [`RSAggregateResult`](index_result::RSAggregateResult) to not require `'index` on stored references
    /// - Use a different aggregate pattern that doesn't store child references
    fn build_aggregate_result(&mut self, doc_id: DocId) {
        // Reset all per-document accumulating fields before building the new aggregate.
        self.result.freq = 0;
        self.result.field_mask = 0;
        self.result.metrics.reset();
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = doc_id;

        for child in &mut self.children {
            if let Some(child_result) = child.current() {
                let child_metrics = std::mem::take(&mut child_result.metrics);
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY:
                // - `child_ptr` was derived from a valid `&mut RSIndexResult`, so it
                //   is aligned and points to initialized memory.
                // - The mutable borrow from `child.current()` ends when coerced to a
                //   raw pointer, so no mutable reference to this data is live.
                // - The underlying data has `'index` lifetime because children own
                //   index-backed results; children are owned by `self` and outlive
                //   this call.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref, child_metrics);
            }
        }
    }
}

/// Outcome of [`new_intersection_iterator`].
pub enum NewIntersectionIterator<I> {
    /// One child was empty — the intersection is trivially empty.
    /// All other children have already been dropped.
    Empty,
    /// Exactly one child survived (or all children were wildcards —
    /// the last one is returned). No intersection wrapper is needed.
    Single(I),
    /// Two or more real, non-wildcard children: build a full [`Intersection`].
    Proceed(Vec<I>),
}

/// Reduce `children` before constructing an intersection.
///
/// 0. No children → `Empty`.
/// 1. Strip wildcards. If all were wildcards → `Single(last_wildcard)`. Any wildcard would be
///    correct (they all match every document); we keep the last as a natural artifact of the
///    iteration that overwrites `last_wildcard` on each new wildcard seen.
/// 2. Any empty child → `Empty` (all others are dropped).
/// 3. Exactly one non-wildcard child → `Single(child)`.
/// 4. Two or more real children → `Proceed(children)`.
pub fn new_intersection_iterator<'index, I>(children: Vec<I>) -> NewIntersectionIterator<I>
where
    I: RQEIterator<'index>,
{
    // Rule 0
    if children.is_empty() {
        return NewIntersectionIterator::Empty;
    }

    // Rule 1: strip wildcards, keep track of the last one seen before any non-wildcard
    let mut last_wildcard: Option<I> = None;
    let mut kept: Vec<I> = Vec::with_capacity(children.len());
    let mut seen_non_wildcard = false;

    for child in children {
        if matches!(
            child.type_(),
            IteratorType::Wildcard | IteratorType::InvIdxWildcard
        ) {
            if seen_non_wildcard {
                drop(child); // wildcard after non-wildcard: discard immediately
            } else {
                last_wildcard = Some(child); // Drop evicts the previous wildcard
            }
        } else {
            seen_non_wildcard = true;
            if let Some(wc) = last_wildcard.take() {
                drop(wc); // first non-wildcard: discard saved wildcard
            }
            kept.push(child);
        }
    }

    if kept.is_empty() {
        // All were wildcards
        return match last_wildcard {
            Some(wc) => NewIntersectionIterator::Single(wc),
            None => NewIntersectionIterator::Empty, // defensive: empty input already handled above
        };
    }

    // Rule 2: empty child detection
    if kept.iter().any(|c| c.type_() == IteratorType::Empty) {
        // Drop all kept children (including the empty one); intersection is empty
        return NewIntersectionIterator::Empty;
    }

    // Rule 3
    if kept.len() == 1 {
        return NewIntersectionIterator::Single(kept.remove(0));
    }

    // Rule 4
    NewIntersectionIterator::Proceed(kept)
}

impl<'index, I> RQEIterator<'index> for Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (!self.is_eof).then_some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        let Some(target) = self.read_from_first_child()? else {
            return Ok(None);
        };
        // FIXME: consider using function pointers to remove runtime checks for each reads.
        if self.needs_relevancy_check() {
            match self.find_consensus_with_relevancy_check(target)? {
                Some(_) => Ok(Some(&mut self.result)),
                None => Ok(None),
            }
        } else {
            match self.find_consensus(target)? {
                Some(doc_id) => {
                    self.build_aggregate_result(doc_id);
                    self.last_doc_id = doc_id;
                    Ok(Some(&mut self.result))
                }
                None => Ok(None),
            }
        }
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }
        // FIXME: consider using function pointers to remove runtime checks for each reads.
        if self.needs_relevancy_check() {
            // Try to agree on the requested doc_id first.
            match self.find_consensus(doc_id)? {
                Some(found_id) => {
                    self.build_aggregate_result(found_id);
                    let self_current_is_relevant = self.current_is_relevant();
                    if found_id == doc_id && self_current_is_relevant {
                        self.last_doc_id = found_id;
                        return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                    }
                    // Either we landed on a different doc, or the exact match wasn't relevant.
                    // In both cases, find the next relevant result.
                    let next_target = if self_current_is_relevant {
                        // Consensus on a different doc_id that IS relevant — return it.
                        self.last_doc_id = found_id;
                        return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
                    } else {
                        // Not relevant — advance past this document.
                        match self.read_from_first_child()? {
                            Some(t) => t,
                            None => return Ok(None),
                        }
                    };
                    match self.find_consensus_with_relevancy_check(next_target)? {
                        Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
                        None => Ok(None),
                    }
                }
                None => Ok(None),
            }
        } else {
            match self.find_consensus(doc_id)? {
                Some(found_id) => {
                    self.build_aggregate_result(found_id);
                    self.last_doc_id = found_id;
                    if found_id == doc_id {
                        Ok(Some(SkipToOutcome::Found(&mut self.result)))
                    } else {
                        Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
                    }
                }
                None => Ok(None),
            }
        }
    }

    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        self.is_eof = self.children.is_empty();
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.children.iter_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_expected
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let mut walk = ChildWalk::Unchanged;
        for child in &mut self.children {
            match child.revalidate(spec)? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Moved { current } => walk = walk.and_moved(current.is_none()),
                RQEValidateStatus::Ok => {}
            }
        }
        self.settle(walk)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Intersect
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0 / self.children.len().max(1) as f64
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Intersection<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawIntersection<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Each child goes through its own `suspend`, so a type-erased child
        // swaps its vtable; the inner iterator's heap address is preserved
        // either way. The buffer pointer and length are read up front so no
        // `&mut Vec<I>` stays live while the slots are retyped.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
            // initialised, exclusively owned); the borrow is used only to reach
            // the buffer pointer and length.
            let children: &mut Vec<I> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: `slot` holds a valid, owned `I`; the helper leaves it in
            // a valid `I::Suspended` state.
            unsafe { suspend_child_slot_in_place(slot) };
        }
        // SAFETY: every slot now holds its suspended child at its original
        // address, so the aggregate's entries still point at live results; the
        // other fields are `Rf`-free. The two forms are layout-identical by
        // invariant 1 on `RawIntersection`, and `Box::from_raw` reuses the
        // allocation, so every interior address survives the cycle.
        unsafe { Box::from_raw(raw as *mut RawIntersection<'index, Suspended, I::Suspended>) }
    }
}

/// How an intersection's children came through revalidation or resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildWalk {
    /// Every child stayed where it stood.
    Unchanged,
    /// At least one child moved, and none ran past its end.
    Moved,
    /// At least one child moved past its end, which ends the intersection.
    MovedToEof,
}

impl ChildWalk {
    /// Folds in one more child that reported a move, `at_eof` saying whether
    /// the move carried it past its end.
    const fn and_moved(self, at_eof: bool) -> Self {
        match self {
            Self::MovedToEof => Self::MovedToEof,
            _ if at_eof => Self::MovedToEof,
            _ => Self::Moved,
        }
    }
}

/// What an intersection does once every child has been revalidated or
/// resumed, as decided by [`settle_after_walk`].
#[must_use = "the verdict decides what the intersection publishes"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettleVerdict {
    /// The intersection publishes nothing: it is spent, or unread with every
    /// child unchanged.
    Idle,
    /// A child ran past its end, so no document can be agreed on again and
    /// the intersection ends.
    Ended,
    /// Every child sits on the document last yielded and has an entry again.
    Refreshed,
    /// At least one child no longer sits on the document last yielded, so the
    /// intersection must re-seek rather than publish the aggregate.
    Reseek,
}

/// Decide what an intersection does once every child has been revalidated or
/// resumed, and leave its result safe to re-narrow.
///
/// Shared by [`revalidate`](RQEIterator::revalidate) and
/// [`resume`](RQESuspendedIterator::resume). Runs after the child walk, so a
/// child's error or abort wins over a substitute put there through
/// [`current`](RQEIterator::current). A child may report a move without
/// leaving its position, so the aggregate is refreshed rather than re-sought
/// whenever every child still backs the document last yielded.
///
/// A substitute is replaced by an empty intersection carrying its weight when
/// nothing is published or the intersection ends, since its suspended pointers
/// cannot be re-validated; otherwise it aborts the intersection, and `None` is
/// returned.
///
/// Entries are positional, as
/// [`build_aggregate_result`](RawIntersection::build_aggregate_result) pushes
/// them, so [`repush_borrowed_entries`] is offered every child in slot order.
/// They are rebuilt for `last_doc_id` rather than the result's `doc_id`, which
/// a substitute may have changed.
fn settle_after_walk<'a, R>(
    result: &mut RawIndexResult<'a, Suspended>,
    children: &mut [R],
    last_doc_id: DocId,
    is_eof: bool,
    walk: ChildWalk,
) -> Option<SettleVerdict>
where
    R: RQEIterator<'a>,
{
    let unread = walk == ChildWalk::Unchanged && is_unread(last_doc_id, result, children);
    let verdict = if is_eof || unread {
        SettleVerdict::Idle
    } else if walk == ChildWalk::MovedToEof {
        SettleVerdict::Ended
    } else if !result_is_borrowed_intersection(result) {
        return None;
    } else {
        SettleVerdict::Refreshed
    };

    if !result_is_borrowed_intersection(result) {
        *result = RawIndexResult::build_intersect(children.len())
            .weight(result.weight)
            .build();
    }
    // Copied out because the aggregate borrows `result` whole.
    let (mut freq, mut field_mask) = (result.freq, result.field_mask);
    let aggregate = result
        .as_aggregate_mut()
        .and_then(|aggregate| aggregate.as_borrowed_mut())
        .expect("a substitute was replaced or refused above");
    if verdict != SettleVerdict::Refreshed {
        // Nothing is published until a read rebuilds the aggregate.
        aggregate.reset();
        return Some(verdict);
    }

    let backing = repush_borrowed_entries::<_, false>(
        aggregate,
        last_doc_id,
        &mut freq,
        &mut field_mask,
        children.iter_mut(),
    );
    let verdict = if backing == children.len() {
        SettleVerdict::Refreshed
    } else {
        // The re-seek moves children before it rebuilds the aggregate, which
        // would leave these entries stale.
        aggregate.reset();
        SettleVerdict::Reseek
    };
    result.freq = freq;
    result.field_mask = field_mask;
    result.doc_id = last_doc_id;
    Some(verdict)
}

impl<'query, S> RQESuspendedIterator<'query> for RawIntersection<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = Intersection<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let last_doc_id = self.last_doc_id;
        let is_eof = self.is_eof;
        let raw = Box::into_raw(self);

        // Resume every child in place, walking them as `revalidate` does. Slots
        // never move, so the entries' addresses stay right, but their
        // provenance does not survive a child's transition: they are rebuilt or
        // cleared below, before the cast.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
            // initialised, exclusively owned); the borrow is used only to reach
            // the buffer pointer and length.
            let children: &mut Vec<S> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        // SAFETY: `raw` is exclusively owned and non-null, so projecting to its
        // `children` field is in bounds; no reference is created here.
        let children_ptr = unsafe { &raw mut (*raw).children };
        // Until the cast, `shell` owns the part-resumed buffer, so every early
        // return and any panic frees it through the guard's drop.
        //
        // SAFETY: `raw` came from `Box::into_raw` and passes to the guard,
        // `children_ptr` is its children `Vec`, and once that is emptied the
        // rest drops as a suspended intersection.
        let mut shell =
            unsafe { FreeSuspendedShell::<_, S, S::Resumed<'a>>::new(raw, children_ptr) };

        let mut walk = ChildWalk::Unchanged;
        for _ in 0..len {
            // Vacated before the call: on `Aborted`/`Err` the helper has
            // consumed the child, and the guard must not drop it.
            let i = shell.vacate_next();
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it
            // on `Aborted`/`Err`, which return through `shell`'s drop.
            match unsafe { resume_child_slot_in_place(elem, guard) }? {
                ResumeSlotOutcome::Unchanged => {}
                ResumeSlotOutcome::Moved => {
                    // `revalidate` reads the EOF from its `Moved` payload; here
                    // it is read off the resumed child.
                    //
                    // SAFETY: the helper just rewrote the slot as a valid
                    // `S::Resumed<'a>`.
                    let resumed = unsafe { &*elem.cast::<S::Resumed<'a>>().cast_const() };
                    walk = walk.and_moved(resumed.at_eof());
                }
                ResumeSlotOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
            }
            // SAFETY: slot `i`, the one last vacated, now holds a valid, owned
            // resumed child: the helper rewrote it on `Unchanged`/`Moved`, and
            // `Aborted` returned above. No slot was dropped before it, so it is
            // kept where it is.
            unsafe { shell.keep_last_vacated() };
        }

        let verdict = {
            // `&mut` is invariant, so narrowing `'query` to `'a` takes a cast;
            // it ties the rebuilt entries to the children's lifetime.
            //
            // SAFETY: `raw` is exclusively owned and non-null, so projecting to
            // its `result` field is in bounds; no reference is created here.
            let result_ptr = unsafe { &raw mut (*raw).result };
            // SAFETY: the projection is valid, aligned and initialised, disjoint
            // from the children buffer, and borrowed only within this block.
            // `'query: 'a`, so every pointer the result claims is valid for `'a`.
            let result: &mut RawIndexResult<'a, Suspended> =
                unsafe { &mut *result_ptr.cast::<RawIndexResult<'a, Suspended>>() };
            debug_assert_eq!(shell.kept(), len, "every slot must hold a resumed child");
            // SAFETY: the loop above rewrote all `len` slots as valid
            // `S::Resumed<'a>`, laid out as `S` (enforced by
            // `resume_child_slot_in_place`), and `raw` is exclusively owned,
            // so the slice is unaliased.
            let children =
                unsafe { std::slice::from_raw_parts_mut(base.cast::<S::Resumed<'a>>(), len) };
            match settle_after_walk(result, children, last_doc_id, is_eof, walk) {
                Some(verdict) => verdict,
                // A substitute's suspended pointers would be re-narrowed
                // unchecked by the cast; `shell` frees the intersection.
                None => return Ok(ResumeOutcome::Aborted),
            }
        };

        // Disarmed only now: an unwind before the cast must go through the guard.
        shell.disarm();

        // SAFETY: every slot holds its resumed child at its original address,
        // and the result is an intersection whose entries were rebuilt from
        // those children or cleared; the other fields are `Rf`-free.
        // Layout-identical by invariant 1 on `RawIntersection`. Reusing the
        // allocation keeps the FFI's cached `header.current` and any parent's
        // pointer into `result` valid.
        let mut active = unsafe { Box::from_raw(raw.cast::<Intersection<'a, S::Resumed<'a>>>()) };

        match active.apply_verdict(verdict)? {
            RQEValidateStatus::Ok => Ok(ResumeOutcome::Ok(active)),
            RQEValidateStatus::Moved { .. } => Ok(ResumeOutcome::Moved(active)),
            RQEValidateStatus::Aborted => unreachable!("applying a verdict never aborts"),
        }
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        self.num_expected
    }
}

impl<'index> crate::interop::ProfileChildren<'index>
    for Intersection<'index, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        Intersection {
            children: self
                .children
                .into_iter()
                .map(crate::c2rust::CRQEIterator::into_profiled)
                .collect(),
            last_doc_id: self.last_doc_id,
            num_expected: self.num_expected,
            is_eof: self.is_eof,
            max_slop: self.max_slop,
            in_order: self.in_order,
            result: self.result,
        }
    }
}

impl<'index, I> ProfilePrint for Intersection<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"INTERSECT");
        ctx.print_optional_counters(map);

        let num_children = self.num_children();
        let mut arr = map.kv_array(c"Child iterators");
        for i in 0..num_children {
            let mut child_map = arr.map();
            let mut child_ctx = ctx.child_ctx();
            self.child_at(i)
                .print_profile(&mut child_map, &mut child_ctx);
        }
    }
}
