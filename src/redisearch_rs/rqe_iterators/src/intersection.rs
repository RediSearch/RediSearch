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
    profile_print::{ProfilePrint, ProfilePrintCtx},
    union::FreeSuspendedShell,
};

use index_result::{RSIndexResult, RSResultKind, RawBorrowedAggregateResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, FieldMask};

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

/// Result of attempting to get all children to agree on a target document ID.
enum AgreeResult {
    /// All children have the target doc_id as their current position.
    Agreed,
    /// A child skipped past the target to a higher doc_id; consensus must restart from this new ID.
    Ahead(DocId),
    /// A child reached EOF; no more documents can match.
    Eof,
}

impl<'query, Rf: Ref, I> RawIntersection<'query, Rf, I> {
    /// Returns the number of child iterators. Mode-independent.
    pub const fn num_children(&self) -> usize {
        self.children.len()
    }

    /// Returns a shared reference to the child at `idx`. Mode-independent.
    pub fn child_at(&self, idx: usize) -> &I {
        &self.children[idx]
    }
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
    pub fn push_child(&mut self, child: I) {
        let est = child.num_estimated();
        if est < self.num_expected {
            self.num_expected = est;
        }
        self.children.push(child);
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
        let mut any_child_moved = false;
        let mut max_child_doc_id: DocId = 0;
        let mut moved_to_eof = false;

        for child in &mut self.children {
            match child.revalidate(spec)? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Moved { current } => {
                    any_child_moved = true;
                    match current {
                        Some(result) => {
                            // Child's last_doc_id is automatically updated by revalidate
                            max_child_doc_id = max_child_doc_id.max(result.doc_id);
                        }
                        None => moved_to_eof = true,
                    }
                }
                RQEValidateStatus::Ok => {}
            }
        }

        if !any_child_moved || self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        if moved_to_eof {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        match self.skip_to(max_child_doc_id)? {
            Some(_) => Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            }),
            None => Ok(RQEValidateStatus::Moved { current: None }),
        }
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
        // Walk children: dispatch each child's `suspend` through the trait so
        // dyn-erased `I` (e.g. [`TypeErasedRQEIterator`](crate::TypeErasedRQEIterator),
        // whose active and suspended forms carry different vtables) correctly
        // transitions. For concrete-typed `I` this is the same whole-box cast
        // that the outer composite would otherwise have done, just per-child
        // instead of per-composite. Either way the inner concrete iterator's
        // heap address is preserved; only the wrapper bytes (which nothing
        // external points at) are re-typed.
        //
        // The buffer pointer and length are taken up front, mirroring `resume`:
        // no typed `&mut Vec<I>` stays live across the loop that retypes the
        // slots it would describe.
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
        // SAFETY: every child slot now holds its suspended form at its original
        // address, so the aggregate's entries — pointers into those slots'
        // result objects — still refer to live objects; the remaining fields
        // are `Rf`-free. Layout-identical to the active form by invariant 1 on
        // `RawIntersection` (const proof above), whose child-slot half — the
        // `I`/`I::Suspended` size and alignment match — is statically enforced
        // by `suspend_child_slot_in_place`. `Box::from_raw` reuses the same
        // allocation, so the box and every interior address survive the cycle.
        unsafe { Box::from_raw(raw as *mut RawIntersection<'index, Suspended, I::Suspended>) }
    }
}

/// Whether every child still backs the rebuilt aggregate.
///
/// An intersection publishes a document only when *all* of its children agree on
/// it, so — unlike a union, which is content with one — anything short of a full
/// house means the result no longer describes a consensus.
///
/// `#[must_use]` sits on the type so it covers every producer: an
/// [`Incomplete`](Self::Incomplete) that nobody looks at is a consensus
/// published without the children that were supposed to have reached it.
#[must_use = "an incomplete aggregate no longer describes a consensus; the \
              caller must rebuild it or abort the resume"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildOutcome {
    /// Every child answered, so the aggregate borrows all of them. The result
    /// stands as a description of its document, and re-narrowing it is sound.
    Complete,
    /// At least one child no longer publishes a result on the intersection's
    /// document, so the aggregate came out short. Re-narrowing it is still sound
    /// — every entry it does hold was taken from a live child — but it no longer
    /// describes a consensus, so the intersection must rebuild it before
    /// publishing it or report [`ResumeOutcome::Aborted`] instead.
    Incomplete,
}

/// Rebuild a suspended intersection's borrowed entries from its just-resumed
/// children.
///
/// The intersection counterpart of the union's
/// [`rebuild_borrowed_entries`](crate::union::rebuild_borrowed_entries), which
/// owns the shared contract: why the entries must be written afresh before the
/// cast, why `metrics` are carried across while `freq` and `field_mask` are
/// recomputed, and why it has to be called last, with no `&mut` taken to a
/// child afterwards. Only what differs for an intersection is documented here.
///
/// # Positional, not matched
///
/// An intersection publishes a document only once
/// [`agree_on_doc_id`](RawIntersection::agree_on_doc_id) reports that *every*
/// child sits on it, so
/// [`build_aggregate_result`](RawIntersection::build_aggregate_result) pushes
/// one entry per child, in slot order, and a child never leaves its slot — an
/// aborting child aborts the whole intersection rather than being compacted
/// over. Entry *i* is child *i*, by construction, for as long as the result
/// lives, so every child is offered and pushed back in slot order.
///
/// # Complete or incomplete
///
/// Where a union is content with one contributor, an intersection needs all of
/// them: a child that no longer publishes a result on `doc_id` leaves the
/// aggregate short, which is reported as [`RebuildOutcome::Incomplete`].
fn rebuild_borrowed_entries<'a, R>(
    aggregate: &mut RawBorrowedAggregateResult<'a, Suspended>,
    doc_id: DocId,
    freq: &mut u32,
    field_mask: &mut FieldMask,
    children: &mut [R],
) -> RebuildOutcome
where
    R: RQEIterator<'a>,
{
    if aggregate.is_empty() && doc_id == 0 {
        // The intersection was suspended before its first read (or right after
        // a rewind): an empty aggregate on document 0, which no child would
        // answer for, so there is nothing to rebuild. An empty aggregate on a
        // real document is a different matter: the intersection only ever
        // publishes one entry per child, so it was emptied through `current()`
        // and falls through to be rebuilt like any other.
        return RebuildOutcome::Complete;
    }

    // Drops the records and the kind mask; `push_borrowed_ptr_from_ref` rebuilds
    // both, entry by entry. Deliberately not `reset_aggregate`, which would also
    // take `doc_id` and the metrics, which cannot be put back. See the union's
    // `rebuild_borrowed_entries`.
    aggregate.reset();
    *freq = 0;
    *field_mask = 0;

    let mut backing = 0;
    for child in children.iter_mut() {
        // A child at EOF publishes no result, and one whose own resume carried
        // it off the agreed document no longer backs the consensus. Either way
        // the aggregate comes out short, which is what the caller is told.
        let Some(current) = child.current() else {
            continue;
        };
        if current.doc_id != doc_id {
            continue;
        }
        // The entry is taken from a shared reborrow, so the tag it carries stays
        // valid for as long as nothing takes `&mut` to this child again. The
        // loop only ever moves on to a *different* child, and the intersection
        // takes none after this call returns.
        let current: &RSIndexResult<'a> = &*current;

        *freq += current.freq;
        *field_mask |= current.field_mask;
        aggregate.push_borrowed_ptr_from_ref(current);
        backing += 1;
    }

    if backing == children.len() {
        RebuildOutcome::Complete
    } else {
        RebuildOutcome::Incomplete
    }
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
        // `current()` hands the result out mutably, so "still the intersection's
        // own aggregate" is a runtime invariant rather than an enforced one — a
        // consumer could have replaced it with a result the intersection never
        // built, and the cast would then re-narrow whatever suspended pointers
        // the substitute holds without re-validating them. An intersection has
        // no way to re-validate a payload it did not build, so it refuses, on
        // `&self`, before `Box::into_raw` opens the raw-pointer section — the
        // same shape `Optional` and `NotOptimized` use for their virtual
        // sentinels. It is also what lets `rebuild_borrowed_entries` take the
        // borrowed aggregate as given.
        //
        // The kind alone is not enough. An aggregate of kind `Intersection` can
        // be the *owned* representation, whose children are boxed into the
        // result's own allocation and are never transitioned. Safe code can
        // build one — `to_owned()` on an intersection result, `push_boxed` an
        // index-backed term into it, assign it through `current()` — and a boxed
        // child's `data` can be index-backed and suspended, so re-narrowing it
        // would promote pointers nothing re-validated. Demanding the borrowed
        // representation is what closes that.
        let is_borrowed_intersection = self.result.kind() == RSResultKind::Intersection
            && self
                .result
                .as_aggregate()
                .is_some_and(|aggregate| aggregate.as_borrowed().is_some());
        if !is_borrowed_intersection {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);

        // Resume every child *in place*, mirroring `Intersection::revalidate`'s
        // walk: any child aborting aborts the whole intersection (an
        // intersection missing a child yields nothing); otherwise track whether
        // anything moved and the furthest live position.
        //
        // The children never leave their slots, so the suspended aggregate's
        // weakened pointers still designate their result objects afterwards —
        // but only the addresses survive a child's transition, so the entries are
        // re-derived from the resumed children below before the box is
        // reinterpreted, or cleared for the re-seek to rebuild; their *contents*
        // can be stale too, and the revalidate-mirroring decisions further down
        // re-derive those.
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
        // From here until the cast, the buffer is part-resumed and part-suspended
        // and `shell` owns it: every early return below, and any panic out of a
        // child's safe trait methods or the rebuild, frees it through the
        // guard's drop rather than through a call the exit has to remember to
        // make. An intersection never compacts, so the guard's resumed prefix
        // always ends where the vacated slot, if any, begins.
        //
        // SAFETY: `raw` came from `Box::into_raw` and passes to the guard,
        // `children_ptr` is its children `Vec`, and with that emptied the rest
        // of the shell (a still-suspended `result` and `Rf`-free scalars) drops
        // as a suspended intersection.
        let mut shell =
            unsafe { FreeSuspendedShell::<_, S, S::Resumed<'a>>::new(raw, children_ptr) };

        let mut any_child_moved = false;
        let mut max_child_doc_id: DocId = 0;
        let mut moved_to_eof = false;
        for i in 0..len {
            // Slot `i` counts as vacated for as long as the helper owns its
            // contents, so the boundary moves *before* the call: on
            // `Aborted`/`Err` the child is consumed and the guard must already
            // know not to drop it.
            shell.cursor = i + 1;
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it
            // on `Aborted`/`Err`, which return through `shell`'s drop.
            match unsafe { resume_child_slot_in_place(elem, guard) }? {
                ResumeSlotOutcome::Unchanged => shell.kept = i + 1,
                ResumeSlotOutcome::Moved => {
                    shell.kept = i + 1;
                    any_child_moved = true;
                    // `revalidate` reads the post-move position out of the
                    // `RQEValidateStatus::Moved { current }` payload; the slot
                    // helper hands back no result, so it is read off the resumed
                    // child instead. The two are equivalent by the
                    // `current()`/`at_eof()` contract: `current()` is `None`
                    // exactly when the child is at EOF.
                    //
                    // Testing `at_eof()` *before* `last_doc_id()` is what makes
                    // that equivalence hold. A child whose re-seek ran past the
                    // end reports `last_doc_id() == 0`, so reading the position
                    // first would fold it into `max_child_doc_id` as "moved
                    // nowhere" instead of ending the intersection.
                    //
                    // SAFETY: the helper just rewrote the slot as a valid
                    // `S::Resumed<'a>`.
                    let resumed = unsafe { &*elem.cast::<S::Resumed<'a>>().cast_const() };
                    if resumed.at_eof() {
                        moved_to_eof = true;
                    } else {
                        max_child_doc_id = max_child_doc_id.max(resumed.last_doc_id());
                    }
                }
                ResumeSlotOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
            }
        }

        // Only the `!any_child_moved` exit below publishes the aggregate without
        // a re-seek. On every other path the re-seek moves children before
        // rebuilding the aggregate itself, which would leave any entry derived
        // here stale, so those paths only clear the records. `freq` and
        // `field_mask` are left for the same rebuild to reset; an intersection
        // that ends at EOF publishes no `current()` to read them.
        //
        // Where it does run, every child is offered and every child is expected
        // to answer: an intersection publishes a document only once all of them
        // agree on it, so anything short of a full house is a consensus it can
        // no longer show.
        let rebuilt = {
            // Viewed at `'a` rather than at `'query`. Narrowing a query lifetime
            // is the safe direction — `'query: 'a` — but `&mut` is invariant, so
            // the compiler will not do it for us and the pointer is re-cast
            // instead. It is what lets the rebuild tie each entry to the same
            // lifetime the children carry, rather than widening theirs to
            // `'query`.
            //
            // SAFETY: `raw` is exclusively owned and non-null, so projecting to
            // its `result` field is in bounds; no reference is created here.
            let result_ptr = unsafe { &raw mut (*raw).result };
            // SAFETY: the projection above is valid, aligned and initialised,
            // and holds a suspended result in a field disjoint from the children
            // buffer; `raw` is exclusively owned and the borrow is confined to
            // this block. `RawIndexResult` differs between the two lifetimes
            // only in the query-pipeline pointers it claims, and `'query: 'a`
            // makes every one of them valid for `'a`.
            let result: &mut RawIndexResult<'a, Suspended> =
                unsafe { &mut *result_ptr.cast::<RawIndexResult<'a, Suspended>>() };
            let doc_id = result.doc_id;
            // `freq` and `field_mask` sit beside the aggregate in `result`, which
            // it borrows whole, so they are rebuilt in copies and written back.
            let (mut freq, mut field_mask) = (result.freq, result.field_mask);
            let aggregate = result
                .as_aggregate_mut()
                .and_then(|aggregate| aggregate.as_borrowed_mut())
                .expect("resume admits only a borrowed intersection aggregate");
            if any_child_moved {
                aggregate.reset();
                None
            } else {
                // SAFETY: `base` addresses `len` consecutive slots, each rewritten
                // by the loop above as a valid `S::Resumed<'a>` — same size and
                // alignment as the `S` the buffer was allocated for, statically
                // enforced by `resume_child_slot_in_place` — and `raw` is
                // exclusively owned, so the slice is unaliased.
                let children =
                    unsafe { std::slice::from_raw_parts_mut(base.cast::<S::Resumed<'a>>(), len) };
                let rebuilt = rebuild_borrowed_entries(
                    aggregate,
                    doc_id,
                    &mut freq,
                    &mut field_mask,
                    children,
                );
                result.freq = freq;
                result.field_mask = field_mask;
                Some(rebuilt)
            }
        };

        // Disarmed only now: until the cast the elements are typed as suspended
        // while holding resumed children, so an unwind must go through the guard.
        shell.disarm();

        // SAFETY: every child slot holds its resumed form at its original
        // address, and the aggregate's entries have just been rebuilt from
        // those resumed children or cleared, so no entry is re-narrowed onto a
        // merely retagged result; the remaining fields are `Rf`-free.
        // Layout-identical to the suspended form by invariant 1 on
        // `RawIntersection` (const proof above). `Box::from_raw` reuses the same
        // allocation, so the FFI's cached `header.current` and any parent's
        // pointer into `result` stay valid across the cycle.
        let mut active = unsafe { Box::from_raw(raw.cast::<Intersection<'a, S::Resumed<'a>>>()) };

        // From here on, mirror `revalidate` decision for decision. Nothing moved
        // (or the intersection had already ended): the children still sit on the
        // positions the aggregate describes, so it stands as-is.
        if !any_child_moved || active.is_eof {
            // Unless it came out short. An incomplete aggregate keeps the
            // position but can no longer show every child behind it, and this
            // is the one path that hands the result straight back — there is no
            // re-seek below it to rebuild what came out short. An intersection
            // that cannot account for one of its children could not have agreed
            // on the document anyway, so it aborts rather than publish a
            // consensus it can no longer show. (An intersection already at EOF
            // publishes no `current` — so neither a short aggregate nor one
            // cleared above has an observer there, and `revalidate`'s `Ok` is
            // still the faithful answer.)
            //
            // Reachable with conforming children: a child error in the middle
            // of a consensus search leaves the intersection off EOF, with an
            // aggregate for the previous document and the children that had
            // already advanced sitting past it.
            if rebuilt == Some(RebuildOutcome::Incomplete) && !active.is_eof {
                return Ok(ResumeOutcome::Aborted);
            }
            return Ok(ResumeOutcome::Ok(active));
        }

        // At least one child moved past its end — the intersection can't continue.
        // `is_eof` suppresses `current`, so the cleared aggregate stays unobserved.
        if moved_to_eof {
            active.is_eof = true;
            return Ok(ResumeOutcome::Moved(active));
        }

        // Some children moved forward. Advance to the furthest one and find the
        // next common document from there; `skip_to` republishes the aggregate
        // cleared above, and an `Err` propagates exactly as it does out of
        // `revalidate`.
        active.skip_to(max_child_doc_id)?;
        Ok(ResumeOutcome::Moved(active))
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
