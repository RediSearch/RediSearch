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

use crate::{IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

use ffi::t_docId;
use inverted_index::RSIndexResult;

/// Yields documents appearing in ALL child iterators using a merge (AND) algorithm.
///
/// Children are sorted by estimated result count (smallest first) to minimize iterations,
/// unless `in_order` is set (which preserves the original child order for positional checks).
/// A document is only yielded when ALL children have a matching entry for it and the
/// term positions satisfy the `max_slop` / `in_order` proximity constraints:
///
/// - `max_slop`: Maximum allowed slop (distance) between term positions. `None` disables proximity
///   validation entirely.
/// - `in_order`: When `true`, terms must appear in the same order as the child iterators.
pub struct Intersection<'index, I> {
    /// Child iterators, sorted by estimated count (smallest first) unless `in_order` is set.
    children: Vec<I>,
    /// Last doc_id successfully found in ALL children (returned by [`last_doc_id()`](Self::last_doc_id)).
    last_doc_id: t_docId,
    num_expected: usize,
    is_eof: bool,
    /// Maximum allowed slop (distance) between term positions. `None` disables proximity
    /// validation entirely.
    max_slop: Option<u32>,
    /// When `true`, terms must appear in the same order as the child iterators.
    in_order: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

/// Result of attempting to get all children to agree on a target document ID.
enum AgreeResult {
    /// All children have the target doc_id as their current position.
    Agreed,
    /// A child skipped past the target to a higher doc_id; consensus must restart from this new ID.
    Ahead(t_docId),
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
    pub fn push_child(&mut self, child: I) {
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
        mut target: t_docId,
    ) -> Result<Option<t_docId>, RQEIteratorError> {
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
    fn read_from_first_child(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
        match self.children[0].read()? {
            Some(r) => Ok(Some(r.doc_id)),
            None => {
                self.is_eof = true;
                Ok(None)
            }
        }
    }

    /// Tries to get all children to agree on the target doc_id.
    fn agree_on_doc_id(&mut self, target: t_docId) -> Result<AgreeResult, RQEIteratorError> {
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
    fn find_consensus(&mut self, mut target: t_docId) -> Result<Option<t_docId>, RQEIteratorError> {
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
    /// - Restructure [`RSAggregateResult`](inverted_index::RSAggregateResult) to not require `'index` on stored references
    /// - Use a different aggregate pattern that doesn't store child references
    fn build_aggregate_result(&mut self, doc_id: t_docId) {
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
        doc_id: t_docId,
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
    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let mut any_child_moved = false;
        let mut max_child_doc_id: t_docId = 0;
        let mut moved_to_eof = false;

        for child in &mut self.children {
            // SAFETY: Delegating to child with the same `spec` passed by our caller.
            match unsafe { child.revalidate(spec) }? {
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
