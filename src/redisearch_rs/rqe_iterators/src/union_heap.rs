/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Heap variant of the union iterator with O(log n) min-finding.

use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref};
use rqe_core::DocId;

use crate::utils::DocIdMinHeap;
use crate::{IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};
use index_spec::IndexSpecReadGuard;

/// Yields documents appearing in ANY child iterator using a binary heap.
///
/// Parameterised over a [`Ref`] mode — see [`UnionHeap`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// `UnionHeap` yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated (unless `QUICK_EXIT` is `true`).
///
/// Uses O(log n) min-finding via a binary heap. Better for large numbers of children
/// (typically >20) where the heap overhead is outweighed by faster min-finding.
///
/// For small numbers of children, consider using [`crate::UnionFlat`] instead.
///
/// # Type Parameters
///
/// - `Rf`: The [`Ref`] mode.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
#[repr(C)]
pub struct RawUnionHeap<'query, Rf: Ref, I, const QUICK_EXIT: bool> {
    children: Vec<I>,
    num_estimated: usize,
    /// Number of children that have not yet reached EOF.
    ///
    /// Tracked separately from [`Self::heap`] because the heap is lazily
    /// populated on the first `read`/`skip_to` call, so `heap.len()` is 0
    /// before that even though all children are active.
    num_active: usize,
    is_eof: bool,
    /// Reused across calls to avoid allocations.
    result: RawIndexResult<'query, Rf>,
    /// Min-heap of `(doc_id, child_index)`.
    heap: DocIdMinHeap,
}

/// Alias for an [`Active`] [`RawUnionHeap`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type UnionHeap<'index, I, const QUICK_EXIT: bool> =
    RawUnionHeap<'index, Active<'index>, I, QUICK_EXIT>;

// Methods used in both modes.
impl<'index, I, const QUICK_EXIT: bool> UnionHeap<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Creates a new heap union iterator. If `children` is empty, returns an
    /// iterator immediately at EOF.
    #[must_use]
    pub fn new(children: Vec<I>) -> Self {
        let num_estimated: usize = children.iter().map(|c| c.num_estimated()).sum();
        let num_children = children.len();

        if children.is_empty() {
            return Self {
                children,
                num_estimated: 0,
                num_active: 0,
                is_eof: true,
                result: RSIndexResult::build_union(0).build(),
                heap: DocIdMinHeap::new(),
            };
        }

        Self {
            children,
            num_estimated,
            num_active: num_children,
            is_eof: false,
            result: RSIndexResult::build_union(num_children).build(),
            heap: DocIdMinHeap::with_capacity(num_children),
        }
    }

    /// Returns the total number of children (including exhausted ones).
    pub const fn num_children_total(&self) -> usize {
        self.children.len()
    }

    /// Returns the number of currently active (non-exhausted) children.
    pub const fn num_children_active(&self) -> usize {
        self.num_active
    }

    /// Returns a shared reference to the child originally at insertion index `idx`.
    ///
    /// If any child was removed, there is no guarantee that the same child will be at this position.
    /// Returns [`None`] if the child is out of range.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        self.children.get(idx)
    }

    /// Returns a mutable iterator over all children (including exhausted ones).
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.children.iter_mut()
    }

    /// Consumes the iterator and returns its children.
    pub fn into_children(self) -> Vec<I> {
        self.children
    }

    /// Consumes the iterator and returns a [`super::UnionTrimmed`] over the same children,
    /// or [`None`] if there are fewer than 3 children.
    pub fn into_trimmed(self, limit: usize, asc: bool) -> Option<super::UnionTrimmed<'index, I>> {
        (self.children.len() >= 3).then(|| super::UnionTrimmed::new(self.children, limit, asc))
    }

    /// Rebuilds the heap from scratch based on current child positions.
    /// Used after revalidation when children may have moved arbitrarily.
    ///
    /// Every child that still owes a result is pushed, and only those that have
    /// run past their last one are left out (see [`RQEIterator::at_eof`]). Leaving
    /// out a child that has merely returned its final document would lose it — and
    /// report EOF for the union outright, if it was the only one left.
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (idx, child) in self.children.iter().enumerate() {
            if !child.at_eof() {
                self.heap.push(child.last_doc_id(), idx);
            }
        }
    }

    /// Advances all lagging children in the heap to at least `doc_id`.
    ///
    /// In `QUICK_EXIT` mode, returns the child index on an exact match,
    /// leaving remaining lagging children for the next call.
    /// Returns `usize::MAX` if no exact match was found.
    fn advance_lagging_children(&mut self, doc_id: DocId) -> Result<usize, RQEIteratorError> {
        if self.heap.is_empty() {
            return Ok(usize::MAX);
        }
        loop {
            let root = self.heap.peek().unwrap();
            if root.doc_id >= doc_id {
                break;
            }

            let child = &mut self.children[root.child_idx];
            match child.skip_to(doc_id)? {
                Some(SkipToOutcome::Found(r)) => {
                    self.heap.replace_root(r.doc_id, root.child_idx);
                    if QUICK_EXIT {
                        return Ok(root.child_idx);
                    }
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    self.heap.replace_root(r.doc_id, root.child_idx);
                }
                None => {
                    self.heap.pop();
                    self.num_active -= 1;
                    if self.heap.is_empty() {
                        break;
                    }
                }
            }
        }
        Ok(usize::MAX)
    }

    /// Ensures all children are at or beyond `doc_id`.
    ///
    /// On the first call (heap empty), initializes the heap by skipping every
    /// child to the target. Otherwise delegates to [`Self::advance_lagging_children`].
    /// Returns a child index on early match, or `usize::MAX` if none.
    fn advance_to(&mut self, doc_id: DocId) -> Result<usize, RQEIteratorError> {
        if self.heap.is_empty() && self.last_doc_id() == 0 {
            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {
                        self.num_active -= 1;
                    }
                }
            }
            Ok(usize::MAX)
        } else {
            self.advance_lagging_children(doc_id)
        }
    }
}

// Methods reachable only when `QUICK_EXIT` is `false`, grouped so that the full-mode
// read path reads together. Each guards its mode at entry rather than being typed on
// `UnionHeap<'index, I, false>`: the caller is the generic `RQEIterator` impl, which
// cannot reach a method that exists for only one value of a const generic (E0599),
// and splitting that impl in two would strand its `ProfileChildren` dependent.
impl<'index, I, const QUICK_EXIT: bool> UnionHeap<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Advances the children at the heap root that are sitting on `current_id`.
    ///
    /// Only [`Self::read_full`] calls this, so the root can never be *behind*
    /// `current_id`: full-mode `read`/`skip_to` advance every child in the heap, and
    /// `revalidate` re-seeks any child a child revalidation resurrected behind the
    /// union. A root behind would stay the minimum and hand back a document already
    /// delivered, so the invariant is asserted rather than handled.
    fn advance_matching_children(&mut self, current_id: DocId) -> Result<(), RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union reads through skip_to instead");
        }

        if self.heap.is_empty() {
            return Ok(());
        }
        loop {
            let root = self.heap.peek().unwrap();
            debug_assert!(
                root.doc_id >= current_id,
                "a full union's child cannot fall behind it: {} < {current_id}",
                root.doc_id,
            );
            if root.doc_id != current_id {
                break;
            }

            let child = &mut self.children[root.child_idx];
            if child.read()?.is_some() {
                self.heap.replace_root(child.last_doc_id(), root.child_idx);
            } else {
                self.heap.pop();
                self.num_active -= 1;
                if self.heap.is_empty() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Aggregates results from all children whose `last_doc_id` equals `min_id`.
    ///
    /// Uses DFS over the heap array, pruning subtrees where `doc_id > min_id`
    /// (heap property guarantees all descendants are also `>= doc_id`).
    ///
    /// `min_id` must be the heap's own minimum. The prune reads any mismatch as
    /// "past `min_id`, and so is everything below", which only holds when nothing in
    /// the heap sorts *below* `min_id` — pointed anywhere else, the descent stops at
    /// the root and the aggregate comes back empty. Only `QUICK_EXIT == false` callers
    /// reach here, and each passes `self.heap.peek()`, so that holds by construction.
    fn build_aggregate_result(&mut self, min_id: DocId) {
        if QUICK_EXIT {
            panic!("a quick union never aggregates; it reports a single child");
        }
        debug_assert_eq!(
            self.heap.peek().map(|min| min.doc_id),
            Some(min_id),
            "the descent's prune is only valid at the heap's minimum",
        );

        self.result.reset_aggregate();
        self.result.doc_id = min_id;

        // Borrow the heap data slice once so the compiler can hoist bounds
        // checks out of the loop.
        let heap_data = self.heap.as_slice();

        if heap_data.is_empty() {
            return;
        }

        // A 64-element stack is sufficient for a binary heap of up to 2^64 elements.
        let mut stack = [0usize; 64];
        let mut stack_len = 1;
        stack[0] = 0;

        while stack_len > 0 {
            stack_len -= 1;
            let heap_idx = stack[stack_len];

            if heap_idx >= heap_data.len() {
                continue;
            }

            let entry = heap_data[heap_idx];
            if entry.doc_id != min_id {
                continue;
            }

            if let Some(child_result) = self.children[entry.child_idx].current() {
                let drained_metrics = std::mem::take(&mut child_result.metrics);
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: We need a raw pointer to decouple the borrow of the child's
                // result from `&mut self.result`. This is sound because:
                // 1. `self.children[i]` and `self.result` are disjoint fields — no aliasing.
                // 2. The child is owned by `self`, so the 'index data remains valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref, drained_metrics);
            }
            // both children of heap_idx are >= doc_id due to heap property
            let left_heap_idx = 2 * heap_idx + 1;
            let right_heap_idx = 2 * heap_idx + 2;

            if left_heap_idx < heap_data.len() && stack_len < 64 {
                stack[stack_len] = left_heap_idx;
                stack_len += 1;
            }
            if right_heap_idx < heap_data.len() && stack_len < 64 {
                stack[stack_len] = right_heap_idx;
                stack_len += 1;
            }
        }
    }

    /// Performs initial read on all children and builds the heap.
    ///
    /// Clears the heap first: a `revalidate` before the first read runs
    /// [`Self::rebuild_heap`] while every child is still at `last_doc_id() == 0`,
    /// and those doc-0 entries would otherwise sort ahead of every real id.
    fn initialize_children(&mut self) -> Result<(), RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union builds its heap through advance_to instead");
        }

        self.heap.clear();
        for (idx, child) in self.children.iter_mut().enumerate() {
            if child.last_doc_id() == 0 && !child.at_eof() {
                if child.read()?.is_some() {
                    self.heap.push(child.last_doc_id(), idx);
                }
            } else if child.last_doc_id() > 0 {
                self.heap.push(child.last_doc_id(), idx);
            }
        }
        // Derived here rather than adjusted, for the same reason the heap is:
        // a `rebuild_heap` before the first read counts only the children it kept,
        // and this pass can seed the heap with one it dropped. Leaving the count
        // alone would let the per-child decrements below run past zero.
        self.num_active = self.heap.len();
        Ok(())
    }

    /// Full mode read — advances matching children and finds minimum.
    fn read_full(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union reads through read_quick");
        }

        let previous_id = self.last_doc_id();
        if previous_id == 0 {
            self.initialize_children()?;
        } else {
            self.advance_matching_children(previous_id)?;
        }

        let Some(min) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        debug_assert!(
            min.doc_id > previous_id,
            "a read must move forward: {previous_id} -> {}",
            min.doc_id,
        );

        self.build_aggregate_result(min.doc_id);
        Ok(Some(&mut self.result))
    }
}

// Methods reachable only when `QUICK_EXIT` is `true`. See the note above.
impl<'index, I, const QUICK_EXIT: bool> UnionHeap<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Quick mode read — delegates to `skip_to(last_doc_id + 1)`.
    fn read_quick(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if !QUICK_EXIT {
            panic!("a full union reads through read_full");
        }

        let next_id = self.last_doc_id().saturating_add(1);
        match self.skip_to(next_id)? {
            Some(SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    /// Sets the union result directly from the child at `child_idx`.
    fn quick_set_from_child(&mut self, child_idx: usize) {
        if !QUICK_EXIT {
            panic!("a full union aggregates every matching child instead");
        }

        let child = &mut self.children[child_idx];

        self.result.reset_aggregate();
        self.result.doc_id = child.last_doc_id();

        if let Some(child_result) = child.current() {
            let drained_metrics = std::mem::take(&mut child_result.metrics);
            let child_ptr: *const RSIndexResult<'index> = child_result;
            // SAFETY: We need a raw pointer to decouple the borrow of the child's
            // result from `&mut self.result`. This is sound because:
            // 1. `self.children[i]` and `self.result` are disjoint fields — no aliasing.
            // 2. The child is owned by `self`, so the 'index data remains valid.
            let child_ref = unsafe { &*child_ptr };
            self.result.push_borrowed(child_ref, drained_metrics);
        }
    }
}

// ============================================================================
// RQEIterator implementation for UnionHeap
// ============================================================================

impl<'index, I, const QUICK_EXIT: bool> RQEIterator<'index> for UnionHeap<'index, I, QUICK_EXIT>
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

        if QUICK_EXIT {
            self.read_quick()
        } else {
            self.read_full()
        }
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        debug_assert!(self.last_doc_id() < doc_id);

        let early_match = self.advance_to(doc_id)?;

        // Early match found during advancement — skip the heap peek.
        if QUICK_EXIT && early_match != usize::MAX {
            self.quick_set_from_child(early_match);
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }

        let Some(min) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        if QUICK_EXIT {
            self.quick_set_from_child(min.child_idx);
        } else {
            self.build_aggregate_result(min.doc_id);
        }

        if min.doc_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    fn rewind(&mut self) {
        self.is_eof = self.children.is_empty();
        self.num_active = self.children.len();
        self.result.reset_aggregate();
        self.children.iter_mut().for_each(|c| c.rewind());
        self.heap.clear();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        let original_last_doc_id = self.last_doc_id();
        let mut any_change = false;

        // Index-based iteration: swap_remove may reorder elements.
        let mut i = 0;
        while i < self.children.len() {
            match self.children[i].revalidate(spec)? {
                RQEValidateStatus::Aborted => {
                    self.children.swap_remove(i);
                    any_change = true;
                }
                RQEValidateStatus::Moved { .. } => {
                    any_change = true;
                    i += 1;
                }
                RQEValidateStatus::Ok => {
                    i += 1;
                }
            }
        }

        if self.children.is_empty() {
            self.is_eof = true;
            self.num_active = 0;
            return Ok(RQEValidateStatus::Aborted);
        }

        if !any_change {
            return Ok(RQEValidateStatus::Ok);
        }

        self.rebuild_heap();
        self.num_active = self.heap.len();

        let Some(mut min) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        };

        // A minimum *behind* the union is not a position to move to — adopting it
        // would replay documents, because `VALIDATE_MOVED` has the caller emit
        // `current` in place of a read and the read after that resumes from there.
        // `iterator_api.h` says `VALIDATE_MOVED` means the position moved *forward*,
        // and `Not::revalidate` asserts that of its child — which is where a
        // `QUICK_EXIT` union usually sits.
        //
        // With `QUICK_EXIT` this is the mode's own early return, and routine:
        // `rebuild_heap` keys on `last_doc_id()`, which answers 0 for a child that has
        // never been read and an earlier round's id for one that
        // `advance_lagging_children` left in the heap when it returned on an exact match.
        //
        // A full union cannot get here: `advance_matching_children` leaves no active
        // child behind the union, and exhaustion is terminal across a revalidation (see
        // [`RQEIterator::at_eof`]), so a child dropped on EOF cannot be re-admitted by
        // `rebuild_heap` behind us. Asserted rather than compensated for — the recovery
        // below is quick mode's, and a full union arriving in it means a child is broken.
        if min.doc_id < original_last_doc_id {
            debug_assert!(
                QUICK_EXIT,
                "a full union's child moved behind the union's position: doc {} comes \
                 before doc {original_last_doc_id}",
                min.doc_id,
            );

            // A child still sitting on the union's document backs the position as it
            // stands: republish from it (the result holds raw pointers into children
            // that may have moved or been dropped) and report `Ok`, with no reads
            // spent. `min.child_idx` cannot serve — that is the lagging child just
            // rejected. Quick mode only: its laggers are ordinary state for the next
            // `read`/`skip_to` to seek past, while a full union must catch them up
            // below either way — `advance_matching_children` and
            // `build_aggregate_result` rely on the root not sitting behind the union.
            if QUICK_EXIT
                && let Some(idx) = self
                    .children
                    .iter()
                    .position(|c| !c.at_eof() && c.last_doc_id() == original_last_doc_id)
            {
                self.quick_set_from_child(idx);

                debug_assert_eq!(
                    self.last_doc_id(),
                    original_last_doc_id,
                    "staying put must leave the position untouched",
                );

                return Ok(RQEValidateStatus::Ok);
            }

            // Nothing already sits on the union's document, but a lagging child may
            // still *hold* it: the early return that stranded it never consumed its
            // position. The union may not step over the document before asking —
            // under a `NOT`, its position is not a delivered result but the next id
            // the `NOT` has yet to exclude, and stepping over it would let that
            // document into the result set. So the laggers are seeked to the
            // abandoned position itself, not past it — which is exactly
            // `advance_lagging_children`, down to quick mode's early return on the
            // child that lands there.
            //
            // These reads cannot be avoided. An `Err` here reaches the caller as
            // `VALIDATE_ABORTED`, freeing the iterator and substituting an empty one
            // — blunt, but better than handing out a position no child backs.
            let early_match = self.advance_lagging_children(original_last_doc_id)?;
            if QUICK_EXIT && early_match != usize::MAX {
                // Still matched after all: the union stays on it, backed by this
                // child; remaining laggers wait for the next call.
                self.quick_set_from_child(early_match);

                debug_assert_eq!(
                    self.last_doc_id(),
                    original_last_doc_id,
                    "staying put must leave the position untouched",
                );

                return Ok(RQEValidateStatus::Ok);
            }

            min = match self.heap.peek() {
                Some(min) => min,
                // Seeking the laggers forward ran the union out of documents.
                None => {
                    self.is_eof = true;
                    return Ok(RQEValidateStatus::Moved { current: None });
                }
            };

            // Every child left in the heap now sits at or past the abandoned
            // position, so the root is a position again: equal to it when children
            // still match the document (full mode only — quick mode returned above),
            // later otherwise.
            debug_assert!(
                min.doc_id >= original_last_doc_id,
                "every lagging child was just seeked to the union's position",
            );
        }

        if QUICK_EXIT {
            self.quick_set_from_child(min.child_idx);
        } else {
            self.build_aggregate_result(min.doc_id);
        }

        // Return MOVED only if lastDocId changed
        if self.last_doc_id() == original_last_doc_id {
            return Ok(RQEValidateStatus::Ok);
        }

        debug_assert!(
            self.last_doc_id() > original_last_doc_id,
            "a reported move must be forward",
        );

        Ok(RQEValidateStatus::Moved {
            current: Some(&mut self.result),
        })
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Union
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        if prioritize_union_children {
            self.children.len().max(1) as f64
        } else {
            1.0
        }
    }
}

impl<'index, const QUICK_EXIT: bool> crate::interop::ProfileChildren<'index>
    for UnionHeap<'index, crate::c2rust::CRQEIterator, QUICK_EXIT>
{
    fn profile_children(self) -> Self {
        UnionHeap {
            children: self
                .children
                .into_iter()
                .map(crate::c2rust::CRQEIterator::into_profiled)
                .collect(),
            num_estimated: self.num_estimated,
            num_active: self.num_active,
            is_eof: self.is_eof,
            result: self.result,
            heap: self.heap,
        }
    }
}
