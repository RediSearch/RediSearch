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
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::union::SettleOutcome;
use crate::utils::DocIdMinHeap;
use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
};
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

    /// Settles the union's position after its children have moved, and reports where
    /// that leaves it.
    ///
    /// The single place that decides a union's post-change position, shared by
    /// [`RQEIterator::revalidate`] and
    /// [`RQESuspendedIterator::resume`](crate::RQESuspendedIterator::resume) so the two
    /// cannot drift apart — the legacy and the `Box<Self>` path must make the same
    /// re-seek and moved-versus-unchanged decisions, and a divergence between them is a
    /// bug. Each caller rebuilds the heap and sets `num_active` first.
    ///
    /// `original_last_doc_id` is the position the union held before the children moved.
    fn settle_after_children_changed(
        &mut self,
        original_last_doc_id: DocId,
    ) -> Result<SettleOutcome, RQEIteratorError> {
        let Some(min) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(SettleOutcome::Eof);
        };

        // Without `QUICK_EXIT` no child can fall behind the union: every child in the
        // heap is advanced on every `read`/`skip_to`, and a child's own `revalidate` may
        // only move it forward — one that has run past its end reports no `current` and
        // is left out by `rebuild_heap`. So the minimum is at worst *equal* to the
        // current position, which is simply a child still sitting on the document it
        // supplied.
        debug_assert!(
            QUICK_EXIT || min.doc_id >= original_last_doc_id,
            "a full union's child cannot fall behind it: {} < {original_last_doc_id}",
            min.doc_id,
        );

        // With `QUICK_EXIT` it can: `rebuild_heap` keys on `last_doc_id()`, which answers
        // 0 for a child that has never been read and an earlier round's id for one that
        // `advance_lagging_children` left in the heap when it returned on an exact match.
        // Such a minimum is not a position to move to — adopting it would replay
        // documents, because a reported move has the caller emit `current` in place of a
        // read and the read after that resumes from there.
        //
        // `QUICK_EXIT` is a const generic, so a full union compiles this away entirely
        // rather than paying for a comparison that its invariant already rules out.
        if QUICK_EXIT && min.doc_id < original_last_doc_id {
            // The result has to be republished either way, because it holds raw pointers
            // into the children's own results: one that moved leaves it describing
            // another document, and one that aborted was dropped above and leaves it
            // dangling. What it can be republished *from* decides the outcome.
            if self.republish_at(original_last_doc_id) {
                debug_assert_eq!(
                    self.last_doc_id(),
                    original_last_doc_id,
                    "staying put must leave the position untouched",
                );

                return Ok(SettleOutcome::Unchanged);
            }

            // Nothing is left on the union's document: the child that supplied it has
            // moved on too. Reporting no change would promise a `current` that no child
            // backs, so the union advances instead — which is what the following read
            // would have done anyway. Skipping over the abandoned document costs
            // nothing: it has already been delivered, and a quick union never promised
            // to aggregate every child that holds it.
            return Ok(if self.read_quick()?.is_some() {
                SettleOutcome::Moved
            } else {
                SettleOutcome::Eof
            });
        }

        let min_doc_id = min.doc_id;
        let min_child_idx = min.child_idx;
        if QUICK_EXIT {
            self.quick_set_from_child(min_child_idx);
        } else {
            self.build_aggregate_result(min_doc_id);
        }

        if self.last_doc_id() == original_last_doc_id {
            return Ok(SettleOutcome::Unchanged);
        }

        debug_assert!(
            self.last_doc_id() > original_last_doc_id,
            "a reported move must be forward",
        );

        Ok(SettleOutcome::Moved)
    }

    /// Republishes the result at `doc_id`, returning whether any child backs it.
    ///
    /// From a *single* child in `QUICK_EXIT` mode — the full aggregate is what that mode
    /// exists to avoid — and by descending the heap otherwise.
    ///
    /// Full mode always answers `true`: its children never fall behind, so the only
    /// position it is ever asked to republish is the heap's own minimum.
    fn republish_at(&mut self, doc_id: DocId) -> bool {
        if QUICK_EXIT {
            // The minimum's index cannot serve here: that is the lagging child whose
            // position was rejected. A child the heap left out holds nothing to
            // contribute, so `at_eof` filters it.
            match self
                .children
                .iter()
                .position(|c| !c.at_eof() && c.last_doc_id() == doc_id)
            {
                Some(idx) => {
                    self.quick_set_from_child(idx);
                    true
                }
                None => false,
            }
        } else {
            self.build_aggregate_result(doc_id);
            true
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
// `UnionHeap<'index, I, false>`: the callers are the generic `RQEIterator` impl, which cannot
// reach a method that exists for only one value of a const generic, and splitting that
// impl in two would also strand the `ProfileChildren` impl, whose `RQEIterator`
// supertrait bound is stated for a generic `QUICK_EXIT`.
impl<'index, I, const QUICK_EXIT: bool> UnionHeap<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Advances the children at the heap root that are sitting on `current_id`.
    ///
    /// Only [`Self::read_full`] calls this, so `QUICK_EXIT` is always `false` here and
    /// the root can never be *behind* `current_id`: a full union advances every child in
    /// the heap on every `read`/`skip_to`, and a child's own `revalidate` may only move
    /// it forward. A root that were behind would have to be seeked rather than read —
    /// one read need not clear `current_id` — and would otherwise stay the minimum and
    /// hand back a document already delivered. That case is asserted away rather than
    /// handled, so it cannot be introduced silently.
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

        // The root was advanced past `previous_id`, so it has to name a later
        // document. Handing back one this union already delivered would have the
        // caller emit it twice.
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

        Ok(
            match self.settle_after_children_changed(original_last_doc_id)? {
                SettleOutcome::Unchanged => RQEValidateStatus::Ok,
                SettleOutcome::Moved => RQEValidateStatus::Moved {
                    current: Some(&mut self.result),
                },
                SettleOutcome::Eof => RQEValidateStatus::Moved { current: None },
            },
        )
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

impl<'index, I, const QUICK_EXIT: bool> RQEIteratorBoxed<'index>
    for UnionHeap<'index, I, QUICK_EXIT>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawUnionHeap<'index, Suspended, I::Suspended, QUICK_EXIT>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Walk children: dispatch each child's `suspend` through the trait
        // so dyn-erased `I` correctly transitions its vtable. See
        // [`crate::boxed::suspend_child_slot_in_place`] for the rationale.
        //
        // SAFETY: `raw` came from `Box::into_raw` and is exclusively owned
        // for the rest of this function, so the children Vec is reachable
        // and unaliased.
        let children: &mut Vec<I> = unsafe { &mut (*raw).children };
        for child in children.iter_mut() {
            // SAFETY: `child` is a valid `&mut I` aliased to nothing else;
            // the function leaves the slot in a valid `I::Suspended` state.
            unsafe { crate::boxed::suspend_child_slot_in_place(child) };
        }
        // SAFETY: `RawUnionHeap` is `#[repr(C)]` over `Vec<I>` (now byte-
        // rewritten as `Vec<I::Suspended>` contents), `result:
        // RawIndexResult<Rf>` (layout-compatible via `SharedPtr`), and
        // a heap of plain doc-ids/indices (no `Rf` in its types).
        unsafe {
            Box::from_raw(raw as *mut RawUnionHeap<'index, Suspended, I::Suspended, QUICK_EXIT>)
        }
    }
}

impl<'query, S, const QUICK_EXIT: bool> RQESuspendedIterator<'query>
    for RawUnionHeap<'query, Suspended, S, QUICK_EXIT>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = UnionHeap<'a, S::Resumed<'a>, QUICK_EXIT>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let RawUnionHeap {
            children,
            num_estimated,
            num_active,
            is_eof,
            result,
            heap: _,
        } = *self;

        let saved_weight = result.weight;
        let saved_last_doc_id = result.doc_id;
        drop(result);

        // `swap_remove_child` keeps EOF children in the Vec at indices
        // `[num_active..]` so [`rewind`](RQEIterator::rewind) can restore
        // them to active. Resume must preserve that split: tail children
        // stay in their resumed-active form so a future rewind picks them
        // up, but they don't count toward `num_active` (their last_doc_id
        // would otherwise be re-inserted into the heap and yielded again
        // after the live children exhaust).
        // No need to track whether any child moved: settling below re-finds the minimum
        // unconditionally, and reports `Unchanged` when it turns out to be where the
        // union already was.
        let mut live: Vec<S::Resumed<'a>> = Vec::with_capacity(num_active);
        let mut dead: Vec<S::Resumed<'a>> =
            Vec::with_capacity(children.len().saturating_sub(num_active));
        for (i, inner) in children.into_iter().enumerate() {
            let active_inner = match Box::new(inner).resume(guard)? {
                ResumeOutcome::Aborted => continue,
                ResumeOutcome::Moved(active_inner) | ResumeOutcome::Ok(active_inner) => {
                    *active_inner
                }
            };
            if i < num_active {
                live.push(active_inner);
            } else {
                dead.push(active_inner);
            }
        }
        let num_children = live.len();
        let mut active_children = live;
        active_children.extend(dead);
        let result = RSIndexResult::build_union(num_children)
            .weight(saved_weight)
            .build();

        let mut active: Box<UnionHeap<'a, S::Resumed<'a>, QUICK_EXIT>> = Box::new(UnionHeap {
            children: active_children,
            num_estimated,
            num_active: num_children,
            is_eof,
            result,
            heap: DocIdMinHeap::with_capacity(num_children),
        });

        if active.is_eof || saved_last_doc_id == 0 {
            return Ok(ResumeOutcome::Ok(active));
        }

        if num_children == 0 {
            return Ok(ResumeOutcome::Aborted);
        }

        // The heap was emptied with the suspended form, so it has to be rebuilt whether
        // or not a child moved — which is why this settles unconditionally rather than
        // short-circuiting on `any_change` the way `revalidate` can. With nothing moved
        // the recomputed minimum *is* `saved_last_doc_id`, so settling reports
        // `Unchanged`, and the special case would only be a second copy of logic that
        // has to agree with the shared one.
        let mut num_active = 0usize;
        for (idx, child) in active.children.iter().enumerate() {
            if !child.at_eof() {
                active.heap.push(child.last_doc_id(), idx);
                num_active += 1;
            }
        }
        active.num_active = num_active;

        Ok(
            match active.settle_after_children_changed(saved_last_doc_id)? {
                SettleOutcome::Unchanged => ResumeOutcome::Ok(active),
                // Both leave the union somewhere other than where it was; `Eof` has
                // already set `is_eof`, so the caller sees no current either way.
                SettleOutcome::Moved | SettleOutcome::Eof => ResumeOutcome::Moved(active),
            },
        )
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
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
