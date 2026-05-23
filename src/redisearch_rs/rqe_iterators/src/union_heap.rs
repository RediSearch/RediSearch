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
    boxed::{ResumeSlotOutcome, rederive_aggregate_entries, resume_child_slot_in_place},
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

// Compile-time proof of invariant 1 on `RawUnionHeap`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. The child elements' own compatibility is their invariant 1
// (enforced generically by the slot helpers); `result` is layout-compatible
// across `Rf` (proven in `index_result`); `heap` and the remaining fields are
// `Rf`-free. `QUICK_EXIT` never affects layout.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawUnionHeap<'static, Active<'static>, AChild, false>;
    type S = RawUnionHeap<'static, Suspended, SChild, false>;
    assert!(offset_of!(A, children) == offset_of!(S, children));
    assert!(offset_of!(A, num_estimated) == offset_of!(S, num_estimated));
    assert!(offset_of!(A, num_active) == offset_of!(S, num_active));
    assert!(offset_of!(A, is_eof) == offset_of!(S, is_eof));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, heap) == offset_of!(S, heap));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Frees a suspended [`RawUnionHeap`]'s reused allocation after the in-place
/// resume consumed the child at `consumed`: drops the compacted resumed prefix
/// `children[..kept]` in place and the still-suspended tail past `consumed`,
/// skips the holes in between (moved-from or consumed), empties the `Vec` so it
/// frees only its buffer, then drops the box (freeing the still-suspended
/// `result`, the heap, and the allocation).
///
/// # Safety
///
/// * `raw` must be exclusively owned and have come from `Box::into_raw`.
/// * `children[..kept]` must hold valid `S::Resumed<'a>` values,
///   `children[kept..=consumed]` must be moved-from or consumed (they are not
///   touched), and `children[consumed + 1..]` must hold valid `S` values — the
///   exact state the in-place resume loop leaves behind when the slot helper
///   reports `Err` for element `consumed`.
unsafe fn free_after_consumed_child<'query, 'a, S, const QUICK_EXIT: bool>(
    raw: *mut RawUnionHeap<'query, Suspended, S, QUICK_EXIT>,
    kept: usize,
    consumed: usize,
) where
    S: RQESuspendedIterator<'query>,
    'query: 'a,
{
    // SAFETY: `raw` is exclusively owned (caller contract); the borrow is used
    // only to reach the buffer pointer, the length, and `set_len`.
    let children: &mut Vec<S> = unsafe { &mut (*raw).children };
    let len = children.len();
    let base = children.as_mut_ptr();
    for i in 0..kept {
        // SAFETY: `i` is in bounds of the children buffer.
        let slot = unsafe { base.add(i) };
        // SAFETY: the slot holds a valid resumed child (caller contract); drop
        // it through the resumed type (same size/alignment, enforced by the
        // slot helper's const guard).
        unsafe { std::ptr::drop_in_place(slot.cast::<S::Resumed<'a>>()) };
    }
    for i in (consumed + 1)..len {
        // SAFETY: `i` is in bounds of the children buffer.
        let slot = unsafe { base.add(i) };
        // SAFETY: the slot still holds a valid suspended child.
        unsafe { std::ptr::drop_in_place(slot) };
    }
    // SAFETY: every element was dropped, moved, or consumed; zeroing the length
    // keeps the `Vec` from dropping them again — it frees only its buffer.
    unsafe { children.set_len(0) };
    // SAFETY: `raw` is a well-formed suspended union again (empty children,
    // still-suspended `result`, `Rf`-free scalars and heap); reclaim and drop it.
    drop(unsafe { Box::from_raw(raw) });
}

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
    /// [`RQESuspendedIterator::resume`] so the two
    /// cannot drift apart — the legacy and the `Box<Self>` path must make the same
    /// re-seek and moved-versus-unchanged decisions, and a divergence between them is a
    /// bug. Each caller rebuilds the heap and sets `num_active` first, re-admitting
    /// every child: a child revalidation can bring a parked child back into play,
    /// and `rebuild_heap` leaves out the ones that are still exhausted.
    ///
    /// `original_last_doc_id` is the position the union held before the children moved.
    fn settle_after_children_changed(
        &mut self,
        original_last_doc_id: DocId,
    ) -> Result<SettleOutcome, RQEIteratorError> {
        let Some(mut min) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(SettleOutcome::Eof);
        };

        // A minimum *behind* the union is not a position to move to — adopting it
        // would replay documents, because a reported move has the caller emit
        // `current` in place of a read and the read after that resumes from there.
        // `iterator_api.h` says `VALIDATE_MOVED` means the position moved
        // *forward*, and `Not::revalidate` asserts that of its child — which is
        // where a `QUICK_EXIT` union usually sits.
        //
        // Both modes can see one. With `QUICK_EXIT` it is the mode's own early
        // return: `rebuild_heap` keys on `last_doc_id()`, which answers 0 for a
        // child that has never been read and an earlier round's id for one that
        // `advance_lagging_children` left in the heap when it returned on an exact
        // match. Without it there is exactly one way: a child that ran out and was
        // dropped can *resurrect* during a revalidation — an inverted-index leaf
        // rewinds and re-seeks the position it held, and rewinding clears the
        // past-the-end state — so `rebuild_heap` re-admits it far behind a union
        // that carried on without it.
        if min.doc_id < original_last_doc_id {
            // A child still sitting on the union's document backs the position as
            // it stands: republish from it (the result holds raw pointers into
            // children that may have moved or been dropped) and report no change,
            // with no reads spent. `min.child_idx` cannot serve — that is the
            // lagging child just rejected. Quick mode only: its laggers are
            // ordinary state for the next `read`/`skip_to` to seek past, while a
            // full union must catch them up below either way —
            // `advance_matching_children` and `build_aggregate_result` rely on
            // the root not sitting behind the union.
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

                return Ok(SettleOutcome::Unchanged);
            }

            // Nothing already sits on the union's document, but a lagging child
            // may still *hold* it: the early return that stranded it never
            // consumed its position. The union may not step over the document
            // before asking — under a `NOT`, its position is not a delivered
            // result but the next id the `NOT` has yet to exclude, and stepping
            // over it would let that document into the result set. So the laggers
            // are seeked to the abandoned position itself, not past it — which is
            // exactly `advance_lagging_children`, down to quick mode's early
            // return on the child that lands there.
            //
            // These reads cannot be avoided. An `Err` here reaches the caller as
            // `VALIDATE_ABORTED`, freeing the iterator and substituting an empty
            // one — blunt, but better than handing out a position no child backs.
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

                return Ok(SettleOutcome::Unchanged);
            }

            min = match self.heap.peek() {
                Some(min) => min,
                // Seeking the laggers forward ran the union out of documents.
                None => {
                    self.is_eof = true;
                    return Ok(SettleOutcome::Eof);
                }
            };

            // Every child left in the heap now sits at or past the abandoned
            // position, so the root is a position again: equal to it when
            // children still match the document (full mode only — quick mode
            // returned above), later otherwise.
            debug_assert!(
                min.doc_id >= original_last_doc_id,
                "every lagging child was just seeked to the union's position",
            );
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
        // The pre-resume position, read off the suspended form. Unlike
        // `revalidate` there is no is-EOF return before the walk: the suspended
        // children must be transitioned regardless of the outcome.
        let original_last_doc_id = self.result.doc_id;

        let raw = Box::into_raw(self);

        // Resume every child *in place* — including the exhausted tail past
        // `num_active`: a leaf can resurrect (its revalidation rewinds and
        // re-seeks, clearing the past-the-end state), and `revalidate` re-admits
        // such a child through `rebuild_heap` + the settle below, so resume must
        // too. Aborted children are removed, mirroring `revalidate`'s
        // swap_remove: survivors are compacted down over the holes.
        //
        // Whatever the walk does to the children, the suspended aggregate's
        // entries are re-derived from the survivors before the cast — the one
        // step that turns pointers whose addresses happen to have survived into
        // usable references. An abort is where the addresses stop surviving too:
        // the aborted child's result is freed outright, and the compaction
        // relocates every survivor behind it, so for a concrete (non-boxed)
        // child the result moves as well. Both show up there as an entry no live
        // child answers for, and clear the aggregate rather than re-narrow it.
        //
        // A panic between slot transitions leaks the allocation (memory-safe);
        // the slot helper guards its own moved-out window.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
            // initialised, exclusively owned); the borrow is used only to reach
            // the buffer pointer and length.
            let children: &mut Vec<S> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        let mut any_change = false;
        let mut kept = 0usize;
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it
            // on `Aborted`/`Err`.
            match unsafe { resume_child_slot_in_place(elem, guard) } {
                Ok(outcome) => {
                    match outcome {
                        ResumeSlotOutcome::Unchanged => {}
                        ResumeSlotOutcome::Moved => any_change = true,
                        ResumeSlotOutcome::Aborted => {
                            // Dropped from the union, like `revalidate`'s
                            // swap_remove of an aborted child; the hole is
                            // compacted over by later survivors.
                            any_change = true;
                            continue;
                        }
                    }
                    if kept < i {
                        // SAFETY: slot `kept` was vacated (its element moved left
                        // or was consumed); move the resumed element `i` into it.
                        // `copy_nonoverlapping` is a move — the source is treated
                        // as vacated from here on.
                        // SAFETY: `kept < i < len`, both in bounds.
                        let dst = unsafe { base.add(kept) };
                        // SAFETY: single-element move between disjoint,
                        // in-bounds slots.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                elem.cast::<S::Resumed<'a>>().cast_const(),
                                dst.cast::<S::Resumed<'a>>(),
                                1,
                            )
                        };
                    }
                    kept += 1;
                }
                Err(e) => {
                    // SAFETY: `children[..kept]` holds compacted resumed
                    // survivors, `children[kept..=i]` holes or the consumed
                    // element, `children[i + 1..]` still-suspended children — the
                    // exact state the teardown documents; `raw` is exclusively
                    // owned.
                    unsafe { free_after_consumed_child::<S, QUICK_EXIT>(raw, kept, i) };
                    return Err(e);
                }
            }
        }
        // SAFETY: `children[..kept]` holds the compacted survivors; everything
        // past it is vacated. `set_len` never drops, so shrinking to the
        // survivors is sound. The stale `num_active` is clamped right after the
        // cast below.
        // SAFETY: `raw` is exclusively owned; the borrow is confined to this
        // statement.
        let children = unsafe { &mut (*raw).children };
        // SAFETY: the first `kept` elements are initialised; `set_len` never
        // drops.
        unsafe { children.set_len(kept) };

        // Every survivor now sits in the compacted prefix in its resumed form,
        // so the aggregate can be re-derived from them — the step that makes its
        // entries usable again, rather than merely re-narrowed, and the last one
        // before the cast. All `kept` of them are offered, including the ones
        // the heap left out as exhausted, because an entry can point at any
        // child the aggregate was built from.
        {
            // SAFETY: `base` addresses the `kept` compacted survivors, each a
            // valid `S::Resumed<'a>` — same size and alignment as the `S` the
            // buffer was allocated for, statically enforced by
            // `resume_child_slot_in_place` — and `raw` is exclusively owned, so
            // the slice is unaliased.
            let children =
                unsafe { std::slice::from_raw_parts_mut(base.cast::<S::Resumed<'a>>(), kept) };
            // SAFETY: `raw` is exclusively owned and `result` is a valid
            // suspended result in a field disjoint from the children buffer; the
            // borrow is confined to this block.
            let result = unsafe { &mut (*raw).result };
            rederive_aggregate_entries(result, children);
        }

        // SAFETY: every surviving child slot holds its resumed form inside the
        // `Vec`'s buffer, and the aggregate's entries have just been re-derived
        // from those survivors (or cleared, if any of them could not be), so no
        // entry is re-narrowed onto a freed, relocated, or merely retagged
        // result; `heap` and the remaining fields are `Rf`-free. Layout-identical
        // to the suspended form by invariant 1 on `RawUnionHeap` (const proof
        // above). `Box::from_raw` reuses the same allocation, so the FFI's cached
        // `header.current` and any parent's pointer into `result` stay valid
        // across the cycle.
        let mut active =
            unsafe { Box::from_raw(raw.cast::<UnionHeap<'a, S::Resumed<'a>, QUICK_EXIT>>()) };
        // Aborted children shrank the vec; the active region cannot outgrow it.
        // (The heap can hold stale entries after a compaction, but every path
        // below that reads it either rebuilds it first or is gated on `is_eof`,
        // and `rewind` clears it.)
        active.num_active = active.num_active.min(active.children.len());

        // From here on, mirror `revalidate` decision for decision — including
        // the order the two questions are asked in. An already-finished union
        // stays finished, whatever its children now report: `revalidate` returns
        // before it looks at a single one, so asking "did they all abort?" first
        // would tear down a spent union that `revalidate` leaves alone, and
        // would answer `Aborted` for a union built with no children at all —
        // a supported construction that starts at EOF. Its position is
        // preserved (`result.doc_id` rode along in the cast), exactly as
        // `revalidate` leaves it. The children were still transitioned above —
        // the one structural difference from `revalidate`, which never touches
        // them.
        if active.is_eof {
            return Ok(ResumeOutcome::Ok(active));
        }

        // All children aborted → the union aborts (a union of nothing is
        // nothing). `revalidate` also sets `is_eof` before reporting `Aborted`;
        // here the box is dropped instead of handed back, so the flag has no
        // observer.
        if active.children.is_empty() {
            return Ok(ResumeOutcome::Aborted);
        }

        // Nothing moved or aborted: the children still sit on the positions the
        // aggregate and the heap describe, so both stand as-is.
        if !any_change {
            return Ok(ResumeOutcome::Ok(active));
        }

        // Re-admit every child, including any parked past `num_active`: their own
        // resume may have brought them back into play. `rebuild_heap` leaves the
        // still-exhausted ones out, and the settle re-derives the position.
        active.rebuild_heap();
        active.num_active = active.heap.len();
        Ok(
            match active.settle_after_children_changed(original_last_doc_id)? {
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
