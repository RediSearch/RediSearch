/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Heap variant of the union iterator with O(log n) min-finding.

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;
use std::marker::PhantomData;

use crate::union::SettleOutcome;
use crate::utils::DocIdMinHeap;
use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place},
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

/// RAII guard owning a suspended [`RawUnionHeap`]'s shell while its children
/// buffer is part-resumed and part-suspended.
///
/// The buffer is three regions during `resume`, and the guard's fields *are*
/// their boundaries: [`kept`](Self::kept) resumed survivors compacted to the
/// front, vacated slots up to [`cursor`](Self::cursor) — moved left by the
/// compaction, or consumed by a child that aborted or failed — and
/// still-suspended children from there to [`len`](Self::len). `resume` advances
/// the boundaries as it walks, so the drop reads the state that actually holds
/// rather than being handed indices that have to agree with it.
///
/// On drop — an early return or a panic — it drops the prefix at the resumed
/// type, drops the suspended tail, leaves the vacated middle alone, empties the
/// `Vec` so it frees only its buffer, and drops the shell (freeing the
/// still-suspended `result`, the heap, and the allocation). Disarmed with
/// [`std::mem::forget`] once the buffer is whole again and the cast is about to
/// run.
struct FreeSuspendedShell<'query, 'a, S, const QUICK_EXIT: bool>
where
    S: RQESuspendedIterator<'query>,
    'query: 'a,
{
    /// The shell, still owned here. Came from `Box::into_raw`.
    raw: *mut RawUnionHeap<'query, Suspended, S, QUICK_EXIT>,
    /// `children[..kept]` hold resumed survivors.
    kept: usize,
    /// `children[kept..cursor]` are vacated and must not be dropped.
    cursor: usize,
    /// `children[cursor..len]` still hold suspended children.
    len: usize,
    /// The type the prefix must be dropped at. Never materialised, so it adds
    /// no drop glue of its own.
    _resumed: PhantomData<fn() -> S::Resumed<'a>>,
}

impl<'query, 'a, S, const QUICK_EXIT: bool> Drop for FreeSuspendedShell<'query, 'a, S, QUICK_EXIT>
where
    S: RQESuspendedIterator<'query>,
    'query: 'a,
{
    fn drop(&mut self) {
        debug_assert!(!self.raw.is_null(), "the shell must still be owned here");
        // The whole teardown rests on this ordering and nothing else checks it.
        // The way to break it is to move `cursor`'s advance in `resume` to
        // *after* the slot helper call — which reads like a tidy-up, since every
        // other field is advanced after its work — leaving a consumed slot
        // inside the suspended tail for the loop below to drop a second time.
        debug_assert!(
            self.kept <= self.cursor && self.cursor <= self.len,
            "region boundaries out of order: kept={}, cursor={}, len={}",
            self.kept,
            self.cursor,
            self.len,
        );
        // SAFETY: `raw` is exclusively owned by this guard; the borrow reaches
        // the buffer pointer and the length, and is confined to this function.
        let children: &mut Vec<S> = unsafe { &mut (*self.raw).children };
        debug_assert!(
            self.len <= children.capacity(),
            "the recorded length outruns the buffer it indexes",
        );
        let base = children.as_mut_ptr();
        for i in 0..self.kept {
            // SAFETY: `i < kept <= len`, in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: the slot holds a resumed survivor; drop it through the
            // resumed type, which shares `S`'s size and alignment (statically
            // enforced by the slot helper's const guard).
            unsafe { std::ptr::drop_in_place(slot.cast::<S::Resumed<'a>>()) };
        }
        for i in self.cursor..self.len {
            // SAFETY: `i < len`, in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: the slot still holds a valid suspended child.
            unsafe { std::ptr::drop_in_place(slot) };
        }
        // SAFETY: every element has been dropped, moved or consumed; zeroing the
        // length keeps the `Vec` from dropping them again, so it frees only its
        // buffer.
        unsafe { children.set_len(0) };
        // SAFETY: the shell is a well-formed suspended union again (empty
        // children, still-suspended `result`, `Rf`-free scalars and heap).
        drop(unsafe { Box::from_raw(self.raw) });
    }
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
    /// bug. Each caller rebuilds the heap and sets `num_active` first, because
    /// removing an aborted child invalidates both — not because a child left out of
    /// the heap can come back. `rebuild_heap` leaves out the ones that are still
    /// exhausted.
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
        // With `QUICK_EXIT` this is the mode's own early return, and routine:
        // `rebuild_heap` keys on `last_doc_id()`, which answers 0 for a child that
        // has never been read and an earlier round's id for one that
        // `advance_lagging_children` left in the heap when it returned on an exact
        // match.
        //
        // A full union cannot get here: `advance_matching_children` leaves no
        // active child behind the union, and exhaustion is terminal across a
        // revalidation *and* a resume (see [`RQEIterator::at_eof`]), so a child
        // dropped on EOF cannot be re-admitted by `rebuild_heap` behind us on
        // either path. Asserted rather than compensated for — the recovery below is
        // quick mode's, and a full union arriving in it means a child is broken.
        if min.doc_id < original_last_doc_id {
            debug_assert!(
                QUICK_EXIT,
                "a full union's child moved behind the union's position: doc {} comes \
                 before doc {original_last_doc_id}",
                min.doc_id,
            );

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
    /// neither `revalidate` nor `resume` can leave one behind the union — a child
    /// dropped on EOF stays dropped, since exhaustion is terminal
    /// ([`at_eof`](RQEIterator::at_eof)). A root behind would stay the minimum and
    /// hand back a document already delivered, so the invariant is asserted rather
    /// than handled.
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

        Ok(self
            .settle_after_children_changed(original_last_doc_id)?
            .into_validate_status(&mut self.result))
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
        // Walked through the buffer pointer rather than `iter_mut`, as `resume`
        // does: each transition retypes a slot from `I` to `I::Suspended`, so a
        // typed `IterMut<I>` would still be describing the buffer as `I` after
        // the first one. Nothing here re-reads a transitioned slot, so that
        // iterator would not actually be misused — but the borrow claims
        // something that stops being true half way through, and every other
        // caller of the slot helper already hands it a raw pointer.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` and is exclusively owned
            // for the rest of this function, so the children Vec is reachable
            // and unaliased. The borrow is used only to reach the buffer pointer
            // and length, and ends here.
            let children: &mut Vec<I> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: `slot` holds a valid, owned `I`; the helper leaves it
            // holding a valid `I::Suspended`.
            unsafe { crate::boxed::suspend_child_slot_in_place(slot) };
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

/// Whether the rebuilt aggregate still has a child behind it.
///
/// `#[must_use]` sits on the type so it covers every producer: an
/// [`Unbacked`](Self::Unbacked) that nobody looks at is a result published with
/// nothing backing the document it claims.
#[must_use = "an unbacked aggregate no longer describes a document; the caller \
              must rebuild it or abort the resume"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildOutcome {
    /// At least one survivor sits on the result's document, and the aggregate
    /// now borrows exactly those survivors. The result stands as a description
    /// of that document, and re-narrowing it is sound.
    Backed,
    /// No survivor sits on the result's document, so the aggregate came out
    /// empty. Re-narrowing it is still sound — it points nowhere — but it no
    /// longer describes a document, so the composite must rebuild it before
    /// publishing it or report [`ResumeOutcome::Aborted`] instead.
    Unbacked,
}

/// Rebuild a suspended union's borrowed entries from its just-resumed children.
///
/// A union's aggregate borrows one entry per child sitting on the union's
/// document, and each entry is a pointer derived from a borrow of that child's
/// result. Transitioning a child hands its allocation through a by-value
/// `Box<Self>`, whose retag invalidates that borrow even though nothing was
/// dropped, moved or written — so every entry has to be written afresh before
/// the union's own allocation is re-narrowed. This is that step, and it is not
/// optional.
///
/// # Why rebuild rather than re-derive
///
/// The alternative is to keep the entry list and refresh each entry in place,
/// which means recognising which child an entry came from — and an entry
/// records only an address, so the only handle available is "the survivor that
/// still lives at that address". That handle is weak in both directions: it
/// stops identifying anything once a dropped child's slot has been compacted
/// over, and it cannot distinguish the child that moved onto that address from
/// the one that was always there.
///
/// A union does not need the handle. Its contributor set is a *function* of the
/// children — every child whose `current()` sits on the union's document, which
/// is exactly what [`build_aggregate_result`](RawUnionHeap::build_aggregate_result)
/// evaluates on the read path — so it can be recomputed from the survivors in
/// one forward pass, and the entries never have to be identified at all.
///
/// # What is recomputed, and what is carried across
///
/// `freq` and `field_mask` are recomputed. They are copies of what each
/// contributing child holds, so they are functions of the same contributor set
/// as the entries; recomputing them together is what keeps the two from
/// disagreeing. Preserving them across a rebuild that lost a contributor would
/// leave the result claiming a frequency and — worse — a set of fields that no
/// surviving entry accounts for, and `field_mask` decides field filtering and
/// field-weighted scoring.
///
/// `metrics` are carried across untouched, because they are *not* a function of
/// the children: [`RSIndexResult::push_borrowed`] **moves** a child's metrics
/// into the union when the aggregate is first built, leaving the child with
/// none. There is nothing to re-accumulate, so resetting them would discard
/// them for good — a KNN child's `__vector_score` vanishing from the reply. The
/// cost of keeping them is that a metric contributed by a child that has since
/// been dropped stays; the alternative loses the surviving children's metrics
/// too, to remove it.
///
/// `doc_id` is likewise untouched: it is the document the rebuild is *for*, and
/// the input to the contributor test rather than an output of it. It is read
/// before anything is cleared, so the clearing step cannot quietly become the
/// one that takes it away.
///
/// # Quick mode
///
/// A `QUICK_EXIT` union reports a single child rather than aggregating, so the
/// rebuild stops at the first contributor. Which child that is need not be the
/// one the union picked before suspending — it picks by heap order, this picks
/// by slot — but any child on the document backs the position equally, which is
/// the same latitude [`settle_after_children_changed`](RawUnionHeap::settle_after_children_changed)
/// already takes when it re-picks a backer after its children moved.
///
/// # Call it last
///
/// Not merely "before the cast": **after** the child walk, **after** any
/// side-table rebuild, and with no `&mut` taken to a child afterwards. Each
/// entry is derived from a shared reborrow of a `&mut` to its child, so any
/// later mutable access to that child — a `read`, a `skip_to`, another
/// `current`, an `iter_mut` over the children, a `swap_remove`, a sort — pops
/// the tag it carries. `&mut` to the union itself is fine; the entries point
/// into the *children's* allocations.
fn rebuild_borrowed_entries<'a, 'child, R, const QUICK_EXIT: bool>(
    result: &mut RawIndexResult<'a, Suspended>,
    children: &mut [R],
) -> RebuildOutcome
where
    R: RQEIterator<'a>,
{
    let doc_id = result.doc_id;
    let Some(aggregate) = result
        .as_aggregate_mut()
        .and_then(|aggregate| aggregate.as_borrowed_mut())
    else {
        // Nothing borrowed here: not an aggregate at all, or an owned one whose
        // children live in the result's own allocation. Either way there is
        // nothing to rebuild
        // and nothing for the caller to make good. `Unbacked` here would oblige
        // it to abort a resume that never had entries at stake.
        return RebuildOutcome::Backed;
    };
    if aggregate.is_empty() {
        // Nothing borrowed, so again nothing to rebuild, and no reason to touch
        // a single child. This is the union that was suspended before its first
        // read: its result is an empty aggregate on document 0, which no child
        // would answer for.
        return RebuildOutcome::Backed;
    }

    // Drops the records and the kind mask; `push_borrowed_ptr_from_ref` rebuilds
    // both, entry by entry. Deliberately not `reset_aggregate`, which would also
    // take `doc_id` — read above, and what the loop below tests each child
    // against — and the metrics, which cannot be put back. See
    // `# What is recomputed` above.
    aggregate.reset();
    result.freq = 0;
    result.field_mask = 0;

    let mut contributors = 0;
    for child in children.iter_mut() {
        // A child at EOF publishes no result, and one whose own resume carried
        // it past the union's document no longer describes that document.
        let Some(current) = child.current() else {
            continue;
        };
        if current.doc_id != doc_id {
            continue;
        }
        // The entry is taken from a shared reborrow, so the tag it carries stays
        // valid for as long as nothing takes `&mut` to this child again. The
        // loop only ever moves on to a *different* child, and the union takes
        // none after this call returns.
        let current: &RSIndexResult<'a> = &*current;

        result.freq += current.freq;
        result.field_mask |= current.field_mask;
        result
            .as_aggregate_mut()
            .and_then(|aggregate| aggregate.as_borrowed_mut())
            .expect("the result borrowed an aggregate a handful of statements ago")
            .push_borrowed_ptr_from_ref(current);
        contributors += 1;

        if QUICK_EXIT {
            break;
        }
    }

    if contributors == 0 {
        RebuildOutcome::Unbacked
    } else {
        RebuildOutcome::Backed
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
        // `current()` hands the result out mutably, so "still a union result" is a
        // runtime invariant rather than an enforced one — a consumer could have
        // replaced it with an index-backed result of another kind. Nothing below
        // would notice: `rebuild_borrowed_entries` has nothing to rebuild for
        // a result that borrows nothing, and the cast would
        // then re-narrow whatever suspended pointers the substitute holds
        // without re-validating them. A union has no way to re-validate a
        // payload it did not build, so it refuses, on `&self`, before
        // `Box::into_raw` opens the raw-pointer section — the same shape
        // `Optional` and `NotOptimized` use for their virtual sentinels.
        //
        // The test is the exact kind, not `is_aggregate()`: that also admits
        // `HybridMetric`, whose children are *owned* boxes rather than borrowed
        // entries. `rebuild_borrowed_entries` takes its "nothing borrowed"
        // early return for one and reports success, leaving boxed children whose
        // own `data` may still be index-backed and suspended — a case no union
        // is equipped to re-validate. This is what keeps it unreachable.
        //
        // The kind alone is not enough: an aggregate of kind `Union` can be the
        // *owned* representation, whose children are boxed into the result's own
        // allocation and are never transitioned. Safe code can build one —
        // `to_owned()` on a union result, `push_boxed` an index-backed term into
        // it, assign it through `current()` — and a boxed child's `data` can be
        // index-backed and suspended, so re-narrowing it would promote pointers
        // nothing re-validated. The rebuild cannot catch it either: an owned
        // aggregate borrows nothing, so it has nothing to rebuild and reports
        // success. Demanding the borrowed representation is what closes that.
        let is_borrowed_union = self.result.kind() == RSResultKind::Union
            && self
                .result
                .as_aggregate()
                .is_some_and(|aggregate| aggregate.as_borrowed().is_some());
        if !is_borrowed_union {
            return Ok(ResumeOutcome::Aborted);
        }

        // The pre-resume position, read off the suspended form. Unlike
        // `revalidate` there is no is-EOF return before the walk: the suspended
        // children must be transitioned regardless of the outcome.
        let original_last_doc_id = self.result.doc_id;

        let raw = Box::into_raw(self);

        // Resume every child *in place* — including the ones the heap left out as
        // exhausted, which is not optional. The cast below retypes the whole buffer
        // at once, so a slot left suspended would be read as an `S::Resumed<'a>`,
        // and even dropping it would be undefined behaviour. Exhaustion also does
        // not put a child beyond reach: [`rewind`] re-admits every one of them, so
        // a child skipped here on the grounds that it was spent would be a
        // suspended iterator the very next rewind rewinds and reads. Aborted
        // children are removed, as `revalidate` removes an aborted child:
        // survivors are compacted down over the holes. Not in the same *order*,
        // though — `revalidate` uses `swap_remove`, which pulls the last child
        // into the hole, while this compaction preserves the survivors'
        // relative order. The surviving set and the union's document stream are
        // identical either way, but index-sensitive accessors (`child_at`,
        // `into_children`, the aggregate's entry order, and quick mode's
        // first-match republish) can answer differently.
        //
        // [`rewind`]: RQEIterator::rewind
        //
        // Whatever the walk does to the children, the suspended aggregate's
        // entries are dealt with before the cast — the one step that turns
        // pointers whose addresses happen to have survived into usable
        // references. They are rebuilt from the survivors rather than repaired
        // in place, so the walk is free to drop and compact without leaving
        // anything for that step to disentangle.
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
        // From here until the cast, the buffer is part-resumed and part-suspended
        // and `shell` owns it: every early return below, and any panic, frees it
        // through the guard's drop rather than through a call the exit has to
        // remember to make.
        let mut shell = FreeSuspendedShell::<'query, 'a, S, QUICK_EXIT> {
            raw,
            kept: 0,
            cursor: 0,
            len,
            _resumed: PhantomData,
        };
        let mut any_change = false;
        for i in 0..len {
            // Slot `i` counts as vacated for as long as the helper owns its
            // contents, so the boundary moves *before* the call, not after: on
            // `Err` the child is consumed and the guard must already know not to
            // drop it.
            shell.cursor = i + 1;
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it
            // on `Aborted`/`Err`.
            //
            // An `Err` returns through `?`, dropping `shell`. That is the one
            // place resume cannot match `revalidate`, and it is structural rather
            // than a choice: a spent union — `is_eof`, nothing left to read —
            // makes `revalidate` return `Ok` before it looks at a child, so a
            // child that would time out is never asked. Resume has to ask every
            // child before it can hand any of them back, and `S::resume` takes
            // the child *by value*, so once it answers `Err` there is nothing
            // left to walk past to reach the `is_eof` exit below. Pinned by
            // `resume_of_a_spent_union_surfaces_a_child_error`.
            match unsafe { resume_child_slot_in_place(elem, guard) }? {
                ResumeSlotOutcome::Unchanged => {}
                ResumeSlotOutcome::Moved => any_change = true,
                ResumeSlotOutcome::Aborted => {
                    // Dropped from the union, as `revalidate` drops an aborted
                    // child; the hole is compacted over by later survivors, in
                    // the order noted above.
                    any_change = true;
                    continue;
                }
            }
            if shell.kept < i {
                // SAFETY: `shell.kept < i < len`, both in bounds.
                let dst = unsafe { base.add(shell.kept) };
                // SAFETY: slot `shell.kept` was vacated (its element moved left or
                // was consumed) and `elem` holds a resumed child, so this is a
                // single-element move between disjoint, in-bounds slots.
                // `copy_nonoverlapping` is a move — the source is treated as
                // vacated from here on, which is what the guard's vacated middle
                // already covers.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        elem.cast::<S::Resumed<'a>>().cast_const(),
                        dst.cast::<S::Resumed<'a>>(),
                        1,
                    )
                };
            }
            shell.kept += 1;
        }
        let kept = shell.kept;
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

        // Every survivor now sits in its resumed form, and the aggregate has to
        // be dealt with before the cast — the step that makes its entries usable
        // again, rather than merely re-narrowed.
        //
        // Every survivor is offered, including the ones the heap left out as
        // exhausted: the rebuild decides membership by asking each child where
        // it sits, and a child the heap dropped is exactly one that will decline.
        // Which is also why the compaction above needs no special handling here
        // — nothing is matched against a slot, so nothing is confused by a
        // survivor having moved into a dropped child's slot.
        let rebuilt = {
            // Viewed at `'a` rather than at `'query`. Narrowing a query lifetime
            // is the safe direction — `'query: 'a` — but `&mut` is invariant, so
            // the compiler will not do it for us and the pointer is re-cast
            // instead. It is what lets the rebuild tie each entry to the same
            // lifetime the children carry, rather than widening theirs to
            // `'query`.
            //
            // SAFETY: `raw` is exclusively owned and `result` is a valid
            // suspended result in a field disjoint from the children buffer; the
            // borrow is confined to this block. `RawIndexResult` differs between
            // the two lifetimes only in the query-pipeline pointers it claims,
            // and `'query: 'a` makes every one of them valid for `'a`.
            //
            // SAFETY: `raw` is exclusively owned and non-null, so projecting to
            // its `result` field is in bounds; no reference is created here.
            let result_ptr = unsafe { &raw mut (*raw).result };
            // SAFETY: the projection above is valid, aligned and initialised, and
            // the two lifetimes differ only in the claim described above.
            let result: &mut RawIndexResult<'a, Suspended> =
                unsafe { &mut *result_ptr.cast::<RawIndexResult<'a, Suspended>>() };
            // SAFETY: `base` addresses the `kept` survivors, each a valid
            // `S::Resumed<'a>` — same size and alignment as the `S` the buffer
            // was allocated for, statically enforced by
            // `resume_child_slot_in_place` — and `raw` is exclusively owned, so
            // the slice is unaliased.
            let children =
                unsafe { std::slice::from_raw_parts_mut(base.cast::<S::Resumed<'a>>(), kept) };
            rebuild_borrowed_entries::<_, QUICK_EXIT>(result, children)
        };

        // The buffer is whole again and the next statement takes ownership of the
        // allocation, so the guard is disarmed here and not before: it stayed
        // armed across the `set_len` and the aggregate work above, where the
        // elements are still *typed* as suspended while holding resumed children,
        // and an unwind out of either would otherwise have dropped them at the
        // wrong type.
        std::mem::forget(shell);

        // SAFETY: every surviving child slot holds its resumed form inside the
        // `Vec`'s buffer, and the aggregate's entries have just been re-derived
        // from those survivors or dropped outright, so no entry is re-narrowed
        // onto a freed, relocated, or merely retagged result; `heap` and the
        // remaining fields are `Rf`-free. Layout-identical
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
        //
        // That difference is observable, and only through `rewind`. The walk
        // above dropped every child that answered `Aborted`, where `revalidate`
        // left them in place having never asked; a later `rewind` re-admits what
        // is there, so the two paths yield different documents from that point
        // on. Not a defect to fix: an aborted child's state is unrecoverable, so
        // the ones this path drops are exactly the ones `revalidate` would go on
        // to rewind and read *after* their index went away. Pinned by
        // `resume_of_a_spent_union_drops_children_that_revalidate_would_rewind`,
        // because the differential harness cannot see it — it never rewinds.
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
            // Unless the aggregate could not be re-derived. This is the only exit
            // that hands a live position back with no rebuild under it, so a
            // union that can no longer say which children back that position
            // aborts rather than publish one that describes nothing. (The
            // `is_eof` exit above needs no such guard: it publishes no `current`,
            // so a cleared aggregate has no observer there and `revalidate`'s
            // `Ok` stays the faithful answer.)
            //
            // Believed unreachable with conforming children, and asserted rather
            // than assumed. On this arm every child reported `Unchanged` and none
            // was dropped, so the children that backed the union's document
            // before the suspend are all still there, still publishing a
            // `current()`, and still on that document — at least one contributor,
            // since the union was sitting on a document it had read. Coming out
            // unbacked means a child broke `Unchanged`'s promise to hold its
            // position.
            debug_assert_ne!(
                rebuilt,
                RebuildOutcome::Unbacked,
                "a union whose children all resumed unchanged cannot lose its aggregate",
            );
            if rebuilt == RebuildOutcome::Unbacked {
                return Ok(ResumeOutcome::Aborted);
            }
            return Ok(ResumeOutcome::Ok(active));
        }

        // As in `revalidate`: the compaction over an aborted child invalidated both
        // the heap (its `child_idx` entries) and `num_active`, so rebuild them from
        // the survivors. `rebuild_heap` leaves out the children that are still
        // exhausted — every one that was, since resume cannot revive them — and the
        // settle re-derives the union's position.
        active.rebuild_heap();
        active.num_active = active.heap.len();
        let settled = active.settle_after_children_changed(original_last_doc_id)?;
        Ok(settled.into_resume_outcome(active))
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
