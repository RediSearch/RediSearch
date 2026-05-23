/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Flat array variant of the union iterator with O(n) min-finding.

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::union::SettleOutcome;
use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place},
};
use index_spec::IndexSpecReadGuard;

/// A child iterator paired with its original insertion index.
///
/// Tracks where the child was in the original `children` vector so that
/// we can restore the original order.
///
/// `#[repr(C)]` so that the `Vec<IndexedChild<I>>` elements stay
/// layout-compatible across the `I` → `I::Suspended` swap: a default-repr
/// struct is free to reorder its fields differently per instantiation, which
/// would corrupt elements when the union is transmuted between Active and
/// Suspended modes.
#[repr(C)]
pub(crate) struct IndexedChild<I> {
    /// Position of this child in the original `children` vector passed to
    /// [`UnionFlat::new`].
    pub(crate) original_index: usize,
    /// The underlying child iterator.
    pub(crate) inner: I,
}

impl<I> std::ops::Deref for IndexedChild<I> {
    type Target = I;
    #[inline(always)]
    fn deref(&self) -> &I {
        &self.inner
    }
}

impl<I> std::ops::DerefMut for IndexedChild<I> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut I {
        &mut self.inner
    }
}

/// Yields documents appearing in ANY child iterator using a flat array scan.
///
/// Parameterised over a [`Ref`] mode — see [`UnionFlat`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// [`UnionFlat`] yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated (unless `QUICK_EXIT` is `true`).
///
/// Uses O(n) min-finding by scanning all children. Best for small numbers of children
/// (typically <20) due to minimal memory overhead and cache-friendly iteration.
///
/// For large numbers of children (>20), a heap-based variant may be more efficient.
///
/// # Type Parameters
///
/// - `Rf`: The [`Ref`] mode.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
#[repr(C)]
pub struct RawUnionFlat<'query, Rf: Ref, I, const QUICK_EXIT: bool> {
    /// Child iterators. Active children are in `children[..num_active]`,
    /// exhausted children are moved to the end and not removed so we can rewind the iterator.
    children: Vec<IndexedChild<I>>,
    /// Number of active (non-EOF) children. Only `children[..num_active]` are scanned.
    num_active: usize,
    /// Sum of all children's estimated counts (upper bound).
    num_estimated: usize,
    /// Whether the iterator has reached EOF (all children exhausted).
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RawIndexResult<'query, Rf>,
}

/// Alias for an [`Active`] [`RawUnionFlat`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type UnionFlat<'index, I, const QUICK_EXIT: bool> =
    RawUnionFlat<'index, Active<'index>, I, QUICK_EXIT>;

// Compile-time proof of invariant 1 on `RawUnionFlat`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. The child elements' own compatibility is their invariant 1
// (enforced generically by the slot helpers), carried through the `#[repr(C)]`
// `IndexedChild`; `result` is layout-compatible across `Rf` (proven in
// `index_result`); the remaining fields are `Rf`-free. `QUICK_EXIT` never
// affects layout.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawUnionFlat<'static, Active<'static>, AChild, false>;
    type S = RawUnionFlat<'static, Suspended, SChild, false>;
    assert!(offset_of!(A, children) == offset_of!(S, children));
    assert!(offset_of!(A, num_active) == offset_of!(S, num_active));
    assert!(offset_of!(A, num_estimated) == offset_of!(S, num_estimated));
    assert!(offset_of!(A, is_eof) == offset_of!(S, is_eof));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Frees a suspended [`RawUnionFlat`]'s reused allocation after the in-place
/// resume consumed the child at `consumed`: drops the compacted resumed prefix
/// `children[..kept]` in place and the still-suspended tail past `consumed`,
/// skips the holes in between (moved-from or consumed), empties the `Vec` so it
/// frees only its buffer, then drops the box (freeing the still-suspended
/// `result` and the allocation).
///
/// # Safety
///
/// * `raw` must be exclusively owned and have come from `Box::into_raw`.
/// * `children[..kept]` must hold valid `IndexedChild<S::Resumed<'a>>` values,
///   `children[kept..=consumed]` must be moved-from or consumed (they are not
///   touched), and `children[consumed + 1..]` must hold valid `IndexedChild<S>`
///   values — the exact state the in-place resume loop leaves behind when the
///   slot helper reports `Err` for element `consumed`.
unsafe fn free_after_consumed_child<'query, 'a, S, const QUICK_EXIT: bool>(
    raw: *mut RawUnionFlat<'query, Suspended, S, QUICK_EXIT>,
    kept: usize,
    consumed: usize,
) where
    S: RQESuspendedIterator<'query>,
    'query: 'a,
{
    // SAFETY: `raw` is exclusively owned (caller contract); the borrow is used
    // only to reach the buffer pointer, the length, and `set_len`.
    let children: &mut Vec<IndexedChild<S>> = unsafe { &mut (*raw).children };
    let len = children.len();
    let base = children.as_mut_ptr();
    for i in 0..kept {
        // SAFETY: `i` is in bounds of the children buffer.
        let slot = unsafe { base.add(i) };
        // SAFETY: the slot holds a valid resumed child (caller contract); drop
        // it through the resumed type (same size/alignment, enforced by the
        // slot helper's const guard).
        unsafe { std::ptr::drop_in_place(slot.cast::<IndexedChild<S::Resumed<'a>>>()) };
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
    // still-suspended `result`, `Rf`-free scalars); reclaim and drop it.
    drop(unsafe { Box::from_raw(raw) });
}

// Methods used in both modes.
impl<'index, I, const QUICK_EXIT: bool> UnionFlat<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Creates a new flat union iterator. If `children` is empty, returns an
    /// iterator immediately at EOF.
    #[must_use]
    pub fn new(children: Vec<I>) -> Self {
        let num_estimated: usize = children.iter().map(|c| c.num_estimated()).sum();
        let num_children = children.len();
        let children: Vec<IndexedChild<I>> = children
            .into_iter()
            .enumerate()
            .map(|(i, inner)| IndexedChild {
                original_index: i,
                inner,
            })
            .collect();

        if children.is_empty() {
            return Self {
                children,
                num_active: 0,
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::build_union(0).build(),
            };
        }

        Self {
            children,
            num_active: num_children,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::build_union(num_children).build(),
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
    /// Returns `None` if the child was permanently removed (e.g. aborted during
    /// revalidation). Scans the children to find the one whose `original_index`
    /// matches, so this is O(n) — intended for profile display, not hot-path
    /// iteration.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        self.children
            .iter()
            .find(|c| c.original_index == idx)
            .map(|c| &c.inner)
    }

    /// Returns a mutable iterator over all children (including exhausted ones).
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.children.iter_mut().map(|c| &mut c.inner)
    }

    /// Consumes the iterator and returns its children.
    pub fn into_children(self) -> Vec<I> {
        self.children.into_iter().map(|c| c.inner).collect()
    }

    /// Consumes the iterator and returns a [`super::UnionTrimmed`] over the same children,
    /// or [`None`] if there are fewer than 3 children.
    pub fn into_trimmed(self, limit: usize, asc: bool) -> Option<super::UnionTrimmed<'index, I>> {
        let children: Vec<I> = self.children.into_iter().map(|c| c.inner).collect();
        (children.len() >= 3).then(|| super::UnionTrimmed::new(children, limit, asc))
    }

    /// Settles the union's position after its children have moved, and reports where
    /// that leaves it.
    ///
    /// The single place that decides a union's post-change position, shared by
    /// [`RQEIterator::revalidate`] and
    /// [`RQESuspendedIterator::resume`] so the two
    /// cannot drift apart — the legacy and the `Box<Self>` path must make the same
    /// re-seek and moved-versus-unchanged decisions, and a divergence between them is a
    /// bug. Each caller re-admits every child first (`num_active = children.len()`),
    /// because removing an aborted child invalidates the active/parked partition —
    /// not because a parked child can come back. The scan below re-parks the ones
    /// that are still exhausted.
    ///
    /// `original_last_doc_id` is the position the union held before the children moved.
    fn settle_after_children_changed(
        &mut self,
        original_last_doc_id: DocId,
    ) -> Result<SettleOutcome, RQEIteratorError> {
        // Only a child that has run past its last result is dropped: one that has
        // merely returned its final document still owes it, and dropping it would
        // lose that document — or report EOF for the whole union, if it was the
        // last active child.
        let mut min_doc_id: DocId = DocId::MAX;
        let mut min_child_idx: usize = 0;
        let mut i = 0;
        while i < self.num_active {
            let child = &self.children[i];
            if child.at_eof() {
                self.swap_remove_child(i);
                // Don't increment i - check the swapped-in child
            } else {
                let child_doc_id = child.last_doc_id();
                if child_doc_id < min_doc_id {
                    min_doc_id = child_doc_id;
                    min_child_idx = i;
                }
                i += 1;
            }
        }

        // Every remaining child is at EOF.
        if self.num_active == 0 {
            self.is_eof = true;
            return Ok(SettleOutcome::Eof);
        }

        // A minimum *behind* the union is not a position to move to — adopting it
        // would replay documents, because a reported move has the caller emit
        // `current` in place of a read and the read after that resumes from there.
        // `iterator_api.h` says `VALIDATE_MOVED` means the position moved
        // *forward*, and `Not::revalidate` asserts that of its child — which is
        // where a `QUICK_EXIT` union usually sits.
        //
        // With `QUICK_EXIT` this is the mode's own early return, and routine:
        // `skip_to_quick` stops on the first exact match, so a later sibling keeps
        // an earlier round's id, or answers 0 having never been read at all.
        //
        // A full union cannot get here: `advance_and_find_min` leaves no active
        // child behind the union, and exhaustion is terminal across a revalidation
        // *and* a resume (see [`RQEIterator::at_eof`]), so a child dropped on EOF
        // cannot re-enter the active set behind us on either path. Asserted rather
        // than compensated for — the recovery below is quick mode's, and a full
        // union arriving in it means a child is broken.
        if min_doc_id < original_last_doc_id {
            debug_assert!(
                QUICK_EXIT,
                "a full union's child moved behind the union's position: doc {min_doc_id} \
                 comes before doc {original_last_doc_id}",
            );

            // A child still sitting on the union's document backs the position as
            // it stands: republish from it (the result holds raw pointers into
            // children that may have moved or been dropped) and report no change,
            // with no reads spent. `min_child_idx` cannot serve — that is the
            // lagging child just rejected. Quick mode only: its laggers are
            // ordinary state for the next `read`/`skip_to` to seek past, while a
            // full union must catch them up below either way —
            // `advance_and_find_min` relies on no active child sitting behind the
            // union.
            if QUICK_EXIT
                && let Some(idx) = self.children[..self.num_active]
                    .iter()
                    .position(|c| c.last_doc_id() == original_last_doc_id)
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
            // may still *hold* it: `skip_to_quick`'s early return strands a
            // sibling without consuming its position. The union may not step over
            // the document before asking — under a `NOT`, its position is not a
            // delivered result but the next id the `NOT` has yet to exclude, and
            // stepping over it would let that document into the result set. So
            // every lagger is seeked to the abandoned position itself, not past
            // it.
            //
            // These reads cannot be avoided. An `Err` here reaches the caller as
            // `VALIDATE_ABORTED`, freeing the iterator and substituting an empty
            // one — blunt, but better than handing out a position no child backs.
            let mut i = 0;
            while i < self.num_active {
                if self.children[i].last_doc_id() >= original_last_doc_id {
                    i += 1;
                    continue;
                }
                match self.children[i].skip_to(original_last_doc_id)? {
                    Some(SkipToOutcome::Found(_)) => {
                        if QUICK_EXIT {
                            // Still matched after all: the union stays on it,
                            // backed by this child; remaining laggers wait for the
                            // next call.
                            self.quick_set_from_child(i);

                            debug_assert_eq!(
                                self.last_doc_id(),
                                original_last_doc_id,
                                "staying put must leave the position untouched",
                            );

                            return Ok(SettleOutcome::Unchanged);
                        }
                        i += 1;
                    }
                    Some(SkipToOutcome::NotFound(_)) => {
                        i += 1;
                    }
                    None => {
                        // The seek exhausted the child. Don't increment i — the
                        // swapped-in element needs to be checked.
                        self.swap_remove_child(i);
                    }
                }
            }

            // Seeking the laggers forward can run the union out of documents.
            if self.num_active == 0 {
                self.is_eof = true;
                return Ok(SettleOutcome::Eof);
            }

            // Every active child now sits at or past the abandoned position, so
            // the minimum is a position again: equal to it when children still
            // match the document (full mode only — quick mode returned above),
            // later otherwise.
            min_doc_id = DocId::MAX;
            for (i, child) in self.children[..self.num_active].iter().enumerate() {
                let child_doc_id = child.last_doc_id();
                if child_doc_id < min_doc_id {
                    min_doc_id = child_doc_id;
                    min_child_idx = i;
                }
            }

            debug_assert!(
                min_doc_id >= original_last_doc_id,
                "every lagging child was just seeked to the union's position",
            );
        }

        // Rebuild result at the new minimum doc_id
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

    /// Swap-removes an exhausted child at `idx` by swapping it with the last active child.
    #[inline]
    fn swap_remove_child(&mut self, idx: usize) {
        debug_assert!(idx < self.num_active);
        self.num_active -= 1;
        if idx < self.num_active {
            self.children.swap(idx, self.num_active);
        }
    }

    /// Adds a single child's current result to the aggregate.
    /// Assumes the aggregate has already been reset if needed.
    fn add_child_to_result(&mut self, child_idx: usize) {
        let child = &mut self.children[child_idx];
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

// Methods reachable only when `QUICK_EXIT` is `false`, grouped so that the full-mode
// read path reads together. Each guards its mode at entry rather than being typed on
// `UnionFlat<'index, I, false>`: the caller is the generic `RQEIterator` impl, which
// cannot reach a method that exists for only one value of a const generic (E0599),
// and splitting that impl in two would strand its `ProfileChildren` dependent.
impl<'index, I, const QUICK_EXIT: bool> UnionFlat<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Advances all active children sitting on `current_id` and finds the minimum
    /// doc_id in a single pass.
    ///
    /// Returns the minimum doc_id among active children, or `DocId::MAX` if all are exhausted.
    ///
    /// Only [`Self::read_full`] calls this, so no child can be *behind* `current_id`:
    /// full-mode [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to)
    /// advance every active child, and neither
    /// [`revalidate`](RQEIterator::revalidate) nor
    /// [`resume`](RQESuspendedIterator::resume) can leave one behind the union — a child dropped
    /// on EOF stays dropped, since exhaustion is terminal
    /// ([`at_eof`](RQEIterator::at_eof)). A child behind would become the minimum
    /// and hand back a document already delivered, so the invariant is asserted
    /// rather than handled.
    fn advance_and_find_min(&mut self, current_id: DocId) -> Result<DocId, RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union reads through skip_to instead");
        }

        let mut min_id: DocId = DocId::MAX;
        let mut i = 0;

        while i < self.num_active {
            let child = &mut self.children[i];

            debug_assert!(
                child.last_doc_id() >= current_id,
                "a full union's child cannot fall behind it: {} < {current_id}",
                child.last_doc_id(),
            );

            // Advance children that match the current doc_id
            if child.last_doc_id() == current_id {
                let read_result = child.read()?;
                // If read returned None, the child has no more documents
                if read_result.is_none() {
                    self.swap_remove_child(i);
                    // Don't increment i - we need to check the swapped-in child
                    continue;
                }
                // Otherwise, child.last_doc_id() was updated by read(); the child
                // is still positioned on that document, so it stays active.
            }

            // Track minimum doc_id (fused with advance loop)
            let doc_id = child.last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }

            i += 1;
        }

        Ok(min_id)
    }

    /// Performs initial read on all children to position them at their first document.
    /// Removes any children that are immediately exhausted (empty iterators).
    /// Returns the minimum doc_id among active children, or `DocId::MAX` if all are exhausted.
    fn initialize_children(&mut self) -> Result<DocId, RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union positions its children through skip_to instead");
        }

        let mut min_id: DocId = DocId::MAX;
        let mut i = 0;
        while i < self.num_active {
            let child = &mut self.children[i];

            // Handle children that haven't been read yet (last_doc_id == 0)
            if child.last_doc_id() == 0 {
                // Check if already at EOF (e.g., empty iterator)
                if child.at_eof() {
                    self.swap_remove_child(i);
                    continue;
                }
                // Perform initial read, also sets child.last_doc_id()
                let read_result = child.read()?;
                if read_result.is_none() {
                    self.swap_remove_child(i);
                    continue;
                }
            }
            // Track minimum doc_id
            let doc_id = child.last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }
            i += 1;
        }
        Ok(min_id)
    }

    /// Builds the result from active children whose `last_doc_id` equals `min_id`.
    /// Only used in Full mode - aggregates ALL matching children.
    fn build_aggregate_result(&mut self, min_id: DocId) {
        if QUICK_EXIT {
            panic!("a quick union never aggregates; it reports a single child");
        }

        self.result.reset_aggregate();
        self.result.doc_id = min_id;

        for child in &mut self.children[..self.num_active] {
            if child.last_doc_id() == min_id
                && let Some(child_result) = child.current()
            {
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

    /// Full mode read - advances matching children and finds minimum in a single fused pass.
    fn read_full(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union reads through read_quick");
        }

        let previous_id = self.last_doc_id();
        let min_id = if previous_id == 0 {
            self.initialize_children()?
        } else {
            self.advance_and_find_min(previous_id)?
        };

        if min_id == DocId::MAX {
            self.is_eof = true;
            return Ok(None);
        }

        debug_assert!(
            min_id > previous_id,
            "a read must move forward: {previous_id} -> {min_id}",
        );

        self.build_aggregate_result(min_id);
        Ok(Some(&mut self.result))
    }

    /// Full mode skip_to - scans all active children and aggregates all matches.
    /// Removes exhausted children via swap-remove.
    ///
    /// Optimization: When a child's `skip_to` returns `Found` (exact match) or when a child
    /// is already at the target doc_id, we add it to the result immediately during the loop.
    /// This avoids a second pass when the target is found (matching C's `UI_Skip_Full_Flat`).
    fn skip_to_full(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if QUICK_EXIT {
            panic!("a quick union skips through skip_to_quick");
        }

        let mut min_id: DocId = DocId::MAX;
        let mut i = 0;

        // Reset aggregate before potentially adding children during the loop
        self.result.reset_aggregate();

        while i < self.num_active {
            let child = &mut self.children[i];

            let child_last_id = child.last_doc_id();

            // Already at or past target doc_id
            if child_last_id >= doc_id {
                if child_last_id < min_id {
                    min_id = child_last_id;
                }
                if child_last_id == doc_id {
                    self.add_child_to_result(i);
                }
                i += 1;
                continue;
            }

            // Call skip_to directly - it handles EOF internally and returns None
            match child.skip_to(doc_id)? {
                Some(SkipToOutcome::Found(r)) => {
                    let id = r.doc_id;
                    if id < min_id {
                        min_id = id;
                    }
                    self.add_child_to_result(i);
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    let id = r.doc_id;
                    if id < min_id {
                        min_id = id;
                    }
                }
                None => {
                    // Child exhausted - swap-remove and continue without incrementing i
                    self.swap_remove_child(i);
                    continue;
                }
            }
            i += 1;
        }

        if min_id == DocId::MAX {
            self.is_eof = true;
            return Ok(None);
        }

        if min_id == doc_id {
            self.result.doc_id = min_id;
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            // NotFound case: need a second pass to collect children at min_id
            self.build_aggregate_result(min_id);
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }
}

// Methods reachable only when `QUICK_EXIT` is `true`. See the note above.
impl<'index, I, const QUICK_EXIT: bool> UnionFlat<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Quick mode read - delegates to `skip_to(last_doc_id + 1)`.
    fn read_quick(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if !QUICK_EXIT {
            panic!("a full union reads through read_full");
        }

        let next_id = self.last_doc_id().saturating_add(1);
        match self.skip_to(next_id)? {
            Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    /// Quick mode skip_to - returns immediately on first exact match.
    /// Tracks minimum doc_id among non-matches for NotFound case.
    fn skip_to_quick(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if !QUICK_EXIT {
            panic!("a full union skips through skip_to_full");
        }

        // Use MAX as sentinel like C uses DOCID_MAX - avoids Option overhead
        let mut min_id: DocId = DocId::MAX;
        let mut min_child_idx: usize = 0;
        let mut i = 0;

        while i < self.num_active {
            let child = &mut self.children[i];

            let child_last_id = child.last_doc_id();

            if child_last_id < doc_id {
                // Child is behind - need to skip
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(_)) => {
                        // Found exact match - set result and return immediately
                        self.quick_set_from_child(i);
                        return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        // Track as potential minimum
                        let id = r.doc_id;
                        if id < min_id {
                            min_id = id;
                            min_child_idx = i;
                        }
                    }
                    None => {
                        // Child reached EOF - swap-remove
                        self.swap_remove_child(i);
                        continue;
                    }
                }
            } else if child_last_id == doc_id {
                // Found exact match - set result and return immediately
                self.quick_set_from_child(i);
                return Ok(Some(SkipToOutcome::Found(&mut self.result)));
            } else {
                // child_last_id > doc_id: Child is ahead - track as potential minimum
                if child_last_id < min_id {
                    min_id = child_last_id;
                    min_child_idx = i;
                }
            }
            i += 1;
        }

        // No exact match found - use minimum if available
        if min_id != DocId::MAX {
            self.quick_set_from_child(min_child_idx);
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        } else {
            self.is_eof = true;
            Ok(None)
        }
    }

    /// Sets the union result from a single child: resets aggregate, sets doc_id, adds child.
    /// Used in Quick mode where we only need one matching child.
    fn quick_set_from_child(&mut self, child_idx: usize) {
        if !QUICK_EXIT {
            panic!("a full union aggregates every matching child instead");
        }

        let child = &mut self.children[child_idx];

        self.result.reset_aggregate();
        self.result.doc_id = child.last_doc_id();

        self.add_child_to_result(child_idx);
    }
}

// ============================================================================
// RQEIterator implementation for UnionFlat
// ============================================================================

impl<'index, I, const QUICK_EXIT: bool> RQEIterator<'index> for UnionFlat<'index, I, QUICK_EXIT>
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

        if QUICK_EXIT {
            self.skip_to_quick(doc_id)
        } else {
            self.skip_to_full(doc_id)
        }
    }

    fn rewind(&mut self) {
        // Restore children to their original insertion order.
        self.children.sort_unstable_by_key(|c| c.original_index);

        self.num_active = self.children.len();
        self.is_eof = self.children.is_empty();
        self.result.reset_aggregate();
        self.children.iter_mut().for_each(|c| c.rewind());
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
        // Already at EOF - nothing to do
        if self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        let original_last_doc_id = self.last_doc_id();
        let mut any_change = false;

        // Revalidate ALL children (including exhausted ones past num_active) and remove aborted ones.
        // The exhausted ones still hold index references, and an aborted one has to go either way —
        // but they cannot come back active, since exhaustion is terminal (see
        // [`RQEIterator::at_eof`]).
        // We use index-based iteration because we need to remove elements while iterating.
        let mut i = 0;
        while i < self.children.len() {
            match self.children[i].revalidate(spec)? {
                RQEValidateStatus::Aborted => {
                    // Remove aborted child using swap_remove for O(1) removal.
                    // Order doesn't matter for union iteration.
                    self.children.swap_remove(i);
                    any_change = true;
                    // Don't increment i - the swapped element needs to be checked
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

        // If all children aborted, we abort too (union of nothing is nothing)
        if self.children.is_empty() {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Aborted);
        }

        // Early return if nothing changed
        if !any_change {
            return Ok(RQEValidateStatus::Ok);
        }

        // Removing an aborted child pulls an arbitrary other one into its slot, so
        // the active/parked split no longer describes anything: rebuild it from
        // scratch and let the settle re-park the children that are still exhausted.
        self.num_active = self.children.len();

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
    for UnionFlat<'index, I, QUICK_EXIT>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawUnionFlat<'index, Suspended, I::Suspended, QUICK_EXIT>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Walk children: dispatch each child's `suspend` through the trait
        // so dyn-erased `I` (e.g. [`TypeErasedRQEIterator`](crate::TypeErasedRQEIterator))
        // correctly transitions its vtable. For concrete-typed `I` this is
        // the same whole-box cast that would otherwise happen at the outer
        // level, just per-child.
        //
        // Walked through the buffer pointer rather than `iter_mut`, as `resume`
        // does: each transition retypes a slot from `I` to `I::Suspended`, so a
        // typed `IterMut<IndexedChild<I>>` would still be describing the buffer
        // as `IndexedChild<I>` after the first one. Nothing here re-reads a
        // transitioned slot, so that iterator would not actually be misused —
        // but the borrow claims something that stops being true half way
        // through, and every other caller of the slot helper already hands it a
        // raw pointer.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` and is exclusively owned
            // for the rest of this function, so the children Vec is reachable
            // and unaliased. The borrow is used only to reach the buffer pointer
            // and length, and ends here.
            let children: &mut Vec<IndexedChild<I>> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` addresses a valid `IndexedChild`; `&raw mut` forms
            // a pointer to its `inner` slot without going through a reference.
            let inner = unsafe { &raw mut (*elem).inner };
            // SAFETY: `inner` holds a valid, owned `I`; the helper leaves the
            // slot holding a valid `I::Suspended`.
            unsafe { crate::boxed::suspend_child_slot_in_place(inner) };
        }
        // SAFETY: `RawUnionFlat` is `#[repr(C)]` over `Vec<IndexedChild<I>>`
        // (now byte-rewritten with `I::Suspended` payloads) and
        // `result: RawIndexResult<Rf>` (layout-compatible via `SharedPtr`).
        unsafe {
            Box::from_raw(raw as *mut RawUnionFlat<'index, Suspended, I::Suspended, QUICK_EXIT>)
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
/// is exactly what [`build_aggregate_result`](RawUnionFlat::build_aggregate_result)
/// evaluates on the read path — so it can be recomputed from the survivors in
/// one forward pass, and the entries never have to be identified at all.
///
/// The read path narrows that scan to the active region and this one does not,
/// which comes to the same set: a child parked past `num_active` is one that ran
/// out, and an exhausted child publishes no `current()` to be counted.
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
/// one the union picked before suspending — it picks by scan order over the
/// active region, this picks by slot over every survivor — but any child on the
/// document backs the position equally, which is the same latitude
/// [`settle_after_children_changed`](RawUnionFlat::settle_after_children_changed)
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
    children: impl IntoIterator<Item = &'child mut R>,
) -> RebuildOutcome
where
    R: RQEIterator<'a> + 'child,
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
    for child in children {
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
    for RawUnionFlat<'query, Suspended, S, QUICK_EXIT>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = UnionFlat<'a, S::Resumed<'a>, QUICK_EXIT>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // `current()` hands the result out mutably, so "still a union result" is
        // a runtime invariant rather than an enforced one — a consumer could
        // have replaced it with an index-backed result of another kind. Nothing
        // below would notice: `rebuild_borrowed_entries` has nothing to rebuild
        // for a result that borrows nothing, and the cast would
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
        if self.result.kind() != RSResultKind::Union {
            return Ok(ResumeOutcome::Aborted);
        }

        // The pre-resume position, read off the suspended form. Unlike
        // `revalidate` there is no is-EOF return before the walk: the suspended
        // children must be transitioned regardless of the outcome.
        let original_last_doc_id = self.result.doc_id;

        let raw = Box::into_raw(self);

        // Resume every child *in place* — including the exhausted tail past
        // `num_active`, which is not optional. The cast below retypes the whole
        // buffer at once, so a slot left suspended would be read as an
        // `S::Resumed<'a>`, and even dropping it would be undefined behaviour.
        // Exhaustion also does not put a child beyond reach: [`rewind`] re-admits
        // every one of them, so a child skipped here on the grounds that it was
        // spent would be a suspended iterator the very next rewind rewinds and
        // reads. Aborted children are removed, like `revalidate`'s aborted
        // children: survivors are compacted down over the holes.
        //
        // [`rewind`]: RQEIterator::rewind
        //
        // The compaction preserves the survivors' relative order where
        // `revalidate` uses `swap_remove`, which pulls the last child into the
        // hole instead. The surviving *set* is the same and `original_index`
        // keeps each child identifiable, so the two orders differ only in the
        // shared settle's tie-breaks between children sitting on the *same*
        // document (`min_child_idx` takes the first, and `QUICK_EXIT`'s
        // `position(..)` likewise): the union's document sequence is identical
        // either way, only which of several equally-matching children backs a
        // `QUICK_EXIT` payload can differ. Matching `swap_remove` here would mean
        // pulling still-suspended children out of the tail and re-entering slots
        // the walk has already transitioned, so the buffer would no longer be a
        // resumed prefix followed by a suspended suffix — the split
        // `free_after_consumed_child` relies on to tear the allocation down.
        // Compacting is the deliberate choice.
        //
        // Whatever the walk does to the children, the suspended aggregate's
        // entries are dealt with before the cast — the one step that turns
        // pointers whose addresses happen to have survived into usable
        // references. Which way round that goes is decided below, and hinges on
        // whether the compaction ran.
        //
        // A panic between slot transitions leaks the allocation (memory-safe);
        // the slot helper guards its own moved-out window.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
            // initialised, exclusively owned); the borrow is used only to reach
            // the buffer pointer and length.
            let children: &mut Vec<IndexedChild<S>> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        let mut any_change = false;
        let mut kept = 0usize;
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid `IndexedChild`; `&raw mut` forms a
            // pointer to its `inner` slot.
            let inner = unsafe { &raw mut (*elem).inner };
            // SAFETY: `inner` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it on
            // `Aborted`/`Err`.
            match unsafe { resume_child_slot_in_place(inner, guard) } {
                Ok(outcome) => {
                    match outcome {
                        ResumeSlotOutcome::Unchanged => {}
                        ResumeSlotOutcome::Moved => any_change = true,
                        ResumeSlotOutcome::Aborted => {
                            // Dropped from the union, as `revalidate` drops an
                            // aborted child; the hole is compacted over by later
                            // survivors.
                            any_change = true;
                            continue;
                        }
                    }
                    if kept < i {
                        // SAFETY: `kept < i < len`, both in bounds.
                        let dst = unsafe { base.add(kept) };
                        // SAFETY: slot `kept` was vacated (its element moved left
                        // or was consumed) and `elem` holds a resumed child, so
                        // this is a single-element move between disjoint,
                        // in-bounds slots. `copy_nonoverlapping` is a move — the
                        // source is treated as vacated from here on.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                elem.cast::<IndexedChild<S::Resumed<'a>>>().cast_const(),
                                dst.cast::<IndexedChild<S::Resumed<'a>>>(),
                                1,
                            )
                        };
                    }
                    kept += 1;
                }
                Err(e) => {
                    // The one place resume cannot match `revalidate`, and it is
                    // structural rather than a choice. A spent union — `is_eof`,
                    // nothing left to read — makes `revalidate` return `Ok`
                    // before it looks at a child, so a child that would time out
                    // is never asked and the union survives. Resume has to ask
                    // every child before it can hand any of them back, and
                    // `S::resume` takes the child *by value*: once it answers
                    // `Err` the child is gone, so there is no walking past it to
                    // reach the `is_eof` exit below. The union is torn down and
                    // the error reaches the caller, where `revalidate` would have
                    // reported `Ok`.
                    //
                    // Pinned by `resume_of_a_spent_union_surfaces_a_child_error`
                    // rather than left to be rediscovered.
                    //
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
        // SAFETY: `raw` is exclusively owned; the borrow is confined to this
        // statement.
        let children = unsafe { &mut (*raw).children };
        // SAFETY: `children[..kept]` holds the compacted, initialised survivors
        // and everything past it is vacated, so shrinking to `kept` describes
        // exactly the live elements; `set_len` never drops, so the vacated tail
        // is left alone. The stale `num_active` is clamped right after the cast
        // below.
        unsafe { children.set_len(kept) };

        // Every survivor now sits in its resumed form, and the aggregate has to be
        // dealt with before the cast — the step that makes its entries usable
        // again, rather than merely re-narrowed.
        //
        // Every survivor is offered, including any parked past `num_active`: the
        // rebuild decides membership by asking each child where it sits, and a
        // parked child is exactly one that will decline. Which is also why the
        // compaction above needs no special handling here — nothing is matched
        // against a slot, so nothing is confused by a survivor having moved into
        // a dropped child's slot.
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
            // `IndexedChild<S::Resumed<'a>>` — same size and alignment as the
            // `IndexedChild<S>` the buffer was allocated for, by `#[repr(C)]`
            // over a child slot whose halves the slot helper statically
            // enforces — and `raw` is exclusively owned, so the slice is
            // unaliased.
            let children = unsafe {
                std::slice::from_raw_parts_mut(base.cast::<IndexedChild<S::Resumed<'a>>>(), kept)
            };
            rebuild_borrowed_entries::<_, QUICK_EXIT>(
                result,
                children.iter_mut().map(|c| &mut c.inner),
            )
        };

        // SAFETY: every surviving child slot holds its resumed form inside the
        // `Vec`'s untouched buffer, and the aggregate's entries have just been
        // re-derived from those survivors or dropped outright, so no entry is
        // re-narrowed onto a freed, relocated, or merely retagged result; the
        // remaining fields are `Rf`-free. Layout-identical
        // to the suspended form by invariant 1 on `RawUnionFlat` (const proof
        // above). `Box::from_raw` reuses the same allocation, so the FFI's cached
        // `header.current` and any parent's pointer into `result` stay valid
        // across the cycle.
        let mut active =
            unsafe { Box::from_raw(raw.cast::<UnionFlat<'a, S::Resumed<'a>, QUICK_EXIT>>()) };
        // Aborted children shrank the vec; the active region cannot outgrow it.
        active.num_active = active.num_active.min(active.children.len());

        // From here on, mirror `revalidate` decision for decision — including
        // the order the two questions are asked in. An already-finished union
        // stays finished, whatever its children now report: `revalidate` returns
        // before it looks at a single one, so asking "did they all abort?" first
        // would tear down a spent union that `revalidate` leaves alone. Its
        // position is preserved (`result.doc_id` rode along in the cast), exactly
        // as `revalidate` leaves it. The children were still transitioned above —
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
        // aggregate describes, so it stands as-is.
        if !any_change {
            // Unless the aggregate could not be re-derived. This is the only exit
            // that hands a live position back with no rebuild under it, so a union
            // that can no longer say which children back that position aborts
            // rather than publish one that describes nothing. (The `is_eof` exit
            // above needs no such guard: it publishes no `current`, so a cleared
            // aggregate has no observer there and `revalidate`'s `Ok` stays the
            // faithful answer.)
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

        // As in `revalidate`: the compaction over an aborted child moved survivors
        // between the active region and the parked tail, so the split describes
        // nothing until it is rebuilt. The settle re-parks the children that are
        // still exhausted — every one that was, since resume cannot revive them —
        // and re-derives the union's position.
        active.num_active = active.children.len();
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
    for UnionFlat<'index, crate::c2rust::CRQEIterator, QUICK_EXIT>
{
    fn profile_children(self) -> Self {
        UnionFlat {
            children: self
                .children
                .into_iter()
                .map(|c| IndexedChild {
                    original_index: c.original_index,
                    inner: c.inner.into_profiled(),
                })
                .collect(),
            num_active: self.num_active,
            num_estimated: self.num_estimated,
            is_eof: self.is_eof,
            result: self.result,
        }
    }
}
