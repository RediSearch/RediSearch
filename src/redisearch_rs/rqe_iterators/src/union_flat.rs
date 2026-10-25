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
use std::ptr::NonNull;

use crate::union::{
    FreeSuspendedShell, RebuildOutcome, SettleOutcome, rebuild_borrowed_entries,
    restore_gathered_metrics,
};
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
///
/// # Invariants
///
/// 1. **Layout compatibility across the child swap.** `RawUnionFlat` is
///    `#[repr(C)]` over a `Vec` of `#[repr(C)]` [`IndexedChild`] elements and a
///    [`RawIndexResult`], so `RawUnionFlat<Active, I>` and
///    `RawUnionFlat<Suspended, I::Suspended>` are layout-identical given that
///    each child `I` and its `I::Suspended` are. This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) / [`resume`](RQESuspendedIterator::resume)
///    reuse the union's allocation and its children buffer across the cycle.
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
        // A move to another document publishes that document's metrics, drained
        // afresh by the rebuild; staying put has to restore these.
        let gathered = std::mem::take(&mut self.result.metrics);

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
        // A move is forward by contract, and `Not::revalidate` asserts that of its
        // child — which is where a `QUICK_EXIT` union usually sits.
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

                restore_gathered_metrics::<QUICK_EXIT>(&mut self.result, gathered);
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

                            restore_gathered_metrics::<QUICK_EXIT>(&mut self.result, gathered);
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
            restore_gathered_metrics::<QUICK_EXIT>(&mut self.result, gathered);
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
            let inner =
                NonNull::new(unsafe { &raw mut (*elem).inner }).expect("child slot is non-null");
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
        // have replaced it with an index-backed result of another kind, and the
        // cast would then re-narrow whatever suspended pointers the substitute
        // holds without re-validating them. A union has no way to re-validate a
        // payload it did not build, so it refuses, on `&self`, before
        // `Box::into_raw` opens the raw-pointer section — the same shape
        // `Optional` and `NotOptimized` use for their virtual sentinels. It is
        // also what lets `rebuild_borrowed_entries` take the borrowed aggregate
        // as given.
        //
        // The test is the exact kind, not `is_aggregate()`: that also admits
        // `HybridMetric`, whose children are *owned* boxes rather than borrowed
        // entries, and whose own `data` may still be index-backed and
        // suspended — a case no union is equipped to re-validate.
        //
        // The kind alone is not enough: an aggregate of kind `Union` can be the
        // *owned* representation, whose children are boxed into the result's own
        // allocation and are never transitioned. Safe code can build one —
        // `to_owned()` on a union result, `push_boxed` an index-backed term into
        // it, assign it through `current()` — and a boxed child's `data` can be
        // index-backed and suspended, so re-narrowing it would promote pointers
        // nothing re-validated. Demanding the borrowed representation is what
        // closes that.
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

        // Resume every child *in place*, including the exhausted tail past
        // `num_active`: the cast below retypes the whole buffer, and [`rewind`]
        // re-admits exhausted children, so none may be left suspended. Aborted
        // children are removed, as in `revalidate`.
        //
        // [`rewind`]: RQEIterator::rewind
        //
        // Survivors are compacted down over the holes rather than
        // `swap_remove`d: pulling a still-suspended child out of the tail would
        // break the resumed-prefix / suspended-suffix split
        // [`FreeSuspendedShell`] tears down by. The surviving set is the same;
        // only tie-breaks between children on the same document can differ,
        // which changes which child backs a `QUICK_EXIT` payload, never the
        // documents yielded.
        //
        // The aggregate's entries are then rebuilt from the survivors, or
        // cleared for the settle to rebuild, before the cast; see
        // `rebuild_borrowed_entries`.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
            // initialised, exclusively owned); the borrow is used only to reach
            // the buffer pointer and length.
            let children: &mut Vec<IndexedChild<S>> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        // SAFETY: `raw` is exclusively owned and non-null, so projecting to its
        // `children` field is in bounds; no reference is created here.
        let children_ptr = unsafe { &raw mut (*raw).children };
        // From here until the cast, the buffer is part-resumed and part-suspended
        // and `shell` owns it: every early return below, and any panic, frees it
        // through the guard's drop rather than through a call the exit has to
        // remember to make.
        //
        // SAFETY: `raw` came from `Box::into_raw` and passes to the guard,
        // `children_ptr` is its children `Vec`, and with that emptied the rest
        // of the shell (a still-suspended `result` and `Rf`-free scalars) drops
        // as a suspended union.
        let mut shell = unsafe {
            FreeSuspendedShell::<_, IndexedChild<S>, IndexedChild<S::Resumed<'a>>>::new(
                raw,
                children_ptr,
            )
        };
        let mut any_change = false;
        for _ in 0..len {
            // The slot counts as vacated for as long as the helper owns its
            // contents, so the boundary moves *before* the call, not after: on
            // `Err` the child is consumed and the guard must already know not to
            // drop it.
            let i = shell.vacate_next();
            // SAFETY: `i` is in bounds of the children buffer.
            let elem = unsafe { base.add(i) };
            // SAFETY: `elem` holds a valid `IndexedChild`; `&raw mut` forms a
            // pointer to its `inner` slot.
            let inner =
                NonNull::new(unsafe { &raw mut (*elem).inner }).expect("child slot is non-null");
            // SAFETY: `inner` holds a valid, owned `S`; the helper rewrites it as
            // a valid `S::Resumed<'a>` on `Unchanged`/`Moved`, and consumes it on
            // `Aborted`/`Err`.
            //
            // An `Err` returns through `?`, dropping `shell` — even for a spent
            // union, which `revalidate` would report `Ok` without asking any
            // child: the child was consumed, so there is no union left to hand
            // back. Pinned by `resume_of_a_spent_union_surfaces_a_child_error`.
            match unsafe { resume_child_slot_in_place(inner, guard) }? {
                ResumeSlotOutcome::Unchanged => {}
                ResumeSlotOutcome::Moved => any_change = true,
                ResumeSlotOutcome::Aborted => {
                    // Dropped from the union, as `revalidate` drops an aborted
                    // child; the hole is compacted over by later survivors.
                    any_change = true;
                    continue;
                }
            }
            // SAFETY: slot `i`, the one last vacated, now holds a valid, owned
            // resumed child: the helper rewrote it on `Unchanged`/`Moved`, and
            // `Aborted` skipped this call.
            unsafe { shell.keep_last_vacated() };
        }
        let kept = shell.kept();
        // SAFETY: `raw` is exclusively owned; the borrow is confined to this
        // statement.
        let children = unsafe { &mut (*raw).children };
        // SAFETY: `children[..kept]` holds the compacted, initialised survivors
        // and everything past it is vacated, so shrinking to `kept` describes
        // exactly the live elements; `set_len` never drops, so the vacated tail
        // is left alone. The stale `num_active` is clamped right after the cast
        // below.
        unsafe { children.set_len(kept) };

        // Only the `!any_change` exit below publishes the aggregate without a
        // settle. On every other path the settle swaps children and seeks
        // laggers before rebuilding the aggregate itself, which would leave any
        // entry derived here stale, so those paths only clear the records.
        // `freq` and `field_mask` are left for the same rebuild to reset; the
        // settle's `Eof` exit publishes no `current()` to read them.
        //
        // Where it does run, every survivor is offered, including any parked
        // past `num_active`: the rebuild asks each child where it sits, so
        // neither parking nor the compaction above needs special handling. They
        // are offered in slot order, the order the read path scans them in, so
        // a full union's entries come back as `build_aggregate_result` pushed
        // them and a `QUICK_EXIT` union keeps the backer it reported: the first
        // child on the document by slot. Every quick-mode pick — `skip_to_quick`,
        // and the settle's stay-put and lagger seek — scans slots in order, so
        // no other record of the backer is needed (`UnionHeap`, whose backer is
        // the heap root, has to find it by address).
        let rebuilt = {
            // Viewed at `'a` so the rebuilt entries carry the children's
            // lifetime; `&mut` is invariant, so the narrowing needs a cast.
            //
            // SAFETY: `raw` is exclusively owned and non-null, so projecting to
            // its `result` field is in bounds; no reference is created here.
            let result_ptr = unsafe { &raw mut (*raw).result };
            // SAFETY: the projection above is valid, aligned and initialised, and
            // holds a suspended result in a field disjoint from the children
            // buffer; `raw` is exclusively owned and the borrow is confined to
            // this block. `RawIndexResult` differs between the two lifetimes only
            // in the query-pipeline pointers it claims, and `'query: 'a` makes
            // every one of them valid for `'a`.
            let result: &mut RawIndexResult<'a, Suspended> =
                unsafe { &mut *result_ptr.cast::<RawIndexResult<'a, Suspended>>() };
            let doc_id = result.doc_id;
            // `freq` and `field_mask` sit beside the aggregate in `result`, which
            // it borrows whole, so they are rebuilt in copies and written back.
            let (mut freq, mut field_mask) = (result.freq, result.field_mask);
            let aggregate = result
                .as_aggregate_mut()
                .and_then(|aggregate| aggregate.as_borrowed_mut())
                .expect("resume admits only a borrowed union aggregate");
            if any_change {
                aggregate.reset();
                None
            } else {
                // SAFETY: `base` addresses the `kept` survivors, each a valid
                // `IndexedChild<S::Resumed<'a>>` — same size and alignment as the
                // `IndexedChild<S>` the buffer was allocated for, by `#[repr(C)]`
                // over a child slot whose halves the slot helper statically
                // enforces — and `raw` is exclusively owned, so the slice is
                // unaliased.
                let children = unsafe {
                    std::slice::from_raw_parts_mut(
                        base.cast::<IndexedChild<S::Resumed<'a>>>(),
                        kept,
                    )
                };
                let rebuilt = rebuild_borrowed_entries::<_, QUICK_EXIT>(
                    aggregate,
                    doc_id,
                    &mut freq,
                    &mut field_mask,
                    children.iter_mut().map(|c| &mut c.inner),
                );
                result.freq = freq;
                result.field_mask = field_mask;
                Some(rebuilt)
            }
        };

        // Disarmed only now: until the cast the elements are typed as suspended
        // while holding resumed children, so an unwind must go through the guard.
        shell.disarm();

        // SAFETY: every surviving child slot holds its resumed form inside the
        // `Vec`'s untouched buffer, and the aggregate's entries have just been
        // rebuilt from those survivors or cleared, so no entry is re-narrowed
        // onto a freed, relocated, or merely retagged result; the remaining fields are `Rf`-free. Layout-identical to the
        // suspended form by invariant 1 on `RawUnionFlat` (const proof above).
        // `Box::from_raw` reuses the same allocation, so the FFI's cached
        // `header.current` and any parent's pointer into `result` stay valid
        // across the cycle.
        let mut active =
            unsafe { Box::from_raw(raw.cast::<UnionFlat<'a, S::Resumed<'a>, QUICK_EXIT>>()) };
        // Aborted children shrank the vec; the active region cannot outgrow it.
        active.num_active = active.num_active.min(active.children.len());

        // From here on, mirror `revalidate` decision for decision, in the same
        // order: a spent union stays spent whatever its children report. Unlike
        // `revalidate`, its aborted children are already gone, which a later
        // `rewind` can observe — deliberately, since their state is
        // unrecoverable. Pinned by
        // `resume_of_a_spent_union_drops_children_that_revalidate_would_rewind`.
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
            // Unless the aggregate could not be rebuilt: this is the only exit
            // that publishes a live position without a settle behind it, so an
            // unbacked one aborts rather than describe nothing. Only a child
            // breaking `Unchanged`'s promise to hold its position can get here.
            let rebuilt = rebuilt.expect("the aggregate is rebuilt whenever no child changed");
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
