/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Flat array variant of the union iterator with O(n) min-finding.

use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::union::SettleOutcome;
use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
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
    /// [`RQESuspendedIterator::resume`](crate::RQESuspendedIterator::resume) so the two
    /// cannot drift apart — the legacy and the `Box<Self>` path must make the same
    /// re-seek and moved-versus-unchanged decisions, and a divergence between them is a
    /// bug. Each caller sets `num_active` first, because they disagree on it
    /// legitimately: `revalidate` re-admits every child, while `resume` must keep the
    /// live/dead split it rebuilt (a dead child's `last_doc_id` would otherwise be
    /// picked up as a spurious minimum and yielded again).
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

        // Without `QUICK_EXIT` no child can fall behind the union: every active child
        // is advanced on every `read`/`skip_to`, and a child's own `revalidate` may only
        // move it forward — one that has run past its end reports no `current` and was
        // dropped by the scan above. So the minimum is at worst *equal* to the current
        // position, which is simply a child still sitting on the document it supplied.
        debug_assert!(
            QUICK_EXIT || min_doc_id >= original_last_doc_id,
            "a full union's child cannot fall behind it: {min_doc_id} < {original_last_doc_id}",
        );

        // With `QUICK_EXIT` it can: `skip_to_quick` returns on the first exact match, so
        // a later sibling keeps an earlier round's id, or answers 0 having never been
        // read at all. Such a minimum is not a position to move to — adopting it would
        // replay documents, because a reported move has the caller emit `current` in
        // place of a read and the read after that resumes from there. `iterator_api.h`
        // says `VALIDATE_MOVED` means the position moved *forward*, and
        // `Not::revalidate` asserts that of its child — which is where a `QUICK_EXIT`
        // union usually sits.
        //
        // `QUICK_EXIT` is a const generic, so a full union compiles this away entirely
        // rather than paying for a comparison that its invariant already rules out.
        if QUICK_EXIT && min_doc_id < original_last_doc_id {
            // The result has to be republished either way, because it holds raw pointers
            // into the children's own results: one that moved leaves it describing
            // another document, and one that aborted was dropped above and leaves it
            // dangling. What it can be republished *from* decides the outcome.
            if self.republish_at(original_last_doc_id) {
                // Still backed, so the union has not moved and its current stands.
                debug_assert_eq!(
                    self.last_doc_id(),
                    original_last_doc_id,
                    "staying put must leave the position untouched",
                );

                return Ok(SettleOutcome::Unchanged);
            }

            // Nothing is left on the union's document: the child that supplied it has
            // moved on too. Reporting no change would promise a `current` that no child
            // backs.
            //
            // So the union advances instead, which is what it would have done on the next
            // read anyway — `read_quick` targets `last_doc_id() + 1` and seeks every
            // lagging child past it, including the one whose position was rejected here.
            // Skipping over the abandoned document costs nothing: it has already been
            // delivered, and a quick union never promised to aggregate every child that
            // holds it, so no contribution is dropped by leaving it behind.
            //
            // This is the one place settling reads. An `Err` here reaches the caller as
            // `VALIDATE_ABORTED`, which frees the iterator and substitutes an empty one —
            // the failure is reported, if bluntly, which beats publishing a result that
            // describes nothing.
            return Ok(if self.read_quick()?.is_some() {
                SettleOutcome::Moved
            } else {
                // Seeking past the abandoned position ran the union out of documents.
                SettleOutcome::Eof
            });
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

    /// Republishes the result at `doc_id`, returning whether any active child backs it.
    ///
    /// From a *single* child in `QUICK_EXIT` mode — the full aggregate is what that mode
    /// exists to avoid — and from every child holding `doc_id` otherwise.
    ///
    /// Full mode always answers `true`: its children never fall behind, so the only
    /// position it is ever asked to republish is one a child still holds.
    fn republish_at(&mut self, doc_id: DocId) -> bool {
        if QUICK_EXIT {
            // The minimum's index cannot serve here: that is the lagging child whose
            // position was rejected. Every child in the active region has a `current`
            // (the scan dropped the rest), so finding one by position is enough.
            match self.children[..self.num_active]
                .iter()
                .position(|c| c.last_doc_id() == doc_id)
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
// `UnionFlat<'index, I, false>`: the callers are the generic `RQEIterator` impl, which cannot
// reach a method that exists for only one value of a const generic, and splitting that
// impl in two would also strand the `ProfileChildren` impl, whose `RQEIterator`
// supertrait bound is stated for a generic `QUICK_EXIT`.
impl<'index, I, const QUICK_EXIT: bool> UnionFlat<'index, I, QUICK_EXIT>
where
    I: RQEIterator<'index>,
{
    /// Advances all active children sitting on `current_id` and finds the minimum
    /// doc_id in a single pass.
    ///
    /// Returns the minimum doc_id among active children, or `DocId::MAX` if all are exhausted.
    ///
    /// Only [`Self::read_full`] calls this, so `QUICK_EXIT` is always `false` here and
    /// no child can be *behind* `current_id`: a full union advances every child on
    /// every `read`/`skip_to`, and a child's own `revalidate` may only move it forward.
    /// A child that were behind would have to be seeked rather than read — one read
    /// need not clear `current_id` — and would otherwise become the minimum and hand
    /// back a document already delivered. That case is asserted away rather than
    /// handled, so it cannot be introduced silently.
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

        // The minimum is taken over children that were all advanced past
        // `previous_id`, so it has to name a later document. Handing back one this
        // union already delivered would have the caller emit it twice.
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
        // Exhausted children must be revalidated because they may become active again after revalidation.
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

        // Re-admit every child, including any parked past `num_active`: their own
        // revalidation may have brought them back into play.
        self.num_active = self.children.len();

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
        // SAFETY: `raw` came from `Box::into_raw` and is exclusively owned
        // for the rest of this function, so the children Vec is reachable
        // and unaliased.
        let children: &mut Vec<IndexedChild<I>> = unsafe { &mut (*raw).children };
        for child in children.iter_mut() {
            // SAFETY: `child.inner` is a valid `I` accessed via a fresh
            // `&mut`; the function leaves the slot in a valid
            // `I::Suspended` state.
            unsafe { crate::boxed::suspend_child_slot_in_place(&mut child.inner) };
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
        let RawUnionFlat {
            children,
            num_active,
            num_estimated,
            is_eof,
            result,
        } = *self;

        // Read plain-data fields off the suspended aggregate before
        // discarding it — the suspended aggregate's borrowed children
        // pointers would dangle after we consume the children by value.
        let saved_weight = result.weight;
        let saved_last_doc_id = result.doc_id;
        drop(result);

        // `swap_remove_child` keeps EOF children in the Vec at indices
        // `[num_active..]` so [`rewind`](RQEIterator::rewind) can sort them
        // back into the active set. Resume must preserve that split: the
        // tail children stay in their resumed-active form so they survive
        // a future rewind, but they don't count toward `num_active` (their
        // last_doc_id would otherwise be picked up as a spurious min and
        // yielded again after the live children exhaust).
        //
        // No need to track whether any child moved: settling below re-finds the minimum
        // unconditionally, and reports `Unchanged` when it turns out to be where the
        // union already was.
        let mut live: Vec<IndexedChild<S::Resumed<'a>>> = Vec::with_capacity(num_active);
        let mut dead: Vec<IndexedChild<S::Resumed<'a>>> =
            Vec::with_capacity(children.len().saturating_sub(num_active));
        for (
            i,
            IndexedChild {
                original_index,
                inner,
            },
        ) in children.into_iter().enumerate()
        {
            let active_inner = match Box::new(inner).resume(guard)? {
                ResumeOutcome::Aborted => continue,
                ResumeOutcome::Moved(active_inner) | ResumeOutcome::Ok(active_inner) => {
                    *active_inner
                }
            };
            let resumed = IndexedChild {
                original_index,
                inner: active_inner,
            };
            if i < num_active {
                live.push(resumed);
            } else {
                dead.push(resumed);
            }
        }
        let num_children = live.len();
        let mut active_children = live;
        active_children.extend(dead);
        let result = RSIndexResult::build_union(num_children)
            .weight(saved_weight)
            .build();

        let mut active: Box<UnionFlat<'a, S::Resumed<'a>, QUICK_EXIT>> = Box::new(UnionFlat {
            children: active_children,
            num_active: num_children,
            num_estimated,
            is_eof,
            result,
        });

        if active.is_eof || saved_last_doc_id == 0 {
            return Ok(ResumeOutcome::Ok(active));
        }

        // `num_children == 0` here means every previously-live child
        // ABORTED during resume (children that reached EOF naturally went
        // into `dead` and don't count toward `num_children`). The union
        // has no recoverable state.
        if num_children == 0 {
            return Ok(ResumeOutcome::Aborted);
        }

        // The result was rebuilt from scratch above, so it has to be repopulated whether
        // or not a child moved — which is why this settles unconditionally rather than
        // short-circuiting on `any_change` the way `revalidate` can. With nothing moved
        // the recomputed minimum *is* `saved_last_doc_id`, so settling reports
        // `Unchanged`, and the special case would only be a second copy of logic that
        // has to agree with the shared one.
        //
        // `num_active` was set to the live count above and must stay that way: the dead
        // tail is kept for a later `rewind`, and re-admitting it here would let a dead
        // child's position become a spurious minimum.
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
