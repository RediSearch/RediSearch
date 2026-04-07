/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Flat array variant of the union iterator with O(n) min-finding.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    utils::ActiveChildren,
};

/// Yields documents appearing in ANY child iterator using a flat array scan.
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
/// - `'index`: Lifetime of the index data.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
pub struct UnionFlat<'index, I, const QUICK_EXIT: bool> {
    /// Child iterators. Exhausted children are deactivated in O(1) rather
    /// than removed, so that children stay in insertion order for profile
    /// display and aggregate-result ordering. On rewind, all children are
    /// reactivated. Aborted children are permanently removed during
    /// revalidation — this is safe because profile display reads children
    /// dynamically (via [`ActiveChildren`]) rather than from a cached snapshot.
    children: ActiveChildren<I>,
    /// Sum of all children's estimated counts (upper bound).
    num_estimated: usize,
    /// Whether the iterator has reached EOF (all children exhausted).
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

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

        if children.is_empty() {
            return Self {
                children: ActiveChildren::new(children),
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::build_union(0).build(),
            };
        }

        Self {
            children: ActiveChildren::new(children),
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
        self.children.num_active()
    }

    /// Returns a shared reference to the child at `idx`.
    pub fn child_at(&self, idx: usize) -> &I {
        self.children.get(idx)
    }

    /// Returns a mutable iterator over all children (including exhausted ones).
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.children.iter_all_mut()
    }

    /// Consumes the iterator and returns a [`super::UnionTrimmed`] over the same children.
    pub fn into_trimmed(self, limit: usize, asc: bool) -> super::UnionTrimmed<'index, I> {
        super::UnionTrimmed::new(self.children.into_inner(), limit, asc)
    }

    /// Advances all active children whose `last_doc_id` equals `current_id` and finds the
    /// minimum doc_id in a single pass.
    ///
    /// Returns the minimum doc_id among active children, or `t_docId::MAX` if all are exhausted.
    fn advance_and_find_min(&mut self, current_id: t_docId) -> Result<t_docId, RQEIteratorError> {
        let mut min_id: t_docId = t_docId::MAX;
        let mut i = 0;

        while i < self.children.len() {
            if !self.children.is_active(i) {
                i += 1;
                continue;
            }

            let child = self.children.get_mut(i);

            // Advance children that match the current doc_id
            if child.last_doc_id() == current_id {
                let read_result = child.read()?;
                // If read returned None, the child has no more documents
                if read_result.is_none() {
                    self.children.deactivate(i);
                    i += 1;
                    continue;
                }
                // Otherwise, child.last_doc_id() was updated by read()
                // Even if at_eof() is now true, last_doc_id() is valid for this round
            }

            // Track minimum doc_id (fused with advance loop)
            let doc_id = self.children.get(i).last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }

            i += 1;
        }

        Ok(min_id)
    }

    /// Builds the result from active children whose `last_doc_id` equals `min_id`.
    /// Only used in Full mode - aggregates ALL matching children.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.result.reset_aggregate();
        self.result.doc_id = min_id;

        for (_, child) in self.children.iter_active_mut() {
            if child.last_doc_id() == min_id
                && let Some(child_result) = child.current()
            {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: We need a raw pointer to decouple the borrow of the child's
                // result from `&mut self.result`. This is sound because:
                // 1. The child (via `self.children`) and `self.result` are disjoint fields — no aliasing.
                // 2. `push_borrowed` takes a shared reference, so no mutation through child_ref.
                // 3. The child is owned by `self`, so the 'index data remains valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);
            }
        }
    }

    /// Performs initial read on all children to position them at their first document.
    /// Deactivates any children that are immediately exhausted (empty iterators).
    /// Returns the minimum doc_id among active children, or `t_docId::MAX` if all are exhausted.
    fn initialize_children(&mut self) -> Result<t_docId, RQEIteratorError> {
        let mut min_id: t_docId = t_docId::MAX;
        let mut i = 0;
        while i < self.children.len() {
            if !self.children.is_active(i) {
                i += 1;
                continue;
            }

            let child = self.children.get_mut(i);

            // Handle children that haven't been read yet (last_doc_id == 0)
            if child.last_doc_id() == 0 {
                // Check if already at EOF (e.g., empty iterator)
                if child.at_eof() {
                    self.children.deactivate(i);
                    i += 1;
                    continue;
                }
                // Perform initial read, also sets child.last_doc_id()
                let read_result = child.read()?;
                if read_result.is_none() {
                    self.children.deactivate(i);
                    i += 1;
                    continue;
                }
            }
            // Track minimum doc_id
            let doc_id = self.children.get(i).last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }
            i += 1;
        }
        Ok(min_id)
    }

    /// Full mode read - advances matching children and finds minimum in a single fused pass.
    fn read_full(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let min_id = if self.last_doc_id() == 0 {
            self.initialize_children()?
        } else {
            self.advance_and_find_min(self.last_doc_id())?
        };

        if min_id == t_docId::MAX {
            self.is_eof = true;
            return Ok(None);
        }

        self.build_aggregate_result(min_id);
        Ok(Some(&mut self.result))
    }

    /// Quick mode read - delegates to `skip_to(last_doc_id + 1)`.
    fn read_quick(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let next_id = self.last_doc_id().saturating_add(1);
        match self.skip_to(next_id)? {
            Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    /// Full mode skip_to - scans all active children and aggregates all matches.
    /// Removes exhausted children, preserving order.
    ///
    /// Optimization: When a child's `skip_to` returns `Found` (exact match) or when a child
    /// is already at the target doc_id, we add it to the result immediately during the loop.
    /// This avoids a second pass when the target is found (matching C's `UI_Skip_Full_Flat`).
    fn skip_to_full(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let mut min_id: t_docId = t_docId::MAX;
        let mut i = 0;

        // Reset aggregate before potentially adding children during the loop
        self.result.reset_aggregate();

        while i < self.children.len() {
            if !self.children.is_active(i) {
                i += 1;
                continue;
            }

            let child = self.children.get_mut(i);
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
                    // Child exhausted
                    self.children.deactivate(i);
                    i += 1;
                    continue;
                }
            }
            i += 1;
        }

        if min_id == t_docId::MAX {
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

    /// Quick mode skip_to - returns immediately on first exact match.
    /// Tracks minimum doc_id among non-matches for NotFound case.
    fn skip_to_quick(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // Use MAX as sentinel like C uses DOCID_MAX - avoids Option overhead
        let mut min_id: t_docId = t_docId::MAX;
        let mut min_child_idx: usize = 0;
        let mut i = 0;

        while i < self.children.len() {
            if !self.children.is_active(i) {
                i += 1;
                continue;
            }

            let child = self.children.get_mut(i);
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
                        // Child reached EOF - deactivate
                        self.children.deactivate(i);
                        i += 1;
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
        if min_id != t_docId::MAX {
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
        let child = self.children.get_mut(child_idx);

        self.result.reset_aggregate();
        self.result.doc_id = child.last_doc_id();

        self.add_child_to_result(child_idx);
    }

    /// Adds a single child's current result to the aggregate.
    /// Assumes the aggregate has already been reset if needed.
    fn add_child_to_result(&mut self, child_idx: usize) {
        let child = self.children.get_mut(child_idx);
        if let Some(child_result) = child.current() {
            let child_ptr: *const RSIndexResult<'index> = child_result;
            // SAFETY: We need a raw pointer to decouple the borrow of the child's
            // result from `&mut self.result`. This is sound because:
            // 1. The child (via `self.children`) and `self.result` are disjoint fields — no aliasing.
            // 2. `push_borrowed` takes a shared reference, so no mutation through child_ref.
            // 3. The child is owned by `self`, so the 'index data remains valid.
            let child_ref = unsafe { &*child_ptr };
            self.result.push_borrowed(child_ref);
        }
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
        doc_id: t_docId,
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
        self.children.activate_all();
        self.is_eof = self.children.is_empty();
        self.result.reset_aggregate();
        self.children.iter_all_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Already at EOF - nothing to do
        if self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        let original_last_doc_id = self.last_doc_id();
        let mut any_change = false;

        // Revalidate ALL children (including inactive/exhausted ones) and remove aborted ones.
        // Exhausted children must be revalidated because they may become active again after revalidation.
        // We use index-based iteration because we need to remove elements while iterating.
        let mut i = 0;
        while i < self.children.len() {
            match self.children.get_mut(i).revalidate()? {
                RQEValidateStatus::Aborted => {
                    // Permanently remove aborted child.
                    self.children.remove(i);
                    any_change = true;
                    // Don't increment i - the shifted element needs to be checked
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

        // Re-activate all children and then deactivate those at EOF.
        self.children.activate_all();
        let mut min_doc_id: t_docId = t_docId::MAX;
        let mut min_child_idx: usize = 0;
        for i in 0..self.children.len() {
            let child = self.children.get(i);
            if child.at_eof() {
                self.children.deactivate(i);
            } else {
                let child_doc_id = child.last_doc_id();
                if child_doc_id < min_doc_id {
                    min_doc_id = child_doc_id;
                    min_child_idx = i;
                }
            }
        }

        // Check if all remaining children are at EOF
        if self.children.num_active() == 0 {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        // Rebuild result at the new minimum doc_id
        if QUICK_EXIT {
            self.quick_set_from_child(min_child_idx);
        } else {
            self.build_aggregate_result(min_doc_id);
        }

        // Return MOVED only if lastDocId changed
        if self.last_doc_id() != original_last_doc_id {
            Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
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
    for UnionFlat<'index, crate::c2rust::CRQEIterator, QUICK_EXIT>
{
    fn profile_children(self) -> Self {
        let profiled: Vec<_> = self
            .children
            .into_inner()
            .into_iter()
            .map(crate::c2rust::CRQEIterator::into_profiled)
            .collect();
        UnionFlat {
            children: ActiveChildren::new(profiled),
            num_estimated: self.num_estimated,
            is_eof: self.is_eof,
            result: self.result,
        }
    }
}
