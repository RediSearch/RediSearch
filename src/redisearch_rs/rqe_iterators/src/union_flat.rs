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

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Yields documents appearing in ANY child iterator using a flat array scan.
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// `UnionFlat` yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated (unless `QUICK_EXIT` is `true`).
///
/// Uses O(n) min-finding by scanning all children. Best for small numbers of children
/// (typically <20) due to minimal memory overhead and cache-friendly iteration.
///
/// For large numbers of children, consider using [`crate::UnionHeap`] instead.
///
/// # Type Parameters
///
/// - `'index`: Lifetime of the index data.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
pub struct UnionFlat<'index, I, const QUICK_EXIT: bool> {
    /// Child iterators. Active children are in `children[..num_active]`,
    /// exhausted children are moved to the end via swap-remove.
    children: Vec<I>,
    /// Number of active (non-EOF) children. Only `children[..num_active]` are scanned.
    num_active: usize,
    /// Last doc_id successfully yielded (returned by `last_doc_id()`).
    last_doc_id: t_docId,
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
                children,
                num_active: 0,
                last_doc_id: 0,
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::union(0),
            };
        }

        Self {
            children,
            num_active: num_children,
            last_doc_id: 0,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::union(num_children),
        }
    }

    /// Advances all active children whose `last_doc_id` equals `current_id` and finds the
    /// minimum doc_id in a single pass. This matches the C implementation's fused loop pattern.
    ///
    /// Returns the minimum doc_id among active children, or None if all are exhausted.
    /// Removes exhausted children via swap-remove to shrink the active set.
    fn advance_and_find_min(
        &mut self,
        current_id: t_docId,
    ) -> Result<Option<t_docId>, RQEIteratorError> {
        let mut min_id = t_docId::MAX;
        let mut i = 0;

        while i < self.num_active {
            let child = &mut self.children[i];

            // Advance children that match the current doc_id
            if child.last_doc_id() == current_id {
                let read_result = child.read()?;
                // If read returned None, the child has no more documents
                if read_result.is_none() {
                    self.swap_remove_child(i);
                    // Don't increment i - we need to check the swapped-in child
                    continue;
                }
                // Otherwise, child.last_doc_id() was updated by read()
                // Even if at_eof() is now true, last_doc_id() is valid for this round
            }

            // Track minimum doc_id (fused with advance loop)
            let doc_id = child.last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }

            i += 1;
        }

        if self.num_active == 0 {
            Ok(None)
        } else {
            Ok(Some(min_id))
        }
    }

    /// Swap-removes an exhausted child at `idx` by swapping it with the last active child.
    /// This is O(1) and avoids shifting elements.
    #[inline]
    fn swap_remove_child(&mut self, idx: usize) {
        debug_assert!(idx < self.num_active);
        self.num_active -= 1;
        if idx < self.num_active {
            self.children.swap(idx, self.num_active);
        }
    }

    /// Builds the result from active children whose `last_doc_id` equals `min_id`.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.last_doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = min_id;

        for child in &mut self.children[..self.num_active] {
            if child.last_doc_id() == min_id
                && let Some(child_result) = child.current()
            {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: child_ptr points to child's result containing data with 'index
                // lifetime. Children are owned by self, so their results remain valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);

                if QUICK_EXIT {
                    return;
                }
            }
        }
    }

    /// Performs initial read on all children to position them at their first document.
    /// Removes any children that are immediately exhausted (empty iterators).
    /// Returns the minimum doc_id among active children, or None if all are exhausted.
    fn initialize_children(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
        let mut min_id = t_docId::MAX;
        let mut i = 0;
        while i < self.num_active {
            let child = &mut self.children[i];
            if child.last_doc_id() == 0 && !child.at_eof() {
                let read_result = child.read()?;
                // If read returned None, the child is empty (no documents)
                if read_result.is_none() {
                    self.swap_remove_child(i);
                    continue;
                }
                // Otherwise, child.last_doc_id() was set by read()
            }
            // Track minimum doc_id
            let doc_id = child.last_doc_id();
            if doc_id < min_id {
                min_id = doc_id;
            }
            i += 1;
        }
        if self.num_active == 0 {
            Ok(None)
        } else {
            Ok(Some(min_id))
        }
    }

    /// Full mode skip_to - scans all active children and aggregates all matches.
    /// Removes exhausted children via swap-remove.
    ///
    /// Uses `t_docId::MAX` sentinel instead of `Option<t_docId>` to avoid unwrap overhead.
    fn skip_to_full(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // Use MAX as sentinel like C uses DOCID_MAX - avoids Option overhead
        let mut min_id: t_docId = t_docId::MAX;
        let mut i = 0;

        while i < self.num_active {
            let child = &mut self.children[i];

            // Cache last_doc_id to avoid redundant calls
            let child_last_id = child.last_doc_id();

            // Already at or past target doc_id
            if child_last_id >= doc_id {
                if child_last_id < min_id {
                    min_id = child_last_id;
                }
                i += 1;
                continue;
            }

            // Need to skip forward
            if !child.at_eof() {
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => {
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
            } else {
                // Child already at EOF - swap-remove
                self.swap_remove_child(i);
                continue;
            }
            i += 1;
        }

        if min_id == t_docId::MAX {
            self.is_eof = true;
            return Ok(None);
        }

        self.build_aggregate_result(min_id);

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    /// Quick mode skip_to - returns immediately on first exact match.
    /// Tracks minimum doc_id among non-matches for NotFound case.
    /// Removes exhausted children via swap-remove.
    ///
    /// Optimizations vs naive implementation:
    /// - Uses `t_docId::MAX` sentinel instead of `Option<t_docId>` to avoid unwrap overhead
    /// - Caches `last_doc_id()` result to avoid redundant calls (C accesses a struct field directly)
    /// - Uses a single comparison chain like C's implementation
    fn skip_to_quick(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // Use MAX as sentinel like C uses DOCID_MAX - avoids Option overhead
        let mut min_id: t_docId = t_docId::MAX;
        let mut min_child_idx: usize = 0;
        let mut i = 0;

        while i < self.num_active {
            let child = &mut self.children[i];

            // Cache last_doc_id to avoid multiple calls (matches C's direct field access pattern)
            let child_last_id = child.last_doc_id();

            if child_last_id < doc_id {
                // Child is behind - need to skip (or remove if at EOF)
                if child.at_eof() {
                    // Update min_child_idx if it points to the child being swapped in
                    if min_child_idx == self.num_active - 1 {
                        min_child_idx = i;
                    }
                    self.swap_remove_child(i);
                    continue;
                }

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
                        if min_child_idx == self.num_active - 1 {
                            min_child_idx = i;
                        }
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
        if min_id != t_docId::MAX {
            self.quick_set_from_child(min_child_idx);
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        } else {
            self.is_eof = true;
            Ok(None)
        }
    }

    /// Sets the union result from a single child (quick mode only).
    /// This avoids iterating over all children like build_aggregate_result does.
    fn quick_set_from_child(&mut self, child_idx: usize) {
        let child = &mut self.children[child_idx];
        self.last_doc_id = child.last_doc_id();
        self.result.doc_id = self.last_doc_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }

        if let Some(child_result) = child.current() {
            let child_ptr: *const RSIndexResult<'index> = child_result;
            // SAFETY: child_ptr points to child's result containing data with 'index
            // lifetime. Children are owned by self, so their results remain valid.
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

        // Quick mode optimization: delegate to skip_to(last_doc_id + 1)
        // This matches the C implementation's UI_Read_Quick_Flat pattern.
        if QUICK_EXIT {
            let next_id = self.last_doc_id.saturating_add(1);
            return match self.skip_to(next_id)? {
                Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => Ok(Some(r)),
                None => Ok(None),
            };
        }

        // Full mode: advance matching children and find minimum in a single fused pass.
        // This matches the C implementation's UI_Read_Full_Flat pattern.
        let min_id = if self.last_doc_id == 0 {
            self.initialize_children()?
        } else {
            self.advance_and_find_min(self.last_doc_id)?
        };

        let Some(min_id) = min_id else {
            self.is_eof = true;
            return Ok(None);
        };

        self.build_aggregate_result(min_id);
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        debug_assert!(self.last_doc_id < doc_id);

        // Quick mode optimization: return immediately on first exact match.
        // This matches the C implementation's UI_Skip_Quick_Flat pattern.
        if QUICK_EXIT {
            return self.skip_to_quick(doc_id);
        }

        // Full mode: must scan all children to find all matches
        self.skip_to_full(doc_id)
    }

    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        // Reset num_active to include all children again
        self.num_active = self.children.len();
        self.is_eof = self.children.is_empty();
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.children.iter_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let original_last_doc_id = self.last_doc_id;
        let mut any_child_moved = false;
        let mut any_child_aborted = false;
        let mut min_doc_id = t_docId::MAX;
        let mut active_children = 0usize;

        for child in &mut self.children {
            match child.revalidate()? {
                RQEValidateStatus::Aborted => {
                    any_child_aborted = true;
                }
                RQEValidateStatus::Moved { current } => {
                    any_child_moved = true;
                    if let Some(result) = current {
                        if result.doc_id < min_doc_id {
                            min_doc_id = result.doc_id;
                        }
                        active_children += 1;
                    }
                }
                RQEValidateStatus::Ok => {
                    if !child.at_eof() {
                        if child.last_doc_id() < min_doc_id {
                            min_doc_id = child.last_doc_id();
                        }
                        active_children += 1;
                    }
                }
            }
        }

        // For union, we only abort if ALL children aborted
        if any_child_aborted {
            // Note: In a more complete implementation, we'd track which children aborted
        }

        if !any_child_moved || self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        if active_children == 0 {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        self.build_aggregate_result(min_doc_id);

        if self.last_doc_id != original_last_doc_id {
            Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }
}
