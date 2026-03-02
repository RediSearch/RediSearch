/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Union iterator implementations.
//!
//! The union iterator yields documents appearing in ANY child iterator (OR semantics).
//! Two separate types are provided for different performance characteristics:
//!
//! - [`UnionFlat`]: Uses a flat array scan for O(n) min-finding. Best for small
//!   numbers of children (typically <20). No heap overhead.
//!
//! - [`UnionHeap`]: Uses a binary heap for O(log n) min-finding. Better for large
//!   numbers of children (typically >20). Has heap allocation overhead.
//!
//! Both types support a `QUICK_EXIT` const generic:
//! - If `true`, returns after finding the first matching child without aggregating.
//! - If `false`, collects results from all children with the same document.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::util::DocIdMinHeap;
use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

// ============================================================================
// Type aliases for convenient access
// ============================================================================

/// Full mode, flat array - aggregates all matching children, O(n) min-finding.
/// This is the most common variant for small numbers of children.
pub type UnionFullFlat<'index, I> = UnionFlat<'index, I, false>;

/// Quick mode, flat array - returns after first match, O(n) min-finding.
pub type UnionQuickFlat<'index, I> = UnionFlat<'index, I, true>;

/// Full mode, heap - aggregates all matching children, O(log n) min-finding.
/// More efficient for large numbers of children (>20).
pub type UnionFullHeap<'index, I> = UnionHeap<'index, I, false>;

/// Quick mode, heap - returns after first match, O(log n) min-finding.
pub type UnionQuickHeap<'index, I> = UnionHeap<'index, I, true>;

/// Backwards compatibility alias - defaults to flat full mode.
pub type Union<'index, I> = UnionFullFlat<'index, I>;

// ============================================================================
// UnionFlat - Flat array variant with O(n) min-finding
// ============================================================================

/// Yields documents appearing in ANY child iterator using a flat array scan.
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// `UnionFlat` yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated (unless `QUICK_EXIT` is `true`).
///
/// Uses O(n) min-finding by scanning all children. Best for small numbers of children
/// (typically <20) due to minimal memory overhead and cache-friendly iteration.
///
/// For large numbers of children, consider using [`UnionHeap`] instead.
///
/// # Type Parameters
///
/// - `'index`: Lifetime of the index data.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
pub struct UnionFlat<'index, I, const QUICK_EXIT: bool> {
    /// Child iterators.
    children: Vec<I>,
    /// Last doc_id successfully yielded (returned by `last_doc_id()`).
    last_doc_id: t_docId,
    /// Sum of all children's estimated counts (upper bound).
    num_estimated: usize,
    /// Whether the iterator has reached EOF (all children exhausted).
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

// ============================================================================
// UnionHeap - Heap variant with O(log n) min-finding
// ============================================================================

/// Yields documents appearing in ANY child iterator using a binary heap.
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// `UnionHeap` yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated (unless `QUICK_EXIT` is `true`).
///
/// Uses O(log n) min-finding via a binary heap. Better for large numbers of children
/// (typically >20) where the heap overhead is outweighed by faster min-finding.
///
/// For small numbers of children, consider using [`UnionFlat`] instead.
///
/// # Type Parameters
///
/// - `'index`: Lifetime of the index data.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
/// - `QUICK_EXIT`: If `true`, returns immediately after finding any matching child.
///   If `false`, aggregates results from all children with the minimum doc_id.
pub struct UnionHeap<'index, I, const QUICK_EXIT: bool> {
    /// Child iterators.
    children: Vec<I>,
    /// Last doc_id successfully yielded (returned by `last_doc_id()`).
    last_doc_id: t_docId,
    /// Sum of all children's estimated counts (upper bound).
    num_estimated: usize,
    /// Whether the iterator has reached EOF (all children exhausted).
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
    /// Custom min-heap of (doc_id, child_index) for O(log n) min-finding.
    /// Provides efficient `replace_root` and `for_each_root` operations.
    heap: DocIdMinHeap,
}

// ============================================================================
// UnionFlat implementation
// ============================================================================

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
                last_doc_id: 0,
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::union(0),
            };
        }

        Self {
            children,
            last_doc_id: 0,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::union(num_children),
        }
    }

    /// Finds the minimum doc_id among all children that have a document we haven't yielded yet.
    /// Uses O(n) scan across all children.
    #[inline]
    fn find_min_doc_id(&self) -> Option<t_docId> {
        self.children
            .iter()
            .filter(|c| c.last_doc_id() > self.last_doc_id)
            .map(|c| c.last_doc_id())
            .min()
    }

    /// Advances all children whose `last_doc_id` equals `current_id`.
    fn advance_matching_children(&mut self, current_id: t_docId) -> Result<(), RQEIteratorError> {
        for child in &mut self.children {
            if child.last_doc_id() == current_id && !child.at_eof() {
                let _ = child.read()?;
            }
        }
        Ok(())
    }

    /// Builds the result from children whose `last_doc_id` equals `min_id`.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.last_doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = min_id;

        for child in &mut self.children {
            if child.last_doc_id() == min_id {
                if let Some(child_result) = child.current() {
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
    }

    /// Performs initial read on all children to position them at their first document.
    fn initialize_children(&mut self) -> Result<(), RQEIteratorError> {
        for child in &mut self.children {
            if child.last_doc_id() == 0 && !child.at_eof() {
                let _ = child.read()?;
            }
        }
        Ok(())
    }

    /// Full mode skip_to - scans all children and aggregates all matches.
    fn skip_to_full(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let mut min_id: Option<t_docId> = None;

        for child in &mut self.children {
            if child.last_doc_id() <= self.last_doc_id && child.at_eof() {
                continue;
            }

            if child.last_doc_id() >= doc_id {
                let id = child.last_doc_id();
                min_id = Some(min_id.map_or(id, |m| m.min(id)));
                continue;
            }

            if !child.at_eof() {
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => {
                        let id = r.doc_id;
                        min_id = Some(min_id.map_or(id, |m| m.min(id)));
                    }
                    None => {}
                }
            }
        }

        let Some(min_id) = min_id else {
            self.is_eof = true;
            return Ok(None);
        };

        self.build_aggregate_result(min_id);

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    /// Quick mode skip_to - returns immediately on first exact match.
    /// Tracks minimum doc_id among non-matches for NotFound case.
    fn skip_to_quick(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let mut min_id: Option<t_docId> = None;
        let mut min_child_idx: Option<usize> = None;

        for (idx, child) in self.children.iter_mut().enumerate() {
            if child.last_doc_id() <= self.last_doc_id && child.at_eof() {
                continue;
            }

            // Check if child already has the exact match
            if child.last_doc_id() == doc_id {
                // Found exact match - set result and return immediately
                self.quick_set_from_child(idx);
                return Ok(Some(SkipToOutcome::Found(&mut self.result)));
            }

            if child.last_doc_id() > doc_id {
                // Child is ahead - track as potential minimum
                let id = child.last_doc_id();
                if min_id.is_none() || id < min_id.unwrap() {
                    min_id = Some(id);
                    min_child_idx = Some(idx);
                }
                continue;
            }

            // Child is behind - need to skip
            if !child.at_eof() {
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(_)) => {
                        // Found exact match - set result and return immediately
                        self.quick_set_from_child(idx);
                        return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        // Track as potential minimum
                        let id = r.doc_id;
                        if min_id.is_none() || id < min_id.unwrap() {
                            min_id = Some(id);
                            min_child_idx = Some(idx);
                        }
                    }
                    None => {
                        // Child reached EOF
                    }
                }
            }
        }

        // No exact match found - use minimum if available
        match (min_id, min_child_idx) {
            (Some(_), Some(idx)) => {
                self.quick_set_from_child(idx);
                Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
            }
            _ => {
                self.is_eof = true;
                Ok(None)
            }
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
// UnionHeap implementation
// ============================================================================

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
                last_doc_id: 0,
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::union(0),
                heap: DocIdMinHeap::new(),
            };
        }

        Self {
            children,
            last_doc_id: 0,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::union(num_children),
            heap: DocIdMinHeap::with_capacity(num_children),
        }
    }

    /// Finds the minimum doc_id using the heap. O(1) peek.
    #[inline]
    fn find_min_doc_id(&self) -> Option<t_docId> {
        self.heap.peek().map(|(doc_id, _)| doc_id)
    }

    /// Rebuilds the heap from scratch based on current child positions.
    /// Used after revalidation when children may have moved arbitrarily.
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (idx, child) in self.children.iter().enumerate() {
            if !child.at_eof() && child.last_doc_id() > self.last_doc_id {
                self.heap.push(child.last_doc_id(), idx);
            }
        }
    }

    /// Advances children at the heap root whose `last_doc_id` equals `current_id`.
    /// Uses O(k log n) heap operations where k = number of matching children.
    ///
    /// Uses `replace_root` for O(log n) in-place replacement instead of pop+push.
    fn advance_matching_children(&mut self, current_id: t_docId) -> Result<(), RQEIteratorError> {
        // Process all entries at the root with the current minimum doc_id
        while let Some((doc_id, idx)) = self.heap.peek() {
            if doc_id != current_id {
                break;
            }

            // Advance this child
            let child = &mut self.children[idx];
            // Read returns Some if there's a new document, None if EOF
            if child.read()?.is_some() {
                // Use replace_root for O(log n) in-place replacement
                self.heap.replace_root(child.last_doc_id(), idx);
            } else {
                // Child is EOF - pop it from heap
                self.heap.pop();
            }
        }
        Ok(())
    }

    /// Builds the result from children at the heap root whose `last_doc_id` equals `min_id`.
    /// Uses O(k) `for_each_root` to iterate without modifying the heap.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.last_doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = min_id;

        // Collect matching child indices using O(k) for_each_root
        let mut matching_indices = Vec::new();
        self.heap.for_each_root(|_, idx| {
            matching_indices.push(idx);
        });

        // Build result from matching children
        for &idx in &matching_indices {
            if let Some(child_result) = self.children[idx].current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: child_ptr points to child's result containing data with 'index
                // lifetime. Children are owned by self, so their results remain valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);

                if QUICK_EXIT {
                    break;
                }
            }
        }
        // No need to push back - for_each_root doesn't modify the heap
    }

    /// Performs initial read on all children and builds the heap.
    fn initialize_children(&mut self) -> Result<(), RQEIteratorError> {
        for (idx, child) in self.children.iter_mut().enumerate() {
            if child.last_doc_id() == 0 && !child.at_eof() {
                // Read returns Some if successful
                if child.read()?.is_some() {
                    // Add to heap - child has a valid document position
                    self.heap.push(child.last_doc_id(), idx);
                }
            } else if child.last_doc_id() > 0 {
                // Child was already initialized (e.g., after rewind scenario)
                self.heap.push(child.last_doc_id(), idx);
            }
        }
        Ok(())
    }

    /// Quick mode skip_to - returns immediately on first exact match.
    /// Uses heap operations efficiently: stops as soon as exact match found.
    fn skip_to_quick(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // Track if we found an exact match and which child
        let mut found_exact_match: Option<usize> = None;

        // If heap is empty (first operation before any read), initialize by skipping all children
        if self.heap.is_empty() && self.last_doc_id == 0 {
            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        // Add to heap and mark as exact match
                        self.heap.push(r.doc_id, idx);
                        if found_exact_match.is_none() {
                            found_exact_match = Some(idx);
                        }
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {
                        // Child is EOF, don't add to heap
                    }
                }
            }

            // If we found exact match, return immediately with that child
            if let Some(idx) = found_exact_match {
                self.quick_set_from_child(idx);
                return Ok(Some(SkipToOutcome::Found(&mut self.result)));
            }
        } else {
            // Skip lagging children using heap operations.
            // While heap root is behind target, pop it, skip that child, push back.
            while let Some((child_doc_id, _)) = self.heap.peek() {
                if child_doc_id >= doc_id {
                    break;
                }
                let (_, idx) = self.heap.pop().unwrap();

                let child = &mut self.children[idx];
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        // Found exact match - push back and return immediately
                        self.heap.push(r.doc_id, idx);
                        self.quick_set_from_child(idx);
                        return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        // Push back with updated position
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {
                        // Child is EOF, don't push back
                    }
                }
            }
        }

        // Check if heap root is exact match (child was already at or past target)
        let Some((min_id, min_idx)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        // Use peek-only result building for quick mode
        self.quick_set_from_child(min_idx);

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    /// Sets the union result from a single child using heap peek (quick mode only).
    /// This avoids pop/push operations unlike build_aggregate_result.
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

        // Full mode: advance matching children and find minimum
        if self.last_doc_id == 0 {
            self.initialize_children()?;
        } else {
            self.advance_matching_children(self.last_doc_id)?;
        }

        let Some(min_id) = self.find_min_doc_id() else {
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

        // Quick mode optimization: delegate to skip_to(last_doc_id + 1)
        // This matches the C implementation's UI_Read_Quick_Heap pattern.
        if QUICK_EXIT {
            let next_id = self.last_doc_id.saturating_add(1);
            return match self.skip_to(next_id)? {
                Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => Ok(Some(r)),
                None => Ok(None),
            };
        }

        // Full mode: advance matching children and find minimum
        if self.last_doc_id == 0 {
            self.initialize_children()?;
        } else {
            self.advance_matching_children(self.last_doc_id)?;
        }

        let Some(min_id) = self.find_min_doc_id() else {
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
        // This matches the C implementation's UI_Skip_Quick_Heap pattern.
        if QUICK_EXIT {
            return self.skip_to_quick(doc_id);
        }

        // Full mode: must process all lagging children
        // If heap is empty (first operation before any read), initialize by skipping all children
        if self.heap.is_empty() && self.last_doc_id == 0 {
            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {
                        // Child is EOF, don't add to heap
                    }
                }
            }
        } else {
            // Skip lagging children using heap operations.
            // While heap root is behind target, pop it, skip that child, push back.
            while let Some((child_doc_id, _)) = self.heap.peek() {
                if child_doc_id >= doc_id {
                    break;
                }
                let (_, idx) = self.heap.pop().unwrap();

                let child = &mut self.children[idx];
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => {
                        // Push back with updated position
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {
                        // Child is EOF, don't push back
                    }
                }
            }
        }

        // Check if we have any children left
        let Some((min_id, _)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        self.build_aggregate_result(min_id);

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
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
        // Clear heap - it will be rebuilt on next read
        self.heap.clear();
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

        // Rebuild the heap since children may have moved arbitrarily
        self.rebuild_heap();

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
