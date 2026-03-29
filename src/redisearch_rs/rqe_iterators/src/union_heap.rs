/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Heap variant of the union iterator with O(log n) min-finding.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::util::DocIdMinHeap;
use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Yields documents appearing in ANY child iterator using a binary heap.
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
                result: RSIndexResult::build_union(0).build(),
                heap: DocIdMinHeap::new(),
            };
        }

        Self {
            children,
            last_doc_id: 0,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::build_union(num_children).build(),
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
    ///
    /// Uses DFS traversal over the heap structure, exploiting the heap property
    /// to prune subtrees: if a node's doc_id > min_id, all its descendants also
    /// have doc_id >= that value, so the entire subtree can be skipped. This is
    /// critical for the disjoint case where only 1 of N children matches.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.last_doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = min_id;

        // Inline heap traversal to avoid borrow conflicts with closure-based API.
        // We access heap.data() directly so we can also access children and result.
        let heap_data = self.heap.data();
        if heap_data.is_empty() {
            return;
        }

        let root_doc_id = heap_data[0].0;

        // Use fixed-size stack for DFS traversal (64 levels supports 2^64 elements)
        let mut stack = [0usize; 64];
        let mut stack_len = 1;
        stack[0] = 0;

        while stack_len > 0 {
            stack_len -= 1;
            let heap_idx = stack[stack_len];

            if heap_idx >= heap_data.len() {
                continue;
            }

            let (doc_id, child_idx) = heap_data[heap_idx];
            if doc_id != root_doc_id {
                // Heap property: children have >= doc_id, skip subtree
                continue;
            }

            // Push result directly from child
            if let Some(child_result) = self.children[child_idx].current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: child_ptr points to child's result containing data with 'index
                // lifetime. Children are owned by self, so their results remain valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);

                if QUICK_EXIT {
                    return;
                }
            }

            // Push heap children onto stack (right first so left is processed first)
            let left_heap_idx = 2 * heap_idx + 1;
            let right_heap_idx = 2 * heap_idx + 2;

            if right_heap_idx < heap_data.len() && stack_len < 63 {
                stack[stack_len] = right_heap_idx;
                stack_len += 1;
            }
            if left_heap_idx < heap_data.len() && stack_len < 63 {
                stack[stack_len] = left_heap_idx;
                stack_len += 1;
            }
        }
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

    /// Advances all lagging children to the target doc_id using heap operations.
    ///
    /// In QUICK_EXIT mode, returns `Some(child_idx)` immediately when an exact match
    /// is found (child landed exactly on doc_id). Otherwise returns `None`.
    ///
    /// This is the core skip logic shared by both quick and full modes.
    fn advance_children_to_target(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<usize>, RQEIteratorError> {
        // If heap is empty (first operation before any read), initialize by skipping all children
        if self.heap.is_empty() && self.last_doc_id == 0 {
            let mut first_exact_match: Option<usize> = None;

            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        self.heap.push(r.doc_id, idx);
                        // In QUICK_EXIT mode, track first exact match for early return
                        if QUICK_EXIT && first_exact_match.is_none() {
                            first_exact_match = Some(idx);
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

            // In QUICK_EXIT mode, return early with the exact match child index
            if QUICK_EXIT {
                return Ok(first_exact_match);
            }
        } else {
            // Skip lagging children using heap operations.
            // While heap root is behind target, skip that child and update its heap position.
            while let Some((child_doc_id, idx)) = self.heap.peek() {
                if child_doc_id >= doc_id {
                    break;
                }

                let child = &mut self.children[idx];
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        // Use replace_root: single sift-down instead of pop+push
                        self.heap.replace_root(r.doc_id, idx);
                        // In QUICK_EXIT mode, return early with exact match
                        if QUICK_EXIT {
                            return Ok(Some(idx));
                        }
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        // Use replace_root: single sift-down instead of pop+push
                        self.heap.replace_root(r.doc_id, idx);
                    }
                    None => {
                        // Child is EOF, remove from heap
                        self.heap.pop();
                    }
                }
            }
        }

        Ok(None)
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

        self.add_child_to_result(child_idx);
    }

    /// Adds a single child's current result to the aggregate.
    /// Assumes the aggregate has already been reset if needed.
    fn add_child_to_result(&mut self, child_idx: usize) {
        let child = &mut self.children[child_idx];
        if let Some(child_result) = child.current() {
            let child_ptr: *const RSIndexResult<'index> = child_result;
            // SAFETY: We need a raw pointer to decouple the borrow of the child's
            // result from `&mut self.result`. This is sound because:
            // 1. `self.children[i]` and `self.result` are disjoint fields — no aliasing.
            // 2. `push_borrowed` takes a shared reference, so no mutation through child_ref.
            // 3. The child is owned by `self`, so the 'index data remains valid.
            let child_ref = unsafe { &*child_ptr };
            self.result.push_borrowed(child_ref);
        }
    }

    /// Full mode skip_to — advances lagging children and aggregates all matches.
    ///
    /// Optimization: During initialization (first skip_to when the heap is empty),
    /// children that land exactly on `doc_id` are collected into the aggregate
    /// immediately. If the heap minimum equals `doc_id`, the aggregate is already
    /// built and the DFS traversal is skipped entirely.
    fn skip_to_full(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let mut found_prebuilt = false;

        if self.heap.is_empty() && self.last_doc_id == 0 {
            // Initialization: skip all children to target, pre-collect exact matches.
            // Reset aggregate before collecting Found children.
            if let Some(agg) = self.result.as_aggregate_mut() {
                agg.reset();
            }

            let mut any_found = false;
            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        self.heap.push(r.doc_id, idx);
                        any_found = true;
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {}
                }
            }

            // If any children landed exactly on doc_id, add them to the aggregate
            // now. This avoids a redundant DFS when the minimum is doc_id.
            if any_found {
                self.result.doc_id = doc_id;
                for idx in 0..self.children.len() {
                    if self.children[idx].last_doc_id() == doc_id {
                        self.add_child_to_result(idx);
                    }
                }
                found_prebuilt = true;
            }
        } else {
            // Steady-state: advance lagging children via heap replace_root.
            while let Some((child_doc_id, idx)) = self.heap.peek() {
                if child_doc_id >= doc_id {
                    break;
                }

                let child = &mut self.children[idx];
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        self.heap.replace_root(r.doc_id, idx);
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        self.heap.replace_root(r.doc_id, idx);
                    }
                    None => {
                        self.heap.pop();
                    }
                }
            }
        }

        // Check if any children left
        let Some((min_id, _)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        if found_prebuilt && min_id == doc_id {
            // Aggregate was already built during initialization — skip DFS.
            self.last_doc_id = doc_id;
        } else {
            // Build aggregate via DFS over the heap.
            self.build_aggregate_result(min_id);
        }

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
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

        if !QUICK_EXIT {
            return self.skip_to_full(doc_id);
        }

        // QUICK_EXIT mode: advance children and return first exact match.
        if let Some(exact_match_idx) = self.advance_children_to_target(doc_id)? {
            self.quick_set_from_child(exact_match_idx);
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }

        // Check if we have any children left
        let Some((min_id, min_idx)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        self.quick_set_from_child(min_idx);

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
        // Already at EOF - nothing to do
        if self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        let original_last_doc_id = self.last_doc_id;
        let mut any_change = false;

        // Revalidate ALL children and remove aborted ones via swap_remove.
        // We use index-based iteration because we need to remove elements while iterating.
        let mut i = 0;
        while i < self.children.len() {
            match self.children[i].revalidate()? {
                RQEValidateStatus::Aborted => {
                    // Remove aborted child using swap_remove for O(1) removal.
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

        // Rebuild the heap since children may have moved arbitrarily
        self.rebuild_heap();

        if self.heap.is_empty() {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        let min_doc_id = self.heap.peek().unwrap().0;
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