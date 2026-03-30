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
            if !child.at_eof() && child.last_doc_id() >= self.last_doc_id {
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

    /// Advances all lagging children in the heap to at least `doc_id`.
    ///
    /// Shared by both quick and full skip_to modes. Does not return early on
    /// exact matches — callers layer their own early-return or aggregation logic
    /// on top.
    fn skip_lagging_children(&mut self, doc_id: t_docId) -> Result<(), RQEIteratorError> {
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
        Ok(())
    }

    /// Quick-mode helper: advances all children to the target doc_id,
    /// returning `Some(child_idx)` immediately when an exact match is found.
    ///
    /// On first call (heap empty), initialises the heap by skipping every child.
    /// On subsequent calls, delegates to [`Self::skip_lagging_children`] and then
    /// checks the heap root for an exact match.
    fn advance_children_to_target(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<usize>, RQEIteratorError> {
        if self.heap.is_empty() && self.last_doc_id == 0 {
            // Initialisation: skip every child to the target, tracking the
            // first one that lands exactly on `doc_id` for an early return.
            let mut first_exact_match: Option<usize> = None;

            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r)) => {
                        self.heap.push(r.doc_id, idx);
                        if first_exact_match.is_none() {
                            first_exact_match = Some(idx);
                        }
                    }
                    Some(SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {}
                }
            }
            return Ok(first_exact_match);
        }

        // Steady-state: advance lagging children, then check for exact match.
        self.skip_lagging_children(doc_id)?;

        // Check if the heap root landed exactly on the target.
        if let Some((root_doc_id, idx)) = self.heap.peek() {
            if root_doc_id == doc_id {
                return Ok(Some(idx));
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
    /// On the first call (heap empty), initialises the heap by skipping every
    /// child to the target.  On subsequent calls, delegates to
    /// [`Self::skip_lagging_children`].  In both cases, the aggregate result is
    /// built via [`Self::build_aggregate_result`]'s DFS over the heap.
    fn skip_to_full(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.heap.is_empty() && self.last_doc_id == 0 {
            // Initialisation: skip every child to the target and build the heap.
            for (idx, child) in self.children.iter_mut().enumerate() {
                if child.at_eof() {
                    continue;
                }
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r)) => {
                        self.heap.push(r.doc_id, idx);
                    }
                    None => {}
                }
            }
        } else {
            // Steady-state: advance lagging children.
            self.skip_lagging_children(doc_id)?;
        }

        // Check if any children left
        let Some((min_id, _)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        // Build aggregate via DFS over the heap.
        self.build_aggregate_result(min_id);

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    /// Full mode read — advances matching children and finds minimum.
    fn read_full(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
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

    /// Quick mode read — delegates to `skip_to(last_doc_id + 1)`.
    ///
    /// This matches the C implementation's `UI_Read_Quick_Heap` pattern.
    fn read_quick(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let next_id = self.last_doc_id.saturating_add(1);
        match self.skip_to_quick(next_id)? {
            Some(SkipToOutcome::Found(r)) | Some(SkipToOutcome::NotFound(r)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    /// Quick mode skip_to — advances children and returns first exact match.
    /// Tracks minimum doc_id among non-matches for NotFound case.
    fn skip_to_quick(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
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
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        debug_assert!(self.last_doc_id < doc_id);

        if QUICK_EXIT {
            self.skip_to_quick(doc_id)
        } else {
            self.skip_to_full(doc_id)
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
