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
    children: Vec<I>,
    num_estimated: usize,
    is_eof: bool,
    /// Reused across calls to avoid allocations.
    result: RSIndexResult<'index>,
    /// Min-heap of `(doc_id, child_index)`.
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
                num_estimated: 0,
                is_eof: true,
                result: RSIndexResult::build_union(0).build(),
                heap: DocIdMinHeap::new(),
            };
        }

        Self {
            children,
            num_estimated,
            is_eof: false,
            result: RSIndexResult::build_union(num_children).build(),
            heap: DocIdMinHeap::with_capacity(num_children),
        }
    }

    /// Rebuilds the heap from scratch based on current child positions.
    /// Used after revalidation when children may have moved arbitrarily.
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (idx, child) in self.children.iter().enumerate() {
            if !child.at_eof() && child.last_doc_id() >= self.last_doc_id() {
                self.heap.push(child.last_doc_id(), idx);
            }
        }
    }

    /// Advances children at the heap root whose `last_doc_id` equals `current_id`.
    fn advance_matching_children(&mut self, current_id: t_docId) -> Result<(), RQEIteratorError> {
        if self.heap.is_empty() {
            return Ok(());
        }
        loop {
            let (doc_id, idx) = self.heap[0];
            if doc_id != current_id {
                break;
            }

            let child = &mut self.children[idx];
            if child.read()?.is_some() {
                self.heap.replace_root(child.last_doc_id(), idx);
            } else {
                self.heap.pop();
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
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.result.doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }

        if self.heap.is_empty() {
            return;
        }

        let mut stack = [0usize; 64];
        let mut stack_len = 1;
        stack[0] = 0;

        while stack_len > 0 {
            stack_len -= 1;
            let heap_idx = stack[stack_len];

            if heap_idx >= self.heap.len() {
                continue;
            }

            let (doc_id, child_idx) = self.heap[heap_idx];
            if doc_id != min_id {
                continue;
            }

            if let Some(child_result) = self.children[child_idx].current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: We need a raw pointer to decouple the borrow of the child's
                // result from `&mut self.result`. This is sound because:
                // 1. `self.children[i]` and `self.result` are disjoint fields — no aliasing.
                // 2. `push_borrowed` takes a shared reference, so no mutation through child_ref.
                // 3. The child is owned by `self`, so the 'index data remains valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);
            }

            let left_heap_idx = 2 * heap_idx + 1;
            let right_heap_idx = 2 * heap_idx + 2;

            if left_heap_idx < self.heap.len() && stack_len < 64 {
                stack[stack_len] = left_heap_idx;
                stack_len += 1;
            }
            if right_heap_idx < self.heap.len() && stack_len < 64 {
                stack[stack_len] = right_heap_idx;
                stack_len += 1;
            }
        }
    }

    /// Performs initial read on all children and builds the heap.
    fn initialize_children(&mut self) -> Result<(), RQEIteratorError> {
        for (idx, child) in self.children.iter_mut().enumerate() {
            if child.last_doc_id() == 0 && !child.at_eof() {
                if child.read()?.is_some() {
                    self.heap.push(child.last_doc_id(), idx);
                }
            } else if child.last_doc_id() > 0 {
                self.heap.push(child.last_doc_id(), idx);
            }
        }
        Ok(())
    }

    /// Advances all lagging children in the heap to at least `doc_id`.
    ///
    /// In `QUICK_EXIT` mode, returns the child index on an exact match,
    /// leaving remaining lagging children for the next call.
    /// Returns `usize::MAX` if no exact match was found.
    fn advance_lagging_children(&mut self, doc_id: t_docId) -> Result<usize, RQEIteratorError> {
        if self.heap.is_empty() {
            return Ok(usize::MAX);
        }
        loop {
            let (child_doc_id, idx) = self.heap[0];
            if child_doc_id >= doc_id {
                break;
            }

            let child = &mut self.children[idx];
            match child.skip_to(doc_id)? {
                Some(SkipToOutcome::Found(r)) => {
                    self.heap.replace_root(r.doc_id, idx);
                    if QUICK_EXIT {
                        return Ok(idx);
                    }
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    self.heap.replace_root(r.doc_id, idx);
                }
                None => {
                    self.heap.pop();
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
    fn advance_to(&mut self, doc_id: t_docId) -> Result<usize, RQEIteratorError> {
        if self.heap.is_empty() && self.last_doc_id() == 0 {
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
            Ok(usize::MAX)
        } else {
            self.advance_lagging_children(doc_id)
        }
    }

    /// Full mode read — advances matching children and finds minimum.
    fn read_full(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.last_doc_id() == 0 {
            self.initialize_children()?;
        } else {
            self.advance_matching_children(self.last_doc_id())?;
        }

        let Some((min_id, _)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        self.build_aggregate_result(min_id);
        Ok(Some(&mut self.result))
    }

    /// Quick mode read — delegates to `skip_to(last_doc_id + 1)`.
    fn read_quick(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let next_id = self.last_doc_id().saturating_add(1);
        match self.skip_to(next_id)? {
            Some(SkipToOutcome::Found(r) | SkipToOutcome::NotFound(r)) => Ok(Some(r)),
            None => Ok(None),
        }
    }

    /// Sets the union result directly from the child at `child_idx`.
    fn quick_set_from_child(&mut self, child_idx: usize) {
        let child = &mut self.children[child_idx];
        self.result.doc_id = child.last_doc_id();

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }

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

        debug_assert!(self.last_doc_id() < doc_id);

        let early_match = self.advance_to(doc_id)?;

        // Early match found during advancement — skip the heap peek.
        if QUICK_EXIT && early_match != usize::MAX {
            self.quick_set_from_child(early_match);
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }

        let Some((min_id, min_idx)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(None);
        };

        if QUICK_EXIT {
            self.quick_set_from_child(min_idx);
        } else {
            self.build_aggregate_result(min_id);
        }

        if min_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.is_eof = self.children.is_empty();
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.children.iter_mut().for_each(|c| c.rewind());
        self.heap.clear();
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
        if self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        let original_last_doc_id = self.last_doc_id();
        let mut any_change = false;

        // Index-based iteration: swap_remove may reorder elements.
        let mut i = 0;
        while i < self.children.len() {
            match self.children[i].revalidate()? {
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
            return Ok(RQEValidateStatus::Aborted);
        }

        if !any_change {
            return Ok(RQEValidateStatus::Ok);
        }

        self.rebuild_heap();

        let Some((min_doc_id, min_idx)) = self.heap.peek() else {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        };

        if QUICK_EXIT {
            self.quick_set_from_child(min_idx);
        } else {
            self.build_aggregate_result(min_doc_id);
        }

        if self.last_doc_id() != original_last_doc_id {
            Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }
}
