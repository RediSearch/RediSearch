/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::rqe_iterator::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};
use std::usize;
use ffi::t_docId;
use inverted_index::RSIndexResult;
use low_memory_thin_vec::LowMemoryThinVec;

enum UnionIteratorMode {
    /// Represents a union iterator that aggregates results from multiple child iterators.
    FlatUnion,
    /// Represents a union iterator that aggregates results from multiple child iterators, but does not flatten the results.
    HeapUnion {
        heap: std::collections::BinaryHeap<Box<dyn RQEIterator>>,
    },
}

/// Represents a union iterator that aggregates results from multiple child iterators
struct UnionIterator {
    /// The active (not depleted) children iterators that are currently being processed
    active_children: LowMemoryThinVec<Box<dyn RQEIterator>>,
    /// The children iterators that are not currently being processed, but can be used later
    /// This is used to avoid re-allocating memory for children iterators that are not currently active.
    children: LowMemoryThinVec<Box<dyn RQEIterator>>,
    /// The last document ID that was read from the iterator
    last_doc_id: t_docId,
    /// The current result of the iterator, which is the union of the results from the active children iterators
    current_result: RSIndexResult,
    /// Indicates if the instance of the iterator is a flat union iterator or a heap union iterator, and use enum invocations on specific methods.
    mode: UnionIteratorMode,
    /// Indicates if this iterator uses the quick implementations.
    quick: bool,
    /// Indicates if the iterator has more results to read.
    has_next: bool,
}

impl UnionIterator {
    // TODO: create a function to return Box<dyn RQEIterator> for union iterators, according to given parameters (Union iterator factory)
    // This should be concise with UnionIteratorReducer from the C code.
    pub fn factory(
        children: LowMemoryThinVec<Box<dyn RQEIterator>>,
        quick: bool,
    ) -> Box<dyn RQEIterator> {
        // Create a new union iterator with the given children iterators
        let mode = UnionIteratorMode::FlatUnion; // Default to FlatUnion, can be changed later
        Box::new(UnionIterator::new(children, mode, quick))
    }

    pub fn new(
        children: LowMemoryThinVec<Box<dyn RQEIterator>>,
        mode: UnionIteratorMode,
        quick: bool,
    ) -> Self {
        let active_children = LowMemoryThinVec::with_capacity(children.len());
        let current_result = RSIndexResult::union(0);
        let last_doc_id = 0;
        let has_next = !children.is_empty();

        UnionIterator {
            active_children,
            children,
            last_doc_id,
            current_result,
            mode,
            quick,
            has_next,
        }
    }

    /// Removes an exhausted child iterator from the active children list and puts it back to the children list.
    #[inline(always)]
    fn remove_exhausted_child(&mut self, idx: usize) {
        // Assert that the index is within bounds
        assert!(
            idx < self.active_children.len(),
            "Index out of bounds for union iterator children"
        );
        // Swap the exhausted child with the last active child and put it back to the children list.
        let child = self.active_children.swap_remove(idx);
        self.children.push(child);
    }

    fn remove_exhausted_children(&mut self) {
        // Remove all exhausted children iterators from the active children list and put them back to the children list.
        // This is used to avoid re-allocating memory for children iterators that are not currently active.
        // This is used when the iterator is being reset to the beginning.
        let mut idx = 0;
        while idx < self.active_children.len() {
            if !self.active_children[idx].has_next() {
                self.remove_exhausted_child(idx);
            } else {
                idx += 1;
            }
        }
    }

    /// Moves all children iterators to the active children list.
    #[inline(always)]
    fn move_to_active(&mut self) {
        // Move all children iterators to the active children list.
        // This is used to avoid re-allocating memory for children iterators that are not currently active.
        // This is used when the iterator is being initialized or when it is being reset
        // to the beginning.
        while !self.children.is_empty() {
            self.active_children.push(self.children.pop().unwrap());
        }
    }

    #[inline(always)]
    fn move_to_children(&mut self) {
        // Move all active children iterators to the children list.
        // This is used to avoid re-allocating memory for children iterators that are not currently active.
        // This is used when the iterator is being reset to the beginning.
        while !self.active_children.is_empty() {
            self.children.push(self.active_children.pop().unwrap());
        }
    }

    /// Synchronizes the active children list with the current state of the iterators.
    fn sync_iter_list(&mut self) {
        self.move_to_active();
        self.remove_exhausted_children();
        // If there are no active children, set has_next to false.
        self.has_next = !self.active_children.is_empty();
    }

    fn set_full_flat(&mut self, results: &LowMemoryThinVec<RSIndexResult>) {
        // Reset the current result
        // TODO: should we move `reset` to the RSIndexResult?
        let current_result = self.current_result.as_union_mut().unwrap();
        current_result.reset();
        // Iterate over all results and add them to the current result
        for result in results.iter() {
            if result.get_doc_id() == self.last_doc_id {
                // If the result is the minimum ID, we add it to the current result
                self.current_result.push(result);
            }
        }
    }

    fn read_full_flat(&mut self) -> Result<Option<RSIndexResult>, RQEIteratorError> {
        let last_id: t_docId = self.last_doc_id;
        // TODO: add doc id limits to rust
        let mut min_id: t_docId = u64::MAX as t_docId;

        let mut results = LowMemoryThinVec::with_capacity(self.active_children.len());
        // Iterate over all active children iterators
        for child in self.active_children.iter_mut() {
            assert!(
                child.last_doc_id() >= last_id,
                "Child iterator's last doc id is less than the union iterator's last doc id"
            );
            // Read the next result from the child iterator
            match child.read() {
                Ok(Some(result)) => {
                    min_id = min_id.min(result.get_doc_id());
                    results.push(result);
                }
                Ok(None) => {
                    // do nothing, the child iterator is exhausted and we will remove it later
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        // Remove exhausted children iterators
        self.remove_exhausted_children();
        if results.is_empty() {
            // If there are no results, all active children are exhausted
            assert!(
                self.active_children.len() == 0,
                "Union iterator has no results, but still has active children"
            );
            // If there are no results, we return None, since we are at the end of the iterator
            self.has_next = false;
            return Ok(None);
        }

        self.last_doc_id = min_id;
        self.set_full_flat(&results);
        return Ok(Some(self.current_result.clone()));
    }

    fn skip_to_full_flat(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome>, RQEIteratorError> {
        let mut min_id = u64::MAX as t_docId;
        let mut results = LowMemoryThinVec::with_capacity(self.active_children.len());
        for child in self.active_children.iter_mut() {
            assert!(
                child.last_doc_id() >= doc_id,
                "Child iterator's last doc id is less than the requested doc id"
            );
            match child.skip_to(doc_id) {
                Ok(Some(SkipToOutcome::Found(result))) => {
                    min_id = min_id.min(result.get_doc_id());
                    results.push(result);
                }
                Ok(Some(SkipToOutcome::NotFound(result))) => {
                    min_id = min_id.min(result.get_doc_id());
                    results.push(result);
                }
                Ok(None) => {
                    // do nothing, the child iterator is exhausted and we will remove it later
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        // Remove exhausted children iterators
        self.remove_exhausted_children();
        if results.is_empty() {
            // if ther are no results, all active children are exhausted
            assert!(
                self.active_children.len() == 0,
                "Union iterator has no results, but still has active children"
            );
            // If there are no results, we return None, since we are at the end of the iterator
            self.has_next = false;
            return Ok(None);
        }

        // If there are results, we need to find the minimum ID and set the current result
        results.retain(|r| r.get_doc_id() == min_id);
        self.set_full_flat(&results);

        if min_id == doc_id {
            // If the minimum ID is equal to the requested doc id, we return the current result
            self.last_doc_id = min_id;
            Ok(Some(SkipToOutcome::Found(self.current_result.clone())))
        } else {
            // If the minimum ID is greater than the requested doc id, we return a NotFound outcome
            self.last_doc_id = min_id;
            Ok(Some(SkipToOutcome::NotFound(self.current_result.clone())))
        }
    }
}

impl RQEIterator for UnionIterator {
    fn read(&mut self) -> Result<Option<RSIndexResult>, RQEIteratorError> {
        if !self.has_next {
            return Ok(None);
        }
        match self.mode {
            UnionIteratorMode::FlatUnion => self.read_full_flat(),
            _ => Ok(None), // Placeholder for other modes, such as HeapUnion
        }
    }

    fn skip_to(&mut self, doc_id: t_docId) -> Result<Option<SkipToOutcome>, RQEIteratorError> {
        assert!(
            self.last_doc_id < doc_id,
            "Union iterator's last doc id is not less than the requested doc id"
        );
        // If the iterator has no next, we return None
        if !self.has_next {
            return Ok(None);
        }
        match self.mode {
            UnionIteratorMode::FlatUnion => self.skip_to_full_flat(doc_id),
            _ => Ok(None), // Placeholder for other modes, such as HeapUnion
        }
    }

    fn revalidate(&mut self) -> RQEValidateStatus {
        // First we will remove all aborted children iterators
        self.move_to_children();
        self.children
            .retain_mut(|child| child.revalidate() != RQEValidateStatus::Aborted);
        if self.children.is_empty() {
            // If there are no children, we are done
            self.has_next = false;
            return RQEValidateStatus::Aborted;
        }

        // Now we will move all children iterators to the active children list and remove exhausted children iterators
        self.sync_iter_list();
        if self.active_children.is_empty() {
            // If there are no active children, we are done
            self.has_next = false;
            return RQEValidateStatus::Ok;
        } else {
            self.has_next = true;
            let original_id = self.last_doc_id;
            self.last_doc_id = self
                .active_children
                .iter()
                .map(|c| c.last_doc_id())
                .min()
                .unwrap_or(u64::MAX as t_docId);
            if self.last_doc_id == original_id {
                // If the last doc id is the same as before, we are still at the same position
                return RQEValidateStatus::Ok;
            } else {
                // If the last doc id has changed, we have moved to a new position
                return RQEValidateStatus::Moved;
            }
        }
        // Unlike the C implementation, we will not call set_full_flat here, since we are not reading any results yet.
        // Instead, we will just reset the last_doc_id and has_next properties.
    }

    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.has_next = false;
        for child in self.active_children.iter_mut() {
            child.rewind();
        }
        for child in self.children.iter_mut() {
            child.rewind();
        }
        self.sync_iter_list();
        // TODO: should we reset the current result?
    }

    fn num_estimated(&self) -> usize {
        let mut sum: usize = 0;
        sum = sum.saturating_add(
            self.active_children
                .iter()
                .map(|c| c.num_estimated())
                .sum::<usize>(),
        );
        sum = sum.saturating_add(
            self.children
                .iter()
                .map(|c| c.num_estimated())
                .sum::<usize>(),
        );
        sum
    }

    fn last_doc_id(&self) -> t_docId {
        return self.last_doc_id;
    }

    fn has_next(&self) -> bool {
        self.has_next
    }
}
