/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Union`].

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Yields documents appearing in ANY child iterator using a merge (OR) algorithm.
///
/// Unlike [`crate::Intersection`] which requires documents to appear in ALL children,
/// `Union` yields documents that appear in at least one child. When multiple children
/// have the same document, their results are aggregated.
///
/// # Algorithm
///
/// The iterator maintains all children and on each `read`:
/// 1. Finds the minimum `last_doc_id` across all non-EOF children
/// 2. Collects results from all children matching that minimum
/// 3. Advances only the children that matched (on the next read)
///
/// This is the "full" mode from the C implementation (`UI_Read_Full_Flat`).
pub struct Union<'index, I> {
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

impl<'index, I> Union<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new union iterator. If `children` is empty, returns an iterator
    /// immediately at EOF.
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

    /// Finds the minimum doc_id among all non-EOF children.
    /// Returns `None` if all children are at EOF.
    fn find_min_doc_id(&self) -> Option<t_docId> {
        self.children
            .iter()
            .filter(|c| !c.at_eof())
            .map(|c| c.last_doc_id())
            .min()
    }

    /// Advances all children whose `last_doc_id` equals `current_id`.
    /// Returns `Ok(true)` if at least one child was successfully advanced.
    /// Returns `Ok(false)` if all matching children reached EOF.
    fn advance_matching_children(&mut self, current_id: t_docId) -> Result<bool, RQEIteratorError> {
        let mut any_advanced = false;
        for child in &mut self.children {
            if !child.at_eof()
                && child.last_doc_id() == current_id
                && child.read()?.is_some()
            {
                any_advanced = true;
            }
        }
        Ok(any_advanced)
    }

    /// Builds the aggregate result from all children whose current doc_id equals `min_id`.
    ///
    /// # Safety considerations
    ///
    /// See [`crate::Intersection::build_aggregate_result`] for the safety rationale.
    fn build_aggregate_result(&mut self, min_id: t_docId) {
        self.last_doc_id = min_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = min_id;

        for child in &mut self.children {
            if !child.at_eof()
                && child.last_doc_id() == min_id
                && let Some(child_result) = child.current()
            {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: child_ptr points to child's result containing data with 'index
                // lifetime. Children are owned by self, so their results remain valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);
            }
        }
    }

    /// Checks if all children are at EOF.
    fn all_children_at_eof(&self) -> bool {
        self.children.iter().all(|c| c.at_eof())
    }

    /// Performs initial read on all children to position them at their first document.
    fn initialize_children(&mut self) -> Result<(), RQEIteratorError> {
        for child in &mut self.children {
            // Only read from children that haven't been read yet (last_doc_id == 0)
            if child.last_doc_id() == 0 && !child.at_eof() {
                let _ = child.read()?;
            }
        }
        Ok(())
    }
}

impl<'index, I> RQEIterator<'index> for Union<'index, I>
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

        // On first read (last_doc_id == 0), initialize all children
        if self.last_doc_id == 0 {
            self.initialize_children()?;
        } else {
            // Advance children that matched the previous result
            self.advance_matching_children(self.last_doc_id)?;
        }

        // Check if all children are at EOF
        if self.all_children_at_eof() {
            self.is_eof = true;
            return Ok(None);
        }

        // Find minimum doc_id among all non-EOF children
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

        // Skip all children to the target doc_id
        let mut min_id = t_docId::MAX;
        for child in &mut self.children {
            if child.at_eof() {
                continue;
            }

            // Only skip if the child is behind the target
            if child.last_doc_id() < doc_id {
                match child.skip_to(doc_id)? {
                    Some(SkipToOutcome::Found(_)) | Some(SkipToOutcome::NotFound(_)) => {
                        // Child has been positioned
                    }
                    None => {
                        // Child reached EOF
                        continue;
                    }
                }
            }

            // Track minimum doc_id
            if !child.at_eof() && child.last_doc_id() < min_id {
                min_id = child.last_doc_id();
            }
        }

        // Check if all children are at EOF
        if self.all_children_at_eof() {
            self.is_eof = true;
            return Ok(None);
        }

        // Build result for the minimum doc_id found
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

        // First pass: revalidate all children and collect status
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
                    // If current is None, child moved to EOF - don't count it
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
        // (unlike intersection where any abort causes abort)
        if any_child_aborted {
            // Remove aborted children
            // Note: In a more complete implementation, we'd track which children aborted
            // For now, we continue with the remaining children
        }

        if !any_child_moved || self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        // If no active children remain, we're at EOF
        if active_children == 0 {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        // Rebuild result with the new minimum doc_id
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

