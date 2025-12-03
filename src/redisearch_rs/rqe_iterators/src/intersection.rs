/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Intersection iterator implementation.
//!
//! The intersection iterator finds documents that exist in ALL child iterators.
//! It uses a zipper/merge algorithm that advances through sorted document IDs,
//! finding common documents across all children.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{Empty, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that yields documents appearing in ALL of its child iterators.
///
/// The `Intersection` iterator implements a merge/zipper algorithm to find documents
/// that exist in every child iterator. It maintains the following invariants:
///
/// - Children are sorted by estimated result count (smallest first) to minimize iterations,
///   unless phrase matching (`in_order`) is enabled.
/// - The `last_doc_id` tracks the current target document ID being searched across all children.
/// - The `doc_ids` array caches the last read document ID from each child to avoid redundant reads.
/// - A document is only yielded when ALL children have a matching entry for it.
///
/// # Algorithm Overview
///
/// 1. Read/skip on the first child to get a candidate `doc_id`
/// 2. For each subsequent child, skip to that `doc_id`
/// 3. If a child is ahead of the target, update the target and restart from child 0
/// 4. If all children match, yield the result
/// 5. Repeat until any child reaches EOF
///
/// # Performance Optimizations
///
/// - **Child Sorting**: Children are sorted by `num_estimated()` (ascending) so the smallest
///   iterator is queried first, reducing the total number of skip operations.
/// - **DocId Caching**: Each child's last document ID is cached to avoid re-reading when
///   already at the target position.
///
/// # Future Extensions
///
/// The following features are planned but not yet implemented:
/// - `max_slop`: Maximum number of intervening positions between terms for phrase matching
/// - `in_order`: Whether terms must appear in query order (disables child sorting)
/// - `field_mask`: Field-level filtering for multi-field searches
pub struct Intersection<'index, I> {
    /// The child iterators to intersect.
    /// Sorted by estimated result count (smallest first) for optimization.
    children: Vec<I>,

    /// Cached document IDs from each child iterator.
    /// `doc_ids[i]` contains the last document ID read from `children[i]`.
    doc_ids: Vec<t_docId>,

    /// The current target document ID being searched across all children.
    /// Updated when a child is found to be ahead of the current target.
    last_doc_id: t_docId,

    /// The last document ID that was successfully found in ALL children.
    /// This is what `last_doc_id()` returns to callers.
    last_found_id: t_docId,

    /// Count of results yielded so far.
    len: usize,

    /// Upper-bound estimate of the number of results.
    /// Set to the minimum of all children's estimates.
    num_expected: usize,

    /// Whether the iterator has reached EOF.
    is_eof: bool,

    /// The aggregate result that combines children's results.
    /// Reused across iterations to avoid allocations.
    result: RSIndexResult<'index>,
}

/// Result of trying to get all children to agree on a document ID.
enum AgreeResult {
    /// All children agree on the target docId.
    Agreed,
    /// A child is ahead of the target; contains the new target to try.
    Ahead(t_docId),
    /// A child reached EOF; the intersection is exhausted.
    Eof,
}

impl<'index, I> Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new intersection iterator from the given child iterators.
    ///
    /// The children are sorted by their estimated result count (smallest first)
    /// to optimize the intersection algorithm by querying the smallest iterator first.
    ///
    /// # Arguments
    ///
    /// * `children` - A vector of child iterators to intersect.
    ///
    /// # Returns
    ///
    /// A new `Intersection` iterator that yields documents appearing in ALL children.
    pub fn new(mut children: Vec<I>) -> Self {
        // Sort children by estimated count (smallest first) for optimization
        children.sort_by_key(|c| c.num_estimated());

        let num_children = children.len();

        // Compute num_expected as the minimum of all children's estimates
        let num_expected = children.iter().map(|c| c.num_estimated()).min().unwrap_or(0);

        // Initialize doc_ids cache with zeros
        let doc_ids = vec![0; num_children];

        Self {
            children,
            doc_ids,
            last_doc_id: 0,
            last_found_id: 0,
            len: 0,
            num_expected,
            is_eof: false,
            result: RSIndexResult::intersect(num_children),
        }
    }

    /// Read the next document ID from the first child iterator.
    ///
    /// Equivalent to `II_ReadFromFirstChild` in the C implementation.
    ///
    /// Returns `Ok(Some(doc_id))` if successful, `Ok(None)` if EOF, or an error.
    fn read_from_first_child(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
        debug_assert!(!self.children.is_empty());

        let first_result = self.children[0].read()?;
        match first_result {
            Some(r) => {
                let doc_id = r.doc_id;
                self.doc_ids[0] = doc_id;
                Ok(Some(doc_id))
            }
            None => {
                self.is_eof = true;
                Ok(None)
            }
        }
    }

    /// Try to get all children to agree on the target document ID.
    ///
    /// Equivalent to `II_AgreeOnDocId` in the C implementation.
    ///
    /// For each child not already at the target, calls `skip_to(target)`.
    /// - If all children land on target: returns `AgreeResult::Agreed`
    /// - If a child lands ahead: returns `AgreeResult::Ahead(new_target)`
    /// - If a child hits EOF: returns `AgreeResult::Eof`
    fn agree_on_doc_id(&mut self, target: t_docId) -> Result<AgreeResult, RQEIteratorError> {
        for (i, child) in self.children.iter_mut().enumerate() {
            // Skip if child is already at the target
            if self.doc_ids[i] == target {
                continue;
            }

            // Try to skip child to the target
            match child.skip_to(target)? {
                None => {
                    // Child hit EOF
                    self.is_eof = true;
                    return Ok(AgreeResult::Eof);
                }
                Some(SkipToOutcome::Found(r)) => {
                    // Child found exact match
                    self.doc_ids[i] = r.doc_id;
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    // Child landed ahead of target
                    self.doc_ids[i] = r.doc_id;
                    return Ok(AgreeResult::Ahead(r.doc_id));
                }
            }
        }

        Ok(AgreeResult::Agreed)
    }

    /// Loop until all children agree on a document ID, or EOF is reached.
    ///
    /// Equivalent to `II_Find_Consensus` in the C implementation.
    ///
    /// Returns `Ok(Some(doc_id))` when consensus is reached, `Ok(None)` on EOF.
    fn find_consensus(&mut self, initial_target: t_docId) -> Result<Option<t_docId>, RQEIteratorError> {
        let mut curr_target: u64 = initial_target;
        loop {
            match self.agree_on_doc_id(curr_target)? {
                AgreeResult::Agreed => return Ok(Some(curr_target)),
                AgreeResult::Ahead(new_target) => curr_target = new_target,
                AgreeResult::Eof => return Ok(None),
            }
        }
    }

    /// Build the aggregate result from all children's current results.
    ///
    /// Equivalent to the result aggregation in `II_AgreeOnDocId` in the C implementation.
    ///
    /// Must be called only after `find_consensus` returns successfully.
    fn build_aggregate_result(&mut self, doc_id: t_docId) {
        self.last_found_id = doc_id;
        self.last_doc_id = doc_id;
        self.len += 1;

        // Reset the aggregate result
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = doc_id;

        // Collect children's results into the aggregate
        // We need to use unsafe to push borrowed references from children
        // because we can't hold mutable references to children while also
        // borrowing their results. The children's results are valid for 'index.
        for child in &mut self.children {
            if let Some(child_result) = child.current() {
                // SAFETY: child_result is valid for 'index lifetime as guaranteed
                // by the RQEIterator trait contract. The reference will remain valid
                // as long as we don't call read/skip_to on the child again.
                let child_ptr: *const RSIndexResult<'index> = child_result;
                unsafe {
                    self.result.push_borrowed(&*child_ptr);
                }
            }
        }
    }
}

impl<'index, I> RQEIterator<'index> for Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        // Edge case: no children means no results 
        // TODO: move to constructor?
        if self.children.is_empty() {
            self.is_eof = true;
            return Ok(None);
        }

        // Step 1: Read from the first child to get an initial target
        let target = match self.read_from_first_child()? {
            Some(doc_id) => doc_id,
            None => return Ok(None),
        };

        // Step 2: Find consensus among all children
        match self.find_consensus(target)? {
            Some(doc_id) => {
                self.build_aggregate_result(doc_id);
                Ok(Some(&mut self.result))
            }
            None => Ok(None),
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        // Edge case: no children means no results
        if self.children.is_empty() {
            self.is_eof = true;
            return Ok(None);
        }

        // Find consensus starting from the requested doc_id
        match self.find_consensus(doc_id)? {
            Some(found_id) => {
                self.build_aggregate_result(found_id);
                if found_id == doc_id {
                    Ok(Some(SkipToOutcome::Found(&mut self.result)))
                } else {
                    Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
                }
            }
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.last_found_id = 0;
        self.len = 0;
        self.is_eof = false;
        self.doc_ids.fill(0);
        self.children.iter_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_expected
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_found_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let mut any_child_moved = false;
        let mut max_child_doc_id: t_docId = 0;
        let mut moved_to_eof = false;

        // Step 1: Revalidate all children and track their status
        for (i, child) in self.children.iter_mut().enumerate() {
            match child.revalidate()? {
                RQEValidateStatus::Aborted => {
                    // If ANY child aborted, the whole intersection is aborted
                    return Ok(RQEValidateStatus::Aborted);
                }
                RQEValidateStatus::Moved { current } => {
                    any_child_moved = true;
                    match current {
                        Some(result) => {
                            // Update cached doc_id and track the maximum
                            self.doc_ids[i] = result.doc_id;
                            max_child_doc_id = max_child_doc_id.max(result.doc_id);
                        }
                        None => {
                            // Child moved to EOF
                            moved_to_eof = true;
                        }
                    }
                }
                RQEValidateStatus::Ok => {
                    // Child is still valid at the same position - nothing to do
                }
            }
        }

        // Step 2: Decide what to return based on children's status
        if !any_child_moved {
            // All children returned Ok - intersection is unchanged
            return Ok(RQEValidateStatus::Ok);
        }

        if self.is_eof {
            // Already at EOF - stay there (no change in observable state)
            return Ok(RQEValidateStatus::Ok);
        }

        if moved_to_eof {
            // Any child at EOF means intersection is at EOF
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        // Step 3: Children moved - find new intersection position
        // Skip to the maximum doc_id among moved children
        match self.skip_to(max_child_doc_id)? {
            Some(_) => {
                // Found a valid intersection point
                Ok(RQEValidateStatus::Moved {
                    current: Some(&mut self.result),
                })
            }
            None => {
                // No intersection found - EOF
                Ok(RQEValidateStatus::Moved { current: None })
            }
        }
    }
}

// =============================================================================
// Reduced Intersection - result of applying optimizations
// =============================================================================

/// Result of reducing an intersection's children.
///
/// The reducer applies the following optimizations:
/// 1. If any child is empty → return `Empty` (intersection with empty set is empty)
/// 2. Remove all wildcard children (intersection with "all" is identity)
/// 3. If no children remain (all were wildcards) → return the last wildcard
/// 4. If one child remains → return that child directly
/// 5. Otherwise → return `Intersection` with remaining children
pub enum ReducedIntersection<'index, I> {
    /// No results possible (empty child found or no children)
    Empty(Empty),
    /// Only one non-wildcard child, or all children were wildcards
    Single(I),
    /// Multiple non-wildcard children remain
    Intersection(Intersection<'index, I>),
}

/// Reduces children and creates the appropriate iterator.
///
/// This is the Rust equivalent of `IntersectionIteratorReducer` in C.
///
/// # Algorithm
/// 1. If any child `is_empty()` → return `Empty`
/// 2. Filter out children where `is_wildcard()` is true
/// 3. If no children remain → return the last wildcard (or Empty if none)
/// 4. If one child remains → return that child
/// 5. Otherwise → create `Intersection` with remaining children
pub fn reduce<'index, I>(mut children: Vec<I>) -> ReducedIntersection<'index, I>
where
    I: RQEIterator<'index>,
{
    // Check for empty children first - if any child is empty, result is empty
    if children.iter().any(|c| c.is_empty()) {
        return ReducedIntersection::Empty(Empty);
    }

    // No children provided
    if children.is_empty() {
        return ReducedIntersection::Empty(Empty);
    }

    // Find and keep track of the last wildcard (in case all are wildcards)
    let last_wildcard_idx = children.iter().rposition(|c| c.is_wildcard());

    // Remove wildcard children (they match everything, so don't affect intersection)
    let mut last_wildcard: Option<I> = None;
    children = children
        .into_iter()
        .enumerate()
        .filter_map(|(i, child)| {
            if child.is_wildcard() {
                // Keep the last wildcard in case we need it
                if Some(i) == last_wildcard_idx {
                    last_wildcard = Some(child);
                }
                None
            } else {
                Some(child)
            }
        })
        .collect();

    // All children were wildcards - return the last one
    if children.is_empty() {
        return match last_wildcard {
            Some(w) => ReducedIntersection::Single(w),
            None => ReducedIntersection::Empty(Empty), // Shouldn't happen
        };
    }

    // Only one non-wildcard child - return it directly
    if children.len() == 1 {
        return ReducedIntersection::Single(children.pop().unwrap());
    }

    // Multiple children - create intersection
    ReducedIntersection::Intersection(Intersection::new(children))
}

impl<'index, I> RQEIterator<'index> for ReducedIntersection<'index, I>
where
    I: RQEIterator<'index>,
{
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.read(),
            ReducedIntersection::Single(s) => s.read(),
            ReducedIntersection::Intersection(i) => i.read(),
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.skip_to(doc_id),
            ReducedIntersection::Single(s) => s.skip_to(doc_id),
            ReducedIntersection::Intersection(i) => i.skip_to(doc_id),
        }
    }

    fn rewind(&mut self) {
        match self {
            ReducedIntersection::Empty(e) => e.rewind(),
            ReducedIntersection::Single(s) => s.rewind(),
            ReducedIntersection::Intersection(i) => i.rewind(),
        }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match self {
            ReducedIntersection::Empty(e) => e.current(),
            ReducedIntersection::Single(s) => s.current(),
            ReducedIntersection::Intersection(i) => i.current(),
        }
    }

    fn last_doc_id(&self) -> t_docId {
        match self {
            ReducedIntersection::Empty(e) => e.last_doc_id(),
            ReducedIntersection::Single(s) => s.last_doc_id(),
            ReducedIntersection::Intersection(i) => i.last_doc_id(),
        }
    }

    fn at_eof(&self) -> bool {
        match self {
            ReducedIntersection::Empty(e) => e.at_eof(),
            ReducedIntersection::Single(s) => s.at_eof(),
            ReducedIntersection::Intersection(i) => i.at_eof(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            ReducedIntersection::Empty(e) => e.num_estimated(),
            ReducedIntersection::Single(s) => s.num_estimated(),
            ReducedIntersection::Intersection(i) => i.num_estimated(),
        }
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self {
            ReducedIntersection::Empty(e) => e.revalidate(),
            ReducedIntersection::Single(s) => s.revalidate(),
            ReducedIntersection::Intersection(i) => i.revalidate(),
        }
    }
}
