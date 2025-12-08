/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Intersection`].
//!
//! # TODO (MOD-12717)
//!
//! The following features from the C++ implementation are not yet ported:
//! - `max_slop`: Maximum allowed slop between term positions
//! - `in_order`: Require terms to appear in order
//!
//! Currently, the iterator operates with `max_slop = -1` semantics (no slop validation).

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Yields documents appearing in ALL child iterators using a merge (AND) algorithm.
///
/// Children are sorted by estimated result count (smallest first) to minimize iterations.
/// A document is only yielded when ALL children have a matching entry for it.
pub struct Intersection<'index, I> {
    /// Child iterators, sorted by estimated count (smallest first).
    children: Vec<I>,
    /// Last doc_id successfully found in ALL children (returned by `last_doc_id()`).
    last_doc_id: t_docId,
    num_expected: usize,
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

/// Result of attempting to get all children to agree on a target document ID.
enum AgreeResult {
    /// All children have the target doc_id as their current position.
    Agreed,
    /// A child skipped past the target to a higher doc_id; consensus must restart from this new ID.
    Ahead(t_docId),
    /// A child reached EOF; no more documents can match.
    Eof,
}

impl<'index, I> Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new intersection iterator. Children are sorted by estimated count. If `children`
    /// is empty, returns an iterator immediately at EOF.
    #[must_use]
    pub fn new(mut children: Vec<I>) -> Self {
        let num_children = children.len();

        if num_children == 0 {
            return Self {
                children,
                last_doc_id: 0,
                num_expected: 0,
                is_eof: true,
                result: RSIndexResult::intersect(0),
            };
        }

        children.sort_by_cached_key(|c| c.num_estimated());
        let num_expected = children
            .first()
            .map(|c| c.num_estimated())
            .expect("should have at least one child");
        Self {
            children,
            last_doc_id: 0,
            num_expected,
            is_eof: false,
            result: RSIndexResult::intersect(num_children),
        }
    }

    /// Reads from the first child. Returns the doc_id or None if EOF.
    fn read_from_first_child(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
        debug_assert!(!self.children.is_empty());

        match self.children[0].read()? {
            Some(r) => Ok(Some(r.doc_id)),
            None => {
                self.is_eof = true;
                Ok(None)
            }
        }
    }

    /// Tries to get all children to agree on the target doc_id.
    fn agree_on_doc_id(&mut self, target: t_docId) -> Result<AgreeResult, RQEIteratorError> {
        for child in &mut self.children {
            // Use child's cached last_doc_id instead of maintaining a parallel array
            if child.last_doc_id() == target {
                continue;
            }

            match child.skip_to(target)? {
                None => {
                    self.is_eof = true;
                    return Ok(AgreeResult::Eof);
                }
                Some(SkipToOutcome::Found(_)) => {
                    // Child's last_doc_id is automatically updated by skip_to
                }
                Some(SkipToOutcome::NotFound(r)) => {
                    return Ok(AgreeResult::Ahead(r.doc_id));
                }
            }
        }
        Ok(AgreeResult::Agreed)
    }

    /// Loops until all children agree on a doc_id, or EOF is reached.
    fn find_consensus(
        &mut self,
        initial_target: t_docId,
    ) -> Result<Option<t_docId>, RQEIteratorError> {
        let mut curr_target = initial_target;
        loop {
            match self.agree_on_doc_id(curr_target)? {
                AgreeResult::Agreed => return Ok(Some(curr_target)),
                AgreeResult::Ahead(new_target) => curr_target = new_target,
                AgreeResult::Eof => return Ok(None),
            }
        }
    }

    /// Builds the aggregate result from all children's current results.
    ///
    /// # Why `unsafe` is used here
    ///
    /// We attempted to solve this with safe split borrows (standalone function taking
    /// `&mut self.result` and `&mut self.children` separately), but it doesn't work due
    /// to a fundamental lifetime mismatch:
    ///
    /// - `push_borrowed` requires `&'index RSIndexResult` - a reference with `'index` lifetime
    /// - `child.current()` returns `&mut RSIndexResult<'index>` - the *data* has `'index`
    ///   lifetime, but the *reference* is bounded by `&mut self`
    ///
    /// The compiler cannot verify that children's internal results live for `'index`,
    /// even though we know they reference index data that does. Splitting the borrow
    /// doesn't help because `current()` still returns a reference bounded by the call.
    ///
    /// Potential alternatives worth exploring:
    /// - Store owned copies instead of borrowed references (memory/perf tradeoff)
    /// - Restructure `RSAggregateResult` to not require `'index` on stored references
    /// - Use a different aggregate pattern that doesn't store child references
    fn build_aggregate_result(&mut self, doc_id: t_docId) {
        self.last_doc_id = doc_id;

        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = doc_id;

        for child in &mut self.children {
            if let Some(child_result) = child.current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY: child_ptr points to child's result containing data with 'index
                // lifetime. Children are owned by self, so their results remain valid.
                let child_ref = unsafe { &*child_ptr };
                self.result.push_borrowed(child_ref);
            }
        }
    }
}

impl<'index, I> RQEIterator<'index> for Intersection<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Returns a mutable reference to the current aggregate result.
    ///
    /// # Behavior
    ///
    /// - Returns `None` if the iterator is at EOF.
    /// - Returns `Some(&mut result)` otherwise.
    ///
    /// # Note
    ///
    /// Before the first call to [`read()`](RQEIterator::read) or
    /// [`skip_to()`](RQEIterator::skip_to), this returns a reference to the
    /// uninitialized result buffer (with `doc_id = 0`). Callers should only
    /// rely on `current()` after a successful read operation.
    #[inline]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.is_eof {
            None
        } else {
            Some(&mut self.result)
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        let target = match self.read_from_first_child()? {
            Some(doc_id) => doc_id,
            None => return Ok(None),
        };

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

    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        self.is_eof = self.children.is_empty();
        self.children.iter_mut().for_each(|c| c.rewind());
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_expected
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
        let mut any_child_moved = false;
        let mut max_child_doc_id: t_docId = 0;
        let mut moved_to_eof = false;

        for child in &mut self.children {
            match child.revalidate()? {
                RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
                RQEValidateStatus::Moved { current } => {
                    any_child_moved = true;
                    match current {
                        Some(result) => {
                            // Child's last_doc_id is automatically updated by revalidate
                            max_child_doc_id = max_child_doc_id.max(result.doc_id);
                        }
                        None => moved_to_eof = true,
                    }
                }
                RQEValidateStatus::Ok => {}
            }
        }

        if !any_child_moved || self.is_eof {
            return Ok(RQEValidateStatus::Ok);
        }

        if moved_to_eof {
            self.is_eof = true;
            return Ok(RQEValidateStatus::Moved { current: None });
        }

        match self.skip_to(max_child_doc_id)? {
            Some(_) => Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            }),
            None => Ok(RQEValidateStatus::Moved { current: None }),
        }
    }
}
