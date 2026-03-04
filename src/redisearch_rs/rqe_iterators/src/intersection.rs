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
//! The intersection iterator supports proximity constraints via two parameters:
//! - `max_slop`: Maximum allowed slop between term positions (negative = no constraint)
//! - `in_order`: Require terms to appear in order

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

unsafe extern "C" {
    /// Checks whether the term positions recorded in `r` satisfy the given proximity constraints.
    ///
    /// Mirrors `IndexResult_IsWithinRange` from `src/index_result/index_result.c`.
    /// Returns non-zero when the result is within range.
    ///
    /// Unlike the Rust `RSIndexResult::is_within_range`, this C implementation correctly
    /// handles union children (e.g. stemmed/synonym expansions) by recursively merging
    /// offset positions via `RSIndexResult_IterateOffsets`.
    ///
    /// Note: the C algorithms treat any negative `maxSlop` value as a strict constraint
    /// (`span > negative` is always true). Always pass a non-negative value; use `i32::MAX`
    /// to express "no slop constraint".
    ///
    /// # Safety
    ///
    /// `r` must be a valid, fully initialised `RSIndexResult`.
    #[expect(
        improper_ctypes,
        reason = "RSIndexResult contains Rust-defined types; the C header uses the same layout"
    )]
    unsafe fn IndexResult_IsWithinRange(
        r: *mut RSIndexResult,
        max_slop: std::os::raw::c_int,
        in_order: std::os::raw::c_int,
    ) -> std::os::raw::c_int;
}

/// Yields documents appearing in ALL child iterators using a merge (AND) algorithm.
///
/// Children are sorted by estimated result count (smallest first) to minimize iterations,
/// unless `in_order` is set (which preserves the original child order for positional checks).
/// A document is only yielded when ALL children have a matching entry for it and the
/// term positions satisfy the `max_slop` / `in_order` proximity constraints.
pub struct Intersection<'index, I> {
    /// Child iterators, sorted by estimated count (smallest first) unless `in_order` is set.
    children: Vec<I>,
    /// Last doc_id successfully found in ALL children (returned by [`last_doc_id()`](Self::last_doc_id)).
    last_doc_id: t_docId,
    num_expected: usize,
    is_eof: bool,
    /// Maximum allowed slop (distance) between term positions. A negative value disables
    /// proximity validation entirely (equivalent to `INT_MAX` in the C implementation).
    max_slop: i32,
    /// When `true`, terms must appear in the same order as the child iterators.
    in_order: bool,

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
    /// Creates a new intersection iterator with proximity constraints.
    ///
    /// - `max_slop`: Maximum allowed distance between term positions. A negative value disables
    ///   proximity validation (every document matching all children is yielded).
    /// - `in_order`: When `true`, terms must appear in the order of the child iterators and
    ///   children are **not** re-sorted by estimated count (their order is meaningful).
    ///
    /// If `children` is empty, returns an iterator immediately at EOF.
    #[must_use]
    pub fn new(mut children: Vec<I>, max_slop: i32, in_order: bool) -> Self {
        // Normalize negative slop to i32::MAX: the C proximity algorithms misinterpret
        // negative values (any span > negative always breaks), so -1 ("no constraint") must
        // be represented as a sufficiently large positive value.
        // TODO: Use a different value to bypass max_slop constraint.
        //       This implementation is a leftover to maximize compatibility with existing C implementation.
        let max_slop = if max_slop < 0 { i32::MAX } else { max_slop };

        // Only sort by estimated count when order doesn't matter for proximity checks.
        if !in_order {
            children.sort_by_cached_key(|c| c.num_estimated());
        }
        let Some(num_expected) = children.iter().map(|c| c.num_estimated()).min() else {
            return Self {
                children,
                last_doc_id: 0,
                num_expected: 0,
                is_eof: true,
                max_slop,
                in_order,
                result: RSIndexResult::intersect(0),
            };
        };
        let num_children = children.len();
        Self {
            children,
            last_doc_id: 0,
            num_expected,
            is_eof: false,
            max_slop,
            in_order,
            result: RSIndexResult::intersect(num_children),
        }
    }

    /// Returns `true` if the current result needs a proximity check after consensus.
    ///
    /// The check is skipped only when both `max_slop == i32::MAX`
    /// and `in_order` is false — the only case where every document that
    /// matches all children is trivially within range.
    fn needs_relevancy_check(&self) -> bool {
        !(self.max_slop == i32::MAX && !self.in_order)
    }

    /// Check if the current aggregate result satisfies the proximity constraints.
    fn current_is_relevant(&self) -> bool {
        // SAFETY:
        // - `self.result` is a valid, fully initialised `RSIndexResult`.
        // - The C function reads from `r` without taking ownership or storing the pointer.
        // - Casting `*const` to `*mut` is required by the C API, which uses a non-const
        //   pointer even though it only reads; no mutation occurs.
        unsafe {
            IndexResult_IsWithinRange(
                &self.result as *const RSIndexResult<'_> as *mut RSIndexResult,
                self.max_slop,
                self.in_order as std::os::raw::c_int,
            ) != 0
        }
    }

    /// Find consensus on a doc_id and verify that the result satisfies the proximity constraints.
    ///
    /// If the agreed-upon document doesn't satisfy the slop/order constraints, advances the first
    /// child and retries until a relevant result is found or EOF is reached.
    fn find_consensus_with_relevancy_check(
        &mut self,
        mut target: t_docId,
    ) -> Result<Option<t_docId>, RQEIteratorError> {
        loop {
            match self.find_consensus(target)? {
                Some(doc_id) => {
                    self.build_aggregate_result(doc_id);
                    if self.current_is_relevant() {
                        self.last_doc_id = doc_id;
                        return Ok(Some(doc_id));
                    }
                    // Not relevant — advance past this document and retry.
                    let Some(next) = self.read_from_first_child()? else {
                        return Ok(None);
                    };
                    target = next;
                }
                None => return Ok(None),
            }
        }
    }

    /// Reads from the first child. Returns the doc_id or None if EOF.
    fn read_from_first_child(&mut self) -> Result<Option<t_docId>, RQEIteratorError> {
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
    fn find_consensus(&mut self, mut target: t_docId) -> Result<Option<t_docId>, RQEIteratorError> {
        loop {
            match self.agree_on_doc_id(target)? {
                AgreeResult::Agreed => return Ok(Some(target)),
                AgreeResult::Ahead(new_target) => target = new_target,
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
    /// - [`push_borrowed`](RSIndexResult::push_borrowed) requires `&'index RSIndexResult` - a reference with `'index` lifetime
    /// - [`child.current()`](RQEIterator::current) returns `&mut RSIndexResult<'index>` - the *data* has `'index`
    ///   lifetime, but the *reference* is bounded by `&mut self`
    ///
    /// The compiler cannot verify that children's internal results live for `'index`,
    /// even though we know they reference index data that does. Splitting the borrow
    /// doesn't help because [`current()`](RQEIterator::current) still returns a reference bounded by the call.
    ///
    /// # TODO
    ///
    /// Explore removing the unsafe code by using one of the following alternatives (as suggested in the PR):
    ///
    /// - Store owned copies instead of borrowed references (memory/perf tradeoff)
    /// - Restructure [`RSAggregateResult`](inverted_index::RSAggregateResult) to not require `'index` on stored references
    /// - Use a different aggregate pattern that doesn't store child references
    fn build_aggregate_result(&mut self, doc_id: t_docId) {
        if let Some(agg) = self.result.as_aggregate_mut() {
            agg.reset();
        }
        self.result.doc_id = doc_id;

        for child in &mut self.children {
            if let Some(child_result) = child.current() {
                let child_ptr: *const RSIndexResult<'index> = child_result;
                // SAFETY:
                // - `child_ptr` was derived from a valid `&mut RSIndexResult`, so it
                //   is aligned and points to initialized memory.
                // - The mutable borrow from `child.current()` ends when coerced to a
                //   raw pointer, so no mutable reference to this data is live.
                // - The underlying data has `'index` lifetime because children own
                //   index-backed results; children are owned by `self` and outlive
                //   this call.
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
    #[inline]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (!self.is_eof).then_some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        let Some(target) = self.read_from_first_child()? else {
            return Ok(None);
        };

        if self.needs_relevancy_check() {
            match self.find_consensus_with_relevancy_check(target)? {
                Some(_) => Ok(Some(&mut self.result)),
                None => Ok(None),
            }
        } else {
            match self.find_consensus(target)? {
                Some(doc_id) => {
                    self.build_aggregate_result(doc_id);
                    self.last_doc_id = doc_id;
                    Ok(Some(&mut self.result))
                }
                None => Ok(None),
            }
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.is_eof {
            return Ok(None);
        }

        if self.needs_relevancy_check() {
            // Try to agree on the requested doc_id first.
            match self.find_consensus(doc_id)? {
                Some(found_id) => {
                    self.build_aggregate_result(found_id);
                    if found_id == doc_id && self.current_is_relevant() {
                        self.last_doc_id = found_id;
                        return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                    }
                    // Either we landed on a different doc, or the exact match wasn't relevant.
                    // In both cases, find the next relevant result.
                    let next_target = if self.current_is_relevant() {
                        // Consensus on a different doc_id that IS relevant — return it.
                        self.last_doc_id = found_id;
                        return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
                    } else {
                        // Not relevant — advance past this document.
                        match self.read_from_first_child()? {
                            Some(t) => t,
                            None => return Ok(None),
                        }
                    };
                    match self.find_consensus_with_relevancy_check(next_target)? {
                        Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
                        None => Ok(None),
                    }
                }
                None => Ok(None),
            }
        } else {
            match self.find_consensus(doc_id)? {
                Some(found_id) => {
                    self.build_aggregate_result(found_id);
                    self.last_doc_id = found_id;
                    if found_id == doc_id {
                        Ok(Some(SkipToOutcome::Found(&mut self.result)))
                    } else {
                        Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
                    }
                }
                None => Ok(None),
            }
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
