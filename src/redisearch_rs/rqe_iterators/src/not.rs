/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Not`] and [`NotOptimized`].

use std::time::Duration;

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, maybe_empty::MaybeEmpty,
    util::TimeoutContext,
};

/// An iterator that negates the results of its child iterator.
///
/// Yields all document IDs from 1 to `max_doc_id` (inclusive) that are **not**
/// present in the child iterator.
pub struct Not<'index, I> {
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID to iterate up to (inclusive).
    max_doc_id: t_docId,
    /// Set to `true` in case the NOT Iterator
    /// detected using the [`TimeoutContext`] a timeout,
    /// and reset to `false` at [`RQEIterator::rewind`].
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RSIndexResult<'index>,
    /// Tracks the execution deadline for this iterator.
    ///
    /// Uses an amortized check to minimize overhead in hot paths. The timeout
    /// is absolute for the iterator's lifetime and does not reset upon rewinding.
    timeout_ctx: Option<TimeoutContext>,
}

impl<'index, I> Not<'index, I>
where
    I: RQEIterator<'index>,
{
    pub fn new(
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Duration,
        skip_timeout_checks: bool,
    ) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            // The `limit` of 5_000 determines the granularity of the timeout check.
            // Each time [`TimeoutContext::check_timeout`] is called (during `read` / `skip_to`),
            // the internal counter goes up. When it reaches this `limit` of 5_000 it will
            // reset that counter and do the actual (OS) expensive timeout check.
            timeout_ctx: if skip_timeout_checks {
                None
            } else {
                Some(TimeoutContext::new(timeout, 5_000, false))
            },
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`] to ensure that in case of an error (timeout),
    /// we also mark this iterator as EOF.
    ///
    /// Returns error [`RQEIteratorError::TimedOut`] if the deadline has been reached or exceeded.
    ///
    /// In case no timeout is enforced it will just return `Ok(())`.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let Some(result) = self.timeout_ctx.as_mut().map(|ctx| ctx.check_timeout()) else {
            return Ok(());
        };
        if matches!(result, Err(RQEIteratorError::TimedOut)) {
            // NOTE: this is not done for optimized version of NOT iterator in C
            self.forced_eof = true;
        }
        result
    }

    /// Wrapper around [`TimeoutContext::reset_counter`] to reset the timeout counter.
    ///
    /// Does nothing when no timeout is enforced.
    #[inline(always)]
    const fn reset_timeout(&mut self) {
        if let Some(ctx) = self.timeout_ctx.as_mut() {
            ctx.reset_counter();
        }
    }

    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Not`] iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Set the child of this [`Not`] iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = MaybeEmpty::new(new_child);
    }

    /// Unset the child of this [`Not`] iterator (make it `None`).
    pub fn unset_child(&mut self) {
        self.child = MaybeEmpty::new_empty();
    }

    /// Take the child of this [`Not`] iterator if it exists.
    pub fn take_child(&mut self) -> Option<I> {
        self.child.take_iterator()
    }
}

impl<'index, I> RQEIterator<'index> for Not<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // skip all child docs, while not EOF and in sync with child
        while !self.at_eof() {
            self.result.doc_id += 1;

            // Sync child if we've moved past its last known position
            let child_at_eof = if self.result.doc_id > self.child.last_doc_id() {
                self.child.read()?.is_none()
            } else {
                false
            };

            // Comparison Logic
            // If child is EOF, or we haven't reached the child's position,
            // or the child skipped past us, this document is a valid result.
            if child_at_eof || self.result.doc_id != self.child.last_doc_id() {
                self.reset_timeout();
                return Ok(Some(&mut self.result));
            }

            // Unified Checkpoint: Exactly one check per iteration.
            // This occurs AFTER the child.read() and before we decide to return.
            self.check_timeout()?;

            // Otherwise: doc_id == child.last_doc_id(), so we skip and loop again.
        }

        debug_assert!(self.at_eof());
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }

        // Do not skip beyond max_doc_id
        if doc_id > self.max_doc_id {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        // Case 1: Child is ahead or at EOF - docId is not in child
        // When child is at EOF, only accept doc_id if it's past the child's last document
        if self.child.last_doc_id() > doc_id
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
        {
            self.result.doc_id = doc_id;
            self.check_timeout()?;

            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            match rc {
                Some(SkipToOutcome::Found(_)) => {
                    // Found value - do not return
                }
                None | Some(SkipToOutcome::NotFound(_)) => {
                    // Not found or EOF - return
                    self.result.doc_id = doc_id;

                    self.check_timeout()?;

                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        self.check_timeout()?;

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        self.result.doc_id = doc_id;
        match self.read()? {
            Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.result.doc_id = 0;
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.forced_eof || self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Get child status
        match self.child.revalidate()? {
            RQEValidateStatus::Aborted => {
                self.child = MaybeEmpty::new_empty();
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Moved { .. } => {
                // Invariant: after read/skip_to, child is always ahead of NOT's position (or at EOF).
                // Moved means child moved forward (can't move backward), so our doc remains valid.
                // Special case: both at initial state (doc_id = 0) is also valid.
                debug_assert!(
                    self.child.at_eof()
                        || self.child.last_doc_id() > self.last_doc_id()
                        || (self.child.last_doc_id() == 0 && self.last_doc_id() == 0)
                );
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Ok => {
                // Child did not move - we did not move
                Ok(RQEValidateStatus::Ok)
            }
        }
    }
}

/// An optimized NOT iterator that uses a wildcard inverted index iterator.
///
/// Unlike [`Not`] which iterates sequentially from 1 to
/// `max_doc_id`, this variant uses a wildcard iterator (`wcii`) that reads
/// from the existing-documents inverted index. It yields all documents
/// present in the wildcard iterator that are **not** present in the child
/// iterator.
///
/// This is applicable when the index has an `existingDocs` inverted index
/// (e.g. `index_all` is enabled or disk-based specs), providing better
/// performance by only visiting documents that actually exist.
pub struct NotOptimized<'index, W, I> {
    /// The wildcard iterator over all existing documents.
    wcii: W,
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID (used as upper bound guard).
    max_doc_id: t_docId,
    /// Sticky EOF flag, set on timeout or when iteration completes.
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RSIndexResult<'index>,
    /// Tracks the execution deadline for this iterator.
    timeout_ctx: Option<TimeoutContext>,
}

impl<'index, W, I> NotOptimized<'index, W, I>
where
    W: RQEIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Create a new optimized NOT iterator.
    ///
    /// `wcii` is the wildcard iterator over all existing documents.
    /// `child` is the iterator whose documents will be excluded.
    /// `max_doc_id` is the upper bound for document IDs.
    /// `weight` is the score weight applied to every returned result.
    /// `timeout` and `skip_timeout_checks` control the amortized timeout.
    pub fn new(
        wcii: W,
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Duration,
        skip_timeout_checks: bool,
    ) -> Self {
        Self {
            wcii,
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            timeout_ctx: if skip_timeout_checks {
                None
            } else {
                Some(TimeoutContext::new(timeout, 5_000, false))
            },
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`] that sets `forced_eof`
    /// on timeout.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let Some(result) = self.timeout_ctx.as_mut().map(|ctx| ctx.check_timeout()) else {
            return Ok(());
        };
        if matches!(result, Err(RQEIteratorError::TimedOut)) {
            self.forced_eof = true;
        }
        result
    }

    /// Get a shared reference to the _child_ iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Set the child of this iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = MaybeEmpty::new(new_child);
    }

    /// Unset the child (make it empty).
    pub fn unset_child(&mut self) {
        self.child = MaybeEmpty::new_empty();
    }

    /// Take the child if it exists.
    pub fn take_child(&mut self) -> Option<I> {
        self.child.take_iterator()
    }

    /// Get a shared reference to the wildcard iterator.
    pub const fn wcii(&self) -> &W {
        &self.wcii
    }

    /// Replace the wildcard iterator.
    pub fn set_wcii(&mut self, wcii: W) {
        self.wcii = wcii;
    }

    /// Check whether the child iterator does **not** contain a document
    /// with the given `doc_id`.
    ///
    /// In Rust, `at_eof()` becomes `true` immediately after consuming the
    /// last element (eager EOF), while in C it is set lazily on the _next_
    /// read. We therefore cannot use `child.at_eof()` alone as proof that
    /// `doc_id` is absent—we must also confirm it is past the child's last
    /// known document.
    #[inline(always)]
    fn child_does_not_have(&self, doc_id: t_docId) -> bool {
        doc_id < self.child.last_doc_id()
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
    }

    /// Internal read logic shared by [`read`](RQEIterator::read) and
    /// [`skip_to`](RQEIterator::skip_to).
    ///
    /// Returns `Ok(true)` if a valid result was found (stored in
    /// `self.result.doc_id`), `Ok(false)` if EOF was reached.
    fn read_inner(&mut self) -> Result<bool, RQEIteratorError> {
        if self.at_eof() {
            return Ok(false);
        }

        // Advance the wildcard iterator to the next document.
        // We check the return value (not `at_eof`) because in Rust iterators
        // may report `at_eof() == true` immediately after returning the last
        // element, while the returned value is still valid.
        if self.wcii.read()?.is_none() {
            self.forced_eof = true;
            return Ok(false);
        }

        loop {
            let wcii_last = self.wcii.last_doc_id();

            if self.child_does_not_have(wcii_last) {
                // Case 1: The wildcard document is not in the child.
                self.result.doc_id = wcii_last;
                return Ok(true);
            } else if wcii_last == self.child.last_doc_id() {
                // Case 2: Both iterators at the same position, advance both.
                self.child.read()?;
                if self.wcii.read()?.is_none() {
                    self.forced_eof = true;
                    return Ok(false);
                }
            } else {
                // Case 3: Child is behind, advance it until it catches up.
                while !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
                    self.child.read()?;
                }
            }
            self.check_timeout()?;
        }
    }
}

impl<'index, W, I> RQEIterator<'index> for NotOptimized<'index, W, I>
where
    W: RQEIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() || self.result.doc_id >= self.max_doc_id {
            self.forced_eof = true;
            return Ok(None);
        }

        if self.read_inner()? {
            Ok(Some(&mut self.result))
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }
        if doc_id > self.max_doc_id {
            self.forced_eof = true;
            return Ok(None);
        }

        // Skip wcii to docId.
        let wcii_outcome = self.wcii.skip_to(doc_id)?;
        if wcii_outcome.is_none() {
            self.forced_eof = true;
            return Ok(None);
        }

        let wcii_last = self.wcii.last_doc_id();

        if self.child_does_not_have(wcii_last) {
            // Case 1: Wildcard document is not in the child.
            self.result.doc_id = wcii_last;
        } else if wcii_last == self.child.last_doc_id() {
            // Case 2: Both at same position. The target (or closest doc)
            // is in child, so find the next valid result.
            if self.read_inner()? {
                return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
            } else {
                return Ok(None);
            }
        } else {
            // Case 3: Wildcard is ahead of child.
            // Check if child also has the document at wcii's position.
            let child_outcome = self.child.skip_to(wcii_last)?;
            match child_outcome {
                Some(SkipToOutcome::Found(_)) => {
                    // Child has this document, find next valid result.
                    if self.read_inner()? {
                        return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
                    } else {
                        return Ok(None);
                    }
                }
                None | Some(SkipToOutcome::NotFound(_)) => {
                    // Child doesn't have this document, it's a valid result.
                    self.result.doc_id = wcii_last;
                }
            }
        }

        // Determine Found vs NotFound based on whether we're at the exact target.
        if self.result.doc_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.result.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.forced_eof
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // 1. Revalidate the wildcard iterator first.
        let wcii_status = self.wcii.revalidate()?;
        if matches!(wcii_status, RQEValidateStatus::Aborted) {
            return Ok(RQEValidateStatus::Aborted);
        }

        // 2. Revalidate the child iterator.
        if matches!(self.child.revalidate()?, RQEValidateStatus::Aborted) {
            // When child is aborted, NOT becomes "NOT nothing" = everything
            // from the wildcard iterator.
            self.child = MaybeEmpty::new_empty();
        }

        // 3. If the wildcard moved, sync state.
        if matches!(wcii_status, RQEValidateStatus::Moved { .. }) {
            if self.wcii.at_eof() {
                self.forced_eof = true;
            } else {
                self.result.doc_id = self.wcii.last_doc_id();

                // If child is behind, skip it forward.
                if self.child.last_doc_id() < self.result.doc_id {
                    let _ = self.child.skip_to(self.result.doc_id)?;
                }

                // If child landed on the same position, advance to next valid.
                if self.child.last_doc_id() == self.result.doc_id {
                    self.read_inner()?;
                }
            }

            Ok(RQEValidateStatus::Moved {
                current: if self.at_eof() {
                    None
                } else {
                    Some(&mut self.result)
                },
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }
}
