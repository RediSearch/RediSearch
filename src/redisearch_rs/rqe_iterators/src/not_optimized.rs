/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`NotOptimized`].

use std::time::Duration;

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    WildcardIterator, maybe_empty::MaybeEmpty, not::NotIterator, utils::TimeoutContext,
};

/// Check the clock every this many loop iterations to amortize syscall cost.
const TIMEOUT_CHECK_GRANULARITY: u32 = 5_000;

/// An optimized NOT iterator that uses a wildcard inverted index iterator.
///
/// Unlike [`Not`](super::not::Not) which iterates sequentially from 1 to
/// `max_doc_id`, this variant uses a
/// [wildcard iterator](crate::wildcard) that reads from the existing-documents inverted
/// index. It yields all documents present in the wildcard iterator that
/// are **not** present in the child iterator.
///
/// This is applicable when the index has an `existingDocs` inverted index
/// (i.e. `index_all` is enabled), providing better performance by only
/// visiting documents that actually exist.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `W` - The wildcard iterator type, must implement [`WildcardIterator`].
/// * `I` - The child iterator type whose results are negated.
pub struct NotOptimized<'index, W, I> {
    /// The wildcard iterator over all existing documents.
    wcii: W,
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID (used as upper bound guard).
    max_doc_id: t_docId,
    /// Sticky EOF flag, set when iteration completes.
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RSIndexResult<'index>,
    /// Tracks the execution deadline for this iterator.
    timeout_ctx: Option<TimeoutContext>,
}

impl<'index, W, I> NotOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Create a new optimized NOT iterator.
    ///
    /// `wcii` is the wildcard iterator over all existing documents.
    /// `child` is the iterator whose documents will be excluded.
    /// `max_doc_id` is the upper bound for document IDs.
    /// `weight` is the score weight applied to every returned result.
    /// `timeout` controls the amortized timeout. Pass [`None`] to skip timeout checks.
    pub fn new(
        wcii: W,
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Option<Duration>,
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
            timeout_ctx: timeout.map(|t| TimeoutContext::new(t, TIMEOUT_CHECK_GRANULARITY, false)),
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`].
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        if let Some(ctx) = self.timeout_ctx.as_mut() {
            ctx.check_timeout()
        } else {
            Ok(())
        }
    }

    /// Advance the wildcard iterator and set [`forced_eof`](Self::forced_eof)
    /// if it is exhausted.
    ///
    /// Returns `Ok(true)` if the wildcard iterator produced a new document,
    /// `Ok(false)` if it reached EOF.
    #[inline(always)]
    fn advance_wcii_or_eof(&mut self) -> Result<bool, RQEIteratorError> {
        if self.wcii.read()?.is_none() {
            self.forced_eof = true;
            return Ok(false);
        }
        Ok(true)
    }

    /// Get a shared reference to the _child_ iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Check whether the child iterator is positionally past `doc_id`
    /// (already advanced beyond it) or fully exhausted, meaning `doc_id`
    /// cannot be in the child without performing additional reads.
    #[inline(always)]
    fn child_is_ahead_or_depleted(&self, doc_id: t_docId) -> bool {
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
            self.forced_eof = true;
            return Ok(false);
        }

        // Advance the wildcard iterator to the next document.
        // We check the return value (not `at_eof`) because iterators
        // may report `at_eof() == true` immediately after returning the last
        // element, while the returned value is still valid.
        if !self.advance_wcii_or_eof()? {
            return Ok(false);
        }

        loop {
            let wcii_last = self.wcii.last_doc_id();

            if self.child_is_ahead_or_depleted(wcii_last) {
                // Case 1: The wildcard document is not in the child.
                self.result.doc_id = wcii_last;
                return Ok(true);
            } else if wcii_last == self.child.last_doc_id() {
                // Case 2: Both iterators at the same position, advance both.
                self.child.read()?;
                if !self.advance_wcii_or_eof()? {
                    return Ok(false);
                }
            } else {
                // Case 3: Child is behind, read it forward to catch up.
                //
                // We use a read loop rather than `skip_to` because the
                // child almost always needs only a single read to reach
                // or pass `wcii_last`. The only scenario where the child
                // lags behind is when GC has removed a doc ID from the
                // wildcard inverted index but not yet from the child's
                // index — and even then the gap is typically tiny.
                // `read` is cheaper than `skip_to`, so the loop is
                // faster in the common case.
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
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
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
        if self.wcii.skip_to(doc_id)?.is_none() {
            self.forced_eof = true;
            return Ok(None);
        }

        let wcii_last = self.wcii.last_doc_id();

        // If child is behind wcii, advance it to catch up.
        if !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
            self.child.skip_to(wcii_last)?;
        }

        // If child landed at the same position, the document is in the
        // child. Advance to find the next valid NOT result.
        if self.child.last_doc_id() == wcii_last {
            if self.read_inner()? {
                return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
            } else {
                return Ok(None);
            }
        }

        // Child is ahead or depleted: wcii_last is a valid result.
        self.result.doc_id = wcii_last;
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
        self.forced_eof || self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // 1. Revalidate the wildcard iterator first.
        // SAFETY: Delegating to children with the same `spec` passed by our caller.
        let wcii_status = unsafe { self.wcii.revalidate(spec) }?;
        if matches!(wcii_status, RQEValidateStatus::Aborted) {
            return Ok(RQEValidateStatus::Aborted);
        }

        // 2. Revalidate the child iterator.
        let child_aborted = matches!(
            // SAFETY: Delegating to child with the same `spec` passed by our caller.
            unsafe { self.child.revalidate(spec) }?,
            RQEValidateStatus::Aborted
        );
        if child_aborted {
            // When child is aborted, NOT becomes "NOT nothing" = everything
            // from the wildcard iterator.
            self.child = MaybeEmpty::new_empty();
        }

        // 3. If the wildcard moved, sync state.
        if matches!(wcii_status, RQEValidateStatus::Moved { .. }) {
            // Sync the EOF flag with the wildcard iterator. This clears a
            // previously-set forced_eof so the iterator can recover.
            self.forced_eof = self.wcii.at_eof();
            // Track whether we land on a valid NOT result. Starts true
            // when wcii is not at EOF (we have a candidate position).
            let mut have_valid_pos = !self.forced_eof;
            if have_valid_pos {
                self.result.doc_id = self.wcii.last_doc_id();

                // If child is behind, skip it forward.
                // Errors are intentionally ignored: a timeout here should
                // not abort the iterator, since we are already committed
                // to returning Moved.
                if self.child.last_doc_id() < self.result.doc_id {
                    let _ = self.child.skip_to(self.result.doc_id);
                }

                // If child landed on the same position, the current
                // result is in the child and invalid for NOT. Advance to
                // the next valid position.
                if self.child.last_doc_id() == self.result.doc_id {
                    match self.read_inner() {
                        Ok(found) => have_valid_pos = found,
                        Err(_) => {
                            // A timeout during revalidation should not
                            // permanently terminate the iterator, but we
                            // have no valid position to return.
                            self.forced_eof = false;
                            have_valid_pos = false;
                        }
                    }
                }
            }

            Ok(RQEValidateStatus::Moved {
                current: if have_valid_pos {
                    Some(&mut self.result)
                } else {
                    None
                },
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::NotOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W> NotIterator<'index>
    for NotOptimized<'index, W, Box<dyn RQEIterator<'index> + 'index>>
where
    W: crate::WildcardIterator<'index>,
{
    fn child(&self) -> Option<&dyn RQEIterator<'index>> {
        NotOptimized::child(self).map(|c| &**c as &dyn RQEIterator<'index>)
    }
}

impl<'index, W: crate::WildcardIterator<'index> + 'index> crate::interop::ProfileChildren<'index>
    for NotOptimized<'index, W, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        NotOptimized {
            wcii: self.wcii,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            max_doc_id: self.max_doc_id,
            forced_eof: self.forced_eof,
            result: self.result,
            timeout_ctx: self.timeout_ctx,
        }
    }
}
