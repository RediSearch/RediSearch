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
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, WildcardIterator,
    maybe_empty::MaybeEmpty, util::TimeoutContext,
};

/// Check the clock every this many loop iterations to amortize syscall cost.
const TIMEOUT_CHECK_GRANULARITY: u32 = 5_000;

/// An optimized NOT iterator that uses a wildcard inverted index iterator.
///
/// Unlike [`Not`](super::not::Not) which iterates sequentially from 1 to
/// `max_doc_id`, this variant uses a
/// [`WildcardIterator`] that reads from the existing-documents inverted
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
    /// Sticky EOF flag, set on timeout or when iteration completes.
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

    /// Check whether the child iterator does **not** contain a document
    /// with the given `doc_id`.
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
            self.forced_eof = true;
            return Ok(false);
        }

        // Advance the wildcard iterator to the next document.
        // We check the return value (not `at_eof`) because iterators
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
        self.forced_eof || self.result.doc_id >= self.max_doc_id
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
}
