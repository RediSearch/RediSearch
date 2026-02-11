/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Optional`].

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;
use std::cmp;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that emits a sequence of results with no gaps, up to a given document id.
/// Results are pulled from an underlying [`RQEIterator`] instance. If there is no entry
/// for a given document id, a virtual result is yielded in its place.
pub struct Optional<'index, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the [`Optional::child`] beyond this bound are ignored.
    /// If the [`Optional::child`] ends before this bound, this [`Optional`] iterator yields virtual
    /// results with no [`Optional::weight`] applied until [`Optional::max_doc_id`] is reached.
    max_doc_id: t_docId,

    /// Weight applied to results produced by the inner [`Optional::child`] iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// Virtual result which will always contain the last doc id,
    /// even if that doc id came from the [`Optional::child`] iterator.
    ///
    /// Only for actual virtual results do we return a reference to it in
    /// functions such as Read/SkipTo.
    result: RSIndexResult<'index>,

    /// The child [`RQEIterator`] provided at construction time.
    /// It is used while it can still produce results. Once exhausted,
    /// the iterator yields virtual results until [`Optional::max_doc_id`] is reached.
    ///
    /// In case child aborts during [`RQEIterator::revalidate`],
    /// this child is turned into [`None`], changed from the [`Some`] state it starts
    /// at when creating using [`Optional::new`]. From that point onward all results
    /// will be virtual until `max_doc_id` is reached.
    child: Option<I>,
}

impl<'index, I> Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the inclusive upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`RQEIterator`]. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `child` [`RQEIterator`] used and wrapped around by this [`Optional`] iterator
    pub const fn new(max_id: t_docId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL),
            child: Some(child),
        }
    }

    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Optional`] iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Set the child of this [`Optional`] iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = Some(new_child);
    }

    /// Unset the child of this [`Optional`] iterator (make it `None`).
    pub fn unset_child(&mut self) {
        self.child = None;
    }

    /// Take the child of this [`Optional`] iterator if it had one.
    /// After this the child iterator of this [`Optional`] will behave
    /// as if it was the `Empty` iterator.
    pub const fn take_child(&mut self) -> Option<I> {
        self.child.take()
    }
}

impl<'index, I> RQEIterator<'index> for Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if let Some(child) = self.child.as_mut()
            && child.last_doc_id() == self.result.doc_id
            && let Some(child_result) = child.current()
        {
            Some(child_result)
        } else {
            Some(&mut self.result)
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        let maybe_real = self
            .child
            .as_mut()
            .map(|child| {
                let child_last_doc_id = child.last_doc_id();
                match child_last_doc_id.cmp(&(self.result.doc_id + 1)) {
                    cmp::Ordering::Less => child.read(),
                    cmp::Ordering::Equal => Ok(child.current()),
                    cmp::Ordering::Greater => Ok(None),
                }
            })
            .transpose()?
            .flatten();

        self.result.doc_id += 1;

        if let Some(real) = maybe_real {
            debug_assert!(
                real.doc_id >= self.result.doc_id,
                "no backwards reads should be possible"
            );

            if real.doc_id == self.result.doc_id {
                real.weight = self.weight;
                return Ok(Some(real));
            }
        }

        Ok(Some(&mut self.result))
    }

    // C-Code: SkipTo for OPTIONAL iterator - Non-optimized version.
    // Skip to a specific docId. If the child has a hit on this docId, return it.
    // Otherwise, return a virtual hit.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.result.doc_id);

        if doc_id > self.max_doc_id || self.at_eof() {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        if let Some(child) = self.child.as_mut() {
            if doc_id > child.last_doc_id() {
                // use current() here to work around
                // borrowing rules to be able to handle
                // both of `doc_id >= child.last_doc_id` cases...
                let _ = child.skip_to(doc_id)?;
            }

            if let Some(real) = child.current()
                && real.doc_id == doc_id
            {
                real.weight = self.weight;
                self.result.doc_id = real.doc_id;
                return Ok(Some(SkipToOutcome::Found(real)));
            }
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let Some(ref mut child) = self.child else {
            return Ok(RQEValidateStatus::Ok);
        };
        let last_child_doc_id = child.last_doc_id();

        // Revalidate the child iterator
        match child.revalidate()? {
            // Abort: Handle child validation results (but continue processing)
            status @ (RQEValidateStatus::Aborted | RQEValidateStatus::Moved { .. }) => {
                if matches!(status, RQEValidateStatus::Aborted) {
                    self.child = None; // Drop it so we become fully virtual until max is reached
                }

                Ok(if last_child_doc_id != self.result.doc_id {
                    // virtual
                    RQEValidateStatus::Ok
                } else {
                    // was real before abort, re-read to
                    // prevent returning stale data.
                    RQEValidateStatus::Moved {
                        current: self.read()?,
                    }
                })
            }
            // If the current result is virtual,
            // or if the child was not moved, we can return VALIDATE_OK
            RQEValidateStatus::Ok => Ok(RQEValidateStatus::Ok),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.result.doc_id = 0;
        if let Some(child) = self.child.as_mut() {
            child.rewind();
        }
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
        self.result.doc_id >= self.max_doc_id
    }
}
