/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Optional iterator implementation

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Iterator that extends a [`RQEIterator`] up to a given upper bound
/// by emitting virtual results after the child iterator is exhausted.
pub struct Optional<'index, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the [`Optional::child`] beyond this bound are ignored.
    /// If the [`Optional::child`] ends before this bound, this [`Optional`] iterator yields virtual
    /// results with no [`Optional::weight`] applied until [`Optional::max_doc_id`] is reached.
    max_doc_id: t_docId,

    /// Weight applied to results produced by the inner [`Optional::child`] iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    result: RSIndexResult<'index>,

    /// The child [`RQEIterator`] provided at construction time.
    /// It is used while it can still produce results. Once exhausted,
    /// the iterator yields virtual results until [`Optional::max_doc_id`] is reached.
    ///
    /// This field is no longer used once [`Optional::child_abort`]` is `true`.
    child: I,

    /// Temporary workaround for lifetime issues when the [`Optional::child`] aborts during revalidation.
    /// When this occurs, this flag is set to `true` and remains so until the end of this
    /// [`Optional`]’s lifetime.
    ///
    /// Once the iterator design supports it, this flag can be removed and the child can
    /// instead be wrapped in an [`Option`], allowing it to be dropped immediately upon abort.
    child_abort: bool,
}

impl<'index, I> Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`RQEIterator`]. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `child` [`RQEIterator`] used and wrpaped around by this [`Optional`] iterator
    pub const fn new(max_id: t_docId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::virt(),
            child,
            child_abort: false,
        }
    }
}

impl<'index, I> RQEIterator<'index> for Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        let maybe_real = (!self.child_abort && self.child.last_doc_id() == self.result.doc_id)
            .then(|| self.child.read())
            .transpose()?
            .flatten();

        self.result.doc_id += 1;

        if let Some(real) = maybe_real {
            debug_assert_eq!(
                self.result.doc_id, real.doc_id,
                "reads are expected to be always sequential"
            );

            real.weight = self.weight;
            return Ok(Some(real));
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

        if !self.child_abort
            && doc_id > self.child.last_doc_id()
            && let Some(SkipToOutcome::Found(real)) = self.child.skip_to(doc_id)?
            && real.doc_id == doc_id
        {
            real.weight = self.weight;
            self.result.doc_id = real.doc_id;
            return Ok(Some(SkipToOutcome::Found(real)));
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let last_child_doc_id;

        if self.child_abort || {
            last_child_doc_id = self.child.last_doc_id();
            self.child.last_doc_id() != self.result.doc_id
        } {
            return Ok(RQEValidateStatus::Ok);
        }

        // Revalidate the child iterator (C-Code: step 1)
        match self.child.revalidate()? {
            // Abort: Handle child validation results (but continue processing)
            // C-Code: step 2
            RQEValidateStatus::Aborted => {
                // Ideally, we would drop the child here.
                // However, since other branches in this function return a derived &mut reference from it,
                // we can’t safely do that. A delayed cleanup would be confusing,
                // so we’ll use this flag for now.
                self.child_abort = true;
                Ok(if last_child_doc_id != self.result.doc_id {
                    // virtual
                    self.result.doc_id += 1;
                    RQEValidateStatus::Moved {
                        current: Some(&mut self.result),
                    }
                } else {
                    // real
                    RQEValidateStatus::Ok
                })
            }
            // If the current result is virtual,
            // or if the child was not moved, we can return VALIDATE_OK
            //
            // C-Code: step 3
            RQEValidateStatus::Ok => Ok(RQEValidateStatus::Ok),
            RQEValidateStatus::Moved { .. } => {
                if last_child_doc_id != self.result.doc_id {
                    // current result is virtual => VALIDATE_OK
                    return Ok(RQEValidateStatus::Ok);
                }

                // Current result is real and child was moved - we need to re-read
                //
                // C-Code: step 4
                Ok(RQEValidateStatus::Moved {
                    current: self.read()?,
                })
            }
        }
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        if !self.child_abort {
            self.child.rewind();
        }
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.max_doc_id
    }
}
