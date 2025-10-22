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
    /// Reads from the `child` beyond this bound are ignored.
    /// If the child ends before this bound, the iterator yields virtual
    /// results with no weight applied until [`Optional::max_id`] is reached.
    max_doc_id: t_docId,

    /// Weight applied to results produced by the inner child iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// The last document identifier that was returned.
    last_doc_id: t_docId,

    /// Child iterator provided at construction time.
    /// Used while it can produce results. After it is exhausted
    /// the iterator yields virtual results until [`Optional::max_id`] is reached.
    child: Option<I>,

    /// Reused result object to avoid allocating on each read.
    result: RSIndexResult<'index>,
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
    /// * `child` [`RQEIterator`] used, can be deallocated early in case of an abort status in
    ///   a call to [`RQEIterator::revalidate`]
    pub fn new(max_id: t_docId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,

            last_doc_id: 0,
            result: RSIndexResult::virt().frequency(1),

            child: Some(child),
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

        // The second condition of this if statement is
        // to catch edge cases where the child is only _just_
        // now exhausted while it wasn't before and thus returned
        // Nothing. This way we still fall back to Virtual results
        // in this case instead of returning None incorrectly.
        //
        // C Version only had the last_doc_id equality check
        // but that's because it has access to the result of child
        // even in cases like the one described here, while the
        // Rust version only exposes the result if the
        // ierator is not EOF.
        //
        // The C version also used to do:
        //
        // ```
        // self.last_doc_id == self.child.last_doc_id()
        // ```
        //
        // Here we instead assume the child handles this fine,
        // as why wouldn't it?
        if let Some(outcome) = self.child.read()? {
            self.last_doc_id = outcome.doc_id;
            outcome.weight = self.weight;
            return Ok(Some(outcome));
        } else {
            self.last_doc_id += 1;
        }

        self.result.doc_id = self.last_doc_id;
        Ok(Some(&mut self.result))
    }

    // [OG C Comment] SkipTo for OPTIONAL iterator - Non-optimized version.
    // Skip to a specific docId. If the child has a hit on this docId, return it.
    // Otherwise, return a virtual hit.
    //
    // TODO: Optimized version will also be integrated here, but in a later PR.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.last_doc_id);

        if doc_id > self.max_doc_id || self.at_eof() {
            self.last_doc_id = doc_id;
            return Ok(None);
        }
        if doc_id > self.child.last_doc_id()
            && let Some(SkipToOutcome::Found(ris)) = self.child.skip_to(doc_id)?
            && ris.doc_id == doc_id
        {
            // real hit
            self.last_doc_id = ris.doc_id;
            ris.weight = self.weight;
            return Ok(Some(SkipToOutcome::Found(ris)));
        }

        // virtual hit
        self.last_doc_id = doc_id;
        self.result.doc_id = self.last_doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(&mut self) -> RQEValidateStatus {
        match self.child.revalidate() {
            RQEValidateStatus::Ok => RQEValidateStatus::Ok,
            RQEValidateStatus::Moved => {
                // Current result is real and child was moved (or aborted) - we need to re-read
                // NOTE: old c code: base->Read(base)
                // TODO: ^^^ should we return read as well?
                RQEValidateStatus::Moved
            }
            RQEValidateStatus::Aborted => {
                // Handle child validation results (but continue processing)
                self.child = None; // NOTE: C code used empty iterator for this, this is same same
                RQEValidateStatus::Ok
            }
        }
    }

    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        self.child.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.last_doc_id >= self.max_doc_id
    }
}
