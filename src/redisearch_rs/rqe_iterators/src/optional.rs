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

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, wildcard::Wildcard};

/// Iterator that extends a [`Wildcard`] iterator up to a given upper bound
/// by emitting virtual results after the child iterator is exhausted.
#[derive(Default)]
pub struct Optional<'index> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the `child` beyond this bound are ignored.
    /// If the child ends before this bound, the iterator yields virtual
    /// results with no weight applied until [`Optional::max_id`] is reached.
    max_id: t_docId,

    /// Weight applied to results produced by the inner child iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// The last document identifier that was returned.
    last_doc_id: t_docId,

    /// Child iterator provided at construction time.
    /// Used while it can produce results. After it is exhausted
    /// the iterator yields virtual results until [`Optional::max_id`] is reached.
    child: Wildcard<'index>,

    /// Reused result object to avoid allocating on each read.
    result: RSIndexResult<'index>,
}

impl<'index> Optional<'index> {
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`Wildcard`] iterator. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `wildcard` is the child [`RQEIterator`]. If `None`, an empty child is used.
    pub fn new(max_id: t_docId, weight: f64, wildcard: Option<Wildcard<'index>>) -> Self {
        Self {
            max_id,
            weight,

            last_doc_id: 0,
            result: RSIndexResult::virt().frequency(1),

            child: wildcard.unwrap_or_default(),
        }
    }
}

impl<'iterator, 'index> RQEIterator<'iterator, 'index> for Optional<'index> {
    fn read(
        &'iterator mut self,
    ) -> Result<Option<&'iterator mut RSIndexResult<'index>>, RQEIteratorError> {
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
        if self.last_doc_id == self.child.last_doc_id()
            && let Some(outcome) = self.child.read()?
        {
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
    // Optimized version will also be integrated here, but in a later PR.
    fn skip_to(
        &'iterator mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'iterator, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.last_doc_id);

        if doc_id > self.max_id || self.at_eof() {
            self.last_doc_id = doc_id;
            return Ok(None);
        }

        if !self.child.at_eof()
            && doc_id > self.child.last_doc_id()
            && let Some(SkipToOutcome::Found(ris)) = self.child.skip_to(doc_id)?
        {
            self.last_doc_id = ris.doc_id;
            ris.weight = self.weight;
            return Ok(Some(SkipToOutcome::Found(ris)));
        }

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
                self.child = Default::default();
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
        self.max_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.last_doc_id >= self.max_id
    }
}
