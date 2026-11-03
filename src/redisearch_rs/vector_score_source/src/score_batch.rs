/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VecSimScoreBatch`] — a [`ScoreBatch`] over a single VecSim query reply.

use rqe_core::DocId;
use rqe_iterators::RQEIteratorError;
use top_k::ScoreBatch;
use vecsim::ReplyResults;

/// Adapts [`ReplyResults`] to the [`ScoreBatch`] interface.
///
/// The batch yields every reply entry, expired documents included; expiration
/// is checked at yield time by the wrapping iterator (see
/// [`VectorScoreSource::is_expired`](crate::VectorScoreSource)).
pub struct VecSimScoreBatch {
    results: ReplyResults,
}

impl VecSimScoreBatch {
    pub(crate) fn new(results: ReplyResults) -> Self {
        Self { results }
    }
}

impl<S: ?Sized> ScoreBatch<S> for VecSimScoreBatch {
    fn next(&mut self, _source: &mut S) -> Result<Option<(DocId, f64)>, RQEIteratorError> {
        Ok(self.results.next())
    }

    fn skip_to(
        &mut self,
        _source: &mut S,
        target: DocId,
    ) -> Result<Option<(DocId, f64)>, RQEIteratorError> {
        Ok(self.results.skip_to(target))
    }
}
