/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VecSimScoreBatchCursor`] — [`ScoreBatch`] adapter over a VecSim query
//! reply.

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::{ExpirationChecker, FieldExpirationChecker};
use top_k::ScoreBatch;
use vecsim::ReplyResults;

/// A [`ScoreBatch`] cursor over a single VecSim batch reply.
///
/// `E` is the [`ExpirationChecker`] strategy, defaulting to the production
/// [`FieldExpirationChecker`]; see [`VectorScoreSource`](crate::VectorScoreSource).
pub struct VecSimScoreBatchCursor<E: ExpirationChecker = FieldExpirationChecker> {
    results: ReplyResults,
    /// Optional per-doc field-expiration filter; when present, expired docs
    /// are skipped during iteration so they never reach the heap or yield path.
    expiration: Option<E>,
}

impl<E: ExpirationChecker> VecSimScoreBatchCursor<E> {
    pub(crate) fn new(results: ReplyResults, expiration: Option<E>) -> Self {
        Self {
            results,
            expiration,
        }
    }
}

impl<E: ExpirationChecker> ScoreBatch for VecSimScoreBatchCursor<E> {
    fn next(&mut self) -> Option<(DocId, f64)> {
        loop {
            // SAFETY: `self.iter` is valid until dropped.
            let (id, score) = self.results.next()?;
            if let Some(checker) = self.expiration.as_ref()
                && checker.has_expiration()
            {
                let probe = RSIndexResult::build_virt().doc_id(id).build();
                if checker.is_expired(&probe) {
                    continue;
                }
            }
            return Some((id, score));
        }
    }

    fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
        self.results.skip_to(target)
    }
}
