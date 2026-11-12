/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Types and the drop rule shared by the score-bound hooks of [`RQEIterator`].

use rqe_core::DocId;

#[cfg(doc)]
use crate::RQEIterator;

/// A bound on the score of every document an iterator can yield from its current position up to
/// and including [`last_doc_id`](Self::last_doc_id), as reported by
/// [`RQEIterator::current_block_bound`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlockBound {
    /// Last document id the bound covers.
    pub last_doc_id: DocId,
    /// Upper bound on the score of each covered document.
    pub max_score: f64,
}

/// Whether a document, or a window of them, whose score is at most `bound` cannot reach
/// `min_competitive_score`.
///
/// `min_competitive_score` is the threshold handed to [`RQEIterator::read_with_threshold`]: the
/// caller has already taken the rounding margin (`SCORE_BOUND_REL_EPSILON` in `score_bound.h`)
/// off the heap minimum, so this is the same strict comparison as the C `ScoreBound_CanDrop`.
#[inline(always)]
pub fn can_drop(bound: f64, min_competitive_score: f64) -> bool {
    bound < min_competitive_score
}

/// Whether `min_competitive_score` can ever make [`can_drop`] true. A withdrawn threshold is
/// `-inf`; a NaN compares false everywhere.
#[inline(always)]
pub(crate) fn is_active(min_competitive_score: f64) -> bool {
    min_competitive_score > f64::NEG_INFINITY
}
