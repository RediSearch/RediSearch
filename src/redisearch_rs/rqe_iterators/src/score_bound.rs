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

/// A score that a candidate must reach to enter the results, as handed to
/// [`RQEIterator::read_with_threshold`].
///
/// Always positive, so a value of this type always applies: a caller with no threshold, or with
/// one that could not drop anything, calls [`read`](RQEIterator::read) instead. The caller has
/// already taken the rounding margin (`SCORE_BOUND_REL_EPSILON` in `score_bound.h`) off the heap
/// minimum.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct MinCompetitiveScore(f64);

impl MinCompetitiveScore {
    /// The threshold `value`, or `None` when it is not positive (NaN included).
    ///
    /// Leaves only report bounds under scorers that never score below 0, so a threshold that is
    /// not positive could not drop anything.
    #[inline(always)]
    pub fn new(value: f64) -> Option<Self> {
        (value > 0.0).then_some(Self(value))
    }

    /// The threshold as a plain score.
    #[inline(always)]
    pub const fn get(self) -> f64 {
        self.0
    }
}

/// Whether a document, or a window of them, whose score is at most `bound` cannot reach
/// `min_competitive_score`.
///
/// The same strict comparison as the C `ScoreBound_CanDrop`.
#[inline(always)]
pub fn can_drop(bound: f64, min_competitive_score: MinCompetitiveScore) -> bool {
    bound < min_competitive_score.0
}
