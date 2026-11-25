/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`ConstantBound`]: score bounds for a leaf whose contribution does not depend on the document.

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, MinCompetitiveScore, can_drop},
};

/// Reports one bound, [`max_score`](Self::max_score), for every document `inner` yields.
///
/// For a leaf that cannot bound its documents itself, but whose share of any document's score
/// has a known maximum under the query's scorer, such as a filter whose results carry a fixed
/// frequency and weight. Without bounds, such a leaf turns score skipping off for every
/// composite above it.
///
/// The block it reports runs to [`DocId::MAX`], so a composite can skip on it across
/// documents. A thresholded read below the bound ends the iterator, since none of its remaining
/// documents can reach the threshold either. That only happens when this leaf is the root:
/// composites never hand their threshold to their children.
pub struct ConstantBound<I> {
    inner: I,
    max_score: f64,
    /// Set when a thresholded read found that the bound cannot reach its threshold, until
    /// [`rewind`](RQEIterator::rewind). `inner` keeps its own position, which nothing reads.
    dropped_rest: bool,
}

impl<I> ConstantBound<I> {
    /// Bound every document `inner` yields by `max_score`.
    ///
    /// # Panics
    ///
    /// When `max_score` is negative or NaN. Composites sum their children's bounds, so a bound
    /// below a child's real contribution of 0 would let the sum fall below the real score.
    pub fn new(inner: I, max_score: f64) -> Self {
        assert!(
            max_score >= 0.0,
            "a constant bound must be at least 0, got {max_score}"
        );
        Self {
            inner,
            max_score,
            dropped_rest: false,
        }
    }

    /// The bound reported for every document.
    pub const fn max_score(&self) -> f64 {
        self.max_score
    }

    /// The wrapped iterator.
    pub const fn inner(&self) -> &I {
        &self.inner
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for ConstantBound<I> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.dropped_rest {
            return None;
        }
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.dropped_rest {
            return Ok(None);
        }
        self.inner.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.dropped_rest {
            return Ok(None);
        }
        self.inner.skip_to(doc_id)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.dropped_rest {
            return Ok(RQEValidateStatus::Ok);
        }
        self.inner.revalidate(spec)
    }

    fn rewind(&mut self) {
        self.dropped_rest = false;
        self.inner.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.inner.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.dropped_rest || self.inner.at_eof()
    }

    fn type_(&self) -> IteratorType {
        self.inner.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.inner
            .intersection_sort_weight(prioritize_union_children)
    }

    fn reports_score_bounds(&self) -> bool {
        true
    }

    fn current_max_score(&self) -> f64 {
        self.max_score
    }

    fn current_block_bound(&self) -> Option<BlockBound> {
        (!self.at_eof() && self.inner.last_doc_id() != 0).then_some(BlockBound {
            last_doc_id: DocId::MAX,
            max_score: self.max_score,
        })
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: MinCompetitiveScore,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if can_drop(self.max_score, min_competitive_score) {
            self.dropped_rest = true;
            return Ok(None);
        }
        self.read()
    }
}

impl<I: ProfilePrint> ProfilePrint for ConstantBound<I> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.inner.print_profile(map, ctx);
    }
}
