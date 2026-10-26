/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VecSimScoreBatch`] — a [`ScoreBatch`] over a single VecSim query reply.

use std::sync::Arc;

use rqe_core::DocId;
use top_k::ScoreBatch;
use vecsim::ReplyResults;

pub(crate) type DocumentFilter<'a> = Arc<dyn Fn(DocId) -> bool + Send + Sync + 'a>;

/// Adapts VecSim replies or a filtered top-k result to [`ScoreBatch`].
pub struct VecSimScoreBatch {
    results: Results,
    filter: Option<DocumentFilter<'static>>,
}

enum Results {
    Native(ReplyResults),
    Buffered(std::vec::IntoIter<(DocId, f64)>),
}

impl VecSimScoreBatch {
    pub(crate) fn new(results: ReplyResults) -> Self {
        Self {
            results: Results::Native(results),
            filter: None,
        }
    }

    pub(crate) fn filtered(results: ReplyResults, filter: Option<DocumentFilter<'static>>) -> Self {
        Self {
            results: Results::Native(results),
            filter,
        }
    }

    pub(crate) fn buffered(results: Vec<(DocId, f64)>) -> Self {
        Self {
            results: Results::Buffered(results.into_iter()),
            filter: None,
        }
    }
}

impl ScoreBatch for VecSimScoreBatch {
    fn next(&mut self) -> Option<(DocId, f64)> {
        loop {
            let item = match &mut self.results {
                Results::Native(results) => results.next(),
                Results::Buffered(results) => results.next(),
            }?;
            if self.filter.as_ref().is_none_or(|filter| filter(item.0)) {
                return Some(item);
            }
        }
    }

    fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
        let item = match &mut self.results {
            Results::Native(results) => results.skip_to(target),
            Results::Buffered(results) => results.find(|(id, _)| *id >= target),
        }?;
        if self.filter.as_ref().is_none_or(|filter| filter(item.0)) {
            Some(item)
        } else {
            self.next()
        }
    }
}
