/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VecSimScoreBatch`] — a [`ScoreBatch`] over a single VecSim query reply.

use ffi::t_docId;
use top_k::ScoreBatch;
use vecsim::ReplyResults;

/// Adapts [`ReplyResults`] to the [`ScoreBatch`] interface.
pub struct VecSimScoreBatch {
    results: ReplyResults,
}

impl VecSimScoreBatch {
    pub(crate) fn new(results: ReplyResults) -> Self {
        Self { results }
    }
}

impl ScoreBatch for VecSimScoreBatch {
    fn next(&mut self) -> Option<(t_docId, f64)> {
        Iterator::next(&mut self.results)
    }

    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)> {
        self.results.skip_to(target)
    }
}
