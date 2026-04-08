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

use ffi::t_docId;
use top_k::ScoreBatch;
use vecsim::ReplyResults;

/// A [`ScoreBatch`] cursor over a single VecSim batch reply.
pub struct VecSimScoreBatchCursor {
    results: ReplyResults,
}

impl VecSimScoreBatchCursor {
    pub(crate) fn new(results: ReplyResults) -> Self {
        Self { results }
    }
}

impl ScoreBatch for VecSimScoreBatchCursor {
    fn next(&mut self) -> Option<(t_docId, f64)> {
        Iterator::next(&mut self.results)
    }

    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)> {
        self.results.skip_to(target)
    }
}
