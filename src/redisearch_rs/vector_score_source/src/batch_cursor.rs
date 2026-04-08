/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VecSimScoreBatchCursor`] — a [`ScoreBatch`] backed by a VecSim query reply.

use ffi::{
    VecSimQueryReply, VecSimQueryReply_Iterator, VecSimQueryReply_IteratorFree,
    VecSimQueryReply_IteratorNext, VecSimQueryResult_GetId, VecSimQueryResult_GetScore,
    VecSimQueryReply_Free, t_docId,
};
use top_k::ScoreBatch;

/// A [`ScoreBatch`] cursor over a single VecSim batch reply.
///
/// Wraps an owned [`VecSimQueryReply`] together with its iterator.
/// Both are freed on drop. Doc IDs within a VecSim BY_ID batch are
/// **strictly increasing**, satisfying [`ScoreBatch`]'s contract.
pub struct VecSimScoreBatchCursor {
    /// Owned; freed on [`Drop`].
    reply: *mut VecSimQueryReply,
    /// Owned; freed on [`Drop`] (before `reply`).
    iter: *mut VecSimQueryReply_Iterator,
}

impl VecSimScoreBatchCursor {
    /// Wrap an existing VecSim reply + iterator.
    ///
    /// # Safety
    ///
    /// - `reply` must be a valid, non-null pointer returned by VecSim that has
    ///   not yet been freed.
    /// - `iter` must be the iterator obtained from `VecSimQueryReply_GetIterator(reply)`
    ///   and must not yet have been freed.
    /// - Ownership of both `reply` and `iter` is transferred to this struct.
    pub(crate) unsafe fn new(
        reply: *mut VecSimQueryReply,
        iter: *mut VecSimQueryReply_Iterator,
    ) -> Self {
        Self { reply, iter }
    }
}

impl Drop for VecSimScoreBatchCursor {
    fn drop(&mut self) {
        // SAFETY: Both pointers were obtained from VecSim and not yet freed.
        // The iterator must be freed before the reply.
        unsafe {
            VecSimQueryReply_IteratorFree(self.iter);
            VecSimQueryReply_Free(self.reply);
        }
        self.iter = std::ptr::null_mut();
        self.reply = std::ptr::null_mut();
    }
}

impl ScoreBatch for VecSimScoreBatchCursor {
    fn next(&mut self) -> Option<(t_docId, f64)> {
        // SAFETY: `self.iter` is valid until dropped.
        let result = unsafe { VecSimQueryReply_IteratorNext(self.iter) };
        if result.is_null() {
            return None;
        }
        // SAFETY: `result` is a valid pointer returned by IteratorNext.
        let id = unsafe { VecSimQueryResult_GetId(result) } as t_docId;
        let score = unsafe { VecSimQueryResult_GetScore(result) };
        Some((id, score))
    }

    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)> {
        // Linear scan — VecSim batch iterators have no random-access API.
        loop {
            let (id, score) = self.next()?;
            if id >= target {
                return Some((id, score));
            }
        }
    }
}
