/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iteration over a VecSim query's results, freed automatically when done.

use std::ptr::NonNull;

use ffi::{
    VecSimQueryReply, VecSimQueryReply_Code_VecSim_QueryReply_TimedOut, VecSimQueryReply_Free,
    VecSimQueryReply_GetCode, VecSimQueryReply_GetIterator, VecSimQueryReply_Iterator,
    VecSimQueryReply_IteratorFree, VecSimQueryReply_IteratorNext, VecSimQueryResult_GetId,
    VecSimQueryResult_GetScore,
};
use rqe_core::DocId;

use crate::{QueryError, ReplyOrder};

/// Owned `VecSimQueryReply`. Freed on [`Drop`].
pub struct QueryReply {
    /// Owned VecSim reply handle.
    ///
    /// # Invariant
    ///
    /// Valid (returned by `VecSimIndex_TopKQuery` or
    /// `VecSimBatchIterator_Next`) and not yet freed, from construction until
    /// [`Drop`].
    inner: NonNull<VecSimQueryReply>,
    /// The Order the reply's results are sorted in, as requested when the query
    /// was issued. VecSim does not expose this on the reply, so we record the
    /// caller-supplied order to keep order-sensitive consumers (e.g.
    /// [`ReplyResults::skip_to`]) correct.
    order: ReplyOrder,
}

impl QueryReply {
    /// Wrap a non-null reply pointer obtained from VecSim.
    ///
    /// `order` must be the sort order that was passed to the VecSim query call
    /// that produced `ptr`, so the reply can correctly answer order-sensitive
    /// queries later.
    ///
    /// # Safety
    ///
    /// The caller must establish the [`inner`](Self::inner) field invariant.
    pub(crate) const unsafe fn from_raw(ptr: NonNull<VecSimQueryReply>, order: ReplyOrder) -> Self {
        Self { inner: ptr, order }
    }

    /// Convert a raw reply pointer into a [`Result`], translating the VecSim
    /// reply code into [`QueryError::TimedOut`] when applicable.
    ///
    /// The caller never has to free: a null `raw` yields `Ok(None)`, and a
    /// timed-out reply is freed before returning [`QueryError::TimedOut`].
    ///
    /// `order` must be the sort order requested from the VecSim query call that
    /// produced `raw`.
    ///
    /// # Safety
    ///
    /// `raw` must be a pointer returned by a VecSim query call that has not yet
    /// been consumed or freed (or null). If non-null, ownership is transferred
    /// to this call.
    pub(crate) unsafe fn from_raw_checked(
        raw: *mut VecSimQueryReply,
        order: ReplyOrder,
    ) -> Result<Option<Self>, QueryError> {
        let Some(reply) = NonNull::new(raw) else {
            return Ok(None);
        };
        // SAFETY: `reply` is non-null and returned by VecSim without being
        // consumed yet, so the `QueryReply::inner` invariant is upheld.
        let reply = unsafe { Self::from_raw(reply, order) };
        // SAFETY: `reply.inner` upholds its invariant.
        let code = unsafe { VecSimQueryReply_GetCode(reply.inner.as_ptr()) };
        if code == VecSimQueryReply_Code_VecSim_QueryReply_TimedOut {
            drop(reply);
            return Err(QueryError::TimedOut);
        }
        Ok(Some(reply))
    }

    /// Consume the reply and produce an iterator over its results.
    pub fn into_results(self) -> Option<ReplyResults> {
        // SAFETY: `self.inner` upholds its invariant.
        let iter = unsafe { VecSimQueryReply_GetIterator(self.inner.as_ptr()) };
        let iter = NonNull::new(iter)?;
        Some(ReplyResults {
            iter,
            order: self.order,
            reply: self,
        })
    }
}

impl Drop for QueryReply {
    fn drop(&mut self) {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimQueryReply_Free(self.inner.as_ptr()) };
    }
}

/// Iterator over the results of a [`QueryReply`].
///
/// Holds the owning [`QueryReply`] so the reply lives at least as long as the
/// iterator.
pub struct ReplyResults {
    /// Owned VecSim reply iterator handle.
    ///
    /// # Invariants
    ///
    /// - Valid (returned by `VecSimQueryReply_GetIterator` on
    ///   [`reply`](Self::reply)) and not yet freed, from construction until
    ///   [`Drop`].
    /// - Declared *before* [`reply`](Self::reply): Rust drops fields in
    ///   declaration order, so the iterator handle is freed first — the order
    ///   required by VecSim.
    iter: NonNull<VecSimQueryReply_Iterator>,
    /// Sort order of the results, propagated from the originating
    /// [`QueryReply`]. Read by [`skip_to`](Self::skip_to) to enforce its
    /// id-ordered precondition.
    order: ReplyOrder,
    /// Owning [`QueryReply`] that backs [`iter`](Self::iter); kept alive for
    /// the lifetime of `self`.
    #[expect(dead_code, reason = "held for Drop ordering: freed after `iter`")]
    reply: QueryReply,
}

impl ReplyResults {
    /// Sort order the underlying results are returned in.
    pub const fn order(&self) -> ReplyOrder {
        self.order
    }

    /// Advance to the first result whose doc id is `>= target`.
    ///
    /// Performs a linear scan: VecSim reply iterators have no random-access
    /// API.
    ///
    /// # Precondition
    ///
    /// Only valid on [`ReplyOrder::ById`] replies.
    pub fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
        debug_assert_eq!(
            self.order,
            ReplyOrder::ById,
            "skip_to requires an id-ordered reply; ByScore replies are not monotonic in doc id"
        );
        self.find(|&(id, _)| id >= target)
    }
}

impl Iterator for ReplyResults {
    type Item = (DocId, f64);

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: `self.iter` upholds its invariant.
        let result = unsafe { VecSimQueryReply_IteratorNext(self.iter.as_ptr()) };
        if result.is_null() {
            return None;
        }
        // SAFETY: `result` is non-null and owned by `self.reply`, which
        // outlives this borrow.
        let id = unsafe { VecSimQueryResult_GetId(result) } as DocId;
        // SAFETY: as above.
        let score = unsafe { VecSimQueryResult_GetScore(result) };
        Some((id, score))
    }
}

impl Drop for ReplyResults {
    fn drop(&mut self) {
        // SAFETY: `self.iter` upholds its invariant; `self.reply` is freed
        // immediately after, by `QueryReply::Drop` (fields drop in
        // declaration order).
        unsafe { VecSimQueryReply_IteratorFree(self.iter.as_ptr()) };
    }
}
