/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VectorScoreSource`] — [`ScoreSource`] implementation backed by VecSim.

use std::{ffi::c_void, num::NonZeroUsize};

use ffi::{
    VecSimBatchIterator, VecSimBatchIterator_Free, VecSimBatchIterator_HasNext,
    VecSimBatchIterator_New, VecSimBatchIterator_Next, VecSimIndex, VecSimIndex_AdhocBfCtx_Free,
    VecSimIndex_AdhocBfCtx_GetDistanceFrom, VecSimIndex_AdhocBfCtx_New,
    VecSimIndex_GetDistanceFrom_Unsafe, VecSimIndex_IndexSize, VecSimIndex_PreferAdHocSearch,
    VecSimQueryParams, VecSimQueryReply_Code_VecSim_QueryReply_TimedOut, VecSimQueryReply_GetCode,
    VecSimQueryReply_GetIterator, VecSimQueryReply_Order_BY_ID, t_docId,
};
use inverted_index::RSIndexResult;
use rqe_iterators::RQEIteratorError;
use top_k::{CollectionStrategy, ScoreSource};

use crate::batch_cursor::VecSimScoreBatchCursor;

/// A [`ScoreSource`] that drives top-k collection against a VecSim index.
///
/// Two execution sub-paths are supported, selected by `is_disk`:
///
/// - **RAM** (`is_disk = false`): Adhoc-BF uses [`VecSimIndex_GetDistanceFrom_Unsafe`]
///   under shared tiered-index locks.
/// - **Disk** (`is_disk = true`): Adhoc-BF uses an [`VecSimAdhocBfCtx`] that
///   preprocesses the query once and serves per-label distance lookups.
///
/// [`VecSimAdhocBfCtx`]: ffi::VecSimAdhocBfCtx
pub struct VectorScoreSource {
    /// Non-owning; the C caller retains the index lifetime.
    index: *mut VecSimIndex,
    /// Byte blob of the query vector (f32 elements).
    query_vector: Vec<u8>,
    query_params: VecSimQueryParams,
    k: NonZeroUsize,
    /// Passed through to VecSim as the timeout context pointer.
    timeout_ctx: *mut c_void,
    /// `true` → disk-index adhoc path; `false` → RAM adhoc path.
    is_disk: bool,

    // ── Batch-mode state (reset on rewind) ──────────────────────────────────
    /// Lazy; created on the first `next_batch` call.
    batch_iter: Option<*mut VecSimBatchIterator>,
    /// 0 = dynamic (computed per batch).
    fixed_batch_size: usize,
    num_iterations: usize,
    /// Rolling estimate of how many child docs pass the filter;
    /// seeded by the caller at construction time.
    child_num_estimated: usize,
    /// `k - heap_count`, updated by `collection_strategy`.
    k_remaining: usize,
}

// SAFETY: VectorScoreSource is used from a single thread (the query execution
// thread). The raw pointers are non-owning (index, timeout_ctx) or are managed
// by the struct itself (batch_iter). It is the caller's responsibility to
// ensure the index outlives the iterator.
unsafe impl Send for VectorScoreSource {}

impl VectorScoreSource {
    /// Create a new `VectorScoreSource`.
    ///
    /// # Safety
    ///
    /// - `index` must be a valid, non-null pointer that remains valid for the
    ///   lifetime of this struct.
    /// - `timeout_ctx` must be valid for the duration of the query, or null.
    #[expect(clippy::too_many_arguments)]
    pub unsafe fn new(
        index: *mut VecSimIndex,
        query_vector: Vec<u8>,
        query_params: VecSimQueryParams,
        k: NonZeroUsize,
        timeout_ctx: *mut c_void,
        is_disk: bool,
        child_num_estimated: usize,
        fixed_batch_size: usize,
    ) -> Self {
        Self {
            index,
            query_vector,
            query_params,
            k,
            timeout_ctx,
            is_disk,
            batch_iter: None,
            fixed_batch_size,
            num_iterations: 0,
            child_num_estimated,
            k_remaining: k.get(),
        }
    }

    /// Return the number of vectors currently in the index.
    pub fn index_size(&self) -> usize {
        // SAFETY: `self.index` is valid for the struct's lifetime.
        unsafe { VecSimIndex_IndexSize(self.index) }
    }

    /// Ask VecSim whether adhoc-BF is preferred over batches for the given
    /// filter-subset size and k.
    pub fn prefer_adhoc(&self, subset_size: usize, k: usize, initial_check: bool) -> bool {
        // SAFETY: `self.index` is valid for the struct's lifetime.
        unsafe { VecSimIndex_PreferAdHocSearch(self.index, subset_size, k, initial_check) }
    }

    /// Compute the next batch size using the same formula as the C hybrid reader
    /// (`hybrid_reader.c:383-391`).
    fn compute_batch_size(&self) -> usize {
        if self.fixed_batch_size > 0 {
            return self.fixed_batch_size;
        }
        let index_size = self.index_size();
        let child_est = self.child_num_estimated.max(1); // guard div-by-zero
        (self.k_remaining * index_size / child_est) + 1
    }

    /// Adhoc-BF distance lookup for RAM indexes.
    ///
    /// Acquires/releases tiered-index shared locks around the call per the VecSim
    /// contract for `VecSimIndex_GetDistanceFrom_Unsafe`.
    fn lookup_score_ram(&self, doc_id: t_docId) -> Option<f64> {
        // SAFETY: `self.index` and `self.query_vector` are valid.
        let distance = unsafe {
            VecSimTieredIndex_AcquireSharedLocks(self.index);
            let d = VecSimIndex_GetDistanceFrom_Unsafe(
                self.index,
                doc_id as usize,
                self.query_vector.as_ptr() as *const c_void,
            );
            VecSimTieredIndex_ReleaseSharedLocks(self.index);
            d
        };
        if distance.is_nan() {
            None
        } else {
            Some(distance)
        }
    }

    /// Adhoc-BF distance lookup for disk indexes via the one-shot BF context.
    fn lookup_score_disk(&self, doc_id: t_docId) -> Option<f64> {
        // SAFETY: `self.index` and `self.query_vector` are valid.
        let ctx = unsafe {
            VecSimIndex_AdhocBfCtx_New(self.index, self.query_vector.as_ptr() as *const c_void)
        };
        if ctx.is_null() {
            return None;
        }
        // SAFETY: `ctx` is valid.
        let distance = unsafe { VecSimIndex_AdhocBfCtx_GetDistanceFrom(ctx, doc_id as usize) };
        // SAFETY: `ctx` is valid and not used after this.
        unsafe { VecSimIndex_AdhocBfCtx_Free(ctx) };
        if distance.is_nan() {
            None
        } else {
            Some(distance)
        }
    }
}

// ── ScoreSource impl ─────────────────────────────────────────────────────────

impl<'index> ScoreSource<'index> for VectorScoreSource {
    type Batch = VecSimScoreBatchCursor;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // Timeout is propagated via query_params.timeoutCtx — VecSim checks it
        // internally and returns VecSim_QueryReply_TimedOut from Next/New.

        // Lazily create the batch iterator on first call.
        if self.batch_iter.is_none() {
            // Pass timeout context via query params so VecSim handles it.
            self.query_params.timeoutCtx = self.timeout_ctx;
            // SAFETY: `self.index` and `self.query_vector` are valid.
            let iter = unsafe {
                VecSimBatchIterator_New(
                    self.index,
                    self.query_vector.as_ptr() as *const c_void,
                    &mut self.query_params,
                )
            };
            if iter.is_null() {
                return Ok(None);
            }
            self.batch_iter = Some(iter);
        }

        let batch_iter = self.batch_iter.unwrap();

        // SAFETY: `batch_iter` is valid.
        if !unsafe { VecSimBatchIterator_HasNext(batch_iter) } {
            return Ok(None);
        }

        let batch_size = self.compute_batch_size();
        self.num_iterations += 1;

        // SAFETY: `batch_iter` is valid.
        let reply = unsafe {
            VecSimBatchIterator_Next(batch_iter, batch_size, VecSimQueryReply_Order_BY_ID)
        };
        if reply.is_null() {
            return Ok(None);
        }

        // SAFETY: `reply` is valid.
        let code = unsafe { VecSimQueryReply_GetCode(reply) };
        if code == VecSimQueryReply_Code_VecSim_QueryReply_TimedOut {
            // SAFETY: `reply` must be freed even on timeout.
            unsafe { ffi::VecSimQueryReply_Free(reply) };
            return Err(RQEIteratorError::TimedOut);
        }

        // SAFETY: `reply` is valid.
        let iter = unsafe { VecSimQueryReply_GetIterator(reply) };
        // SAFETY: Ownership of both `reply` and `iter` is passed to the cursor.
        let cursor = unsafe { VecSimScoreBatchCursor::new(reply, iter) };
        Ok(Some(cursor))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        if self.is_disk {
            self.lookup_score_disk(doc_id)
        } else {
            self.lookup_score_ram(doc_id)
        }
    }

    fn num_estimated(&self) -> usize {
        self.k.get().min(self.index_size())
    }

    fn rewind(&mut self) {
        if let Some(iter) = self.batch_iter.take() {
            // SAFETY: `iter` is valid and we're taking ownership.
            unsafe { VecSimBatchIterator_Free(iter) };
        }
        self.num_iterations = 0;
        self.k_remaining = self.k.get();
    }

    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index> {
        let mut result = RSIndexResult::build_metric()
            .doc_id(doc_id)
            .num_value(score)
            .build();
        if !self.own_key.is_null() {
            // SAFETY: `own_key` is set by `getAdditionalMetricsRP` in pipeline_construction.c
            // before any reads occur, and the key lives in the query's RLookup structure for
            // at least `'index` (the query lifetime).
            let key: &'index ffi::RLookupKey = unsafe { &*(self.own_key as *const ffi::RLookupKey) };
            result.metrics.push_with_key(key, score);
        }
        result
    }

    fn iterator_type(&self) -> rqe_iterators::IteratorType {
        rqe_iterators::IteratorType::Hybrid
    }

    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy {
        self.k_remaining = k.saturating_sub(heap_count);
        if heap_count >= k {
            return CollectionStrategy::Stop;
        }
        // SAFETY: `self.index` is valid.
        let prefer_adhoc = unsafe {
            VecSimIndex_PreferAdHocSearch(
                self.index,
                self.child_num_estimated,
                k,
                false, // not initial check
            )
        };
        if prefer_adhoc {
            CollectionStrategy::SwitchToAdhoc
        } else {
            CollectionStrategy::Continue
        }
    }
}

impl Drop for VectorScoreSource {
    fn drop(&mut self) {
        if let Some(iter) = self.batch_iter.take() {
            // SAFETY: `iter` is valid.
            unsafe { VecSimBatchIterator_Free(iter) };
        }
    }
}

// ── VecSim tiered-lock helpers (forward declarations used in lookup_score_ram) ─

unsafe extern "C" {
    fn VecSimTieredIndex_AcquireSharedLocks(index: *mut VecSimIndex);
    fn VecSimTieredIndex_ReleaseSharedLocks(index: *mut VecSimIndex);
}
