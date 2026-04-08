/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VectorScoreSource`] — [`ScoreSource`] implementation backed by VecSim.

use std::{ffi::c_void, num::NonZeroUsize, ptr::NonNull};

use ffi::{
    VecSimAdhocBfCtx, VecSimBatchIterator, VecSimBatchIterator_Free, VecSimBatchIterator_HasNext,
    VecSimBatchIterator_New, VecSimBatchIterator_Next, VecSimIndex, VecSimIndex_AdhocBfCtx_Free,
    VecSimIndex_AdhocBfCtx_GetDistanceFrom, VecSimIndex_AdhocBfCtx_New,
    VecSimIndex_GetDistanceFrom_Unsafe, VecSimIndex_IndexSize, VecSimIndex_PreferAdHocSearch,
    VecSimIndex_TopKQuery, VecSimQueryParams, VecSimQueryReply_Code_VecSim_QueryReply_TimedOut,
    VecSimQueryReply_GetCode, VecSimQueryReply_GetIterator, VecSimQueryReply_Order_BY_ID,
    VecSimTieredIndex_AcquireSharedLocks, VecSimTieredIndex_ReleaseSharedLocks, t_docId,
};
use index_result::RSIndexResult;
use rqe_iterators::RQEIteratorError;
use top_k::{BatchStrategy, ScoreSource};

use crate::batch_cursor::VecSimScoreBatchCursor;

/// Adhoc-BF path selection and the scan-scoped state owned by each path.
///
/// State inside a variant is live only between
/// [`VectorScoreSource::begin_adhoc`] and [`VectorScoreSource::end_adhoc`].
enum AdhocPathState {
    /// RAM index: adhoc-BF uses [`VecSimIndex_GetDistanceFrom_Unsafe`] under
    /// tiered-index shared locks acquired once per scan.
    Ram {
        /// `true` while the shared lock is held; `end_adhoc` only releases
        /// when this is set.
        locked: bool,
    },
    /// Disk index: adhoc-BF uses a one-shot [`VecSimAdhocBfCtx`] created by
    /// `begin_adhoc` and freed by `end_adhoc`.
    ///
    /// [`VecSimAdhocBfCtx`]: ffi::VecSimAdhocBfCtx
    Disk {
        /// `Some` between `begin_adhoc` and `end_adhoc`; `None` outside.
        ctx: Option<NonNull<VecSimAdhocBfCtx>>,
    },
}

/// A [`ScoreSource`] that drives top-k collection against a VecSim index.
///
/// Two adhoc-BF execution paths are supported:
///
/// - RAM uses [`VecSimIndex_GetDistanceFrom_Unsafe`] under shared locks
/// - Disk uses an [`VecSimAdhocBfCtx`] that preprocesses the query once per scan.
///
/// [`VecSimAdhocBfCtx`]: ffi::VecSimAdhocBfCtx
pub struct VectorScoreSource {
    /// Non-owning; the C caller retains the index lifetime.
    index: NonNull<VecSimIndex>,
    /// Byte blob of the query vector (f32 elements).
    query_vector: Vec<u8>,
    query_params: VecSimQueryParams,
    k: NonZeroUsize,
    /// Passed through to VecSim as the timeout context pointer.
    timeout_ctx: *mut c_void,
    /// Adhoc-BF path selection and scan-scoped state.
    adhoc_state: AdhocPathState,

    // ── Batch config (immutable after construction) ─────────────────────────
    /// 0 = dynamic (computed per batch).
    fixed_batch_size: usize,
    /// Seed for `child_num_estimated`; restored on rewind.
    initial_child_num_estimated: usize,

    // ── Batch state (reset on rewind) ───────────────────────────────────────
    /// Lazy; created on the first `next_batch` call.
    batch_iter: Option<NonNull<VecSimBatchIterator>>,
    num_iterations: usize,
    /// Rolling estimate of how many child docs pass the filter;
    /// seeded from `initial_child_num_estimated`, refined each batch.
    child_num_estimated: usize,
    /// `k - heap_count`, updated by `batch_strategy`.
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
    /// - `index` must remain valid for the lifetime of this struct.
    /// - `timeout_ctx` must be valid for the duration of the query, or null.
    #[expect(clippy::too_many_arguments)]
    pub unsafe fn new(
        index: NonNull<VecSimIndex>,
        query_vector: Vec<u8>,
        query_params: VecSimQueryParams,
        k: NonZeroUsize,
        timeout_ctx: *mut c_void,
        is_disk: bool,
        child_num_estimated: usize,
        fixed_batch_size: usize,
    ) -> Self {
        let adhoc_state = if is_disk {
            AdhocPathState::Disk { ctx: None }
        } else {
            AdhocPathState::Ram { locked: false }
        };
        Self {
            index,
            query_vector,
            query_params,
            k,
            timeout_ctx,
            adhoc_state,
            batch_iter: None,
            fixed_batch_size,
            num_iterations: 0,
            child_num_estimated,
            initial_child_num_estimated: child_num_estimated,
            k_remaining: k.get(),
        }
    }

    /// Return the number of vectors currently in the index.
    pub fn index_size(&self) -> usize {
        // SAFETY: `self.index` is valid for the struct's lifetime.
        unsafe { VecSimIndex_IndexSize(self.index.as_ptr()) }
    }

    /// Ask VecSim whether adhoc-BF is preferred over batches for the given
    /// filter-subset size and k.
    pub fn prefer_adhoc(&self, subset_size: usize, k: usize, initial_check: bool) -> bool {
        // SAFETY: `self.index` is valid for the struct's lifetime.
        unsafe { VecSimIndex_PreferAdHocSearch(self.index.as_ptr(), subset_size, k, initial_check) }
    }

    /// Compute the next batch size.
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
    /// Caller must hold the tiered-index shared lock — `begin_adhoc` acquires
    /// it once for the whole scan, matching the C reference
    /// (`hybrid_reader.c:279-300`).
    fn lookup_score_ram(&self, doc_id: t_docId) -> Option<f64> {
        // SAFETY: `self.index` and `self.query_vector` are valid; the
        // shared lock is held for the duration of the adhoc scan.
        let distance = unsafe {
            VecSimIndex_GetDistanceFrom_Unsafe(
                self.index.as_ptr(),
                doc_id as usize,
                self.query_vector.as_ptr() as *const c_void,
            )
        };
        if distance.is_nan() {
            None
        } else {
            Some(distance)
        }
    }

    /// Adhoc-BF distance lookup for disk indexes using the scan-scoped
    /// [`VecSimAdhocBfCtx`] created in `begin_adhoc`.
    ///
    /// Returns `None` if the context was not created (begin_adhoc skipped, or
    /// `VecSimIndex_AdhocBfCtx_New` returned null).
    ///
    /// [`VecSimAdhocBfCtx`]: ffi::VecSimAdhocBfCtx
    fn lookup_score_disk(&self, ctx: NonNull<VecSimAdhocBfCtx>, doc_id: t_docId) -> Option<f64> {
        // SAFETY: `ctx` is live for the duration of the scan.
        let distance =
            unsafe { VecSimIndex_AdhocBfCtx_GetDistanceFrom(ctx.as_ptr(), doc_id as usize) };
        if distance.is_nan() {
            None
        } else {
            Some(distance)
        }
    }
}

impl ScoreSource for VectorScoreSource {
    type Batch = VecSimScoreBatchCursor;

    fn next_batch_unfiltered(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // Single-shot HNSW query for the unfiltered path; called exactly once
        // per evaluation by `prepare_unfiltered_direct`.
        self.query_params.timeoutCtx = self.timeout_ctx;
        // SAFETY: `self.index` and `self.query_vector` are valid.
        let reply = unsafe {
            VecSimIndex_TopKQuery(
                self.index.as_ptr(),
                self.query_vector.as_ptr() as *const c_void,
                self.k.get(),
                &mut self.query_params,
                VecSimQueryReply_Order_BY_ID,
            )
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
        if iter.is_null() {
            // SAFETY: we still own `reply` and must free it before returning.
            unsafe { ffi::VecSimQueryReply_Free(reply) };
            return Ok(None);
        }
        // SAFETY: Ownership of both `reply` and `iter` is passed to the cursor.
        let cursor = unsafe { VecSimScoreBatchCursor::new(reply, iter) };
        Ok(Some(cursor))
    }

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
                    self.index.as_ptr(),
                    self.query_vector.as_ptr() as *const c_void,
                    &mut self.query_params,
                )
            };
            let Some(iter) = NonNull::new(iter) else {
                return Ok(None);
            };
            self.batch_iter = Some(iter);
        }

        let batch_iter = self.batch_iter.unwrap();

        // SAFETY: `batch_iter` is valid.
        if !unsafe { VecSimBatchIterator_HasNext(batch_iter.as_ptr()) } {
            return Ok(None);
        }

        let batch_size = self.compute_batch_size();
        self.num_iterations += 1;

        // SAFETY: `batch_iter` is valid.
        let reply = unsafe {
            VecSimBatchIterator_Next(
                batch_iter.as_ptr(),
                batch_size,
                VecSimQueryReply_Order_BY_ID,
            )
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
        if iter.is_null() {
            // SAFETY: we still own `reply` and must free it before returning.
            unsafe { ffi::VecSimQueryReply_Free(reply) };
            return Ok(None);
        }
        // SAFETY: Ownership of both `reply` and `iter` is passed to the cursor.
        let cursor = unsafe { VecSimScoreBatchCursor::new(reply, iter) };
        Ok(Some(cursor))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        match self.adhoc_state {
            AdhocPathState::Ram { locked: true } => self.lookup_score_ram(doc_id),
            AdhocPathState::Ram { locked: false } => None,
            AdhocPathState::Disk { ctx: Some(ctx) } => self.lookup_score_disk(ctx, doc_id),
            AdhocPathState::Disk { ctx: None } => None,
        }
    }

    fn begin_adhoc(&mut self) {
        match &mut self.adhoc_state {
            AdhocPathState::Ram { locked } => {
                debug_assert!(!*locked, "begin_adhoc called twice without end_adhoc");
                // SAFETY: `self.index` is valid for the struct's lifetime.
                unsafe { VecSimTieredIndex_AcquireSharedLocks(self.index.as_ptr()) };
                *locked = true;
            }
            AdhocPathState::Disk { ctx } => {
                debug_assert!(ctx.is_none(), "begin_adhoc called twice without end_adhoc");
                // SAFETY: `self.index` and `self.query_vector` are valid.
                let new_ctx = unsafe {
                    VecSimIndex_AdhocBfCtx_New(
                        self.index.as_ptr(),
                        self.query_vector.as_ptr() as *const c_void,
                    )
                };
                // `VecSimIndex_AdhocBfCtx_New` is documented to return non-null
                // on the disk path; this state variant is only reachable when
                // `is_disk` was true at construction.
                debug_assert!(
                    !new_ctx.is_null(),
                    "VecSimIndex_AdhocBfCtx_New returned null on disk path"
                );
                if new_ctx.is_null() {
                    // Invariant broken in a release build: without a context every
                    // `lookup_score` returns `None`, so the scan silently yields no
                    // results. Log once per scan so the failure is diagnosable.
                    tracing::error!(
                        "VecSimIndex_AdhocBfCtx_New returned null on disk path; adhoc scan will yield no results"
                    );
                }
                *ctx = NonNull::new(new_ctx);
            }
        }
    }

    fn end_adhoc(&mut self) {
        match &mut self.adhoc_state {
            AdhocPathState::Ram { locked } => {
                if *locked {
                    // SAFETY: `self.index` is valid; we acquired the lock in `begin_adhoc`.
                    unsafe { VecSimTieredIndex_ReleaseSharedLocks(self.index.as_ptr()) };
                    *locked = false;
                }
            }
            AdhocPathState::Disk { ctx } => {
                if let Some(c) = ctx.take() {
                    // SAFETY: `c` was returned non-null by `AdhocBfCtx_New` and
                    // is not used after this call.
                    unsafe { VecSimIndex_AdhocBfCtx_Free(c.as_ptr()) };
                }
            }
        }
    }

    fn num_estimated(&self) -> usize {
        self.k.get().min(self.index_size())
    }

    fn rewind(&mut self) {
        if let Some(iter) = self.batch_iter.take() {
            // SAFETY: `iter` is valid and we're taking ownership.
            unsafe { VecSimBatchIterator_Free(iter.as_ptr()) };
        }
        self.num_iterations = 0;
        self.k_remaining = self.k.get();
        self.child_num_estimated = self.initial_child_num_estimated;
    }
    fn build_result<'r>(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_metric()
            .doc_id(doc_id)
            .num_value(score)
            .build()
        // TODO: MOD-14210: push the score to `result.metrics`. This needs `self` to know its key.
    }

    fn iterator_type(&self) -> rqe_iterators::IteratorType {
        rqe_iterators::IteratorType::Hybrid
    }

    fn batch_strategy(&mut self, heap_count: usize, k: usize) -> BatchStrategy {
        // n_res_left = k - heap_count_before_this_batch (k_remaining set by previous call).
        let n_res_left = self.k_remaining;
        self.k_remaining = k.saturating_sub(heap_count);
        if heap_count >= k {
            return BatchStrategy::Stop;
        }
        // Refine `child_num_estimated` from actual batch hit rate so subsequent heuristic
        // calls reflect observed selectivity rather than the initial guess.
        // n_res_left > 0 is guaranteed: heap_count < k (checked above) and k_remaining >= 1.
        let new_results_cur_batch = heap_count.saturating_sub(k.saturating_sub(n_res_left));
        let index_size = self.index_size();
        let cur_ratio = new_results_cur_batch as f64 / n_res_left as f64;
        let cur_child_est = (cur_ratio * index_size as f64) as usize;
        // Rolling average; cap at old estimate to suppress upward drift.
        let old_est = self.child_num_estimated;
        self.child_num_estimated = ((old_est + cur_child_est) / 2).min(old_est);

        // SAFETY: `self.index` is valid.
        let prefer_adhoc = unsafe {
            VecSimIndex_PreferAdHocSearch(self.index.as_ptr(), self.child_num_estimated, k, false)
        };
        if prefer_adhoc {
            BatchStrategy::SwitchToAdhoc
        } else {
            BatchStrategy::Continue
        }
    }
}

impl Drop for VectorScoreSource {
    fn drop(&mut self) {
        if let Some(iter) = self.batch_iter.take() {
            // SAFETY: `iter` is valid.
            unsafe { VecSimBatchIterator_Free(iter.as_ptr()) };
        }
        // Defensive: release any adhoc-scoped resources still held (RAM shared
        // lock or Disk `VecSimAdhocBfCtx`) if a scan was abandoned without a
        // matching `end_adhoc` call. `end_adhoc` is idempotent.
        self.end_adhoc();
    }
}
