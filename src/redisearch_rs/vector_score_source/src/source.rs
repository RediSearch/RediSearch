/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VectorScoreSource`] — [`ScoreSource`] implementation backed by VecSim.

use std::{
    ffi::{c_int, c_void},
    num::NonZeroUsize,
    ptr,
};

use ffi::{
    QueryIterator, RLookupKey, RLookupKeyHandle, TimeoutCtx, VecSearchMode, VecSimBatchIterator,
    VecSimBatchIterator_Free, VecSimBatchIterator_HasNext, VecSimBatchIterator_New,
    VecSimBatchIterator_Next, VecSimIndex, VecSimIndex_AdhocBfCtx_Free,
    VecSimIndex_AdhocBfCtx_GetDistanceFrom, VecSimIndex_AdhocBfCtx_New, VecSimIndex_BasicInfo,
    VecSimIndex_GetDistanceFrom_Unsafe, VecSimIndex_IndexSize, VecSimIndex_PreferAdHocSearch,
    VecSimIndex_TopKQuery, VecSimMetric_VecSimMetric_Cosine, VecSimParams_GetQueryBlobSize,
    VecSimQueryParams, VecSimQueryReply_Code_VecSim_QueryReply_TimedOut, VecSimQueryReply_GetCode,
    VecSimQueryReply_GetIterator, VecSimQueryReply_Order_BY_ID, VecSimQueryReply_Order_BY_SCORE,
    VecSim_Normalize, t_docId, timespec,
};
use inverted_index::RSIndexResult;
use rqe_iterators::RQEIteratorError;
use rqe_iterators::expiration_checker::{ExpirationChecker, FieldExpirationChecker};
use top_k::{AdhocStrategy, BatchStrategy, ScoreSource};

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
    /// Byte blob of the query vector (raw, as supplied by the caller).
    /// Passed to `VecSimIndex_TopKQuery` and `VecSimBatchIterator_New`, both of
    /// which apply metric-specific preprocessing (e.g. cosine normalization)
    /// internally.
    query_vector: Vec<u8>,
    /// Byte blob used for the RAM Adhoc-BF distance lookup
    /// ([`VecSimIndex_GetDistanceFrom_Unsafe`]).
    ///
    /// The `_Unsafe` variant trusts the caller to have already preprocessed the
    /// query, so for cosine metric we hold a normalized copy here. For other
    /// metrics this is `None` and `lookup_score_ram` falls back to
    /// `query_vector` (matching `hybrid_reader.c`'s `computeDistances_RAM`,
    /// which only normalizes for `VecSimMetric_Cosine`).
    lookup_query_vector: Option<Vec<u8>>,
    query_params: VecSimQueryParams,
    k: NonZeroUsize,
    /// Heap-allocated timeout context passed to VecSim as a `*mut c_void`.
    /// Boxed so the pointer remains stable even if `VectorScoreSource` is moved.
    timeout_ctx: Box<TimeoutCtx>,
    /// `true` → disk-index adhoc path; `false` → RAM adhoc path.
    is_disk: bool,

    // ── Batch-mode state (reset on rewind) ──────────────────────────────────
    /// Lazy; created on the first `next_batch` call.
    batch_iter: Option<*mut VecSimBatchIterator>,
    /// 0 = dynamic (computed per batch).
    fixed_batch_size: usize,
    /// Total number of batches fetched so far (Batches mode only).
    pub num_iterations: usize,
    /// Maximum batch size used across all batches (Batches mode only).
    pub max_batch_size: usize,
    /// Zero-based batch index at which `max_batch_size` was first observed.
    pub max_batch_iteration: usize,
    /// Rolling estimate of how many child docs pass the filter;
    /// seeded by the caller at construction time, refined each batch.
    child_num_estimated: usize,
    /// Snapshot of `child_num_estimated` at construction; restored on rewind.
    initial_child_num_estimated: usize,
    /// `k - heap_count`, updated by `batch_strategy`.
    k_remaining: usize,
    /// `true` once `next_batch_unfiltered` has been called; subsequent calls
    /// short-circuit to `Ok(None)` so the source honors the single-shot
    /// contract without re-issuing the HNSW query (which would also re-poll
    /// the timeout context and could spuriously fail). Reset by `rewind`.
    unfiltered_consumed: bool,

    /// Optional field-expiration filter for the vector field. Mirrors the C
    /// `HybridIterator`'s post-yield `DocTable_VerifyFieldExpirationPredicate`
    /// check: docs whose vector field has expired are dropped from each path
    /// (cursor batch yield + Adhoc-BF lookup).
    expiration: Option<FieldExpirationChecker>,

    // ── FFI-specific fields (set by C callers via accessor functions) ────────
    /// Score key for this iterator's metric output; set by the metrics loader.
    pub own_key: *mut RLookupKey,
    /// Back-reference to the handle that points to `own_key`; set alongside it.
    pub key_handle: *mut RLookupKeyHandle,
    /// Raw owning pointer to the C child iterator; stored for `HybridIterator_GetChild`.
    pub child_raw: *mut QueryIterator,
}

// SAFETY: VectorScoreSource is used from a single thread (the query execution
// thread). `index`, `own_key`, `key_handle`, and `child_raw` are non-owning raw
// pointers; the caller ensures they outlive the iterator. `batch_iter` is managed
// by the struct itself. All other fields are owned Rust values.
unsafe impl Send for VectorScoreSource {}

impl VectorScoreSource {
    /// Create a new `VectorScoreSource`.
    ///
    /// `timeout` is the query deadline as an absolute `timespec`.
    /// `skip_timeout_checks` mirrors `sctx->time.skipTimeoutChecks`: when
    /// `true`, the VecSim periodic counter check is disabled
    /// (`REDISEARCH_UNINITIALIZED`).
    ///
    /// # Safety
    ///
    /// - `index` must be a valid, non-null pointer that remains valid for the
    ///   lifetime of this struct.
    #[expect(clippy::too_many_arguments)]
    pub unsafe fn new(
        index: *mut VecSimIndex,
        query_vector: Vec<u8>,
        mut query_params: VecSimQueryParams,
        k: NonZeroUsize,
        timeout: timespec,
        skip_timeout_checks: bool,
        is_disk: bool,
        child_num_estimated: usize,
        fixed_batch_size: usize,
        expiration: Option<FieldExpirationChecker>,
    ) -> Self {
        // Allocate the timeout context before building the struct so we can
        // bake the stable heap pointer into `query_params.timeoutCtx` once.
        // Box's pointee address never changes even if `VectorScoreSource` is moved.
        let mut timeout_ctx = Box::new(TimeoutCtx {
            timeout,
            // u32::MAX ≡ REDISEARCH_UNINITIALIZED = (uint32_t)(-1)
            counter: if skip_timeout_checks { u32::MAX } else { 0 },
        });
        query_params.timeoutCtx = timeout_ctx.as_mut() as *mut TimeoutCtx as *mut c_void;

        // For cosine indexes, the RAM Adhoc-BF path calls
        // `VecSimIndex_GetDistanceFrom_Unsafe`, which assumes the query has
        // already been normalized — see `hybrid_reader.c::computeDistances_RAM`.
        // Pre-build the normalized blob once. `VecSimParams_GetQueryBlobSize`
        // sizes the buffer correctly for INT8/UINT8 cosine, where
        // `VecSim_Normalize` appends a norm float past `dim*sizeof(type)`.
        // SAFETY: `index` is valid per caller's contract.
        let lookup_query_vector = if is_disk {
            // Disk path uses `VecSimIndex_AdhocBfCtx_New`, which preprocesses
            // the query internally; nothing to do here.
            None
        } else {
            let info = unsafe { VecSimIndex_BasicInfo(index) };
            if info.metric == VecSimMetric_VecSimMetric_Cosine {
                // SAFETY: `info.type_`, `info.dim`, `info.metric` are valid.
                let blob_size =
                    unsafe { VecSimParams_GetQueryBlobSize(info.type_, info.dim, info.metric) };
                let mut blob = vec![0u8; blob_size];
                let copy_len = query_vector.len().min(blob_size);
                blob[..copy_len].copy_from_slice(&query_vector[..copy_len]);
                // SAFETY: `blob` is `blob_size` bytes, matching what
                // `VecSim_Normalize` expects for the given (type, dim, cosine).
                unsafe { VecSim_Normalize(blob.as_mut_ptr() as *mut c_void, info.dim, info.type_) };
                Some(blob)
            } else {
                None
            }
        };

        Self {
            index,
            query_vector,
            lookup_query_vector,
            query_params,
            k,
            timeout_ctx,
            is_disk,
            batch_iter: None,
            fixed_batch_size,
            num_iterations: 0,
            max_batch_size: 0,
            max_batch_iteration: 0,
            child_num_estimated,
            initial_child_num_estimated: child_num_estimated,
            k_remaining: k.get(),
            unfiltered_consumed: false,
            expiration,
            own_key: ptr::null_mut(),
            key_handle: ptr::null_mut(),
            child_raw: ptr::null_mut(),
        }
    }

    /// Return the number of vectors currently in the index.
    pub fn index_size(&self) -> usize {
        // SAFETY: `self.index` is valid for the struct's lifetime.
        unsafe { VecSimIndex_IndexSize(self.index) }
    }

    /// User-supplied [`VecSearchMode`] from the query's `HYBRID_POLICY` clause,
    /// or `EMPTY_MODE` (0) when the user did not set one.
    pub fn user_search_mode(&self) -> VecSearchMode {
        self.query_params.searchMode
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
        // For cosine, use the pre-normalized blob built in `new`; the `_Unsafe`
        // variant doesn't normalize for us. Other metrics use the raw query.
        let blob = self
            .lookup_query_vector
            .as_deref()
            .unwrap_or(&self.query_vector);
        // SAFETY: `self.index` is valid; `blob` is non-null and sized for
        // the index's (type, dim, metric).
        let distance = unsafe {
            VecSimTieredIndex_AcquireSharedLocks(self.index);
            let d = VecSimIndex_GetDistanceFrom_Unsafe(
                self.index,
                doc_id as usize,
                blob.as_ptr() as *const c_void,
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

    fn next_batch_unfiltered(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        if self.unfiltered_consumed {
            return Ok(None);
        }
        self.unfiltered_consumed = true;
        // `query_params.timeoutCtx` was set once at construction and remains valid.
        // SAFETY: `self.index` and `self.query_vector` are valid.
        let reply = unsafe {
            VecSimIndex_TopKQuery(
                self.index,
                self.query_vector.as_ptr() as *const c_void,
                self.k.get(),
                &mut self.query_params,
                VecSimQueryReply_Order_BY_SCORE,
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
        // SAFETY: Ownership of both `reply` and `iter` is passed to the cursor.
        let cursor = unsafe { VecSimScoreBatchCursor::new(reply, iter, self.expiration) };
        Ok(Some(cursor))
    }

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // Timeout is propagated via query_params.timeoutCtx — VecSim checks it
        // internally and returns VecSim_QueryReply_TimedOut from Next/New.

        // Lazily create the batch iterator on first call.
        if self.batch_iter.is_none() {
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
        // Track max batch size and which iteration it occurred on (zero-based).
        if batch_size > self.max_batch_size {
            self.max_batch_size = batch_size;
            self.max_batch_iteration = self.num_iterations; // zero-based before increment
        }
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
        let cursor = unsafe { VecSimScoreBatchCursor::new(reply, iter, self.expiration) };
        Ok(Some(cursor))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        if let Some(checker) = self.expiration.as_ref()
            && checker.has_expiration()
        {
            // Mirror the C `HybridIterator`'s post-yield expiration check:
            // expired docs are excluded from the Adhoc-BF result set.
            let probe = RSIndexResult::build_virt().doc_id(doc_id).build();
            if checker.is_expired(&probe) {
                return None;
            }
        }
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
        self.max_batch_size = 0;
        self.max_batch_iteration = 0;
        self.k_remaining = self.k.get();
        self.child_num_estimated = self.initial_child_num_estimated;
        self.unfiltered_consumed = false;
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
            let key: &'index ffi::RLookupKey =
                unsafe { &*(self.own_key as *const ffi::RLookupKey) };
            result.metrics.push_with_key(key, score);
        }
        result
    }

    fn attach_score_metric(&self, result: &mut RSIndexResult<'index>, score: f64) {
        if self.own_key.is_null() {
            return;
        }
        // SAFETY: `own_key` is set by `getAdditionalMetricsRP` in pipeline_construction.c
        // before any reads occur, and the key lives in the query's RLookup structure for
        // at least `'index` (the query lifetime).
        let key: &'index ffi::RLookupKey = unsafe { &*(self.own_key as *const ffi::RLookupKey) };

        // The child reuses one storage slot across yields, so an entry from a
        // previous yield may already exist for our key. Update in place when
        // present; push otherwise. Any non-matching metrics from the child's
        // own subtree are left untouched.
        if let Some(entry) = result.metrics.find_by_key_mut(key) {
            entry.set_value(score);
        } else {
            result.metrics.push_with_key(key, score);
        }
    }

    fn iterator_type(&self) -> rqe_iterators::IteratorType {
        rqe_iterators::IteratorType::Hybrid
    }

    fn adhoc_strategy(&mut self, _heap_count: usize, _k: usize) -> AdhocStrategy {
        // The child yields documents in doc-ID order, not score order, so we
        // must scan every match to guarantee a correct top-k — stopping when
        // the heap fills would freeze the answer at the first k child docs.
        // The bounded `TopKHeap` keeps the result set at k entries regardless.
        // SAFETY: `self.timeout_ctx` is heap-allocated and stable for the struct's lifetime.
        if unsafe { RS_VecSimCheckTimeout(self.timeout_ctx.as_mut()) } != 0 {
            return AdhocStrategy::TimedOut;
        }
        AdhocStrategy::Continue
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
            VecSimIndex_PreferAdHocSearch(self.index, self.child_num_estimated, k, false)
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
            unsafe { VecSimBatchIterator_Free(iter) };
        }
    }
}

// ── C forward declarations ────────────────────────────────────────────────────

unsafe extern "C" {
    fn VecSimTieredIndex_AcquireSharedLocks(index: *mut VecSimIndex);
    fn VecSimTieredIndex_ReleaseSharedLocks(index: *mut VecSimIndex);
    // Thin wrapper around `vecsimTimeoutCallback` (hybrid_reader.c) so the
    // test-mockable function-pointer indirection is preserved.
    fn RS_VecSimCheckTimeout(ctx: *mut TimeoutCtx) -> c_int;
}
