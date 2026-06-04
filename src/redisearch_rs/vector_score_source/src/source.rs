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

use ffi::{TimeoutCtx, VecSimIndex, VecSimQueryParams, t_docId, timespec};
use index_result::RSIndexResult;
use rqe_iterators::RQEIteratorError;
use top_k::{BatchStrategy, ScoreSource};
use vecsim::{
    AdhocBfCtx, BatchIterator, IndexRef, QueryError, QueryVector, ReplyOrder, SharedLockGuard,
};

use crate::batch_cursor::VecSimScoreBatchCursor;

/// Adhoc-BF path selection and the scan-scoped state owned by each path.
///
/// State inside a variant is live only between
/// [`VectorScoreSource::begin_adhoc`] and [`VectorScoreSource::end_adhoc`].
enum AdhocPathState<'index> {
    /// RAM index: adhoc-BF uses the unsafe RAM distance lookup under
    /// tiered-index shared locks acquired once per scan.
    Ram {
        /// `Some` while the shared locks are held; cleared by `end_adhoc`.
        guard: Option<SharedLockGuard<'index>>,
    },
    /// Disk index: adhoc-BF uses a one-shot [`AdhocBfCtx`] created by
    /// `begin_adhoc` and freed by `end_adhoc`.
    Disk {
        /// `Some` between `begin_adhoc` and `end_adhoc`; `None` outside.
        ctx: Option<AdhocBfCtx<'index>>,
    },
}

/// A [`ScoreSource`] that drives top-k collection against a VecSim index.
///
/// Two adhoc-BF execution paths are supported:
///
/// - RAM uses the unsafe RAM distance lookup under shared locks
/// - Disk uses a preprocessed [`AdhocBfCtx`] per scan.
pub struct VectorScoreSource<'index> {
    /// Non-owning reference to the VecSim index.
    ///
    /// `'index` reflects the [`VectorScoreSource::new`] safety contract: the C
    /// caller keeps the index alive for at least `'index`, which outlives this
    /// struct.
    index: IndexRef<'index>,
    /// Query vector, length-checked against [`index`](Self::index) at
    /// construction. Owned by `self` and reused by every query path.
    query_vector: QueryVector<'index>,
    query_params: VecSimQueryParams,
    k: NonZeroUsize,
    /// Heap-allocated timeout context passed to VecSim as a `*mut c_void`.
    /// Boxed so the pointer remains stable even if `VectorScoreSource` is moved.
    timeout_ctx: Box<TimeoutCtx>,
    /// Adhoc-BF path selection and scan-scoped state.
    adhoc_state: AdhocPathState<'index>,

    /// Fixed batch size; `0` means compute dynamically per batch. Immutable
    /// after construction.
    fixed_batch_size: usize,
    /// Seed for [`child_num_estimated`](Self::child_num_estimated); restored
    /// on rewind. Immutable after construction.
    initial_child_num_estimated: usize,

    /// Batch iterator, lazily created on the first `next_batch` call. Reset
    /// on rewind.
    ///
    /// Both lifetime slots are `'index`: the iterator borrows the index (`'index`)
    /// and reads the `timeoutCtx` referent, which [`new`](Self::new) requires
    /// to stay valid for `'index` as well.
    batch_iter: Option<BatchIterator<'index, 'index>>,
    /// Number of batches consumed so far. Reset on rewind.
    num_iterations: usize,
    /// Rolling estimate of how many child docs pass the filter; seeded from
    /// [`initial_child_num_estimated`](Self::initial_child_num_estimated) and
    /// refined each batch. Reset on rewind.
    child_num_estimated: usize,
    /// `k - heap_count`, updated by `batch_strategy`. Reset on rewind.
    k_remaining: usize,
}

// SAFETY: VectorScoreSource is used from a single thread (the query execution
// thread). The raw pointers are non-owning (index, timeout_ctx) or are managed
// by the struct itself (batch_iter). It is the caller's responsibility to
// ensure the index outlives the iterator.
unsafe impl Send for VectorScoreSource<'_> {}

impl<'index> VectorScoreSource<'index> {
    /// Create a new `VectorScoreSource`.
    ///
    /// `timeout` is the query deadline as an absolute `timespec`.
    /// `skip_timeout_checks` mirrors `sctx->time.skipTimeoutChecks`: when
    /// `true`, the VecSim periodic counter check is disabled
    /// (`REDISEARCH_UNINITIALIZED`).
    ///
    /// # Safety
    ///
    /// - `index` must remain valid for `'index`, which outlives the returned
    ///   `VectorScoreSource<'index>`.
    /// - `timeout_ctx` must remain valid for `'index`, or be null. The lazily
    ///   created batch iterator copies it and reads it on every batch, so it
    ///   must outlive the iterator (which never outlives the struct).
    /// - `query_vector` must satisfy the [`QueryVector`] length invariant for
    ///   `index`. Every query path hands it to VecSim, which reads that many
    ///   bytes; a shorter blob is read out of bounds.
    #[expect(clippy::too_many_arguments)]
    pub unsafe fn new(
        index: NonNull<VecSimIndex>,
        query_vector: Vec<u8>,
        query_params: VecSimQueryParams,
        k: NonZeroUsize,
        timeout: timespec,
        skip_timeout_checks: bool,
        is_disk: bool,
        child_num_estimated: usize,
        fixed_batch_size: usize,
    ) -> Self {
        // SAFETY: caller-upheld: `index` is valid for the struct's lifetime.
        let index = unsafe { IndexRef::from_raw(index) };
        // SAFETY: caller-upheld: `query_vector` satisfies the `QueryVector`
        // length invariant for `index`, the layout VecSim reads on every query path.
        let query_vector = unsafe { QueryVector::new(index, query_vector) };
        let adhoc_state = if is_disk {
            AdhocPathState::Disk { ctx: None }
        } else {
            AdhocPathState::Ram { guard: None }
        };
        // `child_num_estimated` is the child's `NumEstimated`, an upper bound
        // that can exceed the index size. Clamp it.
        let child_num_estimated = child_num_estimated.min(index.size());
        Self {
            index,
            query_vector,
            query_params,
            k,
            timeout_ctx: Box::new(TimeoutCtx {
                timeout,
                // u32::MAX ≡ REDISEARCH_UNINITIALIZED = (uint32_t)(-1)
                counter: if skip_timeout_checks { u32::MAX } else { 0 },
            }),
            adhoc_state,
            batch_iter: None,
            fixed_batch_size,
            num_iterations: 0,
            child_num_estimated,
            initial_child_num_estimated: child_num_estimated,
            k_remaining: k.get(),
        }
    }

    /// Return a `*mut c_void` pointing to the owned [`TimeoutCtx`],
    /// suitable for assignment to [`VecSimQueryParams::timeoutCtx`].
    fn timeout_ctx_ptr(&mut self) -> *mut c_void {
        self.timeout_ctx.as_mut() as *mut TimeoutCtx as *mut c_void
    }

    /// Return the number of vectors currently in the index.
    pub fn index_size(&self) -> usize {
        self.index.size()
    }

    /// Ask VecSim whether adhoc-BF is preferred over batches for the given
    /// filter-subset size and k.
    pub fn prefer_adhoc(&self, subset_size: usize, k: usize, initial_check: bool) -> bool {
        self.index.prefer_adhoc(subset_size, k, initial_check)
    }

    /// Compute the next batch size.
    fn compute_batch_size(&self) -> NonZeroUsize {
        if let Some(fixed) = NonZeroUsize::new(self.fixed_batch_size) {
            return fixed;
        }
        let index_size = self.index_size();
        let child_est = self.child_num_estimated;
        let estimate = self.k_remaining * index_size /
            // guard div-by-zero
            child_est.max(1);
        // The `+ 1` guarantees a non-zero size.
        NonZeroUsize::new(estimate + 1).unwrap()
    }
}

impl<'index> ScoreSource for VectorScoreSource<'index> {
    type Batch = VecSimScoreBatchCursor;

    fn all_results_unfiltered_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // Single-shot top-k query for the unfiltered path; called exactly once
        // per evaluation by `prepare_unfiltered_direct`.
        self.query_params.timeoutCtx = self.timeout_ctx_ptr();
        let reply = self
            .index
            .top_k_query(
                &self.query_vector,
                self.k,
                &mut self.query_params,
                ReplyOrder::ByScore,
            )
            .map_err(|QueryError::TimedOut| RQEIteratorError::TimedOut)?;
        Ok(reply
            .and_then(|r| r.into_results())
            .map(VecSimScoreBatchCursor::new))
    }

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        // Timeout is propagated via self.query_params.timeoutCtx. VecSim checks it
        // internally and returns VecSim_QueryReply_TimedOut from Next/New.

        // Lazily create the batch iterator on first call.
        if self.batch_iter.is_none() {
            if self.initial_child_num_estimated == 0 {
                return Ok(None);
            }
            // Pass timeout context via query params so VecSim handles it.
            self.query_params.timeoutCtx = self.timeout_ctx_ptr();
            // Raw pointer rather than `&mut self.query_params`: the iterator is
            // stored back into `self`, so a `'params` borrow of our own field
            // would be self-referential. `batch_iterator_unchecked` lets us pick
            // `'params = 'index` instead.
            let params: *mut VecSimQueryParams = &mut self.query_params;
            // SAFETY:
            // - `params` points to `self.query_params`, valid for this call.
            // - The returned iterator's `'params` is unified with `'index`. Its
            //   `timeoutCtx` is `self.timeout_ctx`, which `new` requires to stay
            //   valid for `'index`, so the referent outlives the iterator.
            self.batch_iter = unsafe {
                self.index
                    .batch_iterator_unchecked(&self.query_vector, params)
            };
            if self.batch_iter.is_none() {
                return Ok(None);
            }
        }

        if !self
            .batch_iter
            .as_ref()
            .expect("just initialised above")
            .has_next()
        {
            return Ok(None);
        }

        let batch_size = self.compute_batch_size();
        self.num_iterations += 1;

        let batch_iter = self.batch_iter.as_mut().expect("just initialised above");
        let reply = batch_iter
            .next(batch_size, ReplyOrder::ById)
            .map_err(|QueryError::TimedOut| RQEIteratorError::TimedOut)?;
        Ok(reply
            .and_then(|r| r.into_results())
            .map(VecSimScoreBatchCursor::new))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        match &self.adhoc_state {
            AdhocPathState::Ram { guard: Some(g) } => g.get_distance_from(doc_id),
            AdhocPathState::Ram { guard: None } => None,
            AdhocPathState::Disk { ctx: Some(ctx) } => ctx.get_distance_from(doc_id),
            AdhocPathState::Disk { ctx: None } => None,
        }
    }

    fn begin_adhoc(&mut self) {
        // Release the batch iterator before acquiring adhoc resources: it and the
        // adhoc locks contend for the same index lock, which cannot be held twice.
        self.batch_iter = None;
        match &mut self.adhoc_state {
            AdhocPathState::Ram { guard } => {
                debug_assert!(
                    guard.is_none(),
                    "begin_adhoc called twice without end_adhoc"
                );
                *guard = Some(self.index.acquire_ram_shared_locks(&self.query_vector));
            }
            AdhocPathState::Disk { ctx } => {
                debug_assert!(ctx.is_none(), "begin_adhoc called twice without end_adhoc");
                let new_ctx = self.index.adhoc_bf_ctx(&self.query_vector);
                debug_assert!(
                    new_ctx.is_some(),
                    "VecSimIndex_AdhocBfCtx_New returned null on disk path"
                );
                if new_ctx.is_none() {
                    // Invariant broken in a release build: without a context every
                    // `lookup_score` returns `None`, so the scan silently yields no
                    // results. Log once per scan so the failure is diagnosable.
                    tracing::error!(
                        "VecSimIndex_AdhocBfCtx_New returned null on disk path; adhoc scan will yield no results"
                    );
                }
                *ctx = new_ctx;
            }
        }
    }

    fn end_adhoc(&mut self) {
        match &mut self.adhoc_state {
            AdhocPathState::Ram { guard } => {
                // Drop the guard, which releases the locks via `SharedLockGuard::Drop`.
                *guard = None;
            }
            AdhocPathState::Disk { ctx } => {
                // Drop the context, which frees it via `AdhocBfCtx::Drop`.
                *ctx = None;
            }
        }
    }

    fn num_estimated(&self) -> usize {
        self.k.get().min(self.index_size())
    }

    fn rewind(&mut self) {
        // Drop the batch iterator, which frees it via `BatchIterator::Drop`.
        self.batch_iter = None;
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
        // Results still needed at the start of this batch.
        let n_res_left = self.k_remaining;
        // Persist for next call of `batch_strategy`: how many results will still be needed after this batch.
        self.k_remaining = k.saturating_sub(heap_count);
        if heap_count >= k {
            return BatchStrategy::Stop;
        }
        // Refine `child_num_estimated` from actual batch hit rate so subsequent heuristic
        // calls reflect observed selectivity rather than the initial guess.
        // n_res_left > 0 is guaranteed: heap_count < k (checked above) and k_remaining >= 1.
        let new_results_cur_batch = heap_count.saturating_sub(k.saturating_sub(n_res_left));
        self.child_num_estimated = refine_child_estimated(
            self.child_num_estimated,
            new_results_cur_batch,
            n_res_left,
            self.index_size(),
        );

        let prefer_adhoc = self.index.prefer_adhoc(self.child_num_estimated, k, false);
        if prefer_adhoc {
            BatchStrategy::SwitchToAdhoc
        } else {
            BatchStrategy::Continue
        }
    }
}

/// Smoothed update of the child-results estimate, averaging the previous
/// estimate with the one implied by this batch's hit rate. Capped at `old_est`,
/// so the estimate is monotonically non-increasing across batches.
fn refine_child_estimated(
    old_est: usize,
    new_results_cur_batch: usize,
    n_res_left: usize,
    index_size: usize,
) -> usize {
    let cur_ratio = new_results_cur_batch as f64 / n_res_left as f64;
    let cur_child_est = (cur_ratio * index_size as f64) as usize;
    ((old_est + cur_child_est) / 2).min(old_est)
}

#[cfg(test)]
mod tests {
    use super::refine_child_estimated;

    #[test]
    fn never_increases_above_previous() {
        assert_eq!(refine_child_estimated(10, 5, 10, 100), 10);
    }

    #[test]
    fn zero_hit_batch_halves() {
        assert_eq!(refine_child_estimated(80, 0, 10, 1_000), 40);
    }

    #[test]
    fn high_hit_batch_clamped_at_previous() {
        assert_eq!(refine_child_estimated(500, 10, 10, 1_000), 500);
    }

    #[test]
    fn cannot_recover_from_zero() {
        assert_eq!(refine_child_estimated(0, 1, 10, 1_000), 0);
    }
}
