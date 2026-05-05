/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! VecSim-backed [`top_k::ScoreSource`] and [`VectorTopKIterator`] for top-k hybrid queries.
//!
//! # Overview
//!
//! This crate provides the concrete [`VectorScoreSource`] implementation of
//! [`top_k::ScoreSource`] that drives [`TopKIterator`] against a VecSim index.

pub mod batch_cursor;
pub mod source;

pub use batch_cursor::VecSimScoreBatchCursor;
pub use source::VectorScoreSource;

use std::{cmp::Ordering, ffi::CStr, num::NonZeroUsize, ptr::NonNull};

use ffi::{VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES};
use redis_reply::MapBuilder;
use rqe_iterators::{
    ExpirationChecker, FieldExpirationChecker, RQEIterator, c2rust::call_print_profile,
    profile_print::ProfilePrintCtx,
};
use top_k::{TopKIterator, TopKMode, TopKSourceProfile};

/// A [`TopKIterator`] parameterised over [`VectorScoreSource`].
///
/// Use [`new_vector_top_k_unfiltered`] or [`new_vector_top_k_filtered`]
/// to construct one; these constructors encode the mode-selection logic.
///
/// `E` is the [`ExpirationChecker`] strategy, defaulting to the production
/// [`FieldExpirationChecker`].
pub type VectorTopKIterator<'index, E = FieldExpirationChecker> =
    TopKIterator<'index, VectorScoreSource<'index, E>>;

/// Ascending comparator — lower distance score is better (vector L2/IP/Cosine).
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Construct a pure KNN [`VectorTopKIterator`] (no filter child).
///
/// Results are streamed directly from the VecSim batch without a heap,
/// using the [`TopKMode::Unfiltered`] path.
pub fn new_vector_top_k_unfiltered<'index, E: ExpirationChecker + Clone + 'index>(
    source: VectorScoreSource<'index, E>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
    TopKIterator::new_with_mode(source, None, k, asc_cmp, TopKMode::Unfiltered)
}

/// Construct a hybrid [`VectorTopKIterator`] with a filter child.
///
/// When the user pinned a policy via the `HYBRID_POLICY` query attribute
/// (reflected in [`VectorScoreSource::requested_search_mode`]), that policy is
/// honored. Otherwise the initial mode (Batches vs AdhocBF) is chosen via
/// [`VecSimIndex_PreferAdHocSearch`] using the child's estimated result count,
/// and the source may switch modes mid-execution via
/// [`BatchStrategy::SwitchToAdhoc`].
///
/// The child is boxed.
/// Use [`new_vector_top_k_filtered_boxed`] when you already have a `Box`.
///
/// [`VectorScoreSource::requested_search_mode`]: source::VectorScoreSource::requested_search_mode
/// [`VecSimIndex_PreferAdHocSearch`]: ffi::VecSimIndex_PreferAdHocSearch
/// [`BatchStrategy::SwitchToAdhoc`]: top_k::BatchStrategy::SwitchToAdhoc
pub fn new_vector_top_k_filtered<'index, E: ExpirationChecker + Clone + 'index>(
    source: VectorScoreSource<'index, E>,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
    new_vector_top_k_filtered_boxed(source, Box::new(child), k)
}

/// Construct a hybrid [`VectorTopKIterator`] with a boxed filter child.
///
/// Accepts an already-boxed `Box<dyn RQEIterator>`, avoiding an extra
/// allocation when the caller already holds one.
///
/// Mode selection mirrors the C hybrid reader (`hybrid_reader.c`): an explicit
/// `HYBRID_POLICY` is honored, and only an unset policy falls back to the
/// [`VecSimIndex_PreferAdHocSearch`] cost heuristic.
///
/// [`VecSimIndex_PreferAdHocSearch`]: ffi::VecSimIndex_PreferAdHocSearch
pub fn new_vector_top_k_filtered_boxed<'index, E: ExpirationChecker + Clone + 'index>(
    source: VectorScoreSource<'index, E>,
    child: Box<dyn RQEIterator<'index> + 'index>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
    // The user pinned a policy via HYBRID_POLICY: honor it verbatim. HYBRID_BATCHES
    // also suppresses the mid-run switch to adhoc — the C reader's
    // `reviewHybridSearchPolicy` returns false for it — which is exactly what
    // `ForcedBatches` (vs `Batches`) encodes. An unset policy falls back to the
    // cost heuristic, which can still switch to adhoc mid-run.
    let requested = source.requested_search_mode();
    let mode = if requested == VecSearchMode_HYBRID_ADHOC_BF {
        TopKMode::AdhocBF
    } else if requested == VecSearchMode_HYBRID_BATCHES {
        TopKMode::ForcedBatches
    } else {
        let child_est = child.num_estimated().min(source.index_size());
        if source.prefer_adhoc(child_est, k.get(), true) {
            TopKMode::AdhocBF
        } else {
            TopKMode::Batches
        }
    };
    TopKIterator::new_with_mode(source, Some(child), k, asc_cmp, mode)
}

impl TopKSourceProfile for VectorScoreSource<'_> {
    fn print_profile(
        &self,
        mode: TopKMode,
        switches: usize,
        map: &mut MapBuilder<'_>,
        ctx: &mut ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"VECTOR");
        ctx.print_optional_counters(map);

        let mode_cstr: &CStr = match mode {
            TopKMode::Unfiltered => c"STANDARD_KNN",
            TopKMode::Batches | TopKMode::ForcedBatches => c"HYBRID_BATCHES",
            TopKMode::AdhocBF if switches > 0 => c"HYBRID_BATCHES_TO_ADHOC_BF",
            TopKMode::AdhocBF => c"HYBRID_ADHOC_BF",
        };
        map.kv_simple_string(c"Vector search mode", mode_cstr);

        let is_batch_mode = matches!(mode, TopKMode::Batches | TopKMode::ForcedBatches)
            || (matches!(mode, TopKMode::AdhocBF) && switches > 0);
        if is_batch_mode {
            map.kv_long_long(c"Batches number", self.num_iterations as i64);
            map.kv_long_long(c"Largest batch size", self.max_batch_size as i64);
            map.kv_long_long(
                c"Largest batch iteration (zero based)",
                self.max_batch_iteration as i64,
            );
        }

        if let Some(child) = NonNull::new(self.child_raw) {
            let mut child_map = map.kv_map(c"Child iterator");
            let mut child_ctx = ctx.child_ctx();
            // SAFETY: `child_raw` was preserved at construction and points to a
            // valid `QueryIterator` whose `PrintProfile` vtable entry was set
            // by `Profile_AddIters` before this call.
            unsafe { call_print_profile(child, &mut child_map, &mut child_ctx) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::c_void, num::NonZeroUsize, ptr, ptr::NonNull};

    use ffi::{
        AlgoParams, BFParams, VecSearchMode, VecSearchMode_EMPTY_MODE,
        VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES, VecSimAlgo_VecSimAlgo_BF,
        VecSimIndex, VecSimIndex_AddVector, VecSimIndex_Free, VecSimIndex_New,
        VecSimMetric_VecSimMetric_L2, VecSimParams, VecSimQueryParams,
        VecSimType_VecSimType_FLOAT32, timespec,
    };
    use rqe_iterators::{IdList, RQEIterator};
    use top_k::TopKMode;

    use super::{VectorScoreSource, new_vector_top_k_filtered_boxed};

    /// FLAT L2 index of `n` 1-D vectors: doc `i` (1..=n) is `[i]`.
    fn build_flat_l2_index(n: usize) -> NonNull<VecSimIndex> {
        let params = VecSimParams {
            algo: VecSimAlgo_VecSimAlgo_BF,
            algoParams: AlgoParams {
                bfParams: BFParams {
                    type_: VecSimType_VecSimType_FLOAT32,
                    dim: 1,
                    metric: VecSimMetric_VecSimMetric_L2,
                    multi: false,
                    initialCapacity: n,
                    blockSize: 0,
                },
            },
            logCtx: ptr::null_mut(),
        };
        // SAFETY: `params` is fully initialised; `VecSimIndex_New` copies what it
        // needs and returns an owned index handle.
        let index = NonNull::new(unsafe { VecSimIndex_New(&params) }).expect("index");
        for i in 1..=n {
            let v = [i as f32];
            // SAFETY: `v` is one f32 matching the index dim/type; valid for the call.
            unsafe { VecSimIndex_AddVector(index.as_ptr(), v.as_ptr().cast::<c_void>(), i) };
        }
        index
    }

    /// Build a [`VectorScoreSource`] over `index` whose query params request the
    /// given hybrid `search_mode`. Other knobs use the same defaults as the
    /// `source` module tests (single `0.0f32` blob, null timeout ctx, RAM path).
    fn make_source(
        index: NonNull<VecSimIndex>,
        k: usize,
        child_num_estimated: usize,
        search_mode: VecSearchMode,
    ) -> VectorScoreSource<'static> {
        // SAFETY: zeroed `VecSimQueryParams` is a valid bit pattern the FLAT
        // backend ignores; we only override the search mode the constructor reads.
        let mut query_params: VecSimQueryParams = unsafe { std::mem::zeroed() };
        query_params.searchMode = search_mode;
        // SAFETY:
        // - The caller keeps `index` alive for the source's lifetime.
        // - The query blob is one f32, matching the FLAT index dim/type.
        // - Timeout checks are skipped (no deadline enforced) and the index is a
        //   RAM (non-disk) index.
        unsafe {
            VectorScoreSource::new(
                index,
                0.0f32.to_ne_bytes().to_vec(),
                query_params,
                NonZeroUsize::new(k).unwrap(),
                timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                true,
                false,
                false, // should_rerank — disk-only, N/A on RAM
                child_num_estimated,
                0,
                None,
            )
        }
    }

    fn make_child<'a>(ids: Vec<u64>) -> Box<dyn RQEIterator<'a> + 'a> {
        Box::new(IdList::<true>::new(ids))
    }

    /// An explicit `HYBRID_POLICY` of ADHOC must pin the iterator to adhoc-BF,
    /// matching the C hybrid reader honoring `qParams.searchMode`.
    #[test]
    fn explicit_adhoc_policy_is_honored() {
        let index = build_flat_l2_index(5);
        let source = make_source(index, 3, 3, VecSearchMode_HYBRID_ADHOC_BF);
        let it = new_vector_top_k_filtered_boxed(
            source,
            make_child(vec![1, 2, 3]),
            NonZeroUsize::new(3).unwrap(),
        );
        assert_eq!(it.mode(), TopKMode::AdhocBF);

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }

    /// An explicit `HYBRID_POLICY` of BATCHES must pin the iterator to
    /// `ForcedBatches`: the C reader's `reviewHybridSearchPolicy` returns false
    /// for user-requested batches, suppressing the mid-run switch to adhoc.
    #[test]
    fn explicit_batches_policy_forces_batches() {
        let index = build_flat_l2_index(5);
        let source = make_source(index, 3, 3, VecSearchMode_HYBRID_BATCHES);
        let it = new_vector_top_k_filtered_boxed(
            source,
            make_child(vec![1, 2, 3]),
            NonZeroUsize::new(3).unwrap(),
        );
        assert_eq!(it.mode(), TopKMode::ForcedBatches);

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }

    /// With no explicit policy the constructor consults the cost heuristic, which
    /// yields the switchable `Batches` or `AdhocBF` — never the forced variant.
    #[test]
    fn unset_policy_uses_heuristic() {
        let index = build_flat_l2_index(5);
        let source = make_source(index, 3, 3, VecSearchMode_EMPTY_MODE);
        let it = new_vector_top_k_filtered_boxed(
            source,
            make_child(vec![1, 2, 3]),
            NonZeroUsize::new(3).unwrap(),
        );
        assert!(
            matches!(it.mode(), TopKMode::Batches | TopKMode::AdhocBF),
            "heuristic path must not force batches; got {:?}",
            it.mode()
        );

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }
}
