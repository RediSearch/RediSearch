/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Constructor mode-selection tests: how `new_vector_top_k_filtered_boxed`
//! maps the requested `HYBRID_POLICY` (or its absence) onto a [`TopKMode`].

use std::{ffi::c_void, num::NonZeroUsize, ptr, ptr::NonNull};

use ffi::{
    AlgoParams, BFParams, VecSearchMode, VecSearchMode_EMPTY_MODE, VecSearchMode_HYBRID_ADHOC_BF,
    VecSearchMode_HYBRID_BATCHES, VecSimAlgo_VecSimAlgo_BF, VecSimIndex, VecSimIndex_AddVector,
    VecSimIndex_Free, VecSimIndex_New, VecSimMetric_VecSimMetric_L2, VecSimParams,
    VecSimQueryParams, VecSimType_VecSimType_FLOAT32, timespec,
};
use rqe_iterators::{IdList, RQEIterator};
use top_k::TopKMode;
use vector_score_source::{VectorScoreSource, new_vector_top_k_filtered_boxed};

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
/// given hybrid `search_mode`.
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
            k,
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
            child_num_estimated,
            0,
        )
    }
}

fn make_child<'a>(ids: Vec<u64>) -> Box<dyn RQEIterator<'a> + 'a> {
    Box::new(IdList::<true>::new(ids))
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_adhoc_policy() {
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

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_batches_policy() {
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
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
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
