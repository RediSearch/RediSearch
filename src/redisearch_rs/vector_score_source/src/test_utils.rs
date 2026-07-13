/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared VecSim test fixtures for this crate's unit tests and the
//! `tests/integration` crate, gated behind the `unittest` feature.
//!
//! Index layout: doc `i` (1..=n) is `[i; dim]` under L2, so distance to query
//! `[q; dim]` is `dim*(q-i)^2` and the nearest neighbours are the highest ids.

use std::{cmp::Ordering, ffi::c_void, ptr, ptr::NonNull};

use ffi::{
    AlgoParams, BFParams, HNSWParams, VecSearchMode, VecSearchMode_EMPTY_MODE,
    VecSimAlgo_VecSimAlgo_BF, VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimIndex, VecSimIndex_AddVector,
    VecSimIndex_New, VecSimMetric, VecSimMetric_VecSimMetric_Cosine, VecSimMetric_VecSimMetric_L2,
    VecSimParams, VecSimQueryParams, VecSimType_VecSimType_FLOAT32, t_docId,
};
use rqe_iterators::{IdList, RQEIterator};

use crate::VectorScoreSource;

/// Ascending comparator: lower distance score is better.
pub fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Native-endian f32 byte blob of `values`, as VecSim expects.
pub fn blob(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_ne_bytes()).collect()
}

/// `[value; dim]` query blob.
pub fn uniform_blob(value: f32, dim: usize) -> Vec<u8> {
    blob(&vec![value; dim])
}

/// HNSW L2 index of `n` vectors; doc `i` (1..=n) is `[i; dim]`.
pub fn build_hnsw_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
    let params = VecSimParams {
        algo: VecSimAlgo_VecSimAlgo_HNSWLIB,
        algoParams: AlgoParams {
            hnswParams: HNSWParams {
                type_: VecSimType_VecSimType_FLOAT32,
                dim,
                metric: VecSimMetric_VecSimMetric_L2,
                multi: false,
                initialCapacity: n,
                blockSize: 0,
                M: 16,
                efConstruction: 100,
                efRuntime: 0,
                epsilon: 0.0,
            },
        },
        logCtx: ptr::null_mut(),
    };
    new_index(&params, |add| {
        for i in 1..=n {
            add(i as t_docId, &vec![i as f32; dim]);
        }
    })
}

/// FLAT (exact brute-force) L2 index of `n` vectors; doc `i` is `[i; dim]`.
pub fn build_flat_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
    new_index(&flat_params(dim, VecSimMetric_VecSimMetric_L2, n), |add| {
        for i in 1..=n {
            add(i as t_docId, &vec![i as f32; dim]);
        }
    })
}

/// FLAT cosine index; doc `i` is `[i/n, 1, 1, ...]`, approaching the `[1; dim]`
/// query as `i` grows.
pub fn build_flat_cosine_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
    new_index(
        &flat_params(dim, VecSimMetric_VecSimMetric_Cosine, n),
        |add| {
            for i in 1..=n {
                let mut v = vec![1.0f32; dim];
                v[0] = i as f32 / n as f32;
                add(i as t_docId, &v);
            }
        },
    )
}

fn flat_params(dim: usize, metric: VecSimMetric, n: usize) -> VecSimParams {
    VecSimParams {
        algo: VecSimAlgo_VecSimAlgo_BF,
        algoParams: AlgoParams {
            bfParams: BFParams {
                type_: VecSimType_VecSimType_FLOAT32,
                dim,
                metric,
                multi: false,
                initialCapacity: n,
                blockSize: 0,
            },
        },
        logCtx: ptr::null_mut(),
    }
}

/// Create an index from `params` and populate it via `fill(add)`.
fn new_index(
    params: &VecSimParams,
    fill: impl FnOnce(&mut dyn FnMut(t_docId, &[f32])),
) -> NonNull<VecSimIndex> {
    // SAFETY: `params` is fully initialised; `VecSimIndex_New` copies what it
    // needs and returns an owned index handle.
    let index = unsafe { VecSimIndex_New(params) };
    let index = NonNull::new(index).expect("VecSimIndex_New returned null");

    let mut add = |id: t_docId, v: &[f32]| {
        // SAFETY: `v` holds `dim` f32 elements matching the index type/dim;
        // valid for the duration of the call.
        unsafe {
            VecSimIndex_AddVector(index.as_ptr(), v.as_ptr() as *const c_void, id as usize);
        }
    };
    fill(&mut add);
    index
}

/// Build a [`VectorScoreSource`] over `index` for the `query` blob, with no
/// pinned `HYBRID_POLICY`. `ef` seeds HNSW's `efRuntime`; `child_est` seeds the
/// batch-size heuristic.
///
/// # Safety
///
/// 1. `index` must outlive the returned source (and any iterator built from it).
/// 2. `query` must match `index`'s type and dimension, the layout VecSim reads
///    in full on every query path; a shorter blob is read out of bounds. Use
///    [`uniform_blob`]/[`blob`] sized to the index `dim`.
pub unsafe fn make_source(
    index: NonNull<VecSimIndex>,
    query: Vec<u8>,
    ef: usize,
    k: usize,
    child_est: usize,
) -> VectorScoreSource<'static> {
    // SAFETY: forwarded to `make_source_with_mode`, same contract.
    unsafe { make_source_with_mode(index, query, ef, VecSearchMode_EMPTY_MODE, k, child_est) }
}

/// [`make_source`] with the requested `HYBRID_POLICY` pinned to `search_mode`.
///
/// # Safety
///
/// 1. `index` must outlive the returned source (and any iterator built from it).
/// 2. `query` must match `index`'s type and dimension, the layout VecSim reads
///    in full on every query path; a shorter blob is read out of bounds. Use
///    [`uniform_blob`]/[`blob`] sized to the index `dim`.
pub unsafe fn make_source_with_mode(
    index: NonNull<VecSimIndex>,
    query: Vec<u8>,
    ef: usize,
    search_mode: VecSearchMode,
    k: usize,
    child_est: usize,
) -> VectorScoreSource<'static> {
    // SAFETY: zeroed is a valid bit pattern for this config; we then set only
    // the fields VecSim reads.
    let mut query_params: VecSimQueryParams = unsafe { std::mem::zeroed() };
    query_params.__bindgen_anon_1.hnswRuntimeParams.efRuntime = ef;
    query_params.searchMode = search_mode;

    // SAFETY: caller-upheld: `index` stays alive and `query` matches its
    // type/dim (this fn's `# Safety`); null timeout ctx and a RAM (non-disk)
    // index.
    unsafe {
        VectorScoreSource::new(
            index,
            query,
            query_params,
            k,
            std::mem::zeroed(),
            true,
            child_est,
            0,
        )
    }
}

/// A filter child yielding `ids`, backed by a sorted [`IdList`].
pub fn make_child(ids: Vec<t_docId>) -> Box<dyn RQEIterator<'static>> {
    Box::new(IdList::<true>::new(ids))
}

/// Drain an iterator into the doc ids it yields, in read order.
pub fn collect_ids<I: RQEIterator<'static>>(it: &mut I) -> Vec<t_docId> {
    std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect()
}
