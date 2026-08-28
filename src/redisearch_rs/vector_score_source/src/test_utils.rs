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

use std::{cell::UnsafeCell, ffi::c_void, ptr, ptr::NonNull};

use ffi::{
    AlgoParams, BFParams, HNSWParams, QueryRequestTimeout,
    QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_UNARMED, VecSearchMode, VecSearchMode_EMPTY_MODE,
    VecSimAlgo_VecSimAlgo_BF, VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimIndex, VecSimIndex_AddVector,
    VecSimIndex_Free, VecSimIndex_New, VecSimMetric, VecSimMetric_VecSimMetric_Cosine,
    VecSimMetric_VecSimMetric_L2, VecSimParams, VecSimQueryParams, VecSimType_VecSimType_FLOAT32,
    t_docId,
};
use rqe_iterators::{ExpirationChecker, IdList, NoOpChecker, RQEIterator};
use top_k::Ascending;

use crate::VectorScoreSource;

/// Lower distance score is better.
pub const fn asc() -> Ascending {
    Ascending
}

/// Native-endian f32 byte blob of `values`, as VecSim expects.
pub fn blob(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_ne_bytes()).collect()
}

/// `[value; dim]` query blob.
pub fn uniform_blob(value: f32, dim: usize) -> Vec<u8> {
    blob(&vec![value; dim])
}

/// Owns a VecSim index, freeing it on drop. Sources built from it borrow it,
/// so they cannot outlive the index.
pub struct TestIndex {
    index: NonNull<VecSimIndex>,
    /// Standalone sources use request timeout state whose active source remains UNARMED.
    timeout: Box<UnsafeCell<QueryRequestTimeout>>,
    /// Dimensionality every query blob for this index must match.
    dim: usize,
}

impl TestIndex {
    /// Create a `dim`-dimensional index from `params` and populate it via
    /// `fill(add)`.
    fn new(
        params: &VecSimParams,
        dim: usize,
        fill: impl FnOnce(&mut dyn FnMut(t_docId, &[f32])),
    ) -> Self {
        // SAFETY: `params` is fully initialised.
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
        // SAFETY: zero is a valid representation of the C timeout object. `UnsafeCell` permits
        // the C timeout API to access the storage without creating an aliased Rust reference.
        let timeout = Box::new(UnsafeCell::new(unsafe {
            std::mem::zeroed::<QueryRequestTimeout>()
        }));
        // Keep the intended state explicit rather than depending only on the enum's numeric value.
        unsafe {
            (*timeout.get()).kind = QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_UNARMED;
        }
        Self {
            index,
            timeout,
            dim,
        }
    }

    /// HNSW L2 index of `n` vectors; doc `i` (1..=n) is `[i; dim]`.
    pub fn hnsw(n: usize, dim: usize) -> Self {
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
        Self::new(&params, dim, |add| {
            for i in 1..=n {
                add(i as t_docId, &vec![i as f32; dim]);
            }
        })
    }

    /// FLAT (exact brute-force) L2 index of `n` vectors; doc `i` is `[i; dim]`.
    pub fn flat(n: usize, dim: usize) -> Self {
        Self::new(
            &flat_params(dim, VecSimMetric_VecSimMetric_L2, n),
            dim,
            |add| {
                for i in 1..=n {
                    add(i as t_docId, &vec![i as f32; dim]);
                }
            },
        )
    }

    /// FLAT cosine index; doc `i` is `[i/n, 1, 1, ...]`, approaching the
    /// `[1; dim]` query as `i` grows.
    pub fn flat_cosine(n: usize, dim: usize) -> Self {
        Self::new(
            &flat_params(dim, VecSimMetric_VecSimMetric_Cosine, n),
            dim,
            |add| {
                for i in 1..=n {
                    let mut v = vec![1.0f32; dim];
                    v[0] = i as f32 / n as f32;
                    add(i as t_docId, &v);
                }
            },
        )
    }

    /// Byte length [`QueryVector`](vecsim::QueryVector) requires for this index,
    /// whose fixtures are all `FLOAT32`.
    fn expected_blob_len(&self) -> usize {
        self.dim * size_of::<f32>()
    }

    /// The request timeout every source built from this index is handed.
    pub fn timeout_ptr(&self) -> *mut QueryRequestTimeout {
        self.timeout.get()
    }

    /// Build a [`VectorScoreSource`] over this index for the `query` blob, with
    /// no pinned `HYBRID_POLICY`. `ef` seeds HNSW's `efRuntime`; `child_est`
    /// seeds the batch-size heuristic.
    ///
    /// # Panics
    ///
    /// If `query` is not sized for this index; use [`uniform_blob`]/[`blob`]
    /// with the `dim` the index was built with.
    pub fn source(
        &self,
        query: Vec<u8>,
        ef: usize,
        k: usize,
        child_est: usize,
    ) -> VectorScoreSource<'_, NoOpChecker> {
        self.source_with_mode(query, ef, VecSearchMode_EMPTY_MODE, k, child_est)
    }

    /// [`Self::source`] with the requested `HYBRID_POLICY` pinned to
    /// `search_mode`.
    ///
    /// # Panics
    ///
    /// If `query` is not sized for this index, as [`Self::source`].
    pub fn source_with_mode(
        &self,
        query: Vec<u8>,
        ef: usize,
        search_mode: VecSearchMode,
        k: usize,
        child_est: usize,
    ) -> VectorScoreSource<'_, NoOpChecker> {
        self.source_inner(query, ef, search_mode, k, child_est, NoOpChecker)
    }

    /// [`Self::source`] with a field-`expiration` filter, consulted at yield
    /// time. `EMPTY_MODE` lets VecSim pick the search strategy.
    ///
    /// # Panics
    ///
    /// If `query` is not sized for this index, as [`Self::source`].
    pub fn source_with_expiration<E: ExpirationChecker>(
        &self,
        query: Vec<u8>,
        ef: usize,
        k: usize,
        child_est: usize,
        expiration: E,
    ) -> VectorScoreSource<'_, E> {
        self.source_inner(
            query,
            ef,
            VecSearchMode_EMPTY_MODE,
            k,
            child_est,
            expiration,
        )
    }

    /// Shared builder behind the `source*` fixtures.
    ///
    /// # Panics
    ///
    /// If `query` is not sized for this index, as [`Self::source`].
    fn source_inner<E: ExpirationChecker>(
        &self,
        query: Vec<u8>,
        ef: usize,
        search_mode: VecSearchMode,
        k: usize,
        child_est: usize,
        expiration: E,
    ) -> VectorScoreSource<'_, E> {
        assert_eq!(
            query.len(),
            self.expected_blob_len(),
            "query blob does not match the index dimensionality"
        );

        // SAFETY: zeroed is a valid bit pattern for this config; we then set only
        // the fields VecSim reads.
        let mut query_params: VecSimQueryParams = unsafe { std::mem::zeroed() };
        query_params.__bindgen_anon_1.hnswRuntimeParams.efRuntime = ef;
        query_params.searchMode = search_mode;

        // SAFETY: 1. `self` is borrowed for the source's lifetime. 2. `query` is
        // sized for it, asserted above. The timeout is owned by `self` and remains UNARMED.
        unsafe {
            VectorScoreSource::new(
                self.index,
                query,
                query_params,
                k,
                NonNull::new(self.timeout.get()).expect("boxed timeout is non-null"),
                child_est,
                0,
                expiration,
            )
        }
    }
}

impl Drop for TestIndex {
    fn drop(&mut self) {
        // SAFETY: sole owner of the index created in `Self::new`, freed once.
        unsafe { VecSimIndex_Free(self.index.as_ptr()) };
    }
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

/// A filter child yielding `ids`, backed by a sorted [`IdList`].
pub fn make_child<'index>(ids: Vec<t_docId>) -> Box<dyn RQEIterator<'index> + 'index> {
    Box::new(IdList::<true>::new(ids))
}

/// Drain an iterator into the doc ids it yields, in read order.
pub fn collect_ids<'index, I: RQEIterator<'index>>(it: &mut I) -> Vec<t_docId> {
    std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect()
}
