/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VectorScoreSource`] tests ported from the Python `tests/pytests` KNN /
//! hybrid suites.
//!
//! These drive a real VecSim index but stand in for the whole query stack: a
//! Python numeric/tag/geo/text filter becomes an [`IdList`] child holding the
//! doc ids that would survive it. The source cannot tell those filters apart,
//! so one filtered-subset test covers the whole family. Each test names the
//! Python test it mirrors.
//!
//! Data model (shared with `source.rs`): unless noted, doc `i` is `[i; dim]`
//! under L2, so distance to query `[q; dim]` is `dim*(q-i)^2`.

use std::{cmp::Ordering, ffi::c_void, num::NonZeroUsize, ptr, ptr::NonNull};

use ffi::{
    AlgoParams, BFParams, HNSWParams, VecSimAlgo_VecSimAlgo_BF, VecSimAlgo_VecSimAlgo_HNSWLIB,
    VecSimIndex, VecSimIndex_AddVector, VecSimIndex_Free, VecSimIndex_New,
    VecSimMetric_VecSimMetric_Cosine, VecSimMetric_VecSimMetric_L2, VecSimParams,
    VecSimQueryParams, VecSimType_VecSimType_FLOAT32,
};
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator, RQEIteratorError};
use top_k::{TopKIterator, TopKMode};
use vector_score_source::{
    VectorScoreSource, new_vector_top_k_filtered, new_vector_top_k_unfiltered,
};

/// Ascending comparator: lower distance score is better (L2/IP/Cosine).
fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Encode `values` as the native-endian f32 byte blob VecSim expects.
fn blob(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_ne_bytes()).collect()
}

/// `[value; dim]` query blob.
fn uniform_blob(value: f32, dim: usize) -> Vec<u8> {
    blob(&vec![value; dim])
}

/// HNSW L2 index of `n` vectors, doc `i` (1..=n) is `[i; dim]`.
fn build_hnsw_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
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
            add(i as DocId, &vec![i as f32; dim]);
        }
    })
}

/// FLAT (exact brute-force) L2 index of `n` vectors, doc `i` is `[i; dim]`.
///
/// FLAT, not HNSW, wherever a non-corner query needs a deterministic ordering.
fn build_flat_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
    let params = flat_params(dim, VecSimMetric_VecSimMetric_L2, n);
    new_index(&params, |add| {
        for i in 1..=n {
            add(i as DocId, &vec![i as f32; dim]);
        }
    })
}

/// FLAT cosine index, doc `i` is `[i/n, 1, 1, ...]`.
///
/// Only the first coordinate varies, so doc direction approaches the `[1; dim]`
/// query as `i` grows; `[i; dim]` would be degenerate (all collinear).
fn build_flat_cosine_index(n: usize, dim: usize) -> NonNull<VecSimIndex> {
    let params = flat_params(dim, VecSimMetric_VecSimMetric_Cosine, n);
    new_index(&params, |add| {
        for i in 1..=n {
            let mut v = vec![1.0f32; dim];
            v[0] = i as f32 / n as f32;
            add(i as DocId, &v);
        }
    })
}

fn flat_params(dim: usize, metric: ffi::VecSimMetric, n: usize) -> VecSimParams {
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

/// Create an index from `params` and populate it via `fill`, handed an
/// `add(doc_id, &[f32])` closure. Centralises the unsafe VecSim FFI calls.
fn new_index(
    params: &VecSimParams,
    fill: impl FnOnce(&mut dyn FnMut(DocId, &[f32])),
) -> NonNull<VecSimIndex> {
    // SAFETY: `params` is a fully initialised config; `VecSimIndex_New` copies
    // what it needs and returns an owned index handle.
    let index = unsafe { VecSimIndex_New(params) };
    let index = NonNull::new(index).expect("VecSimIndex_New returned null");

    let mut add = |id: DocId, v: &[f32]| {
        // SAFETY: `v` holds `dim` f32 elements matching the index type/dim;
        // the pointer is valid for the duration of the call.
        unsafe {
            VecSimIndex_AddVector(index.as_ptr(), v.as_ptr() as *const c_void, id as usize);
        }
    };
    fill(&mut add);
    index
}

/// Construct a [`VectorScoreSource`] for query vector `query`.
///
/// # Safety
///
/// `index` must outlive the returned source (and any iterator built from it).
unsafe fn make_source(
    index: NonNull<VecSimIndex>,
    query: Vec<u8>,
    ef: usize,
    k: usize,
    child_est: usize,
) -> VectorScoreSource<'static> {
    // SAFETY: zeroed is a valid bit pattern for this POD-with-union config;
    // we then set the only field VecSim reads for an HNSW query (BF ignores it).
    let mut query_params: VecSimQueryParams = unsafe { std::mem::zeroed() };
    query_params.__bindgen_anon_1.hnswRuntimeParams.efRuntime = ef;

    // SAFETY: caller guarantees `index` outlives the source; `timeout_ctx` is
    // null and the index is a RAM (non-disk) index.
    unsafe {
        VectorScoreSource::new(
            index,
            query,
            query_params,
            NonZeroUsize::new(k).unwrap(),
            std::mem::zeroed(),
            true,
            false,
            false,
            child_est,
            0,
            None,
        )
    }
}

fn make_child(ids: Vec<DocId>) -> Box<dyn RQEIterator<'static>> {
    Box::new(IdList::<true>::new(ids))
}

/// Drain an iterator into the doc ids it yields, in read order.
fn collect_ids<I: RQEIterator<'static>>(it: &mut I) -> Vec<DocId> {
    std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect()
}

// FLAT backend coverage (source.rs only exercises HNSW).

/// From `test_hybrid_query_batches_mode_with_text`: unfiltered KNN on FLAT.
#[test]
fn flat_unfiltered_returns_top_k_nearest_by_score() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // Streamed by score: nearest first, so id 100 (distance 0) down to 91.
    assert_eq!(collect_ids(&mut it), (91..=100).rev().collect::<Vec<_>>());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_hybrid_query_batches_mode_with_text`: filtered KNN on FLAT.
#[test]
fn flat_filtered_full_child_yields_best_first() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Metric coverage (source.rs only covers L2).

/// From `test_vecsim.py::test_hybrid_query_cosine`: cosine nearest = highest
/// ids. Set membership (not strict order) to absorb FLOAT32 cosine ULP ties,
/// matching the Python tolerance.
#[test]
fn cosine_metric_filtered_top_k_are_highest_ids() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_cosine_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(1.0, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    let ids = collect_ids(&mut it);
    assert_eq!(ids.first().copied(), Some(n as DocId));
    let window: Vec<DocId> = ((n as DocId - 14)..=n as DocId).collect();
    for id in &ids {
        assert!(window.contains(id), "id {id} outside top-15 cosine window");
    }
    assert_eq!(ids.len(), k);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Non-corner query ordering (source.rs always queries the corner [n; dim]).

/// From `test_vecsim.py::test_hybrid_query_batches_mode_with_tags`: query the
/// middle id. Equal-distance pairs `(mid-d, mid+d)` resolve to the lower id
/// first (heap tie-break).
#[test]
fn middle_query_orders_by_distance_then_lower_id() {
    let (n, k, dim) = (100usize, 10usize, 4usize);
    let mid = (n / 2) as DocId;
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(mid as f32, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    let mut expected = vec![mid];
    for d in 1..=4 {
        expected.push(mid - d);
        expected.push(mid + d);
    }
    expected.push(mid - 5);
    assert_eq!(collect_ids(&mut it), expected);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// dim = 1 coverage (test_ft_aggregate_basic).

/// From `test_vecsim.py::test_ft_aggregate_basic` (KNN-3 portion).
#[test]
fn dim1_unfiltered_knn_top3() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), vec![1, 2, 3]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::test_ft_aggregate_basic` (hybrid `(@n:[0 5])` portion):
/// the filter passes ids 6..=10, the 3 nearest to `[0]` are 6, 7, 8.
///
/// `new_vector_top_k_filtered` auto-selects the mode, matching the real query:
/// a 10-doc index trips VecSim's small-index ADHOC_BF heuristic.
#[test]
fn dim1_filtered_subset_knn_top3() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    let child_ids: Vec<DocId> = (6..=10).collect();
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, child_ids.len()) };
    let child = make_child(child_ids);
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), vec![6, 7, 8]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// Regression: a forced Batches scan that switches to adhoc mid-run must not
/// emit a doc id twice. The same dim-1 layout makes `batch_strategy` flip to
/// `SwitchToAdhoc` after the first batch; before the heap was cleared on that
/// switch, `collect_adhoc`'s full rescan re-admitted batch-phase docs (`[6, 6,
/// 7]` instead of `[6, 7, 8]`).
#[test]
fn batches_switch_to_adhoc_yields_no_duplicates() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    let child_ids: Vec<DocId> = (6..=10).collect();
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, child_ids.len()) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    assert_eq!(collect_ids(&mut it), vec![6, 7, 8]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Mode-selection heuristic (PreferAdHocSearch).

/// From `test_vecsim.py::test_hybrid_query_with_geo`: a small index with a small
/// passing subset prefers adhoc-BF.
#[test]
fn small_index_prefers_adhoc() {
    let (n, k, dim) = (1000, 10, 4);
    let index = build_hnsw_index(n, dim);

    let subset = 31;
    // SAFETY: index outlives the source (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, subset) };
    assert!(source.prefer_adhoc(subset, k, true));

    drop(source);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::test_hybrid_query_with_numeric`: a contiguous high
/// subset yields the same top-k under Batches and AdhocBF.
#[test]
fn numeric_like_subset_batches_matches_adhoc() {
    let (n, k, dim) = (200, 10, 4);
    let child_ids: Vec<DocId> = (150..=n as DocId).collect();
    let expected: Vec<DocId> = ((n as DocId - 9)..=n as DocId).rev().collect();

    for mode in [TopKMode::Batches, TopKMode::AdhocBF] {
        let index = build_hnsw_index(n, dim);
        // SAFETY: index outlives the iterator (freed at end of scope).
        let source =
            unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, child_ids.len()) };
        let mut it = TopKIterator::new_with_mode(
            source,
            Some(make_child(child_ids.clone())),
            NonZeroUsize::new(k).unwrap(),
            asc,
            mode,
        );

        assert_eq!(collect_ids(&mut it), expected, "mode {mode:?}");

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }
}

/// From `test_hybrid.py::test_knn_custom_k`: a non-default k returns exactly k.
#[test]
fn custom_k_returns_exactly_k() {
    let dim = 4;
    let n = 100;
    for k in [2usize, 5] {
        let index = build_hnsw_index(n, dim);
        // SAFETY: index outlives the iterator (freed at end of scope).
        let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
        let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

        // Streamed by score: nearest (highest id) first.
        let expected: Vec<DocId> = ((n as DocId - k as DocId + 1)..=n as DocId).rev().collect();
        assert_eq!(collect_ids(&mut it), expected, "k = {k}");

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }
}

unsafe extern "C" {
    fn VecSim_SetTimeoutCallbackFunction(
        cb: Option<unsafe extern "C" fn(*mut c_void) -> std::ffi::c_int>,
    );
}

unsafe extern "C" fn always_timed_out(_ctx: *mut c_void) -> std::ffi::c_int {
    1
}

unsafe extern "C" fn never_timed_out(_ctx: *mut c_void) -> std::ffi::c_int {
    0
}

/// Installs the always-timeout callback; restores the no-op on drop so a
/// panicking assertion cannot leak timeout state to later tests.
///
/// Concurrent query tests rely on nextest's process isolation.
struct MockTimeout;
impl MockTimeout {
    fn enable() -> Self {
        // SAFETY: the fn pointer is valid for the whole program.
        unsafe { VecSim_SetTimeoutCallbackFunction(Some(always_timed_out)) };
        MockTimeout
    }
}
impl Drop for MockTimeout {
    fn drop(&mut self) {
        // SAFETY: restoring the global callback to a benign no-op.
        unsafe { VecSim_SetTimeoutCallbackFunction(Some(never_timed_out)) };
    }
}

/// From `test_vecsim.py::TestTimeoutReached` (KNN branch).
#[test]
fn unfiltered_propagates_timeout() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_hnsw_index(n, dim);
    let _mock = MockTimeout::enable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::TestTimeoutReached` (hybrid BATCHES branch).
#[test]
fn batches_propagates_timeout() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_hnsw_index(n, dim);
    let _mock = MockTimeout::enable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child((1..=n as DocId).collect())),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}
