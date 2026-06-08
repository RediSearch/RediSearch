/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! End-to-end tests for [`VectorScoreSource`] driving a *real* VecSim index.
//!
//! Data model (mirrors the C++ `testHybridVector` reference in
//! `tests/cpptests/test_cpp_index.cpp`): document `i` is stored as the vector
//! `[i, i, i, i]` and the query vector is `[max_id; d]` under the L2 metric.
//! Distance therefore decreases monotonically with the doc id, so the nearest
//! neighbours are simply the highest ids — making every expected ordering
//! trivially predictable.

use std::{cmp::Ordering, ffi::c_void, num::NonZeroUsize, ptr, ptr::NonNull};

use ffi::{
    AlgoParams, HNSWParams, VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimIndex, VecSimIndex_AddVector,
    VecSimIndex_Free, VecSimIndex_New, VecSimMetric_VecSimMetric_L2, VecSimParams,
    VecSimQueryParams, VecSimType_VecSimType_FLOAT32, t_docId, timespec,
};
use std::collections::HashSet;

use index_result::RSResultKind;
use rqe_iterators::{ExpirationChecker, FieldExpirationChecker, IdList, RQEIterator};
use rqe_iterators_test_utils::MockExpirationChecker;
use top_k::{TopKIterator, TopKMode};
use vector_score_source::{
    VectorScoreSource, new_vector_top_k_filtered, new_vector_top_k_unfiltered,
};

const DIM: usize = 4;

/// Ascending comparator: lower distance score is better (L2/IP/Cosine).
fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Encode `[value; DIM]` as the little/native-endian f32 byte blob VecSim expects.
fn query_blob(value: f32) -> Vec<u8> {
    vec![value; DIM]
        .iter()
        .flat_map(|f| f.to_ne_bytes())
        .collect()
}

/// Build an HNSW index of `n` vectors where doc `i` (1..=n) is `[i; DIM]`.
fn build_hnsw_index(n: usize) -> NonNull<VecSimIndex> {
    let params = VecSimParams {
        algo: VecSimAlgo_VecSimAlgo_HNSWLIB,
        algoParams: AlgoParams {
            hnswParams: HNSWParams {
                type_: VecSimType_VecSimType_FLOAT32,
                dim: DIM,
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
    // SAFETY: `params` is a fully initialised HNSW config; `VecSimIndex_New`
    // copies what it needs and returns an owned index handle.
    let index = unsafe { VecSimIndex_New(&params) };
    let index = NonNull::new(index).expect("VecSimIndex_New returned null");

    for i in 1..=n {
        let vec = vec![i as f32; DIM];
        // SAFETY: `vec` holds DIM f32 elements matching the index type/dim;
        // the pointer is valid for the duration of the call.
        unsafe {
            VecSimIndex_AddVector(index.as_ptr(), vec.as_ptr() as *const c_void, i);
        }
    }
    index
}

/// Construct a [`VectorScoreSource`] querying for the corner `[n; DIM]` vector.
///
/// `child_est` seeds the batch-size heuristic. It must reflect the actual
/// number of docs the filter child is expected to yield — production passes
/// `child.num_estimated()` here — otherwise the heuristic mis-sizes batches.
///
/// # Safety
///
/// `index` must outlive the returned source (and any iterator built from it).
unsafe fn make_source(
    index: NonNull<VecSimIndex>,
    n: usize,
    k: usize,
    child_est: usize,
) -> VectorScoreSource<'static> {
    // SAFETY: caller-upheld `index` lifetime; no expiration filter.
    unsafe { make_source_with_expiration(index, n, k, child_est, None::<FieldExpirationChecker>) }
}

/// As [`make_source`], but installs an optional expiration filter.
///
/// # Safety
///
/// `index` must outlive the returned source.
unsafe fn make_source_with_expiration<E: ExpirationChecker>(
    index: NonNull<VecSimIndex>,
    n: usize,
    k: usize,
    child_est: usize,
    expiration: Option<E>,
) -> VectorScoreSource<'static, E> {
    // SAFETY: zeroed is a valid bit pattern for this POD-with-union config
    // struct; we then set the only field VecSim reads for an HNSW query.
    let mut query_params: VecSimQueryParams = unsafe { std::mem::zeroed() };
    query_params.__bindgen_anon_1.hnswRuntimeParams.efRuntime = n;

    // SAFETY: the caller guarantees `index` outlives the source; timeout
    // checks are skipped (no deadline enforced) and the index is a RAM
    // (non-disk) HNSW index.
    unsafe {
        VectorScoreSource::new(
            index,
            query_blob(n as f32),
            query_params,
            NonZeroUsize::new(k).unwrap(),
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
            false,
            false,
            child_est,
            0,
            expiration,
        )
    }
}

fn make_child(ids: Vec<t_docId>) -> Box<dyn RQEIterator<'static>> {
    Box::new(IdList::<true>::new(ids))
}

/// Drain an iterator into the doc ids it yields, in read order.
fn collect_ids<I: RQEIterator<'static>>(it: &mut I) -> Vec<t_docId> {
    std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect()
}

#[test]
fn unfiltered_returns_top_k_nearest_by_score() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: `index` is freed after the iterator is dropped at end of scope.
    let source = unsafe { make_source(index, n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // The unfiltered path streams VecSim's reply ordered by score, so the k
    // nearest neighbours come out best-first: id 100 (distance 0) down to 91.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn unfiltered_results_are_metric_kind_with_exact_distance() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // Estimate is capped at k (k < index size here).
    assert_eq!(it.num_estimated(), k);

    // Unfiltered streams by score, so ids 100..=91 come out best-first. Each
    // result is a Metric carrying the exact squared-L2 distance DIM*(n-id)^2.
    let mut expected_id = n as t_docId;
    while let Some(res) = it.read().unwrap() {
        assert_eq!(res.kind(), RSResultKind::Metric);
        assert_eq!(res.doc_id, expected_id);
        let dist = res
            .as_numeric()
            .expect("metric result carries a numeric value");
        let want = DIM as f64 * (n as f64 - expected_id as f64).powi(2);
        assert!(
            (dist - want).abs() < 1e-3,
            "id {expected_id}: distance {dist}, expected {want}"
        );
        expected_id -= 1;
    }
    assert_eq!(expected_id, n as t_docId - k as t_docId);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn first_result_after_rewind_is_best() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Batches drains the heap best-first, so after a rewind the first read must
    // again be the single best (lowest-distance) doc: the highest id at distance 0.
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    let _ = collect_ids(&mut it);
    it.rewind();

    assert_eq!(it.num_estimated(), k);
    let res = it.read().unwrap().expect("a result after rewind");
    assert_eq!(res.doc_id, n as t_docId);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn filtered_full_child_yields_best_first() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Public constructor auto-selects batches vs adhoc; either way the heap is
    // drained best-first, so ordering is deterministic.
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    // Best (lowest distance) first → highest ids first.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn filtered_batches_partial_child_intersects() {
    let n = 100;
    let k = 10;
    let step = 4;
    let index = build_hnsw_index(n);

    let child_ids: Vec<t_docId> = (1..=n)
        .filter(|i| i % step == 0)
        .map(|i| i as t_docId)
        .collect();
    // Seed the heuristic with the real child size so the heap fills within the
    // first (large) batch — mirroring how production sizes batches. Passing a
    // too-large estimate would shrink batches and force many passes, where
    // VecSim's approximate batch iterator can re-surface high-scoring docs.
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    // Only multiples of `step` pass the filter; best-first gives the top-k of
    // those: 100, 96, 92, ... 64.
    let ids = collect_ids(&mut it);
    let expected: Vec<t_docId> = (0..k).map(|c| (n - step * c) as t_docId).collect();
    assert_eq!(ids, expected);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn filtered_adhoc_matches_batches() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Force the adhoc-BF path (RAM lookups under shared locks) and verify it
    // produces the same ranking as the batches path.
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn filtered_adhoc_drops_nan_distance_docs() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // The adhoc-BF path looks up a distance per child id. Ids that were never
    // added to the vector index have no label, so VecSim returns NaN — the
    // source must drop them. Mixing real ids (91..=100) with phantom ids
    // (> n) exercises that filter against a real index, no mock needed.
    let real: Vec<t_docId> = (91..=100).collect();
    let phantom: Vec<t_docId> = vec![201, 202, 203];
    let mut child_ids = real.clone();
    child_ids.extend(&phantom);
    child_ids.sort_unstable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    // AdhocBF drives per-id lookups; the NaN drop lives only on this path
    // (Batches intersects against VecSim's real-id stream, so phantom ids
    // never reach the distance lookup).
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, real.iter().rev().copied().collect::<Vec<_>>());
    for p in phantom {
        assert!(
            !ids.contains(&p),
            "phantom id {p} with NaN distance must be filtered"
        );
    }

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn rewind_replays_same_results() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    let first = collect_ids(&mut it);
    assert!(it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    let second = collect_ids(&mut it);
    assert_eq!(first, second);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn disjoint_child_yields_nothing() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // No filter id exists in the index, so the intersection is empty.
    let child_ids = vec![1000, 2000, 3000];
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    let child = make_child(child_ids);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn unfiltered_skips_expired_docs() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // Mark the two nearest neighbours (ids 100, 99) expired.
    let checker = MockExpirationChecker::new(HashSet::from([100, 99]));
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source_with_expiration(index, n, k, n, Some(checker)) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // Top-k by score is ids 100..=91; the two expired nearest neighbours are
    // dropped without refill, leaving 98..=91.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=98).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
fn index_size_reflects_added_vectors() {
    let n = 42;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the source (freed at end of scope).
    let source = unsafe { make_source(index, n, 10, n) };
    assert_eq!(source.index_size(), n);

    drop(source);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}
